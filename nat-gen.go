package main

import (
	"bytes"
	"encoding/xml"
	"flag"
	//"flag"
	"fmt"
	"github.com/astaxie/beego/config"
	//"io"
	//"cgn-nat/customer"
	//"cgn-nat/session"
	"io/ioutil"
	"log"
	"nat-gen/autoconfig"
	"nat-gen/logs"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	//"strings"
	"time"
)

type SessionInfo struct {
	info  string
	atime string
	wtime string
}

type SyslogInfo struct {
	data []byte
	id   uint64
}
type Session interface {
	WriteSysLog(s []byte)
	InitSession()
	ReloadXML(iniconf config.Configer)
}

var sessiontype *string = flag.String("i", "c", "session type [c customer][s session]")
var session Session
var f *os.File
var err error
var exit chan bool
var datachan chan SyslogInfo
var logfile *logs.BeeLogger
var logdetail *logs.BeeLogger
var serverport, cache, threadnum, cacheprint int
var serverip string
var profileserver string
var logflag string

var execsql string
var xmlnode []string
var recvin uint64
var temprecv uint64
var natconfig *autoconfig.AutoConfig

func init() {
	//config
	iniconf, err := config.NewConfig("ini", "config.ini")
	if err != nil {
		panic(err.Error())
	}
	//serverinit
	serverip = iniconf.String("Server::ip")
	if serverip == "" {
		serverip = "0.0.0.0"
	}
	serverport, _ = iniconf.Int("Server::port")
	if serverport == 0 || serverport < 0 || serverport > 65535 {
		serverport = 3064
	}
	cache, _ = iniconf.Int("Server::cache")
	if cache == 0 || cache < 0 || cache > 3000000 {
		cache = 100000
	}
	datachan = make(chan SyslogInfo, cache)
	threadnum, _ = iniconf.Int("Server::threadnum")
	if threadnum == 0 || threadnum < 0 || threadnum > 30000 {
		threadnum = 10
	}
	profileserver = iniconf.String("Server::profileserver")

	logdir, err := os.Stat("log")
	if err != nil {
		err = os.Mkdir("log", 0777)
		if err != nil {
			panic(err)
		}
	} else {
		if logdir.IsDir() == false {
			err = os.Mkdir("log", 0777)
			if err != nil {
				panic(err)
			}
		}
	}
	logname := iniconf.String("Log::logname")
	if len(logname) == 0 {
		logname = "log/server.log"
	} else {
		logname = "log/" + logname
	}

	logsize, _ := iniconf.Int("Log::maxsize")
	if logsize == 0 {
		logsize = 500 * 1024 * 1024
	} else {
		logsize = logsize * 1024 * 1024
	}
	logsaveday, _ := iniconf.Int("Log::maxdays")
	if logsaveday == 0 {
		logsaveday = 7
	}
	loglevel, _ := iniconf.Int("Log::debug")
	if loglevel == 0 || loglevel < 0 || loglevel > 4 {
		loglevel = 1
	}
	cacheprint, _ = iniconf.Int("Log::cacheprint")
	if cacheprint == 0 || cacheprint < 0 {
		cacheprint = 10
	}
	logflag = iniconf.String("Log::logdetail")
	logfile = logs.NewLogger(10000)
	logfile.SetLevel(loglevel)
	logstr := fmt.Sprintf(`{"filename":"%s","maxsize":%d,"maxdays":%d}`, logname, logsize, logsaveday)
	logfile.SetLogger("file", logstr)
	logdetail = logs.NewLogger(10000)
	logdetail.SetLevel(1)
	logname = "log/logdetail.log"
	logstr = fmt.Sprintf(`{"filename":"%s","maxsize":%d,"maxdays":%d}`, logname, logsize, logsaveday)
	logdetail.SetLogger("file", logstr)
	execsql = iniconf.String("Exec::sql")
	xmlnode, err = LoadXmlNode("./nat-gen.xml")
	if err != nil {
		fmt.Println("load nat-gen.xml err.")
		os.Exit(2)
	}
	natconfig = new(autoconfig.AutoConfig)

}
func InitConfig() {
	for {
		natconfig.Load("config.ini")
		if natconfig.IsReload() {
			iniconf, err := config.NewConfig("ini", "config.ini")
			if err != nil {
				panic(err.Error())
			}

			loglevel, _ := iniconf.Int("Log::debug")
			if loglevel == 0 || loglevel < 0 || loglevel > 4 {
				loglevel = 1
			}
			logfile.SetLevel(loglevel)
			logflag = iniconf.String("Log::logdetail")
			session.ReloadXML(iniconf)
			logfile.Info("reload config success .")

		}
		time.Sleep(3 * time.Second)
	}

}
func main() {
	flag.Parse()
	if sessiontype != nil {
		fmt.Println("CGN Type ", *sessiontype)
	}
	go func() {
		log.Println(http.ListenAndServe(profileserver, nil))
	}()
	if *sessiontype == "c" || *sessiontype == "C" {
		natss := natcustomer.NewSession(logfile, xmlnode, execsql)
		session = natss
	} else if *sessiontype == "S" || *sessiontype == "s" {
		natss := natsession.NewSession(logfile, xmlnode, execsql)
		session = natss
	} else {
		fmt.Println("can not use this nat type.")
		os.Exit(1)
	}

	session.InitSession()

	addr, _ := net.ResolveUDPAddr("udp", serverip+":"+strconv.Itoa(serverport))
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		panic(err)
	}
	logfile.Info("started.")
	//启动监听线程
	go func() {
		for {
			var b [512]byte
			var recvlog SyslogInfo
			n, _ := conn.Read(b[:])
			recvin++
			recvlog.data = make([]byte, n)
			recvlog.data = b[:n]
			recvlog.id = recvin
			if logflag == "true" {
				logdetail.Info("%s,%d", string(recvlog.data), recvlog.id)
			}
			datachan <- recvlog
		}
	}()
	//启动对应数量线程
	for i := 0; i < threadnum; i++ {
		go WriteSysLog()
	}
	//启动统计线程
	ticker := time.NewTicker(time.Duration(cacheprint) * time.Second)
	go func() {
		for _ = range ticker.C {
			temp := (recvin - temprecv) / uint64(cacheprint)
			logfile.Info("now the recv cache is %d,recv in is %d,speed is %d.", cache-len(datachan), recvin, temp)
			temprecv = recvin
		}
	}()
	go InitConfig()
	fmt.Println("Started.")
	<-exit
}

func LoadXmlNode(filename string) (xmlnode []string, err error) {
	xmlfile, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}
	xmldecode := xml.NewDecoder(bytes.NewBuffer(xmlfile))
	var attrname string
	for t, err := xmldecode.Token(); err == nil; t, err = xmldecode.Token() {
		switch se := t.(type) {
		case xml.StartElement:
			attrname = se.Name.Local
		case xml.CharData:
			content := string([]byte(se))
			if content[0] != 15 && content[0] != 40 && content[0] != 12 && content[0] != 10 {
				xmlnode = append(xmlnode, attrname)

			}
		case xml.EndElement:
		default:
		}
	}
	return
}

func WriteSysLog() {
	for {
		rawdata := <-datachan
		session.WriteSysLog(rawdata.data)
	}
}
