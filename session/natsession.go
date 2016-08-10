package natsession

import (
	"errors"
	"fmt"
	"github.com/astaxie/beego/config"
	"io"
	"nat-gen/logs"
	"os"
	"strings"
	"sync"
	"time"
)

type SessionInfo struct {
	info  string
	atime string
	wtime string
}

var f *os.File
var err error
var writedata chan string
var filename string
var fileindex uint64
var tmpindex uint64
var count uint64
var timecount int64
var execsql string
var useattr []string
var filetimespan int
var filecount int
var sqlnode []string
var mutex sync.Mutex
var userinfo map[string]SessionInfo

type Session struct {
	logfile *logs.BeeLogger
	xmlnode []string
	execsql string
}

func NewSession(l *logs.BeeLogger, x []string, s string) *Session {
	ss := new(Session)
	ss.logfile = l
	ss.xmlnode = x
	ss.execsql = s
	return ss
}
func (s *Session) ReloadXML(iniconf config.Configer) {
	filecount, _ = iniconf.Int("Server::filecount")
	if filecount == 0 {
		filecount = 10000
	}
	filetimespan, _ = iniconf.Int("Server::filetimespan")
	if filetimespan == 0 {
		filetimespan = 15
	}
}
func (s *Session) InitSession() {
	iniconf, err := config.NewConfig("ini", "config.ini")
	if err != nil {
		panic(err.Error())
	}
	filecount, _ = iniconf.Int("Server::filecount")
	if filecount == 0 {
		filecount = 10000
	}
	filetimespan, _ = iniconf.Int("Server::filetimespan")
	if filetimespan == 0 {
		filetimespan = 15
	}
	cache, _ := iniconf.Int("Server::cache")
	if cache == 0 || cache < 0 || cache > 3000000 {
		cache = 100000
	}
	writedata = make(chan string, cache)
	sqlnode = FormatSql(s.execsql)
	userinfo = make(map[string]SessionInfo)
	filename = fmt.Sprintf("log/%s_%d.log", time.Now().Format("20060102150405"), fileindex)
	f, err = os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		panic(err.Error())
	}
	go s.WriteToFile()
}
func (s *Session) WriteSysLog(data []byte) {
	str := string(data)
	lognode, err := DecodeSyslog(ReplaceDot(str), s.xmlnode)
	if err != nil {
		s.logfile.Info("%s|%s", str, err.Error())
		return
	}
	l4, _ := lognode["L4"]
	if l4 == "6" {
		lognode["L4"] = "TCP"
	} else if l4 == "17" {
		lognode["L4"] = "UDP"
	} else {
		s.logfile.Debug("discard log[%s]", str)
		return
	}
	str = EncodeSysLog(lognode, sqlnode)
	var info SessionInfo
	y, _ := lognode["Year"]
	m, _ := lognode["Mon"]
	d, _ := lognode["Day"]
	h, _ := lognode["Hms"]
	msgid, _ := lognode["MsgId"]
	pubip, _ := lognode["OriSIp"]
	pubport, _ := lognode["TranFPort"]
	key := pubip + ":" + pubport
	//info.timestamp = CalcTime(y, m, d, hms)
	month := EncodeMon(m)
	hms := strings.Replace(h, ":", "", -1)
	tstr := y + month + d + hms
	info.atime = tstr
	info.wtime = tstr
	info.info = str
	s.logfile.Debug("info = %s,tstr = %s ,key = %s, msgid = %d.", info.info, tstr, key, msgid[len(msgid)-1])
	session, ok := GetUserSession(key, info, msgid[len(msgid)-1])
	if ok {
		str = str + session.atime + " " + session.wtime + "\n"
		writedata <- str
	}
}

func (s *Session) WriteToFile() {
	for {
		str := <-writedata
		//fmt.Println(str)
		s.logfile.Debug("write to file=%s", str)
		if fileindex != tmpindex {
			f.Close()
			filename = fmt.Sprintf("log/%s_%d.log", time.Now().Format("20060102150405"), fileindex)
			f, err = os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
			if err != nil {
				panic(err.Error())
			}
			tmpindex = fileindex
		}
		if (time.Now().Unix()-timecount >= int64(filetimespan*60)) || (count >= uint64(filecount)) {
			fileindex++
			timecount = time.Now().Unix()
			count = 0
		}
		if fileindex == 5 {
			fileindex = 0
		}
		count++
		io.WriteString(f, str)
	}

}

func GetUserSession(key string, info SessionInfo, stype byte) (SessionInfo, bool) {
	var usersessioninfo SessionInfo
	var r = false
	mutex.Lock()
	value, ok := userinfo[key]
	if ok {
		switch stype {
		case 'A':
			delete(userinfo, key)
			userinfo[key] = info
		case 'W':
			usersessioninfo.info = info.info
			usersessioninfo.atime = value.atime
			usersessioninfo.wtime = info.wtime
			delete(userinfo, key)
			r = true
		}
	} else {
		switch stype {
		case 'A':
			userinfo[key] = info
		case 'W':
			usersessioninfo = info
			//delete(userinfo, key)
			r = true
		}
	}
	mutex.Unlock()
	return usersessioninfo, r
}

func FormatSql(sql string) (sqlnode []string) {
	//sql = strings.ToUpper(sql)
	sqlnode = strings.Split(sql, " ")
	return
}
func EncodeSysLog(lognode map[string]string, sqlnode []string) (result string) {
	for _, n := range sqlnode {
		v, ok := lognode[n]
		if ok {
			result += v + " "
		} else {
			result += " "
		}
	}
	return
}
func DecodeSyslog(logstr string, xmlnode []string) (lognode map[string]string, err error) {

	str := strings.Replace(logstr, "  ", " ", -1)
	node := strings.Split(str, " ")
	if len(node) != len(xmlnode) {
		err = errors.New("syslog format err.")
		return
	}
	lognode = make(map[string]string)
	for index, v := range xmlnode {
		lognode[v] = node[index]
	}

	return
}
func ReplaceDot(s string) string {
	source := []byte(s)
	tmp := make([]byte, 1024)
	var tmpindex int
	for index, v := range source {
		if v == '[' {
			//fmt.Println(string(source[:index]))
			tmp = source[:index]
			tmpindex = index
			//fmt.Println(string(tmp))
		} else if v == ']' {
			//fmt.Println(string(source[tmpindex+1 : index]))
			tmp = append(tmp, source[tmpindex+1:index]...)
		}
	}
	return string(tmp)
}
func EncodeMon(m string) string {
	var month string
	switch m {
	case "Jan":
		month = "01"
	case "Feb":
		month = "02"
	case "Mar":
		month = "03"
	case "Apr":
		month = "04"
	case "May":
		month = "05"
	case "Jun":
		month = "06"
	case "Jul":
		month = "07"
	case "Aug":
		month = "08"
	case "Sep":
		month = "09"
	case "Oct":
		month = "10"
	case "Nov":
		month = "11"
	case "Dec":
		month = "12"
	}
	return month
}
