package natcustomer

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/astaxie/beego/config"
	_ "github.com/go-sql-driver/mysql"
	"nat-gen/logs"
	"os"
	"regexp"
	"strings"
	"time"
)

type Session struct {
	logfile *logs.BeeLogger
	xmlnode []string
	execsql string
}

var db *sql.DB
var stmt *sql.Stmt
var sqlnode []string

func NewSession(l *logs.BeeLogger, x []string, s string) *Session {
	ss := new(Session)
	ss.logfile = l
	ss.xmlnode = x
	ss.execsql = s
	return ss
}
func (s *Session) InitSession() {
	iniconf, err := config.NewConfig("ini", "config.ini")
	dbhost := iniconf.String("Db::dbhost")
	dbport := iniconf.String("Db::dbport")
	dbuser := iniconf.String("Db::dbuser")
	dbpassword := iniconf.String("Db::dbpassword")
	dbname := iniconf.String("Db::dbname")
	dsn := dbuser + ":" + dbpassword + "@tcp(" + dbhost + ":" + dbport + ")/" + dbname + "?charset=utf8&loc=Asia%2FShanghai"
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		panic(err.Error())
	}
	db.SetMaxIdleConns(500)
	db.SetMaxOpenConns(500)
	var dbisok = make(chan bool, 0)
	go func() {
		var isok bool
		for i := 0; i < 5; i++ {
			fmt.Println("start to pin")
			err = db.Ping()
			if err != nil {
				fmt.Printf("Db connect error,rety 10 seconds after, now rety %d count\n", i+1)
				time.Sleep(time.Second * 1)
				continue
			}
			isok = true
		}
		dbisok <- isok
	}()
	isok := <-dbisok
	if isok == false {
		fmt.Println("Db not connect,please check db!")
		os.Exit(2)
	} else {
		fmt.Println("Db connect.")
	}
	s.execsql = FormatSql(s.execsql)
	stmt, err = db.Prepare(s.execsql)
	if err != nil {
		s.logfile.Info("Prepare err:%s|[%s]", s.execsql, err.Error())
		os.Exit(1)
	}
	go func() {
		for {
			db.Ping()
			time.Sleep(5 * time.Second)
		}

	}()
}
func (s *Session) WriteSysLog(data []byte) {
	str := string(data)
	lognode, err := DecodeSyslog(ReplaceDot(str), s.xmlnode)
	if err != nil {
		s.logfile.Info("%s|%s", str, err.Error())
		return
	}
	y, _ := lognode["Year"]
	m, _ := lognode["Mon"]
	d, _ := lognode["Day"]
	hms, _ := lognode["Hms"]
	month := EncodeMon(m)
	tstr := y + "-" + month + "-" + d + " " + hms
	lognode["Map_Time"] = tstr
	lognode["Type"] = EncodeMsgId(lognode["MsgId"])
	sql := stmtsql(lognode, sqlnode)
	//row, err := stmt.Query(lognode["Type"], lognode["OriSIp"], lognode["TranFPort"], lognode["TranSPort"], lognode["TranSIp"], lognode["Map_Time"])
	row, err := stmt.QuerySlice(sql)
	if err != nil {
		s.logfile.Info("Query err:%s|[%s]", str, err.Error())
		return
	}
	for row.Next() {
		var result int
		row.Scan(&result)
		s.logfile.Info("result:%s|%d", str, result)
	}
	s.logfile.Debug("result is null.[%s]", str)
}
func stmtsql(lognode map[string]string, sqlnode []string) []string {
	rs := make([]string, 0, 10)
	for _, n := range sqlnode {
		v, ok := lognode[n]
		if ok {
			rs = append(rs, v)
		} else {
			rs = append(rs, " ")
		}
	}
	return rs
}
func (s *Session) ReloadXML(iniconf config.Configer) {
	return
}
func FormatSql(sql string) string {

	var digitsRegexp = regexp.MustCompile(`\${.*?}`)
	node := digitsRegexp.FindAllString(sql, -1)
	for _, v := range node {
		v = strings.Replace(v, "${", "", -1)
		v = strings.Replace(v, "}", "", -1)
		sqlnode = append(sqlnode, v)
	}
	s := digitsRegexp.ReplaceAllString(sql, "?")

	return s
}
func ReplaceDot(s string) string {
	source := []byte(s)
	tmp := make([]byte, 1024)
	var tmpindex int
	for index, v := range source {
		if v == '[' {
			tmp = source[:index]
			tmpindex = index
		} else if v == ']' {
			tmp = append(tmp, source[tmpindex+1:index]...)
		}
	}
	return string(tmp)
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
func EncodeMsgId(id string) string {
	b := []byte(id)
	if b[len(b)-1] == 'A' {
		return "1"
	} else {
		return "2"
	}
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
