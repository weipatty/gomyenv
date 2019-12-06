package thread

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"gomyenv"
	"gomyenv/my_mysql"
	"gomyenv/statictics"
	"strconv"
	"strings"
	"time"
)

/////////////////////////
type sqlWorkerStatictics struct {
	total_sql         *statictics.StaticticsUnit
	notsame_sql       *statictics.StaticticsUnit
	cuted_sql         *statictics.StaticticsUnit
	warn_diff_sql     *statictics.StaticticsUnit
	error_sql_mysql   *statictics.StaticticsUnit
	error_sql_myshard *statictics.StaticticsUnit
}

func NewDefaultSqlWorkerStatictics() *sqlWorkerStatictics {
	return &sqlWorkerStatictics{
		total_sql:         statictics.NewDefaultStaticticsUnit(),
		notsame_sql:       statictics.NewDefaultStaticticsUnit(),
		cuted_sql:         statictics.NewDefaultStaticticsUnit(),
		warn_diff_sql:     statictics.NewDefaultStaticticsUnit(),
		error_sql_mysql:   statictics.NewDefaultStaticticsUnit(),
		error_sql_myshard: statictics.NewDefaultStaticticsUnit(),
	}
}
func (this *sqlWorkerStatictics) show() {
	this.total_sql.ShowTitle()
	this.total_sql.ShowLine("total_sql")
	this.notsame_sql.ShowLine("notsame_sql")
	this.cuted_sql.ShowLine("cuted_sql")
	this.warn_diff_sql.ShowLine("warn_diff_sql")
	this.error_sql_mysql.ShowLine("error_sql_mysql")
	this.error_sql_myshard.ShowLine("error_sql_myshard")
}

/////////////////////////
type SqlWorkerManager struct {
	Manager
	sqlChan    chan string
	statictics *sqlWorkerStatictics

	save_file             gomyenv.File
	error_sql_record_file gomyenv.File
	warn_diff_sql_file    gomyenv.File
	myshard_config        string
	mysql_config          string

	Check_diff    int
	rewrite_limit int
}

func (this *SqlWorkerManager) SetSaveFile(file string) {
	this.save_file.FileName = file
}
func (this *SqlWorkerManager) SetRewriteLimit(limit int) {
	this.rewrite_limit = limit
}
func (this *SqlWorkerManager) Show() {
	this.statictics.show()
}

func (this *SqlWorkerManager) Start(count int, sqlChan chan string, warn_diff_sql_file string, error_sql_record_file string, myshard_config string, mysql_config string) {
	this.statictics = NewDefaultSqlWorkerStatictics()

	this.sqlChan = sqlChan
	this.warn_diff_sql_file.FileName = warn_diff_sql_file
	this.error_sql_record_file.FileName = error_sql_record_file
	this.myshard_config = myshard_config
	this.mysql_config = mysql_config
	if this.rewrite_limit <= 0 {
		this.rewrite_limit = 200000 // default
	}
	for i := 1; i <= count; i++ {
		this.AddWorker() //before run add
		go this.run(i)
	}
}

func (this *SqlWorkerManager) Stop() {
}

func (this *SqlWorkerManager) run(id int) {
	defer this.SubWorker()

	var (
		myshard_db   *sql.DB
		mysql_db     *sql.DB
		myshard_conn *sql.Conn
		mysql_conn   *sql.Conn
		err          error
		conn_string  string
	)

	conn_string = gomyenv.OptionsConvertDbConfigStringPortRand(this.myshard_config)
	fmt.Println("myshard conn_string", conn_string)
	myshard_db, err = sql.Open("mysql", conn_string)
	gomyenv.CheckNil(err)
	defer myshard_db.Close()

	conn_string = gomyenv.OptionsConvertDbConfigString(this.mysql_config)
	mysql_db, err = sql.Open("mysql", conn_string)
	gomyenv.CheckNil(err)
	defer mysql_db.Close()

	//myshard_db.SetConnMaxLifetime()
	//myshard_db.SetMaxIdleConns(0)
	//mysql_db.SetMaxIdleConns(0)

	//var max_idle_conn int = gomyenv.Frame().Options.GetInt("sql_worker_count")
	//fmt.Println("max_idle_conn",max_idle_conn)
	//myshard.SetMaxIdleConns(max_idle_conn)

	myshard_conn, err = myshard_db.Conn(context.Background())
	gomyenv.CheckNil(err)
	defer myshard_conn.Close()
	//if not doing this:Cannot assign requested address
	mysql_conn, err = mysql_db.Conn(context.Background())
	gomyenv.CheckNil(err)
	defer mysql_conn.Close()

	for sql := range this.sqlChan {
		this.statictics.total_sql.Add(1)
		//retry at most 2
		for i := 1; i <= 2; i++ {
			ret := this.execSql(&sql, mysql_conn, myshard_conn, i)
			//ok
			if ret == 0 {
				break
			}
			//reconnect and retry
			if ret < 0 {
				mysql_conn.Close()
				myshard_conn.Close()
				myshard_conn, err = myshard_db.Conn(context.Background())
				gomyenv.CheckNil(err)
				mysql_conn, err = mysql_db.Conn(context.Background())
				gomyenv.CheckNil(err)
			}
			//ret>0,just retry
		}
	}
}

//return 0:finish -1:reconnect and reexec 1:reexec
func (this *SqlWorkerManager) execSql(sql_string *string, mysql_conn *sql.Conn, myshard_conn *sql.Conn, times int) int {
	//var err mysql.MySQLError
	//fmt.Println(sql_string)
	myshard_rows, err := myshard_conn.QueryContext(context.Background(), *sql_string)
	if err != nil {
		if strings.Contains(err.Error(), "syntax error, unexpected") {
			this.statictics.cuted_sql.Add(1)
		} else {
			this.statictics.error_sql_myshard.Add(1)
			this.error_sql_record_file.WriteFile(GenerateDesp(*sql_string, "myshard:"+err.Error(), times))
		}
		if mysql_err, ok := err.(*mysql.MySQLError); ok {
			if mysql_err.Number < 2000 || mysql_err.Number > 2017 {
				return 0
			}
		}
		fmt.Println("reconnect myshard:" + *sql_string + ";" + err.Error())
		return -1
	}
	defer myshard_rows.Close()

	mysql_rows, err := mysql_conn.QueryContext(context.Background(), *sql_string)
	//mysql_rows,err := this.mysql.Query(sql_string)
	if err != nil {
		this.statictics.error_sql_mysql.Add(1)
		this.error_sql_record_file.WriteFile(GenerateDesp(*sql_string, "mysql:"+err.Error(), times))
		if mysql_err, ok := err.(*mysql.MySQLError); ok {
			if mysql_err.Number < 2000 || mysql_err.Number > 2017 {
				return 0
			}
		}
		fmt.Println("reconnect mysql:" + *sql_string + ";" + err.Error())
		return -1
	}
	defer mysql_rows.Close()
	if this.Check_diff > 0 {
		ret, err := my_mysql.RowsCompare(mysql_rows, myshard_rows)
		if err != nil {
			if ret < 0 {
				limit_idx := strings.LastIndex(*sql_string, " limit ")
				//idx no posible at begin,ensure sql_string[0:limit_idx-1]
				if limit_idx > 0 {
					if !strings.Contains(*sql_string, " where ") {
						this.statictics.warn_diff_sql.Add(1)
						this.warn_diff_sql_file.WriteFile(GenerateDesp(*sql_string, err.Error()+" limit without where", times))
					} else {
						if times > 1 {
							this.statictics.notsame_sql.Add(1)
							this.save_file.WriteFile(GenerateDesp(*sql_string, err.Error()+" rewrite still notsame", times))
						} else {
							this.statictics.warn_diff_sql.Add(1)
							this.warn_diff_sql_file.WriteFile(GenerateDesp(*sql_string, err.Error()+" try to entxend limit", times))
							//the effect is:[) ,and add a big limit maybe myshard must have a limit
							*sql_string = (*sql_string)[0:limit_idx] + " limit " + strconv.Itoa(this.rewrite_limit)
							//fmt.Println("rewrite_limit sql:"+*sql_string)
							return 1 //reexec
						}
					}
				} else {
					this.statictics.notsame_sql.Add(1)
					this.save_file.WriteFile(GenerateDesp(*sql_string, err.Error(), times))
				}
			} else if ret > 0 {
				this.statictics.warn_diff_sql.Add(1)
				this.warn_diff_sql_file.WriteFile(GenerateDesp(*sql_string, err.Error(), times))
			}
		}
	}
	return 0
}

func GenerateDesp(sql_string string, desp string, times int) string {
	return sql_string + ";" + desp + "[" + time.Now().Local().String() + "]" + "times:" + strconv.Itoa(times)
}

func RowsDiff(rows1 *sql.Rows, rows2 *sql.Rows) error {
	//fmt.Println(len(*rows1),len(*rows2))
	return errors.New("RowsDiff no impl")
}
