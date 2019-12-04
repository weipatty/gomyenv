package thread

import (
    "fmt"
    //"time"
    "math/rand"
    "context"
    "database/sql"
    "github.com/go-sql-driver/mysql"
    "gomyenv"
    "gomyenv/statictics"
)

/////////////////////////
type sqlQpsStatictics struct {
    total_sql           *statictics.StaticticsUnit
    error_sql           *statictics.StaticticsUnit
}
func NewDefaultsqlQpsStatictics() *sqlQpsStatictics {
    return &sqlQpsStatictics{
        total_sql:              statictics.NewDefaultStaticticsUnit(),
        error_sql:              statictics.NewDefaultStaticticsUnit(),
    }
}
func (this *sqlQpsStatictics)show(){
    this.total_sql.ShowTitle()
    this.total_sql.ShowLine("total_sql")
    this.error_sql.ShowLine("error_sql")
}

/////////////////////////
type SqlexecWorkerManager struct{
    Manager
    sqlChan                 chan string
    statictics              *sqlQpsStatictics
    config_list              []string
}

func (this *SqlexecWorkerManager)Show(){
    this.statictics.show()
}

func (this *SqlexecWorkerManager)Start(count int,sqlChan chan string,config_list []string){
    this.statictics                         = NewDefaultsqlQpsStatictics()
    this.sqlChan                            = sqlChan
    this.config_list                        = config_list

    for i := 1; i <= count; i++{
        this.AddWorker() //before run add
        go this.run(i)
    }
}

func (this *SqlexecWorkerManager)Stop(){
}

func (this *SqlexecWorkerManager)run(id int){
    defer this.SubWorker()

    var conn_string string
    conn_string = this.config_list[rand.Intn(len(this.config_list))]
    fmt.Println("db conn_string",conn_string)

    db,err := sql.Open("mysql",conn_string)
    gomyenv.CheckNil(err)
    defer db.Close()

    conn,err := db.Conn(context.Background())
    gomyenv.CheckNil(err)
    defer conn.Close()

    for sql := range this.sqlChan {
        this.statictics.total_sql.Add(1) 
        this.execSql(sql,conn)
        //time.Sleep(time.Duration(100)*time.Millisecond)
    } 
}

func (this *SqlexecWorkerManager)execSql(sql_string string,conn *sql.Conn){
    //var err mysql.MySQLError
    
    rows,err := conn.QueryContext(context.Background(),sql_string)
    if err != nil {
        if mysql_err,ok := err.(*mysql.MySQLError); ok{
            if mysql_err.Number<2000 || mysql_err.Number>2017{ 
                this.statictics.error_sql.Add(1)
            }else{
                panic("mysql:"+sql_string+";"+err.Error())
            }
        }
        return   
    }
    defer rows.Close()
}
