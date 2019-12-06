package thread

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"gomyenv"
	"gomyenv/statictics"
	"os"
	"strings"
	"time"
)

/////////////////////////
type FileParseInfo struct {
	File_name    string
	Separator    string
	Total_column int
	Need_column  int
}

/////////////////////////
type fileWorkerStatictics struct {
	total_line       *statictics.StaticticsUnit
	error_line       *statictics.StaticticsUnit
	notselect_line   *statictics.StaticticsUnit
	filt_line        *statictics.StaticticsUnit
	myshard_sql_line *statictics.StaticticsUnit
	finish_file      int
}

func NewDefaultFileWorkerStatictics() *fileWorkerStatictics {
	return &fileWorkerStatictics{
		total_line:       statictics.NewDefaultStaticticsUnit(),
		error_line:       statictics.NewDefaultStaticticsUnit(),
		notselect_line:   statictics.NewDefaultStaticticsUnit(),
		filt_line:        statictics.NewDefaultStaticticsUnit(),
		myshard_sql_line: statictics.NewDefaultStaticticsUnit(),
	}
}
func (this *fileWorkerStatictics) show() {
	this.total_line.ShowTitle()
	this.total_line.ShowLine("total_line")
	this.error_line.ShowLine("error_line")
	this.notselect_line.ShowLine("notselect_line")
	this.myshard_sql_line.ShowLine("myshard_sql_line")
	this.filt_line.ShowLine("filt_line")
	fmt.Printf("%50s %15s\n", "name", "count")
	fmt.Printf("%50s %15d\n", "finish_file", this.finish_file)
}

/////////////////////////
type FileWorkerManager struct {
	Manager
	fileChan         chan FileParseInfo
	sqlChan          chan string
	statictics       *fileWorkerStatictics
	myshard_sql_file gomyenv.File
	filt_list        []string
}

func (this *FileWorkerManager) Show() {
	this.statictics.show()
}

func (this *FileWorkerManager) SetFiltList(global_filt_word string) {
	this.filt_list = strings.Split(global_filt_word, "|")
}

func (this *FileWorkerManager) Start(count int, fileChan chan FileParseInfo, sqlChan chan string, myshard_sql_file string, global_filt_word string) {
	this.statictics = NewDefaultFileWorkerStatictics()

	this.fileChan = fileChan
	this.sqlChan = sqlChan
	this.myshard_sql_file.FileName = myshard_sql_file

	this.filt_list = strings.Split(global_filt_word, "|")

	for i := 1; i <= count; i++ {
		this.AddWorker() //before run add
		go this.run(i)
	}
}

func (this *FileWorkerManager) Stop() {
}

func (this *FileWorkerManager) run(id int) {
	defer this.SubWorker()
	for fileinfo := range this.fileChan {
		this.AddRunning()
		start := time.Now().Local()
		fmt.Println(start, "begin parseFile", fileinfo.File_name)
		this.parseFile(fileinfo)
		fmt.Println(time.Now().Local(), "finish parseFile", fileinfo.File_name, "use", time.Now().Local().Sub(start))
		this.statictics.finish_file += 1
		this.SubRunning()
	}
}

func (this *FileWorkerManager) parseFile(fileinfo FileParseInfo) {
	var (
		line       string
		err        error
		file       *os.File
		gz_reader  *gzip.Reader
		buf_reader *bufio.Reader
	)

	file, err = os.Open(fileinfo.File_name)
	gomyenv.CheckNil(err)
	defer file.Close()

	if strings.Contains(fileinfo.File_name, "gz") {
		gz_reader, err = gzip.NewReader(file)
		gomyenv.CheckNil(err)
		defer gz_reader.Close()
		buf_reader = bufio.NewReader(gz_reader)
	} else {
		buf_reader = bufio.NewReader(file)
	}

	for {
		line, err = buf_reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				break
			} else {
				fmt.Println("read file error", fileinfo.File_name)
				panic(err)
			}
		}
		this.statictics.total_line.Add(1)
		//fmt.Println("line",line)
		line = strings.TrimSpace(line)
		line_list := strings.Split(line, fileinfo.Separator)
		//fmt.Println("line_list",line_list,"len",len(line_list))
		if len(line_list) != fileinfo.Total_column {
			this.statictics.error_line.Add(1)
			continue
		}
		//sql := strings.TrimSpace(line_list[fileinfo.Need_column])
		sql := strings.Trim(line_list[fileinfo.Need_column], ". ")
		//fmt.Println("sql",sql)
		//fmt.Println(line,sql,len(filt_list))
		if gomyenv.StringFiltList(sql, this.filt_list) {
			this.statictics.filt_line.Add(1)
			continue
		}
		//fmt.Println("sql prefix 1","2",sql[:6])
		if !strings.HasPrefix(sql, "select") {
			this.statictics.notselect_line.Add(1)
			continue
		}
		if strings.Contains(sql, "__date_partition") {
			if len(this.myshard_sql_file.FileName) > 0 {
				this.myshard_sql_file.WriteFile(sql)
			}
			this.statictics.myshard_sql_line.Add(1)
			continue
		}
		this.sqlChan <- sql
	}
}
