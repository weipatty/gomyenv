package gomyenv


import (
	"fmt"
	"sync"
	"os"
	"strings"
	"io/ioutil"
)

func CheckNil(err error){
  if err != nil{
    fmt.Println("error",err)
    panic(err)
  }
}

type File struct{
	mutex sync.Mutex
	FileName  string
}
func (this *File)_WriteFile(msg string){
	fmt.Println("WriteFile")
	var b = []byte(msg)
	this.mutex.Lock()
	defer this.mutex.Unlock()
    ioutil.WriteFile(this.FileName, b, 0666) 
}
func (this *File)WriteFile(content string){
    this.mutex.Lock()
	defer this.mutex.Unlock()
	if len(this.FileName)>0{
		f, err := os.OpenFile(this.FileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
		  	fmt.Println("OpenFile failed",err.Error())
		} else {
			//ioutil func WriteFile(filename string, data []byte, perm os.FileMode) error
		  	_, err = f.Write([]byte(content+"\n"))
		  	if err != nil {
		  		fmt.Println("Write failed",err.Error())
		  	}
		  	f.Close() 
		} 
	}else{
		fmt.Println(content)
	}
}

func StringFiltList(str string,filt_list []string)bool{
	for _,filt := range filt_list{
		if len(filt)>0 && strings.Contains(str,filt){
			return true
		}
	}
	return false
}
func InIntList(i int,list []int)bool{
	for _,item := range list{
		if i==item{
			return true
		}
	}
	return false
}


