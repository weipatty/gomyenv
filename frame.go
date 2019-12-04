package gomyenv


import (
	"fmt"
	"sync"
	"os"
	"os/signal"
	"syscall"
)


type MyMain interface {  
    Run() int
    Stop()
}
type MyLog interface {  
    Println(a ...interface{})
}
type MySignal interface {  
	RegisterSignal()
}
type MyArgparse interface {  
    ParseArg() int
    GetOptionsFile() string
}
type MyOptions interface {  
    ParseConfig(file string) int
    GetConfig(key string,must bool) string
    GetInt(key string) int
    GetIntList(key string) []int
}



type HelloMain struct{
}
func (this HelloMain)Run()int{
	fmt.Println("hello main")
	return 0
}
func (this HelloMain)Stop(){
	fmt.Println("hello main stop")
}

type MyLogFmt struct{
}
func (this MyLogFmt)Println(a ...interface{}){
	fmt.Println(a...)
}

type CommonSignal struct{
	signal_chan chan os.Signal
}
func (this *CommonSignal)RegisterSignal(){
	this.signal_chan = make(chan os.Signal,1)
	signal.Notify(this.signal_chan,syscall.SIGINT,syscall.SIGTERM)
	go this.signal_hander()
}
func (this CommonSignal)signal_hander(){
    for s := range this.signal_chan{
    	fmt.Println("catch signal",s)
        switch s {
	        case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
	 			Frame().Main.Stop()
	        default:
	            fmt.Println("default signal", s)
        }
    }
}

type frame struct{
	Options  MyOptions
	Argparse MyArgparse
	Main     MyMain
	Log      MyLog
	Signal   MySignal

	OptionsFileMust 	bool
}
var f *frame 
var once sync.Once
func Frame() *frame{
    once.Do(func(){
    	f = &frame{OptionsFileMust:true}
    })
    return f
}


func (this *frame)SetOptions(options MyOptions){
	this.Options = options
}
func (this *frame)SetArgparse(argparse MyArgparse){
	this.Argparse = argparse
}
func (this *frame)SetMyMain(main MyMain){
	this.Main = main
}
func (this *frame)SetMyLog(log MyLog){
	this.Log = log
}
func (this *frame)SetMySignal(signal MySignal){
	this.Signal = signal
}


func (this *frame)RunMain()int{
	if this.Log==nil{
		this.Log = &MyLogFmt{}
	}
	if this.Argparse==nil{
		this.Argparse = &CommonArgparse{}
	}
	if this.Argparse.ParseArg()!=0{
		fmt.Println("ParseArg fail")
		return -1
	}
	if this.Options==nil{
		this.Options = &CommonOptions{options_map: make(map[string]string)}
	}
	if this.OptionsFileMust && this.Options.ParseConfig(this.Argparse.GetOptionsFile())!=0{
		fmt.Println("ParseConfig fail")
		return -1
	}
	if this.Signal==nil{
		this.Signal = &CommonSignal{}
	}
	this.Signal.RegisterSignal()
	if this.Main==nil{
		this.Main = &HelloMain{}
	}
	return this.Main.Run()
}