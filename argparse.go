package gomyenv


import (
	"fmt"
	"flag"
)

type CommonArgparse struct{
	options_file string
}
func (this *CommonArgparse)ParseArg()int{
    flag.StringVar(&this.options_file, "f", "", "options_file")
    flag.Parse()
    if this.options_file==""{
        fmt.Println("OptionsFile fail")
        return -1
    } 
    return 0
}
func (this CommonArgparse)GetOptionsFile()string{
    return this.options_file
}
