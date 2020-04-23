package gomyenv

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"
	"time"
)

func GetTimeStampString(ms int64) (str string) {
	return time.Unix(ms/1000, ms%1000*1000).Format("2006-01-02 15:04:05")
}

func GetNowTimeStampString() (str string) {
	return time.Now().Local().Format("2006-01-02 15:04:05")
}

func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func GetStringFmt(model string) (str string) {
	return strings.Replace(model, "%d", "%s", -1)
}

func isSlice(obj interface{}) (val reflect.Value, ok bool) {
	val = reflect.ValueOf(obj)
	if val.Kind() == reflect.Slice {
		ok = true
	}
	return
}

// interface{} -> []interface{}
func CreateAnyTypeSlice(slice interface{}) ([]interface{}, bool) {
	val, ok := isSlice(slice)
	if !ok {
		return nil, false
	}
	sliceLen := val.Len()
	out := make([]interface{}, sliceLen)
	for i := 0; i < sliceLen; i++ {
		out[i] = val.Index(i).Interface()
	}
	return out, true
}

// DefaultIP get a default non local ip, err is not nil, ip return 127.0.0.1
func DefaultIP() (ip string, err error) {
	ip = "127.0.0.1"

	ifaces, err := net.Interfaces()
	if err != nil {
		return
	}

	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			if ipStr := getAddrDefaultIP(addr); len(ipStr) > 0 {
				return ipStr, nil
			}
		}
	}

	err = errors.New("no ip found")
	return
}

func getAddrDefaultIP(addr net.Addr) string {
	var ip net.IP
	switch v := addr.(type) {
	case *net.IPNet:
		ip = v.IP
	case *net.IPAddr:
		ip = v.IP
	}
	if ip.IsUnspecified() || ip.IsLoopback() {
		return ""
	}

	ip = ip.To4()
	if ip == nil {
		return ""
	}

	return ip.String()
}

func Wait(second int) (err error) {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	fmt.Println("wait second:", second)
	select {
	case <-time.After(time.Second * time.Duration(second)):
		{
			fmt.Println("waited second:", second)
		}
	case <-sc:
		{
			close(sc)
			return errors.New("get exit")
		}
	}
	return nil
}
