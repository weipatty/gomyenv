package gomyenv

import (
	"fmt"
	"io/ioutil"
	"net"
)

func TcpSend(addrString string, send string) (result []byte, err error) {
	addr, err := net.ResolveTCPAddr("tcp4", addrString)
	if err != nil {
		fmt.Println("ResolveTCPAddr fail", err)
		return result, err
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		fmt.Println("DialTCP fail", addr, err)
		return result, err
	}
	defer conn.Close()
	_, err = conn.Write([]byte(send))
	if err != nil {
		fmt.Println("Write fail", err)
		return result, err
	}
	//fmt.Println(cmd, "send size:", size)
	result, err = ioutil.ReadAll(conn)
	if err != nil {
		fmt.Println("ReadAll fail", err)
		return result, err
	}
	return result, nil
}
