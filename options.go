package gomyenv

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

type CommonOptions struct {
	options_map map[string]string
}

func (this *CommonOptions) ParseConfig(file string) int {
	f, err := os.Open(file)
	if err != nil {
		fmt.Println("open fail file", file, "err", err)
		return -1
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("ReadLine fail file", file, "err", err)
			return -1
		}
		s := strings.TrimSpace(string(b))
		if strings.HasPrefix(s, "#") {
			continue
		}
		index := strings.Index(s, "=")
		if index < 0 {
			continue
		}
		key := strings.TrimSpace(s[:index])
		if len(key) == 0 {
			continue
		}
		value := strings.TrimSpace(s[index+1:])
		if len(value) == 0 {
			continue
		}
		this.options_map[key] = value
	}
	for k, v := range this.options_map {
		fmt.Println(k, ":", v)
	}
	fmt.Println("")
	return 0
}

func (this CommonOptions) GetConfig(key string, must bool) string {
	value, ok := this.options_map[key]
	if ok {
		return value
	}
	if must {
		panic(fmt.Sprintf("no such config : %s", key))
	}
	return ""
}

func (this CommonOptions) GetInt(key string) int {
	value := this.GetConfig(key, true)
	int_value, _ := strconv.Atoi(value)
	return int_value
}

func (this CommonOptions) GetIntList(key string) (int_list []int) {
	value := this.GetConfig(key, true)
	str_list := strings.Split(value, "|")
	for _, item := range str_list {
		int_value, _ := strconv.Atoi(item)
		int_list = append(int_list, int_value)
	}
	return int_list
}

func OptionsConvertDbConfigString(config string) string {
	config_list := strings.Split(config, "|")
	if len(config_list) == 5 {
		return config_list[2] + ":" + config_list[3] + "@tcp(" + config_list[0] + ":" + config_list[1] + ")/" + config_list[4] + "?charset=utf8mb4"
	}
	return ""
}

func OptionsConvertDbConfigStringPortList(config string) (port_list []string) {
	config_list := strings.Split(config, "|")
	if len(config_list) == 5 {
		port_range := strings.Split(config_list[1], "-")
		if len(port_range) == 2 {
			min, _ := strconv.Atoi(port_range[0])
			max, _ := strconv.Atoi(port_range[1])
			for port := min; port <= max; port++ {
				conn_string := config_list[2] + ":" + config_list[3] + "@tcp(" + config_list[0] + ":" + strconv.Itoa(port) + ")/" + config_list[4] + "?charset=utf8mb4"
				port_list = append(port_list, conn_string)
			}
		} else if len(port_range) == 1 {
			conn_string := config_list[2] + ":" + config_list[3] + "@tcp(" + config_list[0] + ":" + config_list[1] + ")/" + config_list[4] + "?charset=utf8mb4"
			port_list = append(port_list, conn_string)
		}
	}
	return port_list
}

func OptionsConvertDbConfigStringPortRand(config string) string {
	conn_string_list := OptionsConvertDbConfigStringPortList(config)
	return conn_string_list[rand.Intn(len(conn_string_list))]
}
