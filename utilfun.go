package gomyenv


import (
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