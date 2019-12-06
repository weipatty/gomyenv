package gomyenv

import (
	"reflect"
	"strings"
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
