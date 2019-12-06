package gomyenv

import (
	"fmt"
	"github.com/Shopify/sarama"
	"reflect"
)

func PrintReflect(input interface{}) {

	getType := reflect.TypeOf(input)
	getKind := getType.Kind()
	fmt.Println("get Type:", getType.Name(), " Kind:", getKind)

	getValue := reflect.ValueOf(input)
	fmt.Println("get all Fields is:", getValue)

	if getKind == reflect.Struct {
		fmt.Println("get NumField is:", getType.NumField())
		for i := 0; i < getType.NumField(); i++ {
			field := getType.Field(i)
			if getValue.Field(i).CanInterface() {
				fmt.Printf("%d %s: %v = %v\n", i, field.Name, field.Type, getValue.Field(i).Interface())
			} else {
				fmt.Printf("%d %s: %v cant interface?\n", i, field.Name, field.Type)
			}
		}
	}

	fmt.Println("get NumMethod is:", getType.NumMethod())
	for i := 0; i < getType.NumMethod(); i++ {
		m := getType.Method(i)
		fmt.Printf("%s: %v\n", m.Name, m.Type)
	}
	if getType.NumMethod() == 0 {
		fmt.Println("no method?notice that func (this *Your)test(t int) use pointer can get it")
		fmt.Println("use func (this Your)test(t int)")
	}
}

func PrintKafkaMetadataResponse(response *sarama.MetadataResponse) {
	fmt.Printf("response:%#v\n", response)
	fmt.Printf("Brokers:%#v\n", response.Brokers)
	fmt.Printf("Brokers0:%#v\n", response.Brokers[0])
	fmt.Printf("Topics:%#v\n", response.Topics)
	fmt.Printf("Topics0:%#v\n", response.Topics[0])
	fmt.Printf("Partitions:%#v\n", response.Topics[0].Partitions)
	fmt.Printf("Partitions0:%#v\n", response.Topics[0].Partitions[0])
}
