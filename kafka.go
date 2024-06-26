package gomyenv

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
	"time"
)

func OpenBroker(broker *sarama.Broker) (err error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_2_0_0
	//fmt.Println(config.Version)
	//fmt.Printf("before:%#v\n",broker)
	err = broker.Open(config)
	//fmt.Printf("before:%#v\n",broker)
	return err
}

func getTopicRetention(broker *sarama.Broker, tables *map[string]string) (err error) {
	var request sarama.DescribeConfigsRequest
	for topic, _ := range *tables {
		//request := sarama.DescribeConfigsRequest{
		//	//Version:2,
		//	Resources: []*sarama.ConfigResource{
		//		&sarama.ConfigResource{Type: sarama.TopicResource, Name: topic, ConfigNames: []string{"retention.ms"}},
		//	},
		//}
		request.Resources = append(request.Resources, &sarama.ConfigResource{Type: sarama.TopicResource, Name: topic, ConfigNames: []string{"retention.ms"}})
	}
	response, err := broker.DescribeConfigs(&request)
	if err != nil {
		return errors.New(err.Error() + "[request kafka DescribeConfigs fail]")
	}
	if len(response.Resources) > 0 {
		for _, r := range response.Resources {
			if r.ErrorCode == 0 && len(r.Configs) > 0 {
				(*tables)[r.Name] += fmt.Sprintf("(retention.ms:%s)", r.Configs[0].Value)
			}
		}
	}
	return nil
}

func getKafkaTopicOffsetResponse(broker *sarama.Broker, meta *sarama.MetadataResponse, ms int64) (response *sarama.OffsetResponse, err error) {
	var request sarama.OffsetRequest
	request.Version = 1
	for _, topic := range meta.Topics {
		for _, partition := range topic.Partitions {
			if broker.ID() == partition.Leader {
				request.AddBlock(topic.Name, partition.ID, ms, 0)
			}
		}
	}
	response, err = broker.GetAvailableOffsets(&request)
	if err != nil {
		return response, errors.New(err.Error() + "[request kafka GetAvailableOffsets fail]")
	}
	return response, nil
}

func calcKafkaTopicTps(response_begin *sarama.OffsetResponse, response_end *sarama.OffsetResponse, intervalMs int64, topics *map[string]string) (err error) {
	//PrintReflect(*response_end)
	//PrintReflect(response_begin)
	for topic, tb := range response_end.Blocks {
		//totalTps := float64(0.00)
		for partition, pb := range tb {
			if pb.Err == 0 {
				//fmt.Println(topic,partition,"start",pb.Offset,"end",response_begin.Blocks[topic][partition].Offset)
				//fmt.Printf("%#v\n",pb)
				tps := float64(0.00)
				if pb.Offset > 0 {
					tps = float64(pb.Offset - response_begin.Blocks[topic][partition].Offset)
					tps = tps / float64(intervalMs) * 1000
				}
				//totalTps += tps
				(*topics)[topic] += fmt.Sprintf("(tps%d:%.2f)", partition, tps)
			} else {
				(*topics)[topic] += fmt.Sprintf("(tps%d:err%d)", partition, pb.Err)
			}
		}
		//(*topics)[topic] += fmt.Sprintf("(tpstotal:%.2f)", totalTps)
	}
	return nil
}

func GetKafkaTopicTps(broker *sarama.Broker, topics *map[string]string) (err error) {
	var request sarama.MetadataRequest
	request.AllowAutoTopicCreation = false
	for topic, _ := range *topics {
		request.Topics = append(request.Topics, topic)
	}
	//if no topic,it return all
	response, err := broker.GetMetadata(&request)
	if err != nil {
		return errors.New(err.Error() + "[request kafka GetMetadata fail]")
	}

	var intervalMs int64 = 5000
	var agoSecond int64 = 5
	ms := (time.Now().Unix() - agoSecond) * 1000
	for _, b := range response.Brokers {
		if err = OpenBroker(b); err != nil {
			return errors.New(err.Error() + "[OpenBroker fail]")
		}
		defer b.Close()
		response_end, err := getKafkaTopicOffsetResponse(b, response, ms)
		if err != nil {
			return errors.New(err.Error() + "[getKafkaTopicOffsetResponse end fail]")
		}
		response_begin, err := getKafkaTopicOffsetResponse(b, response, ms-intervalMs)
		if err != nil {
			return errors.New(err.Error() + "[getKafkaTopicOffsetResponse begin fail]")
		}
		calcKafkaTopicTps(response_begin, response_end, intervalMs, topics)
	}
	return nil
}

func GetKafkaTopicInfo(broker *sarama.Broker, topics *map[string]string) (err error) {
	err = getTopicRetention(broker, topics)
	if err != nil {
		return errors.New(err.Error() + "[getTopicRetention fail]")
	}
	err = GetKafkaTopicTps(broker, topics)
	if err != nil {
		return errors.New(err.Error() + "[getKafkaTopicTps fail]")
	}
	return nil
}

// caller need to call client.Close()
func getController(brokers string) (controller *sarama.Broker, client sarama.Client,err error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_2_0_0

	client, err = sarama.NewClient(strings.Split(brokers, ","), config)
	if err != nil {
		fmt.Println("NewClient fail", brokers, err)
		return nil, nil,err
	}
	//fmt.Println(config.Version)
	broker, err := client.Controller()
	if err != nil {
		fmt.Println("Controller fail", brokers, err)
		client.Close()
		return nil, nil,err
	}
	return broker, client,nil
}

func GetTopicInfo(brokers string, topics *map[string]string) (err error) {
	broker,client, err := getController(brokers)
	if err != nil {
		fmt.Println("getController fail", brokers, err)
		return err
	}
	defer client.Close()
	err = getTopicRetention(broker, topics)
	if err != nil {
		return errors.New(err.Error() + "[getTopicRetention fail]")
	}
	err = GetKafkaTopicTps(broker, topics)
	if err != nil {
		return errors.New(err.Error() + "[getKafkaTopicTps fail]")
	}
	return nil
}

func CreateTopic(brokers string, topic string) (err error) {
	broker,client, err := getController(brokers)
	if err != nil {
		fmt.Println("getController fail", brokers, err)
		return err
	}
	defer client.Close()
	//request
	request := &sarama.CreateTopicsRequest{
		Timeout: 5 * time.Second,
		TopicDetails: map[string]*sarama.TopicDetail{
			topic: &sarama.TopicDetail{
				NumPartitions:     1,
				ReplicationFactor: 3,
				ReplicaAssignment: nil,
				ConfigEntries:     nil,
			},
		},
	}
	//fmt.Printf("%#v\n", request)
	response, err := broker.CreateTopics(request)
	if err != nil {
		fmt.Println("CreateTopics fail", brokers, err)
		return err
	}
	//fmt.Printf("%#v\n", response)
	for key, value := range response.TopicErrors {
		fmt.Println("topic", key, int16(value.Err), value.Error())
		if value.Err != sarama.ErrNoError && value.Err != sarama.ErrTopicAlreadyExists {
			return errors.New(value.Error())
		}
	}
	return nil
}
