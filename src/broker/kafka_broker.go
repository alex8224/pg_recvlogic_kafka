package broker

import (
	"C"
	"context"
	"fmt"
	"log"
	"net/url"

	"github.com/Shopify/sarama"
)

var (
	producer sarama.SyncProducer
	topic    string
)

//create a new kafka connection
func NewKafkaConnection(brokerUrl string) {
	burl, err := url.Parse(brokerUrl)
	if err != nil {
		log.Fatalf("no valid url %s\n", brokerUrl)
	}

	if burl.Scheme != "kafka" {
		log.Fatal("schema not correct")
	}

	topic = burl.Query().Get("topic")
	if topic == "" {
		log.Fatalf("topic not pass!!!!")
	}
	fmt.Printf("topic is %s\n", topic)
	if producer == nil {
		tmpprod, err := sarama.NewSyncProducer([]string{burl.Host}, nil)
		if err != nil {
			log.Fatalf("connect to kafka failed, %s\n", err)
		}
		producer = tmpprod
		fmt.Printf("connect to %s\n", brokerUrl)
	}
}

func sendMess(msg []byte) {
	part, off, err := producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(msg)})
	if err != nil {
		fmt.Printf("%s, msg size is :%d\n", err, len(msg))
		return
	}
	fmt.Println(part, off)
}

//pass wal msg to kafka
func SendBytesTo(ctx context.Context, buff chan []byte) {
	if producer == nil {
		fmt.Println("no broker connection...")
		return
	}
	for {
		select {
		case rowbytes := <-buff:
			sendMess(rowbytes)
		case <-ctx.Done():
			fmt.Println("broker stoped!!!", ctx.Err())
			return
		}
	}
}
