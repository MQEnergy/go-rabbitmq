package main

import (
	"fmt"
	go_rabbitmq "github.com/MQEnergy/go-rabbitmq"
	"sync"
	"time"
)

func main() {
	config := &go_rabbitmq.Config{
		User:     "root",
		Password: "",
		Host:     "",
		Port:     "5672",
		Vhost:    "",
	}
	mq := go_rabbitmq.New(config, "test", "", "", 0, 1)
	time.Sleep(time.Second * 1)
	amqphandler(mq, 3)
}

// amqphandler 消息队列处理
func amqphandler(mq *go_rabbitmq.RabbitMQ, consumerNum int) error {
	var wg sync.WaitGroup
	cherrors := make(chan error)
	wg.Add(consumerNum)
	for i := 0; i < consumerNum; i++ {
		fmt.Printf("正在开启消费者：第 %d 个\n", i+1)
		go func() {
			defer wg.Done()
			deliveries, err := mq.Consume()
			if err != nil {
				cherrors <- err
			}
			for d := range deliveries {
				// 消费者逻辑 to do
				fmt.Printf("got %dbyte delivery: [%v] %s %q\n", len(d.Body), d.DeliveryTag, d.Exchange, d.Body)
				d.Ack(false)
			}
		}()
	}
	select {
	case err := <-cherrors:
		close(cherrors)
		fmt.Printf("Consumer failed: %s\n", err)
		return err
	}
	wg.Wait()
	return nil
}
