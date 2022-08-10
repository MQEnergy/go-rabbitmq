package main

import (
	"fmt"
	gorabbitmq "github.com/MQEnergy/go-rabbitmq"
	"sync"
	"time"
)

func main() {
	config := &gorabbitmq.Config{
		User:     "guest",
		Password: "guest",
		Host:     "127.0.0.1",
		Port:     "5672",
		Vhost:    "",
	}
	// 注意 队列是否持久化.false:队列在内存中,服务器挂掉后,队列就没了;true:服务器重启后,队列将会重新生成.注意:只是队列持久化,不代表队列中的消息持久化!!!!
	// 已存在的队列 查看 Features参数是否为持久化（D），不存在的队列按需设置是否持久化
	mq := gorabbitmq.New(config, "test", "", "", 0, 10, false)
	time.Sleep(time.Second * 1)
	amqphandler(mq, 3)
}

// amqphandler 消息队列处理
func amqphandler(mq *gorabbitmq.RabbitMQ, consumerNum int) error {
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
