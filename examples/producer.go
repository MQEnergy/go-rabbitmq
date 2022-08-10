package main

import (
	"fmt"
	gorabbitmq "github.com/MQEnergy/go-rabbitmq"
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
	mq := gorabbitmq.New(config, "test", "bms", "", 0, 1, false)
	// 需要等待一秒钟
	time.Sleep(time.Second * 1)

	for {
		time.Sleep(time.Second * 1)
		data := []byte("{\"hello\":\"world " + time.Now().Format("2006-01-02 15:04:05") + "\"}")
		if err := mq.Push(data); err != nil {
			panic(err)
		}
		fmt.Println("Push succeeded!", string(data))
	}
}
