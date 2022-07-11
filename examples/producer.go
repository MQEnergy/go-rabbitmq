package main

import (
	"fmt"
	go_rabbitmq "github.com/MQEnergy/go-rabbitmq"
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
	// 需要等待一秒钟
	time.Sleep(time.Second * 1)

	data := []byte("{\"hello\":\"world " + time.Now().Format("2006-01-02 15:04:05") + "\"}")
	if err := mq.Push(data); err != nil {
		panic(err)
	}
	fmt.Println("Push succeeded!", string(data))
}
