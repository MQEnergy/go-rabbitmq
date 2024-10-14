package go_rabbitmq

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"net/url"
	"time"
)

type (
	RabbitMQ struct {
		conn            *amqp.Connection
		channel         *amqp.Channel
		notifyConnClose chan *amqp.Error
		notifyChanClose chan *amqp.Error
		notifyConfirm   chan amqp.Confirmation
		QueueName       string // 队列名称
		Exchange        string // 交换机 可为空
		RouteKey        string // 路由键 可为空
		Addr            string // 连接地址
		Type            string // 交换机连接方式 direct topic fanout headers 可为空
		Done            chan bool
		isReady         bool
		PrefetchCount   int  // 消费者消费数据限流数
		Durable         bool // 是否queue队列持久化
	}

	// Config amqp配置
	Config struct {
		User     string
		Password string
		Host     string
		Port     string
		Vhost    string
	}
)

const (
	// 连接失败后重新连接服务器时间间隔
	reconnectDelay = 5 * time.Second

	// 建立通道时出现通道异常时间间隔
	reInitDelay = 2 * time.Second

	// 重新发送消息时，服务器没有确认时间间隔
	resendDelay = 5 * time.Second
)

var (
	// 交换机连接方式
	exchangeTypeList = []string{"topic", "direct", "fanout", "headers"}

	errNotConnected  = errors.New("not connected to a server")
	errAlreadyClosed = errors.New("already closed: not connected to the server")
	errShutdown      = errors.New("session is shutting down")
	errFailedToPush  = errors.New("failed to push: not connected")
	err              error
)

// New 创建一个新的消费者状态实例，并自动尝试连接到服务器
func New(config *Config, queueName, exchange, routeKey string, exchangeType, prefetchCount int, durable bool) (*RabbitMQ, error) {
	// amqp 出现url.Parse导致的错误 是因为特殊字符需要进行urlencode编码
	password := url.QueryEscape(config.Password)
	// amqp://账号:密码@rabbitmq服务器地址:端口号/vhost
	addr := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", config.User, password, config.Host, config.Port, config.Vhost)
	var _type string
	if exchangeType < len(exchangeTypeList) {
		_type = exchangeTypeList[exchangeType]
	} else {
		_type = "topic"
	}
	if routeKey == "" {
		if exchange == "" {
			routeKey = queueName
		} else {
			routeKey = exchange + "." + queueName
		}
	}
	if prefetchCount == 0 {
		prefetchCount = 1
	}
	rabbitmq := &RabbitMQ{
		QueueName:     queueName,
		Exchange:      exchange,
		Type:          _type,
		RouteKey:      routeKey,
		Addr:          addr,
		Done:          make(chan bool),
		PrefetchCount: prefetchCount,
		Durable:       durable,
	}
	rabbitmq.conn, err = rabbitmq.connect(addr)
	if err != nil {
		return nil, err
	}
	if err := rabbitmq.init(rabbitmq.conn); err != nil {
		return nil, err
	}
	go rabbitmq.handleReconnect(rabbitmq.Addr)
	return rabbitmq, nil
}

// handleReconnect 将在notifyConnClose上等待连接错误，然后不断尝试重新连接。
func (m *RabbitMQ) handleReconnect(addr string) {
	for {
		m.isReady = false
		// 企图连接
		conn, err := m.connect(addr)
		if err != nil {
			//	连接失败 尝试重连
			select {
			case <-m.Done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}
		if done := m.handleReInit(conn); done {
			break
		}
	}
}

// connect 创建一个新的AMQP连接
func (m *RabbitMQ) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}
	m.changeConnection(conn)
	// 连接成功
	return conn, nil
}

// handleReInit 等待一个通道错误，然后不断尝试重新初始化两个通道
func (m *RabbitMQ) handleReInit(conn *amqp.Connection) bool {
	for {
		m.isReady = false
		if err := m.init(conn); err != nil {
			// 初始化channel失败 重试
			select {
			case <-m.Done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}
		select {
		case <-m.Done:
			return true
		case <-m.notifyConnClose:
			// 连接关闭 重新连接
			return false
		case <-m.notifyChanClose:
			//	channel关闭重新init
		}
	}
}

// init 将初始化通道并声明队列 如果传输交换机就声明
func (m *RabbitMQ) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	err = ch.Confirm(false)
	if err != nil {
		return err
	}
	_, err = ch.QueueDeclare(
		m.QueueName,
		// 是否持久化 队列是否持久化.false:队列在内存中,服务器挂掉后,队列就没了;true:服务器重启后,队列将会重新生成.注意:只是队列持久化,不代表队列中的消息持久化!!!!
		m.Durable,
		// 是否为自动删除
		false,
		// 是否具有排他性
		false,
		// 是否阻塞
		false,
		// 额外属性
		nil)
	if err != nil {
		return err
	}

	if m.Exchange != "" {
		// 如果没有exchange就创建
		if err := ch.ExchangeDeclare(
			m.Exchange,
			m.Type,
			// 是否持久化
			true,
			// 是否为自动删除
			false,
			// 是否具有排他性
			false,
			// 是否阻塞
			false,
			// 额外属性
			nil); err != nil {
			return err
		}
		// 绑定Queue
		if err := ch.QueueBind(m.QueueName, m.RouteKey, m.Exchange, false, nil); err != nil {
			return err
		}
	}
	m.changeChannel(ch)
	m.isReady = true
	return nil
}

// changeConnection 接受一个到队列的新连接， 并更新关闭监听器
func (m *RabbitMQ) changeConnection(conn *amqp.Connection) {
	m.conn = conn
	m.notifyConnClose = make(chan *amqp.Error)
	m.conn.NotifyClose(m.notifyConnClose)
}

// changeChannel 获取到队列的新通道，并更新通道监听器
func (m *RabbitMQ) changeChannel(ch *amqp.Channel) {
	m.channel = ch
	m.notifyChanClose = make(chan *amqp.Error)
	m.notifyConfirm = make(chan amqp.Confirmation, 1)
	m.channel.NotifyClose(m.notifyChanClose)
	m.channel.NotifyPublish(m.notifyConfirm)
}

// Push 将数据推送到队列中，并等待确认。
// 如果在resendTimeout时间内没有收到确认信息，
// 它不断地重新发送消息，直到收到一个确认。
// 直到服务器发送确认信息。错误是只在推送操作本身失败时返回，参见UnsafePush。
func (m *RabbitMQ) Push(data []byte) error {
	if m.isReady == false {
		return errFailedToPush
	}
	for {
		if err := m.UnsafePush(data); err != nil {
			// 推送失败 重试
			select {
			case <-m.Done:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		select {
		case confirm := <-m.notifyConfirm:
			if confirm.Ack {
				// 推送确认
				return nil
			}
		case <-time.After(resendDelay):
		}
		// 推送未被确认，重试
	}
}

// UnsafePush 将push到队列而不检查确认。如果连接失败，则返回错误。
// 没有提供服务器是否会接收消息。
func (m *RabbitMQ) UnsafePush(data []byte) error {
	if m.isReady == false {
		return errNotConnected
	}
	return m.channel.Publish(
		m.Exchange,
		m.RouteKey,
		// 如果为true, 会根据exchange类型和routkey规则，如果无法找到符合条件的队列那么会把发送的消息返回给发送者
		false,
		// 如果为true, 当exchange发送消息到队列后发现队列上没有绑定消费者，则会把消息发还给发送者
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		})
}

// Consume 将不断地将队列项放到通道上。
// 需要调用delivery.Ack 当它已经成功处理，或当它失败时，调用 delivery.Nack。
// 忽略这个参数会导致数据在服务器上堆积。
func (m *RabbitMQ) Consume() (<-chan amqp.Delivery, error) {
	// Set our quality of service.  Since we're sharing 3 consumers on the same
	// channel, we want at least 3 messages in flight.
	if err := m.channel.Qos(m.PrefetchCount, 0, false); err != nil {
		return nil, err
	}

	return m.channel.Consume(
		m.QueueName,
		// 用来区分多个消费者
		"",
		// 是否自动应答
		false,
		// 是否具有排他性
		false,
		// 如果设置为true，表示不能将同一个connection中发送的消息传递给这个connection中的消费者
		false,
		// 队列消费是否阻塞
		false,
		nil,
	)
}

// Close 关闭通道和连接。
func (m *RabbitMQ) Close() error {
	if m.isReady == false {
		return errAlreadyClosed
	}
	if err := m.channel.Close(); err != nil {
		return err
	}
	if err := m.conn.Close(); err != nil {
		return err
	}
	close(m.Done)
	m.isReady = false
	return nil
}
