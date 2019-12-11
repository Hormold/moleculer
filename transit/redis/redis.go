package redis

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

const (
	DurationNotDefined = time.Duration(-1)
)

type safeHandler func(moleculer.Payload) error

type binding struct {
	queueName string
	topic     string
	pattern   string
}

type subscriber struct {
	command string
	nodeID  string
	handler transit.TransportHandler
}

var DefaultConfig = RedisOptions{
	Prefetch: 1,

	AutoDeleteQueues:    DurationNotDefined,
	EventTimeToLive:     DurationNotDefined,
	HeartbeatTimeToLive: DurationNotDefined,
}

type RedisTransporter struct {
	opts          *RedisOptions
	prefix        string
	logger        *log.Entry
	serializer    serializer.Serializer
	pool          *redis.Pool
	connectionPub redis.Conn
	connectionSub redis.Conn
	channel       redis.PubSubConn

	connectionDisconnecting bool
	connectionRecovering    bool

	nodeID      string
	subscribers []subscriber
	bindings    []binding
}

type RedisOptions struct {
	Url string

	Logger     *log.Entry
	Serializer serializer.Serializer

	DisableReconnect    bool
	AutoDeleteQueues    time.Duration
	EventTimeToLive     time.Duration
	HeartbeatTimeToLive time.Duration
	Prefetch            int
}

func mergeConfigs(baseConfig RedisOptions, userConfig RedisOptions) RedisOptions {
	// Number of requests a broker will handle concurrently
	if userConfig.Prefetch != 0 {
		baseConfig.Prefetch = userConfig.Prefetch
	}

	if userConfig.AutoDeleteQueues != 0 {
		baseConfig.AutoDeleteQueues = userConfig.AutoDeleteQueues
	}

	baseConfig.DisableReconnect = userConfig.DisableReconnect

	// Support for multiple URLs (clusters)
	if len(userConfig.Url) != 0 {
		baseConfig.Url = userConfig.Url
	}

	if userConfig.Logger != nil {
		baseConfig.Logger = userConfig.Logger
	}

	return baseConfig
}

func CreateRedisTransporter(options RedisOptions) transit.Transport {
	options = mergeConfigs(DefaultConfig, options)

	return &RedisTransporter{
		opts:   &options,
		logger: options.Logger,
	}
}

func (t *RedisTransporter) Connect() chan error {
	endChan := make(chan error)

	go func() {
		t.logger.Debug("Redis Connect() - url: ", t.opts.Url)

		isConnected := false
		connectAttempt := 0

		for {
			connectAttempt++

			err := t.doConnect(t.opts.Url)
			if err != nil {
				t.logger.Error("Redis Connect() - Error: ", err, " url: ", t.opts.Url)
			} else if !isConnected {
				isConnected = true
				endChan <- nil
			} else {
				// recovery subscribers
				for _, subscriber := range t.subscribers {
					t.subscribeInternal(subscriber)
				}

				t.connectionRecovering = false
			}

			if t.opts.DisableReconnect {
				return
			}

			t.connectionRecovering = true

			time.Sleep(5 * time.Second)
		}
	}()
	return endChan
}

func (t *RedisTransporter) doConnect(uri string) error {
	//var redispool *redis.Pool
	t.pool = &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", uri)
		},
	}
	t.connectionPub = t.pool.Get()
	t.connectionSub = t.pool.Get()

	_, err := t.connectionPub.Do("PING")
	if err != nil {
		return errors.Wrap(err, "Redis Pub failed to connect")
	}

	_, err = t.connectionSub.Do("PING")
	if err != nil {
		return errors.Wrap(err, "Redis Sub failed to connect")
	}

	t.channel = redis.PubSubConn{Conn: t.connectionSub}

	go func() {
		for {
			switch v := t.channel.Receive().(type) {
			case redis.PMessage:
				payload := t.serializer.BytesToPayload(&v.Data)
				t.onMessage(payload)
			}
		}
	}()

	t.logger.Info("Redis is connected")

	return nil
}

func (t *RedisTransporter) onMessage(payload moleculer.Payload) {
	//t.logger.Debug(fmt.Sprintf("Incoming %s packet from '%s'", topic, payload.Get("sender").String()))
	fmt.Println(payload)
	for _, subscriber := range t.subscribers {
		fmt.Println(subscriber)
	}
}

func (t *RedisTransporter) Disconnect() chan error {
	errChan := make(chan error)

	t.connectionDisconnecting = true

	go func() {
		if t.connectionPub != nil {

			if err := t.connectionPub.Close(); err != nil {
				t.logger.Error("Redis Pub Disconnect() - Connection close error: ", err)
				errChan <- err
				return
			}

			t.connectionPub = nil
		}

		if t.connectionSub != nil {

			t.subscribers = []subscriber{}
			t.bindings = []binding{}
			t.connectionDisconnecting = true

			if err := t.connectionSub.Close(); err != nil {
				t.logger.Error("Redis Sub Disconnect() - Connection close error: ", err)
				errChan <- err
				return
			}

			t.connectionSub = nil
		}

		errChan <- nil
	}()

	return errChan
}

func (t *RedisTransporter) Subscribe(command, nodeID string, handler transit.TransportHandler) {
	if t.connectionSub == nil {
		msg := fmt.Sprint("Redis.Subscribe() No connection :( -> command: ", command, " nodeID: ", nodeID)
		t.logger.Warn(msg)
		panic(errors.New(msg))
	}

	subscriber := subscriber{command, nodeID, handler}

	// Save subscribers for recovery logic
	t.subscribers = append(t.subscribers, subscriber)

	t.subscribeInternal(subscriber)
}

func (t *RedisTransporter) subscribeInternal(subscriber subscriber) {
	if t.connectionSub == nil {
		msg := fmt.Sprint("Redis.Subscribe() No connection :( -> command: ", subscriber.command, " nodeID: ", subscriber.nodeID)
		t.logger.Warn(msg)
		panic(errors.New(msg))
	}
	topic := t.topicName(subscriber.command, subscriber.nodeID)

	if err := t.channel.PSubscribe(topic); err != nil {
		msg := fmt.Sprint("Redis.Subscribe() Error on subscribe :( -> command: ", subscriber.command, " nodeID: ", subscriber.nodeID)
		t.logger.Warn(msg)
		panic(errors.New(msg))
	}
}

func (t *RedisTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	if t.connectionPub == nil {
		msg := fmt.Sprint("redis.Publish() No connection :( -> command: ", command, " nodeID: ", nodeID)
		t.logger.Warn(msg)
		panic(errors.New(msg))
	}
	topic := t.topicName(command, nodeID)
	t.logger.Debug("redis.Publish() command: ", command, " topic: ", topic, " nodeID: ", nodeID)
	t.logger.Trace("message: \n", message, "\n - end")
	_, err := t.connectionPub.Do("PUBLISH", topic, t.serializer.PayloadToBytes(message))
	if err != nil {
		t.logger.Error("Error on publish: error: ", err, " command: ", command, " topic: ", topic)
		panic(err)
	}
}

func (t *RedisTransporter) waitForRecovering() {
	for {
		if !t.connectionRecovering {
			return
		}

		time.Sleep(time.Second)
	}
}

func (t *RedisTransporter) SetPrefix(prefix string) {
	t.prefix = prefix
}

func (t *RedisTransporter) SetNodeID(nodeID string) {
	t.nodeID = nodeID
}

func (t *RedisTransporter) SetSerializer(serializer serializer.Serializer) {
	t.serializer = serializer
}

func (t *RedisTransporter) topicName(command string, nodeID string) string {
	parts := []string{t.prefix, command}
	if nodeID != "" {
		parts = append(parts, nodeID)
	}
	return strings.Join(parts, ".")
}
