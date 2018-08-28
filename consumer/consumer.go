package consumer

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"
)

// Adapter is the Kafka-Consumer interface
type Adapter interface {
	Close() error
	CommitOffsets() error
	Errors() <-chan error
	HighWaterMarks() map[string]map[int32]int64
	MarkOffset(msg *sarama.ConsumerMessage, metadata string)
	MarkOffsets(s *cluster.OffsetStash)
	MarkPartitionOffset(topic string, partition int32, offset int64, metadata string)
	Messages() <-chan *sarama.ConsumerMessage
	Notifications() <-chan *cluster.Notification
	Partitions() <-chan cluster.PartitionConsumer
	ResetOffset(msg *sarama.ConsumerMessage, metadata string)
	ResetOffsets(s *cluster.OffsetStash)
	ResetPartitionOffset(topic string, partition int32, offset int64, metadata string)
	Subscriptions() map[string][]int32
}

// Config wraps configuration for consumer
type Config struct {
	ConsumerGroup string
	KafkaBrokers  []string
	// Allow overwriting default sarama-config
	SaramaConfig *cluster.Config
	Topics       []string
}

// Consumer wraps sarama-cluster's consumer
type Consumer struct {
	consumer         Adapter
	isClosed         bool
	isLoggingEnabled bool
	closeChan        chan bool
}

// To facilitate testing. This var gets overwritten by custom init function.
// We don't pass the init function as argument or via dependency-injection
// because the purpose of this library is to abstract the kafka configs.
var initFunc func([]string, string, []string, *cluster.Config) (*cluster.Consumer, error)

func init() {
	initFunc = cluster.NewConsumer
}

// New returns a configured Sarama Kafka-Consumer instance
func New(initConfig *Config) (*Consumer, error) {
	if initConfig.KafkaBrokers == nil || len(initConfig.KafkaBrokers) == 0 {
		errorLogMsg := errors.New("error creating new ConsumerGroup, no Kafka Brokers set")
		return nil, errorLogMsg
	}

	var config *cluster.Config
	if initConfig.SaramaConfig != nil {
		config = initConfig.SaramaConfig
	} else {
		config = cluster.NewConfig()
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
		config.Consumer.MaxProcessingTime = 10 * time.Second
		config.Consumer.Return.Errors = true
	}

	consumer, err := initFunc(initConfig.KafkaBrokers, initConfig.ConsumerGroup, initConfig.Topics, config)

	if err != nil {
		errorLogMsg := errors.Wrap(err, "Failed to join consumer group: "+initConfig.ConsumerGroup)
		return nil, errorLogMsg
	}

	proxyConsumer := Consumer{
		consumer:         consumer,
		isClosed:         false,
		isLoggingEnabled: false,
	}

	// Don't run this function when mocking consumer, where
	// initial consumer is nil.
	// This initialization is controlled by mock consumer.
	if consumer != nil {
		proxyConsumer.handleKeyInterrupt()
	}
	return &proxyConsumer, nil
}

// EnableLogging logs events to console
func (c *Consumer) EnableLogging() {
	c.isLoggingEnabled = true
}

// IsClosed returns a bool specifying if Kafka consumer is closed
func (c *Consumer) IsClosed() bool {
	return c.isClosed
}

// SaramaConsumerGroup returns the original Sarama Kafka consumer-group
func (c *Consumer) SaramaConsumerGroup() Adapter {
	return c.consumer
}

func (c *Consumer) handleKeyInterrupt() {
	// Capture the Ctrl+C signal (interrupt or kill)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	// Elegant exit
	go func() {
		<-sigChan
		log.Println("Keyboard-Interrupt signal received, cleaning up before closing")
		closeError := <-c.Close()
		log.Println(closeError)
	}()
}

// Errors returns the error-channel for Consumer.
func (c *Consumer) Errors() <-chan error {
	return c.consumer.Errors()
}

// Messages returns the messages-channel for Consumer.
func (c *Consumer) Messages() <-chan *sarama.ConsumerMessage {
	return c.consumer.Messages()
}

// Close attempts to close the consumer,
// and returns any occurring errors over channel
func (c *Consumer) Close() chan error {
	if c.IsClosed() {
		return nil
	}

	closeErrorChan := make(chan error, 1)
	go func() {
		err := c.SaramaConsumerGroup().Close()
		if err != nil {
			if c.isLoggingEnabled {
				log.Fatal("Error closing consumer.", err)
			}
			closeErrorChan <- err
		}
		if c.isLoggingEnabled {
			log.Println("Consumer closed.")
		}
		c.isClosed = true
	}()

	return closeErrorChan
}
