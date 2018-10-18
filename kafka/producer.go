package kafka

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// ProducerConfig wraps configuration for producer
type ProducerConfig struct {
	KafkaBrokers []string
	// Allow overwriting default sarama-config
	SaramaConfig *sarama.Config
}

// Producer wraps sarama's AsyncProducer
type Producer struct {
	sarama.AsyncProducer
}

// NewProducer returns a configured Sarama AsyncProducer.
func NewProducer(config *ProducerConfig) (*Producer, error) {
	if config.KafkaBrokers == nil || len(config.KafkaBrokers) == 0 {
		return nil, errors.New("no Kafka Brokers provided for ProducerGroup")
	}

	// If Sarama configuration is provied, use that, or use default.
	var saramaConfig *sarama.Config
	if config.SaramaConfig != nil {
		saramaConfig = config.SaramaConfig
	} else {
		saramaConfig = sarama.NewConfig()
		saramaConfig.Producer.Return.Errors = true
		saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
		saramaConfig.Producer.Compression = sarama.CompressionNone
		saramaConfig.Version = sarama.V0_11_0_2
	}

	producer, err := sarama.NewAsyncProducer(config.KafkaBrokers, saramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "producer connection error")
	}

	asyncProducer := Producer{producer}
	asyncProducer.handleKeyInterrupt()
	return &asyncProducer, nil
}

func (p *Producer) handleKeyInterrupt() {
	// Capture the Ctrl+C signal (interrupt or kill)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	// Elegant exit
	go func() {
		<-sigChan
		// We always log here, special situation
		log.Println("Keyboard-Interrupt signal received.")
		err := p.Close()
		if err != nil {
			log.Println(err.Error())
		}
	}()
}
