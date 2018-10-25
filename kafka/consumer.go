package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// ConsumerConfig wraps configuration for Sarama Consumer-Group.
type ConsumerConfig struct {
	// Name for ConsumerGroup
	GroupName    string
	KafkaBrokers []string
	// Overwrites the default sarama-config
	SaramaConfig *sarama.Config
	Topics       []string
}

// Consumer wraps Sarama Consumer-Group.
type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	topics        []string
}

// NewConsumer returns a configured Sarama Consumer-Group.
func NewConsumer(config *ConsumerConfig) (*Consumer, error) {
	if config.KafkaBrokers == nil || len(config.KafkaBrokers) == 0 {
		err := errors.New("no Kafka Brokers provided for ConsumerGroup")
		return nil, err
	}
	if config.Topics == nil || len(config.Topics) == 0 {
		err := errors.New("no Kafka Topics provided to listen for ConsumerGroup")
		return nil, err
	}
	if config.GroupName == "" {
		err := errors.New("no name was provided for ConsumerGroup")
		return nil, err
	}

	// If Sarama configuration is provied, use that, or use default.
	var saramaConfig *sarama.Config
	if config.SaramaConfig != nil {
		saramaConfig = config.SaramaConfig
	} else {
		saramaConfig = sarama.NewConfig()
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
		saramaConfig.Consumer.MaxProcessingTime = 10 * time.Second
		saramaConfig.Consumer.Return.Errors = true
		saramaConfig.Version = sarama.V2_0_0_0
	}

	// Create Consumer-Group
	consumerGroup, err := sarama.NewConsumerGroup(config.KafkaBrokers, config.GroupName, saramaConfig)

	if err != nil {
		err = errors.Wrap(
			err,
			"Error creating Sarama Consumer-Group: "+config.GroupName,
		)
		return nil, err
	}

	consumer := &Consumer{
		consumerGroup: consumerGroup,
		topics:        config.Topics,
	}
	return consumer, nil
}

// Errors returns error-channel for Consumer-Group.
func (c *Consumer) Errors() <-chan error {
	return c.consumerGroup.Errors()
}

// Consume starts consuming messages using provided Context and ConsumerGroupHandler.
func (c *Consumer) Consume(
	ctx context.Context,
	handler sarama.ConsumerGroupHandler,
) error {
	err := c.consumerGroup.Consume(ctx, c.topics, handler)
	if err != nil {
		err = errors.Wrap(err, "Error getting consumer-channel for ConsumerGroup")
		return err
	}
	return nil
}

// SaramaConsumer returns the wrapper Sarama Consumer-Group.
// Only use this when you really have to.
func (c *Consumer) SaramaConsumer() *sarama.ConsumerGroup {
	return &c.consumerGroup
}

// Close stops the ConsumerGroup and detaches any running sessions. It is required to call
// this function before the object passes out of scope, as it will otherwise leak memory.
func (c *Consumer) Close() error {
	return c.consumerGroup.Close()
}
