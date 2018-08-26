package mocks

import (
	"github.com/pkg/errors"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

// Consumer defines a mock consumer.
// It provides writeable channels to mock various message-types.
type Consumer struct {
	ErrorsChan        chan error
	MessagesChan      chan *sarama.ConsumerMessage
	NotificationsChan chan *cluster.Notification
	PartitionsChan    chan cluster.PartitionConsumer

	// We don't call this var as "isClosed" because similar
	// behavior is also shared by mock-producer, and the names
	// would conflict.
	isConsumerClosed bool
}

// Close mocks the #Close function of sarama-cluster Close.
// This closes all the channels for this mock.
func (c *Consumer) Close() error {
	close(c.ErrorsChan)
	close(c.MessagesChan)
	close(c.NotificationsChan)
	close(c.PartitionsChan)
	c.isConsumerClosed = true
	return nil
}

// CommitOffsets mock is a no-op (currently)
func (c *Consumer) CommitOffsets() error {
	return nil
}

// Errors mocks the #Errors function of sarama-cluster Consumer.
// Returns a read-only error channel. The errors can
// be mocked by writing to ErrorsChan var of this struct.
func (c *Consumer) Errors() <-chan error {
	return (<-chan error)(c.ErrorsChan)
}

// HighWaterMarks mock is a no-op (currently)
func (c *Consumer) HighWaterMarks() map[string]map[int32]int64 {
	return nil
}

// MarkOffset mock is a no-op (currently)
func (c *Consumer) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {}

// MarkOffsets mock is a no-op (currently)
func (c *Consumer) MarkOffsets(s *cluster.OffsetStash) {}

// MarkPartitionOffset mock is a no-op (currently)
func (c *Consumer) MarkPartitionOffset(topic string, partition int32, offset int64, metadata string) {}

// Messages mocks the #Messages function of sarama-cluster Consumer.
// Returns a read-only messages channel. The messages can
// be mocked by writing to MessagesChan var of this struct.
func (c *Consumer) Messages() <-chan *sarama.ConsumerMessage {
	return (<-chan *sarama.ConsumerMessage)(c.MessagesChan)
}

// Notifications mocks the #Messages function of sarama-cluster Consumer.
// Returns a read-only notifications channel. The notifications can
// be mocked by writing to NotificationsChan var of this struct.
func (c *Consumer) Notifications() <-chan *cluster.Notification {
	return (<-chan *cluster.Notification)(c.NotificationsChan)
}

// Partitions mocks the #Partitions function of sarama-cluster Consumer.
// Returns a read-only partitions channel. The messages can
// be mocked by writing to PartitionsChan var of this struct.
func (c *Consumer) Partitions() <-chan cluster.PartitionConsumer {
	return (<-chan cluster.PartitionConsumer)(c.PartitionsChan)
}

// ResetOffset mock is a no-op (currently)
func (c *Consumer) ResetOffset(msg *sarama.ConsumerMessage, metadata string) {}

// ResetOffsets mock is a no-op (currently)
func (c *Consumer) ResetOffsets(s *cluster.OffsetStash) {}

// ResetPartitionOffset mock is a no-op (currently)
func (c *Consumer) ResetPartitionOffset(topic string, partition int32, offset int64, metadata string) {
}

// Subscriptions mock is a no-op (currently)
func (c *Consumer) Subscriptions() map[string][]int32 {
	return nil
}

// IsClosed returns a bool specifying if the mock consumer is closed
func (c *Consumer) IsClosed() bool {
	return c.isConsumerClosed
}

// CreateMockError creates a mock consumer error
func (c *Consumer) CreateMockError() error {
	return errors.New("Mock Consumer connection-error")
}

// CreateMockMessage creates a mock consumer error
func (c *Consumer) CreateMockMessage(topic string, key string, value string) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{
		Key:   []byte(key),
		Value: []byte(value),
		Topic: topic,
	}
}

// CreateMockNotification creates a mock consumer error
func (c *Consumer) CreateMockNotification() *cluster.Notification {
	return &cluster.Notification{
		Type: 1,
	}
}
