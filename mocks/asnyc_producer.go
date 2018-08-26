package mocks

import (
	"github.com/pkg/errors"

	"github.com/Shopify/sarama"
)

// AsyncProducer defines a mock async-producer.
// It provides writeable channels to mock various message-types.
type AsyncProducer struct {
	ErrorsChan    chan *sarama.ProducerError
	InputChan     chan *sarama.ProducerMessage
	SuccessesChan chan *sarama.ProducerMessage

	// We don't call this var as "isClosed" because similar
	// behavior is also shared by mock-consumer, and the names
	// would conflict.
	isProducerClosed bool
}

// AsyncClose mocks the #AsyncClose function of sarama AsyncProducer
func (a *AsyncProducer) AsyncClose() {
	a.Close()
}

// Close mocks the #Close function of sarama AsyncProducer.
// This closes all the channels for this mock.
func (a *AsyncProducer) Close() error {
	close(a.ErrorsChan)
	close(a.InputChan)
	close(a.SuccessesChan)
	a.isProducerClosed = true
	return nil
}

// Errors mocks the #Errors function of sarama AsyncProducer.
// Returns a read-only error channel. The errors can
// be mocked by writing to ErrorsChan var of this struct.
func (a *AsyncProducer) Errors() <-chan *sarama.ProducerError {
	return (<-chan *sarama.ProducerError)(a.ErrorsChan)
}

// Input mocks the #Input function of sarama AsyncProducer.
// Returns a write-only input channel. The inputs can
// be mocked by reading from InputChan var of this struct.
func (a *AsyncProducer) Input() chan<- *sarama.ProducerMessage {
	return (chan<- *sarama.ProducerMessage)(a.InputChan)
}

// Successes mocks the #Successes function of sarama AsyncProducer.
// Returns a read-only successes channel. The successes can
// be mocked by writing to SuccessesChan var of this struct.
func (a *AsyncProducer) Successes() <-chan *sarama.ProducerMessage {
	return (<-chan *sarama.ProducerMessage)(a.SuccessesChan)
}

// CreateMockMessage creates a mock producer message
func (a *AsyncProducer) CreateMockMessage(
	topic string,
	key string,
	value string,
) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}
}

// CreateMockError creates a mock producer error
func (a *AsyncProducer) CreateMockError(
	topic string,
	key string,
	value string,
) *sarama.ProducerError {
	return &sarama.ProducerError{
		Msg: a.CreateMockMessage(topic, key, value),
		Err: errors.New("Some mock producer-error"),
	}
}

// IsClosed returns a bool specifying if the mock producer is closed
func (a *AsyncProducer) IsClosed() bool {
	return a.isProducerClosed
}
