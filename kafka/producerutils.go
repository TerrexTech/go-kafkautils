package kafka

import "github.com/Shopify/sarama"

// CreateKeyMessage creates producer-formatted message with key.
func CreateKeyMessage(topic string, key string, value []byte) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}

	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	return msg
}

// CreateMessage creates keyless producer-formatted message.
func CreateMessage(topic string, value []byte) *sarama.ProducerMessage {
	return CreateKeyMessage(topic, "", value)
}
