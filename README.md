## KafkaUtils for Sarama library

This is just a simple Go library providing convenience-wrappers over Shopify's [Sarama][0] library.

### Usage:

* Install [dep][1] dependencies:

```
dep ensure
```

* Import/Use in your code!

```Go
import github.com/TerrexTech/go-kafkautils/kafka
```

* **[Go Docs][2]**

* Check **[tests][3]** for examples.

### Minimal examples:

#### Consumer Group (referred to as just *Consumer* in library):

```Go
// Handler for Consumer messages
type msgHandler struct {}

func (mh msgHandler) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (mh msgHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (mh msgHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		v := string(msg.Value)
		log.Println(v)
  }
  return nil
}

func main() {
  config := &kafka.ConsumerConfig{
    ConsumerGroup: "test",
    KafkaBrokers:  []string{"localhost:9092"},
    Topics:        []string{"test"},
  }
  consumer, err := kafka.NewConsumer(config)
  if err != nil {
    panic(err)
  }

  // Read Errors
  go func() {
    for err := proxyConsumer.Errors() {
      log.Println(err)
    }
  }()

  // Read Messages
  for msg := proxyConsumer.Messages() {
    log.Println(msg)
    // ...More Operations
  }

  // Don't forget to close when done
  consumer.Close()
}
```

#### Producer:

```Go
config := &producer.Config{
  KafkaBrokers: []string{"localhost:9092"},
}
producer, err := kafka.NewProducer(config)
if err != nil {
  panic(err)
}

go func() {
  for err := asyncProducer.Errors() {
    log.Println(err)
  }
}()

// Create message
strTime := strconv.Itoa(int(time.Now().Unix()))
msg := producer.CreateKeyMessage("testTopic", strTime, []byte("testValue"))

// Produce message
asyncProducer.Input() <- msg
```

  [0]: https://github.com/Shopify/sarama
  [1]: https://github.com/golang/dep
  [2]: https://godoc.org/github.com/TerrexTech/go-kafkautils/kafka
  [3]: https://github.com/TerrexTech/go-kafkautils/blob/master/test/kafka_suite_test.go
