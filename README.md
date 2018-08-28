## KafkaUtils for Sarama library

This is just a simple Go library providing abstraction over Shopify's [Sarama][0] library.

### Usage:

* Install [dep][1] dependencies:

```
dep ensure
```
* Import/Use in your code!

```Go
import github.com/TerrexTech/go-kafkautils/consumer // Import Consumer
import github.com/TerrexTech/go-kafkautils/producer // Import Producer
```

### Docs:

* **[Consumer][2]**
* **[Producer][3]**

### Minimal examples:

#### Consumer:

```Go
config := consumer.Config{
  ConsumerGroup: "test",
  KafkaBrokers:  []string{"localhost:9092"},
  Topics:        []string{"test"},
}

proxyconsumer, err := consumer.New(&config)
if err != nil {
  panic(err)
}
proxyconsumer.EnableLogging()

// Read Errors
go func() {
  for err := proxyConsumer.Errors() {
    log.Println(err)
  }
}()

// Read Messages
go func() {
  for msg := proxyConsumer.Messages() {
    log.Println(msg)
  }
}()
```

#### Producer:

```Go
config := producer.Config{
  KafkaBrokers: []string{"localhost:9092"},
}
asyncProducer, err := producer.New(&config)
if err != nil {
  panic(err)
}
asyncProducer.EnableLogging()

go func() {
  for err := asyncProducer.Errors() {
    log.Println(err)
  }
}()

strTime := strconv.Itoa(int(time.Now().Unix()))
msg := asyncProducer.CreateKeyMessage("test", strTime, "testValue")

input, _ := asyncProducer.Input()
input <- msg // Produce message
```

  [0]: https://github.com/Shopify/sarama
  [1]: https://github.com/golang/dep
  [2]: https://godoc.org/github.com/TerrexTech/go-kafkautils/consumer
  [3]: https://godoc.org/github.com/TerrexTech/go-kafkautils/producer
