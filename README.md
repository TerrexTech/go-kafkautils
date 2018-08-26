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
msgHandler := func(msg *sarama.ConsumerMessage, c *consumer.Consumer) {
  // Convert from []byte to string
  println("Received message: ", string(msg.Value))
  consumer := c.Get()
  if !c.IsClosed() {
    consumer.MarkOffset(msg, "")
  } else {
    log.Fatalln("Consumer was closed before offsets could be marked.")
  }
}

errHandler := func(e *error) {
  log.Fatalln((*e).Error())
}

config := consumer.Config{
  ConsumerGroup: "test",
  ErrHandler:    errHandler,
  KafkaBrokers:  []string{"localhost:9092"},
  MsgHandler:    msgHandler,
  Topics:        []string{"test"},
}

proxyconsumer, _ := consumer.New(&config)
proxyconsumer.EnableLogging()

// Temporary hack for simplicity. Use channels/app-logic in actual application.
time.Sleep(100000 * time.Millisecond)
```

#### Producer:

```Go
errHandler := func(err *sarama.ProducerError) {
  errs := *err
  fmt.Println(errs.Error())
}
config := producer.Config{
  ErrHandler:   errHandler,
  KafkaBrokers: []string{"localhost:9092"},
}
asyncProducer, err := producer.New(&config)
asyncProducer.EnableLogging()

strTime := strconv.Itoa(int(time.Now().Unix()))
msg := asyncProducer.CreateKeyMessage("test", strTime, "testValue")

if err != nil {
  panic(err)
}
input, _ := asyncProducer.Input()
input <- msg

// Temporary hack for simplicity. Use channels/app-logic in actual application.
time.Sleep(2000 * time.Millisecond)
```

  [0]: https://github.com/Shopify/sarama
  [1]: https://github.com/golang/dep
  [2]: https://godoc.org/github.com/TerrexTech/go-kafkautils/consumer
  [3]: https://godoc.org/github.com/TerrexTech/go-kafkautils/producer
