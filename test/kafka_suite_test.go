package test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/joho/godotenv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

// Handler for Consumer Messages
type msgHandler struct {
	valuesMap map[string]*struct{}
}

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

		if mh.valuesMap[v] != nil {
			delete(mh.valuesMap, string(msg.Value))
		}
		if len(mh.valuesMap) == 0 {
			return nil
		}
	}
	return nil
}

// TestKafkaUtils tests the critical KafkaUtils functions.
func TestKafkaUtils(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("./test.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_TEST_GROUP",
		"KAFKA_TEST_TOPIC",
	)
	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "KafkaUtils Suite")
}

var _ = Describe("Consumer", func() {
	var (
		c *kafka.Consumer
		p *kafka.Producer
		// This map is used to check if the values
		// received were correct.
		valuesMap map[string]*struct{}
	)

	BeforeSuite(func() {
		brokersStr := os.Getenv("KAFKA_BROKERS")
		consumerGroup := os.Getenv("KAFKA_CONSUMER_TEST_GROUP")
		testTopic := os.Getenv("KAFKA_TEST_TOPIC")
		brokers := *commonutil.ParseHosts(brokersStr)

		var err error
		c, err = kafka.NewConsumer(&kafka.ConsumerConfig{
			GroupName:    consumerGroup,
			KafkaBrokers: brokers,
			Topics:       []string{testTopic},
		})
		Expect(err).ToNot(HaveOccurred())

		p, err = kafka.NewProducer(&kafka.ProducerConfig{
			KafkaBrokers: brokers,
		})
		Expect(err).ToNot(HaveOccurred())

		// Produce test messages
		valuesMap = make(map[string]*struct{}, 1000)
		for i := 0; i < 1000; i++ {
			v := fmt.Sprintf("valued-%d", i)
			p.Input() <- kafka.CreateMessage(testTopic, []byte(v))

			valuesMap[v] = &struct{}{}
		}
		p.Close()
	})

	Context("produced messages are to be consumed", func() {
		It("should consume messages", func(done Done) {
			go func() {
				for err := range c.Errors() {
					Expect(err).ToNot(HaveOccurred())
				}
			}()
			handler := msgHandler{valuesMap}
			err := c.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			err = c.Close()
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}, 5)
	})

	// ========= Other tests (usage examples can be taken from above tests)

	Context("new producer is requested", func() {
		It("should return the error when initializing new producer fails", func() {
			config := &kafka.ProducerConfig{
				KafkaBrokers: []string{"invalid-broker"},
			}
			_, err := kafka.NewProducer(config)
			Expect(err).To(HaveOccurred())
		})

		It("should return error if Kafka-Brokers are not specified", func() {
			config := &kafka.ProducerConfig{}
			_, err := kafka.NewProducer(config)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("new consumer is requested", func() {
		It("should return error when initializing new consumer fails", func() {
			config := &kafka.ConsumerConfig{
				KafkaBrokers: []string{"invalid-broker"},
			}
			_, err := kafka.NewConsumer(config)
			Expect(err).To(HaveOccurred())
		})

		It("should return error if Kafka-Brokers are not specified", func() {
			config := &kafka.ConsumerConfig{
				GroupName: "test",
				Topics:    []string{"test"},
			}
			_, err := kafka.NewConsumer(config)
			Expect(err).To(HaveOccurred())
		})

		It("should return error if Consumer-Topics are not specified", func() {
			config := &kafka.ConsumerConfig{
				KafkaBrokers: []string{"test"},
				GroupName:    "test",
			}
			_, err := kafka.NewConsumer(config)
			Expect(err).To(HaveOccurred())
		})

		It("should return error if Consumer Group-Name is not specified", func() {
			config := &kafka.ConsumerConfig{
				KafkaBrokers: []string{"test"},
				Topics:       []string{"test"},
			}
			_, err := kafka.NewConsumer(config)
			Expect(err).To(HaveOccurred())
		})

		It("should return error if no config-options is specified", func() {
			config := &kafka.ConsumerConfig{}
			_, err := kafka.NewConsumer(config)
			Expect(err).To(HaveOccurred())
		})
	})
})
