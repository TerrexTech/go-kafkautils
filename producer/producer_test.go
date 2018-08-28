package producer

import (
	"sync"
	"testing"
	"time"

	"github.com/TerrexTech/go-kafkautils/mocks"

	"github.com/Shopify/sarama"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestProducer tests the critical Producer functions
func TestProducer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Producer Suite")
}

func setupMockBroker() (*sarama.MockBroker, string) {
	mockBroker := sarama.NewMockBroker(GinkgoT(), 0)
	metadataResponse := new(sarama.MetadataResponse)

	metadataResponse.AddBroker(mockBroker.Addr(), mockBroker.BrokerID())
	metadataResponse.AddTopicPartition("test", 0, mockBroker.BrokerID(), nil, nil, sarama.ErrNoError)
	mockBroker.Returns(metadataResponse)

	return mockBroker, mockBroker.Addr()
}

func setupMockProducer(config *Config) (*Producer, *mocks.AsyncProducer) {
	fakeProducer := mocks.AsyncProducer{
		ErrorsChan:    make(chan *sarama.ProducerError),
		InputChan:     make(chan *sarama.ProducerMessage),
		SuccessesChan: make(chan *sarama.ProducerMessage),
	}

	asyncProducer, _ := New(config)
	// Close the actual producer, since it will be replaced by a mock
	asyncProducer.producer.Close()
	asyncProducer.producer = &fakeProducer

	return asyncProducer, &fakeProducer
}

var _ = Describe("Producer", func() {
	Context("new instance is requested", func() {
		var (
			mockBroker *sarama.MockBroker
			brokerAddr string
			config     *Config

			asyncProducer *Producer
			err           error
		)

		BeforeEach(func() {
			// Setup a default basic producer-pipeline
			mockBroker, brokerAddr = setupMockBroker()

			config = &Config{
				KafkaBrokers: []string{brokerAddr},
			}
			asyncProducer, err = New(config)
		})
		AfterEach(func() {
			asyncProducer.Close()
			mockBroker.Close()
		})

		It("should return a new sarama-producer instance", func() {
			Expect(*asyncProducer).To(BeAssignableToTypeOf(Producer{}))
			Expect(err).To(BeNil())
		})

		It("should return the error when initializing new producer fails", func() {
			config = &Config{
				KafkaBrokers: []string{"invalid-broker"},
			}
			_, err := New(config)
			Expect(err).To(HaveOccurred())
		})

		It("should return error when no brokers are provided", func() {
			config = &Config{}
			_, err := New(config)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("input message is provided to producer", func() {
		var (
			mockBroker *sarama.MockBroker
			brokerAddr string
			config     *Config

			asyncProducer *Producer
			fakeProducer  *mocks.AsyncProducer

			isMsgProduced bool
		)

		BeforeEach(func() {
			isMsgProduced = false
			// Setup a default basic producer-pipeline
			mockBroker, brokerAddr = setupMockBroker()

			config = &Config{
				KafkaBrokers: []string{brokerAddr},
			}
			asyncProducer, fakeProducer = setupMockProducer(config)
		})
		AfterEach(func() {
			asyncProducer.Close()
			mockBroker.Close()
		})

		It("should return error if producer is closed", func() {
			asyncProducer.Close()
			// Allow producer to close
			time.Sleep(5 * time.Millisecond)

			input, err := asyncProducer.Input()
			Expect(input).To(BeNil())
			Expect(err).To(HaveOccurred())
		})

		It("should produce the message", func() {
			input, err := asyncProducer.Input()
			Expect(input).ToNot(BeNil())
			Expect(err).ToNot(HaveOccurred())

			// Read from mock-producer's Input channel to check
			// if the message was produced
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				for range fakeProducer.InputChan {
					isMsgProduced = true
					wg.Done()
				}
			}()

			// Produce the message
			input <- fakeProducer.CreateMockMessage("test", "test", "test")
			wg.Wait()
			Expect(isMsgProduced).To(BeTrue())
		})
	})

	Context("an error occurs while producing messages", func() {
		var (
			mockBroker *sarama.MockBroker
			brokerAddr string
			config     *Config

			asyncProducer *Producer
			fakeProducer  *mocks.AsyncProducer
		)

		BeforeEach(func() {
			// Setup a default basic producer-pipeline
			mockBroker, brokerAddr = setupMockBroker()

			config = &Config{
				KafkaBrokers: []string{brokerAddr},
			}
			asyncProducer, fakeProducer = setupMockProducer(config)
		})
		AfterEach(func() {
			asyncProducer.Close()
			mockBroker.Close()
		})

		It("should run the error-handler function", func() {
			isErrorForwarded := false
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				for _ = range fakeProducer.Errors() {
					isErrorForwarded = true
				}
			}()
			go func() {
				fakeProducer.ErrorsChan <- fakeProducer.CreateMockError("test", "test", "test")
				defer wg.Done()
			}()
			wg.Wait()

			// Wait for error-handler routines to complete
			time.Sleep(5 * time.Millisecond)
			Expect(isErrorForwarded).To(BeTrue())
		})
	})

	Context("producer is requested to be closed", func() {
		var (
			mockBroker *sarama.MockBroker
			brokerAddr string
			config     Config

			asyncProducer *Producer
			fakeProducer  *mocks.AsyncProducer
		)

		BeforeEach(func() {
			// Setup a default basic producer-pipeline
			mockBroker, brokerAddr = setupMockBroker()
			config = Config{
				KafkaBrokers: []string{brokerAddr},
			}
			asyncProducer, fakeProducer = setupMockProducer(&config)
		})

		AfterEach(func() {
			asyncProducer.Close()
			mockBroker.Close()
		})

		It("should return nil if already closed", func() {
			asyncProducer.Close()
			// Allow producer to close
			time.Sleep(5 * time.Millisecond)
			Expect(asyncProducer.Close()).To(BeNil())
		})

		It("should close producer if its open", func() {
			Expect(asyncProducer.IsClosed()).To(BeFalse())
			asyncProducer.Close()

			// Allow producer to close
			time.Sleep(5 * time.Millisecond)
			Expect(fakeProducer.IsClosed()).To(BeTrue())
			Expect(asyncProducer.IsClosed()).To(BeTrue())
		})
	})

})
