package consumer

import (
	"sync"
	"testing"
	"time"

	"github.com/TerrexTech/go-kafkautils/mocks"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestConsumer tests the critical Consumer functions
func TestConsumer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Consumer Suite")
}

var mockInitFunc = func([]string, string, []string, *cluster.Config) (*cluster.Consumer, error) {
	return nil, nil
}

func setupMockConsumer(initConfig *Config) (*Consumer, *mocks.Consumer, error) {
	// Overwrite the original init function
	initFunc = mockInitFunc
	proxyConsumer, err := New(initConfig)

	fakeConsumer := &mocks.Consumer{
		ErrorsChan:        make(chan error),
		MessagesChan:      make(chan *sarama.ConsumerMessage),
		NotificationsChan: make(chan *cluster.Notification),
		PartitionsChan:    make(chan cluster.PartitionConsumer),
	}

	proxyConsumer.consumer = fakeConsumer
	// Re-attach the handlers
	proxyConsumer.handleKeyInterrupt()
	proxyConsumer.handleErrors(initConfig.ErrHandler)
	proxyConsumer.handleMessages(initConfig.MsgHandler)
	proxyConsumer.handleNotifications(initConfig.NtfnHandler)
	return proxyConsumer, fakeConsumer, err
}

// Unlike sarama, sarama-cluster does not provide a mock broker.
// So we'll have to interfave the consumer more extensively.
// Be advised: Some minor hacks have been used to facilitate testing.
var _ = Describe("Consumer", func() {
	Context("new instance is requested", func() {
		var (
			config     *Config
			errHandler func(*error)

			proxyConsumer *Consumer
			err           error
		)

		BeforeEach(func() {
			initFunc = mockInitFunc

			errHandler = func(err *error) {
				Fail("Error occurred: " + (*err).Error())
			}
			config = &Config{
				ConsumerGroup: "test-group",
				KafkaBrokers:  []string{"test-broker"},
				ErrHandler:    errHandler,
				Topics:        []string{"test-topics"},
			}
			proxyConsumer, _, err = setupMockConsumer(config)
		})
		AfterEach(func() {
			proxyConsumer.Close()
		})

		It("should return a new sarama-cluster consumer instance", func() {
			Expect(*proxyConsumer).To(BeAssignableToTypeOf(Consumer{}))
			Expect(err).To(BeNil())
		})

		It("should return the error when initializing new consumer fails", func() {
			initFunc = cluster.NewConsumer
			_, err := New(config)
			Expect(err).To(HaveOccurred())
		})

		It("should return error when no brokers are provided", func() {
			config = &Config{
				ErrHandler: errHandler,
			}
			_, err := New(config)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("an error occurs while fetching messages", func() {
		var (
			config             *Config
			errHandler         func(*error)
			isErrHandlerCalled bool
			fakeConsumer       *mocks.Consumer
		)

		BeforeEach(func() {
			isErrHandlerCalled = false
			errHandler = func(_ *error) {
				isErrHandlerCalled = true
			}
			config = &Config{
				ConsumerGroup: "test-group",
				KafkaBrokers:  []string{"test-broker"},
				ErrHandler:    errHandler,
				Topics:        []string{"test-topics"},
			}
			_, fakeConsumer, _ = setupMockConsumer(config)
		})
		AfterEach(func() {
			fakeConsumer.Close()
		})

		It("should run the error-handler function", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				fakeConsumer.ErrorsChan <- fakeConsumer.CreateMockError()
				defer wg.Done()
			}()
			wg.Wait()

			// Wait for error-handler routines to complete
			time.Sleep(5 * time.Millisecond)
			Expect(isErrHandlerCalled).To(BeTrue())
		})
	})

	Context("new message is received", func() {
		var (
			config             *Config
			errHandler         func(*error)
			msgHandler         func(*sarama.ConsumerMessage, *Consumer)
			isMsgHandlerCalled bool
			fakeConsumer       *mocks.Consumer
		)

		BeforeEach(func() {
			isMsgHandlerCalled = false
			errHandler = func(err *error) {
				Fail("Error occurred: " + (*err).Error())
			}
			msgHandler = func(_ *sarama.ConsumerMessage, _ *Consumer) {
				isMsgHandlerCalled = true
			}

			config = &Config{
				ConsumerGroup: "test-group",
				KafkaBrokers:  []string{"test-broker"},
				ErrHandler:    errHandler,
				MsgHandler:    msgHandler,
				Topics:        []string{"test-topics"},
			}
			_, fakeConsumer, _ = setupMockConsumer(config)
		})
		AfterEach(func() {
			fakeConsumer.Close()
		})

		It("should run the message-handler function", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				fakeConsumer.MessagesChan <- fakeConsumer.CreateMockMessage("test", "test", "test")
				defer wg.Done()
			}()
			wg.Wait()

			// Wait for error-handler routines to complete
			time.Sleep(5 * time.Millisecond)
			Expect(isMsgHandlerCalled).To(BeTrue())
		})
	})

	Context("new notification is received", func() {
		var (
			config              *Config
			errHandler          func(*error)
			ntfnHandler         func(*cluster.Notification)
			isNtfnHandlerCalled bool
			fakeConsumer        *mocks.Consumer
		)

		BeforeEach(func() {
			isNtfnHandlerCalled = false
			errHandler = func(err *error) {
				Fail("Error occurred: " + (*err).Error())
			}
			ntfnHandler = func(_ *cluster.Notification) {
				isNtfnHandlerCalled = true
			}

			config = &Config{
				ConsumerGroup: "test-group",
				KafkaBrokers:  []string{"test-broker"},
				ErrHandler:    errHandler,
				NtfnHandler:   ntfnHandler,
				Topics:        []string{"test-topics"},
			}
			_, fakeConsumer, _ = setupMockConsumer(config)
		})
		AfterEach(func() {
			fakeConsumer.Close()
		})

		It("should run the message-handler function", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				fakeConsumer.NotificationsChan <- fakeConsumer.CreateMockNotification()
				defer wg.Done()
			}()
			wg.Wait()

			// Wait for error-handler routines to complete
			time.Sleep(5 * time.Millisecond)
			Expect(isNtfnHandlerCalled).To(BeTrue())
		})
	})

	Context("consumer is requested to be closed", func() {
		var (
			config     *Config
			errHandler func(*error)

			proxyConsumer *Consumer
			fakeConsumer  *mocks.Consumer
		)

		BeforeEach(func() {
			errHandler = func(err *error) {
				Fail("Error occurred: " + (*err).Error())
			}
			config = &Config{
				ConsumerGroup: "test-group",
				KafkaBrokers:  []string{"test-broker"},
				ErrHandler:    errHandler,
				Topics:        []string{"test-topics"},
			}
			proxyConsumer, fakeConsumer, _ = setupMockConsumer(config)
		})
		AfterEach(func() {
			proxyConsumer.Close()
		})

		It("should return nil if already closed", func() {
			proxyConsumer.Close()
			// Allow consumer to close
			time.Sleep(5 * time.Millisecond)
			Expect(proxyConsumer.Close()).To(BeNil())
		})

		It("should close consumer if its open", func() {
			Expect(proxyConsumer.IsClosed()).To(BeFalse())
			proxyConsumer.Close()

			// Allow consumer to close
			time.Sleep(5 * time.Millisecond)
			Expect(proxyConsumer.IsClosed()).To(BeTrue())
			Expect(fakeConsumer.IsClosed()).To(BeTrue())
		})
	})
})
