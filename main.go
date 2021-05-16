package main

import (
	"flag"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/streadway/amqp"
	"github.com/thanhftu/eventsservice/rest"
	"github.com/thanhftu/lib/configuration"
	"github.com/thanhftu/lib/msgqueue"
	msgqueue_amqp "github.com/thanhftu/lib/msgqueue/amqp"
	"github.com/thanhftu/lib/msgqueue/kafka"
	"github.com/thanhftu/lib/persistence/dblayer"
)

func main() {
	var eventEmitter msgqueue.EventEmitter

	confPath := flag.String("conf", `.\configuration\config.json`, "flag to set the path to the configuration json file")
	flag.Parse()
	//extract configuration
	config, _ := configuration.ExtractConfiguration(*confPath)
	fmt.Println(config.KafkaMessageBrokers)
	fmt.Println(config.MessageBrokerType)
	// fmt.Println("env: ", os.Getenv("KAFKA_BROKER_URLS"))

	// for using  kafka if using rabbitmq, comment this line
	// config.MessageBrokerType = "kafka"
	// config.KafkaMessageBrokers = []string{"localhost:9092"}

	switch config.MessageBrokerType {
	case "amqp":
		conn, err := amqp.Dial(config.AMQPMessageBroker)
		if err != nil {
			panic(err)
		}

		eventEmitter, err = msgqueue_amqp.NewAMQPEventEmitter(conn, "events")
		if err != nil {
			panic(err)
		}
	case "kafka":

		conf := sarama.NewConfig()
		conf.Producer.Return.Successes = true
		conn, err := sarama.NewClient(config.KafkaMessageBrokers, conf)
		if err != nil {
			panic(err)
		}

		eventEmitter, err = kafka.NewKafkaEventEmitter(conn)
		if err != nil {
			panic(err)
		}
	default:
		panic("Bad message broker type: " + config.MessageBrokerType)
	}

	fmt.Println("Connecting to database")
	dbhandler, _ := dblayer.NewPersistenceLayer(config.Databasetype, config.DBConnection)

	fmt.Println("Serving API")
	//RESTful API start
	fmt.Println("EndPoint", config.RestfulEndpoint)
	err := rest.ServeAPI(config.RestfulEndpoint, dbhandler, eventEmitter)
	if err != nil {
		panic(err)
	}
}
