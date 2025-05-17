package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/binary"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	ya_consumer "ya-consumer"

	"github.com/IBM/sarama"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
)

//go:embed avro_schema.json
var avroSchema string

func main() {
	slog.Info("start producer")
	defer slog.Info("end producer")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, os.Interrupt)
	defer cancel()

	app, err := ya_consumer.NewApp()
	if err != nil {
		log.Fatal(err)
	}

	schemaID, codec, err := initAvroCodec(app.SchemaRegistryClient, app.SchemaRegistrySubject)
	if err != nil {
		log.Fatal(err)
	}

	producer, err := sarama.NewAsyncProducer(app.KafkaBrokers, newReliableProducerConfig())
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		_ = producer.Close()
	}()

	go func() {
		for err = range producer.Errors() {
			slog.Error("Produce failed", "err", err)
		}
	}()

	go func() {
		for success := range producer.Successes() {
			slog.Info("Produced", "partition", success.Partition)
		}
	}()

	for i := 0; i < 10000; i++ {
		patientID := fmt.Sprintf("patient-%d", i)

		select {
		case <-ctx.Done():
			return
		default:
		}

		native := map[string]interface{}{
			"patient_id": patientID,
			"heartbeat":  72 + i%5,
			"timestamp":  time.Now().UnixMilli(),
		}

		avroData, err := codec.BinaryFromNative(nil, native)
		if err != nil {
			slog.Error("avro form body", "err", err)
		}

		var kafkaValue bytes.Buffer
		kafkaValue.WriteByte(0)
		if err := binary.Write(&kafkaValue, binary.BigEndian, schemaID); err != nil {
			log.Fatalf("failed to write schema ID: %v", err)
		}
		kafkaValue.Write(avroData)

		producer.Input() <- &sarama.ProducerMessage{
			Topic: app.KafkaTopic,
			Key:   sarama.StringEncoder(patientID),
			Value: sarama.ByteEncoder(kafkaValue.Bytes()),
		}
	}

}

func newReliableProducerConfig() *sarama.Config {
	cfg := sarama.NewConfig()

	// Надёжность
	cfg.Producer.RequiredAcks = sarama.WaitForAll       // Дождаться подтверждения от всех реплик
	cfg.Producer.Retry.Max = 10                         // Кол-во ретраев при ошибках
	cfg.Producer.Retry.Backoff = 100 * time.Millisecond // Задержка между ретраями

	// Быстродействие
	cfg.Producer.Flush.Frequency = 10 * time.Millisecond // Частота сброса сообщений в брокер
	cfg.Producer.Flush.Messages = 100                    // Сколько сообщений минимум сбрасывать за раз
	cfg.Producer.Flush.MaxMessages = 200                 // Ограничение на размер батча

	// Потокобезопасность
	cfg.Producer.Return.Successes = true // Нужно для правильной работы с async producer
	cfg.Producer.Return.Errors = true

	// Балансировка ключей
	cfg.Producer.Partitioner = sarama.NewHashPartitioner // Определённый партиционер (по ключу)

	// Сжатие (опционально)
	cfg.Producer.Compression = sarama.CompressionSnappy // Легковесное быстрое сжатие

	return cfg
}

func initAvroCodec(schemaRegistryClient *srclient.SchemaRegistryClient, subject string) (uint32, *goavro.Codec, error) {
	schema, err := schemaRegistryClient.GetLatestSchema(subject)
	if err != nil {
		schema, err = schemaRegistryClient.CreateSchema(subject, avroSchema, srclient.Avro)
		if err != nil {
			return 0, nil, fmt.Errorf("error creating avro schema: %v", err)
		}
	}

	schemaID := uint32(schema.ID())
	codec, err := goavro.NewCodec(schema.Schema())
	if err != nil {
		return 0, nil, fmt.Errorf("failed to create Avro codec: %w", err)
	}

	return schemaID, codec, nil
}
