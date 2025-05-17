package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	ya_consumer "ya-consumer"

	"github.com/IBM/sarama"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
)

const (
	groupID = "vitals-consumer"
)

func main() {
	slog.Info("start consumer")
	defer slog.Info("end consumer")

	app, err := ya_consumer.NewApp()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(app.KafkaBrokers, groupID, config)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer func() {
		_ = consumerGroup.Close()
	}()

	handler := &AvroConsumer{
		schemaRegistry: app.SchemaRegistryClient,
	}

	for {
		if err := consumerGroup.Consume(ctx, []string{app.KafkaTopic}, handler); err != nil {
			log.Printf("Consumer error: %v", err)
		}
		if ctx.Err() != nil {
			break
		}
	}
}

type AvroConsumer struct {
	schemaRegistry *srclient.SchemaRegistryClient
	codecCache     sync.Map // map[int]*goavro.Codec
}

func (c *AvroConsumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (c *AvroConsumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (c *AvroConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	const workers = 8

	type job struct {
		msg *sarama.ConsumerMessage
	}

	jobs := make(chan job, 100)
	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case j, ok := <-jobs:
					if !ok {
						return
					}
					native, err := c.decodeAvro(j.msg.Value)
					if err != nil {
						slog.Error("decode", "err", err)
						continue
					}
					slog.Info("processed", "offset", j.msg.Offset, "data", native)

					// Потокобезопасный offset commit
					session.MarkMessage(j.msg, "")
				}
			}
		}()
	}

	for msg := range claim.Messages() {
		select {
		case <-ctx.Done():
			break
		case jobs <- job{msg: msg}:
		}
	}

	close(jobs)
	wg.Wait()
	return nil
}

func (c *AvroConsumer) decodeAvro(data []byte) (map[string]interface{}, error) {
	if len(data) < 5 || data[0] != 0 {
		return nil, fmt.Errorf("invalid Avro binary message format")
	}

	schemaID := int(binary.BigEndian.Uint32(data[1:5]))

	if val, ok := c.codecCache.Load(schemaID); ok {
		codec := val.(*goavro.Codec)
		return decode(codec, data[5:])
	}

	schema, err := c.schemaRegistry.GetSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema %d: %w", schemaID, err)
	}

	codec, err := goavro.NewCodec(schema.Schema())
	if err != nil {
		return nil, fmt.Errorf("invalid Avro schema: %w", err)
	}

	c.codecCache.Store(schemaID, codec)
	return decode(codec, data[5:])
}

func decode(codec *goavro.Codec, binaryData []byte) (map[string]interface{}, error) {
	native, _, err := codec.NativeFromBinary(binaryData)
	if err != nil {
		return nil, fmt.Errorf("avro decode failed: %w", err)
	}
	record, ok := native.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected Avro value type")
	}
	return record, nil
}
