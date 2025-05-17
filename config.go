package ya_consumer

import (
	_ "embed"
	"fmt"
	"os"

	"github.com/Netflix/go-env"
	"github.com/joho/godotenv"
	"github.com/riferrei/srclient"
)

type App struct {
	SchemaRegistryClient  *srclient.SchemaRegistryClient
	SchemaRegistrySubject string `env:"KAFKA_SCHEMA_REGISTRY_SUBJECT,required=true,default=vitals-value"`

	KafkaSchemaRegistryURL string   `env:"KAFKA_SCHEMA_REGISTRY_URL,required=true"`
	KafkaBrokers           []string `env:"KAFKA_BROKERS,required=true"`
	KafkaTopic             string   `env:"KAFKA_TOPIC,required=true,default=vitals"`
}

func NewApp() (App, error) {
	cfg := App{}

	if err := godotenv.Load(".env"); err != nil && !os.IsNotExist(err) {
		return App{}, fmt.Errorf("dot env load: %w", err)
	}

	_, err := env.UnmarshalFromEnviron(&cfg)
	if err != nil {
		return App{}, err
	}

	cfg.SchemaRegistryClient = srclient.NewSchemaRegistryClient(cfg.KafkaSchemaRegistryURL)
	_, err = cfg.SchemaRegistryClient.GetSubjects()
	if err != nil {
		return App{}, fmt.Errorf("get subjects: %w", err)
	}

	return cfg, nil
}
