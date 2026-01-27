package mika

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/emillamm/envx"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func LoadKgoClient(
	env envx.EnvX,
	consumeTopics []string,
	group string,
	startOffset kgo.Offset,
) (client *kgo.Client, err error) {

	// Get broker addresses from environment, default to localhost:29092
	brokersStr, err := env.String("KAFKA_BROKERS").Default("localhost:29092")
	if err != nil {
		return nil, fmt.Errorf("failed to read KAFKA_BROKERS: %w", err)
	}
	brokers := strings.Split(brokersStr, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumeResetOffset(startOffset),
	}

	// Configure SASL authentication if mechanism is specified
	saslMechanism, err := env.String("KAFKA_SASL_MECHANISM").Default("")
	if err != nil {
		return nil, fmt.Errorf("failed to read KAFKA_SASL_MECHANISM: %w", err)
	}

	if saslMechanism != "" {
		saslOpt, err := configureSASL(env, saslMechanism)
		if err != nil {
			return nil, err
		}
		opts = append(opts, saslOpt)
	}

	// Configure TLS if enabled
	tlsEnabled, err := env.Bool("KAFKA_TLS_ENABLED").Default(false)
	if err != nil {
		return nil, fmt.Errorf("failed to read KAFKA_TLS_ENABLED: %w", err)
	}

	if tlsEnabled {
		tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
	}

	// add topics
	if len(consumeTopics) > 0 {
		opts = append(opts, kgo.ConsumeTopics(consumeTopics...))
	}

	// add group
	if group != "" {
		opts = append(opts, kgo.ConsumerGroup(group))
	}

	client, err = kgo.NewClient(opts...)
	return
}

func configureSASL(env envx.EnvX, mechanism string) (kgo.Opt, error) {
	username, err := env.String("KAFKA_SASL_USERNAME").Value()
	if err != nil {
		return nil, fmt.Errorf("KAFKA_SASL_USERNAME is required when KAFKA_SASL_MECHANISM is set: %w", err)
	}

	password, err := env.String("KAFKA_SASL_PASSWORD").Value()
	if err != nil {
		return nil, fmt.Errorf("KAFKA_SASL_PASSWORD is required when KAFKA_SASL_MECHANISM is set: %w", err)
	}

	var saslMech sasl.Mechanism
	switch strings.ToUpper(mechanism) {
	case "PLAIN":
		saslMech = plain.Auth{
			User: username,
			Pass: password,
		}.AsMechanism()
	case "SCRAM-SHA-256":
		saslMech = scram.Auth{
			User: username,
			Pass: password,
		}.AsSha256Mechanism()
	case "SCRAM-SHA-512":
		saslMech = scram.Auth{
			User: username,
			Pass: password,
		}.AsSha512Mechanism()
	default:
		return nil, fmt.Errorf("unsupported KAFKA_SASL_MECHANISM: %s (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", mechanism)
	}

	return kgo.SASL(saslMech), nil
}

