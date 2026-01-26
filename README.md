# MiKa (Micro Kafka)

MiKa is a lightweight Kafka client written in Go that simplifies working with Kafka in event-driven systems. It is wrapper around the fully-fledged twmb/franz-go Kafka client which handles all communication between the client and the Kafka brokers.

## Why MiKa?
MiKa offers the following functionality out of the box
* **Consumer-to-topic binding**: Consumers are attached to topics (1-to-1) and registered during client initialization. MiKa is responsible for delegating records to the correct consumer based on the topic where the record originated from. From a users perspective, this makes Kafka behave more like a traditional messaging broker.
* **Record Acknowledgement**: MiKa supports record acknowledgment which puts the responsibility on the application to mark records as either succeeded (acked) or failed after consumption. Failed records can be retried or parked in a DLQ.
* **Retries and DLQ**: The client can be configured with a retry and DLQ topic. This serves as a mechanism to "unblock" the client while preventing data loss when records are processed unsuccessfully. These records can be replayed later automatically (via the retry topic) or manually (via the DLQ topic). Multiple clients can share the same retry/DLQ topic while only retrying records belonging to that client.
* **Graceful shutdown**: The client can be shut down gracefully to allow already polled records to finish processing within a configurable timeframe.

## Usage
### Installation
```
go get github.com/emillamm/mika
```

### Initialize and configure the client
The client is created by calling `NewKafkaClient(ctx, env)` which takes a context and function of the signature `func(string)string` to get environment configuration. If the context expires, the client will be closed which can be useful in tests. See section Environment configuration about which environment variables can be used to configure the client. 

```
ctx := context.Background()
env := os.Getenv
client := mika.NewKafkaClient(ctx, env)
```

The client can be configured with a consumer group, a retry and DLQ topic. These configurations are optional although a consumer group is required if either retries or DLQ are configured. In most cases you would want to attach a unique consumer group to the client.

```
client.SetGroup("my-consumer-group")
client.SetRetryTopic("my-retry-topic")
client.SetDlqTopic("my-dlq-topic")
```

### Register consumers
```
topic := "my-topic"
numberOfRetries := 2
useDlq := true

func myConsumerFunc(record *mika.ConsumeRecord) {
    // process record
}

client.RegisterConsumer(topic, numberOfRetries, useDlq, myConsumerFunc)
```

### Start the client

Calling `client.Start()` will start a poll, process, commit loop if any consumers are registered and enabled. The method returns a `chan error` that will receive errors encountered during the lifecycle of the client including fetch and commit errors. Any encountered fetch or commit errors will result in the client being closed.

```
for err := range client.Start() {
    // handle error
}
```

### Consume records
The consumer function takes a `ConsumeRecord` as an argument which exposes the underlying `*kgo.Record` that contains the data for the record (see documentation). It also provides `Ack()` and `Fail(reason error)` methods that should be called when the record has been processed.

```
func consumeMyTopic(record *mika.ConsumeRecord) {
    bytes := record.Underlying.Value
    if err := processBytes(bytes); err != nil {
        record.Fail(err)
    }
    record.Ack()
}
```

Here is how Ack/Fail work under the hood.
Under the hood, records are polled in batches by the client and delegated to the consumers based on the topic they came from. A batch is committed and a new poll is started once all records are *completed*. In this context, completed means either of the following

* The record is "acked" by calling `record.Ack()`
* The record is "failed" by calling `record.Fail(reason)` and the record has not exceeded the number of retries allowed by the consumer.
* The record is "failed" by calling `record.Fail(reason)` and the consumer has enabled DLQ.

If a batch of records is never fully completed the poll loop will stall forever. It is important to note that *all* consumers on this client will stall, not just the offending consumer. To avoid this, it is recommended to use a DLQ or call "ack" failed records that are not worth reprocessing. 

### Stop the client
The client can be stopped gracefully which allows the current batch of polled records to be processed within a timeframe that can be controlled by a context.
```
ctx, _ := context.WithTimeout(context.Background(), 15 * time.Second)
client.CloseGracefully(ctx) // non-blocking
client.WaitForDone() // blocks until the client is fully closed
```

## Environment Configuration

MiKa uses environment variables for connection and consumer settings.

### Connection Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BROKERS` | Comma-separated list of broker addresses | `localhost:29092` |
| `KAFKA_TLS_ENABLED` | Enable TLS encryption | `false` |
| `KAFKA_SASL_MECHANISM` | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` | (none) |
| `KAFKA_SASL_USERNAME` | SASL username (required when mechanism is set) | |
| `KAFKA_SASL_PASSWORD` | SASL password (required when mechanism is set) | |

### Consumer Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_CONSUMER_START_FROM` | RFC3339 timestamp for initial offset (new consumer groups only) | (none) |

### Connecting to Confluent Cloud

To connect to a Confluent Cloud cluster, set the following environment variables:

```bash
export KAFKA_BROKERS="pkc-xxxxx.region.cloud.confluent.cloud:9092"
export KAFKA_TLS_ENABLED=true
export KAFKA_SASL_MECHANISM=PLAIN
export KAFKA_SASL_USERNAME="<API_KEY>"
export KAFKA_SASL_PASSWORD="<API_SECRET>"
```

The API key and secret can be generated from the Confluent Cloud Console under your cluster's API keys section.
