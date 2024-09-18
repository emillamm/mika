# Mika (Micro Kafka)

Mika is a lightweight Kafka client written in Go that simplifies working with Kafka in event-driven systems. It is wrapper around the fully-fledged twmb/franz-go Kafka client which handles all communication between the client and the Kafka brokers.

## Why Mika?
Mika offers the following functionality which is otherwise not provided out of the box by the Kafka consumer API.
* Consumer-to-topic binding: Consumers are attached to topics (1-to-1) and registered during client initialization which makes Kafka behave more like a traditional messaging system.
* Record Ack'ing: The application marks records as succeeded (ack) or failed after processing them. Failed records can be retried or parked in a DLQ.
* Retries and DLQ: The client can be configured with a retry and DLQ topic which serves as a mechanism to "unblock" the client when records are processed unsuccessfully without losing records. These records can be replayed automatically (via the retry topic) or manually (via the DLQ topic).


