# MiKa (Micro Kafka)

MiKa is a lightweight Kafka client written in Go that simplifies working with Kafka in event-driven systems. It is wrapper around the fully-fledged twmb/franz-go Kafka client which handles all communication between the client and the Kafka brokers.

## Why MiKa?
MiKa offers the following functionality which is not provided out of the box by the Kafka consumer API.
* *Consumer-to-topic binding*: Consumers are attached to topics (1-to-1) and registered during client initialization. MiKa is responsible for delegating records to the correct consumer based on the topic where the record originated from. From a users perspective, this makes Kafka behave more like a traditional messaging broker.
* *Record Acknowledgement*: MiKa supports record acknowledgment which puts the responsibility on the application to mark records as either succeeded (acked) or failed after consumption. Failed records can be retried or parked in a DLQ.
* *Retries and DLQ*: The client can be configured with a retry and DLQ topic. This serves as a mechanism to "unblock" the client while preventing data loss when records are processed unsuccessfully. These records can be replayed later automatically (via the retry topic) or manually (via the DLQ topic). Multiple clients can share the same retry/DLQ topic while only retrying records belonging to that client.
* *Graceful shutdown*: The client can be shut down gracefully to allow already polled records to finish processing within a configurable timeframe.


