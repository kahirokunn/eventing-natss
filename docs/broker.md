# NATS JetStream Broker

Use the NATS JetStream Broker when producers need durable event storage and
Trigger subscribers should consume events from NATS JetStream.

## Overview

The NATS JetStream Broker is a Knative Eventing Broker implementation backed by NATS JetStream. It provides:

- **Durable message storage** - Events are persisted in JetStream streams
- **At-least-once delivery** - Events are redelivered until acknowledged
- **Trigger filtering** - Filter events using CloudEvents attributes
- **Dead letter sink support** - Failed events can be sent to a dead letter sink
- **Configurable retry policies** - Configure backoff and retry behavior

## Components

The broker consists of two main components:

### Broker Controller (`natsjs-broker-controller`)

Reconciles `Broker` resources with the `NatsJetStreamBroker` class annotation. It creates:

- One JetStream stream per Broker for event storage
- One filter Deployment per Broker when that Broker has at least one Trigger
- An optional KEDA ScaledObject for the filter Deployment

### Filter (`natsjs-broker-filter`)

Handles event delivery to trigger subscribers:

- Consumes events from JetStream streams
- Applies trigger filters to events
- Dispatches matching events to subscriber endpoints
- Handles retries and dead letter sinks

### Ingress (`natsjs-broker-ingress`)

Receives events via HTTP and publishes them to JetStream streams.

## Installation

1. Install NATS JetStream:

```shell
kubectl apply -f ./config/broker/nats.yaml
```

2. Install the broker controller and data plane:

```shell
ko apply -f ./config/broker
```

## Usage

### Creating a Broker

```yaml
apiVersion: eventing.knative.dev/v1
kind: Broker
metadata:
  name: my-broker
  namespace: default
  annotations:
    eventing.knative.dev/broker.class: NatsJetStreamBroker
spec:
  delivery:
    retry: 3
    backoffPolicy: exponential
    backoffDelay: PT1S
```

### Creating Triggers

Triggers subscribe to events from the broker and deliver them to subscribers.

#### Basic Trigger (no filter - receives all events)

```yaml
apiVersion: eventing.knative.dev/v1
kind: Trigger
metadata:
  name: my-trigger
  namespace: default
spec:
  broker: my-broker
  subscriber:
    ref:
      apiVersion: v1
      kind: Service
      name: my-service
```

#### Trigger with Legacy Attributes Filter

The legacy filter uses the `filter.attributes` field to match CloudEvents attributes:

```yaml
apiVersion: eventing.knative.dev/v1
kind: Trigger
metadata:
  name: order-created-trigger
  namespace: default
spec:
  broker: my-broker
  filter:
    attributes:
      type: com.example.order.created
      source: /orders
  subscriber:
    ref:
      apiVersion: v1
      kind: Service
      name: order-processor
```

#### Trigger with New Subscriptions API Filters

The new `filters` field (plural) supports more advanced filtering using the CloudEvents Subscriptions API:

**Exact Match:**
```yaml
apiVersion: eventing.knative.dev/v1
kind: Trigger
metadata:
  name: exact-filter-trigger
spec:
  broker: my-broker
  filters:
    - exact:
        type: com.example.order.created
  subscriber:
    ref:
      apiVersion: v1
      kind: Service
      name: order-processor
```

### Dead Letter Sink

Configure a dead letter sink to receive events that fail delivery after all retries:

```yaml
apiVersion: eventing.knative.dev/v1
kind: Trigger
metadata:
  name: trigger-with-dls
spec:
  broker: my-broker
  subscriber:
    ref:
      apiVersion: v1
      kind: Service
      name: my-service
  delivery:
    deadLetterSink:
      ref:
        apiVersion: v1
        kind: Service
        name: dead-letter-service
    retry: 3
    backoffPolicy: exponential
    backoffDelay: PT1S
```

## Configuration

### Scale a Broker Filter to Zero with KEDA

Cluster operators can install KEDA to stop filter Pods while every Trigger on a
Broker is idle. The scale target is the Broker's shared filter Deployment, not
an individual Trigger. For example, `shop/orders` owns the Deployment and
ScaledObject named `orders-broker-filter`. Every Trigger on `shop/orders` has a
separate JetStream Consumer, and the ScaledObject checks all of those Consumers.

| Who configures it | Resource | Example | Required condition |
|---|---|---|---|
| Cluster operator | `config-nats-broker` in `knative-eventing` | `nats.nats-io.svc.cluster.local:8222` | KEDA can reach the NATS monitoring port |
| Broker owner | Broker annotation | `autoscaling.eventing.knative.dev/class: keda.autoscaling.knative.dev` | KEDA CRDs and operator are installed |
| Trigger owner | Optional Trigger lag annotations | `lag-threshold: "10"` | Trigger references the autoscaled Broker |

KEDA is optional and is not installed by the eventing-natss release manifests.
Install KEDA 2.19 or newer first, then expose the NATS monitoring port to the
KEDA operator. KEDA 2.19 added native fallback while scaling from zero; CI tests
this integration with KEDA 2.20.2. The generated trigger metadata follows the
[KEDA NATS JetStream scaler](https://keda.sh/docs/2.20/scalers/nats-jetstream/).
Configure that endpoint in `config-nats-broker`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: config-nats-broker
  namespace: knative-eventing
data:
  eventing-nats: |
    url: nats://nats.nats-io.svc.cluster.local:4222
    autoscaler:
      monitoringEndpoint: nats.nats-io.svc.cluster.local:8222
      account: "$G"
      useHttps: false
```

`monitoringEndpoint` is `host:port` without `http://` or `https://`. Restart the
Broker controller after changing this ConfigMap, as with the NATS connection
settings.

Opt one Broker in explicitly:

```yaml
apiVersion: eventing.knative.dev/v1
kind: Broker
metadata:
  name: orders
  namespace: shop
  annotations:
    eventing.knative.dev/broker.class: NatsJetStreamBroker
    autoscaling.eventing.knative.dev/class: keda.autoscaling.knative.dev
    autoscaling.eventing.knative.dev/min-scale: "0"
    autoscaling.eventing.knative.dev/max-scale: "20"
    autoscaling.eventing.knative.dev/polling-interval: "10"
    autoscaling.eventing.knative.dev/cooldown-period: "30"
    autoscaling.eventing.knative.dev/lag-threshold: "100"
```

| Annotation | Where to set it | Default | Decision it controls |
|---|---|---:|---|
| `autoscaling.eventing.knative.dev/class` | Broker | unset | The exact value `keda.autoscaling.knative.dev` enables KEDA |
| `autoscaling.eventing.knative.dev/min-scale` | Broker | `0` | Minimum filter replicas |
| `autoscaling.eventing.knative.dev/max-scale` | Broker | `50` | Maximum filter replicas |
| `autoscaling.eventing.knative.dev/polling-interval` | Broker | `10` | Seconds between KEDA lag checks |
| `autoscaling.eventing.knative.dev/cooldown-period` | Broker | `30` | Idle seconds before scaling to zero |
| `autoscaling.eventing.knative.dev/lag-threshold` | Broker or Trigger | `100` | Pending messages per desired replica |
| `autoscaling.eventing.knative.dev/activation-lag-threshold` | Broker or Trigger | `0` | Pending messages required to wake from zero |

For the two lag annotations, a value on the Trigger overrides the Broker value.
The other annotations are Broker-wide because all of its Triggers share the
same Deployment. When several Consumers have backlog, KEDA uses the largest
replica request rather than adding the requests together.

Every generated ScaledObject also includes this safety fallback:

```yaml
spec:
  fallback:
    failureThreshold: 3
    replicas: max(1, min-scale)
    behavior: static
```

The fallback keeps at least one filter Pod running after the NATS scaler
repeatedly fails, including when the filter was already at zero replicas. It
never reduces the Deployment below `min-scale`, and does not replace the
normal `max-scale` or lag settings.

The annotation names and defaults match the Kafka Broker autoscaler, but the
resource mapping does not:

| Question | Kafka Broker | NATS JetStream Broker |
|---|---|---|
| What opts in? | Cluster-wide feature flag, with per-resource overrides | Exact KEDA class annotation on each Broker |
| What does KEDA scale? | A Trigger's internal ConsumerGroup scale target | The Broker's `<broker>-broker-filter` Deployment |
| How many ScaledObjects? | One for each autoscaled ConsumerGroup | One for each opted-in Broker |
| Where is lag configured? | On the Trigger or other Kafka resource | Broker default, optionally overridden by each Trigger |

While idle, this is expected:

```shell
kubectl -n shop get deployment orders-broker-filter
# READY   UP-TO-DATE   AVAILABLE
# 0/0     0            0

kubectl -n shop get scaledobject orders-broker-filter
kubectl -n shop get broker orders
# The Broker remains Ready; its Filter condition reason is ScaledToZero.
```

The recovery path depends on what failed:

| Failure | Who restores delivery | Result |
|---|---|---|
| NATS monitoring endpoint or scaler request fails | KEDA native fallback | The ScaledObject and HPA remain installed and KEDA requests `max(1, min-scale)` replicas |
| KEDA operator or metrics API stops while the filter is at zero | NATS Broker controller | Every 30 seconds it reads each JetStream Consumer over the NATS data connection; backlog above `activation-lag-threshold` causes a scale to `max(1, min-scale)` and an `AutoscalerSafetyWakeup` event |
| KEDA CRD, permissions, or autoscaling configuration is unavailable | NATS Broker controller static fallback | It removes the ScaledObject when possible and repeatedly enforces `max(1, min-scale)` replicas until cleanup succeeds |

For example, if `orders-broker-filter` is at zero and the KEDA operator stops,
publishing an event for `shop/orders` leaves it in JetStream until the Broker
controller observes that Consumer's backlog and starts one filter Pod. The
ScaledObject is preserved so KEDA resumes normal scaling after recovery. If
both KEDA and the NATS Broker controller are stopped, neither component can
wake a zero-replica filter.

Removing the class annotation deletes the ScaledObject and restores the
configured static `filter.replicas` value.

### Filter Environment Variables

These variables set the **broker-wide defaults** for the filter deployment. All triggers in the same broker share these defaults unless they override them with per-trigger annotations (see below).

Deployment template labels and environment variables are additive. The
controller-owned Broker, role, namespace, stream, NATS, metrics, logging,
leader-election, Pod, and container identity values take precedence when a
template uses the same key or environment variable name.

| Variable | Description | Default |
|----------|-------------|---------|
| `NATS_URL` | NATS server URL | Required |
| `POD_NAME` | Pod name for identification | Required |
| `CONTAINER_NAME` | Container name for identification | Required |
| `BROKER_NAME` | Broker served by this filter process | Required |
| `BROKER_NAMESPACE` | Namespace containing that Broker and its Triggers | Required |
| `STREAM_NAME` | JetStream stream owned by that Broker | Required |
| `CONSUMER_FETCH_BATCH_SIZE` | Number of messages to fetch per batch | `10` |
| `CONSUMER_FETCH_TIMEOUT` | How long a fetch waits for messages before returning empty | `200ms` |
| `CONSUMER_MAX_CONCURRENCY` | Maximum concurrent in-flight HTTP dispatches per trigger | `20` |

### Configuring Consumer Fetch via Broker Annotation

Set broker-wide defaults for all triggers using the `natsjetstream.eventing.knative.dev/config` annotation. These are injected as environment variables into the filter deployment.

```yaml
apiVersion: eventing.knative.dev/v1
kind: Broker
metadata:
  name: my-broker
  annotations:
    eventing.knative.dev/broker.class: NatsJetStreamBroker
    natsjetstream.eventing.knative.dev/config: |
      {
        "filter": {
          "replicas": 2,
          "env": [
            {"name": "CONSUMER_FETCH_BATCH_SIZE", "value": "50"},
            {"name": "CONSUMER_FETCH_TIMEOUT", "value": "1s"},
            {"name": "CONSUMER_MAX_CONCURRENCY", "value": "40"}
          ]
        }
      }
```

### Per-Trigger Configuration

Each trigger can override the broker-wide defaults via annotations. This lets individual triggers with different throughput or latency requirements coexist in the same broker without affecting each other.

| Annotation | Description | Default |
|------------|-------------|---------|
| `natsjetstream.eventing.knative.dev/fetch-batch-size` | Messages fetched per pull request | `CONSUMER_FETCH_BATCH_SIZE` |
| `natsjetstream.eventing.knative.dev/fetch-timeout` | Wait time per fetch when no messages arrive (Go duration, e.g. `200ms`, `1s`) | `CONSUMER_FETCH_TIMEOUT` |
| `natsjetstream.eventing.knative.dev/max-concurrency` | Maximum concurrent in-flight HTTP dispatches for this trigger | `CONSUMER_MAX_CONCURRENCY` |

```yaml
apiVersion: eventing.knative.dev/v1
kind: Trigger
metadata:
  name: high-throughput-trigger
  annotations:
    natsjetstream.eventing.knative.dev/fetch-batch-size: "100"
    natsjetstream.eventing.knative.dev/fetch-timeout: "1s"
    natsjetstream.eventing.knative.dev/max-concurrency: "50"
spec:
  broker: my-broker
  subscriber:
    ref:
      apiVersion: v1
      kind: Service
      name: analytics-service
```

### Backpressure and Concurrency Model

The filter dispatches messages concurrently using a per-trigger counting semaphore controlled by `max-concurrency`. The semaphore is acquired **before** fetching each message's dispatch goroutine, so when all slots are occupied the fetch loop stalls and new messages remain in the JetStream stream with their AckWait clocks not yet running. This prevents unbounded goroutine growth and avoids the AckWait expiry / duplicate-delivery problem that arises when messages are fetched faster than they can be processed.

Each dispatched message also carries a context deadline equal to the consumer's `AckWait` (set via `trigger.spec.delivery.timeout`). If the subscriber does not respond within that window the HTTP call is cancelled and JetStream redelivers the message automatically.

Slow triggers affect only their own semaphore — they cannot starve or delay other triggers sharing the same filter pod.

### Tuning Guidelines

| Scenario | Batch Size | Fetch Timeout | Max Concurrency | Use Case |
|----------|------------|---------------|-----------------|----------|
| High Throughput | 50–100 | 1–2s | 50–100 | Batch processing, analytics |
| Low Latency | 1–5 | 100ms | 10–20 | Real-time notifications |
| Slow Subscriber | 5–10 | 200ms | 2–5 | Rate-limited or expensive downstream |
| Balanced (default) | 10 | 200ms | 20 | General purpose |

The fetch loop caps each pull request to the number of free semaphore slots, so a batch is never larger than the available dispatch capacity. This means `max-concurrency` and `fetch-batch-size` can be tuned independently — there is no requirement for one to be larger than the other.

## Architecture

```
                                    ┌─────────────────────┐
                                    │   Event Producer    │
                                    └──────────┬──────────┘
                                               │
                                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                            Broker                                         │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────┐   │
│  │     Ingress     │───▶│ JetStream Stream │◀───│       Filter        │   │
│  │  (HTTP Server)  │    │   (Persistence)  │    │ (Consumer Manager)  │   │
│  └─────────────────┘    └─────────────────┘    └─────────┬───────────┘   │
└──────────────────────────────────────────────────────────┼───────────────┘
                                                           │
                         ┌─────────────────────────────────┼─────────────────┐
                         │                                 │                 │
                         ▼                                 ▼                 ▼
              ┌─────────────────┐             ┌─────────────────┐   ┌───────────────┐
              │   Trigger A     │             │   Trigger B     │   │   Trigger C   │
              │ (type=created)  │             │ (type=updated)  │   │ (no filter)   │
              └────────┬────────┘             └────────┬────────┘   └───────┬───────┘
                       │                               │                    │
                       ▼                               ▼                    ▼
              ┌─────────────────┐             ┌─────────────────┐   ┌───────────────┐
              │   Subscriber A  │             │   Subscriber B  │   │  Subscriber C │
              └─────────────────┘             └─────────────────┘   └───────────────┘
```

## Retry Behavior

When a subscriber returns an error or times out:

1. **5xx errors, 408, 429**: Message is redelivered with backoff
2. **4xx errors (except 408, 429)**: Message is terminated (non-retriable)
3. **Network errors**: Message is redelivered with backoff

After all retries are exhausted, if a dead letter sink is configured, the event is sent there before being acknowledged.

## Troubleshooting

### Events not being delivered

1. Check that the trigger filter matches the event attributes
2. Verify the subscriber service is running and accessible
3. Check filter pod logs: `kubectl logs -l app=natsjs-broker-filter`

### High latency or low throughput

1. Increase `CONSUMER_FETCH_BATCH_SIZE` (or the trigger annotation) to fetch more messages per round-trip
2. Increase `CONSUMER_MAX_CONCURRENCY` (or the trigger annotation) to allow more concurrent dispatches
3. Check NATS server health and connection

### Duplicate message delivery

If consumers are receiving the same event more than once, the most likely cause is that the subscriber is taking longer than `trigger.spec.delivery.timeout` (the consumer's AckWait) to respond. JetStream redelivers the message once AckWait expires. Increase the timeout or reduce subscriber latency.

### Events going to dead letter sink

1. Check subscriber logs for errors
2. Verify subscriber endpoint is correct
3. Review retry configuration
