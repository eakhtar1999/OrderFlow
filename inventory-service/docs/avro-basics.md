# Avro basics — annotated from a real poison-message run

This note starts from zero — what Avro actually *is*, why it needs
Schema Registry at all — then walks your exact captured log from placing
a `sku-poison` order, section by section, pointing at which concept each
line is evidence of. Read top to bottom once for the concepts, then use
it as a reference to decode future logs.

If you haven't yet, skim `avro-schemas/order-created.avsc` and
`inventory-service/config/KafkaConsumerConfig.java` alongside this — the
concepts below map directly onto specific lines in both.

## 1. What Avro actually is

Avro is two things bolted together:

1. **A schema definition language** — `.avsc` JSON files like
   `avro-schemas/order-created.avsc`, describing field names and types.
2. **A binary encoding** that, given a schema, packs values as tightly as
   possible — no field names, no delimiters, no punctuation. Just bytes,
   in the order the schema says to expect them.

That second part is the whole point, and also the whole complication.
Compare to JSON: `{"orderId":"abc","quantity":2}` is **self-describing**
— you can read those bytes with zero outside context and know exactly
what they mean. Avro-encoded bytes for the same data are just something
like `03 61 62 63 04` — meaningless without the schema that produced
them. **Avro trades self-description for size and speed, and pays for it
by needing the schema supplied separately, out of band.**

That "out of band" part is Schema Registry's entire job.

## 2. What's actually on the wire (Confluent's framing)

A Kafka message value produced by `KafkaAvroSerializer` isn't pure Avro
bytes — Confluent wraps it:

```
[ magic byte: 0x0 ] [ schema ID: 4 bytes, big-endian int ] [ Avro-encoded payload ]
```

The **schema ID** is the load-bearing part. It's an integer Schema
Registry assigns when a schema is registered — every message says "I was
written using schema #7," not "here is my schema." A consumer that
doesn't already know what schema #7 looks like has to go ask Schema
Registry.

## 3. Schema Registry's role, and the setting that explains your WARN line

Schema Registry stores every version of every schema, organized by
**subject**. Two settings from your own log's `KafkaAvroSerializerConfig`
dump directly control how that subject name gets picked and how new
schemas get registered:

```
key.subject.name.strategy = class io.confluent.kafka.serializers.subject.TopicNameStrategy
value.subject.name.strategy = class io.confluent.kafka.serializers.subject.TopicNameStrategy
auto.register.schemas = true
```

- **`TopicNameStrategy`** derives the subject name straight from the
  Kafka topic name: `<topic>-value`. This is *exactly* why
  `order-created-retry-0`, `-retry-1`, `-retry-2`, and `-dlt` each ended
  up with their own separate schema subject (`order-created-retry-0-value`,
  etc.) even though every one of them carries "the same" event —
  republishing to a differently-named topic always means a
  differently-named subject under this strategy.
- **`auto.register.schemas = true`** (the default — we never set this
  explicitly anywhere) is why nobody ever ran a manual "register this
  schema" command. The very first time `KafkaAvroSerializer` needed to
  send a message whose schema wasn't in Schema Registry yet, it just
  registered it itself, on the spot, and got a schema ID back.

## 4. The producer-side flow

Every time `OrderEventProducer` (or, as in your log, the retry-topic
machinery) sends an Avro record:

1. `KafkaAvroSerializer.serialize()` reads the schema straight off the
   Avro object itself (every generated class has a `.getSchema()`
   method — no separate lookup needed for *what* the schema is).
2. It checks a local in-memory cache: "have I already resolved a schema
   ID for this exact schema, for this subject?" First time, no.
3. It calls Schema Registry: "does a schema exactly matching this exist
   under subject `X`?" If yes, reuse that ID. If no — and
   `auto.register.schemas=true` — **register it now**, get a new ID back.
4. It prepends `[magic byte][schema ID]` to the Avro-encoded bytes and
   hands the whole thing to the underlying `KafkaProducer` to send.

## 5. The consumer-side flow

`KafkaAvroDeserializer`, on receiving bytes:

1. Reads the magic byte + schema ID straight off the front.
2. Calls Schema Registry: "give me schema ID `N`" (cached after the first
   lookup — not a round trip per message).
3. That's the **writer's schema** — whatever the producer actually used.
4. With `specific.avro.reader=true` (set in `KafkaConsumerConfig.java`),
   the deserializer also knows the **reader's schema** — whatever's
   compiled into *this* JVM's generated `OrderCreatedEvent` class.
5. Avro's **schema resolution** reconciles the two, field by field: a
   field the writer has that the reader doesn't ask for gets silently
   dropped; a field the reader wants that the writer didn't send gets
   filled from its `default`. This exact mechanism is what made the live
   schema-evolution test in Build Order Step 3 work — an old-compiled
   consumer reading a message from a newer schema without any code
   change.

---

## 6. Your actual log, section by section

### `📥 Received order-created orderId=91d607ab...`

By the time this line prints, **Avro deserialization has already
succeeded** — the main listener wouldn't have a usable `OrderCreatedEvent`
object to read `.getCustomerId()` from otherwise. If deserialization had
failed (corrupt bytes, unknown schema ID), you'd see a
`SerializationException` here instead, before this log line, the same
way we saw one for real back in Step 3 when the `Instant` field wasn't
supported yet.

### The WARN: non-existent partition

```
WARN ... DeadLetterPublishingRecoverer : Destination resolver returned
non-existent partition order-created-retry-0-2, KafkaProducer will
determine partition to use for this topic
```

This is Spring Kafka trying to be helpful and hitting a wall we
predicted back in Step 4's testing. It attempted to republish the failed
record to **the same partition number it came from** (partition 2 of
`order-created`) — a nice property when it works, since it preserves
per-partition ordering across the retry hop. But `order-created-retry-0`
only has **1 partition** (partition 0 — remember, retry/DLT topics
default to 1 partition regardless of the main topic's 3). Partition 2
doesn't exist there, so the resolver gives up and falls back to Kafka's
default partitioner (hash-of-key, since the record has a key) instead.
**Harmless** — the message still gets published — but it's the concrete,
observable consequence of a trade-off documented in
`OrderEventListener.java`'s concept footer, not a new problem.

### The giant `ProducerConfig values:` dump

This whole block is one thing happening: **a producer being created for
the first time**, lazily, right when it's first needed — the moment this
service actually has to republish a failed record. `inventory-service`
had zero producers active from startup through every successful order;
this is the very first time it needed one.

A few fields worth clocking:
- `client.id = inventory-service-producer-1` — Spring auto-generated an
  identity for it since we never configured one explicitly (contrast
  with the consumer side, where we deliberately generate `clientId`
  ourselves in `KafkaConsumerConfig.java` — this producer never got that
  treatment, since it's Spring's retry-topic infrastructure creating it
  under the hood, not our own code).
- `enable.idempotence = true` and `acks = -1` (i.e. `all`) — this
  producer inherited the same durability posture order-service's
  producer was deliberately configured with, except here it's just the
  *client library's own default* since Kafka 3.x, not something we set.
- `transactional.id = null` — plain idempotent producer, no transactions
  yet (that's Build Order Step 5).

### The `KafkaAvroSerializerConfig values:` dump

This is the layer *on top of* the raw producer config — Avro-specific
settings, all defaults we never touched:
```
auto.register.schemas = true
use.latest.version = false
value.subject.name.strategy = class ...TopicNameStrategy
schema.registry.url = [http://localhost:8085]
```
`use.latest.version = false` is worth understanding: it means this
producer doesn't just blindly grab "whatever the latest registered
schema happens to be" — it registers (or reuses) the schema matching the
*actual Java object* it's serializing. In a real production setup, a
common pattern flips this: disable `auto.register.schemas`, set
`use.latest.version = true`, and register schemas through a separate
CI/CD step instead of letting every producer auto-register on the fly.
We're using the beginner-friendly default (auto-register everything) —
worth knowing it's a deliberate simplification, not the only way.

### `These configurations '[schema.registry.url]' were supplied but are not used yet.`

Looks alarming, isn't. `schema.registry.url` is an Avro-serializer-level
property, not a core Kafka `ProducerConfig` one — the raw
`ProducerConfig` validation layer doesn't recognize it (because it's
consumed one layer up, by `KafkaAvroSerializerConfig`, which you can see
DOES list it in its own dump right below). This exact "supplied but not
used yet" line shows up in essentially every Confluent Avro Kafka client
you'll ever run — safe to ignore.

### `Instantiated an idempotent producer` / `ProducerId set to 2 with epoch 0`

The idempotent-producer handshake: the broker hands this producer a
unique `(ProducerId, epoch)` pair, which it stamps on every batch it
sends from now on. If a network blip makes this producer retry a send,
the broker recognizes the duplicate `(ProducerId, epoch, sequence
number)` and drops it instead of double-writing — the same mechanism
`order-service`'s producer relies on, explained in its own
`application.yml` comments.

### The three retry-topic lines

```
Seeking to offset 0 for partition order-created-retry-0-0
📥 Received order-created orderId=91d607ab... (retry-0)
Seeking to offset 0 for partition order-created-retry-1-0
📥 Received order-created orderId=91d607ab... (retry-1)
Seeking to offset 0 for partition order-created-retry-2-0
📥 Received order-created orderId=91d607ab... (retry-2)
```

Each of these is the poison message, **still fully valid Avro**,
travelling through a different topic and a different consumer group each
time, hitting `onOrderCreated` again, throwing again — Avro deserialization
succeeds at every single hop; the failure is 100% business logic
(`POISON_PRODUCT_ID` check), never a serialization problem. Worth
noticing: nothing about Avro or Schema Registry is even aware retries are
happening — from their point of view these are just three more ordinary
produce/consume cycles.

### `DeadLetterPublishingRecovererFactory : ... won't be retried. Sending to DLT`

Spring's own bookkeeping: it's tracked that this record has now failed
`attempts` (4) times total and decides to route it to the DLT instead of
trying again. This line is Spring's decision log — distinct from, and
happening just before, our own `@DltHandler`'s log line.

### The exception stack trace

Strip the noise and it's one straight line, wrapped in layers:

```
KafkaMessageListenerContainer (Spring's polling loop)
  → MessagingMessageListenerAdapter (Spring's method-invocation glue)
    → OrderEventListener.onOrderCreated(...)   <- OUR CODE
      → throws RuntimeException("Simulated processing failure...")
```

Every frame above `OrderEventListener.onOrderCreated` in the trace is
Spring's own machinery re-wrapping the exception as it propagates back up
(`ListenerExecutionFailedException`, `TimestampedException`) so the
retry-topic infrastructure can inspect *when* and *what* failed. The
`Caused by:` at the very bottom is the only line that's actually "ours."

### `💀 DEAD LETTER: ... exhausted all retry attempts — Listener failed; Simulated processing failure...`

Our own `@DltHandler` method, finally running. The message text —
`"Listener failed; Simulated processing failure for order..."` — is
Spring concatenating the wrapper exception's own message ("Listener
failed") with our cause's message, exactly what
`@Header(KafkaHeaders.EXCEPTION_MESSAGE)` hands you. The event object
itself deserialized via Avro perfectly cleanly the whole way through —
this entire saga was a business-logic failure wearing an Avro pipeline,
not an Avro failure at all.

---

## Quick reference: Avro vs. plain JSON (Steps 1-2 vs. Step 3+)

| | JSON (Steps 1-2) | Avro (Step 3+) |
|---|---|---|
| Self-describing on the wire? | Yes — field names in every message | No — just a schema ID + packed bytes |
| Needs a registry? | No | Yes (Schema Registry) |
| Contract enforcement | None — hand-copied DTOs, drift possible | Registry rejects incompatible changes |
| Consumer needs to know target type how? | `JsonDeserializer.VALUE_DEFAULT_TYPE` | `SPECIFIC_AVRO_READER_CONFIG=true` |
| Message size | Larger (field names repeated every message) | Smaller (no field names on the wire) |
| A type-instantiation attack surface? | Yes — needed `TRUSTED_PACKAGES` | No — messages carry a schema ID, never a class name |

## Mini self-check

1. If `auto.register.schemas` were `false` and Schema Registry had never
   seen this schema before, what would happen when order-service tried to
   publish the very first order? *(It would fail — the serializer has
   nothing to fall back to without either auto-registration or a
   pre-registered schema matching exactly.)*
2. Why did `order-created-retry-0-value` end up as a completely separate
   subject from `order-created-value`, instead of Schema Registry
   recognizing "this is the same event"? *(`TopicNameStrategy` derives
   the subject purely from the topic name — it has no concept of "this
   is logically the same event republished elsewhere.")*
3. Does the `WARN` about "non-existent partition" mean the message failed
   to publish to the retry topic? *(No — it's a fallback notice. The
   record still gets published, just via Kafka's default partitioner
   instead of the partition-preserving one Spring tried first.)*
