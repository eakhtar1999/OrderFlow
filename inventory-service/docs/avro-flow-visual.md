# The Avro flow, visually — and a mental model to keep in your head

`avro-basics.md` explains the concepts and decodes a real (messy, error
path) log. This note is the calmer companion: one simple, happy-path
order, drawn out step by step, plus a mental model small enough to
actually remember.

## The one-sentence mental model

> **A Kafka message doesn't carry a dictionary — it carries a library
> card number. Schema Registry is the library holding the actual
> dictionary that number points to.**

That's it. Everything below is just that idea, worked out in detail.

## The cast, as an analogy

| In real life | In this system |
|---|---|
| A sealed envelope with only a tracking number written on the outside | An Avro-encoded Kafka message (magic byte + schema ID + packed bytes) |
| A shared library where "manual #1" tells you exactly how to unpack any envelope marked #1 | Schema Registry |
| The clerk who checks out (or registers) manual #1 before sealing the envelope | The producer, via `KafkaAvroSerializer` |
| The clerk who looks up manual #1 to unpack the envelope correctly | The consumer, via `KafkaAvroDeserializer` |
| The manual itself — a list of "field 1 is a string, field 2 is a number, in this order" | The `.avsc` schema file |

Nobody mails the manual with every envelope. That would be wasteful —
everyone already knows where the library is.

## One order, traced end to end

Say a customer places this order:

```
customerId: "cust-1"
region:     "us-east"
items:      [ { productId: "sku-42", quantity: 2 } ]
```

### Step 1 — order-service builds a plain Java object

Nothing Avro-specific yet. Just an object in memory:

```
OrderCreatedEvent {
  orderId:     "abc-123"
  customerId:  "cust-1"
  region:      "us-east"
  items:       [ {productId: "sku-42", quantity: 2} ]
  totalAmount: 19.98
  createdAt:   1785500000000
}
```

### Step 2 — the producer turns it into bytes

```
      Your Java object
             │
             ▼
┌──────────────────────────────────────────────┐
│  KafkaAvroSerializer                           │
│                                                 │
│  1. "What shape am I?"                         │
│      → reads OrderCreatedEvent.getSchema()      │
│                                                 │
│  2. "Does the library already have this exact   │
│      shape, for subject 'order-created-value'?" │
│      → asks Schema Registry over HTTP           │
│                                                 │
│  3. "Yes — it's manual #1."                     │
│      (or: "No — here, file this as manual #1"   │
│       if it's the very first time)              │
│                                                 │
│  4. Packs: [magic byte][schema id = 1][bytes]   │
└──────────────────────────────────────────────┘
             │
             ▼
     Kafka topic: order-created
```

### Step 3 — what actually goes on the wire

Not literal binary — just the *shape* of it:

```
┌──────┬─────────────┬────────────────────────────────┐
│ 0x00 │ 0x00000001  │  <tightly packed values, no      │
│      │             │   field names, in schema order>  │
├──────┴─────────────┴────────────────────────────────┤
│ magic │  schema id   │  "abc-123", "cust-1", "us-east", │
│ byte  │  = 1         │  [{"sku-42", 2}], 19.98, ...     │
└───────┴──────────────┴──────────────────────────────┘
```

No `"orderId":` anywhere in there. If you don't know it's manual #1,
these bytes mean nothing.

### Step 4 — the consumer turns bytes back into an object

```
     Kafka topic: order-created
             │
             ▼
┌──────────────────────────────────────────────┐
│  KafkaAvroDeserializer                         │
│                                                 │
│  1. Reads the first 5 bytes: magic=0x00, id=1  │
│                                                 │
│  2. "Library, what does manual #1 say?"        │
│      → asks Schema Registry (first time only —  │
│         cached locally after that)              │
│                                                 │
│  3. Gets back the WRITER's schema (what          │
│      order-service used to pack this)           │
│                                                 │
│  4. Already knows the READER's schema too —      │
│      it's baked into THIS jvm's generated        │
│      OrderCreatedEvent class                    │
│                                                 │
│  5. Reconciles writer vs. reader, field by       │
│      field (see below)                          │
└──────────────────────────────────────────────┘
             │
             ▼
   OrderEventListener.onOrderCreated(event, ack)
```

## What happens when the two schemas don't match exactly

This is the part that feels like magic until you see it drawn out. Say
`order-service` was upgraded and now writes a `giftMessage` field that
`inventory-service` doesn't know about yet:

```
   Writer's schema (v2)          Reader's schema (v1)
   ┌────────────────┐            ┌────────────────┐
   │ orderId         │            │ orderId         │
   │ customerId      │            │ customerId      │
   │ region          │            │ region          │
   │ items           │            │ items           │
   │ totalAmount     │            │ totalAmount     │
   │ createdAt       │            │ createdAt       │
   │ giftMessage NEW │            │  (never heard    │
   │                 │            │   of it)         │
   └────────────────┘            └────────────────┘
             \_______________  _______________/
                             \/
              Only the OVERLAP makes it into
              the object your code sees.
              giftMessage is silently dropped —
              not an error, just ignored.
```

Flip it the other way — an old writer, a new reader expecting a field
the old data doesn't have — and the reader's `default` value fills the
gap instead of blowing up. That's the whole trick behind "schema
evolution": **the two sides never need to match exactly, only be
compatible enough for the overlap to make sense.**

## The checklist Schema Registry runs on every new schema version

```
  A new schema shows up for a subject
              │
              ▼
   Does this subject already have
   a schema registered?
              │
        ┌─────┴─────┐
        NO           YES
        │             │
        ▼             ▼
   Register it    Check compatibility mode (default: BACKWARD)
   as version 1        │
                        ▼
              Can a consumer still running
              the PREVIOUS schema read data
              written with this NEW schema?
                        │
                  ┌─────┴─────┐
                 YES           NO
                  │             │
                  ▼             ▼
           Register as     REJECT.
           next version    Producer gets an error.
                            Nothing gets published.
```

This is exactly what you tested for real in Build Order Step 3: adding
`giftMessage` with a default sailed through this checklist; adding a
required field with no default hit the REJECT branch.

## The whole pipeline, one picture

```
┌──────────────┐        ┌───────────────────┐        ┌──────────────────┐
│ order-service │        │  Schema Registry    │        │ inventory-service  │
│              │        │  (the "library")     │        │                    │
│  builds an    │◄──────►│  order-created-value │◄──────►│  KafkaAvroDeserial- │
│  OrderCreated │ register│  = manual #1, #2...  │ lookup │  izer + your        │
│  Event object │  schema │                      │ schema │  generated class    │
│              │        └───────────────────┘        └──────────────────┘
│  KafkaAvro    │                                              ▲
│  Serializer   │                                              │
│              │────── [magic][id][bytes] ──────────────────►│
└──────────────┘         via Kafka topic: order-created        │
                                                          onOrderCreated(event, ack)
```

Both services talk to Schema Registry *directly*, independently — never
to each other. That's what makes this scale to a tenth consumer later
(fraud-detection-service, analytics-service, ...) with zero extra
coordination: each one just asks the same library for the same manual.

## If you remember only three things

1. **Messages carry an ID, not a dictionary.** The dictionary (schema)
   lives in Schema Registry, fetched once per ID, cached after that.
2. **Reader and writer don't need to match exactly** — only overlap
   enough to make sense. Avro reconciles the difference silently: extra
   writer fields get dropped, missing reader fields get defaulted.
3. **Schema Registry is a gatekeeper, not just storage.** Every new
   version gets checked against the compatibility mode before it's
   allowed to exist — that's the whole mechanism that replaced "hope the
   other team didn't change anything" from Steps 1-2.
