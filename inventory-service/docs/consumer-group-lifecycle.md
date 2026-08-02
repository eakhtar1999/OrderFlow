# Deep dive: consumer group lifecycle, with orders flowing continuously

This note walks the **full lifecycle** of `inventory-service`'s consumer
groups — instance A starting alone, instance B joining, instance B dying,
instance B rejoining — while order-service keeps publishing the entire
time. It combines two live investigations (Build Order Step 2's scaling
walkthrough and a follow-up on B rejoining) into one staged reference,
plus an interview-style quiz at the bottom to test whether you actually
internalized it or just skimmed it.

Read this after you've run the root README's Step 2 walkthrough yourself
at least once — it'll mean a lot more with real log lines of your own
next to it.

## The cast of components

| Component | What it does |
|---|---|
| **Heartbeat thread** | Runs in the background of every `KafkaConsumer`, separate from the polling thread. Pings the Group Coordinator every `heartbeat.interval.ms` (default 3s). |
| **Group Coordinator** | One specific broker per `group.id`, determined by hashing `group.id` onto a partition of the internal `__consumer_offsets` topic. Owns that group's membership state and offset commits. In our single-broker docker-compose setup, it's trivially always the one broker — this distinction matters far more once you have a real multi-broker cluster. |
| **`session.timeout.ms`** | How long the coordinator waits without a heartbeat before declaring a member dead (default 45s on `kafka-clients` 3.7.1 — never overridden in this project). |
| **`JoinGroupRequest`** | How a consumer announces "I want in" (or "I'm still here, membership changed") to the coordinator. |
| **Group leader** | One member per generation, chosen by the coordinator, responsible for *computing* the partition assignment — client-side, not broker-side. |
| **Partition assignor** | The algorithm the leader runs. This project uses the default `RangeAssignor` (`protocol='range'` in your logs) — confirmed eager, stop-the-world rebalancing, not `CooperativeStickyAssignor`. |
| **`SyncGroupRequest`** | How the coordinator distributes the leader's computed assignment back to every member individually. |
| **Generation** | A per-group, monotonically increasing counter, bumped every time group membership changes. **Each of our 5 groups has its own independent generation counter** — seeing `generationId=6` and `generationId=8` close together in your logs almost certainly means two *different* groups, not one group rebalancing twice. |
| **`memberId`** | A coordinator-issued identity, ephemeral by default. Persists across rebalances *while the consumer stays alive and in the group* — but a brand-new JVM (a restart) always gets a brand-new, unrelated `memberId`. Only static group membership (`group.instance.id` — deliberately NOT enabled in this project, see `KafkaConsumerConfig.java`) would let a restarted instance reclaim its old identity. |
| **Committed / current offset** | Per-partition, per-group pointer in `__consumer_offsets`, advanced only by `acknowledgment.acknowledge()` (our `MANUAL_IMMEDIATE` ack mode). |
| **End offset (LEO)** | Per-partition, broker-side counter of how many records have ever been appended — advances the instant a produce succeeds, independent of any consumer. |
| **Lag** | `End offset − Committed offset`. What Kafka UI shows per partition/group. |

One fact that shapes almost everything below: since Build Order Step 4,
every `inventory-service` JVM runs **5 separate consumers in 5 separate
groups**, all subscribed from the moment the app starts — not just the
main listener:

| Container | group.id | partitions |
|---|---|---|
| main | `inventory-service-group` | 3 (order-created) |
| retry-0 | `inventory-service-group-retry-0` | 1 |
| retry-1 | `inventory-service-group-retry-1` | 1 |
| retry-2 | `inventory-service-group-retry-2` | 1 |
| DLT | `inventory-service-group-dlt` | 1 |

Everything staged below happens **five times in parallel**, every time.

---

## Stage A — instance A spins up alone

1. Spring builds all 5 `KafkaConsumer` objects. Each registers a JMX
   mbean keyed by `client.id` for metrics. **All 5 share the exact same
   `client.id` string** (generated once in `KafkaConsumerConfig.clientId`
   and reused across every container) — Kafka's client auto-appends a
   `-0`, `-1`, ... suffix to keep mbean names unique when it sees repeats,
   and that auto-suffixing occasionally races when 5 consumers spin up in
   the same JVM within milliseconds of each other:
   ```
   The mbean of App info: [kafka.consumer], id: [inventory-instance-...-0]
   already exists, so skipping a new mbean creation.
   ```
   **This is not a rejoin-specific quirk** — it happens on the very first
   startup too, for the same root cause (5 containers, 1 shared
   `client.id`), not because of any prior death or restart.
2. Each consumer fetches cluster metadata, discovers its group's
   coordinator (`FindCoordinatorRequest`).
3. `JoinGroupRequest` ×5 — brand-new groups, generation 1, A is the only
   member so it's trivially the leader.
4. `RangeAssignor` computes: A gets ALL of order-created's 3 partitions,
   and the sole partition of each retry/DLT topic too.
5. `SyncGroupRequest` completes, `onPartitionsAssigned` fires for
   everything A owns.
6. **Offset positioning**: none of these 5 groups have ever committed
   anything — `AUTO_OFFSET_RESET_CONFIG=earliest` applies to every
   partition, current offset starts at the oldest retained record (0, in
   a fresh topic).
7. Orders start flowing. A processes and acks each one; committed offset
   climbs order-created's partitions in step with end offset. Lag stays
   ~0 in steady state — A is faster than the order rate.

## Stage B — instance B joins (orders still flowing)

1. B's 5 consumers start; same mbean-collision note applies *within B's
   own JVM* — nothing to do with A.
2. B sends `JoinGroupRequest` for all 5 groups, gets a brand-new,
   randomly-issued `memberId` for each — no relationship to any identity
   it might have had in a previous life (there was no previous life; this
   is Stage B, first time B has ever existed).
3. **Any new member joining forces a full rebalance of that group** —
   this disrupts A too, even though A did nothing wrong. Eager
   rebalancing means A gives up ALL its partitions in that group
   momentarily, not just the ones about to move.
4. Leader (whichever member the coordinator processes first this
   generation) recomputes assignment from scratch:
   - order-created (3 partitions, 2 members): typically a 2-1 split.
   - Each retry/DLT topic (1 partition, 2 members): only ONE of A/B gets
     it — `RangeAssignor` can't split a single partition. The loser sits
     completely idle for that specific group, while staying fully active
     on order-created.
5. `SyncGroupRequest` completes for both. Every partition — including the
   ones A keeps — gets a brief `REVOKED` → `ASSIGNED` cycle on A, plus a
   fresh `ASSIGNED` on B.
6. **Offset positioning**: order-created's partitions already have real
   committed history from Stage A — both A and B resume from committed
   offsets, not earliest. Retry/DLT partitions, if nothing has ever
   failed yet, still have zero commits — whichever of A/B ends up owning
   them hits the earliest-reset path:
   ```
   Resetting offset for partition order-created-dlt-0 to position FetchPosition{...}
   ```
   This line means "this group has never committed anything on this
   partition" — it is not evidence of anything B-specific.
7. Orders flowing throughout: a brief lag blip during the handshake
   (sub-few-seconds, nothing like the death-detection delay below),
   drained within moments once both resume polling.

## Stage C — instance B is killed (`kill -9`), orders still flowing

1. B's process dies instantly. No `LeaveGroupRequest` is sent — contrast
   with a graceful Ctrl-C, which would notify the coordinator immediately.
2. The coordinator has no way to know yet. It keeps waiting for B's
   heartbeat. **Detection takes up to `session.timeout.ms`** (default
   45s) — in our own test run, the observed gap was ~90 seconds
   (heartbeat timeout plus scheduling slack).
3. **During this undetected-death window**: any order landing on a
   partition B owned (in ANY of the 5 groups it happened to own a slice
   of) still gets appended fine — end offset climbs — but nothing is
   consuming it. Committed offset is frozen. **Lag grows silently**, with
   no visible sign anything's wrong beyond that number climbing in Kafka
   UI.
4. Once the timeout expires, the coordinator kicks B out, bumps the
   generation, triggers a full eager rebalance across every affected
   group. A briefly pauses everywhere (again — eager, stop-the-world),
   then gets B's orphaned partitions folded back in:
   ```
   🔀 partitions REVOKED (graceful) -> [order-created-0, order-created-1]
   🔀 partitions ASSIGNED -> [order-created-0, order-created-1, order-created-2]
   ```
   **Not `LOST`** — `onPartitionsLost` only fires on a consumer's own
   client when *it* gets fenced out before revoking cleanly. A peer dying
   is invisible to the survivor except as an ordinary membership change.
5. **Offset positioning**: A resumes each reclaimed partition from
   B's *last committed* offset — not from wherever B's in-memory
   processing had actually gotten to. If B was mid-processing a record
   (already polled, maybe even already called `tryReserve()`) when killed
   but hadn't yet called `acknowledge()`, that record was never
   committed — A re-fetches and reprocesses it. Since `tryReserve()`
   isn't idempotent, this is the concrete mechanism behind the
   "reprocessing could double-decrement stock" gap called out in
   `OrderEventListener`'s comments (closed properly by Step 9's Redis
   dedupe store, not yet).
6. A works through the backlog that piled up during the undetected-death
   window — lag drains back toward 0 as it catches up.

## Stage D — instance B restarts and rejoins (orders still flowing)

Mechanically, this is **Stage B again** — B's JVM is brand new, and Kafka
has zero memory of the B that died. The one thing worth being explicit
about: **it is not a resumption of B's old identity.**

1. Same mbean-collision note fires again — unrelated to the prior death,
   same root cause (5 containers, 1 shared `client.id`, this JVM).
2. B gets an entirely new, randomly-issued `memberId` per group — nothing
   ties it back to the `memberId` it had before it died. That old
   `memberId` is simply gone; the coordinator already forgot it the
   moment the session timeout expired in Stage C.
3. Full eager rebalance again, across all 5 groups, disrupting A exactly
   as before.
4. **`RangeAssignor` sorts members by `memberId` string** to compute
   contiguous ranges. Since B's new `memberId` is an unrelated fresh
   value, **there is no guarantee B gets back the same partitions it had
   before dying.** The 2-1 split could easily flip to 1-2, or land
   completely differently on the retry/DLT topics' single partitions.
5. Offset positioning: same rule as always — resume from committed where
   it exists (order-created definitely has real history by now), fall
   back to earliest only for groups/partitions that have never committed
   anything (typically still just the untouched retry/DLT topics).
6. Orders flowing throughout: same brief pause-and-drain pattern as every
   other rebalance in this document.

---

## Summary across all 4 stages

| Stage | Trigger | Detection speed | Partition split | Offset start point |
|---|---|---|---|---|
| A alone | app startup | — | A owns everything | `earliest` (no history) |
| B joins | `JoinGroupRequest` | near-instant | recomputed from scratch | committed where it exists, else `earliest` |
| B killed | missed heartbeats | up to `session.timeout.ms` (~45s; ~90s observed) | A reclaims everything | resumes from **B's last commit**, not B's in-memory progress |
| B rejoins | `JoinGroupRequest` (new memberId) | near-instant | recomputed from scratch, **not guaranteed to match pre-death split** | committed where it exists, else `earliest` |

---

## 🧠 Interview Q&A — twisted questions designed to confuse you

**Q1. If B dies and A ends up owning all 3 partitions, does A ALWAYS pick
up right where B left off, with zero reprocessing?**
🎯 *The trap*: assuming "committed offset" means "everything B actually
finished." **A:** No — A resumes from B's *last committed* offset, which
can be behind B's actual in-memory progress. Any record B had polled but
not yet acknowledged gets redelivered to A and reprocessed from scratch.

**Q2. `AUTO_OFFSET_RESET_CONFIG` is `earliest`. When B rejoins after being
killed, does it replay the entire topic from offset 0?**
🎯 *The trap*: confusing "earliest" with "always resets to earliest."
**A:** No — `earliest` only applies when there's *no committed offset at
all* for that group+partition. Once a group has any commit history
(which order-created will, almost immediately), every rejoin resumes
from the committed offset, not from the beginning.

**Q3. Is the Group Coordinator the same broker as the leader of the
order-created partitions?**
🎯 *The trap*: assuming "coordinator" is a topic-data role. **A:** Not
necessarily — the coordinator is whichever broker leads the specific
`__consumer_offsets` partition that `group.id` hashes to, entirely
unrelated to who leads order-created's own partitions. They can be
different brokers in a real cluster (in our single-broker setup they're
trivially the same one).

**Q4. If B restarts and rejoins, will it get back the same partitions it
owned before it died?**
🎯 *The trap*: assuming Kafka "remembers" B. **A:** No guarantee at all —
B gets an entirely new, randomly-issued `memberId` on restart (no
identity persistence without static group membership), and
`RangeAssignor` computes assignment by sorting on `memberId`. The split
after rejoin can differ completely from the split before death.

**Q5. Retry topics have 1 partition each. With both A and B running, do
they process retries in parallel?**
🎯 *The trap*: assuming scaling the main topic scales everything.
**A:** No — a 1-partition topic can only ever be owned by ONE consumer in
a group at a time. Whichever of A/B doesn't get it sits fully idle *for
that specific group*, even while fully active on order-created's 3
partitions.

**Q6. During the undetected-death window (before the 45s timeout fires),
does the committed offset for B's orphaned partition roll backward at
all?**
🎯 *The trap*: assuming a "failure" undoes progress. **A:** No — offsets
never roll back automatically. The committed offset simply stays frozen
exactly where B last left it. Nothing is undone; new messages just pile
up as growing lag on top of that frozen point.

**Q7. Does "current offset" refer to the offset of the last message that
was actually processed?**
🎯 *The trap*: an off-by-one that trips up almost everyone at some point.
**A:** No — committed/current offset is "the next offset this group
should read," i.e., **one past** the last successfully processed record,
not the offset of that record itself.

**Q8. Suppose B isn't truly dead — just paused by a long GC, and it
"wakes up" after the coordinator has already reassigned its partitions to
A. Can B still commit an offset and corrupt A's progress?**
🎯 *The trap*: assuming only `kill -9` matters. This is the real "zombie
consumer" scenario `kill -9` can't demonstrate (a killed process can't
wake back up). **A:** B will try, tagging its request with its OLD
generation number — the coordinator rejects it (fenced out,
`IllegalGenerationException`/similar), because the group has already
moved to a new generation. B's already-fully-processed work before the
freeze isn't undone, but its attempt to commit past the fence point is
rejected — preventing it from silently overwriting A's newer progress.
This generation-fencing mechanism is exactly what `onPartitionsLost`
(see `KafkaConsumerConfig.java`) is designed to surface on the fenced
consumer's own side.

**Q9. The mbean "already exists, skipping" warning — is that something
specific to B rejoining after a crash?**
🎯 *The trap*: pattern-matching "weird log line near a rejoin" to "caused
by the rejoin." **A:** No — it happens on every single startup of any
instance, first-ever or the tenth restart, because all 5 listener
containers in one JVM share the same `client.id`. It's a same-JVM,
same-startup collision, unrelated to any prior death.

**Q10. With 2 instances and 3 order-created partitions, is the 2-1 split
always the SAME two partitions on the SAME instance, every single time
you restart the cluster?**
🎯 *The trap*: assuming determinism where there is none. **A:** No —
`RangeAssignor`'s output depends on sorting randomly-generated
`memberId`s. Which physical instance ends up with 2 partitions versus 1,
and *which* partitions specifically, is not something the application
controls or can predict — verify with Kafka UI, don't assume.
