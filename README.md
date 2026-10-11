# Faultline

**Deterministic simulation testing for distributed systems, in your browser.**

Faultline runs a real Raft cluster inside a seeded discrete-event simulator, attacks it with crashes, process bounces, network partitions (including Jepsen-style bridge partitions), packet loss and reordering, and checks every execution two ways: Raft's safety invariants, and linearizability of every client operation. When something breaks it shrinks the failing run to a minimal scenario, explains it in plain English, and lets you scrub through it event by event. Every failure is a short string you can paste into the CLI or the web app to replay the exact same execution, bit for bit.

This is the testing technique behind FoundationDB, TigerBeetle and Jepsen, built from scratch with zero runtime dependencies.

```
$ faultline hunt --bug stale-read
✗ seed 6 after 6 seeds (0.08s): linearizability
  shrunk in 221 runs: faults 42 → 2, events 1987 → 630, clients 3 → 3

Key moments — 12 shown · 380 of 631 events are causally upstream of the violation
    178ms #61    n1 ← n2  Vote GRANTED t1
                 ↳ n1 becomes LEADER of term 1
    588ms #369   c0 ← n1  ok "c5.9"
    667ms #432   PARTITION {n1} | {n0 n2 n3 n4}
    820ms #454   n0 ← n3  Vote GRANTED t2
                 ↳ n0 becomes LEADER of term 2
   1124ms #599   CRASH n0
   1371ms #630   c0 ← n1  ok "c5.9" ◀ violation

linearizability: Stale read on "x". c1's write of "c6.10" was acknowledged at 1041ms, after c0's
write of "c5.9" had finished, and then c0's read started at 1155ms and still saw "c5.9".
No order of these operations that respects real time can explain that.
```

## Results

| | |
|---|---|
| Simulator throughput | ~730k–830k events/s on one core (Node 22) |
| Correct Raft soak | 30,000 seeds (20k five-node, 10k three-node), 70M events, 210k elections, 3.5M client ops checked linearizable, **0 violations** |
| Planted bugs found | 4 of 4 |
| Shrinking | typically 40+ faults → 1–3 faults |

| Planted bug | What it is | How it shows up | Typical first seed |
|---|---|---|---|
| `stale-read` | Leader serves reads locally without confirming leadership | Linearizability only. Every Raft invariant still holds. | ~6 |
| `forget-vote` | `votedFor` is not persisted across restarts | Two leaders in one term. Needs a bridge partition plus a crash at the right moment. | ~69 (rare, ~0.2% of seeds) |
| `no-uptodate-check` | Votes granted without the log up-to-date check | Leader missing committed entries | 1 |
| `truncate-always` | Followers truncate on every AppendEntries, not just on conflict | A delayed, reordered message deletes acknowledged entries | ~10 |

### It found a bug in its own checker

During the 30,000-seed soak, seed 1373 reported a Leader Completeness violation in the *correct* implementation. Shrinking and replaying showed a node legitimately winning a stale term (13) after term 14 had already committed more entries. Raft's Leader Completeness property only binds leaders of terms after the commit, and my checker was stricter than the paper. The checker is fixed and the shrunk repro is now a regression test (`test/sim.test.ts`). This is exactly the class of mistake the tool exists to catch.

## Try it

```bash
npm install
npm run build           # dist/cli.js and dist/faultline.html (single self-contained file)
npm test                # 20 tests, ~2s
node dist/cli.js soak --seeds 2000
node dist/cli.js hunt --bug forget-vote
node dist/cli.js replay <repro-string>
open dist/faultline.html
```

The web app is one 87 KB HTML file with no server and no network calls. Hunting runs in a Web Worker so the UI stays live.

## How it works

```
src/
  rng.ts              mulberry32 PRNG with independent derived streams
  heap.ts             binary heap event queue, ordered by (time, sequence)
  raft.ts             Raft: elections, replication, commit, dedup KV state machine, 4 bug switches
  client.ts           closed-loop clients with retries and redirects; record the history
  sim.ts              discrete-event simulator: network, timers, crashes, partitions, trace recorder
  faults.ts           nemesis schedule generator (crash/bounce/partition/bridge/heal)
  invariants.ts       online checks: Election Safety, Leader Completeness, State Machine Safety
  linearizability.ts  Wing & Gong search with Lowe's memoization, per key; binary search for the first bad op
  shrink.ts           trim to violation, ddmin over faults, drop clients, disable network chaos
  causal.ts           causal slice over the event graph
  story.ts            picks key moments and writes the explanation
  repro.ts            scenario <-> base64url string
ui/                   web app (vanilla TS, SVG) + hunting worker
```

**Determinism.** The node code can only touch the world through an `Env` interface: send, set a timer, persist, read the clock, draw a random number. The simulator owns all of it. There is no wall clock, no real I/O and no shared mutable state, so a scenario `(seed, config, fault schedule)` fully determines the execution. Tests assert bit-identical replays.

**Stable randomness.** Every process and every directed network link has its own RNG stream derived from the seed. Removing one fault perturbs only the traffic it actually touches, which is what makes delta debugging converge instead of turning every edit into a different execution.

**Linearizability.** Clients record call and return points. The checker searches for a sequential order consistent with real time and register semantics; pending writes may or may not take effect, pending reads are dropped. Because linearizability is prefix-closed, a binary search over return points finds the exact operation whose completion made the history impossible.

**Causal slice.** Each event records two parents: the event that sent the message it handles, and the last event that actually changed its process's state. Heartbeats that changed nothing do not propagate causality, so the slice is about half the run instead of nearly all of it. On top of that, `story.ts` picks the handful of moments a human needs: faults, elections, the votes or commits in question, and the operations the anomaly hinges on.

**Shrinking.** Cut the run at the violation (safe because client behaviour never depends on the run length), then Zeller's ddmin over the fault list, then try fewer clients and no random drops or delay spikes, repeating until nothing changes. The result must still produce the same class of failure (safety vs. linearizability).

## Demo script (90 seconds)

1. Open the app. It starts on a real stale-read failure. Read the red verdict out loud.
2. Point at the cluster: two nodes both think they lead, the partitioned link is dashed red.
3. Press ⏮ then Play. Key moments mode skips quiet stretches: election, partition, new leader, crash, stale read.
4. Point at the client history: the red read overlaps nothing that could justify its value.
5. Select "votedFor not persisted" and hit **Hunt**. It fuzzes thousands of seeds per second, finds a double vote, shrinks it, and you replay it.
6. Select "Correct Raft" and hunt: thousands of seeds, clean.
7. Copy the repro string, paste it into `faultline replay` in a terminal: same execution, same verdict.

## Limitations and next steps

- One system under test (Raft with a KV state machine). The simulator and checkers are generic over the `Env` interface, but a second protocol (e.g. chain replication or a Paxos variant) has not been written yet.
- Single-threaded event handlers; no modelling of disk latency, fsync loss or clock skew yet. Torn writes and lost fsyncs would be a natural next nemesis.
- The linearizability checker is exponential in the worst case. It has a step budget and reports "inconclusive" instead of hanging; in 3.5M checked operations it never hit the budget.
- Shrinking is 1-minimal over faults, not globally minimal. The rare `forget-vote` bug sometimes keeps ~9 faults because its timing is fragile.
- The UI is tuned for desktop; it works on phones but the timeline is small.

## License

MIT
