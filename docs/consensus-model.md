# Consensus model (MVP)

## Roles

- A **control-plane voter** serves the client API on its host and participates
  in cluster consensus with a configured positive integer quorum weight. A
  client may send a request to any control-plane voter; there is no single
  client-facing API leader.
- A **worker** runs an execution agent and Xen `libxl` operations. It does not
  vote and its weight is zero by exclusion from the voter configuration.
- Workload placement weights are out of scope until a scheduler exists.

## Weighted quorum

When a control-plane server is added, its positive integer quorum weight is
specified as part of that explicit cluster API operation. For a fixed
membership configuration, let `W` be the sum of all configured voter weights.
A quorum is a set of distinct configured voters whose weight is at least
`floor(W / 2) + 1`. The same voter configuration and quorum rule must be used
for elections and log-commit acknowledgements. Acknowledgements from unknown
members do not count.

Weights are stable configuration, not health or availability scores. They do
not change automatically when a server is slow, offline, or unreachable. A
weight change is allowed only in response to an explicit API update request;
the update must identify the target server and its new positive weight. The
request is a cluster-state command and must be serialized through consensus.

The configuration is immutable once constructed. To change weights or voters,
the consensus layer must enter a joint configuration that requires a quorum
under both the old and new weighted memberships, then activate the new
configuration. The configuration must not be recomputed from currently
reachable nodes: network partitions must not lower the quorum threshold
independently on each side.

Weights determine voting influence and failure tolerance, not workload
placement. A high-weight node can form a quorum alone if its weight exceeds
half of the cluster total, so operators must choose weights with that failure
mode in mind.

## Implementation boundary

`src/cluster/weighted_quorum.rs` implements membership validation, static
weighted-majority calculation, explicit add/update/remove commands, and the
joint quorum predicate for old/new configurations. `src/cluster/membership_state.rs`
exposes `MembershipLogEntry` and `MembershipStateMachine` for deterministic
application of committed transitions. `src/cluster/membership_commit.rs`
tracks acknowledgements for membership records, validates that each ack comes
from a voter in the applicable configuration, and advances the commit index
only over a contiguous prefix whose records meet their required quorum. It
allows one outstanding membership record at a time, so the next transition is
derived only from an already committed configuration. Both entering and
leaving joint mode currently require a joint quorum.

`src/cluster/paxos.rs` implements the prepare/accept round for one slot. Any
voting node can be a proposer; there is no elected or fixed API leader. A
proposer first collects promises from a weighted quorum, preserves the value
from the highest previously accepted ballot if one exists, then gathers a
weighted accept quorum. The resulting `ChosenValue` carries the acceptor IDs as
a quorum certificate that other nodes can verify before learning the value.
During a membership transition the same operations use the joint quorum rule.
Ballot rounds must be unique per proposer and durably advanced by the caller.

These are consensus-facing primitives, not a complete cluster runtime or HTTP
endpoints. `src/cluster/paxos_storage.rs` provides a node-local SQLite store in
WAL mode with `synchronous=FULL`. The durable acceptor methods commit promise,
accepted, and learned state before returning; the store also advances each
proposer's ballot counter transactionally. SQLite's `FULL` setting synchronizes
the WAL at transaction commit, subject to the filesystem and storage device
honoring sync requests. The membership tracker and application/commit index are
not persisted yet.

A complete runtime still has to coordinate and recover multiple slots in
order, apply committed values, authenticate and transport RPCs, and expose the
API on every control-plane node. Client requests may enter on any such node.
Worker execution and `libxl` integration are also not implemented.
