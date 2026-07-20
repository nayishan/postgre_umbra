# Umbra FPW-to-Remap Design Story

[Chinese](./UMBRA_FPW_STORY_ZH.md)

## 1. Background: Which FPW Cost Umbra Targets

PostgreSQL currently relies on full-page writes (FPW) for crash-recovery
correctness.  The basic rule is that after a checkpoint, the first update to a
page logs a full-page image into WAL and uses that image as the new recovery
baseline.

Umbra does not try to remove every full-page image.  The current implementation
targets the ordinary checkpoint-boundary image path.  For ordinary data-page WAL
records that satisfy the automatic remap conditions, Umbra replaces the default
full-page image with remap-aware recovery metadata.  The following conservative
paths still keep image ownership:

- `REGBUF_FORCE_IMAGE`
- `XLR_CHECK_CONSISTENCY`
- auxiliary-fork and unconverted `XLOG_FPI_FOR_HINT` callers

So the more accurate goal is:

- do not claim that all FPIs disappear
- provide an alternative recovery-baseline representation for the ordinary
  checkpoint-boundary case

This is worth exploring because the stock `md` ordinary checkpoint-boundary
image path repeatedly binds several costs together:

- the first-dirty path after a checkpoint introduces an extra owner path, but
  the more important point is the I/O cost behind it
- WAL grows substantially because of full-page images, increasing WAL write and
  sync pressure
- data-file write amplification and WAL-side write amplification stack together
- for update-heavy workloads, this I/O pressure repeats in every checkpoint
  interval

Umbra is not a local tweak to that path.  It tries to take over the path with a
different way to express the recovery baseline.

## 2. Current Scope, Non-Goals, and Terminology

This section fixes the scope, non-goals, and terminology.

Current scope:

- use remap-based recovery metadata to take over the default ordinary
  checkpoint-boundary FPW image path
- discuss a PostgreSQL `storage manager` / physical storage-layer prototype,
  not a new table AM or a general storage engine
- use `P1-P9` to describe the semantic split of the current PoC branch, not to
  describe an exact one-to-one mapping between arbitrary working branches and
  patch numbers

Current non-goals and conservative boundaries:

- do not claim to remove every full-page image; `REGBUF_FORCE_IMAGE`,
  `XLR_CHECK_CONSISTENCY`, auxiliary-fork hints, and unconverted hint callers
  still keep image ownership
- do not claim that compactor, AIO, primary/standby physical-page alignment,
  `CREATE DATABASE` copy strategy, or explicit range-born protocol are fully
  engineered and closed
- do not treat `md + fpw=off` as a correctness-equivalent baseline

Terms used throughout this document:

- `birth`: the first durable `lblk -> pblk` relation for a logical page
- `remap`: moving an existing logical page to a new physical page
- `mapset`: direct publication of one mapping relation
- ordinary checkpoint-boundary FPW: the ordinary first-dirty-after-checkpoint
  path that defaults to a page image
- reclaim boundary: the physical boundary up to which reclaim / unlink may
  safely advance
- `compactor`: the background organizer that scans sparse regions and moves
  still-live pages away
- `reclaim`: the lifecycle action that enters the safe unlink path once a
  physical region is confirmed to have no live mappings

## 3. Design Boundary: Storage Metadata and WAL Own the Recovery Core

Umbra is deliberately narrow in scope.  It does not try to rewrite PostgreSQL's
execution layer or spread changes into large upper-layer abstractions.

More precisely, the current implementation is not a new table AM and not a
standalone general-purpose storage engine.  It is closer to a prototype at the
PostgreSQL `storage manager` / physical storage layer.  Upper layers still use
logical block numbers; below `smgr`, Umbra provides `lblk -> pblk` translation
for mapped forks.

From the crash-recovery perspective, the correctness core converges on two
layers:

- storage metadata
- WAL

Storage metadata has two object types:

- per-page map entry
- fork-level superblock

The "crash-recovery core" here does not mean that only these three modules
participate in recovery.  It means that after a crash, redo needs three kinds of
minimal durable truth to restore correct page contents:

- map entry: which physical block a logical block should currently map to
- superblock: fork-level boundary state, such as logical EOF, physical capacity,
  and committed allocator frontier
- WAL: which map-entry, superblock, and physical-page lifecycle changes were
  atomically published, and in what order redo must replay them

As long as those three facts stay consistent in redo, an ordinary remap update
can be recovered as "old physical page plus WAL delta", without using a
checkpoint-boundary full-page image as the recovery baseline.

Runtime concurrency correctness has one more explicit mechanism:

- inflight claim / barrier

This mechanism is not WAL encoding and not durable truth in the recovery log.  It
serializes publication order among foreground remap, background compactor
relocation, and physical writes, so that the durable truth later written to WAL
is itself valid.  A more precise split is:

- durable truth for crash recovery mainly comes from `map entry + superblock +
  WAL`
- runtime concurrency correctness also explicitly depends on inflight / barrier

## 4. Map Entry: Page-Level Mapping Truth

Umbra's core abstraction is to split logical page identity from physical
placement.

In this model:

- the logical page is the data-page identity that upper layers care about
- the physical page is only the on-disk location currently carrying that logical
  page
- the map entry records the current `lblk -> pblk` relation

Therefore, the map entry tells us which physical page currently stores one
logical page.  It is the page-level local truth.

## 5. Superblock: Fork-Level Global Truth

A per-page map entry is not enough.  Many correctness properties are not
expressible by one mapping alone; they depend on the boundary state of the
whole fork.  The superblock owns that state.

The superblock is Umbra's fork-level correctness anchor.  It maintains at least
these facts:

- logical boundary of the fork, such as `logical_nblocks`
- committed physical allocation boundary, such as `next_free_pblkno`
- materialized physical capacity
- the safe boundary for reclaim / unlink
- fork-level frontier facts needed by redo

One easy-to-miss distinction is the runtime reservation frontier.  Runtime code
also needs a reservation frontier in `MapSuperEntry` shared state to allocate
new `pblk` values concurrently in foreground backends.  That frontier is not
flushed to disk and does not participate in checkpoint.  The on-disk
`next_free_pblkno` in the superblock only represents the committed frontier.
The implementation should maintain and assert:
`committed next_free <= reservation frontier`.

In short:

- map entry owns page-level mapping truth
- superblock owns global fork-boundary truth

Many extend, truncate, reclaim, unlink, and redo correctness properties
ultimately depend on the superblock.

## 6. WAL: Atomic Publication and Recoverability

Storage metadata describes state, but state alone is not enough.  To take over
the ordinary checkpoint-boundary image path, Umbra must publish and replay the
following actions atomically:

- birth
- remap
- mapset
- related fork-level frontier changes, such as committed frontier, logical
  size, and capacity

That is why the WAL layer exists in this design.

For the ordinary checkpoint-boundary path handled by Umbra, recovery
correctness no longer depends on whether the record carries a full-page image.
It depends on:

- whether map entry and superblock state are correct
- whether those state changes were recorded and replayed by WAL as atomic
  events

This must not be expanded into "Umbra no longer depends on images in any
recovery path".  Conservative image owners still exist, and the paths listed
above keep PostgreSQL's original image semantics.

### 6.1 Lifecycle of the Old Physical Page Before Remap

The old physical page in an ordinary remap is not a temporary page that can be
immediately discarded or reused.  Before the remap is published, it is the
committed physical baseline pointed to by the current map entry.  It is also
the old baseline that no-image delta redo may need to read.

The lifecycle is:

- before remap publication, `old_pblk` is still the current durable mapping for
  `lblk`; even if a backend has chosen `new_pblk`, `old_pblk` cannot be treated
  as free space
- after successful WAL insert, the remap record publishes the `old_pblk ->
  new_pblk` transition as an atomic event; normal runtime state switches the map
  entry to `new_pblk`
- during crash recovery, for a no-image remap, redo first reads the old physical
  page through `old_pblk`, applies the WAL delta on top of that old baseline,
  and only then publishes `new_pblk`
- after remap publication, `old_pblk` is no longer the current mapping of that
  logical page, but it still does not enter foreground reuse; it only becomes a
  candidate for background space cleanup, constrained by live mappings, reclaim
  boundary, checkpoint, and redo semantics

So Umbra does not turn "overwrite the old page" into "immediately reuse the old
page".  It changes the recovery baseline: the old physical page remains
available when a WAL record needs it, while the new physical page becomes the
current mapping through remap.

## 7. Foreground Policy: Allocate New Pages, Do Not Dispose of Old Pages

If the goal is to make the ordinary checkpoint-boundary first-dirty path
lighter, then immediate old-page reclaim, reusable-page search, and synchronous
space cleanup should not be pushed back into the foreground path.

The foreground tradeoff in Umbra is:

- the foreground always takes a new physical page
- the old physical page is not immediately reused in the foreground path; the
  foreground only publishes the new mapping and does not dispose of the old page
- the foreground does not perform immediate space cleanup

That is, foreground allocation is closer to a monotonically advancing frontier.
Old pages are not rewritten in the hot path, and the foreground does not tidy
them up opportunistically.

This does not mean the system never processes old pages.  It means the
foreground does not own old-page disposal or long-term space convergence.  Later
cleanup, reclaim, and unlink are background policy decisions.  Under high
capacity pressure, the foreground may still trigger one-shot preallocation, but
that does not move long-term cleanup back into the hot path.

## 8. MAP Buffer and Mapwriter: Keep New Complexity in MAP Metadata

Once logical pages are split from physical pages, MAP metadata becomes durable
metadata in its own right.  It needs:

- its own cache
- its own I/O state
- its own flush and extension maintenance path

Therefore, in addition to PostgreSQL's existing data-page buffer pool, Umbra
adds a buffer cache dedicated to MAP metadata.  This double-buffering shape
cannot be completely removed, but the new buffering is mostly contained in the
MAP metadata layer rather than spreading into a second generic data-page cache.

More directly: `mapwriter` can be viewed as a MAP-metadata background writer
modeled after PostgreSQL `bgwriter`, with one extra duty: physical
preallocation for mapped forks near the low-water mark.  The current contract is
closer to:

- ordinary MAP pages are materialized on first dirty, not later by mapwriter or
  checkpoint
- checkpoint and mapwriter only flush ordinary MAP metadata blocks that already
  exist
- superblock flush is still owned by checkpoint
- mapwriter owns ordinary MAP flush
- mapwriter also owns background preallocation / physical capacity expansion
- under high low-water pressure, the foreground may still perform one-shot
  preallocation

Therefore, mapwriter can be understood as "`bgwriter` for MAP metadata plus a
physical-capacity preallocator".  It is not the owner of logical EOF, not the
owner of superblock checkpoint flush, and not a generic data-page writer.

## 9. Compactor: Long-Term Space Convergence

The benefit of monotonic foreground allocation is a simple hot path.  The cost
is that physical layout becomes sparse over time.  Opportunistic reclaim alone
is not enough for long-term space convergence, so a background process is needed
to move live pages out of sparse extents / segments and eventually create
conditions for reclaim and segment unlink.

That process is the compactor.  It is not the crash-recovery core, but it is the
background mechanism that makes "foreground only publishes new mappings and does
not dispose of old pages" sustainable over time.

Compactor and reclaim are not synonyms.  Compactor scans, chooses candidate
extents, relocates live pages, and advances the reclaim boundary when
conditions allow.  Reclaim is the later lifecycle action: only when a segment is
below the reclaim boundary and has no live mapping references does the physical
unlink get handed to the sync-request / checkpointer path.

The current compactor's first goal is not "clean as aggressively as possible".
It is "avoid interfering with the foreground".  It is a best-effort, bounded,
back-off-on-contention background organizer, not an aggressive reclaim sweeper.

At a high level, it works like this:

- scan MAP and count live block density by extent to find low-live-ratio
  candidate regions
- only process regions below the reclaim boundary and explicitly avoid the
  current physical tail
- relocate live pages in candidate extents by switching their mappings to new
  physical pages
- after an extent / segment becomes empty, defer real reclaim / unlink to the
  later queue

From the non-interference perspective, the important constraints are:

- when foreground allocation pressure rises, compactor skips the round instead
  of competing with foreground work
- each round processes only a bounded number of relations and relocation moves
- hot-path locking uses conditional acquire heavily; if a superblock or MAP
  buffer is busy, the current implementation tends to skip rather than wait
- relocation commits only if the old mapping is still the current published
  truth; if the foreground already won an update, compactor abandons that move
- real segment unlink is not executed synchronously by compactor; it is deferred
  through the reclaim / sync-request path

The result is that compactor behaves more like "gently yielding to foreground
work" than "maximizing background cleanup throughput".  Its first job is to
avoid noticeably slowing foreground allocation and writes.

## 10. Inflight / Barrier: Foreground-Background Migration Concurrency

Once both foreground code and compactor can migrate the same logical page, the
system needs shared state to describe that a migration for this logical block is
already in progress.  That is the role of inflight / barrier.

This explicit mechanism exists because compactor relocation is currently a raw
physical copy, not a shared-buffer-aware copy.  Without extra serialization,
foreground remap, background relocation, and physical writes could conflict
around the same `lblk`.

Inflight / barrier is not a space-management policy.  It is the concurrency
control for migration publication:

- prevent concurrent publication of multiple new mappings for the same `lblk`
- make the loser wait for stable committed MAP truth instead of borrowing
  someone else's owner-local target
- reduce foreground/background conflicts to owner / claim / barrier semantics

The more precise split is:

- `map entry + superblock + WAL` define the durable state truth that crash
  recovery must restore
- inflight / barrier defines how runtime code safely publishes those state
  changes

The former answers "what must redo ultimately restore"; the latter answers "who
may publish this change during concurrent execution".

## 11. File Deletion and Segment Lifecycle

File deletion in Umbra is not a normal unlink problem.  It is about when a
segment lifecycle reaches a safe deletion boundary.

There are at least two cases:

- truncate / drop driven deletion
- reclaim deletion triggered after compactor cleanup

The stable external contract is not the complete pending-state rule set.  It is
the following boundary:

- the superblock maintains the reclaim boundary
- compactor uses published live mappings and live-map scans to decide whether a
  candidate region still has live pages, and moves those live pages away
- reclaim registers later physical unlink only when a segment is below the
  reclaim boundary and has no live mapping references
- real physical unlink is deferred through PostgreSQL's sync-request /
  checkpointer path
- redo must accept this lifecycle boundary instead of deciding only from "is the
  file empty now"

The sync-request deferral has a checkpoint-epoch rule.  `SyncPreCheckpoint()`
absorbs requests that arrived before the checkpoint and then advances the
checkpoint cycle counter.  A reclaim unlink request registered after checkpoint
A starts is therefore tagged with A's cycle.  Checkpoint A's
`SyncPostCheckpoint()` must not remove that request, because A has not provided
a completed checkpoint boundary for it.  The request becomes eligible only after
the next checkpoint B has completed and entered `SyncPostCheckpoint()`.

In timeline form:

```text
A start: cycle 5 -> 6
request registers with cycle 6
A end/post: request cycle is still current, so it is not unlinked
B start: cycle 6 -> 7
B end/post: request cycle 6 is older than current cycle 7, so unlink may run
```

This means the correctness point is not the moment B starts and the counter
advances.  It is B's completed checkpoint followed by post-checkpoint unlink
processing.

Inflight / pending state still affects internal correctness, but it is better
treated as an implementation detail rather than the main criterion to explain in
community-facing text.  The key external points are:

- unlink is not "delete when empty"
- unlink is constrained by reclaim boundary, live mappings, checkpoint, and redo
  semantics

## 12. Current Verification Status

The current PoC verification target is: this owner / recovery model is
executable on the covered paths.  It is not a proof that all boundaries are
exhaustively covered.

The basic verification already includes:

- `make check` in `md` mode
- `src/test/recovery check` in `md` mode
- `make check` in `Umbra` mode
- `src/test/recovery check` in `Umbra` mode

Umbra-specific recovery TAP coverage further covers topics directly related to
this design story:

- MAP superblock / map fork policy / mapwriter activity
- truncate / remap / 2PC remap / skip-WAL dense map redo
- reclaim / internal segment unlink / compactor relocation
- range remap zeroextend / ordinary slim block remap / compact birth block remap

These tests support this statement:

- the PoC is no longer only a design sketch; on the currently covered paths, it
  has a minimal compile / regression / recovery loop

But the following points cannot be claimed as fully closed based only on the
current tests:

- stronger proof and coverage for primary/standby physical-page alignment when
  checkpoint cadence differs
- `CREATE DATABASE` copy strategy: `FILE_COPY` is supported; `WAL_LOG` is not
  supported yet, and with the Umbra storage manager enabled it falls back to
  `FILE_COPY`
- explicit `range-born / batch mapping publish` owner model
- dedicated verification for internal metadata fork / MAP fork crossing
  `RELSEG_SIZE` segment boundaries, for example beyond `1GB`
- more complete native AIO paths and stronger methodology stress tests

## 13. Performance Observations

This section provides directional performance signals for the current PoC.  It
does not attempt to make a strict benchmark claim.  The methodology is still
thin: it lacks complete hardware details, repeated runs, error / variance
ranges, and ablation for individual mechanisms.  The data below is better read
as a directional observation, not as a formal community performance conclusion.

The safer performance story should not treat `md + fpw=off` as a
correctness-equivalent baseline.  The fair default baseline is:

- `md + fpw=on`

This point is better used as a mechanism upper-bound / sensitivity point:

- `md + fpw=off`

On `master`, under the same workload, we compared three modes:

- `md + fpw=on`
- `md + fpw=off`
- `Umbra + fpw=on`

Common settings:

- `checkpoint_timeout = 2min`
- `max_wal_size = 20GB`
- `shared_buffers = 50GB`
- `logging_collector = on`
- `runMins = 10`
- `newOrderWeight = 45`
- `paymentWeight = 43`
- `deliveryWeight = 4`
- `stockLevelWeight = 4`
- `orderStatusWeight = 4`

Raw throughput results are listed first.

`checksum=off`

| clients | `md + fpw=on` | `md + fpw=off` | `Umbra + fpw=on` |
| --- | ---: | ---: | ---: |
| 10 | 158709 | 154283 | 155781 |
| 50 | 577005 | 626954 | 656353 |
| 200 | 641899 | 981436 | 995635 |
| 500 | 322660 | 943295 | 859058 |
| 1000 | 275609 | 899631 | 729989 |

`checksum=on`

| clients | `md + fpw=on` | `md + fpw=off` | `Umbra + fpw=on` |
| --- | ---: | ---: | ---: |
| 10 | 155754 | 152025 | 150606 |
| 50 | 601974 | 635597 | 650844 |
| 200 | 621176 | 1015923 | 938311 |
| 500 | 316950 | 972795 | 729801 |
| 1000 | 282713 | 891770 | 674865 |

For WAL volume, under the same transaction count, the ratio
`WAL(md + fpw=on) / WAL(Umbra + fpw=on)` is:

`checksum=on`

| clients | `WAL(md + fpw=on) / WAL(Umbra + fpw=on)` |
| --- | ---: |
| 10 | 1.82 |
| 50 | 2.11 |
| 200 | 3.81 |
| 500 | 4.58 |
| 1000 | 4.87 |

`checksum=off`

| clients | `WAL(md + fpw=on) / WAL(Umbra + fpw=on)` |
| --- | ---: |
| 10 | 2.03 |
| 50 | 2.51 |
| 200 | 5.22 |
| 500 | 6.90 |
| 1000 | 6.55 |

These numbers show more directly that under the same transaction count, Umbra
does not only recover throughput lost to ordinary checkpoint-boundary FPW; it
also substantially reduces the corresponding WAL-volume pressure.  The gap
widens as concurrency rises.

From the raw numbers, `Umbra + fpw=on` shows clear and stable improvement over
`md + fpw=on`:

- with `checksum=off`, the improvement at 50 / 200 / 500 / 1000 clients is
  about `+13.8% / +55.1% / +166.2% / +164.9%`
- with `checksum=on`, the improvement at 50 / 200 / 500 / 1000 clients is about
  `+8.1% / +51.1% / +130.3% / +138.7%`

At 10 clients, all three results are close.  That looks more like low-concurrency
noise or a non-FPW-dominated region.  The gap opens above 50 clients, where
ordinary checkpoint-boundary I/O cost starts to accumulate repeatedly.

At the same time, `Umbra + fpw=on` is close to, but does not fully reach, the
`md + fpw=off` upper bound at most points:

- with `checksum=off`, Umbra is already very close to the upper bound at 50 and
  200 clients, but remains clearly behind `md + fpw=off` at 500 and 1000 clients
- with `checksum=on`, this is more visible, suggesting that Umbra recovers much
  of the ordinary-FPW-related I/O cost but does not consume all remaining system
  cost

The current data supports only a qualitative conclusion:

- Umbra recovers a large part of the throughput lost by `md + fpw=on` on the
  ordinary FPW path; the safer interpretation is recovery of related I/O cost
- this benefit is visible with both `checksum=on` and `checksum=off`
- `md + fpw=off` should only be treated as a sensitivity reference for "where
  the system upper bound might be if this FPW cost is removed", not as a
  semantic peer baseline

The data is not enough for fine-grained attribution, such as "which foreground
hot path contributes exactly how much" or "which mechanism is the fixed main
source of benefit".  It supports:

- a large part of the benefit is related to ordinary checkpoint-boundary FPW
  being taken over by remap metadata, thereby recovering related I/O cost

Further attribution would require dedicated ablation and a fuller methodology
for WAL write / sync pressure, data write amplification, preallocation, and
other sub-mechanisms.

## 14. Open Engineering Work

The following items should be described as follow-up work, not as completed
capabilities:

1. `compactor` engineering: the framework exists, but background convergence
   efficiency and directory-discovery cost control are not fully engineered; the
   sparse-segment discovery cost problem should not be described as solved; for
   the PoC, this is engineering follow-up and does not block the minimal
   remap/recovery loop.
2. `CREATE DATABASE` copy strategy: PostgreSQL's existing `FILE_COPY` directory
   / file copy path is supported; `WAL_LOG`, which copies database contents
   block-by-block and logs each block to WAL, is not supported yet.  With the
   Umbra storage manager enabled, it falls back to `FILE_COPY`.  This is an
   explicit limitation and should not be overclaimed.
3. `superblock shared-entry replacement`: the current shape is still closer to
   allocate/free than replacement/eviction.  In practice, capacity pressure is
   mitigated by increasing `map_superblocks`.  This remains engineering
   follow-up.
4. `AIO` integration: the necessary adaptation exists, but it is not a complete
   Umbra-native rewrite.  The async I/O side should not be described as fully
   closed.
5. `range-born / batch mapping publish`: there is no explicit upper-layer
   interface yet, so the current implementation mainly uses conservative `smgr`
   fallback.  Multi-block extension still depends on compatibility with older
   AM/WAL ordering.
6. `primary/standby physical-page alignment`: the issue is identified.  The
   current implementation adds a stronger publication / flush constraint such
   as `FlushOneBuffer()` on the local no-image remap redo path and has local
   recovery coverage, but primary/standby physical-page alignment should not be
   described as systematically closed.

Compressed into one sentence: the current PoC has established the core
correctness / recovery loop and has a compile / regression / recovery shape, but
it still carries explicitly marked host-tree follow-up work.

## 15. Semantic Layers of the Current PoC

This section only describes the semantic boundaries that each layer should own
if the current PoC is organized as `P1-P9`.  It is not an exact mapping from
arbitrary working branches to commit numbers, and it does not describe release
cadence.

The split should not be understood as a mechanical directory split.  It is a
state-machine and owner-boundary split: earlier layers establish the minimal
recoverable mechanism, while later layers add checkpoint, mapwriter, compactor,
and other engineering capabilities.

The purpose is for each layer to state which correctness owner or engineering
boundary it introduces:

- earlier layers should mostly establish base mechanisms, without mixing in
  later engineering follow-up
- later layers introduce WAL/redo, checkpoint, mapwriter, compactor, recovery
  tests, and related capabilities
- incomplete parts should be explicitly marked as follow-up rather than implied
  as complete

For the current PoC branch, a natural semantic split is `P1-P9`:

- `P1`: establish the `smgr` implementation boundary, add the `--with-umbra`
  choice point, and keep the ordinary `md` path unchanged
- `P2`: introduce the `umfile` physical file layer and metadata storage
  primitives; cover physical files, segments, create / unlink, read / write /
  extend / truncate
- `P3`: introduce the metadata disk format and identity-mapping bootstrap so
  that metadata fork, superblock layout, and initial mapping state stand on
  their own
- `P4`: introduce the shared-memory MAP cache and checkpoint flush foundation,
  so MAP metadata cache, materialization, dirty, and flush semantics stand on
  their own
- `P5`: introduce MAP access policy, logical-to-physical translation, and the
  materialization contract, so `MAIN/FSM/VM` keep logical block numbers above
  `smgr` while resolving `lblk -> pblk` below it
- `P6`: introduce WAL records, mapped birth, and the redo state machine; build
  the minimal WAL/redo owners for `MAP_SET`, truncate, metadata lifecycle, and
  skip-WAL pending
- `P7`: introduce ordinary remap, block-reference remap, and
  checkpoint-boundary FPW replacement, closing the alternative representation
  for the ordinary checkpoint-boundary image path
- `P8`: add checkpoint / mapwriter writeback and physical preallocation, giving
  clear owners to MAP metadata writeback, background preallocation, and one-shot
  foreground preallocation under low-water pressure
- `P9`: introduce the compactor framework and foreground non-interference
  policy, converging inflight / barrier, reclaim, delayed unlink, and compactor
  relocation into a background organization framework

The point of this order is that earlier layers build the correctness owner
model, while later layers handle engineering pressure.  Host-tree integration
points such as `CREATE DATABASE` copy strategy, AIO, and primary/standby
physical-page alignment should not be hidden as if the core mechanism already
closed them.  They are better described as explicit follow-up.

Thus, `P1-P9` expresses only the semantic boundary of the current PoC: which
parts belong to the minimal correctness loop, which parts are engineering
enhancements, and which parts are still compatibility fallback or follow-up.
Tests and documentation should also belong to their related semantic layer,
instead of being flattened into a generic "test / documentation layer".

## 16. Summary

Umbra does not claim that PostgreSQL no longer needs full-page images.  It also
should not be broadly described as a new storage engine.  More accurately, it is
a remap-based recovery-baseline representation for the ordinary
checkpoint-boundary FPW path at the PostgreSQL `storage manager` / physical
storage layer.  Its core is:

- split logical page identity from physical placement in the storage layer
- use map entries for page-level mapping truth
- use the superblock for fork-level global truth
- use WAL to publish state changes atomically and recoverably

On top of that:

- the foreground always allocates a new physical page and does not dispose of old
  pages in the hot path
- mapwriter mainly smooths MAP metadata in the background, rather than owning
  all expansion
- compactor owns long-term space convergence
- inflight / barrier keeps foreground/background migration and physical writes
  concurrency-safe
- file deletion and segment lifecycle are constrained by reclaim boundary, live
  mappings, checkpoint, and redo semantics

The value of this design is not a single local optimization and not simply
"turning FPW off".  It challenges the cost model bound to ordinary
checkpoint-boundary FPW while trying to keep the crash-recovery semantics
required by `md + fpw=on`.

## Appendix: Implementation Transparency

The implementation process should be transparent.  Umbra's core architecture,
boundary definitions, and key state-machine reasoning come from the author's own
design and prototyping work around PostgreSQL storage / WAL / recovery
semantics.  The author also maintains the early `shadow` validation prototype:
<https://github.com/nayishan/postgre_umbra/tree/shadow-pg12-archive>.

To expand the prototype into the current PoC, the author used AI coding
assistants such as Codex extensively for concrete implementation, boilerplate
expansion, and local refactoring.  That work heavily depends on the prior logic
analysis, the `shadow` prototype, and the shapes and call order of existing
PostgreSQL implementation.

The responsibility boundary is also important: core design, boundary definition,
and key logic decisions are the author's responsibility; AI mainly accelerates
tedious implementation details.  Current AI systems still cannot independently
reason about database-kernel concurrency timing, owner models, or
crash-recovery semantics.  Some areas may therefore still show style
inconsistency or require further engineering convergence.  The current status is
PoC, not a finished product with final host-tree polish.
