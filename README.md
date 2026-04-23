# Umbra on PostgreSQL master

[English](./README.md) | [中文](./README_ZH.md)

This repository hosts the current Umbra prototype on top of PostgreSQL master.

Umbra is a storage-manager variant in which selected relation forks keep
ordinary PostgreSQL logical block numbers at the upper layers, but are stored
through an internal logical-to-physical mapping layer underneath.  MAIN, FSM,
and VM can therefore still be addressed as logical blocks while Umbra
translates them to physical blocks stored in data-fork files.

In this model, a remap means moving one logical block from its old physical
block to a newly published physical block.  The purpose of that remap is to
give ordinary checkpoint-boundary updates a different recovery baseline.
Instead of overwriting the old physical page and logging a full-page image just
to protect that overwrite, Umbra can publish a new physical page for the same
logical block and record the old/new physical mapping in WAL.  During redo, a
remap record is replayed through the mapping view expected by that record; a
delta-only remap uses the old physical page plus WAL delta instead of treating
the update as overwrite-in-place on the new physical page.  This is the
mechanism Umbra uses to reduce ordinary full-page-image pressure while
preserving crash-recovery ordering.

This branch family is correctness-first.  It is useful for design review,
implementation reading, and testing.  It is not presented as a finished
production feature.

## Branch Layout

- `umbra-poc-pgmaster`
  - PostgreSQL master based Umbra PoC
  - full implementation branch for full-tree reading and testing
- `shadow-pg12-archive`
  - archived PostgreSQL 12.2 shadow prototype

## Current Scope

The current implementation includes:

- a `--with-umbra` build option and Umbra `smgr` integration
- an internal metadata fork per relation that stores:
  - the MAP superblock, which records fork-level state such as:
    - logical EOF
    - physical capacity
    - committed allocator frontier
  - MAP pages, which record per-block mapping facts:
    - `lblk -> pblk` entries
    - unmapped versus mapped state for ordinary logical blocks
- a MAP subsystem that owns:
  - logical-to-physical lookup
  - shared superblock state and related runtime state for:
    - logical EOF
    - allocator/frontier state
    - reclaim boundaries
- two background workers:
  - `mapwriter`
    - MAP-page flush
    - preallocation
  - `mapcompactor`
    - reclaim
    - compaction
- remap-aware WAL/redo support, including:
  - remap-aware block headers on ordinary WAL records
  - redo-side remap interpretation in `xlogutils.c`
- Umbra recovery TAP coverage in `src/test/recovery`

## Design In One Page

Umbra should be read as a storage-layer split with six distinct pieces.

1. Upper PostgreSQL layers keep ordinary logical addressing.
   Relations, forks, and block numbers are still presented as logical objects
   to normal PostgreSQL callers.  Umbra changes physical placement underneath
   `smgr`; it does not ask upper layers to reason in physical block numbers.

2. Persistent truth lives in the metadata fork.
   Each relation has an internal metadata fork containing:
   - a MAP superblock for fork-level facts such as logical EOF, physical
     capacity, and the committed allocator frontier; this superblock is stored
     as a small 512-byte metadata sector
   - MAP pages for per-block `lblk -> pblk` mapping facts; these pages are
     compact fixed-entry metadata pages, closer to CLOG-style metadata than to
     ordinary PostgreSQL data pages, so they do not use ordinary data-page FPW
     semantics

3. Runtime access is split from physical file I/O.
   - `umbra.c` owns mapped-fork runtime semantics.
   - the MAP subsystem owns lookup and shared runtime state.
   - `umfile.c` owns physical file and segment operations.

4. WAL is the owner boundary for MAP state changes.
   - fork-level superblock facts become redo-visible through WAL.
   - physical-page lifecycle transitions become redo-visible through WAL.
   - logical-to-physical mapping changes become redo-visible through WAL.
   Ordinary block references carry page-replay remap metadata; Umbra rmgr
   records cover explicit MAP lifecycle actions outside ordinary block
   references.

5. Redo replays remap records through the record's expected mapping view.
   - redo first restores the old/new mapping view carried by the WAL record.
   - without an image, replay uses the old physical page plus WAL delta, not
     overwrite-in-place on the new physical page.
   - with an image, redo installs the image into the newly published mapping.

6. Background maintenance stays separate from the foreground access path.
   - `mapwriter` handles MAP-page flush and preallocation
   - `mapcompactor` handles reclaim and compaction
   This keeps long-term space convergence out of the hot foreground allocation
   path.

## Documentation

Detailed design notes live under [doc/umbra/](./doc/umbra/).

Primary English documents:

- [Architecture](./doc/umbra/ARCHITECTURE.md)
- [WAL and Redo](./doc/umbra/WAL_AND_REDO.md)
- [Review Guide](./doc/umbra/REVIEW_GUIDE.md)
- [Prototype and Branch Navigation](./doc/umbra/PROTOTYPE.md)
- [FPW-to-remap design story](./doc/umbra/UMBRA_FPW_STORY.md)

Chinese companion material:

- [Architecture](./doc/umbra/ARCHITECTURE_ZH.md)
- [WAL and Redo](./doc/umbra/WAL_AND_REDO_ZH.md)
- [Review Guide](./doc/umbra/REVIEW_GUIDE_ZH.md)
- [Prototype and Branch Navigation](./doc/umbra/PROTOTYPE_ZH.md)
- [FPW-to-remap 设计故事](./doc/umbra/UMBRA_FPW_STORY_ZH.md)

## Testing Baseline

The current correctness baseline is the md/Umbra matrix below.  When switching
between modes in the same source tree, clean the previous build first.

```sh
make distclean
./configure
make
make check
make -C src/test/recovery check

make distclean
./configure --with-umbra
make
make check
make -C src/test/recovery check
```

One especially important recovery test is:

```sh
make -C src/test/recovery check PROVE_TESTS=t/074_umbra_torn_page_remap.pl
```

This test acts as a negative control in md mode and validates torn-page remap
recovery in Umbra mode.

## Preliminary Performance Indicators

Current performance evidence is directional only.  Two early signals are worth
showing together:

- TPCC-style throughput under the same workload
- WAL-size ratio under the same workload

The throughput view matters because WAL-size reduction alone does not fully
describe performance.  The fair default baseline is:

- `md + fpw=on`

The `md + fpw=off` numbers are useful as a sensitivity / upper-bound reference,
not as a correctness-equivalent baseline.

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

### TPCC-Style Throughput

#### Checksums Disabled

| clients | `md + fpw=on` | `md + fpw=off` | `Umbra + fpw=on` |
| ------- | ------------: | -------------: | ---------------: |
| 10      |        158709 |         154283 |           155781 |
| 50      |        577005 |         626954 |           656353 |
| 200     |        641899 |         981436 |           995635 |
| 500     |        322660 |         943295 |           859058 |
| 1000    |        275609 |         899631 |           729989 |

#### Checksums Enabled

| clients | `md + fpw=on` | `md + fpw=off` | `Umbra + fpw=on` |
| ------- | ------------: | -------------: | ---------------: |
| 10      |        155754 |         152025 |           150606 |
| 50      |        601974 |         635597 |           650844 |
| 200     |        621176 |        1015923 |           938311 |
| 500     |        316950 |         972795 |           729801 |
| 1000    |        282713 |         891770 |           674865 |

### WAL-Size Ratio

- `md WAL bytes with full_page_writes=on`
- divided by
- `Umbra WAL bytes with full_page_writes=on`

Larger values mean Umbra generated less WAL for the same workload.

#### Checksums Disabled

| clients | md WAL / Umbra WAL |
| ------- | ------------------ |
| 10      | 2.03               |
| 50      | 2.51               |
| 200     | 5.22               |
| 500     | 6.90               |
| 1000    | 6.55               |

#### Checksums Enabled

| clients | md WAL / Umbra WAL |
| ------- | ------------------ |
| 10      | 1.82               |
| 50      | 2.11               |
| 200     | 3.81               |
| 500     | 4.58               |
| 1000    | 4.87               |

Taken together, the throughput and WAL-size numbers show that Umbra is not only
reducing WAL volume.  Under the same workload, it also recovers a large part of
the throughput lost to ordinary checkpoint-boundary full-page-image pressure.

These numbers should be read as:

- preliminary
- directional
- not yet a complete benchmark

They should not be read as a final claim about throughput, latency, or full
replication/recovery cost.
