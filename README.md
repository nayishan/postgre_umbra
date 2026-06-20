# Shadow Storage Manager on PostgreSQL 12.2

[English](./README.md) | [中文](./README_ZH.md)

This branch contains the early Shadow prototype on top of PostgreSQL 12.2.

Shadow has a deliberately small goal: below PostgreSQL's `smgr` boundary, keep
two physical copies for each protected relation block and use one bit of
metadata to record which copy is currently valid.  When PostgreSQL would
normally protect a checkpoint-boundary page overwrite with a full-page image,
Shadow records a shadow switch in the WAL block reference instead.  Later
buffer flushes write the dirty page to the other physical copy.  Redo can then
start from the old physical page and apply WAL delta, instead of depending on
an 8KB full-page image in WAL.

The value of this branch is that it shows the minimal idea.  It is not the
current full implementation.  The current implementation is the Umbra prototype
on the `umbra-poc-pgmaster` branch.

The original PostgreSQL top-level README is still available as
[README](./README).

## Branch Layout

- `shadow-pg12-archive`
  - archived Shadow prototype on PostgreSQL 12.2
  - useful for reading the minimal design: two physical pages, one status bit,
    a WAL shadow marker, and redo-side view switching
- `umbra-poc-pgmaster`
  - Umbra prototype on PostgreSQL master
  - the full engineering version of the same idea, with MAP metadata, remap
    WAL, mapwriter, compactor, tests, and documentation

## Prototype Goals

The original Shadow prototype had two goals:

1. Verify that the core logic is complete enough to preserve crash-recovery
   correctness with shadow page switching.
2. Obtain an initial performance signal before moving to a fuller design.

## Initial Performance Sample

The original benchmark compared ordinary PostgreSQL with `full_page_writes=on`,
ordinary PostgreSQL with `full_page_writes=off`, and Shadow.

The recorded PostgreSQL settings were:

| Setting | Value |
| --- | --- |
| `checkpoint_timeout` | `2min` |
| `max_wal_size` | `20GB` |
| `shared_buffers` | `50GB` |
| `logging_collector` | `on` |

The recorded workload settings were:

| Setting | Value |
| --- | ---: |
| `runMins` | 10 |
| `newOrderWeight` | 45 |
| `paymentWeight` | 43 |
| `deliveryWeight` | 4 |
| `stocklevelWeight` | 4 |
| `orderStatusWeight` | 4 |

| Clients | `On` | `Off` | `Shd` |
| --- | ---: | ---: | ---: |
| 10 | 150065 | 154499 | 142478 |
| 50 | 542035 | 618000 | 646693 |
| 200 | 683428 | 924056 | 876086 |
| 500 | 345125 | 853901 | 839828 |
| 1000 | 242905 | 795212 | 691147 |

Here `On` means ordinary PostgreSQL with `full_page_writes=on`, `Off` means
ordinary PostgreSQL with `full_page_writes=off`, and `Shd` means Shadow.  The
table should be read as early prototype evidence, not as a current performance
claim.

## Mental Model

Shadow manages three relation forks: `MAIN`, `FSM`, and `VM`.  Each managed
fork has a corresponding shadow fork:

| Logical fork | Shadow fork |
| --- | --- |
| `main` | `shd` |
| `fsm` | `fsm_shd` |
| `vm` | `vm_shd` |

Upper PostgreSQL layers still address relations with ordinary `(RelFileNode,
ForkNumber, BlockNumber)` identity.  Shadow decides inside `smgr` whether an
operation should access the ordinary fork or the shadow fork.

Each Shadow-managed block has one status bit:

- `PING` means the valid page is in the ordinary fork.
- `PONG` means the valid page is in the shadow fork.

When a WAL record needs to protect a block's pre-checkpoint image, Shadow does
not write that block's full-page image into WAL.  Instead, it marks the WAL
block header with `BKPBLOCK_HAS_SHADOW` and appends the target status.  After
WAL insertion succeeds, Shadow flips the block status bit.  Subsequent buffer
flushes write the new page to the target fork, while the old fork still keeps
the redo baseline.

During redo, a block reference with `BKPBLOCK_HAS_SHADOW` first switches the
status back to the old page and reads that page as the delta replay input.
Then it restores the status recorded by the WAL record so that the redo result
is written to the new physical copy.

## Code Map

Main entry points:

- `src/backend/storage/smgr/shadow.c`
  - Shadow `smgr` implementation
  - ordinary/shadow fork file I/O
  - block status-bit lookup and switching
  - SLRU initialization, checkpoint, and shutdown for Shadow metadata
- `src/backend/storage/smgr/smgr.c`
  - Shadow storage manager integration
  - extra `smgr` APIs used by WAL, relation lifecycle, and database lifecycle
    code to maintain Shadow metadata
- `src/include/common/relpath.h`
  - adds `SHADOW_FORKNUM`, `FSM_SHADOW_FORKNUM`, and
    `VISIBILITYMAP_SHADOW_FORKNUM`
- `src/common/relpath.c`
  - adds physical fork suffixes: `shd`, `fsm_shd`, and `vm_shd`
- `src/backend/access/transam/xloginsert.c`
  - writes `BKPBLOCK_HAS_SHADOW` on the ordinary path that would otherwise
    need a full-page write
  - flips the block status bit after WAL insertion succeeds
- `src/backend/access/transam/xlogreader.c`
  - decodes the shadow block header and attached block metadata
- `src/backend/access/transam/xlogutils.c`
  - interprets shadow block references during redo by reading through the old
    status and then restoring the new status
- `src/backend/catalog/storage.c`
  - maintains Shadow metadata during relation and database create/drop
- `src/bin/initdb/initdb.c`
  - initializes `pg_shadow/db`, `pg_shadow/rel`, and `pg_shadow/block`

## Metadata Layout

Shadow uses cluster-level SLRU metadata instead of a per-relation metadata
fork.

The data directory contains:

- `pg_shadow/db`
  - maps database OIDs to Shadow database ids
- `pg_shadow/rel/shd_rel_<n>`
  - maps relation OIDs to local relation ids for each Shadow database id
- `pg_shadow/block/shd_blk_<n>`
  - stores per-block status bits for managed relation/fork groups

Current capacity is hard-coded:

- `MAX_SHD_DBS = 9`
- `SHD_MAX_REL_PER_DB = 2048`
- `NUM_FORK = 3`

This means the prototype only covers a small fixed database/relation space.  If
allocation exceeds those limits, Shadow requests a checkpoint and retries; if
allocation still fails, it raises `PANIC`.

## Build And Run

This branch has no `--with-shadow` configure option and no runtime switch.

`SMGR_WHICH` is hard-coded to `1` in `src/include/miscadmin.h`, so the build
uses the Shadow storage manager by default.  Build it like ordinary PostgreSQL
12.2 source:

```sh
./configure --prefix="$PWD/install"
make -j
make install
```

Initialize and start a cluster with the usual PostgreSQL commands:

```sh
./install/bin/initdb -D data
./install/bin/pg_ctl -D data -l logfile start
```

`initdb` creates the extra `pg_shadow` metadata directories.  Keep
`full_page_writes=on` when running this prototype, because Shadow reuses
PostgreSQL's normal "this block needs FPW protection" decision point to decide
when to perform a shadow switch.

## Limitations

This is a research prototype with important limits:

- The baseline is PostgreSQL 12.2, not current PostgreSQL master.
- Shadow is hard-coded on; the same binary cannot switch between ordinary `md`
  and Shadow storage managers at runtime.
- Only non-temporary relations are Shadow-managed; temporary relations bypass
  Shadow.
- `INIT_FORKNUM` bypasses Shadow.
- Only `MAIN`, `FSM`, and `VM` forks are covered.
- Each managed block keeps two fixed physical copies, so space usage is close
  to twice the managed fork size, plus `pg_shadow` metadata.
- Database and relation capacity uses small hard-coded limits.
- There is no MAP compaction, physical-space reclaim, background mapwriter, or
  preallocation mechanism.
- WAL records only shadow status switches, not a general `logical block ->
  physical block` mapping.
- Paths such as `REGBUF_FORCE_IMAGE` still keep full-page images; this
  prototype is not a global FPW-off mode and does not replace every FPI case.
- `xloginsert.c` still carries early assumptions: ordinary page changes must be
  replayable from WAL delta, and WAL records that rely on FPI-only semantics
  are not handled systematically.

## Shadow vs Umbra

Shadow and Umbra address the same core question: how to reduce ordinary
checkpoint-boundary full-page-image pressure while preserving deterministic
crash recovery.

The difference is the abstraction level.

| Area | Shadow | Umbra |
| --- | --- | --- |
| PostgreSQL baseline | PostgreSQL 12.2 | PostgreSQL master |
| Physical model | each logical block has two fixed physical copies: ordinary fork and shadow fork | logical blocks point to arbitrary physical blocks through MAP |
| Metadata | cluster-level `pg_shadow` SLRU for db/rel/block state | per-relation internal metadata fork with MAP superblock and MAP pages |
| Block state | one bit: `PING`/`PONG` | `lblk -> pblk` mapping plus fork-level state |
| WAL record | `BKPBLOCK_HAS_SHADOW` plus `grelId/status` | remap metadata on block references plus Umbra rmgr lifecycle records |
| Space management | fixed double copy, no compaction or reclaim | physical allocation, preallocation, reclaim, and compaction |
| Background workers | none | `mapwriter`, `mapcompactor` |
| Enablement | `SMGR_WHICH` hard-coded to Shadow | `--with-umbra` build option |
| Scope | minimal prototype for understanding the core idea | fuller engineering prototype for reading, testing, and review |

Shadow can be read as Umbra's smallest thought experiment:

1. Upper layers keep using logical block numbers.
2. `smgr` decides the physical location underneath.
3. WAL must carry enough information for redo to know which old page to replay
   from.
4. Redo must publish the result to a new physical location without destroying
   the old baseline first.

Umbra keeps those principles, but generalizes "two fixed physical locations"
into a MAP layer and replaces the hard-coded SLRU prototype with relation-local
metadata, WAL lifecycle records, background maintenance, and testable recovery
paths.
