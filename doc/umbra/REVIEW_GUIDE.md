# Umbra Review Guide

This note is for reviewers and maintainers reading the Umbra PostgreSQL master
PoC patch series. It does not replace the architecture and WAL/redo documents.
It describes how to read the patch and which invariants should be checked
first.

## 1. Patch Shape

The patch is not intended to hide subsystem boundaries.  The main review units
are:

- build flag and storage-manager dispatch
- internal metadata fork and physical file layer
- MAP buffer, superblock, in-flight owner, and write-barrier subsystem
- WAL block-header remap encoding
- redo-time remap interpretation
- skip-WAL dense-map bootstrap
- background mapwriter/mapcompactor maintenance
- recovery and regression tests

For a line-by-line review, it is usually better to read the patch by those
units rather than by file order.

## 2. What Umbra Changes

Umbra keeps PostgreSQL's upper-layer logical block addressing.  The storage
manager translates mapped forks from logical block numbers to physical block
numbers underneath.

The mapped forks are:

- `MAIN_FORKNUM`
- `FSM_FORKNUM`
- `VISIBILITYMAP_FORKNUM`

The persistent mapping state lives in an internal metadata fork owned by
Umbra.  That metadata fork is not a normal PostgreSQL page fork and must not
enter generic shared-buffer, full-page-image, checksum, or page-LSN paths.

## 3. Where to Start Reading

Start with:

- `src/backend/storage/smgr/smgr.c`
  - storage-manager dispatch and the `--with-umbra` boundary
- `src/backend/storage/smgr/umbra.c`
  - runtime access policy and logical-to-physical translation
- `src/backend/storage/smgr/umfile.c`
  - physical file operations below Umbra
- `src/backend/storage/map/`
  - MAP metadata, reservations, writeback, and background work
- `src/backend/access/transam/xloginsert.c`
  - producer-side remap decisions and header encoding
- `src/backend/access/transam/xlogreader.c`
  - remap header parsing
- `src/backend/access/transam/xlogutils.c`
  - redo-time remap interpretation
- `src/backend/access/transam/umbra_xlog.c`
  - Umbra rmgr records

## 4. Core Correctness Invariants

Review these invariants before focusing on micro-optimizations:

- WAL publication wins before committed MAP publication.
- A pending reservation chooses a physical block but does not publish the
  logical-to-physical mapping or committed allocator frontier.
- First-born pages publish logical EOF explicitly through WAL-owned remap or
  range remap state.
- Ordinary remap-without-image redo consumes the old physical baseline before
  publishing the new physical mapping.
- `next_free_pblkno` is the committed allocator frontier, not necessarily
  `new_pblkno + 1`.
- The runtime reservation frontier lives in `MapSuperEntry` shared state, not
  in the on-disk superblock.
- committed `next_free_pblkno <= reservation frontier` must hold under the
  shared-entry lock and should be asserted in the implementation.
- MAP superblock logical EOF, materialized physical frontier, and allocator
  frontier are separate facts.
- Checkpoint and mapwriter write existing MAP metadata blocks; they do not
  materialize missing MAP blocks during flush.
- Reclaim unlink requests registered in one checkpoint cycle must not be
  physically removed at that checkpoint's post phase; they are eligible only
  after a following checkpoint has completed and reaches `SyncPostCheckpoint()`.
- Redo owns redo-only metadata bootstrap for mapped forks.

## 5. WAL Review Checklist

Umbra has two WAL-visible mechanisms.

Block-reference remap metadata is attached to ordinary WAL records with
`BKPBLOCK_HAS_REMAP`:

- full remap header:
  - `old_pblkno`
  - `new_pblkno`
  - `logical_nblocks`
  - `next_free_pblkno`
- compact birth header:
  - `new_pblkno`
  - `logical_nblocks`
  - `next_free_pblkno`
- ordinary slim header:
  - `old_pblkno`
  - `new_pblkno`
  - `next_free_pblkno`

Umbra rmgr records are separate lifecycle records:

- `XLOG_UMBRA_MAP_SET`
- `XLOG_UMBRA_RANGE_REMAP`
- `XLOG_UMBRA_RANGE_REMAP_COMPACT`
- `XLOG_UMBRA_SKIP_WAL_DENSE_MAP`
- `XLOG_UMBRA_RECLAIM_UNLINK`

The important review point is that these are complementary, not substitutes.
Block-header remap is for replaying ordinary WAL block content against the
right physical baseline.  Umbra rmgr records are for explicit MAP lifecycle
events outside the ordinary block-reference owner.

## 6. Full-Page Image Boundaries

Umbra does not globally disable full-page writes.

It replaces the ordinary checkpoint-boundary image path with remap metadata
when the record is eligible for automatic remap.  Images are still kept when
the caller explicitly owns an image or when consistency checking requires one.

Known conservative cases:

- `REGBUF_FORCE_IMAGE` keeps image semantics.
- `XLR_CHECK_CONSISTENCY` keeps verification images.
- auxiliary-fork and unconverted hint callers keep PostgreSQL's
  `XLOG_FPI_FOR_HINT` rule.

Converted core MAIN-fork hint callers use `XLOG2_HINT_DELTA`.  They claim an
exact old-P/new-P remap before changing the page and WAL-log sorted final-byte
ranges without a page image.  This is a separate hint-write protocol, not an
automatic change to every `MarkBufferDirtyHint()` caller.

## 7. Skip-WAL Dense Map

Skip-WAL relations are handled as a dense physical build while the relation is
still in the skip-WAL pending window.

The WAL anchor is `XLOG_UMBRA_SKIP_WAL_DENSE_MAP`.  For each encoded fork it
means:

- `[0, nblocks)` is dense
- `pblk == lblk` in that range
- `logical_nblocks = nblocks`
- `physical_nblocks = nblocks`
- `next_free_pblkno = nblocks`

The record does not encode empty forks.  An entry with `nblocks == 0` has no
mapping work and should not be produced.

The record is not a data-file fsync replacement and is not a generic
`MAP_SUPER_INIT`.  The existing skip-WAL sync protocol still owns durability;
the dense-map record gives redo an exact mapping/frontier anchor.

## 8. What Is Intentionally Not Solved Here

The current patch does not try to solve every possible WAL byte optimization.

It intentionally does not add:

- a tiny birth header that drops both frontier fields
- per-block remap variant tags inside mixed records
- remap optimization for checksum-driven hint FPIs
- range relocation WAL for compactor moves
- a default-on storage-manager behavior

Those are separate follow-up designs.  The current patch favors deterministic
ownership and reviewable replay semantics over maximum header compression.

## 9. Test Baseline

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

Umbra-only recovery tests are expected to skip in md mode and run in
`--with-umbra` mode.

The torn-page remap test is especially important:

- `src/test/recovery/t/074_umbra_torn_page_remap.pl`
  - md negative control with `full_page_writes=off`
  - Umbra positive recovery path with `full_page_writes=on`
  - recovery verification uses an ordered relation digest, not just row count

## 10. Longer Reference Material

Reviewers should also read:

- [ARCHITECTURE.md](./ARCHITECTURE.md)
- [WAL_AND_REDO.md](./WAL_AND_REDO.md)
- [PROTOTYPE.md](./PROTOTYPE.md)
- [UMBRA_FPW_STORY_ZH.md](./UMBRA_FPW_STORY_ZH.md)
