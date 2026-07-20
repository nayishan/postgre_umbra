# Umbra WAL and Redo Semantics on PostgreSQL Master

This document describes the current WAL payload and redo rules used by the
PostgreSQL master Umbra PoC.

The design has two WAL-visible pieces:

- remap metadata attached to ordinary block references
- Umbra rmgr records for MAP lifecycle operations

Those two mechanisms are complementary.  They solve different problems and are
replayed in different layers.

## 1. Ordinary Block Records with Remap Metadata

Umbra extends ordinary WAL block references with an extra block-header payload
when `BKPBLOCK_HAS_REMAP` is set.

The full payload is:

- `old_pblkno`
- `new_pblkno`
- `logical_nblocks`
- `next_free_pblkno`

The meaning of those fields is:

- `old_pblkno`
  - the old published physical baseline for this logical block
  - `InvalidBlockNumber` means first published mapping
- `new_pblkno`
  - the physical block that becomes the new published target
- `logical_nblocks`
  - logical frontier payload needed when redo is publishing a first-born page
- `next_free_pblkno`
  - allocator frontier payload that keeps replay-side physical allocation
    deterministic

The remap header does not try to encode every superblock fact.  It only carries
the block-local transition plus the frontier state redo needs to keep the MAP
view deterministic.

The current record-level remap format is encoded in `xl_info`:

- full remap:
  - `old_pblkno`
  - `new_pblkno`
  - `logical_nblocks`
  - `next_free_pblkno`
- compact birth:
  - `new_pblkno`
  - `logical_nblocks`
  - `next_free_pblkno`
- ordinary slim:
  - `old_pblkno`
  - `new_pblkno`
  - `next_free_pblkno`

Compact birth records omit `old_pblkno`, because first-born remaps always use
`InvalidBlockNumber` for the old physical block.

Ordinary slim records omit `logical_nblocks`, because ordinary remap has a
valid old physical baseline and does not publish a first-born logical EOF.

The branch deliberately does not use a "tiny birth" header that carries only
`new_pblkno`.  Such a format is only safe when the replay-side frontier can be
derived locally.  That condition is too narrow for the current owner model, so
compact birth remains the conservative birth fallback.

## 2. Producer-Side Decisions in `xloginsert.c`

Producer-side logic lives in `XLogRecordAssembleUmbra()`.

The code still starts from PostgreSQL's normal questions:

- does this record need a backup image?
- does it need data payload?

Umbra then adds a second question:

- does this block record need remap metadata?

Those decisions are related, but they are not collapsed into one boolean.

### 2.1 Automatic checkpoint-boundary remap

For ordinary data-bearing records:

- `REGBUF_FORCE_IMAGE` means:
  - backup image yes
  - automatic remap no
- `REGBUF_NO_IMAGE` means:
  - backup image no
  - automatic remap no
- `!doPageWrites` means:
  - backup image no
  - automatic remap no
- converted Umbra MAIN-fork hints use `XLOG2_HINT_DELTA`:
  - claim exact old-P/new-P before changing page bytes
  - record sorted final-byte ranges without an image
- auxiliary-fork and unconverted hint callers keep MD's
  `RM_XLOG_ID / XLOG_FPI_FOR_HINT` rule
- ordinary checkpoint-boundary case means:
  - no backup image
  - remap if `page_lsn <= RedoRecPtr`

That last rule is where Umbra replaces MD's ordinary checkpoint-boundary backup
image path with remap-aware WAL.

### 2.2 `REGBUF_LOGICAL_BIRTH`

Umbra also supports an explicit first-born owner path through
`REGBUF_LOGICAL_BIRTH`.

When a registered buffer is marked logical-birth and does not already carry
remap metadata, WAL assembly:

1. opens the relation
2. tries to find an already-published mapping
3. tries to find a pending reserved mapping
4. only if both are absent, reserves a fresh physical block

The outcome is then recorded as:

- `old_pblkno = InvalidBlockNumber`
- `new_pblkno = chosen physical block`
- `has_remap = true`

This is not "always allocate a new pblk immediately".  It is "WAL assembly owns
the first-born publication if no prior mapping or reservation already exists".
The chosen `pblk` may come from a runtime reservation frontier, but that
reservation is not yet committed superblock state.

### 2.3 When remap metadata is included

The current inclusion rule is:

- if the block already has remap metadata, include it
- otherwise, if the automatic remap rule says this record needs remap, build
  and include it

So remap-bearing records come from two sources:

- explicit logical-birth ownership
- ordinary checkpoint-boundary remap

### 2.4 Interaction with images

When remap metadata is included:

- `BKPBLOCK_HAS_REMAP` is set
- the remap header is filled

If the remap came from the ordinary checkpoint-boundary path, Umbra suppresses
the ordinary backup image unless:

- the caller explicitly forced an image, or
- `XLR_CHECK_CONSISTENCY` requires one

That means the current rule is not:

- "full-page image and remap are always mutually exclusive"

It is:

- "automatic checkpoint-boundary remap replaces the default image path"
- explicit image owners can still coexist with remap metadata

## 3. Post-Insert Publication

After WAL insertion succeeds, `XLogCommitBlockRemapsUmbra()` publishes the
winner state for each block record that carried remap metadata.

That commit step:

- installs the new mapping with `UmMapSetMapping()`
- bumps committed `next_free_pblkno` when needed
- bumps `logical_nblocks` for first-born publication
- updates cached relation size state for WAL-owned first-born pages
- releases pending reservations

This is an important owner boundary:

- the block record is assembled before insert
- publication becomes durable owner state only after insert succeeds
- runtime reservation state may run ahead transiently, but checkpoint-visible
  superblock state is published only at this boundary

## 4. Umbra RMGR Records

Umbra also has a small rmgr (`RM_UMBRA_ID`) for MAP lifecycle operations.

Current records include:

- `XLOG_UMBRA_MAP_SET`
- `XLOG_UMBRA_RANGE_REMAP`
- `XLOG_UMBRA_RANGE_REMAP_COMPACT`
- `XLOG_UMBRA_SKIP_WAL_DENSE_MAP`
- `XLOG_UMBRA_RECLAIM_UNLINK`

`XLOG_UMBRA_SKIP_WAL_DENSE_MAP` is a redo anchor for skip-WAL relations.  It
does not replace the skip-WAL sync protocol.  For each encoded fork it states:

- `[0, nblocks)` is dense
- `pblk == lblk` in that range
- `logical_nblocks = nblocks`
- `physical_nblocks = nblocks`
- `next_free_pblkno = nblocks`

The producer should not encode empty forks.  A `nblocks == 0` entry has no
mapping work and does not advance any frontier.

These records are not a replacement for block-header remap metadata.

Their purpose is different:

- block-header remap metadata is for ordinary WAL block replay
- Umbra rmgr records are for explicit MAP lifecycle actions such as
  compactor/reclaim/state maintenance

### 4.1 Why this branch does not use an explicit RangeMap / range-born owner model

The current branch deliberately does not make `range-born / batch mapping
publish` a first-class upper-layer contract.

The reason is not that range publication is impossible.  The problem is owner
clarity.

At the PostgreSQL call sites Umbra currently has explicit ownership for:

- one logical block being born for the first time
- one logical block being remapped at a checkpoint-boundary WAL site
- explicit Umbra-internal lifecycle records such as compactor/reclaim work

What is still missing is a concrete upper-layer use site that already owns
range publication as one semantic unit.  A future example could be something
like a hash-AM split/redistribution path where a well-defined logical range is
materialized and published under one owner.  The current branch does not wire
such a caller yet.

It does **not** yet have a generic upper-layer interface that says:

- this WAL owner is publishing a whole logical range at once
- this range has one well-defined ordering point
- redo can treat that range as a single published unit

Without that interface, a generic RangeMap-style contract would push too much
ambiguity into WAL assembly and redo:

- which layer owns range extent vs. per-block visibility
- when logical EOF becomes durable for the whole range
- how allocator frontier publication is synchronized with older AM/WAL ordering
- whether a later block in the same range may become visible before an earlier
  block's WAL ownership is fully established

The current branch therefore stays conservative:

- ordinary upper-layer WAL continues to publish remap state per block
- first-born publication remains explicit per block
- range-shaped operations are limited to internal Umbra-controlled lifecycle
  paths where ownership is already local and bounded

That is why the branch has range remap records for internal lifecycle work, but
does not yet describe a generic upper-layer RangeMap contract as a settled
feature.

## 5. Redo Entry in `xlogutils.c`

Redo-side interpretation lives in `XLogReadBufferForRedoExtendedUmbra()`.

The redo entry layer owns:

- metadata bootstrap for mapped-fork redo
- redo-time MAP state seeding
- interpretation of `has_remap`
- distinction between remap-with-image and remap-without-image

It intentionally does not push those semantics down into a generic read helper,
because the generic helper does not know:

- whether the record has remap metadata
- whether the record has a block image
- what `old_pblkno` and `new_pblkno` are

## 6. Redo Bootstrap Before Replay

Before replaying mapped-fork data, redo first ensures:

- the relation is open
- MAP state is seeded on the `SMgrRelation`
- the metadata fork exists when the fork requires mapping

That work is currently done by:

- `XLogUmbraMapStateForRedo()`
- `XLogUmbraEnsureMetadataForRedo()`
- `XLogUmbraEnsureMappedBlockForRedo()`

This is redo-only bootstrap logic and intentionally belongs at the redo-entry
layer.

## 7. Redo Cases

### 7.1 No remap metadata

If `has_remap` is false, Umbra first ensures metadata for mapped forks and then
falls back to the ordinary PostgreSQL-style block restore/read path:

- image -> restore image
- no image -> read current block view and compare LSN

This is the least interesting Umbra case; it mostly behaves like md plus
metadata availability checks.

### 7.2 Remap with image

If `has_remap` and `has_image` are both true, redo:

1. installs the new mapping immediately
2. bumps `next_free_pblkno` if provided
3. bumps `logical_nblocks` for first-born publication
4. ensures the mapped block exists
5. restores the block image into that new mapping view

This is the current "phase-1" remap replay path.

### 7.3 Remap without image, zero/init mode

If `has_remap` is true, `has_image` is false, and redo is in zero/init mode,
redo:

1. installs the new mapping immediately
2. bumps frontier payload as needed
3. ensures the mapped block exists
4. reads the block in zero/init mode

This covers first-born and initialization-style replay where no old physical
baseline is needed.

### 7.4 Remap without image, ordinary mode

This is the most Umbra-specific case.

Redo requires a valid old physical baseline.  A delta-only remap is therefore
replayed as old physical page plus WAL delta, not as overwrite-in-place on the
new physical page.  Redo does not publish the new mapping first; instead it:

1. temporarily installs `old_pblkno` as the current mapping
2. ensures the old mapped block is readable
3. reads and locks the buffer through that old mapping view
4. dirties and flushes that buffer state
5. switches the mapping to `new_pblkno`
6. bumps `next_free_pblkno` if carried in the record

The important rule is:

- remap-without-image replay first reads through the old mapping view
- it applies the WAL delta against that old physical baseline
- it publishes the new mapping only after that baseline has been consumed

That is what makes delta replay deterministic without requiring a full-page
image in the ordinary checkpoint-boundary case.

## 8. First-Born Pages

A first-born page is identified by:

- `old_pblkno == InvalidBlockNumber`

Current first-born handling is split:

- producer side may reserve and publish WAL-owned first-born remap metadata
- post-insert publication bumps logical frontier
- redo side uses `logical_nblocks` payload to keep replay-side logical EOF in
  sync

This avoids depending on generic `smgrextend()` ownership for WAL-owned logical
births.

## 9. Metadata Fork and Redo

Mapped-fork redo depends on metadata-fork availability.

The current rule is:

- redo creates metadata when mapped replay requires it
- normal data paths should not repeatedly rediscover metadata existence

This keeps redo-only bootstrap in redo owner code instead of leaking it into
unrelated access paths.

## 10. Current Conservative Choices

The branch still chooses conservative rules in a few places:

- explicit image owners keep their image semantics
- first-born and initialization cases carry dedicated frontier payload when it
  cannot be derived from a stronger WAL anchor
- converted core MAIN-fork hints use image-free `XLOG2_HINT_DELTA`; auxiliary
  and unconverted hint paths still use PostgreSQL's `XLOG_FPI_FOR_HINT`
- redo keeps a very explicit old-view/new-view split for remap-without-image
- remap format is record-level, so mixed birth/ordinary remap records can fall
  back to the full header rather than using per-block variant tags

Those rules are deliberate.  The current branch favors deterministic ownership
and clear replay state over collapsing every case into a smaller but harder to
reason about WAL contract.

## 11. Summary

The current Umbra WAL/redo design can be summarized as:

- ordinary block records may carry remap metadata
- remap metadata records physical transition plus frontier state
- WAL publication of that state is committed only after insert succeeds
- redo explicitly distinguishes no-remap, remap-with-image, and
  remap-without-image
- Umbra rmgr records remain available for MAP lifecycle operations outside the
  ordinary block-header remap path

That is the basis on which the current master PoC reduces ordinary
checkpoint-boundary backup-image pressure while keeping replay deterministic.
