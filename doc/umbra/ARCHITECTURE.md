# Umbra Architecture on PostgreSQL Master

This document describes the current module boundaries and ownership rules of
the PostgreSQL master Umbra PoC.

The main architectural intent is:

- upper PostgreSQL layers continue to speak in logical block numbers
- Umbra translates mapped forks to physical block numbers underneath
- the MAP subsystem owns persistent mapping facts
- `umbra.c` owns runtime interpretation of those facts
- `umfile.c` owns physical file operations

## 1. Top-Level Layers

Umbra is not a standalone engine next to PostgreSQL.  It is a storage-manager
variant integrated into:

- `smgr`
- WAL record assembly
- redo entry points
- checkpoint/writeback
- postmaster background workers

The main layers are:

- `src/backend/storage/smgr/umbra.c`
  - Umbra `smgr` implementation
- `src/backend/storage/smgr/umfile.c`
  - low-level physical file and segment manager
- `src/backend/storage/map/*`
  - shared MAP metadata, buffer, superblock, and background-maintenance logic
- `src/backend/access/transam/xloginsert.c`
  - producer-side remap-aware WAL assembly
- `src/backend/access/transam/xlogutils.c`
  - redo-side remap interpretation
- `src/backend/access/transam/umbra_xlog.c`
  - Umbra rmgr records for MAP lifecycle operations

## 2. Relation-Local Umbra State

`SMgrRelation` no longer carries a public Umbra-specific struct layout.

Umbra keeps its relation-local state behind `reln->umbra_private`, where
`umbra.c` stores:

- a borrowed `UmbraFileContext *`
- an explicit relation-local MAP state

The current MAP state is not derived from multiple booleans anymore.  It is an
explicit state machine:

- `UMBRA_MAP_POLICY_BYPASS_MAP`
- `UMBRA_MAP_POLICY_SKIP_WAL_PENDING_MAP`
- `UMBRA_MAP_POLICY_REQUIRE_MAP`

That state is seeded by create/open/redo owner points and then consumed by the
runtime access path.

## 3. Metadata Fork

Umbra uses an internal metadata fork to store:

- block 0: MAP superblock
- blocks 1..: MAP pages

The metadata fork is:

- internal to Umbra
- dense, not sparse
- special-cased in path and sync handling

The metadata fork stores mapping state for all three mapped forks, `MAIN`,
`FSM`, and `VM`, but it does not store their page contents.  `MAIN/FSM/VM`
remain relation forks addressed by logical block number at upper layers.  MAP
pages in the metadata fork only answer: for this fork and this logical block,
which physical block is current?

The layout is not three independent map forks.  It is one metadata fork with
fixed repeated groups:

- block 0: MAP superblock
- blocks 1..: repeated MAP page groups
- each group starts with 1 FSM map page
- then 1 VM map page
- then 8192 MAIN map pages

Each MAP page is a fixed-entry array, and each entry records one `lblk -> pblk`
mapping for the corresponding fork.  In that sense, the metadata fork is one
internal MAP file with formula-defined `mapfsm`, `mapvm`, and `mapmain` logical
regions.

Its page format is also not the same as ordinary PostgreSQL data pages.  Block
0 is packed as a 512-byte MAP superblock sector.  Blocks 1.. are compact
fixed-entry MAP metadata pages, closer in spirit to CLOG-style metadata than
to heap/index data pages.  They therefore do not use ordinary data-page
full-page-image semantics.

This matters because internal metadata forks must not be passed through generic
core helpers that only understand PostgreSQL's built-in forks.  Metadata path
construction stays inside Umbra-aware helpers such as `UmMetadataRelPathPerm()`
and related wrappers.

## 4. `umbra.c`: Runtime Storage Semantics

`src/backend/storage/smgr/umbra.c` sits at the `smgr` boundary.

It owns:

- access classification for a relation/fork
- mapped-vs-bypass decisions
- logical-block to physical-block translation
- publish/consume rules for mapped births and remaps
- metadata-fork lifecycle wrappers
- `FileTag` conversion for Umbra-managed files

It does not own:

- raw segment file management
- MAP page layout
- shared superblock table logic

The important separation in this file is:

1. classify runtime access state
2. consume MAP facts
3. issue physical I/O through `umfile`

That separation is why thin metadata wrappers such as `UmMetadataExists()` and
`UmMetadataRead()` are still useful: they keep the internal metadata fork
details localized instead of scattering `UMBRA_METADATA_FORKNUM` and dense-fork
assumptions across the tree.

## 5. `umfile.c`: Physical File Layer

`src/backend/storage/smgr/umfile.c` owns the physical side of Umbra storage.

It is responsible for:

- backend-local file context registry
- segment open/close management
- dense versus sparse physical existence semantics
- physical read/write/extend/zeroextend
- unlink, sync, delayed-unlink, and write-session helpers

Current writeback architecture uses `UmFileWriteSession` so callers such as MAP
flush pass only storage identity and block information.  The MAP layer no
longer needs to manipulate `UmbraFileContext` directly when flushing.

Checkpoint/bgwriter writeback uses `umfile_write_session_begin_uncached()`,
which intentionally avoids long-lived registry reuse in background processes.
That prevents stale relation-local file state from being kept across relation
lifecycle changes.

## 6. MAP Subsystem

The MAP subsystem is now split by functional domain.

### 6.1 `map.c`

This file now mainly owns:

- mapping lookup
- mapping allocation
- mapping publication
- truncate/lifecycle operations

### 6.2 `mapbuf.c`

This file owns MAP buffer-local state:

- buffer state bits
- pin/unpin
- MAP buffer I/O ownership
- using `MapMarkBufferDirty()` to ensure the corresponding metadata-fork block
  exists before an ordinary MAP page is marked dirty

The important rule here is:

- ordinary MAP page modifications must be dirtied through
  `MapMarkBufferDirty()`; if the metadata-fork block does not exist yet, that
  path creates the MAP block first
- checkpoint/writeback later writes existing blocks only

This mirrors ordinary buffer-pool ownership more closely than the older design
that allowed flush-time materialization.

### 6.3 `mapflush.c`

This file owns:

- checkpoint flush of MAP buffers
- checkpoint flush of superblocks
- mapwriter background flush of ordinary MAP pages

Current ownership rules are:

- mapwriter flushes regular MAP pages only
- checkpoint owns superblock flush
- flush writes existing metadata blocks
- flush no longer zeroextends missing metadata blocks on demand

### 6.4 `mapbgproc.c`

This file owns background maintenance:

- preallocation
- reclaim enqueue/dequeue work
- compactor stepping
- writer/compactor wakeup helpers

`mapwriter` and `mapcompactor` are now driven directly from the MAP layer
rather than through an `smgr` wrapper layer.

### 6.5 `mapclock.c`

This file owns:

- clock sweep victim selection
- MAP cache table
- sync-start reporting

### 6.6 `mapsuper.c`

This file owns:

- MAP superblock read/pack/CRC helpers
- shared `MapSuperEntry` hash-table management
- logical frontier, physical frontier, and allocator frontier updates
- runtime extending state for fork materialization

The current shared-entry model distinguishes:

- logical EOF (`logical_nblocks`) in the on-disk superblock
- materialized physical frontier (`phys_capacity` / physical nblocks) in the
  on-disk superblock
- committed allocator frontier (`next_free_phys_block`) in the on-disk
  superblock
- reservation frontier in `MapSuperEntry` runtime state only

That split is important both for correctness and for WAL/redo.

The allocator invariant is:

- committed `next_free_phys_block <= reservation frontier`

That should be asserted while holding `MapSuperEntry.lock`; reservation may run
ahead in shared memory, but checkpoint-visible superblock state must not.

### 6.7 `mapinit.c`

This file owns:

- shared-memory initialization
- backend initialization
- shared statistics
- GUC-backed globals

### 6.8 `mapinflight.c`

This file owns in-flight remap ownership tracking.

It uses per-MAP-buffer pending bits to serialize ownership of a logical MAP
entry while a backend is preparing or publishing a new physical mapping.  The
chosen physical block remains backend-local until WAL insertion commits the
owner state.

This mechanism is about ownership and barriers, not durable publication.  The
durable superblock frontier must not advance here.  Runtime reservation state
may run ahead in shared memory, but committed `next_free_pblkno` is published
later by WAL-owned commit/redo.

## 7. Checkpoint and Writeback Ownership

One of the largest recent architectural cleanups is the checkpoint/writeback
contract.

The current contract is:

- synthesized ordinary MAP pages carry `MAPBUF_NOT_MATERIALIZED`
- if the corresponding metadata-fork block does not exist yet, the first writer
  that dirties such a page creates that MAP block under the page content lock
- checkpoint/mapwriter later write the existing block only

That means:

- missing MAP-block creation is no longer hidden inside flush
- `MapFlushBuffer()` uses write-existing semantics
- background writeback no longer invents missing metadata blocks

This is the same broad ownership rule as the normal buffer pool:

- extension / MAP-block creation happens before writeback
- writeback persists existing dirty state

## 8. Background Processes

Umbra currently adds two background workers under `postmaster` when built with
`--with-umbra`:

- `mapwriter`
  - sync-start accounting
  - ordinary MAP page flush
  - preallocation
- `mapcompactor`
  - relocation and reclaim work

These processes call directly into the MAP layer rather than through a generic
`smgr` forwarding API.  That keeps ownership clearer:

- MAP background work belongs to the MAP subsystem
- `smgr` remains the storage-manager boundary for relation storage calls

## 9. WAL and Redo Boundaries

The current boundary is:

- `xloginsert.c`
  - decides whether a block record carries remap metadata
  - fills the remap header payload
  - commits mapping/frontier publication after WAL insertion succeeds
- `xlogutils.c`
  - ensures metadata and MAP state for redo
  - interprets remap-with-image and remap-without-image
  - temporarily reconstructs the old mapping view when needed
- `umbra_xlog.c`
  - handles Umbra rmgr records such as `MAP_SET`, range remap, and reclaim

The redo-entry layer owns remap interpretation because generic block-read
helpers do not know enough about:

- `has_remap`
- `has_image`
- `old_pblkno`
- `new_pblkno`
- frontier payload

The detailed rules are described in [WAL_AND_REDO.md](./WAL_AND_REDO.md).

## 10. Current Invariants

The current code relies on these invariants:

- metadata fork handling stays inside Umbra-aware helpers
- runtime access state is explicit, not reconstructed from multiple booleans
- ordinary MAP pages are materialized before flush
- checkpoint/mapwriter write existing metadata blocks only
- remap publication after WAL insertion is an owner action
- redo owns redo-only metadata bootstrap and remap interpretation
- skip-WAL dense-map WAL describes exact mapping/frontier facts, but does not
  replace the existing data-file sync protocol
- reclaim unlink requests are not removed at the checkpoint start that advances
  the cycle counter; physical unlink is eligible only in the later
  `SyncPostCheckpoint()` after a following checkpoint has completed
- full-page images are still kept for explicit image owners and WAL
  consistency checking

These invariants should be treated as design constraints when reviewing later
WAL-size optimizations.  For example, `next_free_pblkno` is a global allocator
frontier, not a value that can always be replaced by `new_pblkno + 1`.

## 11. Architectural Choices

The current PoC makes two deliberate architectural choices that are worth
stating explicitly.

### 11.1 Space Cleanup Policy

Once logical block numbering is decoupled from physical placement, Umbra has at
least two possible physical-space-management models:

1. immediately reuse freed physical blocks, closer to PostgreSQL's traditional
   reusable-space style; or
2. let the physical frontier move forward, while treating reclaim/reuse as a
   later background concern instead of a foreground allocation requirement.

The current PoC chooses the second model on purpose.

The reason is not that reuse is impossible, but that immediate reuse would push
substantial allocator complexity back into the foreground path:

- free-space accounting would become part of normal allocation decisions
- remap publication would need tighter coupling with reuse eligibility
- WAL/redo would need to preserve more allocator-state invariants
- in-flight ownership and recovery races would become harder to reason about

After Umbra has already decoupled logical identity from physical placement, the
main value of that decoupling is simplicity of ownership and correctness.  The
foreground path therefore prefers monotonic physical advancement, while
compaction/reclaim remain the place where old physical space is cleaned up and
made reusable later.

In other words:

- immediate physical reuse is not the primary design goal of the PoC
- correctness and simpler ownership are prioritized over aggressive reuse
- reclaim exists, but it is intentionally a background policy rather than a
  synchronous allocator contract

### 11.2 Double-Buffering Boundary

Umbra also deliberately keeps its buffering complexity inside Umbra-specific
layers instead of trying to collapse everything into PostgreSQL's generic
buffering model immediately.

This means the PoC tolerates a double-buffering shape:

- PostgreSQL keeps its ordinary upper buffer/cache behavior
- Umbra keeps its own MAP buffers, superblock shared state, in-flight tracking,
  and physical-file writeback state

That choice is intentional for three reasons:

1. it keeps Umbra-specific complexity inside Umbra rather than leaking remap,
   allocator, and metadata-lifecycle rules into generic PostgreSQL buffer
   ownership;
2. it allows the project to measure and understand the system-level impact of
   the extra buffering layer instead of assuming up front that it must be
   eliminated; and
3. it keeps the design open to future deployment models, including
   cloud-oriented environments where storage-side services and local caching
   boundaries may not match a traditional single-node assumption.

This is a design trade-off, not a claim that double buffering is always ideal.
The current PoC chooses modular isolation first, and leaves deeper buffer-model
consolidation as a later optimization/design question.

## 12. Open Architectural Debt

The codebase is much cleaner than the earlier PG18-era branch, but a few
medium size debts remain:

- `mapsuper.c` is now the largest MAP module and could later be split into
  on-disk-superblock helpers versus shared-super-entry management
- `mapbgproc.c` still combines preallocation, reclaim, compaction, and wakeup
  logic
- `umfile.c` still mixes context/session management with raw segment/file
  operations

Those are now refactoring opportunities, not immediate ownership bugs.
