# Umbra Prototype and Repository Navigation

This document explains how the current PostgreSQL master PoC relates to the
earlier PostgreSQL 12.2 shadow prototype.

## 1. Repository Layout

The public repository is intended to keep both the old prototype and the
current master-port implementation in one place, separated by Git branches
rather than by copying source trees into subdirectories.

Repository:

- `https://github.com/nayishan/postgre_umbra`

Expected branch roles:

- `umbra-poc-pgmaster`
  - PostgreSQL master based Umbra PoC
  - full implementation branch intended for community reading
  - includes MAP metadata, WAL/redo integration, mapwriter, compactor, tests,
    and documentation
- `shadow-pg12-archive`
  - archived PostgreSQL 12.2 shadow prototype
  - useful for understanding the original minimal idea without the full
    master-port integration burden

The important rule should remain:

- one repository
- separate branches
- clear README navigation
- no mixing PostgreSQL 12.2 prototype files into the PostgreSQL master branch

## 2. Why Keep The Prototype

The PostgreSQL 12.2 shadow prototype is useful because it shows the original
idea with fewer host-tree integration details.

It is not a substitute for the master PoC, but it helps answer questions such
as:

- what is the minimal logical-to-physical mapping idea?
- why does Umbra live below upper PostgreSQL logical block addressing?
- how did the MAP state-machine idea evolve?
- which parts are core design and which parts are master-port engineering?

The master PoC is much larger because it must deal with:

- current `smgr` boundaries
- WAL block registration
- redo paths
- checkpoint/writeback
- relation lifecycle
- skip-WAL relations
- background maintenance
- TAP recovery tests

## 3. How To Read The Two Branches

Read the branches in this order if the goal is to understand the design:

1. Read the repository `README.md` on `umbra-poc-pgmaster`.
2. Read `doc/umbra/ARCHITECTURE.md` for the current module boundaries.
3. Read `doc/umbra/WAL_AND_REDO.md` for the WAL and recovery model.
4. Read `doc/umbra/REVIEW_GUIDE.md` for suggested review entry points.
5. Read `doc/umbra/UMBRA_FPW_STORY_ZH.md` if the Chinese design story is useful
   context.
6. If anything is still unclear, go back to the shadow prototype for the
   minimal mapping idea.

The prototype should be treated as background material.  The master PoC is the
branch that should be used for current testing and review.

## 4. Development Transparency

The original design direction, boundary choices, and state-machine reasoning
come from the author.  The PostgreSQL 12.2 shadow prototype was used as an
important reference while building the master PoC.

The master-port implementation also used AI coding assistance extensively for
repetitive implementation work and for code shaped after both the prototype
and existing PostgreSQL subsystems.  That assistance was not sufficient to
reason independently about database-kernel concurrency, WAL ordering, or
recovery correctness.  The difficult part was repeatedly checking the logic,
finding incorrect assumptions, and correcting the implementation.
