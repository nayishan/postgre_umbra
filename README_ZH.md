# Shadow 在 PostgreSQL 12.2 上的存储管理原型

[English](./README.md) | [中文](./README_ZH.md)

这个分支是早期的 Shadow 原型，基于 PostgreSQL 12.2。

Shadow 的目标很直接：在 PostgreSQL 的 `smgr` 层下面，为每个需要保护的
relation block 保留两份物理页，并用一个 1-bit 元数据记录当前有效的是哪一份。
当 PostgreSQL 本来会因为 checkpoint 边界而写 full-page image 时，Shadow
改为在 WAL block reference 中记录一次 shadow 切换；后续脏页刷盘写到另一份
物理页。这样 redo 可以从旧物理页出发应用 WAL delta，而不是依赖 WAL 里的
8KB full-page image。

这个分支的价值是展示最小可行思路。它不是当前完整实现。当前完整实现放在
`umbra-poc-pgmaster` 分支，名称为 Umbra。

原始 PostgreSQL 顶层说明仍保留在 [README](./README)。

## 分支布局

- `shadow-pg12-archive`
  - PostgreSQL 12.2 上的 Shadow 原型归档分支。
  - 适合阅读最小设计：双物理页、1-bit 状态、WAL shadow 标记、redo 时切换读写视图。
- `umbra-poc-pgmaster`
  - PostgreSQL master 上的 Umbra 原型分支。
  - 是 Shadow 思路的完整工程化版本，包含 MAP 元数据、remap WAL、mapwriter、
    compactor、测试和文档。

## 原型目标

当时 Shadow 原型有两个目的：

1. 验证核心逻辑的完整性，也就是 shadow page 切换是否足以支撑 crash recovery
   正确性。
2. 得到初步性能结果，为后续更完整的设计提供方向。

## 初步性能数据

当时的 benchmark 对比了普通 PostgreSQL `full_page_writes=on`、普通
PostgreSQL `full_page_writes=off` 和 Shadow。

当时记录的 PostgreSQL 配置是：

| Setting | Value |
| --- | --- |
| `checkpoint_timeout` | `2min` |
| `max_wal_size` | `20GB` |
| `shared_buffers` | `50GB` |
| `logging_collector` | `on` |

当时记录的 workload 配置是：

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

这里 `On` 表示普通 PostgreSQL 且 `full_page_writes=on`，`Off` 表示普通
PostgreSQL 且 `full_page_writes=off`，`Shd` 表示 Shadow。这个表应理解为早期
原型的初步结果，不是当前实现的性能承诺。

## Mental Model

Shadow 管理 `MAIN`、`FSM`、`VM` 三类 fork。每类 fork 都有一个对应的 shadow
fork：

| Logical fork | Shadow fork |
| --- | --- |
| `main` | `shd` |
| `fsm` | `fsm_shd` |
| `vm` | `vm_shd` |

对上层 PostgreSQL 来说，relation 仍然按普通 `(RelFileNode, ForkNumber,
BlockNumber)` 访问。Shadow 在 `smgr` 层决定实际读写普通 fork 还是 shadow
fork。

每个被 Shadow 管理的 block 有一个状态 bit：

- `PING` 表示当前有效页在普通 fork；
- `PONG` 表示当前有效页在 shadow fork。

当一个 WAL record 需要保护某个 block 的 checkpoint 前镜像时，Shadow 不把该
block 的 full-page image 写入 WAL，而是在 WAL block header 中加
`BKPBLOCK_HAS_SHADOW`，并附带目标状态。WAL insert 成功后，Shadow 切换该
block 的状态 bit。之后 buffer flush 会把新页写到目标 fork，旧 fork 里的页
仍然作为 redo 基线保留。

redo 遇到 `BKPBLOCK_HAS_SHADOW` 时，会先把状态临时切回旧页，读取旧页作为
delta replay 的输入；随后再恢复 WAL record 记录的新状态，让 redo 结果写回
新页。

## Code Map

主要代码路径：

- `src/backend/storage/smgr/shadow.c`
  - Shadow `smgr` 实现；
  - 双 fork 文件读写；
  - block 状态 bit 的读写和切换；
  - Shadow metadata 的 SLRU 初始化、checkpoint 和 shutdown。
- `src/backend/storage/smgr/smgr.c`
  - 接入 Shadow storage manager；
  - 扩展 `smgr` API，使 WAL、relation lifecycle 和 database lifecycle 能访问
    Shadow metadata。
- `src/include/common/relpath.h`
  - 增加 `SHADOW_FORKNUM`、`FSM_SHADOW_FORKNUM`、
    `VISIBILITYMAP_SHADOW_FORKNUM`。
- `src/common/relpath.c`
  - 增加物理文件后缀：`shd`、`fsm_shd`、`vm_shd`。
- `src/backend/access/transam/xloginsert.c`
  - 在原本需要 full-page write 的普通路径上写入 `BKPBLOCK_HAS_SHADOW`；
  - WAL insert 成功后切换 block 状态 bit。
- `src/backend/access/transam/xlogreader.c`
  - 解码 shadow block header 和附带的 block metadata。
- `src/backend/access/transam/xlogutils.c`
  - redo 时按 shadow 状态读取旧页，再切换回新状态。
- `src/backend/catalog/storage.c`
  - relation 和 database 创建/删除时维护 Shadow metadata。
- `src/bin/initdb/initdb.c`
  - 初始化 `pg_shadow/db`、`pg_shadow/rel`、`pg_shadow/block` 目录。

## Metadata Layout

Shadow 使用集群级 SLRU metadata，而不是每个 relation 自带 metadata fork。

数据目录下会出现：

- `pg_shadow/db`
  - database OID 到 Shadow database id 的映射。
- `pg_shadow/rel/shd_rel_<n>`
  - 每个 Shadow database id 下的 relation OID 到 local relation id 的映射。
- `pg_shadow/block/shd_blk_<n>`
  - 每个 managed relation/fork group 的 block 状态 bit。

当前容量是硬编码的：

- `MAX_SHD_DBS = 9`
- `SHD_MAX_REL_PER_DB = 2048`
- `NUM_FORK = 3`

也就是说，这个原型只能覆盖很小的固定 database/relation 空间。超过限制时会
触发 checkpoint 重试，仍无法分配时会 `PANIC`。

## Build And Run

这个分支没有 `--with-shadow` 配置项，也没有运行时切换开关。

`src/include/miscadmin.h` 中 `SMGR_WHICH` 被硬编码为 `1`，表示默认使用 Shadow
storage manager。因此按普通 PostgreSQL 12.2 源码构建即可：

```sh
./configure --prefix="$PWD/install"
make -j
make install
```

初始化和启动也使用普通 PostgreSQL 命令：

```sh
./install/bin/initdb -D data
./install/bin/pg_ctl -D data -l logfile start
```

`initdb` 会额外创建 `pg_shadow` metadata 目录。运行时应保持
`full_page_writes=on`，因为 Shadow 正是借用 PostgreSQL 原本“需要 FPW”的判断
点来决定何时做 shadow 切换。

## 使用限制

这是一个研究原型，限制很多：

- 基线是 PostgreSQL 12.2，不是当前 PostgreSQL master。
- Shadow 是硬编码启用的，不能在同一套二进制里和普通 `md` storage manager
  做运行时切换。
- 只管理非临时 relation；temp relation 不走 Shadow。
- `INIT_FORKNUM` 不走 Shadow。
- 只覆盖 `MAIN`、`FSM`、`VM` 三类 fork。
- 每个 managed block 固定保留两份物理页，因此空间开销接近 managed fork 的
  两倍，再加上 `pg_shadow` metadata。
- database 和 relation 容量是硬编码的小上限。
- 没有 MAP compaction、空间回收、后台 mapwriter 或预分配机制。
- WAL 只记录 shadow 状态切换，不记录通用 `logical block -> physical block`
  映射。
- `REGBUF_FORCE_IMAGE` 等路径仍保留 full-page image；这个原型不是全局关闭
  FPW，也不是完整替代所有 FPI 场景。
- `xloginsert.c` 中仍保留早期假设：普通页面修改需要能由 WAL delta 正确重放；
  依赖 FPI-only 语义的 WAL record 没有被系统化处理。

## Shadow vs Umbra

Shadow 和 Umbra 解决的是同一个核心问题：怎样减少 checkpoint 边界普通更新
对 full-page image 的依赖，同时保留 crash recovery 的确定性。

差别在于抽象层级。

| Area | Shadow | Umbra |
| --- | --- | --- |
| PostgreSQL baseline | PostgreSQL 12.2 | PostgreSQL master |
| Physical model | 每个逻辑块固定两份物理页：普通 fork 和 shadow fork | 逻辑块通过 MAP 指向任意物理块 |
| Metadata | 集群级 `pg_shadow` SLRU，记录 db/rel/block 状态 | 每个 relation 的内部 metadata fork，包含 MAP superblock 和 MAP page |
| Block state | 1 bit：`PING`/`PONG` | `lblk -> pblk` 映射和 fork-level 状态 |
| WAL record | `BKPBLOCK_HAS_SHADOW` + `grelId/status` | block reference remap metadata + Umbra rmgr lifecycle records |
| Space management | 固定双份页，没有压实回收 | 支持物理分配、预分配、回收和 compaction |
| Background worker | 无 | `mapwriter`、`mapcompactor` |
| Enablement | `SMGR_WHICH` 硬编码为 Shadow | `--with-umbra` 构建选项 |
| Scope | 最小原型，便于理解核心想法 | 更完整的工程化实现，面向阅读、测试和审阅 |

可以把 Shadow 看成 Umbra 的最小思想实验：

1. 上层仍然使用逻辑 block number。
2. `smgr` 在下方决定实际物理位置。
3. WAL 必须携带足够信息，让 redo 知道该从哪份旧页开始回放。
4. redo 结果不能破坏旧基线，必须发布到新的物理位置。

Umbra 保留这些核心原则，但把“两个固定物理位置”推广成通用 MAP，把硬编码容量
和简单 SLRU 状态推进成 relation-local metadata、WAL lifecycle、后台维护和
可测试恢复路径。
