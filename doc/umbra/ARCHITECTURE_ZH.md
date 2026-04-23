# Umbra 架构说明

本文档是 `ARCHITECTURE.md` 的中文配套版本，说明当前 PostgreSQL master
上的 Umbra 原型如何分层，以及各模块各自负责什么。

## 1. 总体目标

Umbra 不是 PostgreSQL 旁边的独立存储引擎，而是接在 PostgreSQL
`storage manager` 边界上的一个存储管理原型。

它的核心目标是：

- 上层 PostgreSQL 继续只使用逻辑块号；
- Umbra 在 `smgr` 下方把需要映射的 fork 翻译成物理块；
- MAP 子系统持久化 `lblk -> pblk` 映射；
- WAL 明确携带 remap 所需信息；
- redo 能在恢复阶段确定性地重建映射关系和页面内容。

## 2. 主要模块

主要代码路径如下：

- `src/backend/storage/smgr/umbra.c`
  - Umbra 的 `smgr` 实现；
  - 运行时访问策略；
  - 逻辑块到物理块的翻译。
- `src/backend/storage/smgr/umfile.c`
  - 物理文件层；
  - 段文件管理；
  - dense/sparse 存在性判断；
  - 同步、删除和延迟删除。
- `src/backend/storage/map/`
  - MAP 页；
  - MAP buffer；
  - superblock；
  - checkpoint / mapwriter 刷盘；
  - 预分配、回收、压实；
  - in-flight owner 跟踪。
- `src/backend/access/transam/xloginsert.c`
  - WAL 生成端的 remap 判定；
  - remap header 填充；
  - WAL insert 成功后的映射发布。
- `src/backend/access/transam/xlogutils.c`
  - redo 端对 remap 的解释与执行。
- `src/backend/access/transam/umbra_xlog.c`
  - Umbra 自己的 rmgr 生命周期记录。

## 3. relation-local 状态

Umbra 的 relation-local 状态挂在 `SMgrRelation->umbra_private` 后面，不把
Umbra 的内部结构暴露给普通 `smgr` 调用方。

当前访问策略使用显式状态，而不是由多个布尔值拼装：

- `UMBRA_MAP_POLICY_BYPASS_MAP`
- `UMBRA_MAP_POLICY_SKIP_WAL_PENDING_MAP`
- `UMBRA_MAP_POLICY_REQUIRE_MAP`

这些状态由 create/open/redo 对应的 owner 点建立，再由运行时访问路径消费。

## 4. metadata fork

每个 Umbra relation 都有一个内部 metadata fork：

- block 0 是 MAP superblock；
- block 1.. 是普通 MAP 页。

metadata fork 是 Umbra 自己的内部结构，不是普通 PostgreSQL 用户可见的
fork。因此 metadata 的路径、同步、删除以及 dense/sparse 语义都必须留在
Umbra-aware helper 中，不能泄漏到通用 fork helper。

metadata fork 同时保存 `MAIN`、`FSM`、`VM` 三类 mapped fork 的映射状态，但
不保存这些 fork 的页面内容。也就是说，`MAIN/FSM/VM` 仍然是上层按逻辑块号访问
的 relation fork；metadata fork 里的 MAP 页只回答“这个 fork 的某个逻辑块现在
对应哪个物理块”。

具体布局不是三个独立的 map fork，而是同一个 metadata fork 中的固定分组：

- block 0：MAP superblock；
- block 1..：重复的 MAP page group；
- 每个 group 先放 1 个 FSM map page；
- 再放 1 个 VM map page；
- 再放 8192 个 MAIN map page。

每个 MAP page 都由固定大小 entry 组成，每个 entry 记录对应 fork 中一个逻辑块
的 `lblk -> pblk` 映射。因此可以把 metadata fork 理解成一个内部 MAP 文件，
里面按稳定公式切出了 `mapfsm`、`mapvm`、`mapmain` 三类逻辑区域。

它的页面格式也不同于普通 PostgreSQL data page。block 0 是按 `512B`
sector 打包的 MAP superblock；block 1.. 是由固定大小 entry 组成的紧凑
MAP metadata page，语义上更接近 CLOG 这类 metadata，而不是 heap/index
data page。因此它们不使用普通 data-page full-page-image 语义。

## 5. MAP 子系统分工

当前 MAP 子系统按职责拆分如下：

- `map.c`
  - 查找；
  - 分配；
  - 映射发布；
  - truncate / 生命周期处理。
- `mapbuf.c`
  - MAP buffer 状态；
  - pin / unpin；
  - buffer I/O 所有权；
  - 通过 `MapMarkBufferDirty()` 保证普通 MAP 页标脏前，metadata fork 中已有
    对应物理 block。
- `mapflush.c`
  - checkpoint 刷盘；
  - mapwriter 刷盘；
  - superblock 刷盘。
- `mapbgproc.c`
  - 预分配；
  - 回收；
  - compactor；
  - writer / compactor 唤醒。
- `mapclock.c`
  - 时钟扫描；
  - MAP 缓存表；
  - sync-start 统计。
- `mapsuper.c`
  - superblock 的打包、解包和 CRC；
  - 共享 `MapSuperEntry` 表；
  - 逻辑 EOF、物理容量和分配前沿。
- `mapinit.c`
  - 共享内存初始化；
  - backend 初始化；
  - 由 GUC 驱动的全局状态。
- `mapinflight.c`
  - in-flight remap owner；
  - 写屏障；
  - pending 标记。

## 6. superblock 状态拆分

superblock 和共享 entry 里同时存在几类不同状态，这些状态不能混在一起：

- `logical_nblocks`
  - 逻辑 EOF；
  - 持久化在 superblock 中。
- `phys_capacity`
  - 已经完成物理物化的容量；
  - 持久化在 superblock 中。
- `next_free_pblkno`
  - 已提交的分配前沿；
  - 持久化在 superblock 中；
  - 由 WAL-owned commit / redo 发布。
- reservation frontier
  - 运行时的预留前沿；
  - 只存在于 `MapSuperEntry` 的共享状态里；
  - 不直接落盘。

关键不变量是：

```text
committed next_free_pblkno <= runtime reservation frontier
```

也就是说，预留前沿可以在内存里领先，但 checkpoint 可见的已提交前沿不能跑到
WAL 已经发布的状态前面去。

## 7. checkpoint 和回写

当前回写规则是：

- 修改普通 MAP 页的路径必须通过 `MapMarkBufferDirty()` 标脏；如果 metadata
  fork 中还没有对应物理 block，这条路径会先创建 MAP block；
- checkpoint / mapwriter 只写已经存在的脏 MAP 页；
- 刷盘阶段不创建缺失的 MAP 页；
- superblock 的刷盘归 checkpoint 所有。

这套规则刻意靠近 PostgreSQL 的普通 buffer pool：

- 创建缺失的 MAP block 属于写回之前的动作；
- 回写只负责把已经存在的脏状态持久化。

## 8. mapwriter 和 mapcompactor

Umbra 目前有两个后台 worker：

- `mapwriter`
  - 统计 MAP 分配压力；
  - 刷普通 MAP 页；
  - 做预分配。
- `mapcompactor`
  - 负责物理迁移；
  - 负责回收。

`mapwriter` 会扫描 `MapSuperEntry` 判断是否需要预分配，但它不负责把脏
superblock 持久化。脏 superblock 仍由 checkpoint 负责刷盘。

## 9. WAL / redo 边界

WAL / redo 的边界如下：

- `xloginsert.c`
  - 生成 remap header；
  - 在 WAL insert 成功后发布映射和 frontier。
- `xlogutils.c`
  - 在 redo 端解释 remap；
  - 区分 remap-with-image 和 remap-without-image。
- `umbra_xlog.c`
  - 记录显式的 MAP 生命周期事件。

redo 端必须理解 remap，因为普通的 block-read helper 并不知道：

- 当前记录是否带 remap；
- 是否带 image；
- 旧物理基线是什么；
- 新物理目标是什么；
- frontier payload 是什么。

## 10. 架构取舍

当前原型有两项刻意保留的架构选择，需要明确写出来。

### 10.1 空间清理策略

一旦逻辑块号和物理块位置解耦，Umbra 至少有两种物理空间管理方式：

1. 像 PostgreSQL 传统可复用空间那样，尽量立即复用已经释放的物理块；
2. 让物理前沿持续向前推进，把 reclaim / reuse 作为后续后台清理策略，而不是
   前台分配路径的同步要求。

当前原型有意选择第二种。

原因不是“不能复用”，而是如果前台路径立即承担复用，就会把一整套复杂度重新
拉回分配主路径：

- free-space accounting 会进入正常分配决策；
- remap 发布会和 reuse eligibility 更紧地耦合；
- WAL / redo 需要维护更多分配状态不变量；
- in-flight 所有权与恢复竞争会更难推理。

既然 Umbra 已经把逻辑身份和物理位置解耦，那么这层解耦带来的一个核心收益，
就是所有权边界和正确性规则更简单。因此当前原型选择：

- 前台路径优先让物理块单调向前推进；
- reclaim / compaction 负责后续清理旧物理空间；
- 复用存在，但它属于后台策略，不是前台同步 contract。

换句话说：

- 当前原型的首要目标不是“立即复用物理块”；
- 当前优先级是正确性和更简单的所有权边界；
- reclaim 确实存在，但它被刻意放在后台，而不是前台分配主路径上。

### 10.2 双层 buffer 边界

Umbra 还刻意保留了一层内部缓冲复杂度，而不是一开始就试图把所有东西直接压进
PostgreSQL 的通用 buffer 模型。

这意味着当前原型接受一种双层缓冲形态：

- PostgreSQL 保留上层通用 buffer / cache 行为；
- Umbra 保留自己的 MAP buffer、superblock 共享状态、in-flight 跟踪，以及
  物理文件回写状态。

这样做有三个原因：

1. 尽量把 Umbra 特有的 remap、分配器、metadata 生命周期复杂度封装在
   Umbra 内部，而不是泄漏到 PostgreSQL 的通用 buffer 所有权模型中；
2. 先实际观察双层 buffer 对整个系统的影响，而不是预设它一定必须被消除；
3. 给未来的部署模型留空间，包括云原生场景里可能出现的存储侧服务与本地缓存
   边界；这些场景未必符合传统单机、单层 buffer 的假设。

这是一种工程取舍，不是说双层 buffer 永远最优。当前原型的选择是：

- 先保证模块隔离和语义清晰；
- 更深入的 buffer 模型合并，留作后续优化和设计问题。

## 11. 当前架构债务

当前仍有一些工程债：

- `mapsuper.c` 仍然偏大；
- `mapbgproc.c` 同时包含预分配、回收、压实和唤醒逻辑；
- `umfile.c` 同时包含 context / session 以及底层段文件操作；
- compactor / reclaim 还不是最终的生产级空间管理。

这些目前是后续重构点，不是当前原型最核心的正确性阻塞项。
