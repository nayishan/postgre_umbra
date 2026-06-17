# Umbra 用 remap 替代 ordinary checkpoint-boundary FPW 路径的中文说明

[English](./UMBRA_FPW_STORY.md)

## 1. 背景：Umbra 挑战的是哪一段 FPW 成本

PostgreSQL 当前依赖 full-page writes（FPW）来保证崩溃恢复正确性。其基本做法是：在 checkpoint 之后，某个页面第一次被修改时，把整页镜像写入 WAL，用它作为新的恢复基线。

Umbra 当前实现挑战的，不是“全局取消所有 full-page image”，而是 ordinary checkpoint-boundary 这条默认 image 路径。对满足自动 remap 条件的普通数据页 WAL 记录，Umbra 用 remap-aware recovery metadata 取代默认 full-page image；但以下保守路径仍然保留 image 语义：

- `REGBUF_FORCE_IMAGE`
- `XLR_CHECK_CONSISTENCY`
- `XLOG_FPI_FOR_HINT`

因此，Umbra 当前更准确的目标是：

- 不去宣称“所有 FPW 都消失了”
- 而是为 ordinary checkpoint-boundary case 提供另一种恢复基线表达方式

这套做法之所以值得尝试，是因为 stock md 的 ordinary checkpoint-boundary image path 确实绑定了几类反复出现的成本：

- checkpoint 边界后的 first-dirty 路径会引入一条额外的 owner path，但更值得强调的是它背后绑定的 I/O 成本
- WAL 会因为 full-page image 明显膨胀，从而带来更高的 WAL 写入与同步压力
- 数据文件侧的写放大和 WAL 侧的写放大会叠加出现
- 在更新密集型 workload 下，这类 I/O 压力会在每个 checkpoint 区间反复出现

Umbra 的目标，不是对这条路径做局部微调，而是尝试用不同的恢复基线表达方式来接管它。

## 2. 当前范围、非目标和术语约定

下面先把当前范围、非目标和术语约定说清楚。

当前范围是：

- 目标是用 remap-based recovery metadata 接管 ordinary checkpoint-boundary FPW 的默认 image 路径
- 讨论对象是 PostgreSQL `storage manager` / 物理存储层原型，而不是新的 table AM 或通用“存储引擎”
- 本文里的 `P1-P9` 用来描述当前 PoC 分支的语义拆分，不用来描述任意工作分支与 patch 编号的一一对应关系

当前非目标或保守保留边界是：

- 不宣称取消所有 full-page image；`REGBUF_FORCE_IMAGE`、`XLR_CHECK_CONSISTENCY`、`XLOG_FPI_FOR_HINT` 等路径仍保留 image owner
- 不把 compactor、AIO、主备物理页一致性、`CREATE DATABASE` 复制路径、显式 range-born 协议说成已经工程化收敛的能力
- 不把 `md + fpw=off` 当成 correctness-equivalent baseline

本文中几个高频术语的约定是：

- `birth`：一个逻辑页第一次获得持久 `lblk -> pblk` 关系
- `remap`：已有逻辑页切换到新的物理页
- `mapset`：直接发布一条映射关系
- ordinary checkpoint-boundary FPW：checkpoint 之后 ordinary first-dirty 默认走 image 的那类路径
- reclaim boundary：可以安全推进 reclaim / unlink 的物理边界
- `compactor`：后台整理器，负责扫描稀疏区域并把仍然 live 的页面搬走
- `reclaim`：生命周期动作，负责在某段物理空间确认没有 live mapping 后进入
  安全删除 / unlink 路径

## 3. 设计边界：崩溃恢复核心收敛在 storage metadata 和 WAL

Umbra 的实现范围是刻意克制的。它不试图重写 PostgreSQL 的执行层，也不希望把变更扩散到更高层的大面积抽象中。

更准确地说，当前实现讨论的不是一个新的 table AM，也不是独立于 PostgreSQL 的通用“存储引擎”；它更接近 PostgreSQL `storage manager` / 物理存储层上的一个原型。上层仍然使用逻辑块号，而 Umbra 在 `smgr` 之下为 mapped fork 提供 `lblk -> pblk` 的翻译。

从 crash recovery 的角度看，它的 correctness core 主要收敛在两层：

- storage metadata
- WAL

其中，storage metadata 又分成两类对象：

- per-page map entry
- fork-level superblock

这里说的 crash-recovery core，不是指只有这三个模块参与恢复，而是指崩溃后
redo 要恢复出正确页面内容时，最小的持久事实来自三类信息：

- map entry：说明某个逻辑块当前应该对应哪个物理块
- superblock：说明这个 fork 的全局边界状态，例如 logical EOF、physical
  capacity、已提交 allocator frontier
- WAL：说明哪些 map entry / superblock / 物理页生命周期变化已经被原子发布，
  以及 redo 必须按什么顺序重放它们

只要这三类事实在 redo 中保持一致，ordinary remap 更新就可以按“旧物理页 +
WAL delta”恢复，而不需要 checkpoint-boundary full-page image 作为恢复基线。

但如果讨论的是运行时并发正确性，当前实现还显式依赖一层额外机制：

- inflight claim / barrier

它不属于 WAL 编码本身，也不是恢复日志里的持久真相；它负责把前台 remap、
后台 compactor relocation、以及物理写入之间的发布顺序串行化，保证之后写入
WAL 的持久事实本身是成立的。因此更准确的说法是：

- crash-recovery core 的持久事实主要来自 `map entry + superblock + WAL`
- runtime concurrency correctness 还显式依赖 inflight / barrier

## 4. map entry：负责单页映射真相

Umbra 的核心抽象，是把逻辑页身份和物理放置拆开。

在这套模型下：

- 逻辑页代表上层真正关心的数据页身份
- 物理页只是当前承载该逻辑页内容的落盘位置
- map entry 负责记录当前 `lblk -> pblk` 的对应关系

因此，单个页面当前映射到哪个物理页，由 map entry 给出。它解决的是单页级别的局部真相。

## 5. superblock：负责 fork 级全局真相

仅有 per-page map entry 还不够，因为很多正确性并不是单个映射能表达的，而是整个 fork 的边界状态决定的。这部分由 superblock 承担。

superblock 是 Umbra 里的 fork-level correctness anchor，负责维护至少以下几类状态：

- 当前 fork 的逻辑边界，例如 `logical_nblocks`
- 已经提交的物理分配边界，例如 `next_free_pblkno`
- 当前已经 materialized 到哪里的物理容量状态
- reclaim / unlink 可以推进到哪条安全边界
- redo 需要恢复的 fork-level frontier facts

这里需要额外强调一个容易混淆的点：运行时还需要一个只存在于
`MapSuperEntry` shared state 里的 reservation frontier，用来给并发前台分配
新 `pblk`。这个 frontier 不落盘、不参与 checkpoint；落盘进入 superblock 的
`next_free_pblkno` 只能表示 committed frontier。实现上应满足并显式断言：
`committed next_free <= reservation frontier`。

换句话说：

- map entry 负责单页映射真相
- superblock 负责全局边界真相

很多 extend、truncate、reclaim、unlink、redo 相关的正确性，最终都依赖 superblock 才能成立。

## 6. WAL：负责动作的原子发布与可恢复性

storage metadata 负责描述状态本身，但仅有状态还不够。Umbra 要接管 ordinary checkpoint-boundary image path，就必须保证以下动作可以被原子地发布，并在 redo 中被一致地重建：

- birth
- remap
- mapset
- 与之相关的 committed frontier、logical size、capacity 等 fork-level 状态推进

这就是 WAL 层存在的理由。

对被 Umbra 接管的 ordinary checkpoint-boundary 路径来说，恢复正确性不再依赖“这条记录是否携带 full-page image”，而是依赖：

- map entry 和 superblock 描述的状态是否正确
- 这些状态变化是否被 WAL 作为原子事件记录并重放

但这不应被扩写成“Umbra 的所有恢复路径都不再依赖 image”。保守 image owner 仍然存在，上面列出的几类路径仍保持 PostgreSQL 原有的 image 语义。

### 6.1 remap 前旧物理页的生命周期

ordinary remap 里的旧物理页不是一个可以立刻丢掉或复用的临时页。它在 remap
发生前，是 map entry 当前指向的 committed physical baseline，也是无 image
delta redo 可能需要读取的旧基线。

这条生命周期可以按下面几步理解：

- remap 发布前：`old_pblk` 仍然是 `lblk` 的当前持久映射；即使 backend 已经
  选出了 `new_pblk`，也不能把 `old_pblk` 当成空闲页处理
- WAL insert 成功后：remap record 把 `old_pblk -> new_pblk` 的转换作为原子
  事件发布；正常运行时 map entry 会切到 `new_pblk`
- crash recovery 时：如果这是 no-image remap，redo 先通过 `old_pblk` 读取旧
  物理页，把 WAL delta 作用在这个旧基线上，然后再发布 `new_pblk`
- remap 发布后：`old_pblk` 不再是该逻辑页的当前映射，但它也不会进入前台
  复用路径；它只能作为后台空间整理的候选对象，受 live mapping、reclaim
  boundary、checkpoint 和 redo 语义共同约束

所以，Umbra 不是把“旧页覆盖写”改成“旧页立即复用”。它真正改变的是恢复基线：
旧物理页在 WAL record 需要时仍然保留为可读取基线，新物理页则通过 remap
成为新的当前映射。

## 7. 前台策略：关键路径只分配新页，不处置旧页

如果目标是把 ordinary checkpoint-boundary 的 first-dirty 路径变轻，就不应该再把“即时回收旧页、寻找可复用页、同步整理空间”这些工作塞回前台。

Umbra 在前台路径上的取舍是：

- 前台总是拿新物理页
- 旧物理页在前台路径上不会被即时复用；前台只发布新映射，不负责处置旧页
- 前台不负责即时空间整理

也就是说，前台更接近单调前进的 frontier 分配。旧页不会在热路径上被重新拿来写，前台也不会顺手把旧页整理掉。

这不是说系统永远不处理旧页，而是说前台不承担旧页处置和空间收敛。后续是否整理、何时 reclaim / unlink，由后台策略决定。在容量压力较高时，前台仍可能触发一次 one-shot preallocation，但它不会把长期空间整理重新拉回热路径。

## 8. MAP buffer 和 mapwriter：新增复杂度主要被限制在 MAP 元数据层

逻辑页和物理页拆开之后，MAP 元数据成为一等持久元数据。它需要：

- 自己的缓存
- 自己的 I/O 状态
- 自己的刷写和扩张维护路径

因此，在 PostgreSQL 原有的数据页 buffer pool 之外，Umbra 额外增加了一套专门服务于 MAP 元数据的 buffer cache。这个双层 buffer 问题不能完全消除，但新增的 buffering 主要被限制在 MAP 元数据层，而不是扩散成第二套通用数据页缓存。

这里也可以更直白地描述：`mapwriter` 可以看成是仿照 PostgreSQL `bgwriter` 的一套 MAP 后台写回机制，但它比 `bgwriter` 额外多承担了一项工作：在低水位附近为 mapped fork 做后台 physical preallocation。当前代码里的 contract 更接近：

- ordinary MAP page 在 first dirty 时 materialize，而不是等 mapwriter / checkpoint 再去创建物理块
- checkpoint 和 mapwriter 只刷“已经存在”的 ordinary MAP metadata block
- superblock flush 仍由 checkpoint 拥有
- mapwriter 负责 ordinary MAP flush
- mapwriter 还负责后台 preallocation / physical capacity 扩张
- 前台在低水位压力过高时，仍可能自己做一次 one-shot preallocation

因此，mapwriter 可以被理解成“MAP 元数据层上的 `bgwriter` + 物理容量预分配器”。但它不是 logical EOF 的 owner，不负责 superblock 的 checkpoint flush，也不是普通数据页写线程。

## 9. compactor：负责长期空间收敛

前台采用单调前进分配的好处是热路径简单，代价是物理布局会随着时间推移逐渐稀疏化。仅靠“随手回收”并不足以让长期空间占用收敛，因此需要一个后台进程负责把 live page 从稀疏 extent / segment 里迁走，最终为 reclaim 和 segment unlink 创造条件。

这个进程就是 compactor。它不是 crash-recovery core 本身，但它是让“前台只发布新映射、不处置旧页”这条策略在长期空间占用上可持续的关键后台机制。

这里的 compactor 和 reclaim 不是同义词。compactor 的主要动作是扫描、选择候选
extent、relocate live page，并在条件满足时推进 reclaim boundary；reclaim 则是
后续生命周期动作，只有在某个 segment 已经低于 reclaim boundary 且确认没有 live
mapping 引用时，才把真正的物理 unlink 交给 sync-request / checkpointer 路径。

不过，当前实现里 compactor 的首要目标并不是“尽可能快地清理干净”，而是“尽量不要干扰前台”。更准确地说，它现在是一个 best-effort、bounded、遇忙就退的后台整理器，而不是一个 aggressively reclaim 的空间清扫器。

当前实现里，它大致按下面的顺序工作：

- 先扫 MAP，按 extent 统计 live block 密度，找出 live 比例很低的候选区域
- 只处理已经落在 reclaim boundary 之下的区域，并显式避开当前物理尾部
- 对候选 extent 里的 live page 做 relocation，把映射切到新的物理页
- 当某个 extent / segment 被搬空后，再把真正的 reclaim / unlink 延后交给后续队列处理

如果从“不要干扰前台”这个角度看，当前实现最重要的约束其实是这些：

- 当前台分配压力升高时，compactor 会直接跳过这一轮，而不是继续和前台抢资源
- 每轮只处理有限数量的 relation，也只允许有限数量的 relocation move，而不是无限制地清理
- 关键路径上的锁获取大量使用 conditional acquire；遇到正在被别人使用的 superblock 或 MAP buffer，当前实现更倾向于跳过，而不是等待
- relocation 只有在旧映射仍然是当前已发布真相时才会提交；如果前台已经赢了更新，compactor 就放弃这次搬迁
- 真正的 segment unlink 也不是 compactor 当场同步执行，而是通过后续 reclaim / sync-request 路径延后处理

这套取舍意味着：当前 compactor 更像“温和地给前台让路”，而不是“最大化后台清理吞吐”。它首先保证前台的分配和写入不被后台整理明显拖慢。

## 10. inflight / barrier：负责前后台迁移并发的一致性

一旦前台和 compactor 都可能迁移同一个逻辑页，就必须有共享状态描述“这个逻辑页的迁移已经在进行中”。这就是 inflight / barrier 的职责。

当前实现里，这层机制之所以是显式的，是因为 compactor relocation 目前仍是 raw physical copy，而不是 shared-buffer-aware copy。没有额外串行化的话，前台 remap、后台 relocation、以及物理写入就可能围绕同一个 `lblk` 发生冲突。

inflight / barrier 解决的不是空间管理策略，而是迁移动作的并发一致性：

- 防止同一个 `lblk` 被并发发布多个新映射
- 让 loser 等到稳定的 committed MAP truth，而不是借用别人的 owner-local target
- 把前后台冲突收敛成 owner / claim / barrier 语义

因此，更准确的划分是：

- `map entry + superblock + WAL` 定义 crash recovery 需要恢复的持久状态真相
- inflight / barrier 定义运行时如何安全地发布这些状态变化

前者解决“redo 最终要恢复什么”，后者解决“并发执行时谁可以发布这个变化”。

## 11. 文件删除与 segment 生命周期

Umbra 里的文件删除不是普通的 unlink 问题，而是 segment 生命周期何时进入安全删除边界的问题。

这至少涉及两类场景：

- truncate / drop 驱动的删除
- compactor 整理后触发的 reclaim 删除

当前分支中，对外更适合描述的 contract 不是“pending 规则的全部细节”，而是下面这条更稳定的边界：

- superblock 维护 reclaim boundary
- compactor 通过已发布的 live mapping 和 live-map scan 判断候选区域是否仍有
  live page，并把 live page 搬走
- reclaim 在 segment 已经低于 reclaim boundary 且没有 live mapping 引用时，
  才注册后续物理 unlink
- 真正的物理 unlink 通过 PostgreSQL 的 sync-request / checkpointer 路径延后执行
- redo 侧要能接受这套生命周期边界，而不是只看“文件现在是不是空的”

sync-request 延后执行还有一条 checkpoint epoch 规则。
`SyncPreCheckpoint()` 会先吸收 checkpoint 开始前已经到达的 request，然后推进
checkpoint cycle counter。checkpoint A start 之后注册的 reclaim unlink request
会被标记为 A 的 cycle。A 自己的 `SyncPostCheckpoint()` 不能删除这个 request，
因为 A 还没有为它提供一个已经完成的 checkpoint 边界。只有后续 checkpoint B
完成并进入 `SyncPostCheckpoint()` 之后，这个 request 才进入可删除状态。

按时间线看就是：

```text
A start: cycle 5 -> 6
request 注册为 cycle 6
A end/post: request cycle 仍然是当前 cycle，所以不 unlink
B start: cycle 6 -> 7
B end/post: request cycle 6 已经旧于当前 cycle 7，所以可以执行 unlink
```

因此，正确性判断点不是 B start 时 counter 递增的瞬间，而是 B 已经完成
checkpoint 并进入 post-checkpoint unlink 处理之后。

inflight / pending 状态当然仍然影响内部正确性，但它们更适合被看作实现细节，而不是对社区信件里要展开的主判据。对外最重要的点仍然是：

- unlink 不是“看空即删”
- unlink 受 reclaim boundary、live mapping、checkpoint 和 redo 语义共同约束

## 12. 验证现状

当前 PoC 的验证重点是“这套 owner / recovery model 在已覆盖路径上是可执行的”，而不是“所有边界都已经被穷尽证明”。

目前至少已经覆盖了下面这类基础验证：

- `md` 模式下的 `make check`
- `md` 模式下的 `src/test/recovery check`
- `Umbra` 模式下的 `make check`
- `Umbra` 模式下的 `src/test/recovery check`

Umbra 专用 recovery TAP 进一步覆盖了几类与本文主线直接相关的主题，包括：

- MAP superblock / map fork policy / mapwriter activity
- truncate / remap / 2PC remap / skip-WAL dense map redo
- reclaim / internal segment unlink / compactor relocation
- range remap zeroextend / ordinary slim block remap / compact birth block remap

这些验证更适合支撑下面这条表述：

- 这套 PoC 已经不是只停留在设计层，而是在当前覆盖路径上具备可编译、可回归、可恢复的最小闭环

但下面这些点仍不能仅凭现有测试就宣称已经完全收敛：

- 主库和备库 checkpoint 节奏不同时，物理页对齐问题的系统性证明与更强测试覆盖
- `CREATE DATABASE` 的复制策略：当前支持 `FILE_COPY`；尚未支持 `WAL_LOG`，启用
  Umbra storage manager 时会回退到 `FILE_COPY`
- 显式 `range-born / batch mapping publish` 所有权模型
- 内部 metadata fork / MAP fork 跨 `RELSEG_SIZE` segment 边界（例如超过 `1GB`）的专项验证
- 更完整的原生 AIO 路径与更强的方法学压力测试

## 13. 性能观察（定性，不构成严格 benchmark 结论）

本节只提供当前 PoC 的定性性能信号，而不试图给出严格 benchmark 结论。方法学目前仍然偏薄：这里缺少完整硬件说明、重复轮次、误差/方差范围，以及针对各子机制的 ablation，因此下面的数据更适合作为“方向性观察”，而不是可直接用于社区性能结论的正式方法学结果。

当前更稳妥的性能叙事，不应该把 `md + fpw=off` 当成 correctness-equivalent baseline。真正公平、语义对等的默认比较对象是：

- `md + fpw=on`

而下面这个点更适合作为“机制上界 / sensitivity point”：

- `md + fpw=off`

在 `master`、相同 workload 下，我们比较了三种模式：

- `md + fpw=on`
- `md + fpw=off`
- `Umbra + fpw=on`

共同测试条件如下：

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

为了避免只看百分比，下面先直接列出原始吞吐结果。

`checksum=off`

| 并发 | `md + fpw=on` | `md + fpw=off` | `Umbra + fpw=on` |
| --- | ---: | ---: | ---: |
| 10 | 158709 | 154283 | 155781 |
| 50 | 577005 | 626954 | 656353 |
| 200 | 641899 | 981436 | 995635 |
| 500 | 322660 | 943295 | 859058 |
| 1000 | 275609 | 899631 | 729989 |

`checksum=on`

| 并发 | `md + fpw=on` | `md + fpw=off` | `Umbra + fpw=on` |
| --- | ---: | ---: | ---: |
| 10 | 155754 | 152025 | 150606 |
| 50 | 601974 | 635597 | 650844 |
| 200 | 621176 | 1015923 | 938311 |
| 500 | 316950 | 972795 | 729801 |
| 1000 | 282713 | 891770 | 674865 |

如果只看 `md + fpw=on` 与 `Umbra + fpw=on` 之间的 WAL 体积差异，在相同事务量下，按 `WAL(md + fpw=on) / WAL(Umbra + fpw=on)` 计算，得到的比值如下：

`checksum=on`

| 并发 | `WAL(md + fpw=on) / WAL(Umbra + fpw=on)` |
| --- | ---: |
| 10 | 1.82 |
| 50 | 2.11 |
| 200 | 3.81 |
| 500 | 4.58 |
| 1000 | 4.87 |

`checksum=off`

| 并发 | `WAL(md + fpw=on) / WAL(Umbra + fpw=on)` |
| --- | ---: |
| 10 | 2.03 |
| 50 | 2.51 |
| 200 | 5.22 |
| 500 | 6.90 |
| 1000 | 6.55 |

这组数更直接地说明：在相同事务量下，Umbra 不只是回收了 ordinary checkpoint-boundary FPW 相关的吞吐损失，也明显压低了对应的 WAL 体积压力；并且随着并发升高，这种差距会进一步拉大。

从这些原始数值看，`Umbra + fpw=on` 相对 `md + fpw=on` 的提升是明显且稳定的：

- `checksum=off` 时，50 / 200 / 500 / 1000 并发下分别约为 `+13.8% / +55.1% / +166.2% / +164.9%`
- `checksum=on` 时，50 / 200 / 500 / 1000 并发下分别约为 `+8.1% / +51.1% / +130.3% / +138.7%`

10 并发点上，三组结果非常接近，更多像低并发区间的噪声或非 FPW 主导区；真正把差距拉开的，是 50 以上并发时 ordinary checkpoint-boundary 路径相关 I/O 成本开始反复累计的那一段。

同时，`Umbra + fpw=on` 在大部分点上接近但没有完全达到 `md + fpw=off` 这条上界：

- `checksum=off` 时，Umbra 在 50 和 200 并发下已经非常接近上界，但在 500 和 1000 并发下仍明显落后于 `md + fpw=off`
- `checksum=on` 时，这个现象更明显，说明 Umbra 回收了大部分 ordinary FPW 相关 I/O 成本，但还没有吃满全部剩余系统成本

因此，当前数据更适合支撑下面这条定性结论：

- Umbra 明显回收了 `md + fpw=on` 相对 ordinary FPW 路径损失掉的大部分吞吐，而这更稳妥地可以理解为对相关 I/O 成本的回收
- 这种收益在 `checksum=on` 和 `checksum=off` 两种条件下都能观察到
- `md + fpw=off` 只能作为“如果去掉这类 FPW 成本，系统上界大概在哪里”的敏感性参照，而不能被当成语义对等基线

这组数据也说明，现阶段不宜再给出过细的绝对收益归因，例如“某个前台热路径固定贡献多少、主要收益固定来自哪一项”。当前数据能支撑的是：

- 很大一部分收益确实与 ordinary checkpoint-boundary FPW 路径被 remap metadata 接管、从而回收相关 I/O 成本有关

但如果要继续把收益拆成“WAL 写入与同步压力下降贡献多少、数据写放大下降贡献多少、preallocation 贡献多少”，还需要专门的 ablation 和更完整的方法学说明。

## 14. 当前还没做完的工程点

下面这些项更适合被明确写成 follow-up，而不是暗示成已经完成的能力：

1. `compactor` 工程化：已有框架，但后台收敛效率和目录发现成本控制还未完全工程化；对外仍不能把稀疏 segment 发现成本问题说成已解决；对 PoC 而言属于工程 follow-up，不阻断最小 remap/recovery 闭环。
2. `CREATE DATABASE` 复制策略：当前支持 PostgreSQL 既有 `FILE_COPY` 目录 / 文件复制路径；尚未支持 `WAL_LOG` 逐块复制并逐块写 WAL 的路径，启用 Umbra storage manager 时会回退到 `FILE_COPY`；这属于明确限制项，不应外扩表述。
3. `superblock shared-entry replacement`：当前仍更接近 allocate/free，而不是 replacement/eviction；现实上需要靠调大 `map_superblocks` 兜底容量压力；这属于工程 follow-up。
4. `AIO` 集成：已完成必要适配，但不是完整的 Umbra 原生重构；不能宣称异步读写侧已经完全收敛；这仍是工程 follow-up。
5. `range-born / batch mapping publish`：缺少上层显式接口，当前主要由 `smgr` 层保守兜底；一次性扩展多块的场景仍依赖兼容旧 AM/WAL 顺序的实现；这属于设计/工程 follow-up。
6. `主备物理页对齐`：这类问题已经被明确识别；当前实现在 `no-image remap redo` 这条局部路径上已经显式加入了 `FlushOneBuffer()` 这类更强的发布 / 落盘约束，并有局部 recovery 覆盖；但还不能把主备物理页对齐说成已经系统性收敛，因此仍应限制更强的复制/恢复一致性表述。

如果要再压缩成一句话，那么当前 PoC 更接近“核心 correctness / recovery 闭环已经建立，并且已经具备可编译、可回归、可恢复的基本形态，只是仍带着一组明确标注的 host-tree follow-up”。

## 15. 当前 PoC 的语义分层

这一节只说明当前 PoC 如果按 `P1-P9` 组织时，各层应该承担的语义边界。它不是
任意工作分支与提交编号的映射，也不描述后续发布节奏。

这个分层不应该按文件目录机械切开，而应该按状态机和 owner 边界来理解：前面的
层次先建立可恢复的最小机制，后面的层次再补齐 checkpoint、mapwriter、
compactor 等工程化能力。

这个分层的目标，是让每一层都能说明自己引入了哪个 correctness owner 或工程边界：

- 前面的层次尽量只建立基础机制，不把后续工程化 follow-up 混进去
- 后面的层次再逐步引入 WAL/redo、checkpoint、mapwriter、compactor、recovery
  tests 等能力
- 对当前还没做完的部分，要明确写成 follow-up，而不是隐含成已经完成的能力

按当前 PoC 分支定义，更自然的拆分顺序应该收敛成 `P1-P9`：

- `P1`：建立 `smgr` 实现边界，引入 `--with-umbra` 选择点，保证普通 `md`
  路径不被改变
- `P2`：引入 `umfile` 物理文件层和 metadata storage primitive，先补齐物理文件、
  segment、create / unlink、read / write / extend / truncate 等底层能力
- `P3`：引入 metadata 磁盘格式和 identity mapping 启动路径，让 metadata fork、
  superblock layout、初始映射状态先独立成立
- `P4`：引入共享内存 MAP cache 和 checkpoint flush 基础，让 MAP metadata 的缓存、
  materialize、dirty / flush 语义先独立成立
- `P5`：引入 MAP 访问策略、逻辑到物理的翻译，以及 materialization contract，让
  `MAIN/FSM/VM` 在上层继续使用逻辑块号，在 `smgr` 之下完成 `lblk -> pblk`
  解析
- `P6`：引入 WAL record、mapped birth 和 redo 状态机，先把 `MAP_SET`、truncate、
  metadata lifecycle、skip-WAL pending 等最小 WAL/redo owner 建起来
- `P7`：引入 ordinary remap、block reference remap 和 checkpoint-boundary FPW
  replacement，让 ordinary checkpoint-boundary image path 的替代表达方式闭环
- `P8`：补齐 checkpoint / mapwriter 回写和物理预分配，让 MAP metadata 回写、
  后台预分配、低水位压力下的一次性前台预分配有清晰 owner
- `P9`：引入 compactor 框架和不干扰前台的策略，把 inflight / barrier、reclaim、
  delayed unlink、compactor relocation 收敛成后台整理框架

这个顺序的重点是：前面的层次先把 correctness owner model 建起来，后面的层次再逐步处理工程化压力。尤其是 `CREATE DATABASE` 复制策略、AIO、主备物理页一致性这些 host-tree 集成问题，不应该被伪装成核心机制已经收敛的一部分；它们更适合作为明确的 follow-up 单独讨论。

因此，这里的 `P1-P9` 只表达当前 PoC 的语义边界：哪些属于核心正确性最小闭环，哪些属于工程化增强，哪些仍然只是兼容性兜底或后续集成项。测试和文档也应该按相关语义层次归属，而不是单独抽成一个泛化的“测试/文档层”。

## 16. 总结

Umbra 不是在宣称“PostgreSQL 从此不再需要 full-page image”，也不该被宽泛地描述成一个新的“存储引擎”；更准确地说，它是在 PostgreSQL `storage manager` / 物理存储层上，针对 ordinary checkpoint-boundary FPW 路径提供一套 remap-based 的恢复基线表达方案。它的核心是：

- 在 storage 层把逻辑页身份和物理放置拆开
- 用 map entry 描述单页映射真相
- 用 superblock 描述 fork 级全局真相
- 用 WAL 保证这些状态变化以原子、可恢复的方式发布

在此基础上：

- 前台总是分配新物理页，不在热路径中处置旧页
- mapwriter 主要负责 MAP 元数据侧的后台平滑，而不是独占所有扩张责任
- compactor 负责长期空间收敛
- inflight / barrier 保证前后台迁移和物理写入的并发安全
- 文件删除和 segment 生命周期由 reclaim boundary、live mapping、checkpoint 和 redo 语义共同约束

这套设计的价值，不是单点微优化，也不是一句“关闭 FPW”就能概括。它真正挑战的是 ordinary checkpoint-boundary FPW 所绑定的那组成本模型，同时尽量保持 `md + fpw=on` 所要求的 crash-recovery 语义。

## 附录：实现过程透明度声明

这组代码的形成过程需要保持透明。Umbra 的核心架构、边界划分和关键状态机，
来自作者本人对 PostgreSQL storage / WAL / recovery 语义的设计和原型化工作。
作者也维护了早期验证原型 `shadow`：
<https://github.com/nayishan/postgre_umbra/tree/shadow-pg12-archive>。

为了把原型扩展成当前规模的 PoC，作者在具体实现、样板代码扩展和局部重构中
大量使用了 AI 编码助手（例如 Codex）。这些实现大量依赖前面的逻辑梳理、
`shadow` 原型，以及 PostgreSQL 现有实现的代码形状和调用顺序。

这里的责任边界也需要说清楚：核心设计、边界定义和关键逻辑判断由作者负责；
AI 主要用于加速繁琐实现细节。当前 AI 仍不能独立把握数据库内核中的并发时序、
owner model 和 crash-recovery 语义，因此代码中仍可能存在风格不统一或需要后续
工程化收敛的区域。当前定位仍是 PoC，而不是完成最终 host-tree polish 的成品。
