# Umbra 在 PostgreSQL master 上的原型说明

[English](./README.md) | [中文](./README_ZH.md)

这个仓库承载了基于 PostgreSQL `master` 的当前 Umbra 原型。

Umbra 可以理解成 PostgreSQL 存储管理层上的一层扩展：上层仍然按普通逻辑
块号访问数据，而底层通过内部的“逻辑块到物理块”映射，把选定 fork 的内容
写入实际物理块。这样 `MAIN`、`FSM`、`VM` 这些 fork 在上层看来仍然是普通
逻辑块，物理布局变化则由 Umbra 在下层负责。

在这个模型里，remap 指的是把同一个逻辑块从旧物理块切换到新发布的物理块。
它的直接目的，是给 ordinary checkpoint-boundary 更新提供另一种恢复基线。
传统 `md` 路径会覆盖旧物理页，因此需要 full-page image 来保护这次覆盖；
Umbra 则可以为同一个逻辑块发布一个新的物理页，并在 WAL 中记录 old/new
physical mapping。redo 时，remap record 要按该 record 期待的映射视图回放；
delta-only remap 使用“旧物理页 + WAL delta”，而不是把这次更新理解成在新物理
页上的原地覆盖。这样 Umbra 才能在保持 crash-recovery 顺序的同时，降低 ordinary
full-page-image 压力。

这条分支的目标是先把正确性、设计边界和可验证性建立起来。它适合做设计审阅、
实现阅读和测试，但不应被表述为已经完成的生产特性。

## 分支布局

- `umbra-poc-pgmaster`
  - 基于 PostgreSQL `master` 的 Umbra 原型分支
  - 用于完整源码阅读和测试
- `shadow-pg12-archive`
  - PostgreSQL 12.2 时代的 `shadow` 原型归档分支

## 当前实现范围

当前实现包含：

- `--with-umbra` 构建选项，以及 Umbra 在 `smgr` 层的接入
- 每个 relation 的内部 `metadata fork`，用来存放：
  - MAP superblock，负责记录 fork 级别状态，例如：
    - 逻辑文件末尾
    - 已物化的物理容量
    - 已提交的分配前沿
  - 普通 MAP page，负责记录逐块映射事实：
    - `lblk -> pblk` 条目
    - 普通逻辑块当前是否已经建立映射
- 一个 MAP 子系统，负责：
  - 逻辑块到物理块的查找
  - superblock 共享状态及相关运行时状态管理，包括：
    - 逻辑文件末尾
    - 分配前沿
    - 回收边界
- 两个后台进程：
  - `mapwriter`
    - MAP page 刷盘
    - 预分配物理空间
  - `mapcompactor`
    - 回收
    - 压缩整理
- 一组围绕 remap 与 redo 的 WAL/恢复支持，包括：
  - 普通 WAL record 上的 remap 元数据
  - `xlogutils.c` 中的 remap 解释与恢复路径
- `src/test/recovery` 下的 Umbra recovery TAP 测试

## 一页设计摘要

Umbra 可以被理解成一个由六个层次组成的存储层拆分。

1. 上层 PostgreSQL 保持普通逻辑寻址。
   对普通 PostgreSQL 调用方来说，relation、fork 和块号这些对象仍然按逻辑
   语义使用。Umbra 改变的是 `smgr` 下方的物理布局，而不是要求上层直接处理
   物理块号。

2. 持久化真相放在 `metadata fork` 中。
   每个 relation 都有一个内部 `metadata fork`，里面包含：
   - 一个 MAP superblock，用来记录 fork 级别事实，例如逻辑文件末尾、
     已物化的物理容量，以及已提交的分配前沿；这个 superblock 是一个很小的
     `512B` metadata sector
   - 一组 MAP page，用来记录逐块的 `lblk -> pblk` 映射事实；这些 page 是由
     固定大小 entry 组成的紧凑 metadata page，形式上更接近 CLOG 这类
     metadata，而不是普通 PostgreSQL data page，因此不走普通 data-page FPW
     语义

3. 运行时访问路径和物理文件 I/O 明确分层。
   - `umbra.c` 负责 mapped fork 的运行时访问语义。
   - MAP 子系统负责查找和共享运行时状态。
   - `umfile.c` 负责真正的物理文件和 segment 操作。

4. WAL 是 MAP 状态变化的 owner 边界。
   - fork 级 superblock 事实通过 WAL 成为 redo 可见状态。
   - 物理页生命周期转换通过 WAL 成为 redo 可见状态。
   - 逻辑页到物理页的映射变化通过 WAL 成为 redo 可见状态。
   普通 block reference 携带页面回放需要的 remap 元数据；普通 block
   reference 之外的显式 MAP lifecycle 动作由 Umbra rmgr record 表达。

5. redo 按 WAL record 期待的映射视图回放 remap。
   - redo 先恢复该 record 携带的 old/new mapping view。
   - 没有 image 时，回放基线是“旧物理页 + WAL delta”，不是在新物理页上做
     原地覆盖。
   - 有 image 时，redo 把 image 安装到新发布的映射上。

6. 后台维护和前台访问路径分开。
   - `mapwriter` 负责 MAP page 刷盘和预分配
   - `mapcompactor` 负责回收和压缩整理
   这样长周期的空间收敛就不会直接挤进前台热路径。

## 文档

更详细的设计说明放在 [doc/umbra/](./doc/umbra/)。

英文主文档：

- [Architecture](./doc/umbra/ARCHITECTURE.md)
- [WAL and Redo](./doc/umbra/WAL_AND_REDO.md)
- [Review Guide](./doc/umbra/REVIEW_GUIDE.md)
- [Prototype and Branch Navigation](./doc/umbra/PROTOTYPE.md)
- [FPW-to-remap design story](./doc/umbra/UMBRA_FPW_STORY.md)

中文配套材料：

- [Architecture](./doc/umbra/ARCHITECTURE_ZH.md)
- [WAL and Redo](./doc/umbra/WAL_AND_REDO_ZH.md)
- [Review Guide](./doc/umbra/REVIEW_GUIDE_ZH.md)
- [Prototype and Branch Navigation](./doc/umbra/PROTOTYPE_ZH.md)
- [FPW-to-remap 设计故事](./doc/umbra/UMBRA_FPW_STORY_ZH.md)

## 测试基线

当前正确性基线是 md/Umbra 双模式矩阵。在同一个源码树里切换构建模式时，
先清理上一次构建。

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

一个特别重要的恢复测试是：

```sh
make -C src/test/recovery check PROVE_TESTS=t/074_umbra_torn_page_remap.pl
```

这个测试在 md 模式下是负对照，在 Umbra 模式下验证 torn-page remap
recovery。

## 初步性能指标

当前性能证据只能视为方向性信号。这里同时给两类早期指标：

- 同一工作负载下的 TPCC 风格吞吐
- 同一工作负载下的 WAL 大小比值

吞吐视角很重要，因为仅看 WAL 降幅并不能完整描述性能。公平的默认基线是：

- `md + fpw=on`

而 `md + fpw=off` 更适合作为敏感性 / 上界参考，不应被看作与正确性约束等价
的基线。

公共设置：

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

### TPCC 风格吞吐

#### Checksums 关闭

| 并发 | `md + fpw=on` | `md + fpw=off` | `Umbra + fpw=on` |
| ---- | ------------: | -------------: | ---------------: |
| 10   |        158709 |         154283 |           155781 |
| 50   |        577005 |         626954 |           656353 |
| 200  |        641899 |         981436 |           995635 |
| 500  |        322660 |         943295 |           859058 |
| 1000 |        275609 |         899631 |           729989 |

#### Checksums 开启

| 并发 | `md + fpw=on` | `md + fpw=off` | `Umbra + fpw=on` |
| ---- | ------------: | -------------: | ---------------: |
| 10   |        155754 |         152025 |           150606 |
| 50   |        601974 |         635597 |           650844 |
| 200  |        621176 |        1015923 |           938311 |
| 500  |        316950 |         972795 |           729801 |
| 1000 |        282713 |         891770 |           674865 |

### WAL 大小比值

- `md WAL bytes with full_page_writes=on`
- 除以
- `Umbra WAL bytes with full_page_writes=on`

比值越大，表示 Umbra 在相同工作负载下生成的 WAL 越少。

#### Checksums 关闭

| 并发 | md WAL / Umbra WAL |
| ---- | ------------------ |
| 10   | 2.03               |
| 50   | 2.51               |
| 200  | 5.22               |
| 500  | 6.90               |
| 1000 | 6.55               |

#### Checksums 开启

| 并发 | md WAL / Umbra WAL |
| ---- | ------------------ |
| 10   | 1.82               |
| 50   | 2.11               |
| 200  | 3.81               |
| 500  | 4.58               |
| 1000 | 4.87               |

把吞吐和 WAL 大小两组数字放在一起看，可以看到 Umbra 不只是降低了 WAL 体积；
在相同工作负载下，它也回收了 ordinary checkpoint-boundary full-page-image
压力带走的大部分吞吐。

这些数字应被理解为：

- 初步结果
- 方向性信号
- 尚不是完整 benchmark

它们不应被读作关于 throughput、latency 或完整 replication/recovery cost 的
最终结论。
