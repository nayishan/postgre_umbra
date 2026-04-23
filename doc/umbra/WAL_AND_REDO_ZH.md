# Umbra 的 WAL 与 redo 语义

本文档是 `WAL_AND_REDO.md` 的中文配套版本，说明当前 Umbra 原型中的 WAL
内容和 redo 规则。

## 1. 两类会出现在 WAL 中的机制

Umbra 有两类会出现在 WAL 中的机制：

- 普通 WAL block reference 上的 remap 元数据；
- Umbra 自己的 rmgr 记录。

两者不是替代关系：

- block-header 里的 remap 信息，用来让普通 WAL block 回放时找到正确的物理基线；
- Umbra rmgr 记录，用来表达 MAP 的生命周期事件。

## 2. remap header

当普通 block reference 设置 `BKPBLOCK_HAS_REMAP` 时，会带上 remap 的字段。

完整字段如下：

- `old_pblkno`
- `new_pblkno`
- `logical_nblocks`
- `next_free_pblkno`

它们分别表示：

- `old_pblkno`
  - 当前逻辑块旧的已发布物理基线；
  - `InvalidBlockNumber` 表示 first-born。
- `new_pblkno`
  - 即将发布的新物理块。
- `logical_nblocks`
  - 在 first-born 或 range birth 时需要推进的逻辑 EOF。
- `next_free_pblkno`
  - 已提交的分配前沿；
  - redo 用它来保持物理分配的确定性。

要注意：`next_free_pblkno` 不一定等于 `new_pblkno + 1`。它表示的是全局的、
已提交的分配前沿。

## 3. WAL 生成端规则

WAL 生成端的逻辑在 `XLogRecordAssembleUmbra()`。

PostgreSQL 原本会判断：

- 是否需要备份镜像；
- 是否需要数据载荷。

Umbra 额外增加一个判断：

- 是否需要 remap 元数据。

这些判断不能被压成一个单独的布尔值。

在 checkpoint 边界上的普通场景里，如果页面满足自动 remap 的条件，Umbra 会用
remap 元数据替代默认的 full-page image 路径。

保守边界如下：

- `REGBUF_FORCE_IMAGE` 保留 image 语义；
- `REGBUF_NO_IMAGE` 不自动 remap；
- `!doPageWrites` 不自动 remap；
- `XLOG_FPI_FOR_HINT` 继续沿用 PostgreSQL 的 hint image 规则；
- `XLR_CHECK_CONSISTENCY` 保留校验 image。

## 4. first-born

`REGBUF_LOGICAL_BIRTH` 是显式的 first-born owner 路径。

WAL 组装时会：

1. 打开 relation；
2. 查找是否已经有已发布的 mapping；
3. 查找是否已经有 pending 的预留 mapping；
4. 如果两者都没有，就预留一个新的物理块。

随后记录：

- `old_pblkno = InvalidBlockNumber`
- `new_pblkno = 选中的物理块`
- `has_remap = true`

这里选中的物理块可能来自运行时的预留前沿，但在 WAL insert 成功之前，它还
不是 superblock 中已经提交的状态。

## 5. WAL insert 之后的发布

`XLogCommitBlockRemapsUmbra()` 在 WAL insert 成功后发布 remap 状态。

它负责：

- 安装新的 `lblk -> pblk` 映射；
- 必要时推进已提交的 `next_free_pblkno`；
- 在 first-born 时推进 `logical_nblocks`；
- 更新由 WAL 拥有的 first-born relation size cache；
- 释放 pending 预留。

这个边界非常重要：

- WAL 组装阶段可以先选择物理块；
- 只有 WAL insert 成功后，才发布已提交的映射和前沿；
- 运行时预留前沿可以领先；
- 磁盘 superblock 中的已提交前沿不能领先于 WAL。

## 6. Umbra 的 rmgr 记录

当前 Umbra 的 rmgr 记录包括：

- `XLOG_UMBRA_MAP_SET`
- `XLOG_UMBRA_RANGE_REMAP`
- `XLOG_UMBRA_RANGE_REMAP_COMPACT`
- `XLOG_UMBRA_SKIP_WAL_DENSE_MAP`
- `XLOG_UMBRA_RECLAIM_UNLINK`

其中 `XLOG_UMBRA_SKIP_WAL_DENSE_MAP` 是 skip-WAL relation 的映射锚点。它表示：

- `[0, nblocks)` 是 dense；
- `pblk == lblk`；
- `logical_nblocks = nblocks`；
- `physical_nblocks = nblocks`；
- `next_free_pblkno = nblocks`。

它不是数据文件 `fsync` 的替代品；skip-WAL 的同步协议仍然独立存在。

### 6.1 为什么当前不把 RangeMap / range-born 做成正式机制

当前这条分支有意**没有**把 `range-born / batch mapping publish` 做成一个正式的
上层 contract。

原因不是“范围发布做不到”，而是当前还缺少足够清晰的 owner 边界。

在 PostgreSQL 的现有调用点上，Umbra 目前能明确拿到的 owner 主要是：

- 单个逻辑块的 first-born；
- checkpoint 边界上单个逻辑块的 remap；
- `compactor` / `reclaim` 这类 Umbra 内部生命周期记录。

当前真正缺少的，是一个已经天然按“范围”拥有发布语义的上层使用点。未来如果有
类似哈希访问方法做 split / redistribution 这样的路径，能够把一个逻辑范围作为
单一 owner 单元来扩展、写 WAL 并发布，那么 range remap 才有比较自然的落点。
当前这条分支还没有接上这样的调用点。

但它还没有一个通用的上层接口，可以明确表达：

- 这次 WAL owner 要一次性发布一个逻辑范围；
- 这个范围有一个清晰、唯一的顺序边界；
- redo 可以把整个范围当成一个已发布单元来处理。

如果在缺少这种接口的情况下，强行引入通用的 RangeMap 式 contract，就会把太多
歧义推到 WAL 组装和 redo 阶段：

- 范围大小由谁拥有，单块可见性又由谁拥有；
- 整个范围的逻辑 EOF 在什么时候算持久化完成；
- 分配前沿的发布如何和现有 AM / WAL 顺序保持一致；
- 同一个范围里，后面的块会不会在前面的块完成 WAL owner 建立之前就先变得可见。

所以当前分支选择更保守的做法：

- 普通上层 WAL 仍然按“单块”发布 remap 状态；
- first-born 仍然按“单块”显式发布；
- 只有在 Umbra 自己完全控制、owner 边界已经收紧的内部生命周期路径上，
  才使用范围形态的操作。

这也是为什么当前分支里会有 range remap 记录，但还不能把“通用上层 RangeMap
contract”描述成已经收敛完成的能力。

## 7. redo 入口

redo 端的核心入口在 `XLogReadBufferForRedoExtendedUmbra()`。

redo 入口层负责：

- mapped fork 的 metadata bootstrap；
- redo 阶段的 MAP 状态播种；
- 解释 `has_remap`；
- 区分带 image 的 remap 和不带 image 的 remap。

这些逻辑不能下推到普通 read helper，因为普通 helper 不理解 remap header 的
所有权语义。

## 8. redo 的几种场景

### 8.1 无 remap

没有 remap 元数据时，Umbra 会先确保 metadata 存在，然后走接近 PostgreSQL
普通 redo 的路径：

- 有 image：恢复 image；
- 无 image：读取当前 block view 并比较 LSN。

### 8.2 带 image 的 remap

有 remap 且有 image 时：

1. 先安装新 mapping；
2. 推进前沿；
3. 在 first-born 时推进逻辑 EOF；
4. 确保 mapped block 已存在；
5. 把 image 恢复到新的 mapping view。

### 8.3 不带 image 的 remap（zero/init）

有 remap、没有 image，而且 redo 是 zero/init 模式时：

1. 安装新 mapping；
2. 推进前沿；
3. 确保 mapped block 已存在；
4. 按 zero/init 模式读取。

### 8.4 不带 image 的 remap（普通 delta 回放）

这是最关键的 Umbra 场景。

普通的、没有 image 的 delta remap 需要旧物理基线。它的回放语义是“旧物理页 +
WAL delta”，不是在新物理页上原地覆盖。因此 redo 不能先发布新的 mapping，而是
必须：

1. 临时安装 `old_pblkno`；
2. 通过旧 mapping view 读取页面；
3. 锁住并修改 buffer；
4. 消费完旧基线后再切换到 `new_pblkno`；
5. 推进 `next_free_pblkno`。

核心规则是：

- remap-without-image redo 先通过旧 mapping view 读取页面；
- WAL delta 作用在这个旧物理基线上；
- 消费完旧基线后，redo 才发布新的 mapping。

这样一来，checkpoint 边界上的普通场景就可以在不依赖 full-page image 的
情况下，仍然保持 redo 的确定性。

## 9. 总结

当前 WAL / redo 模型可以概括成：

- 普通 block record 可以携带 remap 元数据；
- remap 元数据表达物理迁移和前沿信息；
- 只有 WAL insert 成功后才发布已提交的映射和前沿；
- redo 明确区分无 remap、带 image 的 remap、以及不带 image 的 remap；
- Umbra rmgr 记录负责普通 block header 之外的 MAP 生命周期事件。

这套模型是 Umbra 降低 checkpoint 边界上 ordinary FPI 压力的正确性基础。
