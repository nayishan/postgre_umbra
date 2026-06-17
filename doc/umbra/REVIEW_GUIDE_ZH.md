# Umbra 审阅指南（中文版）

本文档是 `REVIEW_GUIDE.md` 的中文配套版本，用于说明阅读当前 Umbra 原型
patch 序列时，先看什么、重点看什么。

## 1. patch 的关注点

审阅时最值得关注的是：

- 架构边界是否合理；
- `smgr` 接入是否清晰；
- MAP 元数据的所有权边界是否清楚；
- WAL / remap 的所有权模型是否正确；
- redo 是否具备确定性；
- checkpoint / 回写边界是否正确；
- 测试是否覆盖了核心风险。

## 2. 建议阅读顺序

建议先从这些文件入手：

- `src/backend/storage/smgr/smgr.c`
  - `storage manager` 的分派逻辑；
  - `--with-umbra` 的边界。
- `src/backend/storage/smgr/umbra.c`
  - 运行时访问策略；
  - 逻辑块到物理块的翻译。
- `src/backend/storage/smgr/umfile.c`
  - 物理文件层；
  - 段文件、同步、删除、dense/sparse 语义。
- `src/backend/storage/map/`
  - MAP 元数据、buffer、superblock、刷盘和后台工作。
- `src/backend/access/transam/xloginsert.c`
  - WAL 生成端的 remap 判定。
- `src/backend/access/transam/xlogreader.c`
  - remap header 的解析。
- `src/backend/access/transam/xlogutils.c`
  - redo 阶段对 remap 的解释。
- `src/backend/access/transam/umbra_xlog.c`
  - Umbra 自己的 rmgr 记录。

## 3. 核心正确性不变量

优先审阅这些不变量：

- WAL 的发布必须先于已提交 MAP 的发布；
- pending 预留只负责选择物理块，不发布已提交映射；
- first-born 必须显式发布逻辑 EOF；
- 不带 image 的 remap redo 必须先消费旧物理基线；
- `next_free_pblkno` 表示已提交的分配前沿；
- 运行时预留前沿只存在于共享内存中；
- 已提交的 `next_free_pblkno <= reservation frontier`；
- 逻辑 EOF、物理容量、分配前沿是不同事实；
- checkpoint / mapwriter 只写已经存在的 MAP 元数据块；
- 在一个 checkpoint cycle 中注册的 reclaim unlink request，不能在同一个
  checkpoint 的 post 阶段被物理删除；只有后续 checkpoint 已经完成并进入
  `SyncPostCheckpoint()` 后才可以删除；
- redo 拥有只在恢复阶段需要的 metadata bootstrap。

## 4. Full-Page Image 的边界

Umbra 并没有全局关闭 full-page writes。

它只是在满足条件的 checkpoint 边界普通场景里，用 remap 元数据替代默认的
image 路径。

保守边界如下：

- `REGBUF_FORCE_IMAGE` 保留 image；
- `XLR_CHECK_CONSISTENCY` 保留校验 image；
- `XLOG_FPI_FOR_HINT` 当前不走 Umbra remap。

hint-bit 的 FPI 优化需要单独的 checksum / torn-page 保护设计，不应该混入
当前的 header 编码优化里。

## 5. Skip-WAL Dense Map

skip-WAL relation 在 pending 窗口内按 dense 物理布局处理。

`XLOG_UMBRA_SKIP_WAL_DENSE_MAP` 表示：

- `[0, nblocks)` 是 dense；
- `pblk == lblk`；
- `logical_nblocks = nblocks`；
- `physical_nblocks = nblocks`；
- `next_free_pblkno = nblocks`。

这个记录不是 `fsync` 的替代品；它只是 redo 阶段的 mapping / frontier 锚点。

## 6. 当前不解决的问题

当前 patch 不试图解决所有 WAL 字节数优化。

明确不作为当前目标的内容包括：

- 更小的 birth header；
- mixed record 中每个 block 各自独立的 variant tag；
- 基于 checksum 的 hint FPI remap 优化；
- 更激进的 compactor range relocation WAL；
- 默认开启 storage manager；
- 完整的生产级空间管理。

当前优先级是确定性的所有权边界，以及可审阅的回放语义。

## 7. 测试基线

完整正确性矩阵如下。在同一个源码树里切换构建模式时，先清理上一次构建。

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

重点测试包括：

- `src/test/recovery/t/074_umbra_torn_page_remap.pl`
  - 在 md 模式下充当反向对照；
  - 在 Umbra 模式下验证：即使 remap 后的新物理页被破坏，恢复仍然能够成功；
  - 恢复后检查的是按顺序计算的 relation 摘要，而不只是 row count。

## 8. 审阅结论应该关注什么

- 架构层面的反馈；
- WAL / remap 所有权模型的反馈；
- redo 正确性的反馈；
- `smgr` 边界的反馈；
- checkpoint / 回写边界的反馈。
