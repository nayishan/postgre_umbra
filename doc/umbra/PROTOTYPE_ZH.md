# Umbra 原型与仓库导航

本文档是 `PROTOTYPE.md` 的中文配套版本，说明当前 PostgreSQL master 上的
Umbra 原型，与早期 PostgreSQL 12.2 shadow 原型之间的关系。

## 1. 仓库结构

建议把早期原型和当前原型放在同一个 GitHub 仓库里，通过分支隔离。

仓库：

- `https://github.com/nayishan/postgre_umbra`

建议分支：

- `umbra-poc-pgmaster`
  - 基于 PostgreSQL master 的完整 Umbra 原型；
  - 面向社区阅读和测试；
  - 包含 MAP 元数据、WAL / redo、mapwriter、compactor、测试和文档。
- `shadow-pg12-archive`
  - PostgreSQL 12.2 的 shadow 原型归档分支；
  - 适合理解最初的核心映射思路。

原则如下：

- 一个仓库；
- 不同分支做物理隔离；
- 不把 PG12 原型文件混入 master 原型分支；
- 用根 `README` 提供清晰导航。

## 2. 为什么保留原型

PG12 shadow 原型的价值，在于展示最小逻辑：

- 为什么要在 `smgr` 下方做逻辑块到物理块的映射；
- 最原始的 MAP 状态机是什么；
- 哪些是核心设计；
- 哪些是迁移到 PostgreSQL master 之后才出现的工程复杂度。

master 原型会更复杂，因为它必须处理：

- 当前的 `smgr` 边界；
- WAL block registration；
- redo；
- checkpoint / 回写；
- relation 生命周期；
- skip-WAL relation；
- 后台维护；
- recovery TAP。

## 3. 阅读顺序

建议按下面的顺序阅读：

1. 先看 `umbra-poc-pgmaster` 分支上的仓库根目录 `README.md`；
2. 后续等文档集导入该分支后，再看 `UMBRA_FPW_STORY_ZH.md`，理解更完整的
   设计演化叙事；
3. 后续等文档集导入该分支后，再看 `ARCHITECTURE.md`，理解模块边界；
4. 后续等文档集导入该分支后，再看 `WAL_AND_REDO.md`，理解正确性的
   owner model；
5. 后续等文档集导入该分支后，再看 `REVIEW_GUIDE.md`，找到代码入口；
6. 如果仍有不理解的地方，再回头看 shadow 原型，理解最小映射思路。

原型是背景材料，master 原型才是当前测试和审阅的对象。

## 4. 实现透明度

核心架构、边界选择和状态机推演来自作者；PG12 shadow 原型也是 master 原型的
重要参考。

在 `master-port` 的实现过程中，也大量使用了 AI 编码助手来处理重复实现和迁移
工作，并参考原型以及 PostgreSQL 现有实现来组织代码。但 AI 并不具备独立理解
数据库内核并发、WAL 顺序和恢复正确性的能力；真正困难的部分，是持续审查逻辑、
识别错误假设并修正实现。
