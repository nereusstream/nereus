# Nereus 总体设计文档

本目录描述 Nereus 的目标架构和能力轨道。它不再是从规划工作区复制来的 Phase 0 快照；
自 2026-07-10 起，这里与 `docs/phase-1-core-stream-storage/` 共同构成仓库内设计基线。

建议阅读顺序：

1. `nereus-design-index.md`：文档权威性、状态和阅读路线；
2. `nereus-terminology.md`：统一术语及禁止混用的边界；
3. `nereus-overall-architecture.md`：目标架构与已实现边界；
4. `nereus-commit-protocol.md`：append、read-index 和物化发布协议；
5. `nereus-futures.md`：能力轨道、依赖关系和交付顺序；
6. 文件名以 `nereus-futureN-` 开头的文档：各能力轨道详细设计。

当前代码正在实现 Future 1 / Phase 1。代码级合同、里程碑和测试以
`../phase-1-core-stream-storage/README.md` 及该目录下的编号文档为准。
