# 协议转换重构实施计划：中间表示、编解码器与分层测试

状态：已完成（阶段 0 至阶段 8 全部实施完毕，待维护者推送发布）。本文件保存在仓库根目录的 `protocol-conversion-ir-refactor.md`，执行进度追加到文末的"进度记录"。

## 0. 使用方式

本计划交给实施模型执行。开发和测试分九个阶段（0 到 8），在 `main` 上分步提交。新引擎一次切换上线，旧引擎在同一个版本里删除（2.4 节）。阶段 0 到 7 的提交不改变生产行为；每个阶段末尾的验收全部通过后，才进入下一阶段。

- 标注"已定"的条目是已经做出的架构决策，直接照做。实施中发现某条决策与代码事实冲突时，停下来把冲突写进进度记录并询问用户，不要自行改方案。
- 计划没有覆盖到的局部选择，按 2.3 节的原则决定，并在进度记录里写一行说明。
- 开始前按 `AGENTS.md` 加载 `project-doc`、`code-complete`、`a-philosophy-of-software-design`、`code-comments`、`humanizer`。新增或改名后端 Go 文件前读 `docs/operations/development_workflow.md#backend_file_layout`。
- 代码片段里的类型和函数签名是契约，字段可以按需补充，已有字段的语义不要改。片段省略了仓库要求的文件总览和声明注释，实施时按 `code-comments` 补齐。
- 涉及 Codex、Claude Code、pi 等客户端的命令行参数来自 2026-11-14 的调研。这些工具更新很快，使用前用对应版本的 `--help` 和官方文档核对。

## 1. 调研结论

### 1.1 现状数据

| 项目 | 数值 | 说明 |
| --- | --- | --- |
| `protocol/bridge` 非测试代码 | 17,391 行，35 个源文件 | 纯叶子包，只依赖 `protocol` 下的兄弟包 |
| `protocol/bridge` 测试 | 11,257 行，约 360 个测试 | 全部是内联 JSON 断言，没有 testdata、golden、fuzz |
| bridge 之外导入 bridge 的非测试文件 | 68 个，合计 29,598 行 | 分布在 `gateway/httpapi`、`gateway/forward`、`gateway/provider/*forward`、`upstream/{openai,gemini,antigravity,grok,qoder}`；另有 22 个测试文件（23,235 行）导入 bridge |
| bridge 之外自带的转换实现 | Qoder 约 5,300 行，Antigravity 约 2,400 行，Grok `request_patch.go` 1,405 行 | 与 HTTP 读写、用量、重试混在一起 |
| 转换读取循环 | 约 12 个 | 每个循环自己扫描 SSE、解析用量、驱动状态机、写出 |
| SSE 写出辅助函数 | 至少 4 份 | `qoder.WriteSSEData`、`gemini.WriteSSE`、`searchtools.flushSSEJSON`、bridge 的 `*ToSSE` |
| 工具名映射机制 | 至少 6 套 | `ResponsesClientToolMapping`、Codex 反向表、Anthropic OAuth 改写、Qoder 映射器、Gemini 调用记忆、`CodexToolCorrector` |
| 录制的真实流量夹具 | 0 | |
| 使用真实客户端的测试 | 0 | 只有 `tools/gemini_sdk` 用官方 Google SDK 做可选检查 |

### 1.2 结构性问题

1. 成对转换加临时串联。请求方向 Chat 到 Messages 走 Chat→Responses→Messages（`gateway/forward/conversion.go:171-179`、`openaiforward/anthropic_flows.go:297-306`）；流方向 Messages 到 Chat 走 Messages→Responses→Chat；Gemini 上游的响应到 Chat 客户端走 Gemini→Messages→Responses→Chat（`upstream/gemini/response_openai.go:292-307`），共三跳。每一跳都按目标协议的子集类型重新序列化，丢失逐跳叠加。Responses 和 Messages 实际上被当成了临时的中间表示，但它们是线协议，表达不了对方的概念。
2. 同一对转换有多份实现。Anthropic 上游到 Responses、Chat 的输出读取在 `gateway/forward/conversion_output.go` 和 `openaiforward/anthropic_output.go` 各有一份；"Anthropic 事件→Responses→Chat"的驱动循环有四份；Claude 到 Gemini 的请求转换有 `bridge.NativeConvertClaudeMessagesToGeminiGenerateContent` 和 `antigravity.TransformClaudeToGeminiWithOptions` 两份；Gemini 到 Claude 的响应有 `NativeGemini*` 和 `GeminiToAnthropic*Processor` 两族；Messages 的类型有 `AnthropicRequest` 和 `ClaudeRequest` 两族。
3. 平台执行器按来源协议各写一套方法（`Forward`、`ForwardAsResponses`、`ForwardAsChatCompletions`），转换流程通过 Ports 接口把原生管线的步骤又实现了一遍。`1122d1b06` 瘦身后，`openAIMessagesExecutionAdapter` 仍有 33 个端口方法。
4. 三种 JSON 处理方式并存：带 `json.RawMessage` 空洞的子集类型、`map[string]any`、gjson 和 sjson 原地修补。类型化路径丢未知字段，map 路径经过 float64 损失大整数，代码里靠十几处注释提醒各自的保真边界。
5. 路线表（`routing/capability/protocol.go:212`）只给出目标协议，实际跑哪个转换器在至少四处重新判断：`unified_text_executor.go:222,258`、`openaiforward/prelude.go:309-380`、`openai_messages_execution_ports.go:140-146`、`openaiforward/chat.go:90-158`。
6. Qoder 自己定义了一套中间结构（`QoderPayloadRequest`、`QoderMessage`），三个入口协议各写一个解析器、一个流写出器和一个缓冲构造器。这说明"先归一再输出"的需求早已存在，只是每个上游各做各的。

### 1.3 逃过现有测试的缺陷

2026-08-01 以来有 48 个不重复的转换修复提交。按根因归类：

| 类别 | 例子 | 现有测试为什么抓不到 |
| --- | --- | --- |
| 多轮重放：网关第 N 轮输出的内容在第 N+1 轮作为历史回来，有时换了协议或提供商 | `b567a28f9`（DeepSeek 多轮工具调用缺 `reasoning_content` 返回 400）、`401dd43b4`、`612436a5a`、`057437f5e`（签名泄漏到 Messages 后跨提供商重放失败） | 单元测试只测单次转换，没有"输出再作为输入"的闭环 |
| 严格或合并式的客户端解码器 | `6b6987483`（缺 `created_at`，Rust serde 反序列化失败）、`091ec1f41`（每个参数增量带 `"name":""`，客户端调用了名为空串的工具）、`ac38d88f0`（`content_index` 不前进，SDK 覆盖内容） | 断言只看字段值，没有按目标协议的语法校验整条流，更没有真实客户端解析 |
| 上游的非标准线格式 | `22235a63c`（只有 data 行、多行 data、结尾没有空行的 SSE）、`4296d1e3a`、`826f45a2c`（后续增量带空 id 和 name）、Gemini 兼容上游重发累计批次 | 夹具是手写的理想报文，没有真实上游的录制 |
| 流的交错和截断 | `7b15de584`（并行工具调用交错，在 `content_block_stop` 之后又出增量）、`bd39eb589`（没有终止块就断流，仍返回 200 和零用量） | 手写事件序列覆盖不到交错和任意位置截断 |
| 跨跳的用量合并 | `4effd6b50`、`85b0079f6`（5m 和 1h 缓存桶丢失或重复计数） | 串联路径上每跳各自换算 |

每次修复都只在转换包里加了内联 JSON 的单元测试。问题的发现渠道是真实客户端（Codex 0.147.0 恢复会话、Claude Code 会话）、GitHub issue 和线上遥测。

### 1.4 Bifrost 和同类项目的启示

调研对象是 `maximhq/bifrost` 主分支（core 1.15.2）、LiteLLM、pi-ai 和 Vercel AI SDK 的源码。

| 观察 | 对本计划的取舍 |
| --- | --- |
| Bifrost 有两套并行的中间表示（Chat 形和 Responses 形），之间靠 3,076 行的 `mux.go` 互转，互转会丢 `CacheControl`、工具调用的 `ExtraContent`、签名、item id 等 | 只设一套中间表示 |
| Bifrost 的 Responses 形中间表示有 30 多种 item 类型，单个提供商的 Responses 适配是 Chat 适配的 3 到 10 倍（Anthropic 的 `responses.go` 有 11,621 行） | 中间表示不照搬任何一家的线协议形状，只建模语义核心，其余走扩展 |
| `ExtraParams` 是无类型 map。入口有时不采集，出口有时不消费，相关 issue 包括 #879、#5274、#8149 | 未知字段用 `encoding/json/v2` 的 embed 回退自动采集；扩展字段只允许同源协议的编码器整体写回，禁止按键名读取；用普查测试让未建模字段可见 |
| 条目列表拍平后丢了块顺序（#9156、#9736） | 中间表示保留每条消息内 part 的顺序，重排只允许发生在目标协议的合法化步骤里，并且要上报 |
| 每次转换随机生成 id，破坏了提示缓存的前缀稳定（#9518） | 请求方向生成的 id 必须由内容确定，不允许随机 |
| 签名靠拼在工具调用 id 或签名字符串里带过去（`_ts_`、`brid_` 前缀） | 设计显式的重放令牌和信封格式（3.10 节） |
| pi-ai 用一套很小的消息加内容块结构，重放数据是带类型的不透明字符串，跨模型时直接丢弃思考块 | 采用"消息加有序 part"的形状；重放令牌带签发方案标记，方案不匹配时丢弃并上报 |
| Bifrost 没有录制回放；测试靠真实提供商的场景矩阵和官方 SDK 的黑盒矩阵。LiteLLM 有用真实 Claude Code、Codex 跑端到端的 PR，但依赖真实提供商 | 场景矩阵照做，再补两样他们没有的：录制语料回放，以及用脚本化假上游驱动真实客户端 |

### 1.5 关于"是否拆成单独的包"

转换算法名义上已经在一个纯包里（`protocol/bridge`），拆包这一步其实做过了。可测性差的原因是包太浅：它只导出逐字段映射和状态机，解码、串联、工具降级、流驱动、SSE 分帧、用量合并这些步骤的编排散落在网关和上游的 68 个文件里，测试转换必须连同 HTTP 夹具一起搭。

已定：保留"转换在 `protocol` 下的纯包里"这个边界，把包做深。新的转换引擎对外只有"字节进、字节出"的窄接口，内部包含解码、IR 变换、编码和流状态。网关只负责 I/O、重试、健康、计费。这样转换可以在内存里用纯函数测完，网关侧的测试只需要覆盖粘合逻辑。

## 2. 目标、范围和原则

### 2.1 目标

1. 一套规范的中间表示（下称 IR）。每个协议实现一个编解码器，四个文本协议加 Qoder 共五个，替代现在的成对转换和串联。
2. 转换是深模块：`convert` 包接收客户端请求字节，返回上游请求字节；接收上游响应帧，返回客户端响应帧。
3. 网关里按上游协议合并流程：每个上游协议一条"原生"路径和一条"转换"路径，转换路径与客户端协议无关。
4. 所有信息丢失都由编码器显式上报，测试验证上报是完整的。
5. 六层测试体系，包含录制语料回放和真实客户端。
6. 一次发布完成切换：所有现有转换路线（包括 Qoder 和 Antigravity）通过验收后统一切到新引擎，旧引擎随同一版本删除。

### 2.2 范围

范围内：四个文本协议（`anthropic_messages`、`openai_responses`、`openai_chat_completions`、`gemini_generate_content`）之间的跨协议转换，Qoder 和 Antigravity 上游，Responses 同协议客户端工具降级（OpenAI API Key、DeepSeek、Grok 及 Responses WebSocket HTTP 桥在不支持 custom、namespace、tool_search 工具时通过 `convert` 的同协议轻量入口改写工具声明和调用条目，其余请求字段与原生流事件保持透传，见 3.8 和 6.6 节），以及原生 Messages、Responses 路径对推理信封的还原和清理（3.10 节）。

范围外：

- 来源协议和目标协议相同且不需要客户端工具降级的请求，继续走现有的原样透传和线级修补，不经过 `convert`。
- 图片、视频、音频、Embeddings、Realtime、Live、Compact、搜索入口。Images 转 Responses、Responses WebSocket 转 HTTP 属于传输和端点适配，保持现状。
- Bedrock、Vertex 对 Messages 请求的信封改写，Claude Code 伪装、Codex 请求变换、模型属性参数修正（`ApplyModelAttributeParameters`）等上游侧的线级后处理。它们在转换完成后作用于目标协议的报文，位置和实现都不变。
- 计费用量的解析。读取循环里现有的用量累加器保持不变，见 3.13 节。
- 新增客户端路线。Gemini 客户端转到非 Gemini 上游在 IR 落地后只差路线表的条目，本计划不做，留作后续。
- 原生路径和转换路径的进一步合并（让转换成为原生执行器外面的一层中间件）。这是合理的后续方向，记录在 9.5 节，本计划不承诺。

### 2.3 原则

- 解码器不丢信息：线上出现的每个成员要么进入建模字段，要么进入类型化的不透明条目，要么进入 `Extra`。
- 只有编码器可以丢信息，每次丢弃、近似或重排都产生一条 `Loss`。
- 适合统一表达的概念建模进 IR；只属于一家的内容用带来源标记的扩展保留；任何编码器需要按名字读取的扩展字段，必须先提升为建模字段。
- 目标协议的合法性规则（角色交替、工具调用配对、块的顺序限制）属于该协议编码器的内部知识，不外泄到 IR 和其他编解码器。
- 平台差异通过 `Dialect` 的能力字段表达，不在编解码器里写平台名判断。`Dialect` 只在 `gateway/provider` 的一个函数里构造。
- 请求方向的输出逐字节稳定：同一个版本里，同样的输入得到同样的字节，不依赖 map 遍历顺序、时钟和随机数。提示缓存命中依赖这一点。跨版本的字节一致不作要求（2.5 节）。
- 发布前以旧实现为行为基准。新旧差异只允许落在 6.2.4 节的验收差异清单里，其余差异要么消除，要么询问用户。基准只比较同一份输入在新旧引擎上的结果，不要求新版本认得旧版本留下的转换状态（2.5 节）。

### 2.4 发布方式（已定）

新引擎只上线一次。所有现有转换路线通过阶段 7 的总验收后，阶段 8 在一个发布里把全部分派点切到新引擎，并删除旧引擎。不设运行时引擎开关，生产中不让两套引擎并存，切换后也不安排观察期。

仓库从 `main` 直接发布，间隔通常是一两天（v0.7.0 到 v0.7.2 用了三天），所以阶段 0 到 7 的每个提交都必须保持生产行为不变：

- 阶段 0 和阶段 1 的新包没有生产代码导入。
- 阶段 2 到 6 的网关接入代码放在接线开关后面。接线开关是执行器构造时接收的装配选项，生产装配不打开它，只有 `internal/app` 的测试辅助函数（`newProtocolLabRouter`）会打开。它不读配置、环境变量和请求头，不用于运行时切换或回退。阶段 2 加一个测试，断言生产装配下接线开关是关闭的。
- 阶段 8 删除接线开关和旧路径，新路径成为唯一实现。

开发期间旧引擎仍是线上实现。线上转换缺陷照常在旧引擎修复；同一个修复要把复现用的请求或上游流加进语料或场景，新引擎也必须通过。修复改变了旧引擎的行为时，重新生成基线和差分报告。

阶段 8 需要一个发布窗口：从切换提交推送开始，到旧引擎删除、文档更新完成并通过 `make verify` 为止，不打发布标签。开始阶段 8 前先和用户确认窗口。切换后要回退只能整体回滚到上一个版本。旧引擎不认识信封（3.10 节），回滚后客户端历史里的信封对旧版本只是一段无法校验的签名，结果取决于各路径现有的签名错误处理。本计划不为回滚做专门兼容。

### 2.5 版本兼容边界（已定）

原则：新版本不需要认得旧引擎留下的转换状态，旧会话在升级后可以丢失其中一部分；客户端协议的正常支持和新引擎下的多轮续聊必须保证。按这个原则逐项评估原计划里的兼容要求：

| 原要求 | 结论 | 新要求、理由和后果 |
| --- | --- | --- |
| 进程配置 `gateway.conversion_engine`，按上游族逐个切默认值，观察一个发布周期后再删旧引擎 | 取消 | 见 2.4 节 |
| 推理令牌在阶段 1 到 6 按旧路线逐条等价（`gAAAA` 嗅探、非 `gAAAA` 签名写回 `encrypted_content`、Gemini 签名不写给 Messages 客户端），阶段 7 加固后仍保留嗅探，兼容加固之前的历史 | 取消 | 首版直接采用 3.10 节的信封方案。客户端历史里不是信封的令牌一律标为 `unknown`：跨协议路线丢弃并上报，同协议路线原样写回。后果：旧引擎写进客户端历史的令牌（例如 Claude Code 历史中 `signature` 字段里的 Grok `encrypted_content`）在升级后不再回传上游，模型在缺少这段推理历史的状态下继续。缺少令牌时的处理与现在客户端没带回令牌的情形相同 |
| 降级后的工具名与旧实现逐字节相同，工具名差分必须为零 | 放宽 | 命名算法仍从旧实现移植，作为起点。与旧版逐字节相同不再是门槛，新要求见 3.11 节。降级名只出现在发往上游的请求里，客户端始终看到原名；每个请求都按完整历史重新降级，同一请求里的声明和历史调用总是一致；Responses WebSocket 的 `ToolMap` 只在一条连接内继承，升级重启后连接会重建。名字变化只影响升级后第一轮的上游提示缓存 |
| 上游提示缓存跨版本命中，即请求字节与旧版一致 | 取消 | 切换后第一轮请求的字节与旧版不同，提示缓存可能未命中一次。同一版本内的字节稳定照旧保证（2.3 节、定律 Q5） |
| Redis 里的 `ReasoningHistory`（键前缀 `reasoning_content:`，按 Responses 推理条目 id 存推理文本，保留 7 天），以及服务端存储要与现有 `Lookup`、`Store` 行为等价 | 取消，存储在阶段 8 删除 | 新引擎把没有令牌的明文推理装进 `plain` 信封，写进 Responses 客户端的 `encrypted_content`。客户端只回传 `encrypted_content` 时（`186447541` 修的那类请求）也能取回文本，不再需要服务端副本。旧键在 7 天内自然过期，不做清理。后果：升级前生成、只剩 `encrypted_content` 的推理条目，升级后发往 Chat 上游时取不到文本，效果等同于现在 Redis 未命中 |
| 进程内状态：`GeminiToolCalls`（一小时）、`AnthropicPromptCache`（Messages 转 Responses 的提示缓存键绑定）、Responses WebSocket 连接内的工具映射 | 不做跨版本兼容 | 这些状态在进程重启时本来就会清空，阶段 8 可以改键和格式。`GeminiToolCalls` 改作 `CallMemory` 的实现继续使用（3.10 节），因为三种客户端协议都没有逐个调用的不透明字段 |
| 与旧实现的行为差分 | 保留，作为发布门槛 | 基线和差分一直跑到阶段 7。有意接受的差异写进 6.2.4 节的清单，其余差异为零。差分测试、`legacy_baseline.json` 和 `diff_ledger.json` 随旧引擎在阶段 8 删除 |
| Qoder 可以推迟到删除旧引擎之后 | 取消 | 所有现有转换路线（包括 Qoder 和 Antigravity）通过阶段 7 后才能切换 |

下面这些要求不属于跨版本兼容，保持不变：

- 客户端协议的正常支持：请求解码保留对无效 UTF-8 字符串的宽容（`jsontext.AllowInvalidUTF8(true)`），同时保持拒绝解码重名键（与网关入口 `wirejson.ValidateRequestObject` 的安全门禁一致）；Chat 的旧式 `function_call` 和 `functions`；Responses 请求体里旧式的 `messages` 字段（`openai_responses_ingress_compat.go`）、`input` 简写，以及 Cursor 发往 `/v1/chat/completions` 的 `input` 形状请求体（`openaiforward/chat.go`）。
- 上游的非标准报文：旧事件名、不规范的 SSE、兼容上游重发累计内容、Codex 流中孤立 `error` 帧被后续 `response.completed` 恢复。
- 计费用量零差异，客户端可见用量解码经 `wirejson.ValidateUsageEnvelope` / `ValidateUsageObject` 守卫；输出帧的 `Semantic` 和 `Terminal` 由 `convert` 按结构化事件直接填写（补齐旧 `CompatOutputMeaning` 漏判的 custom/search 工具、refusal 及 `incomplete`/`failed`/`error` 终态，归入 D3），读取循环靠它决定前导帧暂存窗口、TTFT 和保活停发。
- 上游侧的线级后处理不变。
- 第 4 节里取自旧实现的默认值和合法化规则。它们记录的是上游接受什么，改动要有证据，并记入 6.2.4 节的清单。

新引擎下的多轮续聊由定律 M1、T5 的多轮场景和 T6 保证，覆盖范围以阶段 7 的验收为准。

## 3. 目标架构

### 3.1 总览

```text
客户端请求 (协议 P 的字节)
        │
        ▼
 convert.NewExchange ── P 的请求解码器 ──► ir.Request
        │                                    │ 变换：连续性恢复、客户端标记清理、工具降级
        │                                    ▼
        │                     Q 的请求编码器 (Dialect) ──► 上游请求字节 + []ir.Loss
        ▼
 现有的上游侧线级后处理、鉴权、发送（不变）
        │
        ▼
 上游响应 (协议 Q 的 SSE 帧或 JSON)
        │
        ▼
 Q 的流解码器 ──► ir.Event 序列（顺序化、每个 part 结束时带完整快照）
        │              │ 变换：工具还原、连续性记录
        ▼              ▼
 现有读取循环      P 的流编码器 ──► 客户端帧（带语义和终止标记）
 的原生观察              │
 （用量、模型、          ▼
  故障分类，不变）   upstream.OutputContext ──► 客户端
```

### 3.2 包布局（已定）

全部位于 `backend/internal/protocol/` 下，角色都是 `pure`，`testkit` 除外。

| 包 | 职责 | 允许依赖的项目包 |
| --- | --- | --- |
| `protocol/sse` | SSE 帧类型、增量读取器、写出函数 | 自身 |
| `protocol/ir` | IR 类型、流事件、校验器、累加器、顺序化器、Loss、重放令牌和信封、语义摘要 | 自身、`protocol` |
| `protocol/qoder` | Qoder 归一化请求、消息、工具与思考指令的纯数据结构（供 `codec/qoderchat` 与 `upstream/qoder` 共用） | 自身、`protocol` |
| `protocol/codec/messages` | Anthropic Messages 编解码器 | 自身、`protocol`、`protocol/ir`、`protocol/sse`、`protocol/anthropic`、`protocol/wirejson` |
| `protocol/codec/responses` | OpenAI Responses 编解码器 | 自身、`protocol`、`protocol/ir`、`protocol/sse`、`protocol/openai`、`protocol/wirejson` |
| `protocol/codec/chat` | OpenAI Chat Completions 编解码器 | 同上 |
| `protocol/codec/generatecontent` | Gemini GenerateContent 编解码器 | 自身、`protocol`、`protocol/ir`、`protocol/sse`、`protocol/gemini`、`protocol/wirejson` |
| `protocol/codec/qoderchat` | Qoder 上游编解码器，只有上游侧的一半 | 自身、`protocol`、`protocol/ir`、`protocol/sse`、`protocol/qoder`、`protocol/wirejson` |
| `protocol/convert` | 转换引擎：`Exchange`、`Stream`、IR 变换、同协议工具降级/还原、`Dialect`、编解码器注册表 | 自身加以上全部 |
| `protocol/testkit` | 测试支撑：语料加载、golden、语法校验器、严格请求校验器、客户端模拟器、语义摘要的 Loss 规则、场景脚本类型。角色 `fixture` | 自身、`protocol`、`protocol/ir`、`protocol/sse`，加 `os`、`embed` |
| `protocol/testkit/mockupstream` | 脚本化上游（`http.Handler`）。角色 `fixture` | 自身、`protocol/testkit`、`protocol/convert`，加 `net/http` |
| `protocol/testkit/cmd/corpus` | 语料导入和检查的命令行工具。角色 `fixture` | 自身、`protocol/testkit` |

编解码器包按协议 ID 的后半段命名，避开已有的 `anthropic`、`openai`、`gemini`、`qoder` 包名，不需要 import 别名。编解码器对兄弟线类型包（`protocol/anthropic` 等）和 `protocol/wirejson` 的依赖只用于复用现有的用量校验与归一化函数，不复用它们的请求响应结构体。`tools/architecture/catalog.go` 的 `leafDependencies` 中每个条目都必须在 `Production` 包含自身包路径，否则外部测试包（`package <pkg>_test`）无法导入被测包；`testkit` 及其两个子目录同样要登记 `leafDependencies`，否则会经 `nearest` 继承 `internal/protocol` 的空依赖集。

依赖方向：`sse` ← `ir` ← `codec/*` ← `convert` ← `testkit/mockupstream`。`testkit` 核心包只依赖 `ir` 和 `sse`，校验器和客户端模拟器都直接操作线上的 JSON 和 SSE 帧，不经过编解码器。这样 `codec/*` 和 `convert` 的包内测试可以导入 `testkit` 而不形成导入环。`ir` 自己的测试如果要用 `testkit`，使用配对真实源文件的 `essence_external_test.go` 或带 `package ir_test` 的 `<场景>_scenario_test.go`（`ir` 没有 `ir.go`，不能建 `ir_external_test.go`，也不能起名 `*_scenario_external_test.go`）。编解码器之间互不导入。新包不导入 `protocol/bridge`，旧包也不导入新包；两者只在测试的差分工具里相遇。

### 3.3 IR 类型（已定）

IR 是"消息加有序 part"的形状。它不等于任何一家的线协议：Responses 的平铺 item 在解码时按角色归并成消息，Messages 的 `tool_result` 块从 user 消息里拆出来成为 `tool` 角色消息。

IR 的 Go 类型带 JSON 标签，可以直接序列化成测试夹具和 golden。联合类型用"带 `Kind` 的结构体加可选指针"表达，由 `Validate` 保证恰好一个分支有值。

```go
package ir

type Request struct {
	Model              string           `json:"model"`
	System             []Part           `json:"system,omitzero"`   // out-of-band leading instructions
	Messages           []Message        `json:"messages"`
	Tools              []Tool           `json:"tools,omitzero"`
	Namespaces         []Namespace      `json:"namespaces,omitzero"` // Responses namespace declarations
	ToolChoice         *ToolChoice      `json:"tool_choice,omitzero"`
	ParallelToolCalls  *bool            `json:"parallel_tool_calls,omitzero"`
	Sampling           Sampling         `json:"sampling,omitzero"`
	MaxOutputTokens    *int64           `json:"max_output_tokens,omitzero"`
	Stop               []string         `json:"stop,omitzero"`
	Reasoning          *ReasoningConfig `json:"reasoning,omitzero"`
	Output             *OutputFormat    `json:"output,omitzero"`
	Stream             bool             `json:"stream,omitzero"`
	IncludeUsage       *bool            `json:"include_usage,omitzero"` // Chat stream_options.include_usage
	ServiceTier        string           `json:"service_tier,omitzero"`
	CacheKey           string           `json:"cache_key,omitzero"` // prompt_cache_key across protocols
	PreviousResponseID string           `json:"previous_response_id,omitzero"`
	SessionID          string           `json:"session_id,omitzero"`
	ConversationID     string           `json:"conversation_id,omitzero"`
	ConversationSet    bool             `json:"conversation_set,omitzero"` // true when Responses top-level conversation is non-null
	Store              *bool            `json:"store,omitzero"`
	UserID             string           `json:"user_id,omitzero"`
	MetadataUserID     string           `json:"metadata_user_id,omitzero"` // metadata.user_id when distinct from top-level user
	Extra              Extra            `json:"extra,omitzero"`
}

type Message struct {
	Role  Role   `json:"role"` // system | developer | user | assistant | tool
	ID    string `json:"id,omitzero"`
	Parts []Part `json:"parts"`
	Extra Extra  `json:"extra,omitzero"`
}

type Part struct {
	Kind        PartKind     `json:"kind"` // text | reasoning | tool_call | tool_result | media | refusal | opaque
	ID          string       `json:"id,omitzero"` // wire item id when the origin protocol assigns one
	Text        string       `json:"text,omitzero"` // text and refusal
	Annotations []Annotation `json:"annotations,omitzero"`
	Reasoning   *Reasoning   `json:"reasoning,omitzero"`
	Call        *ToolCall    `json:"call,omitzero"`
	Result      *ToolResult  `json:"result,omitzero"`
	Media       *Media       `json:"media,omitzero"`
	Opaque      *OpaqueItem  `json:"opaque,omitzero"`
	Cache       *CacheMark   `json:"cache,omitzero"` // Anthropic cache_control breakpoint: TTL "", "5m", "1h"
	Extra       Extra        `json:"extra,omitzero"`
}

type Reasoning struct {
	Segments []string     `json:"segments,omitzero"` // visible reasoning text, one entry per wire segment
	Summary  bool         `json:"summary,omitzero"`  // provider summary rather than raw reasoning
	Redacted bool         `json:"redacted,omitzero"` // Anthropic redacted_thinking
	Token    *ReplayToken `json:"token,omitzero"`
}

// ReplayToken is opaque continuity data that only its issuer scheme can accept.
type ReplayToken struct {
	Scheme     string `json:"scheme,omitzero"`      // anthropic | antigravity_claude | openai | xai | gemini | plain | unknown; assigned by convert
	ProviderID int64  `json:"provider_id,omitzero"` // optional issuing provider id for account-bound schemes (openai, xai)
	Data       string `json:"data"`                 // token text exactly as the wire field carries it
}

type ToolCall struct {
	CallID     string       `json:"call_id"`
	Tool       ToolRef      `json:"tool"`
	Arguments  string       `json:"arguments"` // JSON text for function tools, free text for custom tools
	Token      *ReplayToken `json:"token,omitzero"` // Gemini thoughtSignature attached to the call
	Unsigned   bool         `json:"unsigned,omitzero"` // CallMemory confirmed the upstream sent no token
	Incomplete bool         `json:"incomplete,omitzero"` // stream ended before the arguments completed
}

type ToolRef struct {
	Kind      ToolKind `json:"kind"` // function | custom | client_search | hosted
	Namespace string   `json:"namespace,omitzero"`
	Name      string   `json:"name"`
}

type ToolResult struct {
	CallID  string  `json:"call_id"`
	Tool    ToolRef `json:"tool,omitzero"` // filled by Link from the matching call
	Content []Part  `json:"content"`       // text and media parts only
	IsError bool    `json:"is_error,omitzero"`
}

type Media struct {
	Type   MediaType `json:"type"`   // image | audio | video | document
	MIME   string    `json:"mime,omitzero"`
	Source string    `json:"source"` // base64 | url | file_id
	Data   string    `json:"data"`   // base64 payload, URL, or file id according to Source
	Name   string    `json:"name,omitzero"`
	Detail string    `json:"detail,omitzero"`
}

// OpaqueItem is a wire item that only its origin protocol can express.
type OpaqueItem struct {
	Origin protocol.ProtocolID `json:"origin"`
	Type   string              `json:"type"`
	Raw    jsontext.Value      `json:"raw"`
}

type Tool struct {
	Kind         ToolKind       `json:"kind"`
	Namespace    string         `json:"namespace,omitzero"`
	Name         string         `json:"name"`
	Description  string         `json:"description,omitzero"`
	InputSchema  jsontext.Value `json:"input_schema,omitzero"` // raw JSON Schema, never re-parsed through float64
	Strict       *bool          `json:"strict,omitzero"`
	Format       jsontext.Value `json:"format,omitzero"`       // custom tool grammar, raw
	Hosted       *OpaqueItem    `json:"hosted,omitzero"`       // hosted tool declaration, raw
	DeferLoading *bool          `json:"defer_loading,omitzero"`
	Cache        *CacheMark     `json:"cache,omitzero"`
	Extra        Extra          `json:"extra,omitzero"`
}

type ToolChoice struct {
	Mode         ToolChoiceMode `json:"mode"` // auto | none | required | tool
	Tool         ToolRef        `json:"tool,omitzero"`
	AllowedTools []ToolRef      `json:"allowed_tools,omitzero"` // explicit subset for Qoder and Gemini allowedFunctionNames
}

type Sampling struct {
	Temperature      *float64 `json:"temperature,omitzero"`
	TopP             *float64 `json:"top_p,omitzero"`
	TopK             *int64   `json:"top_k,omitzero"`
	Seed             *int64   `json:"seed,omitzero"`
	FrequencyPenalty *float64 `json:"frequency_penalty,omitzero"`
	PresencePenalty  *float64 `json:"presence_penalty,omitzero"`
	CandidateCount   *int64   `json:"candidate_count,omitzero"`
}

type ReasoningConfig struct {
	Mode             string `json:"mode,omitzero"`    // "" | enabled | adaptive | disabled
	Effort           string `json:"effort,omitzero"`  // "" | none | minimal | low | medium | high | xhigh | max
	BudgetTokens     *int64 `json:"budget_tokens,omitzero"`
	Summary          string `json:"summary,omitzero"` // "" | auto | concise | detailed
	IncludeEncrypted bool   `json:"include_encrypted,omitzero"`
}

type OutputFormat struct {
	Kind      string         `json:"kind,omitzero"` // "" | text | json_object | json_schema
	Name      string         `json:"name,omitzero"`
	Schema    jsontext.Value `json:"schema,omitzero"`
	Strict    *bool          `json:"strict,omitzero"`
	Verbosity string         `json:"verbosity,omitzero"`
}

type Response struct {
	ID          string  `json:"id,omitzero"`
	Model       string  `json:"model,omitzero"`
	CreatedAt   int64   `json:"created_at,omitzero"`
	Parts       []Part  `json:"parts"`
	Finish      Finish  `json:"finish"`
	Usage       *Usage  `json:"usage,omitzero"`
	ServiceTier string  `json:"service_tier,omitzero"`
	Extra       Extra   `json:"extra,omitzero"`
}

type Finish struct {
	Reason       FinishReason `json:"reason"` // stop | length | tool_calls | content_filter | stop_sequence | pause | error | other
	StopSequence string       `json:"stop_sequence,omitzero"`
}

// Usage keeps disjoint input buckets. Protocol totals are derived by encoders.
type Usage struct {
	Input              int64  `json:"input"` // fresh input: excludes cache reads and cache writes
	CacheRead          int64  `json:"cache_read"`
	CacheWrite         int64  `json:"cache_write"` // aggregate of all TTL classes
	CacheWrite5m       *int64 `json:"cache_write_5m,omitzero"`
	CacheWrite30m      *int64 `json:"cache_write_30m,omitzero"`
	CacheWrite1h       *int64 `json:"cache_write_1h,omitzero"`
	CacheWrite12h      *int64 `json:"cache_write_12h,omitzero"`
	Output             int64  `json:"output"` // includes reasoning
	Reasoning          int64  `json:"reasoning,omitzero"`
	ImageInput         int64  `json:"image_input,omitzero"`
	ImageOutput        int64  `json:"image_output,omitzero"`
	AudioInput         int64  `json:"audio_input,omitzero"`
	AudioOutput        int64  `json:"audio_output,omitzero"`
	AcceptedPrediction int64  `json:"accepted_prediction,omitzero"`
	RejectedPrediction int64  `json:"rejected_prediction,omitzero"`
	Speed              string `json:"speed,omitzero"` // Anthropic usage.speed ("fast")
}
```

配套函数，全部在 `ir` 包：

- `Validate(*Request) error`、`ValidateResponse(*Response) error`：结构不变量，包括 Kind 与分支匹配、`tool` 消息只含 `tool_result`、`ToolResult.Content` 只含 text 和 media。
- `Link(*Request)`：按 `CallID` 把每个 `ToolResult.Tool` 填成对应调用的 `ToolRef`。Gemini 编码器需要函数名，Responses 编码器需要区分 custom 和 function。
- `Essence(*Request) EssenceDoc`、`ResponseEssence(*Response) EssenceDoc`：与协议无关的语义摘要，供测试比较（5.4 节）。

各协议与用量桶的对应关系：Messages 的 `input_tokens` 等于 `Input`；Chat 的 `prompt_tokens` 和 Responses 的 `input_tokens` 等于 `Input + CacheRead + CacheWrite`。这与 `docs/interfaces/protocol_capabilities.md#client_usage_cache` 的口径一致。

### 3.4 扩展机制（已定）

IR 有三种保留协议特有信息的手段，适用场景不同：

| 手段 | 放什么 | 谁可以消费 |
| --- | --- | --- |
| 建模字段加来源标记 | 各家都有、语义相同、只是取值互不通用的内容。目前是 `ReplayToken`（Anthropic 的 `signature`、Responses 的 `encrypted_content`、Gemini 的 `thoughtSignature`） | 所有编码器，按 `Scheme` 是否与目标匹配决定写出或丢弃 |
| `OpaqueItem` | 整个条目只有来源协议能表达：Responses 的 `compaction`、`local_shell_call`、`mcp_*`、`image_generation_call`，Messages 的 `server_tool_use`、`web_search_tool_result`，托管工具声明 | 来源协议自己的编码器原样写回；其他编码器丢弃并上报 `opaque_item_dropped` |
| `Extra` | 已建模对象上未建模的成员 | 只有 `Origin` 相同的编码器整体写回；其他编码器忽略并上报 `extra_dropped` |

```go
// Extra holds members the origin codec did not model.
type Extra struct {
	Origin  protocol.ProtocolID        `json:"origin"`
	Members map[string]jsontext.Value `json:"members"`
}

func (e Extra) IsZero() bool { return len(e.Members) == 0 }
```

采集方式：编解码器的私有线结构体在每个对象上放一个 `map[string]jsontext.Value` 字段，标签为 `json:",embed"`。已在本仓库的 Go 1.27.4 上验证：这个字段会接住所有未声明的成员，值保持原始字节（大整数、小数位、`<&>` 都不变），编码时在已声明字段之后写回，配合 `json.Deterministic(true)` 按键名排序。`Extra.IsZero` 保证没有未知成员时（即使填了 `Origin`）仍被 `omitzero` 省略。注意 `json/v2` 默认区分字段名大小写，会把大小写变体（例如 `"Model"`）收进 `Extra.Members`；网关入口的 `wirejson.ValidateRequestObject` 继续在前置阶段拦截这类大小写碰撞和非规范拼写。

防止滥用的规则，都由测试强制：

1. 编解码器和变换代码不得按键名读写 `Extra.Members`。`convert` 包里放一个源码扫描测试（通过 `testkit` 读取源文件），在 `protocol/codec` 和 `protocol/convert` 的非测试代码里发现 `.Members[` 就失败。需要读取的字段必须提升为建模字段。
2. 未建模字段普查：对语料库里每个客户端请求，列出落进 `Extra` 和 `OpaqueItem` 的 JSON 路径，写成 golden（`testdata/golden/census/<protocol>.txt`）。新版客户端发来新字段时，这个 golden 会变，评审时决定建模、保持不透明还是有意忽略。
3. 新增建模字段的门槛：至少两个协议有等价概念，或者某个编码器必须读取它才能生成合法请求。只有一家有、其他家只能丢弃的内容留在 `Extra` 或 `OpaqueItem`。

原始请求体不进 IR。网关已经在 `requeststate` 里持有原始字节，原样透传路径继续用它。

### 3.5 流事件模型（已定）

```go
type Event struct {
	Kind        EventKind     `json:"kind"` // response_start | part_start | part_delta | part_end | usage | response_end | error
	Response    *ResponseMeta `json:"response,omitzero"`     // response_start: ID, Model, CreatedAt, ServiceTier
	Index       int           `json:"index,omitzero"`        // part events
	Part        *Part         `json:"part,omitzero"`         // part_start: skeleton; part_end: complete snapshot
	Delta       *Delta        `json:"delta,omitzero"`        // part_delta
	Usage       *Usage        `json:"usage,omitzero"`        // usage: full merged snapshot, latest wins
	Finish      *Finish       `json:"finish,omitzero"`       // response_end
	ServiceTier string        `json:"service_tier,omitzero"` // response_end when reported on terminal frame
	Err         *StreamError  `json:"error,omitzero"`        // error: Type, Code, Message, Status
}

type Delta struct {
	Text      string `json:"text,omitzero"`      // text, refusal, or reasoning text according to the open part
	Arguments string `json:"arguments,omitzero"` // tool call argument fragment
}
```

规范事件流的不变量。解码器保证这些不变量，编码器可以依赖它们，`ir.ValidateStream` 检查它们：

1. 第一个事件是唯一的 `response_start`。
2. part 是顺序的：任一时刻最多一个 part 处于打开状态，`Index` 从 0 开始逐个递增。
3. `part_delta` 只出现在对应 part 打开期间，增量类型与 part 的 Kind 相符。
4. `part_end` 携带该 part 的完整最终快照：文本全文、工具调用的完整参数、推理的全部分段和重放令牌。解码器负责累积。
5. `usage` 事件可以出现在 `response_start` 之后的任何位置，每次都是解码器按协议规则合并稀疏帧后的完整快照（尾帧只报告输出 token 时不清空先前已观测的输入和缓存桶）。
6. 恰好一个终止事件（`response_end` 或 `error`），之后没有任何事件。`response_end` 之前所有 part 已关闭；上游在 `finishReason` 之后还会发独立用量尾帧的协议（如 Gemini），解码器先记下结束原因，等流结束调用 `Close(nil)` 时再发出最终 `usage` 和 `response_end`。
7. 上游异常结束时，解码器的 `Close(cause)` 先关闭打开的 part（工具调用标 `Incomplete`），再发 `error`。

第 2 条把"交错"这类问题集中到解码器。Chat 上游的并行工具调用会按 index 交错到达，解码器用 `ir.Sequencer` 缓冲非当前 part 的事件，当前 part 关闭后按序释放。文本流不受影响；交错的工具参数会晚一点到客户端，这与现在 `chatcompletions_anthropic_bridge.go` 在收尾时统一发出工具调用的行为等价。

第 4 条让非流式聚合变成"收集所有 `part_end` 快照"，也让需要完整内容的编码器（Gemini 客户端要完整的参数对象，custom 工具还原要完整的包装）不必自己累积。

`ir.Accumulator` 把事件流折叠成 `ir.Response`。非流式响应的编码统一走"事件流→Accumulator→`EncodeResponse`"，上游被强制流式而客户端要非流式时也是这条路。

### 3.6 编解码器契约（已定）

接口定义在 `convert` 包（使用方定义接口），各编解码器包导出具体类型，由 `convert/registry.go` 里的显式表登记，不用 `init` 注册。

```go
package convert

// clientCodec is the half a protocol needs to act as the client-facing side.
type clientCodec interface {
	DecodeRequest(body []byte) (*ir.Request, error)
	EncodeResponse(resp *ir.Response, view ClientView) ([]byte, []ir.Loss, error)
	NewStreamEncoder(view ClientView) StreamEncoder
}

// upstreamCodec is the half a protocol needs to act as the upstream-facing side.
type upstreamCodec interface {
	EncodeRequest(req *ir.Request, dialect Dialect) ([]byte, []ir.Loss, error)
	DecodeResponse(body []byte) (*ir.Response, error)
	NewStreamDecoder() StreamDecoder
}

type StreamDecoder interface {
	Decode(frame sse.Frame) ([]ir.Event, error)
	Close(cause error) []ir.Event
}

type StreamEncoder interface {
	Encode(event ir.Event) ([]sse.Frame, []ir.Loss, error)
}

// ClientView carries what a response encoder needs to know about the client.
type ClientView struct {
	Model   string      // model name to present to the client
	Request *ir.Request // decoded client request, for echo fields and include_usage
	Runtime Runtime
}

type Runtime struct {
	Now   func() time.Time
	NewID func(prefix string) string // response-side ids only
}
```

四个文本协议两半都实现。`qoderchat` 只实现 `upstreamCodec`。四个协议都实现客户端侧的一半，原因有两个：测试用目标协议的请求解码器把转换结果解回 IR 做比较；脚本化上游用目标协议的响应编码器生成上游响应。

约束：

- 编解码器是同步的纯计算，不起 goroutine，不读时钟和随机数（`Runtime` 除外），不做日志。
- 请求解码对字符串中的无效 UTF-8 宽容（`jsontext.AllowInvalidUTF8(true)`），与现在 `encoding/json` v1 的字符串容忍度一致；重复键保持 `json/v2` 默认拒绝（不设 `AllowDuplicateNames(true)`），与网关入口 `wirejson.ValidateRequestObject` 的重名键门禁一致，同时避免 Chat/Responses 字符串形式的 `arguments` 在转成 Messages/Gemini JSON 对象时夹带重名键。
- 请求编码用 `json.Deterministic(true)`。请求方向需要生成 id 时（例如目标协议要求而历史条目没有），用内容的 SHA-256 前缀派生，不用 `Runtime.NewID`。
- 编解码器不认识信封和方案。解码时把 `signature`、`redacted_thinking.data`、`encrypted_content`、`thoughtSignature` 的原文放进 `Token.Data`，`Scheme` 和 `ProviderID` 留空；编码时原样写出 `Token.Data`。方案判定、提供商作用域检查、信封的封装和解析、令牌取舍都在 `convert`（3.10 节）。脚本化上游因此可以直接用响应编码器合成真实上游会发的裸令牌。
- 响应编码器负责目标协议要求的全部必填字段和回显字段。Responses 的 `response` 对象要有 `id`、`object`、`created_at`、`status`、`model`、`output`，并从 `ClientView.Request` 回显 `tools`、`tool_choice`、`parallel_tool_calls`、`reasoning`、`text`、`store`、`instructions`、`max_output_tokens`、`previous_response_id`。`6b6987483` 就是缺 `created_at` 导致 Rust 客户端反序列化失败。
- 流编码器输出的每个 `sse.Frame` 带 `Semantic` 和 `Terminal` 标记，由 `convert` 直接根据结构化事件和流编码器状态填写（3.8 节），读取循环用它暂存前导帧并调用 `OutputContext.NextEvent`。

### 3.7 Dialect（已定）

`Dialect` 描述一个上游端点在基础协议之外的能力差异。它是纯数据，定义在 `convert` 包。

```go
type Dialect struct {
	Name                   string        // diagnostic label, for example "openai", "deepseek", "antigravity"
	ProviderID             int64         // selected provider id for account-bound replay schemes (openai, xai)
	Tools                  ToolSupport   // bit set: Function, Custom, Namespace, ClientSearch, HostedWebSearch
	ToolNameMaxBytes       int           // 0 means unlimited
	ReplayScheme           string        // token scheme this endpoint issues and accepts; "" means none
	ReasoningHistory       HistoryMode   // how plaintext reasoning returns in history: none | tool_turns | all
	SchemaDialect          SchemaDialect // json_schema | gemini | antigravity
	Reasoning              ReasoningCapabilities
	StripEffort            bool   // clear request effort after recording Info().Effort when effort_model_mapping_strip_effort matched
	DropTemperature        bool   // model attributes forbid temperature
	DropTopP               bool   // model attributes forbid top_p
	DropTopK               bool   // model attributes forbid top_k
	OmitStopSequences      bool   // target forbids stop sequences
	OmitStoreAndInclude    bool   // Responses target (for example Grok) omits cross-protocol store/include defaults
	AllowBillingHeader     bool   // retain x-anthropic-billing-header in system text (Qoder)
	PlaceholderToken       string // Gemini: thoughtSignature placeholder when a call has none
}

// ReasoningCapabilities is the model-side input for effort and budget resolution.
type ReasoningCapabilities struct {
	ControlsKnown  bool
	Supported      *bool
	Toggle         bool
	Efforts        []string
	Budget         bool
	BudgetMin      int
	BudgetMax      int
	ForceBudget    bool
	AdaptiveBudget int
	OutputLimit    *int
	BudgetPadding  int
}
```

`ReasoningCapabilities` 的字段直接对应现有的 `bridge.GeminiThinkingOptions`，供 Gemini 和 Messages 编码器把 `ir.ReasoningConfig` 解析成各自的预算或档位。解析算法从 `bridge/gemini_thinking.go` 的 `ResolveGeminiThinking` 移植，输入从"从三种请求体里解析"改为 `ir.ReasoningConfig`。注意 `ControlsKnown == false`（模型未登记推理选项）时 `Efforts` 为空，编码器仍须保留客户端传入的 `"max"` 档位（对齐旧 `ModelConversionOptions` 的 `SupportsMaxEffort == true`），同时对 `"ultra"` 档位返回 400 校验错误。

构造位置（已定）：`gateway/provider/model_attributes.go` 新增 `ConversionDialect(ctx context.Context, target *ExecutionProvider, result modelrules.Resolution, upstream protocol.ProtocolID) convert.Dialect`，取代 `ModelConversionOptions` 和 `GeminiThinking`。这是唯一允许根据平台、提供商类型和 `modelrules.Resolution` 取值的地方，取值来源：

| 字段 | 取值依据 |
| --- | --- |
| `ProviderID` | `target.Record.ID`，用于 `openai` 和 `xai` 方案的同提供商重放判定 |
| `Tools` | 上游协议是 Responses 且为 OpenAI OAuth / OAuth-like（`!APIKey`）时为全集；OpenAI API Key、Grok、DeepSeek 等需要客户端工具适配的 Responses 路径只有 `Function`（依据现有的 `passthrough.go:238`、`prelude.go:299`、`grok/request_patch.go`、`openai_ws_http_bridge.go:626`，OpenCode/Kimi 按 `RequiresResponsesClientToolEncoding` 对齐）。Messages 上游另有 `HostedWebSearch` |
| `ToolNameMaxBytes` | Chat 和 Responses 的函数名为 64（`chatToolNameMaxLen`）；Gemini 和 Messages 按官方文档当前的限制，实施时核对 |
| `ReplayScheme` | 从 `result.Rules.SignatureMode`、规范模型作者（`result.Target`）和目标协议推导，不单纯看 `target.Record.Platform`：Anthropic/Vertex/Bedrock 及 OpenCode Claude 为 `anthropic`；Antigravity 的 `claude-*` 模型单独设为 `antigravity_claude`（避免与同分组 Gemini 签名混用）；OpenAI 及 OpenCode OpenAI 为 `openai`；Grok 及 OpenCode xAI 为 `xai`；Gemini、Antigravity 的 `gemini-*` 模型及 OpenCode Gemini 为 `gemini`；其余（含 `passback` 模型）为空 |
| `ReasoningHistory` | Chat 上游为 `tool_turns`（对应 `b567a28f9`）；Messages 上游在 `result.Rules.SignatureMode == "passback"`（`deepseek-*`、`kimi-*`、`glm-*`、`minimax-m*`、`qwen*-thinking`，含 OpenCode 对应模型）时同样为 `tool_turns`，写出无签名 `thinking` 块；其他为 `none` |
| `SchemaDialect` | Antigravity 为 `antigravity`（`CleanJSONSchema`），Gemini 为 `gemini`（`NativeCleanToolSchema`），其他为 `json_schema` |
| `Reasoning` | 现有 `GeminiThinking` 函数里从 `modelrules.Resolution` 取值的那段逻辑 |
| `StripEffort` | `ExecutionModelPolicy(target).ShouldStripWireEffort(ctx, model)` 为真时置真：先保留原始 `Info().Effort` 供计费和日志记录，再在编码前清空 `req.Reasoning.Effort`，防止 Messages 和 Gemini 编码器从 effort 派生残留的 `budget_tokens` / `thinkingBudget` |
| `DropTemperature` 等采样限制 | 从 `result.Attributes` 和目标平台规则读取。部分转换入口（`openaiforward/anthropic_flows.go`、`raw_fallback.go`、`antigravity_messages.go`）原先不在编码后调 `ApplyModelAttributeParameters`，且 `ApplyModelAttributeParameters` 只删顶层字段、删不到 Gemini 的 `generationConfig.*`，由 `Dialect` 统一向编码器提供 |
| `OmitStoreAndInclude` | Grok 的 Responses 转换入口置真（对齐 `grok_chat_execution.go:97-98` 不写 `store` 和 `include`） |
| `AllowBillingHeader` | `upstream == qoder_chat` 时置真（对齐 Qoder 保留并归一化 `x-anthropic-billing-header` 的指纹规则） |
| `PlaceholderToken` | Gemini 及 Antigravity 的 `gemini-*` 模型在非 `count_tokens` 请求下取 `DummyThoughtSignature`；Antigravity 的 `claude-*` 模型和 `count_tokens` 必须为空 |

### 3.8 转换引擎（已定）

```go
package convert

type Route struct {
	Client   protocol.ProtocolID
	Upstream protocol.ProtocolID
	Dialect  Dialect
}

type Options struct {
	Route       Route
	ClientModel string   // model name presented back to the client
	Runtime     Runtime
	Calls       CallMemory // optional upstream call bindings, see 3.10
	Tools       *ToolMap   // optional inherited tool map, for WebSocket sessions
}

// NewExchange decodes the client request for one upstream attempt.
func NewExchange(clientBody []byte, opts Options) (*Exchange, error)

func (x *Exchange) UpstreamRequest() ([]byte, error) // encoded once, then cached
func (x *Exchange) Losses() []ir.Loss
func (x *Exchange) ToolMap() *ToolMap
func (x *Exchange) Info() RequestInfo // Model, Stream, IncludeUsage, Effort

// Response converts a complete upstream response body.
func (x *Exchange) Response(upstreamBody []byte) ([]byte, error)

// Stream converts upstream frames into client frames as they arrive.
func (x *Exchange) Stream() *Stream

// Buffer consumes upstream frames and yields one complete client response body.
func (x *Exchange) Buffer() *Buffer

func (s *Stream) Write(frame sse.Frame) ([]sse.Frame, error)
func (s *Stream) Close(cause error) ([]sse.Frame, error) // nil cause: normal end of upstream
func (s *Stream) Summary() Summary                        // response id, finish, client-visible usage, content flags

func (b *Buffer) Write(frame sse.Frame) error
func (b *Buffer) Close(cause error) ([]byte, error)

// NeedsConversion reports whether a request uses cross-protocol IR conversion.
func NeedsConversion(route Route) bool // route.Client != route.Upstream

// NeedsResponsesToolAdaptation reports whether a same-protocol Responses request
// requires client-tool lowering (including inherited tools on WS turns that omit tools).
func NeedsResponsesToolAdaptation(dialect Dialect, clientBody []byte, inherited *ToolMap) bool

// AdaptResponsesTools lowers unsupported Responses client tools in place while
// preserving all unrelated request fields, item-level cache breakpoints, and compaction items.
func AdaptResponsesTools(clientBody []byte, dialect Dialect, inherited *ToolMap) ([]byte, *ToolMap, []ir.Loss, error)

// RestoreResponsesTools restores lowered client tool calls in a buffered Responses JSON body,
// and NewResponsesToolStream restores them across native Responses SSE frames without dropping
// hosted-tool streaming events.
func RestoreResponsesTools(upstreamBody []byte, tools *ToolMap) ([]byte, error)
func NewResponsesToolStream(tools *ToolMap) *ResponsesToolStream

// PrepareNativeReplay opens or removes gateway carriers in a request body that
// is forwarded natively. Bodies without a carrier at protocol token paths are returned unchanged.
func PrepareNativeReplay(route Route, body []byte) ([]byte, []ir.Loss, error)
```

每次跨协议上游尝试创建一个 `Exchange`。换号后路线不同，重新创建。解码结果不跨尝试缓存，先保持简单，基准测试表明有必要时再加。同协议 Responses 的客户端工具降级（阶段 5）走 `AdaptResponsesTools` 和 `NewResponsesToolStream`，内部复用 `lowerTools` / `raiseTools` 的命名与包装算法，只改写 `tools` 声明和工具调用/输出条目，保持其余请求字段、条目级 `prompt_cache_breakpoint`、Compact 报文和托管工具中间流事件原样透传。

请求管线：

1. 客户端协议的 `DecodeRequest`（请求体为 `/v1/chat/completions` 上无 `messages` 但带 `input` 的 Cursor 请求时，解码用 `ProtocolOpenAIResponses`，客户端响应视图仍保持 `ProtocolOpenAIChatCompletions`），然后 `ir.Validate`；先记录 `Info()`（保留剥离前的 `Effort` 供计费和日志使用）。
2. `openCarriers`：解析客户端历史里的令牌。信封还原成方案、签发提供商 ID 和签发方原文，不是信封的标为 `unknown`；按客户端调用 id 从 `CallMemory` 取回上游调用 id 和调用令牌（3.10 节）。
3. `stripClientMarkers`：跨协议且 `!Dialect.AllowBillingHeader` 时删除只对来源协议有意义的客户端标记，目前只有以 `x-anthropic-billing-header: ` 开头的 system 文本（现有 `isAnthropicBillingHeaderText`）。上报 `client_marker_dropped`。
4. `lowerTools`：`Dialect.Tools` 不支持请求里用到的工具种类时降级，生成 `ToolMap`（3.11 节）。
5. `selectReplayTokens`：按 3.10 节的写出规则保留或丢弃令牌，`plain` 令牌还原成推理文本。
6. 应用 `Dialect` 的跨协议请求调整：`StripEffort` 为真时清空 `req.Reasoning.Effort`；`DropTemperature` / `DropTopP` / `DropTopK` / `OmitStopSequences` 为真时删除对应参数并上报 `sampling_dropped` / `stop_dropped`；目标为 `ProtocolOpenAIResponses`（此时必为跨协议）时按来源协议补齐默认项（4.2 节）。
7. `ir.Link`。
8. 上游协议的 `EncodeRequest`。

响应管线，每个上游帧：

1. 上游协议的 `StreamDecoder.Decode`，然后给解码出的令牌标上 `Route.Dialect.ReplayScheme` 和 `Route.Dialect.ProviderID`。
2. `raiseTools`：按 `ToolMap` 还原工具身份。
3. `sealCarriers`：调用令牌和上游调用 id 写进 `CallMemory`；推理令牌和没有令牌的明文推理封装成信封（3.10 节）。
4. 客户端协议的 `StreamEncoder.Encode`。
5. 给每个输出帧填 `Semantic` 和 `Terminal`：`convert/output_meaning.go` 直接从结构化 `ir.Event` 与流编码器状态计算，覆盖 function/custom/search 工具、非空文本/推理/refusal 增量，以及 `[DONE]`、`message_stop`、`response.completed`、`response.incomplete`、`response.failed`、`error` 和 Chat 错误帧，不再用 `gjson` 二次反解析输出字节。

### 3.9 Loss（已定）

```go
type Loss struct {
	Kind   LossKind `json:"kind"`
	Path   string   `json:"path"`   // IR path, for example "messages[3].parts[1]"
	Detail string   `json:"detail,omitzero"`
}
```

初始的 Kind 集合：`extra_dropped`、`opaque_item_dropped`、`replay_token_dropped`、`reasoning_text_dropped`、`reasoning_text_inlined`、`cache_mark_dropped`、`media_unsupported`、`media_relocated`、`tool_lowered`、`tool_choice_approximated`、`hosted_tool_dropped`、`schema_approximated`、`system_relocated`、`parts_reordered`、`tool_pairing_repaired`、`sampling_dropped`、`stop_dropped`、`annotation_dropped`、`output_format_approximated`、`client_marker_dropped`、`candidate_count_ignored`、`id_rewritten`。新增 Kind 时同步在 `testkit` 里实现它对语义摘要的作用（5.4 节）。

用途：测试里验证上报完整并写成 golden；生产里由读取流程在 debug 级别记录每条路线的 Loss 计数，不影响请求。

### 3.10 推理连续性（已定）

问题：推理的重放数据由某个上游签发，只有同一方案的上游能接受。客户端用别的协议时，这份数据要么借客户端协议的不透明字段带回来，要么存在服务端。旧实现的做法分散在几处，全靠隐含约定：

- Responses 上游的 `encrypted_content` 原样写进 Messages 客户端的 `thinking.signature`（`responses_to_anthropic.go:113-121`）。下一轮 Messages 转 Responses 时，非空且不以 `gAAAA` 开头的签名写回 `encrypted_content`（`anthropic_to_responses.go:353-362`，为 Grok 引入，见 `d0cb4573e`）；Grok 拒绝时再用 `StripThinkingSignaturesJSON` 去掉签名重试。
- Responses 客户端转 Chat 上游时，DeepSeek 等要求工具轮次带 `reasoning_content`。网关在输出推理条目时把摘要文本按 item id 存进 Redis（`gateway/session/reasoning_history.go`，7 天），下一轮按 id 取回。
- Gemini 的推理签名不写给 Messages 客户端，`signature` 留空（`2435ab3f5`）。Gemini 的调用 id 和 `thoughtSignature` 按 API Key、提供商和上游模型存在进程内的表里（`gateway/session/gemini_tool_calls.go`，一小时），未命中时用兼容签名。

新引擎从第一个版本起采用下面的方案，上面这些规则不移植，也不为旧引擎写进客户端历史的令牌做嗅探（2.5 节）。

令牌的方案：

- 编解码器只搬运令牌原文（3.6 节），方案由 `convert` 判定。
- 从上游响应解码出的令牌，标上 `Route.Dialect.ReplayScheme`。`ReplayScheme` 为空的端点签发的令牌在封装前丢弃并上报，reasoning part 按没有令牌处理。旧实现如果把这类令牌交给了客户端，差分会把它列出来，按 6.2.4 的第 3 类处理。
- 从客户端请求解码出的令牌，`ir.ParseCarrier` 能解析的取信封里的方案；解析不了的一律标为 `unknown`，不按前缀猜签发方。

信封格式，定义在 `ir/token.go`：

```text
trc1:<scheme>:<payload>
payload 是 JSON 对象的 base64url 编码（无填充）：
  {"d": 签发方令牌原文, "t": 推理文本, "r": 是否为 redacted_thinking, "p": 签发提供商 ID（可选正整数）}
```

- `scheme` 取 `anthropic`、`antigravity_claude`、`openai`、`xai`、`gemini`、`plain` 之一。`ParseCarrier` 只接受这六个值和能解码的载荷，其他字符串都不是信封。
- `t` 出现在三种方案里。`anthropic` 和 `antigravity_claude`：上游用签名校验 thinking 文本，文本必须原样回到上游，不能依赖客户端回传的摘要。`plain`：文本只有这一份副本。其他方案的令牌自成一体，不带文本。
- `p` 在 `openai` 和 `xai` 方案里记录签发该密文的提供商 ID（`Dialect.ProviderID`）。这两个方案的加密推理与签发账号绑定，同分组换到其他账号时在本地即可识别并丢弃，省去一次必败的上游 `400 invalid_encrypted_content` 往返。
- 信封不加密，也不签名。它装的内容本来就在客户端可见的字段里，令牌真伪由上游自己校验；`plain` 的文本相当于客户端自己写的 `reasoning_content`。
- `ir.FormatCarrier` 和 `ir.ParseCarrier` 是仅有的两个入口，`convert` 的变换、原生路径的处理以及 `OpenAIEncryptedContentDigest` 共用它们。`upstream/openai/encrypted_content.go` 的 `OpenAIEncryptedContentDigest` 计算 SHA-256 前先调用 `ir.ParseCarrier` 取出内部签发方原文 `d`，确保客户端信封态和上游裸密文态在 HTTP 与 WS 会话失效密文表里得到同一个摘要。

客户端方向，`sealCarriers` 在客户端编码之前执行：

- 跨协议转换路线上，带令牌的 reasoning part 把令牌改写成信封再交给客户端编码器：Messages 写进 `thinking.signature`，redacted 的写进 `redacted_thinking.data`；Responses 写进 `reasoning.encrypted_content`。阶段 5 的同协议轻量工具适配不改写推理条目，保持原生裸令牌透传。
- 没有令牌但有文本的 reasoning part（Chat 上游的 `reasoning_content`、没有签名的 Gemini 思考摘要、`passback` Messages 上游的无签名 thinking）封装成 `plain` 信封。Messages 客户端不会再拿到空的 `signature`；Responses 客户端只回传 `encrypted_content` 时也能取回文本。
- Chat 客户端没有不透明字段。令牌丢弃并上报 `replay_token_dropped`，推理文本照常写进 `reasoning_content`。
- 调用令牌（Gemini `functionCall` 上的 `thoughtSignature`）和上游调用 id 写进 `CallMemory`，键是客户端看到的调用 id。三种客户端协议都没有逐个调用的不透明字段，这是方案里唯一需要服务端状态的部分。

上游方向，`openCarriers` 解析令牌，`selectReplayTokens` 按下表取舍：

| 令牌方案 | 写出条件 | 写出内容 | 不满足时 |
| --- | --- | --- | --- |
| 与 `Dialect.ReplayScheme` 相同 | 非账号绑定方案，或信封 `p` 与 `Dialect.ProviderID` 任一为 0，或两者相等 | 签发方原文；`anthropic` 和 `antigravity_claude` 同时用信封里的文本替换 thinking 文本 | 丢弃令牌，上报 `replay_token_dropped`；推理文本按 `ReasoningHistory` 处理 |
| `plain` | `Dialect.ReasoningHistory` 允许写回明文推理 | 推理文本（Chat 的 `reasoning_content`，或 `passback` Messages 的无签名 `thinking` 块） | 丢弃文本，上报 `reasoning_text_dropped` |
| `unknown` | 同协议路线 | 原文，等同于现在的原样透传 | 丢弃令牌，上报 `replay_token_dropped`；推理文本按 `ReasoningHistory` 处理 |
| 其他方案 | 不写出 | | 同上 |

调用令牌由 `openCarriers` 从 `CallMemory` 取回。命中且有令牌时写回原调用；命中但上游当时没有签名时，tool_call part 标 `Unsigned`；未命中时由 Gemini 编码器按现有规则写占位签名（4.4 节）。上游因签名被拒时的重试（`upstream/gemini/exchange_openai.go` 用兼容签名重试，`upstream/anthropic` 的签名修复，以及 `AsConverted` 跨 Claude 模型切号遇到签名 400 时的剥离重试）保留在上游侧，不进 `convert`。

`CallMemory` 由网关注入，注入前已经按 API Key、提供商和上游模型限定了范围：

```go
// CallMemory keeps upstream call identity that no client protocol can carry.
type CallMemory interface {
	Remember(clientCallID string, binding CallBinding)
	Recall(clientCallID string) (CallBinding, bool)
}

// CallBinding is the upstream identity behind one client-visible call id.
type CallBinding struct {
	UpstreamID string          // original upstream call id; empty when the upstream sent none
	Token      *ir.ReplayToken // call-bound replay token; nil for an observed unsigned call
}
```

网关用现有的 `session.GeminiToolCalls` 实现它（同时挂载到 `googleforward.Gemini` 和 `googleforward.Antigravity`），作用域、一小时有效期和容量上限都不变。

原生路径，`convert.PrepareNativeReplay`：客户端带着有信封的历史，下一轮可能落到原生透传路径，例如同一分组换了提供商，或者模型改由原生协议的提供商服务。Messages 原生路径（Anthropic 平台 `messageforward`、兼容平台的 Anthropic 端点）和 Responses 原生路径（OpenAI、Grok 等 HTTP Responses、`/v1/responses/input_tokens` 预检，以及专用 WS `gateway/ws/passthrough.go`、连接池 WS `gateway/ws/stream.go` 和 WS HTTP 桥 `openai_ws_http_bridge.go` 的原生转发分支）在发往上游之前调用它：

- 先做 `bytes.Contains(body, "trc1:")` 快速短路，命中后再用 `jsontext.Decoder`（开启 `jsontext.AllowInvalidUTF8(true)`）只扫描协议令牌路径：Messages 的 `messages[*]`（`role == "assistant"`）下 `content[*]` 的 `thinking.signature` 与 `redacted_thinking.data`；Responses HTTP 和 WS `response.create` 帧的顶层 `input[*]` 或 `response.input[*]` 中 `type == "reasoning"` 的 `encrypted_content`（忽略 `session.update` 等其他 WS 帧及用户/工具正文）。这些路径上没有合法 `trc1:` 信封时直接原样返回 `body` 切片，避免正文中出现字面量 `"trc1:"` 时改写空白和转义（守住 D10 的逐字节不变约束）。
- 信封方案与上游 `ReplayScheme` 匹配（`openai` 和 `xai` 同时满足 `p` 匹配规则）：还原成签发方原文 `d`，`anthropic` 同时还原 thinking 文本 `t`。
- 方案或账号不匹配：
  - Messages 且 `Dialect.ReasoningHistory == tool_turns`（`passback` 模型，如 Kimi、DeepSeek、GLM）：若信封带文本 `t`（`plain`、`anthropic`、`antigravity_claude`），保留 `thinking` 块并写入 `t`、清空 `signature`；否则删除该块。
  - 其他 Messages 路径：删除对应的 `thinking` 或 `redacted_thinking` 块；删除后若该条 assistant 消息的 `content` 数组变空，填入 `{"type":"text","text":"(assistant content removed)"}` 防止上游报空内容 400。若请求启用了 thinking 且当前工具循环的最后一条 assistant 消息不再以 `thinking` / `redacted_thinking` 块开头，用 `jsontext` 流式将该请求剩余的 `thinking` 块降级为普通文本块并移除顶层 `thinking`（不调用经过 `[]any` / `float64` 反序列化的旧 `FilterThinkingBlocksForRetry`）。
  - Responses 与 WS：若该 `reasoning` 条目仍有非空的 `summary` 或 `content`，只删除 `encrypted_content` 字段（`store: false` 时一并移除合成 `id`）；若没有任何可见内容剩余，删除整条 `reasoning` 条目（单元素 `input` 变空时保留空数组或按端点规则处理，不留孤立 `id`）。
- 除发生还原、删除或上述必要的消息级修复外，其余字段的原始字节保持不变。

Gemini 和 Chat 的原生路径不需要这一步：Gemini 客户端没有转换路线，历史里不会出现信封；Chat 没有不透明字段。

### 3.11 工具身份的降级和还原

现状：`bridge/responses_client_tools.go`、`responses_namespace.go`、`responses_tool_identity.go`、`responses_tool_history.go`、`responses_tool_search_discoveries.go`、`responses_input_namespace.go`、`client_tools_json.go`、`responses_tool_adaptation.go` 合计约 2,700 行，在线报文上把 Responses 的 custom、namespace、tool_search 工具降成普通函数，再在响应里还原。跨协议转换时这套逻辑通过 `ResponsesClientToolMapping`（77 处引用）和 `ChatCompletionsResponseToResponses` 的四个还原参数传递。

新设计（已定）：降级和还原是 `convert` 包里的两个 IR 变换，与协议无关。

- `lowerTools(req, dialect) (*ToolMap, []ir.Loss)`：
  - `Dialect.Tools` 缺 `Custom` 时，custom 工具降为函数，参数 schema 用现有的 `customToolInputSchema`，历史里的 custom 调用把输入包成 `{"input": "..."}`。
  - 缺 `ClientSearch` 时，tool_search 降为名为 `tool_search` 的代理函数，schema 用现有的 `toolSearchProxySchema`。
  - 缺 `Namespace` 时，命名空间工具拍平成单一名字。命名算法从 `flattenNamespaceToolName` 和 `customToolAlias` 移植，包括 64 字节上限、哈希后缀、Codex 默认 `functions` 作用域视为裸名、冲突检测。历史里声明已经消失的调用，用 `responsesToolHistoryNames` 的算法取重放名。
  - tool_search 结果里发现的工具声明提升进工具列表（`EffectiveResponsesTools` 的现有规则），`additional_tools` 条目由 Responses 解码器直接并入 `Tools`。
- `raiseTools(events, toolMap)`：对事件流做逆向。被降级的 custom 调用在 `part_start` 时改回 custom 身份，吞掉参数增量，在 `part_end` 拿到完整包装后解出 `input`，发一个增量再结束。tool_search 代理和命名空间同理改回身份。
- `ToolMap` 可以序列化，可以通过 `Options.Tools` 或 `AdaptResponsesTools` 的 `inherited` 参数传入上一轮状态，供 Responses WebSocket 会话跨轮继承（现有 `AdaptResponsesClientToolsJSONWithMapping` 的用法）。`ToolMap` 同时保存降级后的工具声明字节切片（`LoweredTools []jsontext.Value`），使 Codex WS 第 1 轮执行本地 `generate: false` 预热、第 2 轮省略 `tools` 字段时仍能补齐已降级的工具列表。

命名要求（已定）：同一个版本里，同样的工具声明和历史得到同样的名字；名字不超过 `Dialect.ToolNameMaxBytes`，符合目标协议的字符集；同一请求内没有冲突；`raiseTools` 能按 `ToolMap` 还原每个名字；历史里声明已经消失的调用也有确定的重放名。不要求与旧版逐字节相同（2.5 节）。移植时先照搬旧算法，差分报告列出新旧不同的名字（6.2.4 节的 D8）；有理由改进算法时可以改，改动写进进度记录。

Codex 反向表、Anthropic OAuth 工具名改写、`CodexToolCorrector`、Qoder 的名字别名是上游侧的线级策略，不在本节范围，保持现状。

### 3.12 JSON 处理约定（已定）

- 编解码器和 IR 使用 `encoding/json/v2` 和 `encoding/json/jsontext`。需要在 `tools/architecture/catalog.go` 的 `pureStandard` 里加入这两个路径（架构检查按完整路径匹配，`encoding/json` 不覆盖子包）。
- 每个编解码器在自己的 `wire.go` 里定义私有的线结构体，不导出，不复用 `protocol/openai`、`protocol/anthropic`、`protocol/gemini` 里现有的结构体。现有结构体是为旧转换器裁剪的子集，带自定义序列化，还被很多调用方使用，改它们风险大。线结构是编解码器的内部知识。
- v2 的行为差异要注意：默认拒绝无效 UTF-8（请求解码和 `PrepareNativeReplay` 显式开启 `jsontext.AllowInvalidUTF8(true)`，重复键保持默认拒绝，见 3.6）；默认大小写敏感；`omitempty` 按 JSON 空值判断，标量、指针和切片用 `omitzero`（注意 `omitzero` 只省略 `nil` 切片/map，空切片必须保持 `nil`，`Extra` 通过 `IsZero()` 省略空集合）；默认不做 HTML 转义。
- 原样保留的 JSON 片段（工具 schema、custom 工具的 format、托管工具声明、`OpaqueItem.Raw`）用 `jsontext.Value`，全程不解析成 `any`。
- 新代码不使用 gjson、sjson、`map[string]any` 处理报文，Gemini 的 schema 清洗除外（它需要遍历任意 schema，沿用现有算法时可以继续用 `map[string]any`，输入输出两端用 `jsontext.Value`）。

### 3.13 网关集成形态：按上游协议合并（已定）

接入后，每个上游协议族只剩两条路径：原生路径（现状，不动）和一条与客户端协议无关的转换路径。

请求侧，现在按来源协议分开的入口合并：

| 上游族 | 现有入口 | 合并后 |
| --- | --- | --- |
| Responses（OpenAI 兼容平台） | `openaiforward.RunMessages`（Messages 来源）、`RunChat`（Chat 来源）、Grok 的 `GrokExecutor.ChatResponses` | 一个转换入口，接收来源协议参数 |
| Chat（OpenAI 兼容平台的 raw chat） | `MessagesViaRawChat`、`ResponsesViaRawChat`（`openaiforward/raw_fallback.go`） | 一个转换入口 |
| Messages（Anthropic 平台） | `forward.AsResponses`、`forward.AsChat`（`gateway/forward/conversion.go`） | 一个 `forward.AsConverted` |
| Messages（兼容平台的 Anthropic 端点） | `ForwardNativeResponses`、`ForwardNativeChat`（`openaiforward/anthropic_flows.go`） | 一个转换入口，与上一行共用输出读取 |
| Gemini | `Gemini.Forward`（Messages 来源）、`ForwardAsResponses`、`ForwardAsChatCompletions`（`googleforward/gemini_openai.go`、`gemini_attempts.go`） | 一个转换入口 |
| Antigravity | `antigravity_messages.go` 的 `Forward`、`antigravity_openai.go` 的两个 `ForwardAs*` | 一个转换入口 |
| Qoder | `payload.go` 的三个解析器 | 一个从 IR 构造 `QoderPayloadRequest` 的函数 |

每个转换入口里，原来的"解析来源类型、调用 bridge 转换、序列化"替换成 `convert.NewExchange` 和 `UpstreamRequest`，后面的伪装、缓存限制（含 `Mimic -> EnforceCacheControlLimit -> InjectAnthropicCacheControlTTL1h` 顺序）、模型属性修正、鉴权、构造、发送步骤不变。`RunChat` 保留对 `/v1/chat/completions` 上无 `messages` 但带 `input` 的 Cursor 请求的识别：目标为 Responses 上游时走 Responses 原生/适配请求并把响应转回 Chat；目标为其他协议时请求侧按 `ProtocolOpenAIResponses` 解码、响应侧保持 `ProtocolOpenAIChatCompletions` 编码。

响应侧，现在按客户端协议分开的读取函数合并，每个上游族一对（流式和缓冲）：

| 上游族 | 现有读取函数 | 合并后 |
| --- | --- | --- |
| Responses | `upstream/openai/response_messages.go` 的 `ReadMessagesStreaming/Buffered`，`response_chat.go` 的 `ReadChatStreaming/Buffered` | `response_converted.go` 的 `ReadConvertedStreaming/Buffered` |
| Chat | `response_raw_chat.go` 的 `ReadCCAsResponsesStreaming/Buffered`、`ReadCCAsMessagesStreaming/Buffered` | `ReadRawChatConvertedStreaming/Buffered` |
| Messages | `gateway/forward/conversion_output.go` 的四个函数，`openaiforward/anthropic_output.go` 的四个函数 | 一对，放在 `openaiforward`，两类平台（含 Vertex 与 Bedrock）共用 |
| Gemini | `upstream/gemini/response.go` 的 `HandleStreamingResponse/HandleNonStreamingResponse`，`response_openai.go` 的三个处理函数 | 一对 |
| Antigravity | `response_stream.go` 的 `HandleClaudeStreamingResponse`，`response_compat_stream.go`、`response_compat_nonstream.go` | 一对 |
| Qoder | `stream_conversion.go` 的三个 `WriteQoder*StreamResponse` 和三个 `BuildQoder*` | 一对 |

合并后的读取函数保留现有循环里的全部上游原生逻辑：SSE 扫描、用量累加（`wire.ParseSSEUsageBytesWithType`、`UsageAccumulator`、`NormalizeStreamUsage`、Gemini 的 `observeRaw`）、转换前的原始 `ResponseModelObserver` 采集（正常返回与读错部分结果均写入 `UpstreamResponseModel`）、故障分类和切号、cyber 策略、缺失终止事件的处理、客户端断开后继续排空。只有"驱动 bridge 状态机并序列化"那一段换成 `stream.Write(frame)` 和写出返回的帧。有两个流控不变量必须保留：

1. 前导帧暂存窗口：`OutputContext.NextEvent` 会无条件置 `CommitForRetry: true`，写出非空字节即关闭故障转移窗口。因此 `ReadConvertedStreaming` 在收到首个 `Semantic == true` 的输出帧或不可重试终态之前，先把非语义前导帧（如 `response.created` 转出的 `message_start` 或 Chat 首块）暂存在本地缓冲，同时经过 `responseStreamLifecycleTracker`（支持 Codex 孤立 `error` 帧被后续 `response.completed` 恢复）、`EmptyCompleted` 检查和 Chat 的 `ChatSilentRefusalDetector`，确保收到可重试错误帧时仍能切号。
2. 延迟提交响应头与提前提交 200 后的终态 SSE 错误：流式读取器统一通过 `NewDeferredOutputContext` 在首个实际输出帧写出时再提交响应头，不在读首帧前调用 `WriteHeader(200)` 关闭等待期保活窗口；一旦 `DownstreamSSEKeepalive` 已提前提交 HTTP `200 text/event-stream`（`CommittedSSE(c) == true`），后续超时或上游失败一律通过无计时的本地错误写出回调发送客户端协议的终态 SSE 错误帧（Responses 客户端固定发合成 `response.failed`，见 `httpapi/stream_error.go`）。

用量的处理（已定）：计费用量继续由读取循环里现有的累加器产生，本次重构不改计费路径。客户端可见的用量由流解码器产生，解码器内部调用同一批带 `wirejson` 校验的归一化函数（`protocol/openai` 的 `ResponsesUsage` 解码和 `UsageAccumulator`，`protocol/anthropic` 的 `MergeAnthropicUsage` / `ParseSSEUsagePassthrough`，Gemini 的跨帧 `usageMetadata` 合并提取），不重新实现别名和缓存桶规则。T5 层有一条断言核对两者一致（5.6 节）。

输出抽象：统一用 `upstream.OutputContext`。`gateway/forward.Output` 接口和 `httpapi.ForwardConversionOutput` 适配器（连同 `openai_native_anthropic_ports.go:224` 的 `writeTextResponsesError` 调用点）在阶段 8 随 `conversion_output.go` 一起迁移并删除。

`UnifiedTextExecutor.native` 里按 `source` 的 `switch` 相应简化：每个平台在"来源协议等于路线协议"时调原生入口，否则调转换入口。`usesOpenAIExecutor` 的分派保持不变。

切换方式（已定）：每个上游族的转换入口和读取函数在阶段 2 到 6 接入，放在 2.4 节的接线开关后面，生产装配一直走旧流程。阶段 8 一次删除旧流程和接线开关。不设进程配置，没有回退开关。阶段 8 之前，T5 对旧路径和新路径都跑：旧路径的结果与 `legacy_baseline.json` 比较，新路径必须全绿。

## 4. 各协议的映射规范

本节给出规范映射。旧实现里的细节行为（默认值、下限、修复规则）以 6.2.4 节的差分流程为准：差分发现新旧不同时，除非属于验收差异清单里的类别，否则向旧行为对齐。推理令牌的取舍不在本节，统一按 3.10 节。每个小节末尾列出要对照的旧代码。

所有解码器的共同规则：

- 字符串形式的 content 解码成单个 text part。
- 未知的块或条目类型解码成 `OpaqueItem`，不报错。
- 每个对象上未声明的成员进 `Extra`。

### 4.1 Messages（`anthropic_messages`）

请求解码：

| 线字段 | IR |
| --- | --- |
| `system`（字符串或 text 块数组） | `System`，块上的 `cache_control` 进 `Part.Cache` |
| `messages[].role` 为 `user`、`assistant`、`system` | `Message.Role` |
| `text` 块 | text part，`citations` 进 `Extra` |
| `image` 块，`source.type` 为 `base64`、`url`、`file` | media part（image） |
| `document` 块 | media part（document），`title`、`context`、`citations` 进 `Extra` |
| `thinking` 块 | reasoning part，`thinking` 进 `Segments`，`signature` 原文进 `Token.Data` |
| `redacted_thinking` 块 | reasoning part，`Redacted` 为真，`data` 原文进 `Token.Data` |
| `tool_use` 块 | tool_call part，`id` 进 `CallID`，`input` 的原始 JSON 文本进 `Arguments` |
| `tool_result` 块 | 归入 `tool` 角色消息的 tool_result part；`content` 的 text 和 image 块进 `Content`；`is_error` 保留 |
| `server_tool_use`、`web_search_tool_result`、其他块 | `OpaqueItem` |
| `tools[]` 无 `type` 或 `type` 为 `custom` | function 工具，`input_schema`、`strict`、`defer_loading`、`cache_control` 对应 |
| `tools[]` 其他 `type` | hosted 工具，整项进 `Tool.Hosted` |
| `tool_choice` 的 `auto`、`any`、`tool`、`none` | `ToolChoice.Mode` 的 `auto`、`required`、`tool`、`none` |
| `tool_choice.disable_parallel_tool_use` | `ParallelToolCalls` 取反 |
| `max_tokens`、`temperature`、`top_p`、`top_k`、`stop_sequences`、`stream` | 对应字段 |
| `thinking.type`、`thinking.budget_tokens` | `Reasoning.Mode`、`Reasoning.BudgetTokens` |
| `output_config.effort`、`output_config.format` | `Reasoning.Effort`、`Output` |
| `metadata.user_id`、`prompt_cache_key`、`session_id`、`conversation_id` | `UserID` 与 `MetadataUserID`、`CacheKey`、`SessionID`、`ConversationID` |
| `speed` 为 `fast` | `ServiceTier` 为 `priority` |

一条 user 消息里既有 `tool_result` 又有其他块时，按出现顺序切成连续段：`tool_result` 段成为 `tool` 消息，其他段成为 `user` 消息，段之间的相对顺序不变。

请求编码（Messages 作为上游），合法化步骤在编码器内部按这个顺序做：

1. `System` 加上开头连续的 system、developer 消息合并成 `system` 块数组。对话中间的 system、developer 消息按旧实现处理并上报 `system_relocated`。
2. `tool` 消息和紧随其后的 `user` 消息合并成一条 user 消息，`tool_result` 块在前。
3. 相邻的同角色消息合并（旧 `mergeConsecutiveMessages`）。
4. 工具配对修复：没有结果的调用和没有调用的结果按旧 `normalizeAnthropicToolPairing` 处理，上报 `tool_pairing_repaired`。
5. reasoning part：经过 `selectReplayTokens` 后仍带令牌的，写成 `thinking` 或 `redacted_thinking` 块，位置保持在同一条 assistant 消息里的原位；没有令牌时，若 `Dialect.ReasoningHistory == tool_turns`（`passback` 模型，如 Kimi、DeepSeek、GLM），在带工具调用的 assistant 消息开头写出无签名的 `thinking` 块，否则按旧行为丢弃或内联成文本并上报对应 Loss。
6. `MaxOutputTokens` 缺省时用旧实现的默认值；`Reasoning` 通过 `Dialect.Reasoning` 解析出 `thinking` 和 `output_config.effort`（`ControlsKnown == false` 时保留 `"max"` 档位，`"ultra"` 档位返回 400）；`ServiceTier` 为 `priority`、`fast` 或 `ultrafast` 时映射出 `speed: "fast"`。
7. `Output` 的 json_schema 写进 `output_config.format`。
8. `ToolResult.Content` 里的图片保留在 `tool_result` 块内。

流解码（Messages 作为上游）：

| 线事件 | IR 事件 |
| --- | --- |
| `message_start` | `response_start`，并发一个 `usage`（保留 `usage.speed` 和 5m/1h 缓存写入桶，经 `wirejson` 校验） |
| `content_block_start` | `part_start`，按块类型建骨架 |
| `content_block_delta` 的 `text_delta`、`thinking_delta` | `part_delta.Text` |
| `input_json_delta` | `part_delta.Arguments` |
| `signature_delta` | 累积进当前 reasoning part 的 `Token`，在 `part_end` 交付 |
| `content_block_stop` | `part_end`，带完整快照 |
| `message_delta` | `usage`（与 `message_start` 用量按字段合并，保留 `speed`）；记下 `stop_reason` 和 `stop_sequence` |
| `message_stop` | `response_end` |
| `error` | `error` |
| `ping` | 丢弃（心跳由读取侧处理） |

`stop_reason` 到 `Finish.Reason`：`end_turn` 为 `stop`，`max_tokens` 和 `model_context_window_exceeded` 为 `length`，`stop_sequence` 为 `stop_sequence`，`tool_use` 为 `tool_calls`，`refusal` 为 `content_filter`，`pause_turn` 为 `pause`。

流编码（Messages 作为客户端）：事件序列必须是 `message_start`，若干个"`content_block_start`、增量、`content_block_stop`"，`message_delta`，`message_stop`。块的 `index` 是已输出块的序号，被丢弃的 part 不占序号。`message_start.message` 带 `id`、`type`、`role`、`model`、空的 `content`、`stop_reason: null`、`usage`（带 `Speed` 时同步写出 `usage.speed`）。reasoning part 的令牌在 `content_block_stop` 之前用 `signature_delta` 发出。`error` 事件写成 `event: error`。

对照的旧代码：`anthropic_to_responses.go`、`responses_to_anthropic_request.go`、`responses_to_anthropic.go`、`anthropic_to_responses_response.go`、`chatcompletions_anthropic_bridge.go`、`compat_replay_guard.go`。

### 4.2 Responses（`openai_responses`）

请求解码：

| 线字段 | IR |
| --- | --- |
| `instructions` | `System`，一个 text part |
| `input` 为字符串 | 一条 user 消息 |
| `message` 条目（含无 `type` 只有 `role` 的简写） | 消息；`input_text`、`output_text` 为 text part，`input_image` 为 media，`input_file` 为 media（document），`refusal` 为 refusal part；`output_text.annotations` 的 `url_citation` 进 `Annotations`，其他进 `Extra` |
| `reasoning` 条目 | assistant 消息里的 reasoning part：`id` 进 `Part.ID`，`summary[]` 进 `Segments` 且 `Summary` 为真，`content[]`（`reasoning_text`）进 `Segments` 且 `Summary` 为假，`encrypted_content` 原文进 `Token.Data` |
| `function_call` 条目 | tool_call part：`call_id`、`name`、`namespace`、`arguments`，`id` 进 `Part.ID` |
| `custom_tool_call` 条目 | tool_call part，`Tool.Kind` 为 custom，`input` 进 `Arguments` |
| `tool_search_call` 条目 | tool_call part，`Tool.Kind` 为 client_search |
| `function_call_output`、`custom_tool_call_output`、`tool_search_output` | `tool` 消息里的 tool_result part；`output` 是字符串时为单个 text，是内容数组时逐项解码 |
| `additional_tools` 条目 | 并入 `Tools`（现有 `LiftResponsesAdditionalTools`） |
| 其他条目（`compaction`、`local_shell_call`、`web_search_call`、`mcp_*`、`image_generation_call`、`item_reference`、`configuration_update` 等） | 所在位置的 `OpaqueItem` |
| `tools[]` 的 `function`、`custom`、`namespace`、`tool_search` | 对应 Kind；namespace 的子工具展开成带 `Namespace` 的工具，声明本身进 `Namespaces` |
| `tools[]` 其他类型（`web_search` 等） | hosted 工具 |
| `tool_choice`、`parallel_tool_calls` | 对应字段（`allowed_tools` 进 `ToolChoice.AllowedTools`） |
| `max_output_tokens`、`temperature`、`top_p`、`stream` | 对应字段 |
| `reasoning.effort`、`reasoning.summary` | `Reasoning.Effort`、`Reasoning.Summary` |
| `include[]` 含 `reasoning.encrypted_content` | `Reasoning.IncludeEncrypted`；其余值进 `Extra` |
| `text.format`、`text.verbosity` | `Output` |
| `service_tier`、`prompt_cache_key`、`previous_response_id`、`store` | 对应字段 |
| `conversation`、`session_id` | `ConversationID`（非 null 时 `ConversationSet` 为真）、`SessionID` |
| `user`、`metadata.user_id`、`safety_identifier` | `UserID`、`MetadataUserID`，`safety_identifier` 进 `Extra` |

平铺条目归并成消息的规则：连续的 assistant 侧条目（reasoning、role 为 assistant 的 message、各类调用、assistant 侧的不透明条目）归入同一条 assistant 消息；连续的工具结果归入同一条 `tool` 消息；user、system、developer 的 message 各自成消息。归并不改变顺序，编码时按原顺序拍平可以得到同样的条目序列。

请求编码（Responses 作为上游）：

1. `System` 写成 `instructions`（多个 part 用换行连接）还是开头的 developer 消息，按旧实现（`anthropic_to_responses.go` 把 system 放进 `input` 的 developer 条目以保持位置）。
2. 消息按顺序拍平成条目。assistant 的 text 用 `output_text`，其他角色用 `input_text`。
3. reasoning part：经过 `selectReplayTokens` 后仍带令牌的，令牌写进 `encrypted_content`；没有令牌只有文本的推理按旧行为处理。需要 `id` 而 IR 里没有时，用内容哈希派生。
4. 跨协议默认策略（仅在 `convert` 确认 `Route.Client != ProtocolOpenAIResponses` 时注入，`Responses.EncodeRequest` 本身不注入这些默认值，以保证定律 Q2 的同协议往返不变）：来源是 Messages 时默认设置 `parallel_tool_calls: true`、`text.verbosity: "medium"`，未显式关闭思考时补 `reasoning.summary: "auto"`（未指定 effort 时补默认 `"medium"`），显式关闭思考（`thinking.type == "disabled"`、`budget_tokens == 0` 或 `effort == "none"`）时省略 `summary` 并按 `Dialect.Reasoning` 模型能力安全回退（仅当 `ControlsKnown` 为真且 `Efforts` 含 `"none"` 时输出 `effort: "none"`，不含 `"none"` 但含 `"minimal"` 时回退为 `"minimal"`，不支持关闭思考的纯推理模型回退至最低可用档位，非推理、仅开关控制或能力未知模型直接省略 `effort`），正数 `max_output_tokens` 取不小于 128；来源是 Chat 时强制 `stream: true` 且正数 `max_output_tokens` 取不小于 128；`!Dialect.OmitStoreAndInclude` 时补 `include: ["reasoning.encrypted_content"]` 和 `store: false`（Grok 跳过这两项）。
5. 工具结果里的图片：Responses 的 `function_call_output.output` 支持内容数组时直接放入；旧实现把图片移到后面的 user 消息，按旧行为做并上报 `media_relocated`。

流解码（Responses 作为上游）：

| 线事件 | IR 事件 |
| --- | --- |
| `response.created` | `response_start`（含 `service_tier`） |
| `response.in_progress` | 无 |
| `response.output_item.added`（reasoning、function_call、custom_tool_call、tool_search_call） | `part_start` |
| `response.output_item.added`（message） | 无，等内容 part |
| `response.content_part.added`（output_text、refusal） | `part_start` |
| `response.output_text.delta`、`response.refusal.delta`、`response.reasoning_summary_text.delta`、`response.reasoning_text.delta` | `part_delta.Text` |
| `response.function_call_arguments.delta`、`response.custom_tool_call_input.delta` | `part_delta.Arguments` |
| `response.content_part.done` | text 或 refusal 的 `part_end` |
| `response.output_item.done` | reasoning、调用类条目的 `part_end`，以该事件里的完整条目为准（`encrypted_content`、最终参数）；message 条目无事件 |
| `response.output_item.done`（其他类型，如 `web_search_call`） | 一对 `part_start`、`part_end`，内容是 `OpaqueItem` |
| `response.completed`、`response.incomplete` | 一个 `usage`（含 30m/12h 缓存写入分档及外层零值回填、图片与音频明细，经 `wirejson` 校验），然后 `response_end`（含 `service_tier`） |
| `response.failed`、`error` | `error`（外层读取循环先过 `responseStreamLifecycleTracker`，Codex 模式下孤立 `error` 帧暂存至终态再决定恢复或发错） |

解码器要容忍上游的不规范：只有 `output_item.done` 没有增量时，直接补发 `part_start` 和 `part_end`；终止事件里 `output` 为空但前面有条目时以累积为准（现有 `BufferedResponseAccumulator.SupplementResponseOutput` 的规则）；`response.done` 等旧事件名和裸的 Response 对象按现有 `protocol/openai/response_lifecycle.go` 的归一化规则处理（`4296d1e3a`）。状态和原因到 `Finish`：`completed` 且含非 hosted 调用为 `tool_calls`，否则 `stop`；`incomplete` 的 `max_output_tokens` 为 `length`，`content_filter` 为 `content_filter`。

流编码（Responses 作为客户端）：

- 每个事件带递增的 `sequence_number`。
- `response.created` 和 `response.in_progress` 带完整的 `response` 骨架（`status: "in_progress"`、空 `output`、可选 `service_tier`）。
- 连续的 text、refusal part 归入同一个 message 条目：第一个 part 开始时发 `output_item.added`，每个 part 发 `content_part.added`、增量、`output_text.done`、`content_part.done`，`content_index` 逐个递增；遇到其他 Kind 的 part 或流结束时发该 message 的 `output_item.done`。这条规则对应 `ac38d88f0`。
- reasoning part：`output_item.added`，每个分段一组 `reasoning_summary_part.added`、`reasoning_summary_text.delta`、`reasoning_summary_text.done`、`reasoning_summary_part.done`，最后 `output_item.done` 带 `encrypted_content`（part 带令牌时）。
- 调用类 part：`output_item.added`（`arguments` 为空串），增量，`function_call_arguments.done`，`output_item.done`。custom 调用用 `custom_tool_call_input.*`。增量事件里不带 `name`（`091ec1f41`）。
- `response.completed` 的 `response.output` 是全部条目，`usage` 里 `input_tokens_details.cached_tokens` 和 `output_tokens_details.reasoning_tokens` 必须存在（Codex 对这两个明细字段是严格解析），30m 和 12h 缓存写入分档相加等于 `cache_write` 时输出 `cache_write_tokens_details`。
- `error` 事件：已经发过 `response.created`，或下游 HTTP `200 text/event-stream` 已由保活心跳提前提交时，统一写合成的 `response.failed`（带 `id`、`object: "response"`、`created_at`、`status: "failed"` 和空 `output`，对齐 `httpapi/stream_error.go`）；其余未提交情况按客户端错误格式写出。

对照的旧代码：`chatcompletions_to_responses.go`、`chatcompletions_responses_bridge.go`、`responses_to_chatcompletions.go`、`response_forward_wire.go`、`protocol/openai/responses_stream_event_wire.go`。

### 4.3 Chat Completions（`openai_chat_completions`）

请求解码：

| 线字段 | IR |
| --- | --- |
| `messages[].role` 为 `system`、`developer`、`user`、`assistant`、`tool`、`function` | 对应角色，`function` 视为 `tool` |
| content 的 `text`、`image_url`、`input_audio`、`file` 部分 | text、media |
| assistant 的 `reasoning_content`，其次 `reasoning` | reasoning part，放在该消息的最前面，`Summary` 为假 |
| assistant 的 `tool_calls[]`，以及旧的 `function_call` | tool_call part，`id` 进 `CallID` |
| assistant 的 `refusal` | refusal part |
| `tool` 消息 | tool_result part，`tool_call_id` 进 `CallID` |
| `tools[]` 的 `function`，以及旧的 `functions[]` | function 工具 |
| `tool_choice`、旧的 `function_call` | `ToolChoice`（含 `allowed_tools`） |
| `max_completion_tokens`，其次 `max_tokens` | `MaxOutputTokens` |
| `temperature`、`top_p`、`seed`、`frequency_penalty`、`presence_penalty`、`n`、`stop` | 对应字段，`stop` 为字符串时成为单元素数组 |
| `reasoning_effort`，以及嵌套的 `reasoning.effort` | `Reasoning.Effort`，嵌套形式优先（现有 `ExtractEffort` 的规则） |
| `response_format` | `Output` |
| `stream`、`stream_options.include_usage` | `Stream`、`IncludeUsage` |
| `service_tier`、`prompt_cache_key`、`user`、`metadata.user_id`、`session_id`、`conversation_id`、`parallel_tool_calls`、`store` | 对应字段 |

请求编码（Chat 作为上游）：

1. `System` 和 system、developer 消息写成 `system` 或 `developer` 角色的消息，角色保留；DeepSeek 不接受 `developer` 的问题由现有的线级后处理解决（`protocol/openai/compatibility_fields.go`），编码器不管。
2. assistant 消息：text part 连接成 `content`；tool_call part 写成 `tool_calls`；reasoning 文本按 `Dialect.ReasoningHistory` 写进 `reasoning_content`（`tool_turns` 表示只在带工具调用的 assistant 消息上写）。重放令牌丢弃并上报。
3. `tool` 消息的每个 tool_result 写成一条 `tool` 角色消息。结果里的图片按旧实现移到后面的 user 消息并加归属标记（现有 `toolOutputMediaMarker`、`toolOutputMediaAttribution`），上报 `media_relocated`。
4. 工具配对修复、空消息清理按旧 `normalizeChatMessages`。
5. `Output` 的 json_schema 写成 `response_format.json_schema` 包装（现有 `response_format.go`）。
6. 参数不是合法 JSON 的工具调用及其结果按旧行为丢弃并上报。
7. 跨协议默认策略（仅在 `Route.Client == ProtocolAnthropicMessages` 时由 `convert` 注入）：缺省开启 `parallel_tool_calls: true`，正数 `max_tokens` 取不小于 128；未显式关闭思考且未指定努力度时补默认 `reasoning_effort: "medium"`，显式关闭思考时按 4.2 节第 4 条相同的 `Dialect.Reasoning` 能力守卫裁决（仅模型明确支持 `"none"` 时发 `"none"`，支持 `"minimal"` 时回退 `"minimal"`，纯推理模型取最低可用档位，普通非推理、仅开关控制或能力未知 Chat 模型直接省略 `reasoning_effort`）。

流解码（Chat 作为上游）：

- 第一个 chunk 产生 `response_start`（`id`、`model`、`created`、`service_tier`）。
- `delta.content` 产生 text part 的开始和增量；`delta.reasoning_content` 或 `delta.reasoning` 产生 reasoning part，前者优先，前者显式为空串时不回退到后者（现有 `chat_stream_compat.go` 的规则）。
- `delta.tool_calls[]` 按 `index` 跟踪。后续增量里的空 `id`、空 `name` 不覆盖已有值（`826f45a2c`）。名字未知之前到达的参数先缓冲。并行调用交错时由 `ir.Sequencer` 顺序化。
- 推理、文本、工具调用之间切换时关闭上一个 part。
- `finish_reason` 记下，等到流结束或 `[DONE]` 时发 `response_end`；带 `usage` 的 chunk（包括 `choices` 为空的尾块）经 `wirejson` 校验和 `UsageAccumulator` 合并，并调用 `protocol.IncludeIndependentReasoningTokens` 计入 Grok 等把 `reasoning_tokens` 独立于 `completion_tokens` 报告的场景，产生 `usage`。
- 没有任何终止信号就断流时，`Close` 发 `error`（`bd39eb589`）。
- 上游重发累计内容的情况不在 Chat 解码器里处理（那是 Gemini 兼容上游的问题，见 4.4）。

流编码（Chat 作为客户端）：第一个 chunk 带 `delta.role: "assistant"`；text 增量写 `delta.content`；reasoning 增量写 `delta.reasoning_content`；工具调用在 `part_start` 时发带 `index`、`id`、`type`、`function.name` 和空 `arguments` 的 chunk，之后的参数增量 chunk 只带 `index` 和 `function.arguments`；`response_end` 发带 `finish_reason` 的 chunk；`IncludeUsage` 为真时再发 `choices` 为空、带 `usage`（含音频与预测明细）的 chunk（旧 `ReadChatStreaming` 在 Responses->Chat 路线上曾忽略客户端开关强制发 usage，统一按 `IncludeUsage` 控制后产生的差异归入 D3）；最后发 `[DONE]`。所有 chunk 的 `id`、`created`、`model` 相同，带 `ServiceTier` 时同步写出 `service_tier`。

`Finish.Reason` 到 `finish_reason`：`stop`、`stop_sequence`、`pause` 为 `stop`，`length` 为 `length`，`tool_calls` 为 `tool_calls`，`content_filter` 为 `content_filter`。

对照的旧代码：`chatcompletions_to_responses.go`、`chatcompletions_anthropic_bridge.go`、`chatcompletions_responses_bridge.go`、`responses_to_chatcompletions.go`、`protocol/openai/chat_stream_compat.go`、`upstream/openai/response_raw_chat.go`。

### 4.4 Gemini GenerateContent（`gemini_generate_content`）

请求解码：

| 线字段 | IR |
| --- | --- |
| `systemInstruction.parts[]` | `System` |
| `contents[].role` 为 `user`、`model` | `user`、`assistant` |
| part 的 `text`（`thought` 不为真） | text part |
| part 的 `text` 且 `thought` 为真 | reasoning part，`Summary` 为真 |
| part 的 `thoughtSignature` | 原文进所在 part 的 `Token.Data`（reasoning 或 tool_call）；挂在普通 text part 上的签名进该 part 的 `Extra` |
| part 的 `inlineData`、`fileData` | media part |
| part 的 `functionCall` | tool_call part，`id` 有则进 `CallID`，没有时用"消息序号加 part 序号"的内容哈希派生 |
| part 的 `functionResponse` | `tool` 消息里的 tool_result part，`response` 的 JSON 文本进一个 text part |
| part 的 `executableCode`、`codeExecutionResult` 等 | `OpaqueItem` |
| `tools[].functionDeclarations[]` | function 工具，`parameters` 或 `parametersJsonSchema` 进 `InputSchema` |
| `tools[]` 的 `googleSearch`、`codeExecution`、`urlContext` 等 | hosted 工具 |
| `toolConfig.functionCallingConfig` 的 `mode` 和 `allowedFunctionNames` | `ToolChoice` |
| `generationConfig` 的 `maxOutputTokens`、`temperature`、`topP`、`topK`、`stopSequences`、`seed`、`candidateCount`、`presencePenalty`、`frequencyPenalty` | 对应字段 |
| `generationConfig.thinkingConfig` 的 `thinkingBudget`、`thinkingLevel`、`includeThoughts` | `Reasoning` |
| `generationConfig` 的 `responseMimeType`、`responseSchema`、`responseJsonSchema` | `Output` |
| `safetySettings`、`cachedContent`、`labels` | `Extra` |

模型名和流式标记在 URL 路径里，不在请求体里。`DecodeRequest` 只处理请求体；`convert.Options` 不需要为此加字段，因为本计划范围内 Gemini 只作为上游，客户端侧的解码只在测试里用（测试直接比较请求体）。

请求编码（Gemini 作为上游）：

1. `System` 和 system、developer 消息合并进 `systemInstruction`。
2. `assistant` 写成 `model`，`tool` 消息写成带 `functionResponse` 的 `user` 内容；相邻同角色内容合并。
3. tool_call part 写成 `functionCall`，`Arguments` 解析成对象；带令牌时写 `thoughtSignature`。没有令牌、没有 `Unsigned` 标记且 `Dialect.PlaceholderToken` 非空时写占位值，条件按现有 `NativeEnsureGeminiFunctionCallThoughtSignatures` 和 `4b05d79a5`：每个步骤的首个调用缺签名时写占位值，后续已知未签名的并行调用保持原样。
4. tool_result part 写成 `functionResponse`，`name` 取 `ToolResult.Tool.Name`，`response` 按旧 `ParseToolResultContent` 的规则包装。
5. reasoning part：带令牌的随 `thought` part 写出；没有令牌时，普通 Gemini 不发历史思考块并上报，而 Antigravity 的 `claude-*` 模型（`Dialect.ReplayScheme == "antigravity_claude"`）按现有 `BuildParts` 规则把无签名历史思考降级为可见 `text` part 并清空本次请求的 `thinkingConfig`。
6. 工具 schema 按 `Dialect.SchemaDialect` 清洗：`gemini` 用现有 `NativeCleanToolSchema` 的算法，`antigravity` 用 `CleanJSONSchema` 的算法。两个算法都实现为纯函数（同时供保留的 `upstream/antigravity.CleanGeminiRequest` 复用），上报 `schema_approximated`。
7. `Reasoning` 通过 `Dialect.Reasoning` 解析成 `thinkingConfig`（移植 `ResolveGeminiThinking`）；`maxOutputTokens` 与预算的关系按 `EnsureMaxTokensGreaterThanBudget`；`generationConfig` 同步按 `Dialect.DropTemperature` / `DropTopP` / `DropTopK` / `OmitStopSequences` 过滤不支持的采样与停止序列参数。
8. 托管的网页搜索工具和函数工具并存时的处理按现有 `BuildInternalGeminiTools`、`InternalHasMixedToolInvocations`。
9. AI Studio 的字段归一化（`NativeNormalizeGeminiRequestForAIStudio`，迁入 `protocol/gemini`）和 Antigravity 的信封、身份补丁是线级后处理，留在 `upstream/gemini` 和 `upstream/antigravity`。

流解码（Gemini 作为上游）：

- 每个帧是一个 `GenerateContentResponse`，只看第一个候选。第一个帧产生 `response_start`（`responseId`、`modelVersion`）。
- text part 的文本是增量，但兼容上游可能发累计文本：按现有 `NativeComputeGeminiTextDelta` 的规则算出真正的增量。
- `thought` 为真的文本产生 reasoning part。
- `functionCall` 一次到齐，产生一组 `part_start`、`part_end`。兼容上游重发累计批次时按现有 `toolFrames` 的去重规则处理（`2435ab3f5`）。同名且没有 id 的多个调用不能合并（`d91aacd8e`）。没有 id 时用 `Runtime.NewID` 生成 `CallID`，`sealCarriers` 把它和上游 id、签名的对应关系写进 `CallMemory`。
- `usageMetadata` 先跨帧合并非 null 字段（避免只有 `candidatesTokenCount` 的尾帧冲掉先前的 `promptTokenCount` / `cachedContentTokenCount`），再计算 `usage`：`Input = promptTokenCount - cachedContentTokenCount`，`CacheRead = cachedContentTokenCount`，`Output = candidatesTokenCount + thoughtsTokenCount`，`Reasoning = thoughtsTokenCount`（现有 `NativeExtractGeminiUsage`）。
- `finishReason` 在 `Decode` 中记下，不在看到 `finishReason` 的当帧立即发 `response_end`（读取侧 `ReadGeminiStream` 在看到终态后会调 `c.Complete()` 停发保活并继续收集最多五秒的用量尾帧，见 `docs/interfaces/gemini_upstream.md#gemini_stream_lifecycle`）；等 `ReadGeminiStream` 返回后调用 `StreamDecoder.Close(nil)` 时再发出最终 `usage` 和 `response_end`，严格遵守 3.5 节不变量 6。`STOP` 且有函数调用为 `tool_calls`，否则 `stop`；`MAX_TOKENS` 为 `length`；`SAFETY`、`RECITATION`、`BLOCKLIST`、`PROHIBITED_CONTENT`、`SPII` 为 `content_filter`；其他为 `other`。
- `groundingMetadata`：阶段 3 保持旧行为，把来源整理成 Markdown 的 "Sources:" 尾注追加为文本，同时把结构化的引用放进 `Annotations`，供以后的编码器使用。

流编码（Gemini 作为客户端，只用于测试和脚本化上游）：每个增量事件写成一个带单个 part 的响应帧；tool_call 在 `part_end` 写成完整的 `functionCall`；`response_end` 写成带 `finishReason` 和 `usageMetadata` 的帧。

对照的旧代码：`gemini_native.go`、`gemini_contents.go`、`gemini_internal_options.go`、`gemini_internal_schema.go`、`gemini_thinking.go`、`gemini_stream.go`、`gemini_response.go`、`gemini_messages_stream.go`、`gemini_compat_stream.go`、`upstream/antigravity/request_transformer.go`。

### 4.5 Qoder（`qoder_chat`，只有上游侧）

- `protocol/qoder` 定义归一化的 `PayloadRequest`、消息、工具、工具选择与客户端能力拒绝错误（`RequestError`）；`codec/qoderchat.EncodeRequest` 将 `ir.Request` 编码成该结构体的 JSON 字节，取代 `ParseQoderChatCompletionsPayload`、`ParseQoderResponsesPayload`、`ParseQoderAnthropicMessagesPayload`（管理端连接测试 `provider/provider/qoder_test_execution.go` 同步改用该路径）。
- 编码阶段完整保留 `02f1b789f` 和 `201a0326d` 的无状态与会话规则：Responses 来源遇到 `Store == true`、`PreviousResponseID != ""`、`ConversationSet`、历史含 `OpaqueItem{Type: "item_reference"}` 或未配对的 `tool_result` 时直接返回类型化 `RequestError`（由外层映射为本地 `400 MODEL_CAPABILITY_UNSUPPORTED`，不触发切号和健康扣分）；提取 `SessionID`、`ConversationID`、`CacheKey`、`MetadataUserID` 和 `ToolChoice.AllowedTools`；`Dialect.AllowBillingHeader` 保留 `x-anthropic-billing-header` 供 Qoder 指纹归一化 `cch=<cch>;`。会话规划、指纹、站点和模型能力（含 `attrs.ToolCall == false` 时裁剪可选工具并拒绝必选 `tool_choice`）仍在 `upstream/qoder` 的 `withEffectiveControls` 执行。
- `StreamDecoder` 把 Qoder 的 SSE 事件解码成 IR 事件，包含现有的文本编码工具调用解析（`stream_tool_text.go`）和参数合并规则（`MergeQoderToolArguments`）。
- 工具名别名（`QoderToolNameMapper`）是 Qoder 侧的线级策略，在解码之后作为事件变换应用，留在 `upstream/qoder`。

## 5. 测试体系

### 5.1 分层

| 层 | 名称 | 位置 | 验证什么 | 何时运行 |
| --- | --- | --- | --- | --- |
| T1 | IR 单元测试 | `protocol/ir`、`protocol/sse` | 校验器、累加器、顺序化器、SSE 读取器、信封 | 每次 `make test-go` |
| T2 | 编解码器一致性 | `protocol/codec/*` | 单协议的解码、编码、流语法、不动点定律、fuzz | 每次 |
| T3 | 转换矩阵 | `protocol/convert` | 场景乘路线的定律、多轮重放、发布前与旧实现的差分 | 每次 |
| T4 | 语料回放 | 同 T2、T3，数据来自录制语料 | 真实客户端请求和真实上游流通过全部定律；未建模字段普查 | 每次 |
| T5 | 网关路由测试 | `internal/app` | 真实路由、执行器、输出包装加脚本化上游：选路、SSE 提交、保活、切号、错误格式、用量一致 | 每次 |
| T6 | 客户端实验室 | `internal/app` 加 `tools/clientlab` | 真实的 Codex、Claude Code、pi 二进制驱动真实网关路由，上游是脚本化假上游或真实提供商 | 路径触发、每晚、手动 |

T1 到 T5 都不需要 Docker 和网络，属于无标签的单元测试。T6 用环境变量开启，默认跳过。

一个重要的限制：T2、T3 里"编码再解码"的自洽测试发现不了编码器和解码器对协议的共同误解。事实来源只有四个：录制的真实流量（T4）、真实客户端（T6）、官方 SDK、真实上游（T6 的 live 模式）。所以语料库和客户端实验室不是可选项。

### 5.2 `protocol/testkit` 的组成

| 文件组 | 内容 |
| --- | --- |
| `corpus*.go` | 用 `embed` 内嵌 `testdata/`，提供按协议、类型、标签查询夹具的函数。纯包的测试通过它读数据，自己不导入 `os` |
| `golden.go` | `Golden(t, name, got)`：与调用包 `testdata/golden/` 下的文件比较，`TOKENROUTER_UPDATE_GOLDEN=1` 时写回（不用 `flag.Bool("-update")`，避免 `go test ./internal/protocol/...` 在未导入 `testkit` 的兄弟包上因未知参数失败） |
| `jsondiff.go` | JSON 语义比较，输出 JSON 路径级的差异 |
| `grammar_*.go` | 四个协议的流语法校验器：输入一串 `sse.Frame`，按状态机检查事件顺序、必填字段、索引连续性、终止条件，返回违规列表 |
| `request_rules_*.go` | 四个协议的严格请求校验器，直接检查线上的 JSON，按规则集返回违规（5.2.1） |
| `clientsim_*.go` | 四个协议的客户端模拟器：给定上一轮请求、本轮响应和工具结果的线报文，按真实客户端的习惯拼出下一轮请求（5.4 的多轮定律用） |
| `essence.go` | 每种 `LossKind` 对语义摘要的作用 |
| `script*.go` | 场景脚本的类型和加载 |
| `converter.go` | `Converter` 接口，让旧 bridge 和新引擎都能被同一套矩阵测试驱动（阶段 8 删除） |
| `doc.go` | 包说明（`testkit` 在阶段 0 已超过 15 个手写源文件，按 `layout.go` 门禁必须提供） |
| `mockupstream/` | 脚本化上游，一个 `http.Handler`（5.2.2）。单独成包，因为它依赖 `convert` |
| `cmd/corpus/main.go` | 语料导入和检查的命令行工具（位于 `internal/` 下，日志输出用 `fmt.Fprintln(os.Stderr, ...)` 或 `log/slog`，不导入标准库 `log`） |

校验器和客户端模拟器故意不经过编解码器：它们是独立写的第二份协议知识，用来检查编解码器，不能和被检查的代码共用实现。

#### 5.2.1 严格请求校验器

脚本化上游和 T3 都用它判断"上游会不会接受这个请求"。每条规则有编号、说明和出处（官方文档链接或提交、issue 编号）。初始规则集，实施时逐条对照官方文档核实：

| 协议 | 规则 |
| --- | --- |
| Messages | 每个 `tool_use` 在紧随的 user 消息里有同 id 的 `tool_result`，反之亦然；`tool_result` 块位于 user 内容的最前面；启用 thinking 时 `thinking` 块带非空 `signature`，且位于同条 assistant 消息的 `tool_use` 之前；`max_tokens` 为正，`budget_tokens` 小于 `max_tokens`；没有空的 text 块；工具名符合字符集和长度限制；严格模式下拒绝未知的顶层字段 |
| Responses | 每个调用输出条目有同 `call_id` 的调用条目，反之亦然（Codex 测试套件的 `validate_request_body_invariants`）；`reasoning` 条目后面跟着它所属的条目；函数名符合 `^[a-zA-Z0-9_-]{1,64}$` |
| Chat | `tool` 消息前面有带相同 `tool_calls[].id` 的 assistant 消息，下一条 user 或 assistant 消息之前全部调用都有结果；assistant 消息有 `content` 或 `tool_calls`；`arguments` 是合法 JSON；函数名符合 `^[a-zA-Z0-9_-]{1,64}$`；方言规则 `deepseek-thinking`：带 `tool_calls` 的 assistant 消息必须有 `reasoning_content` |
| Gemini | `functionResponse` 对应前一个 model 回合里同名的 `functionCall`；方言规则 `gemini-signature`：model 回合里每个步骤的第一个 `functionCall` 带 `thoughtSignature`；schema 不含该方言不支持的关键字；函数名符合官方限制 |

线上出现新的上游 400 时，流程是：加一条规则和一个复现夹具，再修代码。规则集因此逐步积累成可执行的提供商知识。

#### 5.2.2 脚本化上游

```go
type Script struct {
	Name    string
	Turns   []Turn
	Default Reply // reply for requests that match no turn, for example client side requests
}

type Turn struct {
	Match  Match   // how to recognize this turn, see below
	Expect []Check // predicates on the decoded upstream request
	Reply  Reply
	Fault  *Fault  // optional: HTTP status, truncate after N frames, delay
}

type Reply struct {
	Events []ir.Event // synthesized through the upstream protocol's response encoder
	Raw    string     // or: name of a recorded raw stream in the corpus, replayed verbatim
}
```

行为：

- 按请求路径识别协议：`/v1/messages`、`/v1/responses`、`/v1/chat/completions`、`/v1beta/models/{model}:generateContent` 和 `:streamGenerateContent`。
- 每个请求先过严格校验器。有违规时返回该提供商风格的 400，并把违规记进日志。测试结束时断言日志里没有预期之外的违规。
- 用该协议的请求解码器解成 IR，按 `Match` 找回合。`Match` 用历史里 assistant 消息和工具结果的数量定位回合，不用全局计数器。真实客户端会发旁路请求（例如 Claude Code 用小模型做的标题生成和后台请求），这些请求匹配不到任何回合，走 `Default`。
- 回复有两种来源：IR 事件经该协议的响应编码器合成，或者把语料库里录制的原始流逐字节重放。有录制时优先用录制。
- 记录收到的全部请求，供测试断言。

脚本化上游另有两种模式，供 T6 使用：`proxy-record`（转发到真实提供商并记录双向原始字节）和 `replay`（按回合序号重放录制的流）。

### 5.3 语料库

目录布局，位于 `backend/internal/protocol/testkit/testdata/`：

```text
scenarios/                 协议无关的场景脚本（IR JSON）
wire/<protocol>/request/   手写的请求夹具
wire/<protocol>/stream/    手写的 SSE 流夹具
wire/<protocol>/response/  手写的非流式响应夹具
corpus/clients/<client>/<version>/<scenario>/<n>.request.json    录制的真实客户端请求
corpus/upstreams/<provider>/<model>/<scenario>/<n>.stream.sse    录制的真实上游流
golden/ir/        解码结果的 IR 快照
golden/loss/      每个夹具乘路线的 Loss 报告
golden/census/    未建模字段普查
golden/wire/      规范场景集的编码输出
legacy_baseline.json   旧实现在各场景乘路线上的通过情况（阶段 8 删除）
diff_ledger.json       新旧差分里已接受的差异（阶段 8 删除）
```

路线清单另放在 `internal/app/testdata/golden/routes.txt`（6.1 节第 7 步），由路线表生成。

每个录制目录带 `meta.json`：协议、客户端或提供商、版本、采集日期、是否流式、备注。

采集方式：语料只来自客户端实验室。T6 的 `record` 开关把网关收到的客户端请求和脚本化上游（或 `proxy-record` 模式的真实上游）的双向字节写到 `testdata/corpus/_incoming/`，再用 `cmd/corpus import` 整理入库。不在生产代码里加流量采集功能，也不从线上用户流量取样。阶段 0 把 `backend/internal/protocol/testkit/testdata/corpus/_incoming/` 加入 `.gitignore`（防止未脱敏的原始捕获进入 `git status` 或快检），并在 `.gitattributes` 里给 `backend/internal/protocol/testkit/testdata/**/*.sse` 标 `-text -whitespace`（允许 `upstream_sse_quirks` 与录制流保留 CRLF 和行尾空格而不触发 `git diff --check` 失败）。`testdata/` 下不放 `.go` 文件。

`cmd/corpus import` 的清洗规则：不保存请求头，只在 `meta.json` 里留 `user-agent`、`anthropic-beta`、`anthropic-version`、`openai-beta`；用占位符替换 API Key、邮箱、用户主目录路径（正则表）；把 base64 媒体替换成小的合成图片；单个文件超过 512 KB 时拒绝入库，要求裁剪历史。`cmd/corpus verify` 检查入库文件不含密钥特征，语料总量不超过 15 MB。

每个受支持的客户端版本至少录一轮完整的场景集。客户端升级版本时重录，旧版本的语料保留。

### 5.4 定律

T2 到 T4 的主体是数据驱动的定律测试：同一个测试函数遍历全部适用的夹具。记 `Dec_P`、`Enc_P` 为协议 P 的解码和编码，`Agg` 为事件流聚合。

请求方向：

| 编号 | 定律 | 抓什么 |
| --- | --- | --- |
| Q1 | 每个 P 的请求夹具都能解码，结果通过 `ir.Validate` | 解码器的覆盖面 |
| Q2 | 令 `y = Enc_P(Dec_P(x))`，则 `Dec_P(y)` 等于 `Dec_P(x)`，且 `Enc_P(Dec_P(y))` 与 `y` 逐字节相等 | 同协议往返稳定、编码确定 |
| Q3 | 对每条路线 P→Q：`Convert(x)` 通过 Q 的严格请求校验器 | 上游会拒绝的请求 |
| Q4 | 对每条路线 P→Q：把编码器上报的每条 Loss 作用到 `Essence(Dec_P(x))` 上，结果等于 `Essence(Dec_Q(Convert(x)))` | 没有上报的丢失和改动 |
| Q5 | 同一输入转换两次，输出逐字节相等 | 不确定性（map 顺序、随机 id） |

Q4 是这套体系的核心。语义摘要是与协议无关的结构：按顺序列出每条消息的角色和每个 part 的种类与规范化内容（文本、工具调用的名字和规范化 JSON 参数、调用 id、媒体的 MIME 和内容哈希、重放令牌打开信封后的方案和原文），加上工具声明（名字、schema 的规范化哈希）、系统指令文本、采样和推理参数。每种 `LossKind` 在 `testkit/essence.go` 里有一个很小的独立实现，说明它允许摘要发生什么变化。源和目标的摘要之间出现任何 Loss 解释不了的差异，测试就失败。这样不需要为每条路线手写期望输出，同时保证"编码器说丢了什么，就只丢了什么"。

响应方向：

| 编号 | 定律 | 抓什么 |
| --- | --- | --- |
| S1 | 每个 Q 的流夹具解码后通过 `ir.ValidateStream` | 解码器对真实上游流的处理 |
| S2 | 有成对的流式和非流式夹具时，`Agg(DecStream_Q(s))` 的摘要等于 `DecResponse_Q(r)` 的摘要 | 流式和非流式解码不一致 |
| S3 | 对每个客户端协议 P：`EncStream_P(events)` 通过 P 的流语法校验器 | 事件顺序、必填字段、索引 |
| S4 | `Agg(DecStream_P(EncStream_P(E)))` 的摘要等于把上报的 Loss 作用到 `Agg(E)` 摘要后的结果 | 响应方向没有上报的丢失 |
| S5 | 把上游流按任意字节边界重新切分后喂给 SSE 读取器，或者把相邻文本增量合并、拆分后喂给解码器，聚合结果不变 | 分块边界、UTF-8 截断 |
| S6 | 流式编码的聚合结果等于非流式编码的结果 | 流式和非流式编码不一致 |
| S7 | 在上游事件流的每个事件边界截断并 `Close(err)`，客户端输出仍通过语法校验器的"异常终止"模式，不 panic | 断流后的悬空块、缺终止事件 |
| S8 | 客户端可见的用量由 `ir.Usage` 按 3.3 的口径推出，三个输入桶只相加一次 | 用量重复计数 |

多轮重放定律（针对 1.3 的第一类缺陷）：

| 编号 | 定律 |
| --- | --- |
| M1 | 取场景的第一轮：客户端请求经路线 P→Q 转换，上游流经 Q→P 转换成客户端响应。用 P 的客户端模拟器把这个响应和工具结果拼成第二轮请求，再经路线 P→Q' 转换。结果必须通过 Q' 的严格请求校验器，包括方言规则。Q' 遍历所有受支持的上游协议，包括与 Q 不同的情况（两轮之间切了提供商），以及 Q' 等于 P、第二轮经 `PrepareNativeReplay` 走原生路径的情况 |

客户端模拟器对每个协议有若干变体，对应真实客户端的不同习惯，例如 Chat 客户端有"保留 `reasoning_content`"和"丢弃未知字段"两种，Responses 客户端有"保留全部输出条目"和"非 OpenAI 提供商时剥掉 `encrypted_content`"两种。变体的依据来自 T6 录到的真实请求。

fuzz（Go 原生 `testing.F`）：每个解码器对任意字节输入不 panic；SSE 读取器对任意切分不 panic。种子取自语料库。CI 里只跑种子，本地和每晚任务跑定时 fuzz。

golden 用得克制：IR 快照、Loss 报告、普查、规范场景集的线输出。其余靠定律。

### 5.5 场景目录

场景用 IR 写一次，每个场景自动展开到所有适用的路线。初始场景：

| 场景 | 内容 |
| --- | --- |
| `text_basic` | 单轮文本，流式和非流式 |
| `text_unicode_split` | 多字节字符和 emoji 被增量边界切开 |
| `system_roles` | system 和 developer 指令，包括对话中间的 |
| `tool_single` | 一次工具调用和结果，两轮 |
| `tool_parallel` | 一轮里两个并行调用 |
| `tool_parallel_interleaved` | 上游把两个调用的参数增量交错发出（仅 Chat 上游） |
| `tool_args_fragmented` | 参数被切成很多小片，包括在转义序列中间切开 |
| `tool_args_empty` | 无参数调用，参数为空串和 `{}` 两种 |
| `tool_result_image` | 工具结果带图片 |
| `tool_result_error` | 工具结果标记为错误 |
| `tool_ids_missing` | 上游的调用没有 id 或后续增量带空 id、空 name |
| `reasoning_summary` | 推理摘要加最终文本 |
| `reasoning_tool_replay` | 推理、工具调用、下一轮带着推理历史回来 |
| `reasoning_token_foreign` | 历史里的重放令牌来自另一种方案 |
| `reasoning_carrier_roundtrip` | 转换路线输出的信封在下一轮回到同方案、另一方案和要求明文推理的上游 |
| `reasoning_carrier_native` | 带信封的历史下一轮走原生 Messages 或 Responses 路径：方案匹配时还原，不匹配时删除，必要时降级 thinking |
| `reasoning_encrypted_only` | Responses 客户端只回传 `encrypted_content`（`plain` 信封），下一轮发往要求 `reasoning_content` 的 Chat 上游 |
| `legacy_history` | 升级前的会话：客户端历史里有旧引擎写下的裸令牌（Messages `signature` 里的 `encrypted_content`）和空签名。请求仍通过严格请求校验器，每个丢弃项都有 Loss |
| `tools_custom` | Responses 的 custom 自由文本工具（Codex 的 `apply_patch`） |
| `tools_namespace` | 命名空间工具，包括拍平后超过 64 字节的名字 |
| `tools_search` | tool_search 及其发现的工具 |
| `tools_hosted_search` | 托管网页搜索工具的声明和结果 |
| `image_input` | base64 和 URL 两种图片输入 |
| `document_input` | PDF 输入 |
| `structured_output` | json_schema 输出格式 |
| `stop_sequence`、`max_tokens` | 两种截断的终止原因 |
| `refusal` | 拒答 |
| `usage_cache` | 带缓存读取和 5m、1h 写入的用量 |
| `stream_truncated` | 上游在各个位置断流 |
| `stream_error_event` | 上游在流中发错误事件 |
| `upstream_sse_quirks` | 只有 data 行、多行 data、CRLF、结尾没有空行、重发累计内容 |
| `empty_response` | 上游返回空内容 |

每个场景文件声明它需要的能力，不适用的路线自动跳过并在测试输出里列出。

### 5.6 T5：网关路由测试

基础是现有的 `internal/app/sse_keepalive_routes_scenario_test.go` 里的 `newHeartbeatRouteRouter`。它调用生产的 `RegisterGatewayRoutes`，用真实的 `httpclient.UpstreamPool` 连到 `httptest.Server`，只替换了认证、持久化存储和定时器。

改造：

1. 把 `newHeartbeatRouteRouter` 泛化成 `newProtocolLabRouter(t, labOptions)`，放进现有的 `internal/app/helpers_test.go`（`newHeartbeatRouteRouter` 保留为调用它的薄包装，兼容 `sse_keepalive_routes_scenario_test.go` 和 `gemini_sdk_scenario_test.go` 的既有调用点）。`labOptions` 可以声明多个提供商（平台、类型、上游地址、模型、启用的协议）和分组的 `protocol_fallbacks`，以及是否打开接线开关（2.4 节，通过在 7 组执行器与运行时的 `Options` 结构体上设置 `UseIRConversion: true` 生效，不在 Wire 中注册裸 `bool`；阶段 8 删除该字段）。路由装配补齐真实的 `ModelsHandler`、`CountTokensHandler`、`OpenAITokens`、`MessagesHandler`（覆盖 `/antigravity/v1/messages`）、`Antigravity`、`Qoder` 以及内存态 `session.GatewayCache`（现有 `newHeartbeatRouteRouter` 将这些入口留成 `nil`，直接请求会空指针 panic）。
2. 新增 `internal/app/protocol_lab_scenario_test.go`。对每个"客户端协议乘提供商种类"的组合和每个场景：启动脚本化上游，用 Go 的 `http.Client` 按客户端协议发请求，断言：
   - 客户端收到的流通过该协议的语法校验器；
   - 脚本化上游的日志里没有校验违规；
   - 客户端可见的用量与记录存储里结算的用量一致（按 3.3 的口径换算后比较）；
   - 上游在首字节前返回可切换错误时，请求切到第二个提供商并成功，客户端流没有混入第一个上游的内容；
   - 上游在输出开始后断流时，客户端收到该协议格式的错误事件，用量仍然被记录；
   - 错误响应是客户端协议的格式；
   - 同一分组里第一轮走转换路线、第二轮落到原生路径（以及反过来）时，第二轮请求通过上游的校验，信封按 3.10 节还原或删除。
   阶段 8 之前，每个组合对旧路径和新路径各跑一遍：旧路径的结果与 `legacy_baseline.json` 比较，新路径必须全绿。
3. 提供商种类至少覆盖：`anthropic`（Messages 上游）、`openai`（Responses 上游）、`openai_chat`（Chat 上游）、`gemini`（Gemini 上游），以及 Kimi 或 DeepSeek 的 Anthropic 端点（Messages 上游，兼容平台路径）。Qoder 和 Antigravity 的认证和会话较特殊，在各自阶段再加。

可选的 SDK 驱动：沿用 `tools/gemini_sdk` 的做法，在 `tools/clientlab/sdk/` 放官方 OpenAI、Anthropic SDK 的小脚本，由环境变量开启，验证官方 SDK 的流累加器能解析网关输出。这一项排在真实客户端之后，可以后补。

### 5.7 T6：客户端实验室

形态（已定）：真实客户端作为子进程，连到进程内的网关（`httptest.NewServer(newProtocolLabRouter(...))`），网关的上游是脚本化上游。不需要 Docker、数据库和真实服务器。测试代码在 `internal/app/client_lab_scenario_test.go`，客户端的版本锁定和配置模板在 `tools/clientlab/`。

客户端和路线矩阵：

| 客户端 | 客户端协议 | 要覆盖的上游协议 |
| --- | --- | --- |
| Codex CLI | Responses（HTTP） | Chat、Messages、Gemini，以及 Responses 原生作为对照 |
| Claude Code | Messages | Responses、Chat、Gemini，以及 Messages 原生作为对照 |
| pi | 每个模型可配 `api`：`openai-completions`、`openai-responses`、`anthropic-messages` | 每种客户端协议乘其余三种上游，加原生对照 |

原生对照组的意义：透传时客户端直接解析脚本化上游合成的流。对照组失败说明脚本化上游或该协议的响应编码器有问题，这是用真实客户端校验编码器的途径。

pi 一个客户端能覆盖三种客户端协议，优先接入。pi 的 `google-generative-ai` 模式只用来做 Gemini 原生对照，因为 Gemini 客户端目前没有转换路线。可选扩展：OpenCode、Gemini CLI（只做原生对照）、Codex 的 WebSocket 传输。

客户端的启动方式（调研结论，实施时用对应版本核对）：

- Codex：临时的 `CODEX_HOME`，其中 `config.toml` 定义 `[model_providers.lab]`（`base_url` 指向实验室网关的 `/v1`，`env_key`，`wire_api = "responses"`，`supports_websockets = false`，重试次数设为 0），并关闭 analytics 和更新检查。命令是 `codex exec --json --skip-git-repo-check --ephemeral -C <工作目录> "<提示>"`，加上允许无人值守执行工具的参数。Codex 只支持 Responses 线协议。它启动时还会向网关请求 `GET {base}/models`，实验室路由必须挂载真实的 `ModelsHandler`（现有夹具里 `endpoints.Models` 为 `nil` 会触发 panic；Claude Code 可能调用的 `CountTokensHandler` 同理一并挂载）。`tools/clientlab/` 下的模板目录避开 `.gitignore` 中未锚定的 `CLAUDE.md`、`.claude`、`tests`、`scripts`、`output`、`tmp` 命名。
- Claude Code：环境变量 `ANTHROPIC_BASE_URL`、`ANTHROPIC_API_KEY`、`ANTHROPIC_MODEL`、`ANTHROPIC_DEFAULT_HAIKU_MODEL`、`CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1`、`DISABLE_AUTOUPDATER=1`、临时的 `CLAUDE_CONFIG_DIR`。命令是 `claude -p "<提示>" --output-format stream-json --verbose --no-session-persistence`，加上允许无人值守执行工具的权限模式。跑两个配置：`--bare`（请求面最小）和默认（带完整的 beta 头、`cache_control`、`context_management` 等）。
- pi：包名已迁到 `@earendil-works/pi-coding-agent`。用 `PI_CODING_AGENT_DIR` 指向临时目录，其中 `models.json` 定义一个提供商和若干模型，每个模型指定 `api` 和 `baseUrl`。命令是 `pi -p --mode json --model lab/<模型> --no-session --offline --no-extensions --no-skills --no-context-files "<提示>"`。JSON 模式下失败不一定体现在退出码上，要断言事件流。

安全边界（已定）：

- 脚本模式下，"模型"让客户端执行的命令全部来自我们写的脚本（例如把一个随机串写进工作目录里的文件），工作目录是 `t.TempDir()`。这种模式可以在开发机和 CI 上直接跑，客户端使用跳过审批的参数（Codex 的 `--dangerously-bypass-approvals-and-sandbox`、Claude Code 的 `--permission-mode bypassPermissions`）。
- live 模式下命令由真实模型决定，只允许在一次性容器里运行。测试在 live 模式下检查环境变量 `TOKENROUTER_CLIENTLAB_CONTAINER=1`，没有就拒绝启动；这个变量只由 `tools/clientlab/Dockerfile` 的入口设置。

T6 的场景（每个场景对矩阵里每个格子各跑一次）：

| 场景 | 脚本 | 断言 |
| --- | --- | --- |
| `lab_text` | 一轮文本回复，含一个随机串 | 客户端退出码为 0，最终消息含随机串 |
| `lab_tool` | 第一轮让客户端用它的 shell 工具把随机串写进 `result.txt`，第二轮在收到工具结果后回复完成 | 文件内容正确；客户端事件流里有一次工具执行且没有错误事件；上游日志显示第二轮请求带着工具结果，没有校验违规 |
| `lab_tool_parallel` | 一轮里两个工具调用 | 两个文件都写入 |
| `lab_reasoning_tool` | 第一轮发推理（带该上游方案的重放令牌）加工具调用 | 第二轮请求通过上游的方言规则（例如 DeepSeek 的 `reasoning_content`、Gemini 的签名） |
| `lab_switch_provider` | 两轮之间把分组的提供商从上游协议 A 换成 B | 第二轮请求通过 B 的校验 |
| `lab_failover` | 第一个提供商在首字节前返回 529，第二个正常 | 客户端无感知地成功 |
| `lab_truncated` | 上游在输出中途断开 | 客户端报告错误或按自身策略重试，不挂起，不崩溃 |
| `lab_custom_tool` | 仅 Codex：让模型用 `apply_patch` 自由文本工具改一个文件 | 文件被修改（验证 custom 工具的降级和还原能被真实客户端解析） |
| `lab_long_stream` | 很长的文本加大参数的工具调用 | 内容完整 |

每个场景还断言：网关没有 5xx，每个请求只结算一次用量。

运行模式，由环境变量选择：

| 模式 | 上游 | 用途 |
| --- | --- | --- |
| `scripted`（默认） | 脚本化上游 | CI 和日常开发，确定、无网络 |
| `record` | 同上，另外把客户端请求和上游往返写到 `_incoming/` | 采集真实客户端的请求进语料库 |
| `live` | `proxy-record` 模式，转发到真实提供商 | 发布前的金丝雀；采集真实上游的流。只断言结构性结果（任务完成、无协议错误） |

live 模式是唯一能验证"真实提供商接受转换后的请求"的手段，费用和不确定性都高，不进常规 CI，由维护者在发布前和客户端大版本更新后手动跑。阶段 7 的 live 金丝雀是切换的前置条件。

阶段 8 之前，T6 的转换格子通过接线开关跑新路径。旧路径的 T6 成本高，只在阶段 0 和阶段 7 各跑一次，记录基线。

### 5.8 CI 和 Make 目标

- `make test-go` 自动包含 T1 到 T5。
- 新增 `make test-clientlab`：`cd tools/clientlab && npm ci`，然后 `TOKENROUTER_CLIENTLAB=1 go -C backend test ./internal/app -run '^TestClientLab' -count=1 -timeout 30m -v`。
- 新增 `.github/workflows/protocol-lab.yml`：
  - 触发条件：PR 改动了 `backend/internal/protocol/**`、`backend/internal/gateway/**`、`backend/internal/upstream/**`、`backend/internal/app/*lab*`、`tools/clientlab/**`；每晚定时；手动触发（可选客户端版本）。
  - 两个 job：`pinned` 用 `tools/clientlab/package-lock.json` 锁定的版本；`latest`（只在定时和手动触发时跑）安装各客户端的最新版，用来尽早发现新版客户端带来的不兼容。`latest` 失败不阻塞合并。
  - 阶段 8 之前，CI 里的 T6 只跑新路径；T5 的旧路径和新路径都在 `make test-go` 里自动运行。
- `tools/check_changed.py` 的快检不跑 T6。
- `tools/clientlab/package.json` 锁定三个客户端的确切版本。调研时的最新版本是 `@openai/codex` 0.169.0、`@anthropic-ai/claude-code` 2.1.323、`@earendil-works/pi-coding-agent` 1.5.2，实施时取当时的稳定版。

### 5.9 缺陷回归流程

写进 `docs/operations/development_workflow.md` 的验证策略：

1. 线上或 T6 发现转换问题后，先把触发它的请求或上游流整理成夹具：能用场景脚本表达的进 `scenarios/`，依赖真实报文细节的进 `corpus/`（经过清洗）。
2. 如果问题是上游拒绝了请求，在严格请求校验器里加规则；如果是客户端解析失败，在流语法校验器里加规则。
3. 确认现有定律在新夹具上失败，再修代码。
4. 不为单个缺陷写一次性的内联 JSON 断言，除非它确实无法用夹具加定律表达。

## 6. 实施阶段

每个阶段结束时都要：运行 `make fmt`、`make fmt-check`、`make lint-go`、`make test-go`、`make check-docs`；涉及 `tools/architecture` 时按 `development_workflow.md` 的要求用无标签和 integration 两个集合验证正反例；按 `project-doc` 评估文档影响；按 Conventional Commits 分步提交。阶段 0 到 7 的提交不能改变生产行为（2.4 节）；阶段 2 起，每次提交前确认"生产装配下接线开关关闭"的测试通过。

### 6.1 阶段 0：测试地基和基线

目标：不改生产行为，先把测试设施建起来，并用它给旧实现画一条基线。这一步同时验证测试设施本身。

步骤：

1. 架构规则与仓库配置（`tools/architecture/`、`.gitignore`、`.gitattributes`）：
   - `catalog.go` 的 `pureStandard` 加 `encoding/json/v2`、`encoding/json/jsontext`。
   - `leafDependencies` 按 3.2 的表为本阶段新建的包（`internal/protocol/sse`、`internal/protocol/ir`、`internal/protocol/testkit`、`internal/protocol/testkit/mockupstream`、`internal/protocol/testkit/cmd/corpus`）加条目，每个条目的 `Production` 都包含包自身路径。
   - `policy.go`：把 `fixture` 分支整体移到 `pure` 分支（包含 `within(dir, "internal/protocol")` 和 `l.leaf != ""` 条件）之前，确保 `internal/protocol/testkit` 及其子目录得到 `fixture` 角色并可使用 `os`、`embed`、`net/http`。
   - 为 `internal/protocol/bridge` 的 `Tests` 登记对 `internal/protocol/testkit` 的依赖（`"internal/app"` 属于 `assembly` 角色，已允许导入任意内部包，不要把 `"internal/app"` 加进 `moduleDependencies` 或尚无消费方的 `filePermissions`）。
   - `.gitignore` 加入 `backend/internal/protocol/testkit/testdata/corpus/_incoming/`；`.gitattributes` 为 `backend/internal/protocol/testkit/testdata/**/*.sse` 标 `-text -whitespace`。
   - 按现有惯例补正反例夹具。
2. `protocol/sse`：`Frame{Event string; Data []byte; Semantic, Terminal bool}`；`Reader`（增量解析，处理只有 data 行、多行 data、CRLF 与 LF 混用、注释行、EOF 处没有空行的最后一帧、单帧大小上限）；`Append(dst []byte, f Frame) []byte`。行为对照 `gateway/forward/conversion_sse.go` 和 `22235a63c` 的修复。测试包含任意切分的 fuzz。
3. `protocol/testkit` 第一版：语料加载、golden（使用环境变量 `TOKENROUTER_UPDATE_GOLDEN=1` 控制写回）、jsondiff、四个协议的流语法校验器和严格请求校验器、客户端模拟器、`Converter` 接口、脚本类型，以及 `doc.go`（本包手写源文件已超过 15 个）。`cmd/corpus/main.go` 不用标准库 `log`。`ir` 包在这一阶段先建出 `testkit` 用到的类型（`Event`、`Loss` 等类型定义先行，行为函数留到阶段 1）。
4. `protocol/testkit/mockupstream` 第一版：这一阶段还没有编解码器，回复只支持 `Raw`（手写或录制的线字节），回合匹配用按协议的轻量 JSON 检查实现（数历史里的工具结果条数）。阶段 1 之后改为把请求解码成 IR 再匹配，并支持用 IR 事件合成回复。源文件命名避开 `mock_upstream_*.go` 前缀重复。
5. 夹具：为 5.5 的每个场景手写四个协议的线夹具（请求、流、非流式响应），放进 `wire/`。数量大，按场景分批提交。
6. 旧实现的适配器和基线：在 `protocol/bridge` 里新增 `conversion_matrix_scenario_test.go`（文件开头注释列出覆盖的源文件），用测试内的适配器把旧转换函数包成 `testkit.Converter`，覆盖 Messages、Responses、Chat 之间的六条路线，以及 Messages 到 Gemini 的请求和 Gemini 到 Messages 的流。对每个场景乘路线跑定律 Q3、S3、S7，把通过情况写进 `legacy_baseline.json`。失败项逐个判断是旧实现的缺陷还是校验器规则写错了；校验器的问题当场修，旧实现的缺陷只记录，不在本阶段修。这个测试一直运行到阶段 8，结果与 `legacy_baseline.json` 不一致时失败，旧引擎的线上修复因此会提醒更新基线（2.4 节）。
7. 路线清单：在 `internal/app` 的测试里遍历平台、提供商类型和客户端协议，用 `capability.ProtocolFallbackTargets` 和 `SupportsProtocolConversion` 生成"客户端协议、平台、提供商类型、上游协议"的转换路线清单（包含 Anthropic `bedrock` 的转换路线，在阶段 2 补齐执行适配），加上不经过路线表的转换用途（Responses 同协议客户端工具降级、`/v1/chat/completions` 上 Cursor 的 `input` 形状请求、`count_tokens` 到 Gemini 和 OpenAI、输入 token 估算、Responses 请求体里旧式 `messages` 的改写），写成 golden `internal/app/testdata/golden/routes.txt`。路线表以后有增减时这个 golden 会变。阶段 7 按它逐行核对覆盖。
8. T5：按 5.6 泛化路由夹具（补齐 `ModelsHandler`、`CountTokensHandler`、`OpenAITokens`、`MessagesHandler` 和内存 `GatewayCache`），新增 `protocol_lab_scenario_test.go`，在旧引擎上跑通，失败项同样记入基线。
9. T6 骨架：`tools/clientlab/`（`package.json`、锁文件、各客户端的配置模板、`README.md`、live 模式用的 `Dockerfile`）；`client_lab_scenario_test.go` 实现三个客户端的驱动和 `lab_text`、`lab_tool` 两个场景；原生对照组先跑通，再跑转换路线，在旧引擎上记录基线。用 `record` 模式采集第一批真实客户端请求，经 `cmd/corpus import` 入库。
10. CI：新增 `protocol-lab.yml`，`Makefile` 加 `test-clientlab`。
11. 文档：`development_workflow.md` 的测试分层加入 T1 到 T6 的说明、运行方法和长期有效的缺陷回归流程（5.9 节，不把开发期临时规则写进长期文档）；`backend_modules.md` 的包地图补齐本阶段新建的全部 5 个包（`protocol/sse`、`protocol/ir`、`protocol/testkit`、`protocol/testkit/mockupstream`、`protocol/testkit/cmd/corpus`）。

验收：

- `make lint-go`、`make test-go` 通过。
- `legacy_baseline.json` 入库，每个失败项有一行原因。
- `routes.txt` 入库，人工核对过它与 3.13 两张表的上游族一致。
- 本机 `make test-clientlab` 能跑完三个客户端的原生对照组。
- `corpus/clients/` 下有三个客户端各至少一个场景的录制请求。

### 6.2 阶段 1：IR、三个文本编解码器、转换引擎

目标：实现 `ir`、`codec/messages`、`codec/responses`、`codec/chat`、`convert`，全部是纯代码，不接入网关。推理连续性直接按 3.10 节的最终方案实现，包括信封、`CallMemory` 接口和 `PrepareNativeReplay`。

#### 6.2.1 `ir`

文件：`request.go`（含 `Extra` 及其 `IsZero`）、`response.go`、`event.go`、`usage.go`（含与 `protocol.TokenUsage` 的换算辅助，保持有效代码不少于 30 行）、`loss.go`、`validate.go`、`accumulator.go`、`sequencer.go`、`token.go`（重放令牌和信封的 `FormatCarrier`、`ParseCarrier`）、`essence.go`、`link.go`。各有对应的 `_test.go`，需导入 `testkit` 的外部测试放在 `essence_external_test.go` 或带 `package ir_test` 的 `*_scenario_test.go`。`token_test.go` 覆盖信封往返、六种方案、可选提供商 ID `p`、非法载荷和普通字符串（包括以 `gAAAA` 开头的裸令牌）都不被当成信封。

#### 6.2.2 编解码器

每个编解码器包的文件：`wire.go`（私有线结构体）、`request_decode.go`、`request_encode.go`、`response.go`（非流式的解码和编码）、`stream_decode.go`、`stream_encode.go`、`usage.go`、`finish.go`，必要时 `legalize.go`。按第 4 节的映射实现，避开重复包名前缀的文件名。`FuzzStreamDecode` 放进 `stream_decode_test.go`，跨多个源文件的 T2 定律测试放进 `conformance_scenario_test.go`（不单独建无配对源文件的 `fuzz_test.go` 或 `laws_test.go`）。实现顺序：Chat（最简单），Messages，Responses。

每完成一个编解码器，跑通 T2：Q1、Q2、Q5、S1、S2、S3、S6、S7、fuzz，以及该协议夹具的 IR golden。

#### 6.2.3 `convert`

文件：`exchange.go`、`stream.go`、`buffer.go`、`registry.go`、`dialect.go`、`tools_lower.go`、`tools_raise.go`、`tools_map.go`、`tools_responses.go`（`NeedsResponsesToolAdaptation`、`AdaptResponsesTools`、`RestoreResponsesTools`、`NewResponsesToolStream`）、`carriers.go`（`CallMemory`、`CallBinding`、`openCarriers`、`selectReplayTokens`、`sealCarriers`）、`native_replay.go`（`PrepareNativeReplay`）、`markers.go`、`output_meaning.go`（直接按结构化事件与编码器状态计算 `Semantic` 和 `Terminal`），达到 15 个源文件时加 `doc.go`。

工具降级的移植：从 3.11 列出的旧文件逐函数移植到 IR 变换与同协议工具适配，命名算法先原样复制。工具名差分对语料库里所有带 custom、namespace、tool_search 工具的请求，比较新旧两边发给上游的工具名集合和每个历史调用的名字，结果写进差分报告；不同的名字归入 D8，并检查 3.11 的命名要求。

`PrepareNativeReplay` 的测试：语料库里每个 Messages、Responses 请求（包括正文中含有字面量 `"trc1:"` 但令牌路径上无信封的请求，以及带无效 UTF-8 字节的请求）经过它之后逐字节不变；`reasoning_carrier_native` 场景覆盖同方案还原、异方案/异账号删除、空 assistant 内容占位、`passback` 保留明文 thinking、Responses 保留明文 `summary`、WS `response.create` / `session.update` 帧，以及 thinking 降级。

跑通 T3 和 T4：Q3、Q4、S4、S5、S8、M1，覆盖三个协议之间的六条路线。M1 包括跨方案的组合：两轮的上游方案不同、第二轮经 `PrepareNativeReplay` 走原生路径、客户端历史来自升级前（`legacy_history`）。

#### 6.2.4 与旧实现的差分和验收差异清单

在 `convert` 的测试里（`legacy_diff_scenario_test.go`），对每个场景和语料夹具乘路线：

- 请求：新旧输出各自用目标协议的解码器解成 IR，比较语义摘要；再对原始 JSON 做语义比较。
- 流：新旧输出的客户端流各自解码聚合，比较摘要；逐帧的 `Semantic`、`Terminal` 标记序列比较。
- 用量：新流解码器的 `ir.Usage` 换算成 `protocol.TokenUsage` 后，与旧读取循环使用的累加器在同一条上游流上的结果比较。
- 工具名：新旧两边发给上游的工具名和历史调用名逐个比较。

路线范围随阶段扩大：阶段 1 是三个文本协议之间的六条路线，阶段 3 加 Gemini，阶段 4 加 Antigravity，阶段 5 加同协议客户端工具轻量适配，阶段 6 加 Qoder。

每个差异归入以下之一：

1. 新实现的缺陷：修。
2. 验收差异清单里的类别：记入 `diff_ledger.json`，写明类别和夹具。
3. 其他差异：不允许自行接受。向旧行为对齐；确实认为旧行为是缺陷的，记入进度记录的"待确认差异"并询问用户。用户同意接受时，作为新类别加进下表，写明接受条件。

验收差异清单。表里是有意接受的全部差异，阶段 7 逐行核对接受条件，这是切换的门槛之一：

| 编号 | 有意接受的差异 | 接受条件 |
| --- | --- | --- |
| D1 | 旧实现重排了 part（例如把一条 assistant 消息里的推理、文本、调用重新分组，或把工具结果统一提前），新实现保持原顺序 | 新输出通过目标协议的严格请求校验器或流语法校验器 |
| D2 | 旧实现因中间协议串联或单跳子集结构体裁剪丢失或改写了目标协议合法的内容与控制项（含多模态文档与图片 URL/`detail`、结构化输出与停止词、工具错误 JSON 包络与工具图片归属文本、`developer` 角色、离散多块文本、`service_tier`/`prompt_cache_key`/`store`、历史与流式 `refusal`、跨协议流式命名空间与 `tool_search` 还原，以及无前置 `output_item.added` 的 `response.output_item.done` 单帧文本恢复），新实现直接保留或还原 | 新输出通过目标协议严格请求校验器或流语法校验器，且满足定律 Q4 或 S4（流式可见推理文本与客户端可见用量不变）。Responses、Chat 到 Gemini 原来经过 Messages 串联，这类差异同样逐条记账 |
| D3 | 旧实现的输出违反目标协议的语法校验器或严格请求校验器，或旧实现漏判输出帧的 `Semantic`、`Terminal` 标记（如 custom/search 工具、refusal、`incomplete`/`failed`/`error` 终态）及忽略客户端 `IncludeUsage` 开关，新实现符合规范 | 对应的校验规则或协议规范有出处 |
| D4 | 旧实现在请求方向用随机 id，新实现用内容派生的 id | 定律 Q5 通过 |
| D5 | 旧实现直接丢弃、合成为空壳块，或将异源专属块原始 JSON 序列化进模型正文（如请求侧未知历史块、流式异源托管 `web_search_call`，以及旧 `gemini_native.go` 将 Anthropic `server_tool_use` 与 `web_search_tool_result` `json.Marshal` 进 `model` 文本 part），新实现统一作为 `OpaqueItem` 丢弃并上报 | 记录 `opaque_item_dropped`；请求侧通过目标协议严格请求校验器且满足定律 Q4，除被丢弃的异源专属块外其余非 `OpaqueItem` 正文与语义内容不变且不向模型正文泄漏异源原始协议 JSON；流式侧客户端可见的文本、可见推理文本、工具调用、结束原因和用量不变，且通过流语法校验器 |
| D6 | 跨协议转换路线写给客户端的令牌字段改成信封（Messages 的 `signature` 和 `redacted_thinking.data`、Responses 的 `encrypted_content`，`openai` 和 `xai` 附带提供商 ID `p`）；Gemini 推理签名和没有令牌的明文推理也以信封写给 Messages、Responses 客户端，旧实现留空或不写 | 客户端可见的文本、工具调用和用量不变；T6 的 Claude Code、Codex、pi 能解析并在下一轮原样带回；`reasoning_carrier_roundtrip` 通过 |
| D7 | 发往上游的令牌按 3.10 节取舍：不再按 `gAAAA` 前缀判断，Messages 客户端历史里的裸签名不再写进 Responses 上游的 `encrypted_content`，跨账号的 `openai` / `xai` 信封在本地直接丢弃，`passback` Messages 上游在工具轮回传无签名 `thinking` 块 | 每个丢弃的令牌都有 `replay_token_dropped`；`legacy_history` 的请求通过严格请求校验器；阶段 7 的 live 金丝雀确认同方案同账号的信封令牌被真实上游接受 |
| D8 | 降级后发给上游的工具名与旧实现不同 | 满足 3.11 的命名要求；客户端看到的工具名与旧实现相同；差分报告逐个列出变化的名字 |
| D9 | Responses 客户端转 Chat 上游时，推理文本从 `plain` 信封取回，不再查 Redis | `reasoning_encrypted_only` 通过；方言规则 `deepseek-thinking` 无违规 |
| D10 | 原生 Messages、Responses 路径在请求含信封时还原或删除令牌（新增行为，旧实现没有这一步） | 不含 `trc1:` 的请求逐字节不变；`reasoning_carrier_native` 通过 |
| D11 | 跨协议推理控制与无令牌明文推理规则：（a）Messages 显式关闭思考（`thinking.type == "disabled"`、`budget_tokens == 0` 或 `effort == "none"`）转往 Responses、Chat 时经 `Dialect.Reasoning` 模型能力守卫裁决（仅目标模型明确支持 `"none"` 时发 `effort="none"` 且省略 `summary`，支持 `"minimal"` 时回退 `"minimal"`，纯推理模型取最低可用档位，普通非推理、仅开关控制或能力未知模型直接省略努力度），不再强制注入 `"medium"` 与 `"auto"`；（b）Chat 转往 `HistoryNone` 上游时丢弃无令牌历史 `reasoning_content` 并上报，不再拼入 `<thinking>` 可见正文标签 | 新输出通过严格请求校验器且满足定律 Q4；显式关闭思考在 `gpt-5.4`、`gpt-5`、`o3`、`gpt-5-codex`、`gpt-4o` 及 DeepSeek/Kimi Chat 上不发出目标模型不支持的 `effort="none"` 或 `summary`；丢弃无令牌明文推理时记录 `reasoning_text_dropped` 且不含 `<thinking>` 标签 |
| D12 | 非 D3 流式分片粒度、连续性信封帧、单活动槽位顺序闭合及 D2/D5 内容块增减引起的 `Semantic`、`Terminal` 标记序列变化：（a）`messages_to_chat` 实时写出 `input_json_delta` 而非缓冲至流末合并，以及 `messages_to_gemini` 在上游省略 `args` 的无参 `functionCall` 闭合前统一刷出规范 `input_json_delta("{}")`；（b）D6 写入 `trc1:plain` 或 `trc1:gemini` 的 `signature_delta` 帧，或写入 `trc1:anthropic` 密文推理条目帧；（c）`ir.Sequencer` 在打开下一 `output_index` 前先闭合当前槽位；（d）D2/D5 保留或丢弃内容块同步增减输出帧 | 新流通过流语法校验器与逐帧 `(Semantic, Terminal)` 线级不变量校验（仅末帧 `Terminal=true`、`Semantic` 与 `Terminal` 互斥、`Semantic` 存在性与 `ResponseHasSemanticContent` 一致）；工具块增量拼接（或回退初始 `input`）后的逐块规范 JSON 与旧实现一致；按已登记原因局部对齐后其余全部共享帧的 `(Semantic, Terminal)` 标记逐帧一致 |
| D13 | 跨协议且 `!Dialect.AllowBillingHeader` 时剥离仅对来源客户端有意义的内部遥测标记（以 `x-anthropic-billing-header:` 开头的系统文本，如旧 `NativeExtractClaudeSystemText` 在 `messages_to_gemini` 上漏过滤并直接拼入 `systemInstruction`），新实现按 3.7 与 3.8 节统一移除并上报 | 记录 `client_marker_dropped`；出站请求不含 `x-anthropic-billing-header:`；其余系统提示块与对话内容无损，通过目标协议严格请求校验器且满足定律 Q3 与 Q4 |
| D14 | Antigravity 统一单跳 `v1internal` 转换规则（`AG1` 至 `AG11` 全部采用选项 A，替代旧 `TransformClaudeToGeminiWithOptions` 与 `NativeConvertClaudeMessagesToGeminiGenerateContent` 双套分裂管线）：（`AG1`）`SchemaAntigravity` 下 `functionResponse.response` 纯文本包为 `{"result": string}`（空串保留 `{"result": ""}`）、错误包为 `{"error": string}`、非保留键结构化 JSON 对象直传、工具结果媒体拆为同轮相邻 `inlineData` / `fileData`；（`AG2`）`CallMemory` 未命中或 `UpstreamID` 为空时保留客户端 `functionCall.id` 与 `functionResponse.id`，同 `ReplayScheme` 命中不同 `UpstreamID` 时同步重写并记 `id_rewritten`；（`AG3`）按客户端显式 `tool_choice` 映射 `AUTO`/`NONE`/`ANY`（含 `allowedFunctionNames`），未传时省略 `functionCallingConfig`（不强制注入 `VALIDATED`），函数与 `googleSearch` 并存时设 `includeServerSideToolInvocations: true`；（`AG4`）省略 `systemInstruction.role`，每请求经 `InjectIdentityPatchToGeminiRequestWithPrompt` 幂等前置自定义或默认身份提示词，剥离 `x-anthropic-billing-header:` 并记 `client_marker_dropped`，保留离散系统块与移位指令原文（不截断 CLI 提示词，不注入免责声明、`mcpXMLProtocol` 与 `SYSTEM_PROMPT_END`）；（`AG5`）经 `WrapV1InternalRequest` 包装外层信封，不发 `request.sessionId`，外层 `requestType` 固定 `"agent"`；（`AG6`）解封同方案 `trc1:` 信封并丢弃裸令牌与异方案信封，`claude-*` 将无令牌思考降级为可见文本并清空当轮 `thinkingConfig`，`gemini-*` 省略无令牌思考且仅对每步首调用或未知无签名调用填 `skip_thought_signature_validator`；（`AG7`）托管 `web_search*` 与旧式搜索函数统一提升为 `googleSearch: {}`，精确 `type: "code_execution"` 转为 `codeExecution: {}`，带版本后缀的异源托管工具（如 `code_execution_20250522`）丢弃并记 `hosted_tool_dropped`；（`AG8`）两模型族统一经 `CleanAntigravitySchema` 无损清洗工具 Schema（保留小写类型、`[Constraint: ...]` 提示与空对象 `reason` 占位）并经 `convert` 降级客户端工具与清洗工具名；（`AG9`）仅在客户端显式传入且 `!OmitStopSequences` 时写出 `stopSequences`，未传上限时省略 `maxOutputTokens`，显式正上限直接按 `thinkingBudget + 1000` 余量抬升并受 `64000`/`65000` 与目录上限封顶，`Toggle` 模型显式关闭思考时写 `{"includeThoughts": false, "thinkingBudget": 0}`，保留结构化输出 Schema；（`AG10`）保留远程图片 URL（`fileData`）、base64 PDF（`inlineData`）、内联拒答、Cursor 顶层 `input` 单跳 Chat 与离散文本分块，丢弃跨协议 `OpaqueItem` 并合法化配对与交替回合；（`AG11`）统一入口 `ForwardConverted` 不对 OpenAI 兼容来源套用 Anthropic OAuth 专用的 `BuildToolNameRewriteFromBody`（`cc_sess_*`）工具名混淆，原工具名直发并由 `ToolMap` 还原 | 内层请求通过严格 Gemini 请求校验器（`antigravity-schema` 及 `gemini-*` 的 `gemini-signature`），外层通过 `v1internal` 信封结构校验（非空 `project`、`model`、`agent-` 前缀 `requestId`、`userAgent: "antigravity"`、`requestType: "agent"` 且无 `request.sessionId`）；编码与后处理满足定律 Q3、Q4（`VerifyEssence`）与 Q5（确定性）；六分组双基准评估中 `AG1` 至 `AG10` 全部差异均匹配精确 JSONPath 叶子路径与载荷语义谓词且 `unclassified_diff_items == 0`，`AG11` 经转换层与网关层测试验证原工具名直发无 `cc_sess_*` 混淆 |

清单之外，以下差异必须为零：用量；`Semantic`、`Terminal` 标记序列（D3、D12 除外）；客户端可见的文本、可见推理文本、工具名、工具参数和结束原因（D1 到 D3 除外）；发往上游的请求语义摘要与线级 JSON（D1 到 D5、D7 到 D11、D13、D14 除外）。

验收：

- T1 到 T4 全部通过，六条路线的定律无失败。
- 差分的未归类差异为零；`diff_ledger.json` 里每条都有类别，并满足该类别的接受条件。
- 用量差分为零。工具名差分报告入库。
- 每个解码器的 fuzz 在本地各跑 10 分钟无崩溃。
- 基准测试（2 MB 的请求体、5,000 帧的流）：新引擎的耗时不超过旧实现对应路径的 2 倍。超过时先分析再决定是否优化，结果写进进度记录。
- 新包没有被任何生产代码导入（阶段 2 才接入）。

### 6.3 阶段 2：接入 OpenAI 兼容平台和 Anthropic 平台

目标：Messages、Responses、Chat 三种上游协议的转换路径接入新引擎，按 3.13 合并流程。新路径放在接线开关后面，生产仍走旧路径。

步骤：

1. 接线开关（2.4 节）：在涉及转换的 7 组执行器与运行时的 `Options` 结构体（`OpenAITextExecutor`、`OpenAIResponseOutput`、`OpenAIWS`、`GrokExecutor`、`messageforward.Options`、`googleforward.Options`、`QoderRuntimeOptions` 及 `UnifiedTextExecutor`）上加 `UseIRConversion bool`，`internal/app` 的生产 `provide*` 保持默认 `false`，`newProtocolLabRouter` 按 `labOptions` 打开（不在 Wire 中注册裸 `bool`）。新增 `internal/app` 单元测试直接调用各生产 `provide*` / `*Options(cfg)` 断言该开关全部关闭（无需启动真实 DB/Redis，阶段 8 删除这个测试）。
2. `gateway/provider/model_attributes.go` 新增 `ConversionDialect`（3.7 节，接收 `modelrules.Resolution`）；`upstream/openai/encrypted_content.go` 的 `OpenAIEncryptedContentDigest` 在计算 SHA-256 前先调 `ir.ParseCarrier` 取出内部密文原文。
3. 逐个上游族接入，每个族一组提交，顺序如下。每个族的做法相同：新增转换入口和合并后的读取函数；在分派点按接线开关选择新旧；旧代码原样保留。
   - Responses 上游：`openaiforward/messages.go`、`chat.go`（保留对 `/v1/chat/completions` 上无 `messages` 但带 `input` 的 Cursor 请求分支处理）、`httpapi/openai_messages_execution_ports.go` 的 `Convert`、`openai_chat_execution_ports.go` 的 `ChatToResponses`；读取函数合并为 `upstream/openai/response_converted.go`（包含前导帧暂存窗口、`responseStreamLifecycleTracker`、`EmptyCompleted` 和 `ChatSilentRefusalDetector`）。`ApplyAnthropicCompatFullReplayGuard`、`TrimCompatResponsesInputToLatestTurn`、`AppendOpenAICompatClaudeCodeTodoGuard` 和 `session` 里的 Messages 提示缓存摘要当前接收旧子集结构体指针，本阶段同步提供基于字节/`jsontext` 或 `ir.Request` 的无损版本供新路径调用（避免经过旧子集结构体丢失 `Extra` 字段）。
   - Chat 上游：`openaiforward/raw_fallback.go`；读取函数合并进 `upstream/openai/response_raw_chat.go` 的新函数。新路径不使用 `ReasoningContentByID` 回调和 `ReasoningHistory`，推理文本来自 `plain` 信封（3.10 节），并在编码后补齐模型属性修正。
   - Messages 上游：`gateway/forward/conversion.go` 新增 `AsConverted`，`openaiforward/anthropic_flows.go` 新增合并后的入口（补齐 `PrepareModelAttributeParameters`，保持 `Mimic -> EnforceCacheControlLimit -> InjectAnthropicCacheControlTTL1h` 的编码后顺序，并增加跨 Claude 模型签名 400 的剥离重试）；读取函数合并成 `openaiforward` 里的一对（使用延迟 `OutputContext`，并在 `CommittedSSE(c)` 为真时发送无计时的本地终态 SSE 错误），Anthropic 平台的 `messageforward` 改为调用它。`messageforward` 同步补齐 `target.View().IsBedrock()` 的转换分支：复用 `bedrock` 的区域路由、`PrepareBedrockRequestBodyWithTokens`、SigV4/API Key 签名，以及二进制 AWS EventStream 到 Anthropic SSE 的解码桥，使实现与 `capability.SupportsProtocolConversion` 和 `anthropic_upstream.md` 一致。
   - Grok 的 Chat 转 Responses：`httpapi/grok_chat_execution.go`（`Dialect.OmitStoreAndInclude` 为真）。
4. 原生路径的信封处理：Messages 原生路径（Anthropic 平台的 `messageforward`、兼容平台的 Anthropic 端点）和全部 Responses 原生路径（OpenAI、Grok 等 HTTP Responses、`/v1/responses/input_tokens` 预检、专用 WS `gateway/ws/passthrough.go`、连接池 WS `gateway/ws/stream.go` 以及 WS HTTP 桥 `openai_ws_http_bridge.go` 的原生转发分支）在发往上游之前调用 `convert.PrepareNativeReplay`，同样放在接线开关后面。
5. 无接线开关的包级辅助函数（`gateway/provider/input_tokens.go`、`gateway/tokenestimate/estimate.go`、`gateway/provider/openai_responses_ingress_compat.go` 的 `convertLegacyResponsesMessages`）：本阶段先写出基于 `convert` 的新辅助函数并补齐与旧实现的差分和单元测试，生产调用点继续调旧实现（严格保证阶段 0 到 7 不改变生产行为），统一在阶段 8 切换调用点。注意 `PrepareAnthropicInputTokens` 发往官方 `https://api.openai.com/v1/responses/input_tokens` 时仍只投影 `{model, instructions, input, tools, tool_choice}` 五个计数字段并对 `input` 执行信封还原；`NormalizeOpenAIResponsesLegacyIngress` 只替换内部的 `convertLegacyResponsesMessages`，保留外层混合报文合并逻辑。
6. 每接入一个族：T5 在新路径下全绿，旧路径与基线一致；T6 相关格子全绿，且不比旧路径的基线差；用量一致性断言通过；混合分组的"转换后原生"（含 Bedrock、Vertex、OpenCode 和 `passback` 模型）场景通过。

验收：

- `make test-go`、`make test-clientlab` 通过；新路径的 T5、T6 结果不劣于旧路径的基线。
- 旧基线里记录的缺陷，在新路径下逐条说明是已修复还是仍存在。
- 生产装配下接线开关关闭的测试通过。

### 6.4 阶段 3：Gemini 编解码器和 Gemini 平台

1. 实现 `codec/generatecontent`（第 4.4 节）。schema 清洗、思考配置解析、文本增量计算、工具帧去重都从旧代码移植。跑通 T2。
2. 差分对象是 `bridge.NativeConvertClaudeMessagesToGeminiGenerateContent`（请求）和 `NativeGeminiMessagesStream`、`NativeGeminiCompatStream`（流）。Responses、Chat 到 Gemini 的旧路径经过 Messages 串联，属于 D2 的差异会比较多，逐条记账。Gemini 推理签名从留空变成信封属于 D6。
3. `CallMemory` 的网关实现：`gateway/session` 里包一层 `GeminiToolCalls`，按请求的 API Key、提供商和上游模型限定范围后作为 `Options.Calls` 传入（3.10 节）。
4. 网关：`googleforward/gemini_openai.go` 的两个 `ForwardAs*` 和 `gemini_attempts.go` 的 Messages 入口在接线开关后面合并成一个转换入口；`upstream/gemini` 的读取函数合并成一对。OpenCode 的 Gemini 路线（`unified_text_executor.go:304-310`）走同一入口。Gemini 原生客户端的 `ForwardNative` 不动。上游拒绝签名时用兼容格式重试的逻辑（`exchange_openai.go`、`exchange_messages.go`）留在 `upstream/gemini`。重试时用清空了 `Dialect.ReplayScheme`、不传 `Calls` 的选项重新创建 `Exchange`（并保留 `StripWireReasoningEffort`）：令牌全部丢弃，每个步骤的首个调用写占位签名，效果与现在的兼容格式重试相同，不需要新增选项。
5. `count_tokens` 到 Gemini（`httpapi/count_tokens_selected.go:109`）在接线开关后面用 `Exchange.UpstreamRequest`（`ReplayScheme` 和 `PlaceholderToken` 置空）得到 Gemini 请求体。`backend_modules.md` 加入 `protocol/codec/generatecontent`。
6. T5 加 `gemini` 提供商的全部场景（含 5 秒稀疏用量尾帧合并）；T6 的 Gemini 上游格子转绿。

验收同阶段 2 的形式，范围是 Gemini 上游。

### 6.5 阶段 4：Antigravity

Antigravity 的上游是包了一层信封的 Gemini 协议，同时承载 Gemini 和 Claude 模型。

1. `ConversionDialect` 为 Antigravity 返回 `SchemaDialect: antigravity` 和相应的 `Reasoning`（包含 `ForceBudget`），并按目标模型族拆分：`claude-*` 用 `ReplayScheme: "antigravity_claude"`、`PlaceholderToken: ""`；`gemini-*` 用 `ReplayScheme: "gemini"`、`PlaceholderToken: DummyThoughtSignature`。`googleforward.Antigravity` 同步挂载 `session.GeminiToolCalls` 作为 `CallMemory`。
2. 请求：`antigravity_messages.go` 和 `antigravity_openai.go` 的三个入口在接线开关后面合并。IR 经 `codec/generatecontent` 编码后，`upstream/antigravity` 继续做身份补丁、`CleanGeminiRequest`、`WrapV1InternalRequest`。调研发现旧代码里 Chat、Responses 来源按模型名分成两种转换实现，而 Messages 来源始终用一种。合并后只有一种编码器，差异由 `Dialect` 表达。差分要同时对照 `TransformClaudeToGeminiWithOptions` 和 `NativeConvertClaudeMessagesToGeminiGenerateContent` 两个旧实现，按"来源协议加模型族"分组记录差异（重点核对 `functionResponse` 的 `result` 对比 `content`、`functionCall.id`、`toolConfig: VALIDATED`、`mcpXMLProtocol` 和 `sessionId`）。这一组差异不预先归类，全部列出后询问用户以哪一种为准；用户的决定作为新类别加进 6.2.4 的清单。
3. 响应与恢复：`response_stream.go`、`response_compat_stream.go`、`response_compat_nonstream.go` 合并成一对读取函数。`recovery_gemini.go` 作用于上游 Gemini 字节，保持不变；`recovery_claude.go`（当前接收 `protocolanthropic.ClaudeRequest` 并调 `TransformClaudeToGeminiWithOptions`）在新路径上新增基于 `Exchange` / `ir.Request`（剥离思考或工具块并修正预算后重编）的恢复分支，同时为 `probe.go` 的 `BuildClaudeTestRequest` 准备基于 `convert` 的新实现（阶段 8 切换）。
4. T5 加 Antigravity 提供商夹具。Antigravity 走 OAuth，T6 不覆盖，靠 T3、T4、T5。

验收同阶段 2 的形式，范围是 Antigravity 上游。

### 6.6 阶段 5：同协议 Responses 客户端工具降级

目标：Responses 客户端到不支持全集客户端工具的 Responses 上游（OpenAI API Key、DeepSeek、Grok 及 Responses WebSocket HTTP 桥）时，custom、namespace、tool_search 工具的降级与还原改走 `convert` 的同协议轻量入口，退役 `bridge` 的线级客户端工具适配代码。

1. 按 3.8 节实现 `NeedsResponsesToolAdaptation`、`AdaptResponsesTools`、`RestoreResponsesTools` 和 `NewResponsesToolStream`。它们内部复用 `lowerTools` / `raiseTools` 的命名、包装和历史发现规则，但只改写请求中的 `tools`、`additional_tools` 及工具调用/输出条目，响应流只拦截并还原被降级的工具调用条目，保持其余请求字段、条目级 `prompt_cache_breakpoint`、`compaction` 条目、推理裸令牌以及 `partial_image` / `web_search_call.searching` / `mcp_*` 等原生托管工具中间流事件原样透传。顶层 `prompt_cache_options` 在非 OpenAI 平台上继续由 `openaiforward/transform.go` 剥离。
2. 在接线开关后面替换全部四个调用点：`openaiforward/passthrough.go:238`（OpenAI API Key HTTP 透传）、`openaiforward/prelude.go:299`（DeepSeek 等兼容 Responses）、`grokforward/responses.go:75` 的 `PatchGrokResponsesBodyWithClientTools`（同时兼容紧随其后的 `in.Compact == true` 压缩构造）、`gateway/ws/httpapi/openai_ws_http_bridge.go:636` 的 `AdaptResponsesClientToolsJSONWithMapping`。WebSocket 会话把每轮（包括本地直接返回的 `generate: false` 预热轮）得到的 `ToolMap`（含 `LoweredTools` 声明）存进连接状态，下一轮省略 `tools` 或 `model` 时通过 `inherited` 传入。
3. 响应侧：新路径使用 `RestoreResponsesTools` 和 `NewResponsesToolStream` 取代 `upstream.NewResponsesClientToolStreamBody` 与 `bridge.RestoreResponsesClientToolPayload`。注意 `gateway/httpapi/response_tool_restoration.go` 和 `gateway/requeststate/response_tools.go` 同时承载 `CodexToolNameReverse`（3.11 节保留不变）和原生命名空间扁平化开关，阶段 8 只清理其中客户端工具映射相关的函数与字段，不整文件删除。
4. T5 加"Responses 客户端到 OpenAI API Key、Grok、DeepSeek 的 Responses 上游（及 WS HTTP 桥预热续接），带 custom 和命名空间工具"的场景；T6 的 `lab_custom_tool` 在这些路线上通过；确认 `record` 模式录到的全部 Codex 请求在不带需降级工具时逐字节不变，带需降级工具时除工具条目外其余字段无损。

验收同阶段 2 的形式，范围是同协议客户端工具降级的路线。

### 6.7 阶段 6：Qoder

Qoder 是切换的前置条件之一，不能推迟到切换之后（2.5 节）。

1. 实现 `protocol/qoder` 和 `codec/qoderchat`（4.5 节），`backend_modules.md` 同步登记这两个新包。
2. 在接线开关后面，把 `upstream/qoder/execute.go` 的三个解析分支换成 `Exchange`（解析 `codec/qoderchat` 输出的 `protocol/qoder.PayloadRequest`，保留 `*qoder.RequestError` 本地 400 映射以抑制误切号），三个流写出器和三个缓冲构造器换成一对读取函数；同步为 `provider/provider/qoder_test_execution.go` 准备基于 `convert` 的连接测试构造分支（阶段 8 切换）。
3. 差分对象是三个 `ParseQoder*Payload` 和三个流写出器。Qoder 的旧流写出器里有自己的 Responses 事件构造（`WriteQoderResponsesStreamResponse`），差分时以语法校验器为准，旧输出违反校验的归入 D3。
4. 现有的 Qoder 测试夹具（`gateway/testkit` 的 Qoder 捕获客户端）接入 T5，覆盖无状态 Responses 的 400 拒绝与工具能力门禁。Qoder 的认证和会话较特殊，T6 不覆盖，靠 T3、T4、T5。

验收同阶段 2 的形式，范围是 Qoder 上游。

### 6.8 阶段 7：发布前总验收

前置条件：阶段 0 到 6 全部通过，进度记录里没有待确认差异。本阶段同样不改变生产行为。验收中发现的问题在新路径上修，修完重跑受影响的验收项。

步骤：

1. 重新执行 10.4 的导入方清点命令，确认每个 bridge 导入方在阶段 8 都有去处。
2. 按 `routes.txt` 逐行核对覆盖：每条路线都有 T3 的定律和 T5 的场景；有真实客户端的路线有 T6 的格子（Antigravity、Qoder 除外）。缺的先补。
3. 旧路径重跑全部基线（`conversion_matrix_scenario_test.go`、T5 旧路径、T6 旧路径各一次），新路径重跑 T1 到 T6，生成最终的差分报告。
4. live 金丝雀（`TOKENROUTER_CLIENTLAB_MODE=live`，只在一次性容器里运行）。需要真实凭据，由维护者执行，结果写进进度记录。组合如下，每个组合跑 `lab_reasoning_tool`，再用 `lab_switch_provider` 让第二轮落到原生路径一次：
   - Codex 到 Anthropic（`anthropic` 方案，信封里的 thinking 文本原样回到上游）；
   - Claude Code 到 OpenAI（`openai`）；
   - Claude Code 和 Codex 到 Gemini（`gemini` 推理令牌和 `CallMemory` 里的调用令牌）；
   - Claude Code 到 Grok，以及 Codex 经方言降级到 Grok（`xai`）；
   - Codex 到 DeepSeek 的 Chat 上游（`plain`，`reasoning_content` 被接受）。
5. 全部编解码器的基准测试。
6. 汇总升级说明的要点：2.5 节列出的后果，6.2.4 清单里客户端或运维能感知到的差异（D6、D7、D9、D10），首轮提示缓存可能未命中，Redis 里 `reasoning_content:` 前缀的旧键会自然过期，回滚的限制（2.4 节）。写进进度记录，阶段 8 照此写 `upgrade_notes.md`。

验收（切换的门槛）：

- `routes.txt` 的每一行都有覆盖。
- 新路径的 T1 到 T6（脚本模式）全绿；live 金丝雀的全部组合通过。
- 差分的未归类差异为零；每条差异都落在 6.2.4 的清单里并满足该行的接受条件；用量差分为零。
- 旧基线里记录的每个缺陷都标明在新引擎下已修复还是仍存在。
- 基准测试不超过旧实现对应路径的 2 倍，或者超出项已有分析并得到用户认可。

### 6.9 阶段 8：切换和退役

前置条件：阶段 7 通过；已和用户确认发布窗口（2.4 节）。本阶段的全部提交推送并通过验收之前，不打发布标签。

1. 切换：删除接线开关和"生产装配下关闭"的测试，所有分派点（含阶段 2、4、6 准备好的 `input_tokens.go`、`tokenestimate/estimate.go`、`openai_responses_ingress_compat.go`、`antigravity/probe.go`、`qoder_test_execution.go`）直接调用新入口；原生 Messages、Responses 路径（含 `/v1/responses/input_tokens` 和全部 WS 分支）始终调用 `PrepareNativeReplay`。
2. 删除旧的转换流程和读取函数（3.13 两张表的"现有"列），迁移 `openai_native_anthropic_ports.go:224` 的 `writeTextResponsesError` 后删除 `gateway/forward.Output` 接口和 `httpapi.ForwardConversionOutput`。删除 `upstream/client_tools_stream.go`（同步清理 `openai_forward_http_adapter.go:148`），并清理 `response_tool_restoration.go` 与 `requeststate/response_tools.go` 中的 `ResponsesClientToolMapping` 状态（保留其中的 `CodexToolNameReverse` 和原生命名空间开关逻辑）。删除旧文件时同步更新所属目录的 `doc.go`。
3. 删除推理连续性的旧机制：`gateway/session/reasoning_history.go`、`ReasoningContentCache` 接口、`rediscache` 里的 `SetReasoningContent`、`GetReasoningContent` 和 `reasoning_content:` 键前缀、`provideReasoningHistory` 及其装配（含 `openai_raw_response_output.go`、`openai_raw_fallback_ports.go`、`openai_chat_execution_ports.go` 里的回调绑定）和 `OpenAIResponseOutput.Reasoning` 字段。Redis 里的旧键自然过期，不写清理脚本。
4. 删除 `protocol/bridge`。删除前把仍被原生或保留路径使用的函数与常量移到对应的协议包：
   - 移到 `protocol/openai`：续接策略（字节级/`jsontext` 版的最新轮裁剪与 `TodoGuard`）、原生流与非流式重构使用的 `BufferedResponseAccumulator`、`ResponsesStreamOutputItems`、`NormalizeResponsesStreamingTerminalOutput`、`ReconstructResponseOutputFromSSE`（`response_nonstream.go`、`response_passthrough.go`），以及原生命名空间开关与 Compact 仍在用的 `StripOpenAIResponsesInputNamespaces`、`FlattenResponsesNamespacesExcept`、`RestoreResponsesNamespaceCalls`。
   - 移到 `protocol/anthropic`：完整重放尾部裁剪策略。
   - 移到 `protocol/gemini`：`NativeExtractGeminiUsage`、`NativeUsageProjection`、`NativeNormalizeGeminiRequestForAIStudio`（`upstream/gemini/request.go`）、`NativeIsGeminiInlineImageMIMEType`（`image_observation.go`）、`DummyThoughtSignature`（`native_gemini_attempts.go`、`antigravity_attempt.go`）、`CleanJSONSchema` 与 `DeepCleanUndefined`（`antigravity.CleanGeminiRequest`）；`upstream/gemini/response_completion.go:181` 和 `upstream/antigravity/execute.go:289,298` 原生路径对 `CompatJSONHasContent` / `CompatOutputMeaning` 的调用切到 `protocol/gemini`（`ObservePayload`）或 `convert`。
   - 迁移或清理 `bridge` 之外导入 `bridge` 的 22 个测试文件，用 `rg 'protocol/bridge"'` 确认全仓（含测试）没有剩余导入。
5. 删除只被旧转换器使用的线类型：在 `antigravity/recovery_claude.go` 和 `probe.go` 全部切完后删除 `protocol/anthropic/claude_types.go` 的 `Claude*` 一族；核对 `session/anthropic_prompt_cache.go` 等外部引用后，清理 `protocol/openai/types.go`、`protocol/anthropic/types.go` 里不再有调用方的类型。仍有调用方的保留。
6. 删除 `testkit/converter.go`（同步更新 `protocol/testkit/doc.go`）、`legacy_baseline.json`、`diff_ledger.json`、`conversion_matrix_scenario_test.go`、差分测试，以及 T5 的旧路径分支和 `labOptions` 里的接线开关选项。差分台账的最后一版留在 git 历史里。
7. 架构规则：从 `catalog.go` 的 `leafDependencies`、`moduleDependencies`（`internal/gateway`、`internal/upstream`）以及五个 `platformDependencies` 条目中删除 `internal/protocol/bridge` 的全部许可；确认文件许可的使用方测试没有过时条目。
8. 文档按第 7 节更新，`docs/operations/upgrade_notes.md` 按阶段 7 汇总的要点写升级说明。
9. 按 `AGENTS.md` 的要求搜索旧名字和描述旧行为的关键词（`bridge`、`ReasoningHistory`、`reasoning_content:`、`ReasoningContentByID`、接线开关的名字、`gAAAA`），处理残留的注释和文档。

验收：

- `make verify` 通过；T6 脚本模式全绿。
- `rg -n "bridge\." backend/internal --type go` 没有指向旧包的引用；第 9 步的关键词没有过时的残留。
- 发布由用户决定。发布说明的升级说明取自 `upgrade_notes.md` 的这一条。

## 7. 架构规则和文档改动清单

架构规则（`tools/architecture/catalog.go`、`policy.go`）：

| 阶段 | 改动 |
| --- | --- |
| 0 | `pureStandard` 加 json v2 两个路径；`sse`、`ir`、`testkit`、`testkit/mockupstream`、`testkit/cmd/corpus` 的 `leafDependencies`（每个 `Production` 均含自身包路径）；`policy.go` 把 `fixture` 分支移到整个 `pure` 分支之前；`internal/protocol/bridge` 的 `Tests` 加 `internal/protocol/testkit`（不动 `internal/app`） |
| 1 | `codec/{messages,responses,chat}` 和 `convert` 的 `leafDependencies`；`internal/protocol/ir`、`internal/protocol/convert`、各 codec 的 `Tests` 加 `internal/protocol/testkit`；`convert` 的 `Tests` 加 `internal/protocol/bridge` 和 `internal/protocol/{openai,anthropic,gemini}`（差分用，`bridge` 许可在阶段 8 删除） |
| 2 到 6 | 阶段 3、6 分别补 `codec/generatecontent`、`protocol/qoder`、`codec/qoderchat` 的 `leafDependencies`；`moduleDependencies` 里的 `internal/gateway`、`internal/upstream`，以及 `platformDependencies` 里的 `internal/upstream/{openai,gemini,antigravity,grok,qoder}`（规则合取，两张表都必须加；阶段 6 还需在 `internal/provider` 加 `internal/protocol/convert` 供 `qoder_test_execution.go` 使用）加 `internal/protocol/convert`、`internal/protocol/sse`、`internal/protocol/ir`（Qoder 另加 `internal/protocol/qoder`） |
| 8 | 从 `leafDependencies`、`moduleDependencies` 和 `platformDependencies` 删除 `internal/protocol/bridge` 相关的全部条目 |

Project Doc：

文档描述的是当前状态。阶段 8 之前生产走旧引擎，描述网关行为的文档到阶段 8 才改；只描述新包本身的文档随包落地。每次改文档或 `@project-doc` 锚点后运行 `make check-docs`。

| 文档 | 改动 | 阶段 |
| --- | --- | --- |
| 新增 `docs/architecture/protocol_conversion.md` | IR 的形状和不变量、扩展机制和三条防滥用规则、Loss、Dialect、转换管线、推理连续性（信封、`CallMemory`、原生路径的处理）、工具降级、新增协议的步骤。带稳定锚点，在 `convert/exchange.go` 的 `NewExchange` 上加 `@project-doc` | 1 起草，写明网关尚未接入；随各阶段补充；8 删掉"尚未接入"的说明 |
| `docs/architecture/index.md` | 列入上面的新文档 | 1 |
| `docs/architecture/backend_modules.md` | 随各阶段新建包同步加入包地图；阶段 8 删除 bridge，静态依赖图里的 `protocol/bridge` 换成 `protocol/convert`；`gateway/session` 一行去掉"推理历史" | 0、1、3、6、8 |
| `docs/architecture/gateway_request_lifecycle.md#protocol_conversion_boundary` | "转发与流式输出"一节改写为按上游协议合并后的结构（保留 `<a id="protocol_conversion_boundary"></a>` 锚点）；删除对 `protocol/bridge` 的引用 | 8 |
| `docs/interfaces/protocol_capabilities.md` | "客户端用量与缓存"里关于转换读取器的描述对齐新结构；说明同协议 Responses 客户端工具轻量适配 | 8 |
| `docs/interfaces/{anthropic,openai,gemini,antigravity,grok,qoder,opencode}_upstream.md` | 各自涉及转换的段落。`openai_upstream.md` 删掉 `ReasoningHistory` 一段（第 100 行）并更新第 89 行包名；`gemini_upstream.md` 改写"签名字段留空"和"内存中保留调用关联"两段（第 45、65-66 行），按 3.10 节描述信封和 `CallMemory`；`grok_upstream.md` 对齐 `xai` 信封和工具适配 | 8 |
| `docs/operations/development_workflow.md` | 测试分层加 T1 到 T6；验证策略的"上游平台""网关"条目加入定律、语料和客户端实验室的要求；缺陷回归流程（5.9）；依赖规则里 protocol 一段（开发期临时要求只留在本计划文件，不写进长期文档） | 0、8 |
| `docs/operations/upgrade_notes.md` | 一条升级说明：转换引擎替换、客户端令牌字段改为信封、旧会话可能丢失的推理连续性、首轮提示缓存可能未命中、`reasoning_content:` 旧键自然过期、回滚的限制 | 8 |

## 8. 风险和应对

| 风险 | 应对 |
| --- | --- |
| 旧实现的隐含行为很多，新实现漏掉 | 阶段 0 的基线和各阶段的差分把每个差异摆出来；接受的差异只限 6.2.4 清单里的类别，其余必须对齐或询问用户 |
| 一次切换覆盖全部路线，没有运行时回退 | 阶段 7 按 `routes.txt` 逐行验收，再加 live 金丝雀；切换放在和用户确认过的发布窗口里；需要回退时整体回滚版本（2.4 节） |
| 接线开关被误打开，半成品随常规发布上线 | 开关只是装配选项，不读配置和环境变量；"生产装配下关闭"的测试每次提交都跑 |
| 开发周期长，期间旧引擎还在接收线上修复（例如 `d91aacd8e` 到 `4b05d79a5` 这一组 Gemini 修复） | 修复照常进旧引擎，同时把复现夹具加进语料，新引擎必须通过；基线测试一直运行，旧引擎行为变化会让它失败，提醒更新基线和差分（2.4 节） |
| 旧会话在升级后丢失推理连续性 | 2.5 节列出的后果是有意接受的；`legacy_history` 场景保证这类请求仍能通过上游校验，升级说明写明 |
| 信封进入客户端历史后，客户端绕过网关直连官方 API 时签名被拒 | 接受。旧引擎写进历史的外来裸令牌有同样的问题；升级说明写明 |
| 计费受影响 | 计费用量的解析路径不改；客户端可见用量复用同一批归一化函数；用量差分必须为零；T5 核对客户端用量和结算用量 |
| 工具名变化影响会话 | 降级名只出现在上游请求里，每个请求按完整历史重新降级；同一版本内由 Q5 和 3.11 的命名要求保证稳定；跨版本只造成一次提示缓存未命中 |
| 请求输出不稳定影响缓存命中 | Q5 定律；请求方向禁止随机 id；`Deterministic(true)` |
| `encoding/json/v2` 比 v1 严格，多出 400 | 请求解码和 `PrepareNativeReplay` 显式放宽无效 UTF-8；重复键保持拒绝（与入口 `wirejson.ValidateRequestObject` 一致）；语料回放覆盖真实请求 |
| 性能回退 | 阶段 1 的基准门槛；`jsontext.Value` 避免对 schema 和媒体的重复解析 |
| 测试自洽但对协议理解有误 | 语料和 T6 提供事实来源；原生对照组用真实客户端校验编码器；live 模式校验真实上游 |
| T6 不稳定或太慢 | 脚本模式完全确定；客户端重试关到 0；锁定版本；`latest` job 不阻塞合并 |
| 客户端新版本改变请求形状 | 普查 golden 让新字段可见；`latest` job 每晚跑；每个版本重录语料 |
| 同协议流量改写工具时破坏其他字段或原生流事件（阶段 5） | 同协议走 `AdaptResponsesTools` 和 `NewResponsesToolStream`，只改写工具声明和工具调用条目，其余请求字段、缓存断点和原生托管工具中间流事件原样透传；录制的 Codex 请求全量验证 |
| 计划的映射表与真实协议有出入 | 映射表是起点；实施时以官方文档和录制语料为准，发现出入就改表并记入进度记录 |

## 9. 决策记录

### 9.1 中间表示的形状

考虑过的方案：

- 以 OpenAI Chat 为枢纽（LiteLLM 的做法）。Chat 表达不了推理条目、重放令牌、平铺的工具条目，只能靠 `provider_specific_fields` 这类无类型口袋。否决。
- 以 Responses 为枢纽（现在的代码事实上在这么做）。Responses 有 30 多种 item，表达不了 Messages 的块内顺序约束和 `cache_control`，还会把 OpenAI 的概念强加给所有编解码器。Bifrost 的 Responses 适配成本可以说明这一点。否决。
- 两套并行的表示（Bifrost）。两套之间的互转本身就是新的丢失来源。否决。
- 独立设计的"消息加有序 part"（pi-ai、Vercel AI SDK 的方向）。语义核心小，part 级别可以挂重放令牌和扩展。采用。

### 9.2 编解码器使用私有线结构体

不复用 `protocol/openai` 等包里的结构体：它们是子集，带为旧转换器定制的序列化，调用方很多。私有结构体让"线格式长什么样"成为每个编解码器的内部信息，可以自由加 embed 回退字段。代价是开发期间有两套线结构体，阶段 8 清掉只被 bridge 使用的那部分。

### 9.3 规范事件流是顺序的

让解码器负责把交错的 part 顺序化，编码器全部按"一次一个 part"实现。备选是允许交错、由需要顺序的编码器各自缓冲，那样每个编码器都要处理一遍交错。顺序化的代价是交错的并行工具参数会延迟到前一个调用结束后才流出，文本不受影响。

### 9.4 转换路径先按上游协议合并，不直接并入原生管线

更彻底的做法是让每个平台执行器只懂原生协议，转换作为外层中间件改写请求、包装输出。这能消掉"转换流程重新实现原生管线步骤"的重复。没有在本计划里做的原因：原生执行器的错误输出、响应头提交、客户端识别目前都与客户端协议耦合，改造面是整个网关执行层，风险和工作量都超出"重构协议转换"。按上游协议合并是它的前置步骤，完成后每个上游族只剩两条路径，届时再评估。

### 9.5 后续方向（本计划不实施）

- 原生管线与转换管线合并（9.4）。
- Gemini 客户端到非 Gemini 上游的路线：编解码器的两半都已具备，需要路线表条目、Gemini 入口的转换入口和 T5、T6 覆盖。
- 引用和 grounding 的结构化互转：IR 已有 `Annotations`，目前编码器只保留旧行为。
- 把 `Loss` 计数接入 Ops 指标，按路线观察客户端实际在用哪些会被丢弃的特性。

### 9.6 一次发布切换，首版采用最终的推理连续性方案（2026-11-14 修订）

修订前的做法：阶段 2 加进程配置 `gateway.conversion_engine`，按上游族逐个把默认值切到新引擎，观察一个发布周期后再删除旧引擎；推理连续性在阶段 1 到 6 与旧路线逐条等价，阶段 7 再换成信封，并保留嗅探来兼容加固之前的历史；Qoder 可以推迟到删除旧引擎之后。

修订后：开发和测试照旧分阶段，全部路线通过阶段 7 的总验收后一次切换，旧引擎随同一版本删除；首版就用 3.10 节的信封方案；跨版本兼容按 2.5 节逐项取舍。

理由：两套引擎在生产中并存时，同一类缺陷要修两处，配置和文档要描述两种行为，迁移期的等价规则写了还要再删。用户接受旧会话在升级后丢失一部分由旧引擎生成的转换状态，嗅探和服务端的推理文本副本也就不再需要。代价是没有运行时回退，只能整体回滚版本，所以发布前的门槛提高到按路线清单逐行验收，再加 live 金丝雀。

### 9.7 实施前代码对齐与边界收口（2026-11-21 修订）

对照 `164bc487b` 之前合入主干的协议与门禁变更，在实施前敲定五项决策：

1. 阶段 5 同协议客户端工具降级采用轻量线级适配：同协议 Responses（含 OpenAI API Key、DeepSeek、Grok 及 WS HTTP 桥）使用 `convert` 内的 `AdaptResponsesTools` 和 `NewResponsesToolStream`，内部共享 `lowerTools` / `raiseTools` 规则，但只改写工具声明和调用条目，避免整包 IR 编解码冲掉条目级 `prompt_cache_breakpoint`、`compaction` 条目和 `partial_image` / `web_search_call.searching` 等托管工具中间流事件。
2. Bedrock 跨协议转换在阶段 2 补齐：在 `messageforward` 的合并转换路径上接入 Bedrock 区域路由、请求体转换、SigV4/API Key 签名及二进制 AWS EventStream 转 SSE 解码，与 `capability.SupportsProtocolConversion` 和文档声明对齐。
3. 账号绑定密文在信封内携带签发提供商 ID：`trc1:` 信封增加可选 `p`（`ProviderID`），`openai` 和 `xai` 方案同号命中时解封回传，跨号切换时本地直接丢弃并上报 `replay_token_dropped`，省去一次必败的 `400 invalid_encrypted_content` 往返；`OpenAIEncryptedContentDigest` 哈希前先解封，对齐 HTTP 与 WS 的失效密文表。
4. Antigravity 双实现细节留到阶段 4 差分表裁决：先按模型族拆开 `antigravity_claude` 与 `gemini` 的 `Dialect`、修好 `recovery_claude.go` 和 `probe.go` 对 `ClaudeRequest` 的依赖，`result` 对比 `content`、`toolConfig` 等字段分歧等阶段 4 跑出完整差分后再决定基准。
5. 无开关包级辅助函数留到阶段 8 切换：`input_tokens.go`、`tokenestimate`、`openai_responses_ingress_compat.go` 等包级函数在阶段 2 先写新实现做对照测试，阶段 8 统一切换，严守阶段 0 到 7 不改生产行为的约束。

## 10. 附录

### 10.1 旧代码到新结构的对照

| 旧位置 | 新位置 |
| --- | --- |
| `bridge/anthropic_to_responses.go`、`responses_to_anthropic_request.go`、`chatcompletions_to_responses.go`、`chatcompletions_anthropic_bridge.go` 的请求部分、`chatcompletions_responses_bridge.go` 的请求部分 | `codec/{messages,responses,chat}` 的 `request_decode.go`、`request_encode.go`、`legalize.go` |
| `bridge/*` 的六个流状态机和非流式转换函数 | 各编解码器的 `stream_decode.go`、`stream_encode.go`、`response.go` |
| `bridge/responses_client_tools.go`、`responses_namespace.go`、`responses_tool_identity.go`、`responses_tool_history.go`、`responses_tool_search_discoveries.go`、`responses_input_namespace.go`、`client_tools_json.go`、`responses_tool_adaptation.go` | `convert/tools_lower.go`、`tools_raise.go`、`tools_map.go`、`tools_responses.go`（原生命名空间开关与 Compact 仍用的三个函数迁至 `protocol/openai`） |
| `bridge/gemini_*.go` | `codec/generatecontent`（原生路径共用的 `DummyThoughtSignature`、AI Studio 归一化、schema 清洗与用量提取迁至 `protocol/gemini`） |
| `bridge/response_format.go` | Chat 和 Responses 编解码器内部 |
| `bridge/compat_output_observation.go` | `convert/output_meaning.go`（结构化标记计算）及 `protocol/gemini` |
| `bridge/compat_continuation.go`、`compat_replay_guard.go` | `protocol/openai`、`protocol/anthropic`（改为字节级/`jsontext` 或 IR 实现，避免旧子集结构体丢未知字段） |
| `bridge/runtime.go`、`request_options.go` | `convert.Runtime`、`convert.Dialect` |
| `gateway/forward/conversion_sse.go` 等各处 SSE 扫描和写出 | `protocol/sse`（转换路径使用；原生路径的扫描器不强制替换） |
| `gateway/provider.ModelConversionOptions`、`GeminiThinking` | `gateway/provider.ConversionDialect` |
| `bridge` 里散落的令牌规则（`gAAAA` 判断、Gemini 签名留空、Responses 令牌写进 Messages `signature`） | 不移植，按 3.10 节在 `convert/carriers.go` 重写 |
| `gateway/session.ReasoningHistory` 及 `rediscache` 里的 `reasoning_content:` 存取 | 阶段 8 删除，由 `plain` 信封取代 |
| `gateway/session.GeminiToolCalls` | 保留，作为 `convert.CallMemory` 的实现（`Gemini` 与 `Antigravity` 共用） |
| `upstream/qoder/payload.go` 的三个解析器、`stream_conversion.go` 的写出器 | `protocol/qoder`、`codec/qoderchat` 加 `upstream/qoder` 里的一对读取函数 |

### 10.2 实施时容易踩的仓库规则

- 测试文件 `X_test.go` 和 `X_external_test.go` 都必须有同目录的 `X.go`；跨多个源文件的测试用 `<场景>_scenario_test.go`（外部包测试直接在文件内写 `package <pkg>_test`，不要起名 `*_scenario_external_test.go`），文件开头注释列出覆盖的源文件；每个包只有一个 `helpers_test.go`。
- 源文件名不能是 `helper.go`、`util.go`、`common.go` 这类看不出主题的名字，也不能重复包名前缀（注意多词目录同样检查拼接前缀：`generatecontent/` 禁 `generate_content_*.go`，`qoderchat/` 禁 `qoder_chat_*.go`，`mockupstream/` 禁 `mock_upstream_*.go`）；去除空行、注释和 import 后不足 30 行且无独立主题的文件须并入相邻主题文件。
- 手写源文件达到 15 个的包要有 `doc.go`（`doc.go` 正文中出现的任何 `[a-z0-9_]+\.go` 都必须存在于同目录；删除文件时同步更新 `doc.go`）。
- 文件内声明顺序是 import、const、var、type、func，`make fmt` 会重排，重排后检查文件总览注释的位置。`internal/` 下的非测试文件（含 `testkit/cmd/corpus/main.go`）禁止导入标准库 `log`。
- 纯角色的包不能导入 `os`、`embed`、`net/http`。需要读文件的测试通过 `testkit` 间接完成。
- 新包默认只能导入 `internal/protocol` 根包，其余依赖要在 `leafDependencies` 里登记（每个条目的 `Production` 必须包含自身包路径）；`upstream` 各具体平台的依赖需要同时在 `moduleDependencies["internal/upstream"]` 和 `platformDependencies` 里登记。
- `testdata/` 下不放 `.go` 文件；含 CRLF 或行尾空格的 `.sse` 夹具在 `.gitattributes` 标 `-text -whitespace`；未脱敏录制目录 `testdata/corpus/_incoming/` 加入 `.gitignore`；`tools/clientlab/` 避开 `.gitignore` 未锚定的 `CLAUDE.md`、`.claude`、`tests`、`scripts`、`output`、`tmp` 名字。
- 注释和 docstring 用英文；godot 要求顶层声明的文档注释以句号结尾。

### 10.3 常用命令

```bash
# 单个包
(cd backend && go test ./internal/protocol/...)

# 更新 golden
(cd backend && TOKENROUTER_UPDATE_GOLDEN=1 go test ./internal/protocol/...)

# fuzz 一个解码器 10 分钟
(cd backend && go test ./internal/protocol/codec/chat -run '^$' -fuzz FuzzStreamDecode -fuzztime 10m)

# 网关路由测试
(cd backend && go test ./internal/app -run 'TestProtocolLab' -count=1)

# 客户端实验室，脚本模式
make test-clientlab

# 客户端实验室，录制语料
TOKENROUTER_CLIENTLAB=1 TOKENROUTER_CLIENTLAB_MODE=record go -C backend test ./internal/app -run '^TestClientLab' -count=1 -v
(cd backend && go run ./internal/protocol/testkit/cmd/corpus import ./internal/protocol/testkit/testdata/corpus/_incoming)

# 架构、lint 与文档检查
make lint-go
make check-docs
```

### 10.4 bridge 导入方清点

阶段 8 删除 `protocol/bridge` 之前，下面每一组（68 个非测试文件、22 个外部测试文件及 4 个间接调用方）都要有去处。清单取自 `rg -l 'internal/protocol/bridge"' backend`，阶段 7 重新执行一次核对。"去处"一列写的是新路径在哪个阶段接入（都在接线开关后面，无开关的包级函数留到阶段 8 切换）；旧路径对 bridge 的导入在阶段 8 统一删除。

| 导入方 | 用到的内容 | 去处 |
| --- | --- | --- |
| `gateway/httpapi`（15 个文件，其中 `count_tokens_selected.go` 属阶段 3）、`gateway/provider/openaiforward`（6 个文件，含 `passthrough.go`）、`gateway/forward`（3 个文件）、`gateway/provider/messageforward`（1 个文件）、`gateway/provider/grokforward`（1 个文件） | 请求转换、流状态机、`ResponsesClientToolMapping`、原生命名空间处理、`Runtime` | 阶段 2、3、5 随流程合并改用 `convert`；`prelude.go` 和 `response_tool_restoration.go` 原生命名空间与 Compact 仍用的函数在阶段 8 迁至 `protocol/openai`，`CodexToolNameReverse` 原地保留 |
| `upstream/openai`（7 个文件） | 流状态机、`BufferedResponseAccumulator`、`ReconstructResponseOutputFromSSE`、终态输出修复 | 阶段 2 转换路径改用 `convert.Stream`；原生路径仍用的 `BufferedResponseAccumulator`、`ResponsesStreamOutputItems`、`NormalizeResponsesStreamingTerminalOutput`、`ReconstructResponseOutputFromSSE` 在阶段 8 移到 `protocol/openai` |
| `upstream/gemini`（7 个文件）、`gateway/provider/googleforward`（5 个文件，含 2 个 Antigravity 文件）、`gateway/httpapi/textattempt`（1 个原生文件） | Gemini 请求和流转换、用量提取、思考配置、AI Studio 归一化、图片 MIME、`DummyThoughtSignature`、`CompatJSONHasContent` | 阶段 3、4 接入转换路径；原生共用的 `NativeExtractGeminiUsage`、`NativeUsageProjection`、`NativeNormalizeGeminiRequestForAIStudio`、`NativeIsGeminiInlineImageMIMEType`、`DummyThoughtSignature` 在阶段 8 移到 `protocol/gemini` |
| `upstream/antigravity`（9 个文件，加间接调用方 `probe.go`） | `BuildContents`、schema 清洗、流处理器、`GeminiThinkingConfig`（`recovery_claude.go`）、原生流内容检测（`execute.go`） | 阶段 4 合并转换路径并重做 `recovery_claude.go` 与 `probe.go`；`CleanGeminiRequest` 共用的 `CleanJSONSchema`、`DeepCleanUndefined` 阶段 8 移到 `protocol/gemini` |
| `upstream/grok`（2 个文件）、`upstream/client_tools_stream.go`（加间接调用方 `openai_forward_http_adapter.go`）、`gateway/requeststate/response_tools.go`、`gateway/ws/httpapi` | 客户端工具的降级和还原 | 阶段 5 改用 `AdaptResponsesTools` 与 `NewResponsesToolStream`；阶段 8 保留 `response_tools.go` 中的 `CodexToolNameReverse` |
| `upstream/qoder`（3 个文件，加间接调用方 `provider/provider/qoder_test_execution.go`） | 工具声明转换、`CompatOutputMeaning`、三个请求解析器 | 阶段 6 接入 `protocol/qoder` 与 `codec/qoderchat`，阶段 8 切换 `qoder_test_execution.go` |
| `gateway/provider/model_attributes.go`（加间接调用方 `googleforward/model_attributes.go`） | `RequestOptions`、`GeminiThinkingOptions` | 阶段 2、3，换成 `ConversionDialect`（接收 `modelrules.Resolution`） |
| `gateway/provider/input_tokens.go`、`gateway/tokenestimate/estimate.go` | 为估算 token 和官方 `/v1/responses/input_tokens` 预检转换请求 | 阶段 2 写基于 `convert` 的对照函数并测试（发往官方预检时只投影 5 个计数字段并还原信封），阶段 8 切换生产调用点 |
| `gateway/provider/openai_responses_ingress_compat.go` | 把混合 Responses 请求体里旧式的 `messages` 字段转成 `input` | 阶段 2 写基于 `convert` 的 `convertLegacyResponsesMessages` 对照实现做差分，保留外层混合字段合并，阶段 8 切换 |
| `gateway/session/reasoning_history.go` | `ExtractResponsesReasoningItem` | 阶段 8 随 `ReasoningHistory` 删除，不迁移 |
| 外部测试文件（22 个 `*_test.go` 文件） | 旧转换器、`ResponsesClientToolMapping`、`DummyThoughtSignature`、`ReconstructResponseOutputFromSSE` 等 | 阶段 8 随被测旧实现删除，或更新到 `convert` 与迁入 `protocol/{openai,gemini}` 后的新位置 |

## 进度记录

（实施时在此追加：日期、阶段、完成的步骤、局部决策、待确认差异。）

- 2026-11-14，计划修订，尚未开始实施。用户的要求：

  > 请把协议转换重构计划改成一次发布完成。开发和测试可以分阶段，所有现有转换路线（包括 Qoder）验收后再统一切换。旧引擎随同一版本退役，取消双引擎并行和回退开关。
  >
  > 首版直接采用最终的推理签名方案。可以接受旧会话在升级后丢失旧引擎生成的部分转换状态。请按这个原则顺手检查其他类似的兼容要求，比如旧缓存和工具名必须与旧版完全一致，评估哪些可以省掉。正常的客户端协议支持和新引擎下的多轮续聊仍要保证。
  >
  > 保留发布前的基线和差分测试，把有意接受的差异写进验收条件。这次只改计划，先别开始实现。

  改动：新增 2.4（发布方式）和 2.5（版本兼容边界）；3.10 改为首版即信封方案，`Sidecar` 收窄为 `CallMemory`，新增 `PrepareNativeReplay`；3.11 放宽工具名要求；3.13 的迁移开关换成接线开关；6.2.4 改为验收差异清单（D1 到 D10）；阶段 7 改为发布前总验收，阶段 8 改为切换和退役；Qoder 不再可推迟；第 7、8、9、10 节相应调整。

- 2026-11-21，实施前全面审查与修订（基准提交 `164bc487b`），尚未开始写代码。改动要点：
  - 对齐 11 月 14 日后合入主干的代码事实：移除 `AllowDuplicateNames(true)` 并接入 `wirejson` 用量校验；`ir.Usage` 和 `ir.Event` 补齐 30m/12h 缓存写入桶、图片输入与音频/预测明细、`ServiceTier`（含 `ultrafast`）与 `Speed`；`ConversionDialect` 改为接收 `modelrules.Resolution` 并补齐 `StripEffort`、采样限制和按模型族推导的 `ReplayScheme` / `ReasoningHistory`（覆盖 OpenCode、`passback` 模型以及 Antigravity `claude-*` 与 `gemini-*` 拆分）；补齐 Qoder 无状态 Responses 400 门禁、`protocol/qoder` 包及会话/工具选择字段；补齐提前提交 200 SSE 后的终态错误输出、Responses 前导帧暂存窗口、Gemini 5 秒尾帧合并与 `Close(nil)` 终态顺序、Cursor `/v1/chat/completions` 的 `input` 分支。
  - 落实 5 项实施前决策（9.7 节）：阶段 5 同协议客户端工具降级采用 `AdaptResponsesTools` / `NewResponsesToolStream` 轻量适配（覆盖 OpenAI API Key、DeepSeek、Grok、WS 预热续接，保留 `CodexToolNameReverse`）；阶段 2 合并 Messages 转换入口时补齐 Bedrock 签名与 AWS EventStream 解码；`trc1:` 信封增加可选提供商 ID `p` 并对齐 `OpenAIEncryptedContentDigest` 哈希与 `PrepareNativeReplay` 路径扫描；修正 Antigravity `recovery_claude.go` 与 `probe.go` 依赖，具体字段分歧留到阶段 4 差分裁决；无开关包级辅助函数留到阶段 8 切换。
  - 修正架构检查与工具链门禁：`policy.go` 角色判定顺序、`leafDependencies` 自引用、`platformDependencies` 双表登记、`testkit/doc.go`、测试与源文件名门禁、`.gitignore` `_incoming/`、`.gitattributes` `.sse` 豁免、`TOKENROUTER_UPDATE_GOLDEN=1`、`newProtocolLabRouter` 空指针修复，以及阶段 8 迁出 `bridge` 的全部 9 类原生路径符号和 22 个测试文件。

- 2026-11-21，阶段 0 第 1 步（批次 0.1，提交 `f8b7df313`）：完成 `tools/architecture/` 规则调整、正反例夹具以及 `.gitignore` 和 `.gitattributes` 配置。
  - 局部决策：`tools/architecture/policy.go` 调整 `locate` 中 `fixture` 与 `pure` 的分支顺序时，将原有的单行中文注释改写为英文函数文档注释，说明未登记路径的默认封闭规则以及 `fixture` 优先匹配的原因；`tools/architecture/policy_test.go` 在本批新增包的依赖断言之外，补充了 `*_integration_test.go`、未登记 `internal/protocol/*` 子包和 `internal/protocol/testkit/*` 子目录继承边界的正反例；Project Doc 经评估无需在本步改动，阶段 0 文档改动留待第 11 步随新包与测试层一并落地。
  - 待确认项：无。

- 2026-11-21，阶段 0 第 2 步（批次 0.2，提交 `c27a8c8fd`）：完成 `backend/internal/protocol/sse` 包（`Frame`、`Reader`、`Append`、单元测试与任意切分 fuzz）。
  - 局部决策：`Frame`、`Append` 与 `Reader` 合入单文件 `sse.go`（配 `sse_test.go`），将 SSE 帧的线格式拆分与写出规则集中在一处维护；`Reader` 提供 `NewReader`、`NewReaderLimit`（非正上限回退到 `DefaultMaxFrameBytes = 500 MiB`，超限返回 `ErrFrameTooLarge`）以及 `Scan`、`Frame`、`Err` 方法，正常 `io.EOF` 时 `Err()` 返回 `nil`，每次派发克隆 `Frame.Data` 供调用方跨轮安全持有；按 WHATWG SSE 规范在首行剥离一次可选 UTF-8 BOM 并剥离 `data:`、`event:` 冒号后至多一个前导空格（兼容无空格，仅在累计 `Data` 非空时派发帧并在空行边界重置 `Event` 与单帧字节计数），`Append` 剥离 `Event` 中的 CR/LF 并将 `Data` 内的 `CRLF`、`LF`、裸 `CR` 统一写成多行 `LF` 的 `data:` 行；断流收尾与单帧上限对齐 `conversion_sse.go` 和 `22235a63c`，非 `EOF` 错误打断无末尾空行的最后一帧时仅在累计 `Data` 通过 `json.Valid` 时交付并在 `Err()` 保留原读取错误；Project Doc 经评估在本步无需改动，留待阶段 0 第 11 步统一更新。
  - 待确认项：无。

- 2026-11-21，阶段 0 第 3 步（批次 0.3，提交 `118ca3c5a`、`e79080542`）：完成 `backend/internal/protocol/ir` 先行类型定义、`backend/internal/protocol/testkit` 第一版和 `backend/internal/protocol/testkit/cmd/corpus` 命令行工具。
  - 局部决策：`protocol/ir` 先行类型拆为 `request.go`、`response.go`（暂合并 `Response`、`Finish` 与 `Usage` 以满足单文件有效代码行数要求，阶段 1 补齐换算函数时再拆出 `usage.go`）、`event.go`、`loss.go` 和 `essence.go`，仅保留 `json/v2` `omitzero` 所需的 `Extra.IsZero()`；`testkit`（含 `doc.go` 共 23 个手写源文件）放置 `testdata/README.md` 支撑早期 `//go:embed testdata` 编译，兼容完整 `ProtocolID` 与短目录别名并提供 `ReadGoProductionSources` 供后续纯包测试扫描源码；`jsondiff.go` 用 `jsontext` 将数字归一化为符号、去零有效数字和十进制指数并记录对象成员线扫描顺序；`golden.go` 仅由 `TOKENROUTER_UPDATE_GOLDEN=1` 控制写回；四套流语法校验器（含 `AllowAbnormal` 异常终止模式）、严格请求校验器（含规则出处目录、Messages 顶层 `cache_control` 许可，以及 `passback`、`function-only`、`deepseek-thinking`、`gemini-signature`、`antigravity-schema` 方言规则）和客户端多轮模拟器全部直接处理线 JSON 与 `sse.Frame`，违规结果按报文扫描顺序确定输出；`testkit/essence.go` 采用固定源下标墓碑记录加存活序号配对物化的两阶段实现覆盖全部 22 种 `LossKind`，避免多条删除或跨管线阶段时发生下标漂移；`corpus_import.go` 与 `cmd/corpus` 共用 `isCredentialKey` 凭据键识别，按原 MIME 替换图片（1x1 PNG）与 PDF（极小合法 `%PDF-1.4`）等 base64 媒体，并在内存中完成 512 KiB 单文件、15 MiB 总预算与密钥校验后再落盘；Project Doc 经评估在本步无需改动，留待阶段 0 第 11 步统一更新。
  - 待确认项：无。

- 2026-11-21，阶段 0 第 4 步（批次 0.4，提交 `59bfac48b`、`1532f0efe`）：完成 `backend/internal/protocol/testkit/mockupstream` 第一版。
  - 局部决策：源文件拆分为 `inspect.go`、`handler.go` 和 `proxy.go`（各配同名 `_test.go`），避开 `mock_upstream_*.go` 重复包名前缀，仅依赖 `internal/protocol/testkit` 与标准库以贴合现有 `leafDependencies` 边界；`inspect.go` 按路径识别 Messages、Responses（含 `/codex/responses` 与无 `/v1` 变体）、Chat Completions 及标准与 Vertex 风格的 Gemini `generateContent` 和 `streamGenerateContent`（含 `?alt=sse`），排除 `/count_tokens`、`/input_tokens`、`:countTokens` 探针路径使其返回 404 且不记入违规，并用 `encoding/json/v2` 与 `jsontext`（开启 `AllowInvalidUTF8(true)`、拒绝重名键）提取模型、流开关、工具声明和历史计数，把 Responses `input[]` 中连续的 `reasoning`、assistant `message` 与各类调用归并计为单次 assistant 回合；`handler.go` 先执行 `testkit.ValidateRequestRules` 并按上游协议返回原生格式 HTTP 400 错误包，按 `Match.MatchesObserved` 匹配回合、未命中走 `Script.Default`、同 `Match` 相邻条目按命中次数推进以覆盖同节点故障重试，阶段 0 仅消费 `Reply.Raw`（区分内联 JSON/SSE 与 `Corpus` 相对路径，遇到 `Events`/`Response` 返回 501 并记错），`Fault.TruncateAfterFrames` 按与 `sse.Reader` 一致的非空 `data` 边界截取原始字节前缀并在支持 `http.Hijacker` 时刷新后关闭底层连接注入 `io.ErrUnexpectedEOF`，且所有请求与违规记录均在写出响应或断连前写入互斥锁保护的切片；`proxy.go` 一并实现 `ModeReplay`、`ModeProxyRecord`（剥离逐跳头与入站 `Accept-Encoding` 以保留出站 `http.Transport` 对 gzip 流的透明解压，并在拼接 `TargetURL` 时去重末尾 `/v1` 与 `/v1beta` 版本段）及 `_incoming/*.capture.json` 落盘；Project Doc 经评估在本步无需改动，留待阶段 0 第 11 步统一更新。
  - 待确认项：无。

- 2026-11-21，阶段 0 第 5 步场景批 1（批次 0.5a，提交 `2f3fca8e6`）：完成 5.5 节前 8 个场景在 `backend/internal/protocol/testkit/testdata/wire/` 下的四协议线夹具与自动遍历校验测试。
  - 局部决策：`testdata/wire/` 下统一采用规范协议 ID 目录名（`anthropic_messages`、`openai_responses`、`openai_chat_completions`、`gemini_generate_content`）及 `<kind>/<scenario>.{json,sse}` 路径，与 `DefaultCorpus().QueryFixtures` 的 `Scenario` 匹配契约对齐，供第 6 步旧 `bridge` 矩阵直接按场景名消费；除 `tool_parallel_interleaved` 的上游响应外，其余 7 个场景在四协议下均同时提供 `request`、`stream`、`response` 三类夹具，并保持同协议成对 `stream` 与 `response` 的输出正文、调用 ID 与参数、结束原因及用量桶一致；`tool_parallel_interleaved` 在四协议下提供客户端 `request`，上游 `stream`（`index` 0 与 1 交替发出参数片段，复现 `7b15de584`）和对照 `response` 仅在 `openai_chat_completions` 下提供；`system_roles` 在原生支持对话中段系统或开发者指令的 Chat 与 Responses 中放入前置及中段 `developer` 消息，在要求严格交替的 Messages 与 Gemini 中采用多块顶层 `system` / `systemInstruction` 加多轮交替历史；`text_unicode_split` 在单帧合法 UTF-8 JSON 字符串内跨越 CJK、ZWJ 家庭 emoji、国旗区域指示符、肤色修饰符与组合重音符切分增量并保留原始多字节 UTF-8；`tool_args_fragmented` 将参数切成 12 片并直接切在 `\"`、`\\`、`\n`、`\u65e5`、`\u672c` 转义序列中间；`tool_args_empty` 在单轮内并列覆盖空串或无增量或省略 `args`（`get_clock`）与 `{}`（`get_uptime`）两种无参调用；`tool_single` 与 `tool_parallel` 在历史调用与响应调用间使用不同 `call_id`，并在 Gemini 步骤首个 `functionCall` 上附带 `thoughtSignature`（同步骤第二个并行调用保持无签名，对应 `4b05d79a5`）及兼容 Antigravity 的大写参数 Schema；新增 `wire_corpus_scenario_test.go`，`TestWireFixturesLoadAndValidate` 自动遍历 `DefaultCorpus()` 中全部 `SourceWire` 夹具校验单文件大小、密钥扫描、按场景名唯一反查、`ValidateRequestRules`（开启 `StrictTopLevel: true`）、`ValidateStreamGrammar` 和非流式响应包络使后续 0.5b 至 0.5e 落盘夹具自动受验，`TestWireBatch05aScenarioCoverageAndReplay` 将本批槽位矩阵、Gemini 方言规则与 `SimulateNextTurn` 多轮重放限定在本批场景集合内；Project Doc 经评估在本步无需改动，留待阶段 0 第 11 步统一更新。
  - 待确认项：无。

- 2026-11-21，阶段 0 第 5 步场景批 2（批次 0.5b，提交 `f7367fbab`）：完成 5.5 节 7 个工具结果与特殊工具场景在 `backend/internal/protocol/testkit/testdata/wire/` 下的 68 个线夹具与槽位重放校验测试。
  - 局部决策：`tool_result_image` 与 `tool_result_error` 在四协议下均提供 `request`、`stream`、`response`，图片统一使用 `SyntheticPNGDataBase64`（1x1 `image/png`），Messages 与 Responses 直接在工具输出内容数组内嵌图片，Chat 按 4.3 节采用 `tool` 消息移位标记加紧邻 `user` 消息归属标记与 `image_url`，Gemini 按 4.4 节在同一 `user` 回合内并列 `functionResponse` 与 `inlineData`，错误输出分别使用 Messages `is_error: true`、Responses 与 Chat 的结构化错误 JSON 字符串以及 Gemini `functionResponse.response.error`；`tool_ids_missing` 在四协议下提供客户端 `request`（其中 Gemini `request` 额外包含无 `id` 的历史 `functionCall` 与 `functionResponse` 对以覆盖 4.4 节请求侧确定性 ID 派生），上游 `stream` 与 `response` 仅在 `openai_chat_completions`（同时覆盖后续增量带空 `id` 与空 `function.name` 的 `826f45a2c` 场景及无 `id` 调用）和 `gemini_generate_content`（单候选内两个同名无 `id` 的 `functionCall`，覆盖 `d91aacd8e`，首个调用带 `thoughtSignature`）下提供；`tools_custom`、`tools_namespace`、`tools_search` 的客户端 `request` 仅在 `openai_responses` 下提供（覆盖 Lark 语法 `apply_patch` 与 `custom` `tool_choice`、默认 `functions` 作用域加普通命名空间加拼接达 86 字节超限命名空间、`tool_search` 加 `defer_loading` 加 `tool_search_output` 历史发现提升与调用），上游 `stream` 与 `response` 在四协议下均提供（Responses 为原生条目，其余三协议按 3.11 节提供 `{"input":"..."}` 分段 JSON 包装、精确 64 字节哈希截断名 `enterprise_observability_telemetry_connector__query_di__5375327d`、`tool_search` 代理函数与 `pager__list_incidents` 函数调用，供第 6 步旧 `bridge` 矩阵直接驱动跨协议还原）；`tools_hosted_search` 在支持托管网页搜索声明与结果的 Messages（`web_search_20250305`、`server_tool_use`、`web_search_tool_result`、`citations_delta`）、Responses（`web_search`、`web_search_call` 及进度事件、`url_citation`）和 Gemini（`googleSearch: {}`、`groundingMetadata`）下提供三类夹具，`openai_chat_completions` 留空；保持 `grammar_chat.go` 的 `ValidateChatStream` 严格不变，在 `wire_corpus_scenario_test.go` 中通过 `expectedUpstreamQuirkStreamRules` 限定 `wire/openai_chat_completions/stream/tool_ids_missing.sse` 必须且仅允许触发 `empty_tool_name_delta` 与 `missing_tool_call_id`，并新增 `TestWireBatch05bScenarioCoverageAndReplay` 覆盖槽位存在与缺省断言、Responses `function-only` 与 Gemini 方言规则及适用组合的 `SimulateNextTurn` 双向重放；Project Doc 经评估在本步无需改动，留待阶段 0 第 11 步统一更新。
  - 待确认项：无。

- 2026-11-21，阶段 0 第 5 步场景批 3（批次 0.5c，提交 `4382b4e49`）：完成 5.5 节 7 个推理与连续性重放场景在 `backend/internal/protocol/testkit/testdata/wire/` 下的 70 个线夹具与槽位重放校验测试。
  - 局部决策：`reasoning_summary` 与 `reasoning_tool_replay` 在四协议下均提供 `request`、`stream`、`response`（Responses 覆盖连续 `summary_index` 0 与 1 的多段推理摘要，Messages 同时覆盖 `thinking` 与 `redacted_thinking` 块，Chat 覆盖工具轮次 `reasoning_content` 以满足 `deepseek-thinking`，Gemini 覆盖 `thought: true` 与步骤首个 `functionCall` 的 `thoughtSignature`）；`reasoning_token_foreign` 在具备线级重放令牌字段的 Messages、Responses、Gemini 下提供带跨方案 `trc1:` 信封（`xai`、`antigravity_claude`、`anthropic`、`openai`）的客户端 `request`，省略无不透明令牌字段的 Chat `request`，并在四协议下均提供上游 `stream` 与 `response`；`reasoning_carrier_roundtrip` 与 `legacy_history` 在跨协议转换客户端 Messages 与 Responses 下提供 `request`（分别覆盖 `trc1:plain:`、带 `p` 的 `trc1:openai:`、带完整原文 `t` 的 `trc1:anthropic:` 与 `trc1:gemini:` 信封，以及升级前遗留在 `thinking.signature` 中的裸 `gAAAA...` 令牌、空签名 `""` 和空 `summary` 遗留密文条目），并在四协议下均提供上游 `stream` 与 `response`；`reasoning_carrier_native` 限定在执行 `PrepareNativeReplay` 的原生 Messages 与 Responses 下提供三类夹具，在 `user` 正文中嵌入 `trc1:plain:example` 与 `trc1:openai:demo` 字面量以检验仅扫描协议令牌路径的边界，并在令牌路径覆盖同方案还原、异方案与异账号删除、空 assistant 内容占位触发、工具循环末尾失去前导思考块时的 thinking 降级，以及 Responses `store: false` 下保留非空 `summary` 与删除空 `summary` 推理条目；`reasoning_encrypted_only` 的客户端 `request` 仅在 `openai_responses` 下提供（构造 `summary: []`、无 `content` 且仅含 `trc1:plain:` `encrypted_content` 的历史工具轮次，复现 `186447541`），并在四协议下均提供上游 `stream` 与 `response`；全部上游 `stream` 与 `response` 夹具均输出签发方原生裸令牌（由阶段 1 `convert` 的 `sealCarriers` 封装信封）；保持 `request_rules_messages.go` 的 `ValidateMessagesRequest` 严格不变，在 `wire_corpus_scenario_test.go` 中新增 `expectedClientIngressRequestRules` 限定转换前的客户端入口夹具 `wire/anthropic_messages/request/legacy_history.json` 在非 `passback` 严格校验下必须且仅允许触发 `MSG-THINKING`（开启 `passback` 方言时零违规通过），并新增 `TestWireBatch05cScenarioCoverageAndReplay` 校验槽位存在与缺省、全部六种 `trc1:` 方案的无填充 `base64url` 载荷规则、方言规则以及成对 `stream` 与 `response` 经 `SimulateNextTurn` 折叠后的 `JSONDiff` 语义等价；Project Doc 经评估在本步无需改动，留待阶段 0 第 11 步统一更新。
  - 待确认项：无。

- 2026-11-21，阶段 0 第 5 步场景批 4（批次 0.5d，提交 `c30572351`）：完成 5.5 节 6 个多模态、结构化输出、截断终止原因与拒答场景在 `backend/internal/protocol/testkit/testdata/wire/` 下的 72 个线夹具与槽位重放校验测试。
  - 局部决策：`image_input`、`document_input`、`structured_output`、`stop_sequence`、`max_tokens` 与 `refusal` 在四协议下均完整提供 `request`、`stream`、`response` 三类夹具；`image_input` 在同一条用户消息中并列 `SyntheticPNGDataBase64`（`image/png`）内联图片与远程 HTTPS 图片 URL（分别覆盖 Messages `source.type` 的 `base64` 与 `url`、Responses `input_image` 的 `data:` 与 `https:` 加 `detail`、Chat `image_url` 的 `data:` 与 `https:` 加 `detail`、Gemini 的 `inlineData` 与 `fileData`）；`document_input` 统一使用 `SyntheticPDFDataBase64`（`application/pdf`），分别覆盖 Messages `document` 块（含 `title`、`context`、`citations`）、Responses `input_file`、Chat `file` 与 Gemini `inlineData`；`structured_output` 在四协议请求中共用同一份严格 JSON Schema（`additionalProperties: false`），分别写入 Messages `output_config.format`、Responses `text.format`、Chat `response_format.json_schema` 与 Gemini `generationConfig`（`responseMimeType: "application/json"` 加 `responseJsonSchema`），流式与缓冲响应均输出跨增量切分但拼接后符合该 Schema 的 JSON 对象字符串；`stop_sequence` 在 Messages（`stop_sequences`）、Chat（`stop`）与 Gemini（`generationConfig.stopSequences`）请求中配置停止序列，在无 `stop` 请求参数的 Responses 中保持合法无 `stop` 请求以支撑跨协议流转换矩阵，响应侧分别覆盖 Messages `stop_reason: "stop_sequence"`（含 `stop_sequence: "</answer>"`）、Responses `status: "completed"`、Chat `finish_reason: "stop"` 与 Gemini `finishReason: "STOP"`；`max_tokens` 统一在 `32` 输出上限处半句截断，响应侧分别覆盖 Messages `stop_reason: "max_tokens"`、Responses `response.incomplete` 事件与 `incomplete_details: {"reason": "max_output_tokens"}`、Chat `finish_reason: "length"` 与 Gemini `finishReason: "MAX_TOKENS"`；`refusal` 采用两轮对话请求并在具备专用拒答历史结构的 Responses（assistant `content[].type: "refusal"`）与 Chat（assistant `content: null` 加 `refusal`）中覆盖历史拒答解码，响应侧分别覆盖 Messages `stop_reason: "refusal"`、Responses `response.refusal.delta` 与 `response.refusal.done` 及 `refusal` 块、Chat `delta.refusal` 与 `message.refusal`（`content: null`）加 `finish_reason: "content_filter"`、Gemini `finishReason: "SAFETY"`；修正 `clientsim_chat.go` 的 `reconstructChatMessageFromSSE`，使仅含 `refusal` 且无文本增量的流式回复与缓冲响应一样将 `message.content` 折叠为 JSON `null`（配单测 `TestSimulateChatNextTurn_RefusalFoldsNullContent`），未放宽任何严格校验器，并在 `wire_corpus_scenario_test.go` 中新增 `TestWireBatch05dScenarioCoverageAndReplay` 覆盖全部槽位、结构契约、Gemini 方言规则及成对 `stream` 与 `response` 的 `SimulateNextTurn` 折叠语义等价；Project Doc 经评估在本步无需改动，留待阶段 0 第 11 步统一更新。
  - 待确认项：无。

- 2026-11-21，阶段 0 第 5 步场景批 5（批次 0.5e，提交 `6d82eb8cc`）：完成 5.5 节最后 5 个场景在 `backend/internal/protocol/testkit/testdata/wire/` 下的 52 个线夹具、本批契约覆盖以及全部 33 个规范场景目录全量落盘断言。
  - 局部决策：`usage_cache`、`upstream_sse_quirks` 与 `empty_response` 在四协议下均完整提供 `request`、`stream`、`response` 三类夹具，中段流故障场景 `stream_truncated` 与 `stream_error_event` 在四协议下提供 `request` 与 `stream` 并省略无对应缓冲响应语义的 `response` 槽位；`usage_cache` 在四协议间对齐同一组底层计数（普通输入 `120`、缓存读取 `640`、缓存写入聚合 `300` 拆为 `180` 加 `120`、输出 `28`），分别覆盖 Messages `speed: "fast"` 加 `"1h"`、`"5m"` 与缺省 `""` 三种 `cache_control` 断点及 `message_delta` 仅含 `output_tokens` 的稀疏尾帧合并、Responses `prompt_cache_key` 与 `service_tier: "priority"` 加含缓存总输入 `1060` 及 `cache_write_tokens_details`（`30m`/`12h`）、Chat `stream_options.include_usage: true` 加音频与预测明细、Gemini `cachedContent` 加 `finishReason` 之后无 `candidates` 的独立 `usageMetadata` 尾帧；保持四套流语法校验器严格不变，`stream_truncated` 构造工具参数 JSON 半截或无终止信号静默 EOF（复现 `bd39eb589` 与 3.5 节不变量 7）并在 `expectedUpstreamQuirkStreamRules` 与 `expectedAbnormalTruncatedStreamRules` 中双向断言正常模式和 `AllowAbnormal: true` 模式下的缺失终态与未闭合块规则，`stream_error_event` 在闭合已开块或条目后发出各协议原生流内错误帧（Messages `event: error`、Responses `response.failed`、Chat 与 Gemini `{"error":{...}}`）并双向断言正常模式触发错误事件规则且 `AllowAbnormal: true` 零违规通过；`upstream_sse_quirks` 四协议流夹具均保留真实 `CRLF`、行尾水平空格、`: ` 注释行、无空格 `data:`、跨行多段 `data:` JSON 及 EOF 无尾随换行（经 `iotest.OneByteReader` 逐字节校验 `sse.NewReader` 分帧），并按协议分别覆盖 Messages 全帧无 `event:` 头与 `ping`（`22235a63c`）、Responses `event: response.created` 裸 `response` 对象加无 `output_item.added` 的 `response.output_item.done` 加空 `output` 的旧式 `event: response.done`（`4296d1e3a`，白名单精确登记 4 条客户端语法规则）、Chat 首行 UTF-8 BOM 加 `id:`/`retry:` 行加 `delta.reasoning` 别名与无换行 `data: [DONE]`、Gemini 累计文本扩展与累计 `functionCall` 批次前缀重发（`2435ab3f5`）；`empty_response` 覆盖四协议零内容与零输出计数及各协议客户端模拟器的空回合历史规则，并在 `wire_corpus_scenario_test.go` 中新增 `TestWireBatch05eScenarioCoverageAndReplay` 与 `TestWireSection55ScenarioCatalogComplete` 断言 5.5 节全部 33 个场景目录在 `testdata/wire/` 下全量落盘且无孤儿或缺失槽位；Project Doc 经评估在本步无需改动，留待阶段 0 第 11 步统一更新。
  - 待确认项：无。

- 2026-11-21，阶段 0 第 6 步（批次 0.6，提交 `7c5d7fec4`、`f36b98e1d`）：完成旧 `protocol/bridge` 实现的 `testkit.Converter` 适配器、7 条路线乘 33 个场景的定律 Q3/S3/S7 矩阵测试以及 `legacy_baseline.json` 基线落盘。
  - 局部决策：在 `backend/internal/protocol/bridge/conversion_matrix_scenario_test.go` 中封装 `legacyBridgeConverter`，复刻 `gateway/forward`、`openaiforward`、`googleforward`、`httpapi`、`upstream/openai` 与 `upstream/gemini` 的生产驱动管线（含 `additional_tools` 提升与 `AdaptResponsesClientTools` 客户端工具降级、`NewResponsesClientToolStreamRestorer` 流式还原、当前请求内明文推理条目 `ReasoningContentByID` 索引、Responses 与 Chat 转换流的 `UsageAccumulator` 跨帧稀疏用量合并、Gemini 尾帧 `usageMetadata` 重放以及断流错误帧写出），并注入固定 UTC 时钟与确定性 SHA-256 随机源保证黄金快照稳定；在 `testkit` 中导出类型别名 `SSEFrame`、`ConversionLoss` 与 `GoldenUpdateEnabled`，使 `bridge` 场景测试仅依赖已授权的 `internal/protocol/testkit` 并维持 3.2 节旧包不直接导入新包的架构边界；在 `testkit/converter.go` 中定义 `LegacyBaseline`、`ValidateLegacyBaseline`、`AssertLegacyBaseline` 与 `ReadEmbeddedLegacyBaseline`，强制每个 `"fail"` 单元携带非空规则列表与单行根因说明，且在 `evaluateLegacyMatrix` 中对未登记规则组合立即报错以防笼统占位原因混入 `backend/internal/protocol/testkit/testdata/legacy_baseline.json`（共 693 格：通过 367、失败 208、跳过 118）；逐一核查全部 208 个失败格确认均为旧引擎真实缺陷并保持严格校验器零放宽，同时依据 Messages 与 Chat 工具调用规范及回归 `bd39eb589` 收紧 `ValidateMessagesStream` 和 `ValidateChatStream` 的 `AllowAbnormal: true` 分支（仅在流以错误帧异常终止或静默半截未收尾时豁免残缺工具 JSON，发出 `message_stop` 或 `finish_reason` 伪称正常完成时照常报 `invalid_tool_input_json` / `invalid_tool_arguments_json`），并在 S3 对 `stream_truncated` 与 `stream_error_event` 增补异常错误终态帧检查（`missing_fault_error_termination`）；Project Doc 经评估在本步无需改动，留待阶段 0 第 11 步统一更新。
  - 待确认项：无。

- 2026-11-21，阶段 0 第 7 步（批次 0.7，提交 `f04c4b815`）：完成 `backend/internal/app/protocol_routes_scenario_test.go` 转换路线清单测试、3.13 节上游族归属校验以及 `backend/internal/app/testdata/golden/routes.txt` 黄金快照落盘。
  - 局部决策：新增跨文件场景测试 `protocol_routes_scenario_test.go`（开头注释列出覆盖的 `gateway_openai_text_execution.go`、`gateway_messages_execution.go`、`gateway_google.go`、`gateway_grok_execution.go`、`qoder_runtime.go`、`gateway_count_tokens.go`、`gateway_openai_tokens.go`），复用 `testkit.Golden` 维护两节六列制表符格式的 `routes.txt` 并配 `parseConversionRoutesGolden` 往返解析校验；`[route_table]` 按 `ProviderPlatforms()`、7 种 `ProviderType` 与 4 个文本客户端协议确定性遍历 `ProtocolFallbackTargets` 和 `SupportsProtocolConversion`，生成全部 64 条单步文本转换路线（含 Anthropic `bedrock` 的 2 条转换路线，以及归入 `gemini` 族的 OpenCode `gemini_generate_content` 路线与独立归入 `antigravity` 族的 Antigravity `oauth` 路线），并在末两列记录 3.13 节的 7 个请求侧上游族和 6 个响应侧上游族；测试同时对 `SupportedGroupClientProtocols("")` 全部 22 个公开客户端协议做互斥完备划分断言、校验 OpenAI OAuth `authMode` 变体不改变文本转换结果，并枚举核对 2.2 节范围外的 13 条非文本 `SupportsProtocolConversion` 适配路线；`[off_table]` 将 5 类不经过路线表的转换用途展开为 13 条具体机制路径并从 `capability` 动态解析适用平台与认证类型，按 `openaiforward/chat.go` 与 `upstream/grok/chat_bridge_eligibility.go` 代码事实将拒绝顶层 `input` 的 Grok 排除在 `cursor_chat_responses_input` 的 `openai_responses` 分支之外（仅保留其经请求前转换进入的 `chat` 分支），并为 `responses_legacy_messages_ingress` 同时登记 HTTP `openai_responses`（`openai:oauth` 与 `anthropic` 5 种类型）和 WS `openai_responses_websocket`（`openai:oauth`）；Project Doc 经评估在本步无需改动，留待阶段 0 第 11 步统一更新。
  - 待确认项：无。

- 2026-11-21，阶段 0 第 8 步（批次 0.8，提交 `006b593c6`、`166cbeb2c`）：完成 `backend/internal/app/helpers_test.go` 中 `newProtocolLabRouter` 泛化装配、`backend/internal/app/protocol_lab_scenario_test.go` T5 网关路由测试以及 `legacy_baseline.json` 中 `gateway_routes` 基线落盘。
  - 局部决策：在 `testkit/converter.go` 中为 `LegacyBaseline` 增加可选分节 `GatewayRoutes *LegacyGatewayBaseline`（`json:"gateway_routes,omitzero"`）并提供 `ValidateLegacyGatewayBaseline` 与 `AssertLegacyGatewayBaseline`，通过 `.legacy_baseline.lock` 目录互斥锁与临时文件原子 `os.Rename` 合并写回，使第 6 步 `protocol/bridge` 的 693 格纯矩阵基线与本步 `internal/app` 的 1,126 格网关路由基线（通过 683、失败 205、跳过 238）共存于同一 `legacy_baseline.json` 且互不覆盖；在 `helpers_test.go` 中将 `newHeartbeatRouteRouter` 泛化为 `newProtocolLabRouter(tb, labOptions) *protocolLabRouter`（保留 `newHeartbeatRouteRouter` 为薄包装），补齐全部 12 个 `RouteEndpoints`（含 `ModelsHandler`、`CountTokensHandler`、`OpenAITokensHandler`、`MessagesHandler` 及其 `/antigravity/v1/messages`、支持本地 `base_url` 覆写的 `AntigravityExecutor`、`QoderRuntime`、`PublicUsage`）和并发安全的内存态 `session.GatewayCache` 加 `session.ReasoningContentCache`，在 `protocolLabProviderStore` 中将错误、限流、过载与临时冷却直接写入 `provider.Record` 供后续选号与重试冷却读取，并在 OpenAI 执行链、WebSocket 入口和 HTTP 绑定间共享同一组 `RuntimeBlockState`、`ModelTransientState`、`ProxyCircuit`、`DeferredService` 与含 `Generic`/`Gemini`/`Groups` 的 `selection.Compatible`，预留 `labOptions.UseIRConversion` 在阶段 0 传入 `true` 时直接 `tb.Fatalf`，对配置 `EnabledProtocols` 与 `BaseURL` 的多槽位平台（`deepseek`、`kimi`、`zhipu`、`opencode`）填充 `api_base_urls` 各槽位避免回退外网默认地址，且 `legacyRouteMiddleware` 的 `ops` 参数传 `nil` 交由路由层 `OpsErrorLoggerMiddleware` 对接 `opts.OpsQueue`；在 `protocol_lab_scenario_test.go` 中覆盖全部 16 条受支持的客户端协议乘提供商种类组合（Gemini 客户端拒绝非 Gemini 上游与 `protocol_fallbacks` 空切片禁转在 `TestProtocolLabRouterEndpointsAndCache` 单独断言），执行 33 个规范场景的 `T5_STREAM` 与 3.3 节 `T5_USAGE` 用量桶核对，以及 `T5_FAILOVER`（Gemini 上游使用 HTTP 401 跳过内部数秒级 `5xx/429` 退避睡眠直接触发外层切号）、`T5_MIDSTREAM_DISCONNECT`、`T5_ERROR_FORMAT` 和基于 `testkit.SimulateNextTurn` 加 `SetProviders` 的 `T5_SWITCH_TO_NATIVE` / `T5_SWITCH_FROM_NATIVE` 两轮异构重放（扫描 Messages、Responses 与 Gemini `thoughtSignature` 路径上的残留 `trc1:` 信封和异族裸令牌），保持严格校验器零放宽并为全部 205 个失败格登记经核实的单行根因；Project Doc 经评估在本步无需改动，留待阶段 0 第 11 步统一更新。
  - 待确认项：无。

- 2026-11-21，阶段 0 第 9 步（批次 0.9，提交 `694e18aa2`、`24ad0b91b`、`aaefd01a0`）：完成 `tools/clientlab/` 版本锁定与模板容器骨架、`backend/internal/app/client_lab_scenario_test.go` T6 客户端实验室驱动、`legacy_baseline.json` 中 `client_lab` 基线落盘以及首批真实客户端录制语料清洗入库。
  - 局部决策：在 `tools/clientlab/` 中锁定 `@openai/codex` `0.171.0`、`@anthropic-ai/claude-code` `2.1.330` 与 `@earendil-works/pi-coding-agent` `1.6.0`（`lockfileVersion: 3` 保留跨平台可选原生包），配置模板置于 `templates/{codex,claude-code,pi}/` 以避开 `.gitignore` 未锚定名称并通过环境变量注入测试密钥，`Dockerfile` 使用非特权用户 `clientlab` 与 `ENTRYPOINT ["/usr/bin/env", "TOKENROUTER_CLIENTLAB_CONTAINER=1"]` 设置一次性容器门禁，同时在 `.dockerignore` 排除 `tools/clientlab/node_modules/` 与 `testdata/corpus/_incoming/` 并在容器 `npm ci` 后按路径显式拷贝 `.dockerignore`、`Dockerfile`、`templates`、`plugin-sdk/go` 和 `backend`；经实测对齐三款客户端的内置 shell 工具与参数（Codex 使用 `exec_command` 与 `cmd` 并选用其内置目录识别的 `gpt-5.5`，Claude Code 使用 `Bash` 与 `command` 并覆盖 `--bare` 和默认完整 beta 两种配置，`pi` 使用 `bash` 与 `command` 覆盖 `openai-completions`、`openai-responses`、`anthropic-messages` 三条跨协议与原生矩阵及 `google-generative-ai` 原生对照且解析 JSON 事件流中的 `stopReason` / `errorMessage`），非推理场景显式关闭思考开关，`lab_tool` 第 1 轮 `Match` 限定 `HasTools: true` 使无工具旁路请求落入 `Script.Default`，Gemini 脚本化工具响应附带步骤级 `thoughtSignature`；在 `testkit/converter.go` 中为 `LegacyBaseline` 新增可选分节 `ClientLab *LegacyClientLabBaseline`（`json:"client_lab,omitzero"`）及锁文件原子合并写回，50 格基线中 14 个原生对照格全数通过、转换格 34 格通过、`codex_via_anthropic` 的 `lab_text` 与 `lab_tool` 2 格记录旧引擎 `ResponsesToAnthropicRequest` 将 `reasoning.effort: "none"` 判为高强度 `budget_tokens: 10240` 高于默认 `max_tokens: 8192` 的真实缺陷；`record` 模式通过网关入口包装器在原生对照轮次直接捕获客户端原始请求写入 `_incoming/`，经 `cmd/corpus import` 与 `verify` 将三款客户端共 21 个请求夹具与 14 个 `meta.json` 清洗入库至 `testdata/corpus/clients/` 并清理临时 `_incoming/`；Project Doc 经评估在本步无需改动，留待阶段 0 第 11 步统一更新。
  - 待确认项：无。

- 2026-11-21，阶段 0 第 10 至 11 步（批次 0.10，提交 `ad845a085`、`2152769b5`）：完成 `.github/workflows/protocol-lab.yml` CI 工作流、`Makefile` 的 `test-clientlab` 目标以及阶段 0 Project Doc 工程文档同步。
  - 局部决策：`Makefile` 在 `.PHONY` 与目标定义中独立新增 `test-clientlab`，不并入常规 `test` 或 `verify`；`.github/workflows/protocol-lab.yml` 对齐仓库现有工作流约定（`actions/checkout@v6`、`./.github/actions/setup-go` 复用 `test` 缓存组、`actions/setup-node@v6` 按 `.node-version` 与 `tools/clientlab/package-lock.json` 恢复 npm 缓存、`permissions: contents: read`、单任务 `timeout-minutes: 40`，以及仅在 `pull_request` 事件启用 `cancel-in-progress`），PR 路径过滤在 5.8 节五组路径之外纳入 `.github/workflows/protocol-lab.yml` 自身以便工作流改动在 PR 中自检，每晚定时设为 `0 4 * * *` 与周一 `0 3 * * 1` 的安全扫描错峰，`workflow_dispatch` 提供可选的 `codex_version`、`claude_code_version`、`pi_version` 输入（留空默认取 `latest`，填 `pinned` 保持锁文件版本，并在 shell 中通过环境变量与字符集白名单校验防范注入），`latest` 任务限定在定时与手动触发运行并设 `continue-on-error: true`，先执行 `npm ci --no-audit --no-fund` 再以 `npm install --no-save --no-package-lock --no-audit --no-fund` 覆盖目标 CLI 包并打印实际版本号，不修改磁盘上的 `package.json` 与 `package-lock.json` 以同时兼容 `TestClientLabConfigTemplatesAndGuards` 与 `TestClientLabScenarios`；核对 `tools/check_changed.py` 与 `tools/test_check_changed.py` 确认快检不跑 T6 子进程，无需改动脚本；在 `docs/architecture/backend_modules.md` 按字母序和既有列宽补齐 `protocol/ir`、`protocol/sse`、`protocol/testkit`、`protocol/testkit/cmd/corpus`、`protocol/testkit/mockupstream` 共 5 个包并保留当前生产仍使用的 `bridge`，在 `docs/operations/development_workflow.md` 保留全部既有锚点，补充 `protocol` 纯包 `encoding/json/v2` 与 `jsontext` 及 `protocol/testkit` `fixture` 依赖规则、`protocol-lab.yml` 说明、带 `<a id="protocol_conversion_testing"></a>` 锚点的 T1 至 T6 测试分层与运行模式小节、5.9 节四步缺陷回归流程以及上游平台与网关验证要求，未写入开发期临时规则。
  - 待确认项：无。

- 2026-11-21，阶段 1 第 6.2.1 步（批次 1.1，提交 `4b24f18bf`、`9404c5438`、`fcc396070`）：完成阶段 1 架构规则登记与 `backend/internal/protocol/ir` 完整行为实现。
  - 局部决策：在 `tools/architecture/catalog.go` 与 `policy_test.go` 按 3.2 与第 7 节阶段 1 规则登记 `codec/{chat,messages,responses}` 和 `convert` 的 `leafDependencies` 及 `Tests` 依赖，开放 `internal/protocol/ir` 测试对 `internal/protocol/testkit` 的导入，并为 `internal/protocol/testkit/mockupstream` 补齐 `internal/protocol`、`internal/protocol/ir` 与 `internal/protocol/sse` 依赖及正反例断言；在 `protocol/ir`（共 11 个手写源文件，未达 15 文件阈值故不设 `doc.go`）中从 `response.go` 拆出 `usage.go`（提供 `UsageFromTokenUsage`、`ToTokenUsage`、明细回填聚合缓存写入及单次相加的含缓存输入总计，并在 `response.go` 保留 `Response`、`Finish`、`FinishReason.IsValid` 与深拷贝以维持单文件有效行数），在 `request.go`、`event.go`、`loss.go` 补齐独立深拷贝与枚举校验；`validate.go` 实现 `Validate`、`ValidateResponse` 与 `ValidateStream`，兼容 Gemini 空 `Model` 与 `-1` 动态思考预算，强制 3.5 节全部 7 条流不变量并放行尾帧将先前未拆分 `Input` 100% 重分类进增长的 `CacheRead` 或 `CacheWrite`；`sequencer.go` 对活动槽位零延迟直通，按上游槽位键升序缓冲交错槽位并保留挂起槽位的 `endPart` 快照以无损合并迟到增量；`accumulator.go` 将规范事件流折叠为 `Response` 并记录 `StreamError`；`token.go` 实现 `FormatCarrier` 与 `ParseCarrier`（固定 `d`/`t`/`r`/`p` 字段顺序与无填充规范 `base64url`，用 `jsontext.Decoder` 拒绝重名键、未知键、非正整数 `p` 及 `gAAAA...` 等裸令牌）；`link.go` 按时间序把 `ToolResult.Tool` 绑定到最近的前序同 `CallID` 调用并回退首见调用索引；`essence.go`（配包内单测与 `essence_external_test.go`）在不修改入参的前提下克隆执行 `Link`、拼接合并多段推理文本、用无 `float64` 精度损失的规范化 JSON 计算 Schema 与参数摘要、展开 `trc1:` 信封，仅对 `base64` 媒体记录归一化 `MIME` 并省略无对应 `LossKind` 的跨协议默认注入字段与响应终态及用量元数据以对齐定律 Q4 和 S4；Project Doc 经评估在本步无需改动，留待阶段 1 后续批次随编解码器与 `convert` 一并落地。
  - 待确认项：无。

- 2026-11-21，阶段 1 第 6.2.2 步（批次 1.2，提交 `638be9c3a`、`8c53b203b`、`b822f0a99`）：完成 `backend/internal/protocol/ir` 共享编解码能力类型与 `backend/internal/protocol/codec/chat`（OpenAI Chat Completions 编解码器、T2 定律 Q1/Q2/Q5/S1/S2/S3/S6/S7、三路解码 fuzz 与 IR golden）。
  - 局部决策：将 `Dialect`、`ToolSupport`、`HistoryMode`、`SchemaDialect`、`ReasoningCapabilities`、`ClientView` 与 `Runtime`（含零值安全的 `NowUnix`、`NextID` 与 `DefaultRuntime`）定义在 `protocol/ir/dialect.go`，供 `codec/*` 与 `convert` 共用并保持 `leafDependencies` 单向无环；为 `ir.OutputFormat` 与 `ir.EssenceOutputFormat` 补齐 `Description` 字段以无损保留 `response_format.json_schema.description` 供 Chat 与 Responses 复用，并在 `testkit/essence.go` 同步支持 `"description"` 近似对账、空原 `CallID` 重写哨兵及 `LossMediaRelocated` 对 `role="tool"` 移位标记的剥离；`codec/chat` 拆为 `wire.go`、`request_decode.go`、`request_encode.go`、`legalize.go`、`response.go`、`stream_decode.go`、`stream_encode.go`、`usage.go`、`finish.go` 共 9 个手写源文件（配单测、三路解码 fuzz、`conformance_scenario_test.go` 与 88 个解码 IR golden）；具备 IR `Extra` 容器的线对象用 `json:",embed"` 收集未知成员并经 `marshalDeterministic` 归一化原始 JSON 值以对齐非法 UTF-8 的 `U+FFFD` 替换行为，拆分入站 `wireInboundMessage` 与无 `Name` 字段的出站 `wireOutMessage` 保留非 `function` 消息的 `name`，将不含建模子字段的顶层 `metadata` 与 `stream_options` 保留进 `Request.Extra`，并在旧式 `functions[]` 迁入 `Tool.Extra` 前剥离 `"type"` 与 `"function"` 冲突键；`ToolResult.IsError` 采用精确双键 `{"error":"...","is_error":true}` 字符串包络往返，工具结果媒体外置时同步写出 `role="tool"` 侧 `toolOutputMediaMarker` 与紧随 `user` 侧归属标记，`HistoryToolTurns` 在带存活 `tool_calls` 的 assistant 轮次始终写出字符串 `reasoning_content` 以满足 `deepseek-thinking`，已完成非 custom 无参函数调用在缓冲响应与流收尾统一写出 `"{}"`；`DecodeResponse` 与正常收尾的 `StreamDecoder` 强制校验非空白 `function.arguments` 为单值合法 JSON 且无重名键（流式正常收尾遇到残缺或重名键参数时标 `Incomplete: true` 并改发终态 `EventError`），入站响应与流式用量均先经 `wirejson.ValidateUsageEnvelope` 与 `ValidateUsageObject` 守卫再经 `openai.UsageAccumulator` 投影不相交 `ir.Usage` 并维持稀疏尾块单调合并；Project Doc 经评估在本步无需改动，留待阶段 1 收尾批随其余编解码器与 `convert` 一并同步。
  - 待确认项：无。

- 2026-11-21，阶段 1 第 6.2.2 步（批次 1.3，提交 `4aac2602b`、`f5de2b20d`）：完成 `backend/internal/protocol/codec/messages`（Anthropic Messages 编解码器、T2 定律 Q1/Q2/Q5/S1/S2/S3/S6/S7、三路解码 fuzz 与 IR golden）。
  - 局部决策：`codec/messages` 拆为 `wire.go`、`request_decode.go`、`request_encode.go`、`legalize.go`、`response.go`、`stream_decode.go`、`stream_encode.go`、`usage.go`、`finish.go` 共 9 个手写源文件（配单测、三路解码 fuzz、`conformance_scenario_test.go` 与 98 个解码 IR golden）；具备 `ir.Extra` 容器的线对象及顶层请求子对象 `thinking`（含 `display` 等字段）、`output_config`、`output_config.format`、`metadata`、`tool_choice` 均通过 `json:",embed"` 保留未知成员并折入 `Request.Extra`，同源编码合并写回对应子对象并剥离冲突顶层键，因方言裁剪未写出任何同源扩展时补记 `LossExtraDropped`，请求侧 `text.citations` 与 `document` 的 `title`/`context`/`citations` 留入 `Part.Extra` 原样往返，`"text"`/`"content"` 源 `document` 退化为 `PartOpaque`，因 `MSG-EMPTY-TEXT` 丢弃空文本块时一并记录 `LossCacheMarkDropped`、`LossExtraDropped` 与 `LossClientMarkerDropped`；`legalize.go` 保持已配对 `tool_result` 的原消息出现顺序并在跨中间非空回合提前移位或块内重排时记录 `LossPartsReordered`；`request_encode.go` 支持 `thinking.type == "disabled"` 与非 `"none"` 的 `output_config.effort` 共存，在启用 `budget_tokens` 时自动抬升 `max_tokens` 留足余量，并在非 `passback` 方言下活跃 `[assistant(tool_use), user(tool_result)]` 循环末轮缺少前导思维块时省略顶层 `thinking` 以满足 `MSG-THINKING`；响应与流将 `text.citations` 和 `citations_delta` 解码进 `Part.Annotations` 并在出站编码时记 `LossAnnotationDropped`，`StreamDecoder` 对无元数据空 `text` 块延迟到非空增量或 `citations_delta` 到达时再发 `EventPartStart` 以对齐 `DecodeResponse`，对 `server_tool_use` 等 `PartOpaque` 块缓冲 `input_json_delta` 并在 `content_block_stop` 校验单值无重名键 JSON 对象后用 `jsontext` 令牌流原地替换 `"input"`，正常收尾遇残缺或重名键工具参数时标 `Incomplete: true` 并改发终态 `EventError`；`StreamEncoder` 在函数工具参数首字节非 `'{'` 时抑制增量使 `input:{}` 生效，仅在 `strings.HasPrefix(finalArgs, emitted)` 成立时补发后缀，并在 `event: error` 前补发未闭合块的 `content_block_stop`；`usage.go` 经 `wirejson.ValidateUsageEnvelope` 与 `ValidateUsageObject` 守卫后委托 `anthropic.MergeAnthropicUsage` 合并稀疏 `message_delta`（在兼容 `prompt_tokens` 拆分前继承已观测缓存桶，保持 `output_tokens: 0` 不覆盖正数累计值）；Project Doc 经评估在本步无需改动，留待阶段 1 收尾批随其余编解码器与 `convert` 一并同步。
  - 待确认项：无。

- 2026-11-21，阶段 1 第 6.2.2 步（批次 1.4，提交 `352d7eda4`、`02b5e1928`）：完成 `backend/internal/protocol/codec/responses`（OpenAI Responses 编解码器、T2 定律 Q1/Q2/Q5/S1/S2/S3/S6/S7、三路解码 fuzz 与 IR golden）。
  - 局部决策：`codec/responses` 拆为 `wire.go`、`request_decode.go`、`request_encode.go`、`legalize.go`、`response.go`、`stream_decode.go`、`stream_encode.go`、`usage.go`、`finish.go` 共 9 个手写源文件（配单测、三路解码 fuzz、`conformance_scenario_test.go` 与 99 个解码 IR golden）；具备 `ir.Extra` 容器的线对象及请求子对象 `reasoning`、`include`、`text`、`text.format`、`metadata`、`conversation`、`tool_choice` 均通过 `json:",embed"` 收集未知成员并经 `marshalDeterministic` 归一化原始 JSON 值以对齐非法 UTF-8 的 `U+FFFD` 替换行为，同源编码合并写回对应子对象并剥离冲突顶层键，在子对象扩展因方言近似丢弃时记录 `LossExtraDropped`；`request_decode.go` 支持 `input` 字符串简写与条目数组、无 `type` 角色消息简写及 `additional_tools` 声明提升，将连续 assistant 侧条目与连续 `*_output` 条目分别归并进单轮消息，从请求 `wireContentPart` 移除块级 `ID` 使块级 `"id"` 留在 `Part.Extra` 而不与消息级 `ID` 冲突，在 `reasoning` 条目同时携带非空 `summary` 与 `content` 时将 `content` 保留进 `Part.Extra["content"]`，并按 3.3 节将 `ToolResult.Tool` 留空交由 `ir.Link` 解析；`legalize.go` 预扫描跨轮次工具调用与输出执行双射配对、保持已配对 `*_output` 的原消息顺序、对无名或跨轮复用 `call_id` 生成确定性 `call_...` ID 并记 `LossIDRewritten`，相邻 assistant 文本与拒答块仅在 `Part.ID` 一致时合并、原为空 `Parts` 的 assistant 回合编码为空 `content: []` 消息，将失序推理条目移至同轮有效后继之前记 `LossPartsReordered`，对缺少后继的推理条目补齐 `LossReplayTokenDropped` 与 `LossReasoningTextDropped` 墓碑，仅在 `tool_search_output` 为紧凑 JSON 数组字符串时写回 `tools` 字段，并按 3.6 节原样写出非空 `Token.Data` 而不检查信封或方案；`response.go`、`stream_decode.go` 与 `stream_encode.go` 在缓冲响应和流式生命周期包络上完整回显 `ClientView.Request` 的 9 个字段，对非空白函数与 `tool_search` 参数校验单值无重名键 JSON（正常收尾失败时标 `Incomplete: true` 并改发终态 `EventError`）且将已完成空参 `tool_search_call` 归一化为 `"{}"`，清理响应 `Extra` 中与包络字段大小写不敏感重名的键，复用 `openai.ResponseLifecycleNormalizer` 兼容裸 `response` 对象、旧式 `response.done`、无 `output_item.added` 直达 `output_item.done` 及可被后续 `response.completed` 恢复的孤立 `error` 帧，流编码维护连续 `sequence_number`、闭合 `reasoning_summary_part` 并用 `strings.HasPrefix` 守卫后缀补发，`usage.go` 经 `wirejson.ValidateUsageEnvelope` 与 `ValidateUsageObject` 守卫后委托 `openai.UsageAccumulator` 投影不相交 `ir.Usage`；在 `testkit/essence.go` 同步支持 `.id` 与 `.call_id` 独立哨兵、原生 `ToolCustom`/`Namespace` 同种类名称清洗与空参 `ToolClientSearch` 降级对齐、重复 `call_id` 孤儿删除时的存活计数保护、无名配对合成 `call_id` 后的 `Result.Tool` 回填、源端原为空 `Parts` 回合保留及中间回合整轮删除后的相邻同角色合并；Project Doc 经评估在本步无需改动，留待阶段 1 收尾批随其余编解码器与 `convert` 一并同步。
  - 待确认项：无。

- 2026-11-21，阶段 1 第 6.2.3 步（批次 1.5，提交 `003bce49e`、`17e05228e`、`466af5019`）：完成 `backend/internal/protocol/convert` 转换引擎、`protocol/testkit/mockupstream` IR 请求解码与合成回复升级，以及三文本协议间六条路线的 T3/T4 定律（Q3/Q4/Q5/S3/S4/S5/S6/S7/S8/M1）、5.3 节未建模字段普查 `golden/census` 与路线 Loss 报告 `golden/loss`。
  - 局部决策：`protocol/convert` 按职责拆为 `exchange.go`、`stream.go`、`buffer.go`、`registry.go`、`dialect.go`、`tools_lower.go`、`tools_raise.go`、`tools_map.go`、`tools_responses.go`、`carriers.go`、`native_replay.go`、`markers.go`、`output_meaning.go` 共 13 个手写源文件（未达 15 文件阈值故不设 `doc.go`），在 `dialect.go` 通过类型别名重导出 `ir.Dialect`、`ir.ClientView`、`ir.Runtime` 及能力枚举，仅在 `Route.Client != Route.Upstream` 时由 `applyDialectRequestRules` 注入目标 Responses 的跨协议默认项以保护单协议 Q2 不动点；在 `ir/loss.go` 新增第 23 个规范枚举 `LossRefusalInlined = "refusal_inlined"` 供无独立拒答块的协议（Messages 及后续 Gemini）将 `PartRefusal` 写成文本块时上报，在 `codec/chat` 补齐 `PartToolCall`/`PartToolResult` 条目 ID 与工具结果 `Extra` 丢失记录、响应侧 `ToolCustom` 自由文本参数的 `{"input":...}` 包装及 `strings.HasPrefix` 尾缀刷新保护，并在 `testkit/essence.go` 与 `ir/essence.go` 同步支持 `LossRefusalInlined`、`trc1:` 信封与同载荷裸令牌方案对齐、历史 `tool_search_output` 发现工具提升对齐、跨协议相邻同角色与多结果 `RoleTool` 归并对齐及 Anthropic `document.title` 对齐；在 `markers.go` 用 `sourceCoords` 跟踪 `stripClientMarkers`、`lowerTools`（历史发现提升工具记 `origIdx = -1` 并在清空降级声明专属 `Tool.Extra` 时记 `LossExtraDropped`）与 `selectReplayTokens` 引起的下标偏移并把目标编码器 `Loss.Path` 映射回原始客户端坐标；`NewExchange` 对 `/v1/chat/completions` 上无 `messages` 但带 `input` 的 Cursor 请求先经有序原始成员提取并剥离 `stream_options.include_usage`、`reasoning_effort` 与单串或数组 `stop`，再委托 Responses 解码器解码并保留 Chat 响应视图；`PrepareNativeReplay` 与同协议 `NeedsResponsesToolAdaptation`/`AdaptResponsesTools`/`RestoreResponsesTools`/`NewResponsesToolStream` 统一开启 `jsontext.AllowInvalidUTF8(true)` 并用有序 `[]rawMember` 无损改写（不经 `map[string]any`、`float64`、`gjson` 或 `sjson`），确保协议令牌路径无合法 `trc1:` 信封的请求逐字节不变，同时完整覆盖 WS 续轮省略 `tools`（保留自定义 `tool_search.description`）、`tools: []` 重置与普通 `function` 工具替换下的历史调用降级和会话 `ToolMap` 清理及独立 `defer_loading` 剥离；`output_meaning.go` 直接从结构化 `ir.Event` 与类型化 Responses 流事件推导 `Semantic` 和 `Terminal`；`mockupstream` 对已注册编解码器的协议解码 `*ir.Request` 供 `Turn.Expect` 断言并支持合成 `Reply.Events` / `Reply.Response`，对未注册协议（`gemini_generate_content`）保留 `InspectRequest` 回退以维持阶段 0 T5/T6 全绿；`NewExchange` 的 `@project-doc` 锚点与 Project Doc 文档同步留待阶段 1 收尾批统一落地。
  - 待确认项：无。

- 2026-11-22，阶段 1 第 6.2.4 步（批次 1.6，提交 `8fcede8f7`、`3d3d8ac69`）：完成三文本协议六条路线的旧实现差分验证（`legacy_diff_scenario_test.go`）、`diff_ledger.json` 与 `tool_names.json` 黄金台账、可对齐旧行为的跨协议默认项修正，以及 5.9 节严格请求校验规则补齐。
  - 局部决策：在 `protocol/testkit` 新增 `diff_ledger.go`（配 `diff_ledger_test.go` 并更新 `doc.go`），通过 `.diff_ledger.lock` 锁文件与临时文件原子重命名支持多阶段按 `scope`（本阶段为 `stage1_text`）合并更新 `testdata/diff_ledger.json`，并在 `convert/testdata/golden/diff/tool_names.json` 落盘降级工具名差分报告；向旧桥接对齐不违反目标协议规范与定律 Q4 的跨协议行为，包括 `codec/chat/legalize.go` 在无工具调用时保持 `ToolCalls` 为 `nil` 使 `omitzero` 省略空数组、`messages_to_responses` 将 `System` 前置为首条 `developer` 消息并记 `LossSystemRelocated`、跨协议发往 Responses 与 Messages 强制开启上游 `stream: true`、发往 Chat 流式请求缺省注入 `stream_options.include_usage: true`、`messages_to_chat` 补齐 `max_tokens` 128 下限与 `parallel_tool_calls: true` 及默认 `reasoning_effort`、跨协议发往 OpenAI 时剥离 Anthropic `metadata.user_id` 长 JSON 指纹、跨协议工具 `Strict` 默认归一化（记 `LossSchemaApproximated("strict")` 并在 `testkit/essence.go` 校验 `nil` 与 `false` 翻转）、跨协议托管 `web_search` 互转（记 `LossToolLowered("hosted_web_search")`，保持 `ir/essence.go` 完整哈希 `Hosted.Raw` 与保留 `Strict`）、跨协议 `ToolFunction` 超长或特殊字符名纳入 `lowerTools` 冲突检测与 `ToolMap` 往返还原、为 `ToolMap` 增加 `CustomNames` 与 `LookupCustomTool` 以还原清洗后的 custom 工具原名、缺少 `tool_search` 时剥离孤立 `defer_loading`、命名空间扁平化固定 64 字节上限及 `tool_search` 代理函数描述对齐；在 `output_meaning.go` 的 `classifyResponsesWireFrame` 补齐无 `summary` 密文推理条目 `response.output_item.done` 的 `Semantic: true` 判定以对齐 `codec/responses/stream_encode.go`；按 5.9 节在 `testkit` 补齐 DeepSeek 思考模式禁止非工具历史 assistant 消息携带 `reasoning_content`（`CHAT-DEEPSEEK-THINKING`，出处 `https://api-docs.deepseek.com/guides/thinking_mode#tool-calls` 与提交 `b567a28f9`）及 Anthropic Messages `output_config.effort` 枚举校验（`MSG-THINKING`，出处 `https://docs.anthropic.com/en/api/messages`），同步更新 `legacy_baseline.json` 与 `multi_turn_scenario_test.go` 原生 DeepSeek 客户端变体门禁；`legacy_diff_scenario_test.go` 对请求侧 `essDiffs` 与 `semJSONDiffs` 按 JSONPath 逐项消解，对流侧按解码、信封、正文、工具调用、`Finish`、`Usage` 六个独立维度校验（确保 `IncludeUsage` 或用量明细 `D3` 仍执行正文、工具与 `Finish` 零差异断言），独立验证新流每帧 `(Semantic, Terminal)` 标记并限定 `stream_flags` 台账仅允许 `D3`；Project Doc 经评估在本步无需改动，留待批次 1.7 统一更新阶段 1 文档。
  - 待确认差异（已获用户裁决通过，全部 `P1` 至 `P7` 已正式纳入 6.2.4 节验收差异清单的 `D2`、`D5`、`D11`、`D12` 类别并迁入 `diff_ledger.json`，待确认状态已清空；其中 `P5` 已按裁决补齐 `Dialect.Reasoning` 模型能力安全回退守卫）：
    1. P1 单跳多模态文档与图片元数据保留（已迁入 `D2`）：旧单跳子集结构体（`AnthropicToResponses`、`AnthropicToChatCompletionsRequest`、`ResponsesToAnthropicRequest`、`ResponsesToChatCompletionsRequestWithMapping`、`ChatCompletionsToResponses`）静默丢弃 base64 PDF 文档块、远程 HTTPS 图片 URL 或图片 `detail` 精度字段；新实现按第 4.1 至 4.3 节保留目标协议原生支持的 `input_file` / `file` / `document`、URL 图片与 `detail`（回退会丢弃用户附件并违反定律 Q4）。复现夹具：`messages_to_responses`、`messages_to_chat`、`responses_to_messages`、`responses_to_chat` 上的 `wire/*/request/document_input.json`；`messages_to_responses`、`messages_to_chat`、`responses_to_messages`、`responses_to_chat`、`chat_to_responses` 上的 `wire/*/request/image_input.json`。
    2. P2 单跳结构化输出与停止序列保留（已迁入 `D2`）：旧单跳桥接未映射 `output_config.format`、`text.format`（`json_schema`）以及 `messages_to_chat` 的 `stop_sequences`；新实现按第 4.1 至 4.3 节保留目标协议原生支持的 JSON Schema 输出约束与 Chat `stop` 数组（回退会丢失结构化输出约束与停止词并违反定律 Q4）。复现夹具：`messages_to_responses`、`messages_to_chat`、`responses_to_messages` 上的 `wire/*/request/structured_output.json`；`messages_to_chat` 上的 `wire/anthropic_messages/request/stop_sequence.json`。
    3. P3 工具错误结果包络与工具图片结果文本及归属标记（已迁入 `D2`）：按第 3.3 节 `ToolResult.IsError` 建模、第 5.4 节定律 Q2/Q4 以及批次 0.5b、1.2、1.4 的局部决策，新实现在无布尔错误位的 Responses 与 Chat 上使用双键 JSON 字符串 `{"error":"...","is_error":true}` 承载错误状态并在转回 Messages 时还原 `is_error: true`，而旧实现直接写纯错误文本或原样透传字符串；`messages_to_chat` 新实现按第 4.3 节请求编码第 3 条在 `role="tool"` 与紧随 `user` 消息写出 `toolOutputMediaMarker` 调用归属文本（旧 `AnthropicToChatCompletionsRequest` 仅移位图片未写归属文本），`responses_to_chat` 新实现从数组形态 `function_call_output` 提取纯文本写入 `role="tool"` 的 `content`（旧实现将 `[{"type":"input_text",...}]` 整段序列化为 JSON 字符串）。复现夹具：`messages_to_responses`、`messages_to_chat`、`responses_to_messages`、`chat_to_messages` 上的 `wire/*/request/tool_result_error.json`；`messages_to_chat` 与 `responses_to_chat` 上的 `wire/*/request/tool_result_image.json`。
    4. P4 `developer` 角色保真、多块文本离散保留与缓存及存储控制字段（已迁入 `D2`）：旧实现将 `developer` 角色降级为 `system`（`responses_to_chat`）或将中段 `developer` 降级为 `user`（`chat_to_responses`），并用 `"\n\n"` 合并多个文本块，同时丢弃 `speed: "fast"` 对应的 `service_tier: "priority"`、`prompt_cache_key` 与显式 `store: false`；新实现按第 4.1 至 4.3 节保留原生 `developer` 角色、离散文本块数组（维持 `ir.Essence` 分块与块级 `cache_control` 对齐）及缓存和存储控制字段。复现夹具：`messages_to_chat`、`responses_to_messages`、`responses_to_chat`、`chat_to_responses` 上的 `wire/*/request/{system_roles,usage_cache}.json`；`responses_to_chat` 上的 `wire/openai_responses/request/{legacy_history,reasoning_carrier_native,reasoning_encrypted_only}.json`；`corpus/clients/claude-code/2.1.330/*`、`corpus/clients/codex/0.171.0/*`、`corpus/clients/pi/1.6.0/lab_*_responses/*`。
    5. P5 显式关闭思考能力守卫映射与无令牌 Chat 明文推理裁剪（已迁入 `D11`）：旧 `AnthropicToResponses` 与 `AnthropicToChatCompletionsRequest` 忽略 `thinking: {"type": "disabled"}` 仍强制输出 `effort: "medium"`（及 `summary: "auto"`），新实现按用户裁决在 `convert/dialect` 对显式关闭思考补齐 `Dialect.Reasoning` 模型能力守卫（仅目标模型明确支持 `"none"` 时输出 `effort: "none"` 且省略 `summary`，支持 `"minimal"` 时回退 `"minimal"`，纯推理模型取最低可用档位，非推理、仅开关控制或能力未知模型直接省略努力度）；旧 `ChatCompletionsToResponses` 将无令牌 `reasoning_content` 拼成 `<thinking>...</thinking>` 可见文本标签写入历史 assistant 消息，新实现按第 3.10 节 `HistoryNone` 丢弃无令牌明文推理并记录 `reasoning_text_dropped`。复现夹具：`messages_to_responses` 与 `messages_to_chat`（含模型能力变体）上的 `corpus/clients/claude-code/2.1.330/*`；`chat_to_responses` 上的 `wire/openai_chat_completions/request/reasoning_tool_replay.json`。
    6. P6 单跳拒答保真、跨协议流式客户端工具降级与流式异源托管搜索块丢弃（已分别迁入 `D2`、`D5` 及 `stream_flags` 的 `D12`）：旧实现在单跳请求（`responses_to_messages`、`chat_to_responses`）与四条单跳流路线（`messages_to_responses`、`messages_to_chat`、`responses_to_chat`、`chat_to_responses`；`responses_to_messages` 上旧实现已保留拒答文本仅漏发 `response.incomplete` 终态故归入 `D3`，串联 `chat_to_messages` 归入 `D2`）中丢弃 `refusal` 内容并将结束原因写成普通 `stop`/`end_turn`，新实现按第 4.1 至 4.3 节保留拒答文本与 `refusal`/`content_filter` 结束原因；在 `messages_to_responses` 与 `chat_to_responses` 流方向，旧流转换器收到 Responses 上游流中的带 `namespace` 函数调用或 `tool_search_call` 时丢失命名空间前缀或整条丢弃 `tool_search_call`，新实现按第 3.11 节扁平化命名空间并输出 `tool_search` 调用（满足定律 S4）；`messages_to_responses` 在 `wire/openai_responses/stream/tools_hosted_search.sse` 上按第 3.4 节与第 4.2 节将异源 `web_search_call` 作为 `OpaqueItem` 丢弃并记录 `opaque_item_dropped`（旧 `resToAnthHandleWebSearchDone` 合成为空结果数组 `content: []` 的 `server_tool_use` 与 `web_search_tool_result` 对，现统一归入扩展后的 `D5`）；`chat_to_responses` 在 `wire/openai_responses/stream/upstream_sse_quirks.sse` 上从无 `output_item.added` 的 `response.output_item.done` / `response.done` 恢复正文 `"Normalized quirky upstream SSE frames cleanly."`（旧桥接输出空正文）。复现夹具：`responses_to_messages` 与 `chat_to_responses` 上的 `wire/*/request/refusal.json`；`messages_to_responses`、`messages_to_chat`、`responses_to_chat`、`chat_to_responses` 上的 `wire/*/stream/refusal.sse`；`messages_to_responses` 与 `chat_to_responses` 上的 `wire/openai_responses/stream/{tools_namespace,tools_search}.sse`；`messages_to_responses` 上的 `wire/openai_responses/stream/tools_hosted_search.sse`；`chat_to_responses` 上的 `wire/openai_responses/stream/upstream_sse_quirks.sse`。
    7. P7 非 `D3` 流式分片粒度、单活动槽位闭合顺序与连续性信封帧引起的 `(Semantic, Terminal)` 序列变化（已迁入 `D12`）：（a）`messages_to_chat` 按第 3.5 与 4.1 节在收到上游 Chat `tool_calls` 分片时实时写出 `input_json_delta(Semantic=true, Terminal=false)`（无参调用刷出 `"{}"`），而旧 `FinalizeChatCompletionsAnthropicStream` 将全部工具分片缓冲至流末合并为单帧 `input_json_delta`（共 11 个夹具）；（b）按第 3.10 节与 `D6` 连续性信封规则，`messages_to_chat` 在无令牌思考块收尾前新增 1 帧携带 `trc1:plain` 信封的 `signature_delta(Semantic=true, Terminal=false)`（共 7 个夹具），`responses_to_messages` 将上游 `redacted_thinking.data` 封装为携带 `trc1:anthropic:r` 信封的无摘要 `reasoning` 条目并写出 `output_item.added(false, false)` 与 `output_item.done(true, false)`（1 个夹具），旧实现均省略对应信封帧；（c）`responses_to_chat` 按第 3.5 节 `ir.Sequencer` 单活动槽位规则在打开 `output_index: 1`（`output_item.added(true, false)`）前先闭合 `output_index: 0`（`function_call_arguments.done(false, false)` 与 `output_item.done(false, false)`）并在交错分片上顺序化输出，而旧 `ChatCompletionsChunkToResponsesEvents` 同时保持多个 `output_index` 开启至流末统一闭合（共 6 个夹具）。复现夹具：`messages_to_chat` 上的 `wire/openai_chat_completions/stream/{reasoning_token_foreign,reasoning_tool_replay,tool_args_empty,tool_args_fragmented,tool_ids_missing,tool_parallel,tool_parallel_interleaved,tool_single,tools_custom,tools_namespace,tools_search}.sse` 与 `wire/openai_chat_completions/stream/{legacy_history,reasoning_carrier_roundtrip,reasoning_encrypted_only,reasoning_summary,upstream_sse_quirks}.sse`；`responses_to_messages` 上的 `wire/anthropic_messages/stream/reasoning_tool_replay.sse`；`responses_to_chat` 上的 `wire/openai_chat_completions/stream/{tool_args_empty,tool_ids_missing,tool_parallel,tool_parallel_interleaved,tools_namespace,tools_search}.sse`。

- 2026-11-22，阶段 1 第 6.2 节验收基准与本地 fuzz 及第 7 节阶段 1 文档（批次 1.7，提交 `c0c7636d9`、`8b82c88a2`、`5a99b3cb4`）：完成 `backend/internal/protocol/convert/benchmark_scenario_test.go` 六路线基准对比、三编解码器包 9 个解码器目标各 10 分钟本地 fuzz 验收（含 3 处边界修复与回归种子），以及 `docs/architecture/protocol_conversion.md` 等阶段 1 Project Doc 文档与 `@project-doc` 代码锚点落地。
  - 实测基准与 10 分钟本地 fuzz 统计（Apple M5 Pro，`darwin/arm64`）：12 组基准中 10 组快于旧实现，最高比值 `1.14x`（满足不超过 `2.0x` 门槛），其中 2 MiB 多轮请求转换 `new/legacy` 耗时比依次为 `messages_to_responses` `0.87x`（`10.95ms` 对 `12.65ms`）、`messages_to_chat` `0.89x`（`10.10ms` 对 `11.34ms`）、`responses_to_messages` `0.29x`（`10.73ms` 对 `36.74ms`）、`responses_to_chat` `0.33x`（`10.92ms` 对 `33.24ms`）、`chat_to_responses` `0.96x`（`8.51ms` 对 `8.89ms`）、`chat_to_messages` `0.24x`（`8.07ms` 对 `33.64ms`），5,000 帧上游 SSE 流转换 `new/legacy` 耗时比依次为 `messages_to_responses` `1.14x`（`10.35ms` 对 `9.12ms`）、`messages_to_chat` `0.61x`（`5.48ms` 对 `9.06ms`）、`responses_to_messages` `0.26x`（`4.36ms` 对 `16.84ms`）、`responses_to_chat` `0.34x`（`5.45ms` 对 `15.99ms`）、`chat_to_responses` `1.03x`（`11.10ms` 对 `10.76ms`）、`chat_to_messages` `0.51x`（`5.03ms` 对 `9.92ms`）；三编解码器包 9 个解码器目标以独立临时 `-test.fuzzcachedir` 各真实跑满 `-fuzztime=10m` 零崩溃（累计 `140,338,758` 次执行）：`codec/chat` 的 `FuzzRequestDecode`（`601.36s`，`11,458,981` execs）、`FuzzResponseDecode`（`601.36s`，`17,479,304` execs）、`FuzzStreamDecode`（`600.80s`，`11,289,432` execs），`codec/messages` 的 `FuzzRequestDecode`（`601.46s`，`13,563,864` execs）、`FuzzResponseDecode`（`600.50s`，`20,180,064` execs）、`FuzzStreamDecode`（`600.46s`，`17,122,519` execs），`codec/responses` 的 `FuzzRequestDecode`（`601.50s`，`17,687,212` execs）、`FuzzResponseDecode`（`601.44s`，`25,225,302` execs）、`FuzzStreamDecode`（`601.38s`，`6,332,080` execs），审查补充改动后另对 `codec/responses` 的 `FuzzRequestDecode`（`2,217,094` execs）与 `FuzzStreamDecode`（`1,417,612` execs）跑 1 分钟 8 并发回归零崩溃；同时确认 `internal/protocol` 之外零生产代码导入阶段 1 新包。
  - 局部决策：`benchmark_scenario_test.go` 附带常规单测 `TestBenchmarkPayloadsValid` 校验 2 MiB 请求体积、精确 5,000 帧计数、目标严格请求规则与流语法规则，旧流对照路径直接从同一原始 SSE 字节经 `sse.NewReader` 扫描并单次执行 `sse.Append` 加 `CompatOutputMeaning`，新流基准每次迭代后将共享 `Exchange.recordedLosses` 重置回请求准备长度以防长跑切片持续增长；修复首轮 10 分钟 fuzz 捕获的 3 个边界问题并在对应 `Fuzz*Decode`（含 `responses/stream_decode_test.go`）补入 `f.Add` 回归种子（不保留 `testdata/fuzz` 目录）：在 `codec/messages` 的 `decodeMediaRequestBlock` 对 `base64` `source.media_type` 执行 `strings.TrimSpace` 对齐出站编码以守住定律 Q2，在 `codec/responses` 的 `encodeResponsesTools` 认领非空命名空间前按声明顺序先刷出前置空命名空间并以 `namespaces[i]` 记录命名空间 `Cache` 与异源 `Extra` 丢失（同步在 `testkit/essence.go` 将 `namespaces[i]` 识别为无副作用放行），在 `codec/responses` 的请求、缓冲响应与流解码器（含 `output_item.added` 初始参数预填与未闭合槽位收尾）统一要求非空 `tool_search_call.arguments` 为单值无重名键 JSON 对象；起草 `docs/architecture/protocol_conversion.md`（196 行，`check_docs.py --stats` 下 `p>=180=0`、`p>=250=0`），设置 `#ir_model`、`#extension_mechanisms`、`#loss_accounting`、`#dialect_capabilities`、`#conversion_pipeline`、`#reasoning_continuity`、`#tool_adaptation`、`#adding_a_protocol` 八个稳定锚点，在 `convert/exchange.go` 的 `NewExchange` 标注 `@project-doc docs/architecture/protocol_conversion.md#conversion_pipeline`，开篇写明当前网关尚未接入、生产仍走 `protocol/bridge`，并在 `docs/architecture/index.md` 与 `docs/architecture/backend_modules.md`（将 `protocol/codec` 标为分类目录并按字母序补齐 `codec/{chat,messages,responses}` 与 `convert`）同步索引。
  - 待确认项：无（批次 1.6 记录的 `P1` 至 `P7` 已获用户裁决正式迁入 6.2.4 节与 `diff_ledger.json`）。

- 2026-11-22，阶段 1 第 6.2.4 步验收审计缺口修复（批次 1.8，提交 `7b4d572a3`、`22bb62561`、`ba62b6f9c`）：完成阶段 0 与阶段 1 手写 Go 文件 `code-comments` 覆盖与硬性下限补齐、流式客户端可见推理文本零差异断言、流式 `(Semantic, Terminal)` 帧标记严格 `D3` 台账与共享帧对齐校验，以及 `P1` 至 `P7` 待确认差异边界复核。
  - 局部决策：将 `codec/responses/stream_decode.go` 中 `handleOutputItemAdded` 对 `tool_search_call` 的 `decodeResponseFunctionArgs` 预校验移至 `getOrCreateItemSlot` 与 `ensureStarted` 之前并补充非法字符串转义回归测试，保证参数预校验失败时不创建槽位或启动 `Sequencer`；在 `legacy_diff_scenario_test.go` 中新增 `extractVisibleReasoningText` 并收紧 `stripStreamCarrierAndToolParts`（仅清除 `PartReasoning.Token` 与 `Redacted` 元数据且仅剔除无可见段落的纯信封空壳），在 `evalStreamContentDiffs` 与 `verifyStreamLedgerAcceptance`（`D6`）强制断言客户端可见推理文本在 `D1` 至 `D3` 之外差异为零；消除 `canonicalizeFramesForFlagAlignment` 的未记账帧合并及 `assertAlignedFrameFlagsMatch` 对 `P6` 与正常流结构语法违规的整条流提前返回，对每条新流执行 `verifyNewStreamFlagInvariants` 线级逐帧与终态不变量校验，将笼统的 `chat_chunk` 拆分为 9 类内容角色标签，限定 `diff_ledger.json` 的 `stream_flags` 条目严格仅属 `D3`（总计 267 条：`D1` 1、`D2` 8、`D3` 203、`D5` 4、`D6` 28、`D7` 11、`D8` 3、`D9` 2、`D10` 7），并对正常流上的 `summary_index_mismatch`、`invalid_output_index`、`tools_custom` 包装、`missing_message_id` 四类 `D3` 修复及 `P6`/`P7` 场景仅做局部授权消解后逐帧校验全部剩余共享帧的 `(Semantic, Terminal)` 标记；统一 `evalStreamContentDiffs` 与 `evalStreamFlagDiffs` 的 `D3`/`P6` 边界（将 `responses_to_messages` 上保留了拒答文本仅漏发 `response.incomplete` 终态的 `refusal.sse` 统一归入 `D3`，删除误导摘要拼接与未命中的 `d3CustomToolUnwrap` 分支），用 `expectedStage1PendingIDs` 双向校验代码中观测到的全部 23 个 `pendingID` 与计划文末 `P1` 至 `P7` 清单精确一致；扫描并补齐 `sse`、`ir`、`codec/{chat,messages,responses}`、`convert`、`testkit` 全部手写 Go 文件的文件概览、常量与变量组、函数内局部类型、共享同步字段组（修正 `ensureUpstreamPrepared` 引用）、字面量判别字段及有状态流生命周期调用规则注释下限；Project Doc 经评估在本批无需改动。
  - 待确认项：无（本批修订的 `P6` 与新增的 `P7` 已随 `P1` 至 `P7` 获用户裁决通过，正式迁入 6.2.4 节 `D2`、`D5`、`D11`、`D12` 与 `diff_ledger.json`）。

- 2026-11-22，阶段 2 第 1 步与第 7 节阶段 2 架构依赖许可准备（批次 2.1，提交 `9e96f685b`、`d98107901`、`e6109c133`）：完成 `tools/architecture/` 中 `gateway` 与 `upstream` 对 `convert`/`ir`/`sse` 的静态依赖开放，完成 7 组执行器与运行时及 `UnifiedTextExecutor` 的 `UseIRConversion` 接线开关、`newProtocolLabRouter` 开关传递，以及生产装配默认关闭的守护测试。
  - 局部决策：在 `tools/architecture/catalog.go` 的 `moduleDependencies`（`internal/gateway`、`internal/upstream`）与 `platformDependencies`（`internal/upstream/{antigravity,gemini,grok,openai,qoder}`）两张合取表中同步开放 `internal/protocol/convert`、`internal/protocol/ir` 与 `internal/protocol/sse`，保持 `codec/*` 封装边界封闭并将 `protocol/qoder` 与 `internal/provider` 的 `convert` 许可留待阶段 6 创建对应包时再加，同时在 `policy_test.go` 补齐正反例与 `_integration_test.go` 覆盖；因 `wshttp.OpenAIWSOptions`（`gatewayws.Parameters`）绑定持久化设置键 `"responses_ws"` 且在 `OpenAIWebSocketExecutor.forTurn()` 中被 `Runtime.Snapshot()` 整值覆盖，将 WebSocket 接线开关置于 `wshttp.OpenAIWSDependencies.UseIRConversion`（内嵌提升至 `OpenAIWebSocketExecutor`）并继续传入 `gatewayws.PassthroughOptions` 与 `gatewayws.StreamOptions`，覆盖专用 WS、连接池 WS 与 HTTP 桥三条链路且避免进入持久化设置；将 `OpenAIResponseOutput` 的开关置于 `OpenAIResponseOptions.UseIRConversion`（独立于 `Configured` 生效，同时供 `OpenAIResponsesExecutor` 与 `OpenAIAuxiliary` 预检读取），为持有未导出 `options` 的 `messageforward.Runtime`、`gatewayhttp.MessagesExecutor`、`gatewayprovider.QoderRuntime` 及惰性构造的 `upstream/qoder.Executor` 增加 nil 安全的只读访问器 `UseIRConversion() bool` 并打通 `QoderRuntimeOptions` 到 `qoder.ExecuteOptions` 的传递链；移除 `newProtocolLabRouter` 在阶段 0 的 `tb.Fatalf` 门禁并将 `labOptions.UseIRConversion` 传递到全部执行器与运行时（在 `TestProtocolLabRouterEndpointsAndCache` 覆盖 `false`/`true` 双向断言），新增跨源文件场景单测 `backend/internal/app/gateway_ir_conversion_wiring_scenario_test.go` 直接调用全部生产 `provide*` 与 `*Options(cfg)` 断言 `UseIRConversion` 默认全关；Project Doc 经评估在本批无需改动。
  - 待确认项：无。

- 2026-11-22，阶段 2 第 2 步（批次 2.2，提交 `c00b5909e`、`7a22a5b3c`）：完成 `upstream/openai/encrypted_content.go` 中 `OpenAIEncryptedContentDigest` 的 `trc1:` 信封解包与 `gateway/provider/model_attributes.go` 的 `ConversionDialect` 构造函数及单元测试。
  - 局部决策：`OpenAIEncryptedContentDigest` 在 `ir.ParseCarrier` 解析成功且 `carrier.Data != ""` 时对内部签发方密文原文 `d` 计算 SHA-256，使客户端信封态与上游裸密文态在 HTTP 和 WebSocket 会话失效密文表共享同一摘要，同时让不含 `d` 的 `trc1:plain:` 信封与其余非信封输入继续按原始字符串计算摘要以防不同明文推理信封碰撞到空串哈希；保留 `ModelConversionOptions` 与 `GeminiThinking` 原样供旧路径使用，`ConversionDialect` 按 3.7、6.4、6.5、6.7 节一次实现完整：Messages 函数名上限设为 `128` 字节，Chat、Responses（含 WS）、Gemini/Antigravity 与 Qoder 设为 `64` 字节；Responses（含 WS）在 OpenAI OAuth / Setup-Token 及 `RequiresResponsesClientToolEncoding() == false` 的 Kimi、OpenCode 上取 `ToolSupportAllResponses`，在 OpenAI API Key、Grok 与 DeepSeek 上取 `ToolSupportFunction`，Messages 与 `gemini_generate_content` 取 `ToolSupportFunction | ToolSupportHostedWebSearch`（对齐 4.4 节第 8 条 `googleSearch` 映射）；`ReplayScheme` 综合 `SignatureMode`、`ServiceID()`、OpenCode `AttributeModel` 规范作者前缀（拒绝未验证或嵌套前缀猜测）和目标协议推导，未识别第三方域名的 OpenAI API Key 中转在排除已知 `passback` 与异族模型前缀后保留 `"openai"`，`Endpoint` 为 `count_tokens` / `countTokens` 时清空 `ReplayScheme` 与 `PlaceholderToken`；`StripEffort` 从 `RoutePlanFromContext` 优先取映射前的 `GroupMappedModel` / `RequestedModel` 传入 `ShouldStripWireEffort`（兼容 `nil` context/target），`Reasoning` 将空指针预算边界转为 `0` 并执行 `.Clone()` 隔离切片与指针，采样与停止词限制汇总 `Attributes` 布尔禁用、`Rules.DropFields` 与 `OmitStopSequences`；Project Doc 经评估在本批无需改动。
  - 待确认项：无。

- 2026-11-22，阶段 2 第 3 步上游族一 Responses 上游（批次 2.3a，提交 `b888c6f63`、`3afff1b4c`、`9855f3aa9`）：完成 `UseIRConversion` 开关下 OpenAI 兼容平台 Responses 上游的合并请求转换入口、无损续接守卫与提示缓存摘要版本、合并流式与缓冲读取器 `upstream/openai/response_converted.go`，以及本族 T5 与 T6 矩阵验证。
  - 局部决策：保持旧 `MessagesPorts`/`ChatPorts` 签名与旧转换分支原样不动，在 `openaiforward` 新增可选搭载接口 `irConvertedPortsCarrier` 与按客户端 `protocol.ProtocolID` 参数化的 `RunConvertedResponses`（`responses_converted.go` 与 `responses_converted_ports.go`），在 `httpapi` 新增 `openAIConvertedResponsesAdapter`、`modelResolutionFromBytes` 与 `OpenAIResponseOutput.ConvertedOptions`；基于 `jsontext`（开启 `AllowInvalidUTF8(true)`）提供无损字节版 `ApplyAnthropicCompatFullReplayGuardBytes`、`TrimCompatResponsesInputToLatestTurnBytes`、`AppendOpenAICompatClaudeCodeTodoGuardBytes`、`BuildAnthropicDigestChainBytes`、`DeriveAnthropicCompatPromptCacheKeyBytes`、`ExtractAnthropicMetadataUserIDBytes` 与 `ExtractAnthropicOutputEffortBytes`，保留未知 `Extra` 成员、大整数精度与数组形态 `function_call_output` 输出，并在重试重算时沿用入站 `callerPromptCacheKey`；核对并修正 `ConversionDialect` 在官方 OpenAI API Key（排除 DeepSeek 宿主）与 Grok 的 Responses 路线上补齐 `ToolSupportHostedWebSearch`（同协议 `NeedsResponsesToolAdaptation` 仅检查 `Custom`/`Namespace`/`ClientSearch`）；对 `/v1/chat/completions` 上无 `messages` 但带 `input` 的 Cursor 请求，目标为 Responses 上游时经无损成员补丁强制 `stream: true` 并将响应经 `convert.Exchange`（客户端协议设为 `ProtocolOpenAIChatCompletions`）转回 Chat，目标为非 Responses 上游时在 `ConvertCursorResponsesToChat` 先经 `modelResolutionFromBytes` 解析目标模型规则再由 `convert.NewExchange` 按 `ProtocolOpenAIResponses` 解码并编码为 Chat；`ReadConvertedBuffered` 与 `ReadConvertedStreaming` 统一在 `lifecycle.NormalizeData` 之后执行 `OpenAICompatPayloadWithEventType` 回填缺失 `"type"`，保留前导非语义帧暂存窗口、孤立 `error` 帧被后续 `response.completed` 恢复、`EmptyCompleted` 切号、接入 `f.Semantic` 的 `ChatSilentRefusalDetector` 大请求守卫、`NewDeferredOutputContext` 延迟提交响应头、`cyber_policy` 未输出前显式提交 HTTP 200 SSE 错误帧（Chat 客户端追加 `data: [DONE]`）、`CommittedSSE(c) == true` 时的无计时本地终态 SSE 错误帧（Responses 客户端固定发合成 `response.failed`）、客户端断开后继续排空计费与原始 `UpstreamResponseModel` 采集以及 debug 级别 `Losses()` 计数；在 `protocol_lab_scenario_test.go` 与 `client_lab_scenario_test.go` 新增 `TestProtocolLabIRResponsesUpstreamMatrix` 和 `TestClientLabIRResponsesUpstreamScenarios` 验证 T5/T6 全绿，并因提交 `3d3d8ac69` 收紧 `MSG-THINKING` 规则同步旧路径 `legacy_baseline.json` 中 `client_lab` 的 `codex_via_anthropic` 两格基线；Project Doc 经评估在本批无需改动。
  - 待确认项：无。

- 2026-11-23，阶段 2 第 3 步上游族二 Chat 上游（批次 2.3b，提交 `da4a0744a`、`01310c76e`）：完成 `UseIRConversion` 开关下 OpenAI 兼容平台 Chat 上游的合并请求转换入口、合并流式与缓冲读取函数 `ReadRawChatConvertedStreaming` / `ReadRawChatConvertedBuffered`，以及本族 T5 与 T6 矩阵验证。
  - 局部决策：保持旧 `RawFallbackPorts`、`raw_fallback.go` 与旧分派分支原样不动，在 `openaiforward` 新增 `ConvertedRawChatPorts`、`ConvertedRawChatCarrier` 及按客户端 `protocol.ProtocolID` 参数化的 `RunConvertedRawChat`（`raw_fallback_converted.go`），在 `httpapi` 新增 `openAIConvertedRawChatAdapter`（含空服务安全的 `UpdateGrokUsage`）与绑定真实 `p.provider` 的 `OpenAIResponseOutput.ConvertedRawOptions`（复用空 `provider` 安全的 `convertedStreamFailure` 并传 `isBareErrorEvent = false`，使每条 Chat 流内终态错误仅经 `StreamFailure` 在 `suppressDefaultRateLimit = true` 下执行一次健康评估）；新路径不调用 `ReasoningContentByID` 与 `ReasoningHistory`，推理文本由 `trc1:plain` 信封恢复，经有序 `jsontext` 成员补丁在 Grok Chat 上无损剥离 Responses 专用的 `prompt_cache_key`、协调显式 Messages `"max"` 努力档位，并对 Messages 与 Responses 统一执行编码后 `PrepareModel` 以及 `Dialect.StripEffort` 下的剥离前努力度计费归属保留；在 `upstream/openai/response_raw_chat.go` 实现 `ReadRawChatConvertedStreaming` 与 `ReadRawChatConvertedBuffered`（原 6 个旧读取函数原样保留），保留帧大小限制、心跳过滤、仅对 `event: error` 执行的 JSON 错误包络回填（普通带 `event:` 头的 Chat 分块原样直通）、经 `wirejson.ValidateUsageEnvelope` 守卫的原生计费用量累加、转换前首次声明 `UpstreamResponseModel` 与顶层 `service_tier` 采集、`buf.Write`/`stream.Write` 成功后再记录 `sawFinishReason`、前导非语义帧暂存窗口与 `ChatSilentRefusalDetector` 大请求守卫、`NewDeferredOutputContext` 延迟提交响应头、`CommittedSSE(c) == true` 时的无计时本地终态 SSE 错误帧（已开始输出后命中 `cyber_policy` 时经 `WriteRawTerminalSSE` 发送 `stream.Close` 生成的终态帧以保持 Responses 连续 `sequence_number` 与 `response.id`）、客户端断开后继续排空计费以及 debug 级别 `Losses()` 计数，同时在 `tools/architecture/catalog.go` 为 `internal/upstream` 与 `internal/upstream/openai` 测试集开放 `internal/protocol/testkit`；在 `protocol_lab_scenario_test.go` 与 `client_lab_scenario_test.go` 新增 `TestProtocolLabIRChatUpstreamMatrix`（含双轮无摘要 `trc1:plain` 无 Redis 往返、HTTP 200 SSE 前导帧窗口切号、Cursor `input` 形态 Chat 请求，以及阶段 2 第 4 步前 `T5_SWITCH_TO_NATIVE` 仅剩 `unstripped_trc1_carrier` 单一规则断言）和 `TestClientLabIRChatUpstreamScenarios`（10 个真实 CLI 格子）验证 T5/T6 全绿；Project Doc 经评估在本批无需改动。
  - 待确认项：无。

- 2026-11-23，阶段 1/2 用户裁决落地（批次 1.9，提交 `53dfac845`、`6781c32c2`）：完成 Messages 显式关闭思考转往 Responses 与 Chat 的 `Dialect.Reasoning` 模型能力安全回退守卫，完成 `P1` 至 `P7` 纳入第 6.2.4 节验收差异清单（扩展 `D2`、`D5` 并新增 `D11`、`D12`），以及全部 `pendingID` 迁入 `diff_ledger.json` 清空待确认项。
  - 局部决策：在 `convert/dialect.go` 中为 Messages 显式关闭思考（`thinking.type == "disabled"`、`budget_tokens == 0` 或 `effort == "none"`）实现 `resolveDisabledThinkingEffort` 并统一清空 Responses 的 `summary`，仅在 `ControlsKnown == true` 且 `Efforts` 明确包含 `"none"` 时输出 `"none"`，支持 `"minimal"` 时回退 `"minimal"`，仅对 `!Toggle && len(Efforts) > 0` 的纯推理模型回退至最低可用规范档位，而对 `Toggle == true` 但不含 `"none"`/`"minimal"` 的双协议模型（如 `models.dev` 合并了 Messages 努力度与 Chat 开关的 `deepseek-v4-pro`）、非推理模型（`gpt-4o`）和目录未知模型（`ControlsKnown == false`，如未登记的 `gpt-5-codex`、`deepseek-reasoner`）直接省略努力度字段，避免误启 Chat 思考或触发上游 400；在 `openaiforward` 的 `responses_converted.go` 与 `raw_fallback_converted.go` 中识别 `thinkingDisabled` 跳过编码后的 `MessagesEffortValue` 覆写，防止伴随的 `output_config.effort` 覆盖能力守卫回退值；在 `app/helpers_test.go` 的 `newProtocolLabRouter` 中仅当 `opts.UseIRConversion == true` 时为 `middleware.ModelAttributes` 注入 `readers.Attributes`，使新路径解析真实内嵌目录能力且保持阶段 0 旧路径基线不变；在 `testkit/diff_ledger.go` 的 `ValidateDiffLedger` 中锁定 `D12` 仅用于 `stream_flags` 且 `stream_flags` 仅允许 `D3` 或 `D12` 的双向边界，删除 `legacy_diff_scenario_test.go` 全部内存态 `pendingID` 豁免并落盘 375 条 `stage1_text` 台账（`D1` 1、`D2` 63、`D3` 203、`D5` 5、`D6` 28、`D7` 11、`D8` 3、`D9` 2、`D10` 7、`D11` 21、`D12` 31），同步更新 `docs/architecture/protocol_conversion.md` 第 6 步说明。
  - 待确认项：无。

- 2026-11-23，阶段 2 第 3 步上游族三 Messages 上游及 Bedrock 转换分支（批次 2.3c，提交 `72acc06af`、`84b02d544`）：完成 `UseIRConversion` 开关下 Anthropic 平台（含 OAuth、Setup Token、API Key、Vertex `service_account` 与 AWS `bedrock`）和兼容平台 Anthropic 端点的合并请求转换入口、合并流式与缓冲读取函数 `ReadConvertedAnthropicStreaming` / `ReadConvertedAnthropicBuffered`、跨 Claude 模型签名 400 无损剥离重试，以及本族 T5 与 T6 矩阵验证。
  - 局部决策：保持旧 `AsResponses`/`AsChat`、`ForwardNativeResponses`/`ForwardNativeChat`、`conversion_output.go`、`anthropic_output.go` 与旧分派分支原样不动；因 `gateway/forward` 为 `core` 层不可导入 `net/http` 或 `openaiforward`，在 `forward` 新增 `AsConverted` 与 `ConvertedMessagesPorts` 委托响应读取，由 `provider` 层的 `messageforward.Runtime.Converted`（`conversion_ir.go`）和 `openaiforward.ForwardConvertedAnthropic`（`anthropic_converted.go`）共用 `openaiforward` 的一对合并读取函数，零新增架构特例许可；两类平台均保持 `Mimic -> EnforceCacheControlLimit -> InjectAnthropicCacheControlTTL1h` 的编码后顺序并执行 `PrepareModelAttributeParameters` 与 `Dialect.StripEffort` 剥离前努力度计费归属保留，客户端解析与转换校验失败（含 `"ultra"` 档位拒绝）写出 `400 invalid_request_error` 并包装 `MODEL_CAPABILITY_UNSUPPORTED` 以免计入提供商调度扣分或退化为 `502`，且 `runConvertedChatToResponses` 在目标为 Anthropic Messages 时跳过 `ConvertCursorResponsesToChat` 直接单跳转换 Cursor `input` 形态 Chat 请求以保留 `trc1:anthropic` 签名和 `ToolMap` 工具身份还原；在 `protocol/anthropic/signature_json.go` 基于 `jsontext`（开启 `AllowInvalidUTF8(true)`）新增 `StripMessagesSignatureBlocksForRetryBytes`，标准 Claude 模式剥离历史 `thinking`/`redacted_thinking` 并在活跃工具循环失去前导推理块时同步移除顶层 `thinking` 与 `context_management.edits` 中的 `clear_thinking_20251015`，`passback` 模式仅剥离 `thinking.signature` 保留思考文本，供所有认证类型遇到跨模型签名 400 时同号重试一次；在 `messageforward` 补齐 `Bedrock` 转换分支，复用 `BedrockRoute` 区域路由、`bedrockBetaTokens`、`PrepareBedrockRequestBodyWithTokens`、SigV4（重试时重新签名）与 API Key 认证及同步 `bedrockEventStreamSSEReader` 二进制 AWS EventStream 转 SSE 解码桥（归一化 `amazon-bedrock-invocationMetrics` 并将异常帧映射为标准 Anthropic `event: error`），同时在 Bedrock 与 Vertex 上经 `jsontext` 无损剥离不支持的顶层 `"speed"` 以防 Bedrock `ValidationException` 和误触快速档计费；`ReadConvertedAnthropicStreaming` 与 `ReadConvertedAnthropicBuffered` 在 `exchange.Stream`/`Buffer` 解码前执行 `RestoreToolNames` 使 OAuth 工具别名还原与 IR `ToolMap` 组合生效，保留 `wirejson.ValidateUsageEnvelope` 计费用量守卫、转换前原始 `UpstreamResponseModel` 采集、按 Anthropic `error.type` 解析且仅在未输出前持久化的单次 `StreamFailure` 健康评估、前导非语义帧暂存窗口、`NewDeferredOutputContext` 与 `CommittedSSE` 无计时终态 SSE 错误写出、`sawMessageStart && sawStop` 流末读错守卫、客户端断开后继续排空计费及 debug 级别 `Losses()` 计数；在 `helpers_test.go` 支持带本地 `base_url` 的 Vertex、`bedrock` 与 Anthropic `oauth` 路由覆写，新增 `TestProtocolLabIRMessagesUpstreamMatrix` 与 `TestClientLabIRMessagesUpstreamScenarios` 验证 T5/T6 全绿（旧路径失败的 `codex_via_anthropic` 在新路径转绿）；Project Doc 经评估在本批无需改动。
  - 待确认项：无。

- 2026-11-23，阶段 2 第 3 步上游族四 Grok 的 Chat 转 Responses（批次 2.3d，提交 `fde3f8eb5`）：完成 `UseIRConversion` 开关下 `httpapi/grok_chat_execution.go` 的 IR 转换入口接入、Grok 跨协议链路闭环核查、`xai` 密文 400 无损剥离重试，以及本族 T5 与 T6 矩阵验证。
  - 局部决策：保持旧 `ChatResponses` 与 `UseIRConversion == false` 分派分支原样不动，在 `GrokExecutor.ChatResponses` 新增门控后的 `chatResponsesIR`，强制 `Dialect.OmitStoreAndInclude = true` 省略跨协议默认 `store` 与 `include`，复用批次 2.3a 合并读取器 `openai.ReadConvertedStreaming` 和 `openai.ReadConvertedBuffered` 与 `upstream.NewDeferredOutputContext`；核查 `routes.txt` 全部 4 条 Grok 跨协议路线闭环（同协议 `grokforward/responses.go` 留待阶段 2 第 4 步 `PrepareNativeReplay` 与阶段 5），使 `openaiforward.RunConvertedResponses` 在显式 `ProtocolOpenAIResponses` 路由与跨阶段切到 Grok Responses 时跳过旧桥接专用的 `GrokChatResponsesBridgeEligibility` 字段子集黑名单直达 `DispatchGrok`，让 `convert.Exchange` 完整处理 `developer` 角色、`stop`、`reasoning_effort`、文档输入以及无 `messages` 带顶层 `input` 的 Cursor 请求（强制上游 `stream: true` 并剥离 Cursor 扩展控制字段），同时在未规划自适应 OAuth 直调分支（`Route.Protocol() == ""`）保留桥接资格判断与 `handled=false` 原生 Chat 回退；经 `jsontext` 有序成员补丁保留标准 Chat 的 Trae 顶层 `instructions` 与 `service_tier` 归一化，在调用 `ApplyGrokResponsesCacheIdentity` 时按 `provider.View().IsGrokOAuth()` 控制 `injectFreeTierTools` 以免向 Grok API Key 无工具请求注入仅限 OAuth CLI 的 `web_search`/`x_search` 伴生工具，并保留 Free 档位函数工具缓存路由、`xai-request-id` 提取、Grok 配额快照观测（补齐 `openAIConvertedResponsesAdapter.UpdateGrokUsage`）与 `grok_missing_usage` 切号检查；在 `protocol/openai/encrypted_replay.go` 基于 `jsontext`（开启 `AllowInvalidUTF8(true)`）新增 `StripResponsesEncryptedReasoningBytes`，供 `chatResponsesIR`（Cursor `input` 形态携带解包后的 `trc1:xai:` 密文）和 `openaiforward`（Messages 转 Grok Responses，外层重算改用无损 `StripMessagesSignatureBlocksForRetryBytes`）遇到同号 HTTP 400 失效密文时无损剥离密文与空骨架重试一次且保留 64 位整数字面量；本地 JSON 解析、`ultra` 推理档位、纯图片模型与 `convert` 能力校验拒绝统一经 `WriteGrokForwardInvalidRequest` / `SourceError` 写出 `400 invalid_request_error`（`CommittedSSE` 为真时发无计时终态 SSE 错误帧）并包装 `MODEL_CAPABILITY_UNSUPPORTED` 跳过调度器负反馈，上游 HTTP 错误经 `handleConvertedChatHTTPError` 先拦截请求级策略与参数拒绝再对提供商错误仅做一次健康评估与一次 `AppendOpsUpstreamError`；在 `helpers_test.go` 为 `newProtocolLabRouter` 接通内存 Grok OAuth 凭据源，在 `protocol_lab_scenario_test.go` 与 `client_lab_scenario_test.go` 通过独立的 `grokLabProviderSpecs()` 新增 `TestProtocolLabIRGrokChatResponsesMatrix` 与 `TestClientLabIRGrokResponsesUpstreamScenarios`（16 个真实 CLI 子进程格）验证 OAuth 与 API Key 两条 Grok Responses 路线全绿且不改变阶段 0 基线格集合；Project Doc 经评估在本批无需改动。
  - 待确认项：无。

- 2026-11-23，阶段 2 第 4 步原生路径信封处理（批次 2.4，提交 `bb08cac1b`、`b44eddca0`）：完成 `UseIRConversion` 开关下全部 Messages 原生路径与全部 Responses 原生路径（含 `/v1/messages/count_tokens`、`/v1/responses/input_tokens` 预检及专用 WS、连接池 WS、WS HTTP 桥三处 WebSocket 路径）的 `convert.PrepareNativeReplay` 接线，收紧 `T5_SWITCH_TO_NATIVE` 混合分组断言并补齐各原生入口单元与场景测试。
  - 局部决策：保持 `UseIRConversion == false` 时全部旧原生路径完全不调用 `PrepareNativeReplay`，在 `gateway/provider/model_attributes.go` 新增 `PrepareNativeReplay` 与 `PrepareNativeReplayWithResolution`，先以 `bytes.Contains(body, []byte(ir.CarrierPrefix))` 零分配快速短路，命中后再按 `ModelRequestState(body)` 计算条件规则并调用 `ConversionDialect`（debug 级别记录 `gateway.native_replay_losses`），对 `ProtocolResponsesWebSocket` 向 `ResolveModelAttributes` 传 `"openai_responses"` 以防未知协议字符串回退到 Chat 槽位，同时在 `ConversionDialect` 与 `convert.Route` 上保留 `ProtocolResponsesWebSocket` 确保 `session.update` 帧与正文字面量 `"trc1:"` 逐字节不变（守住 D10）；Messages 原生路径在 `gateway/forward/messages.go` 经可选接口 `nativeMessagesReplayPreparer` 置于 `NormalizeDateline` 之后、`CacheLimit` 与 `FilterThinking` 之前执行（避免旧 `FilterThinkingBlocksInternal` 先经 `map[string]any` 处理未解封块损伤 64 位整数或产生空 `content: []`），在 `messageforward` 的 `passthrough.go`、`bedrock.go`（置于 `ApplyModelAttributeParameters` 与 `bedrockBetaTokens` 之前使工具循环降级后的顶层 `thinking` 同步生效）和 `requests.go`（覆盖直接构造器与普通/透传两条 `buildCountRequest` 计数链路）补齐门控调用，并在 `openaiforward/anthropic_flows.go` 与 `httpapi/openai_native_anthropic_ports.go` 接入 `NativeMessagesReplayPreparer` 覆盖 OpenCode Claude 同方案签名还原及 Kimi/DeepSeek/GLM 等 `passback` 模型无签名思考文本保留；Responses HTTP 与预检路径在三条互不重叠的原生分支上各执行一次且不在跨协议分派（`DispatchRawChat`、`DispatchAnthropic`）前提前剥离：`openaiforward/passthrough.go`（排除 `CompactPath`）置于 `CompatibilityBodyWithSkip` 之后以保留 `store: false` 下跨账号条目的可见 `summary`，`httpapi/openai_responses_execution.go` 置于 `TransformRequest` 之后、`lineageEntryBody` 之前（仅在字节变化时刷新 `requestView`），`grokforward/responses.go`（排除 `in.Compact`）通过 `Options.PrepareNativeReplay` 回调置于 `PatchGrokResponsesBodyWithClientTools` 之前，`httpapi/openai_auxiliary_input_tokens.go` 置于 `PrepareNativeInputTokens` 投影之前以便跨账号剥离仍能观测到顶层 `store: false` 并同步移除合成 `id`；三处 WebSocket 路径遵循 `internal/gateway/ws` 的 `core` 分层边界，在 `ws/passthrough_contract.go` 定义可选接口 `nativeReplayDialectResolver` 与 `prepareOutboundNativeReplay` 并由 `ws/httpapi` 的 `wsPassthroughAdapter` 实现（`wsStreamAdapter` 内嵌复用当轮 `attributeContext` 会话，零新增架构例外许可）：`ws/passthrough.go` 在首轮 `PrepareDial` 前改写出站副本保留 `turnPayloads` 原始信封并在后续 `isResponseCreate` 入队前校验改写，`ws/stream.go` 在 `lease.WriteRequest` 前改写出站副本且令 `invalid_encrypted_content` 摘要提取读取改写后的 `outboundPayload`，`openai_ws_http_bridge.go` 在 `proxyOpenAIWSHTTPBridgeTurn` 与 `resolveGrokWSCacheIdentity` 两处同步执行 `prepareHTTPBridgeNativeReplay` 使 Grok WS 缓存身份哈希与出站 HTTP 请求体一致；在 `helpers_test.go` 为 `protocolLabTransport` 增加 `HostOverrides` 支持使官方 `api.openai.com` 的 `input_tokens` 请求在本地测试中落入受控上游校验，收紧 `protocol_lab_scenario_test.go` 全部 4 处 `T5_SWITCH_TO_NATIVE` 断言为 `BaselineStatusPass`，并新增 `TestProtocolLabIRNativeReplayMatrix` 与各包单元测试；Project Doc 经评估在本批无需改动。
  - 待确认项：无。

- 2026-11-23，阶段 2 第 5 至 6 步与阶段 2 收尾对账（批次 2.5，提交 `b7b4c5ecf`、`506f7d985`、`7cb9f7aab`、`05d539ce9`）：完成三处无接线开关包级辅助函数的 `convert` 与 `jsontext` 新实现及全语料差分验证，完成阶段 2 范围 T5 混合分组与 T6 全部 18 格扩展场景全绿验证，完成旧基线 358 条缺陷逐条对账报告 `stage2_baseline_resolution.json` 落盘，并补充 `docs/architecture/protocol_conversion.md` 阶段 2 接线说明及清理前序批次尾项。
  - 阶段 2 旧基线缺陷逐条对账统计（`backend/internal/app/testdata/golden/stage2_baseline_resolution.json`）：`TestProtocolLabStage2BaselineResolution` 对账 `legacy_baseline.json` 中阶段 2 上游范围共计 1,474 格（旧基线通过 836、跳过 280、失败 358），阶段 2 新路径回归 0 格、跨协议转换路线遗留失败 0 格；全部 358 条旧 `fail` 缺陷逐条核验结果均写入 `stage2_baseline_resolution.json`，其中 `fixed` 339 条（纯转换矩阵 `entries` 594 格中的 188 条全数修复，T5 网关路由 `gateway_routes` 844 格中的 168 条修复 149 条，T6 客户端实验室 `client_lab` 36 格中 `codex_via_anthropic` 的 2 条全数修复），保留 `still_present` 19 条（全部位于第 2.2 节与第 10.1 节明确不经过 `convert` 的四条同协议原生透传路线 `messages_via_anthropic`、`messages_via_deepseek_anthropic`、`responses_via_openai`、`chat_via_openai_chat`，主要为原生 SSE 行扫描器对流末无空行尾帧或中途断流未补发块闭合帧的既有行为，均已逐条记录核实原因）。
  - 局部决策：保持无开关生产调用点继续调用旧函数留待阶段 8 切换，仅在已有 `UseIRConversion` 门控的 `OpenAIAuxiliary`（`/v1/messages/count_tokens` 与 `/v1/responses/input_tokens`）按开关切向新实现；在 `gateway/tokenestimate/estimate.go` 实现 `CountingPayload`、`ProjectCountingPayload`、`MarshalCountingPayload`、`EstimateCountingPayload`、`ResponsesBytes` 与 `AnthropicIR`，在 `gateway/provider/input_tokens.go` 实现 `PrepareAnthropicInputTokensIR`、`PrepareAnthropicInputTokensWithAttributes` 与 `PrepareNativeInputTokensIR`（先在完整请求体上执行 `PrepareNativeReplay` 解封同账号 `trc1:openai:` 密文或剥离跨账号与异族信封，再投影官方计数端点允许的 `{model, instructions, input, tools, tool_choice}` 五个字段，且 `MarshalCountingPayload` 与 `countingPayloadToLegacyRequest` 统一开启 `jsontext.AllowInvalidUTF8(true)` 将原始字符串中的非法 UTF-8 字节替换为 `U+FFFD`），在 `gateway/provider/openai_responses_ingress_compat.go` 实现 `NormalizeOpenAIResponsesLegacyIngressIR` 与 `convertLegacyResponsesMessagesIR`（保留外层权威原生 `input`、`prompt` 与 `commands` 归一化逻辑，将嵌套 Chat 工具与平面 Responses 工具隔离防止 `codec/chat` 误拒混合报文并无损保留 64 位整数字面量与扩展字段），三组语料差分测试均补齐 `default:` 分支对 `DefaultCorpus` 全部线夹具和客户端录制语料按 6.2.4 节 `D1`/`D2`/`D3`/`D4`/`D7`/`D8`/`D10`/`D11` 逐项对账；在 `protocol_lab_scenario_test.go` 新增 `TestProtocolLabIRStage2MixedGroupAndOpenCodeMatrix` 验证 OpenCode 六条阶段 2 转换路线、Bedrock 二进制 EventStream / Vertex / OpenCode / Kimi 与 DeepSeek `passback` 的两轮“转换后原生”重放及官方 `input_tokens` 五字段投影；在 `client_lab_scenario_test.go` 新增 `TestClientLabIRStage2ExtendedScenarios`，覆盖阶段 2 全部 18 个矩阵格子（6 个原生对照加 12 个跨协议转换）上的 `lab_tool_parallel`、`lab_reasoning_tool`、`lab_switch_provider`（含 `pi_responses` 全配置与方言切换）、`lab_failover`、`lab_truncated`（通过 twin `Turn` 同时兼容客户端报错退出与自动重试恢复并断言每请求单次 `UsageLog` 结算）、`lab_custom_tool` 与 `lab_long_stream`；在 `docs/architecture/protocol_conversion.md` 新增 `#gateway_integration` 小节说明阶段 2 四类上游族合并入口与原生 `PrepareNativeReplay` 门控接线并声明当前生产默认关闭 `UseIRConversion`，同时在 `tools/architecture/catalog.go` 补齐 `internal/gateway` 测试集对 `internal/protocol/testkit` 的许可并清理前序批次审查记录的注释与导入别名尾项。
  - 待确认项：无。

- 2026-11-24，阶段 3 第 1 步与第 7 节阶段 3 架构依赖规则登记（批次 3.1，提交 `dd5d80329`、`a5789c405`、`51cfb0825`）：完成 `tools/architecture/` 中 `protocol/codec/generatecontent` 的边界登记、Google Gemini GenerateContent 编解码器完整实现、`protocol/convert` 与 `mockupstream` 接入，以及 T2 一致性定律（Q1/Q2/Q5/S1/S2/S3/S6/S7）、IR golden 与三项解码器各 10 分钟本地 fuzz 验证。
  - 10 分钟本地 fuzz 统计：`codec/generatecontent` 的三个解码器目标以独立临时 `-test.fuzzcachedir` 各真实跑满 `-fuzztime=10m` 零崩溃（累计 `172,640,490` 次执行），其中 `FuzzRequestDecode` 运行 `601.51s` 执行 `81,525,626` 次、`FuzzResponseDecode` 运行 `601.29s` 执行 `44,219,498` 次、`FuzzStreamDecode` 运行 `601.28s` 执行 `46,895,366` 次；首轮 fuzz 捕获的 4 处同源合并 `Message.Extra`、降级调用配对 `LossToolLowered`、空白扩展键拒绝与 `gemcall_` 序号重算边界已全部转化为 `f.Add` 回归种子，审查修复后另以临时缓存目录执行 2 分钟三路回归（`9,795,449`、`11,207,607`、`11,598,461` execs）与 `FuzzRequestDecode` 复检（`843,416` execs）均零崩溃，仓库内无 `testdata/fuzz` 残留。
  - 局部决策：在 `tools/architecture/catalog.go` 为 `internal/protocol/codec/generatecontent` 登记 `leafDependencies`（`Production` 限定为自身、`protocol`、`protocol/ir`、`protocol/sse`、`protocol/gemini`、`protocol/wirejson`，`Tests` 开放 `protocol/testkit`）并在 `internal/protocol/convert` 加入 `codec/generatecontent`，同步在 `policy_test.go` 补充正反例断言及更新 `docs/architecture/{backend_modules,protocol_conversion}.md`；`codec/generatecontent` 拆为 `wire.go`、`schema.go`、`request_decode.go`、`request_encode.go`、`legalize.go`、`response.go`、`stream_decode.go`、`stream_encode.go`、`usage.go`、`finish.go` 共 10 个手写源文件（未达 15 文件阈值故不设 `doc.go`，配单测、三路解码 fuzz、`conformance_scenario_test.go` 与 91 个解码 IR golden）；在独立纯函数文件 `schema.go` 基于 `jsontext.Decoder` 保真数值令牌 `schemaNumber` 实现 `CleanGeminiSchema`（保留用户自定义属性名并将 64 位整数 `exclusiveMinimum` 转为更严格的 `minimum`）与 `CleanAntigravitySchema`（展开 `$defs`/`$ref`、合并 `allOf`/`anyOf`/`oneOf`、迁移约束提示至 `description`、为空小写对象注入 `reason` 并执行 7 键白名单与 `[undefined]` 清理）供阶段 4 复用；请求侧对缺省 `id` 的历史 `functionCall` 按步骤序号、调用序号、函数名与紧凑参数派生确定性 `gemcall_` 标识并在同源出站省略线级 `"id"`，合并 `System` 与 `system`/`developer` 至 `systemInstruction`，按 `(!inFunctionStep || !cand.unsigned)` 为每步首调用（含 `CallMemory` 已知无签名调用）及未知无签名并行调用补写 `PlaceholderToken`，保持编解码器对令牌方案与 `trc1:` 信封无感知（原样写出非空 `Token.Data`），在 `antigravity_claude` 下将无令牌历史思考块降级为可见文本并清空当轮 `thinkingConfig`，同时严格对齐 `ResolveGeminiThinking`（含 `"high"`/`default` 的 `10240` 预算、禁用思考的 `includeThoughts: false`、元数据守卫与 `EnsureMaxTokensGreaterThanBudget` 余量补足）及混合函数与内置工具的 `includeServerSideToolInvocations: true`；响应与流侧在上游省略 `functionCall.id` 时保留空 `CallID` 交由 `convert` 合成客户端 ID 并维持 `CallBinding.UpstreamID == ""`，`StreamDecoder` 延迟至 `Close(nil)` 发出 `EventResponseEnd` 以接纳 5 秒稀疏用量尾帧（经 `wirejson` 守卫后转为不相交 `ir.Usage`），将纯工具批次重发裁剪限定在紧邻连续完整工具帧，按 `closedByID` 抑制已闭合显式 ID 重发并经 `convert` 内部可选接口 `lateToolSignatureDrainer` 将迟到 `thoughtSignature` 刷入 `CallMemory` 与 `Buffer` 聚合响应，在缓冲 `DecodeResponse` 合并同显式 ID 分片参数与迟到签名，严格对齐 `NativeComputeGeminiTextDelta`、`buildGroundingText` 尾注格式以及相邻带签名思考/文本块的定律 S2 分块规则；`convert` 注册后 `mockupstream` 自动对 Gemini 请求启用 IR 解码与合成流式/缓冲回复。
  - 待确认项：无。

- 2026-11-24，阶段 3 第 2 步 Gemini 路线 T3/T4 定律与旧实现差分对账（批次 3.2，提交 `ff1e62de8`、`d49c3756b`、`72bd36c25`）：完成 `{Messages, Responses, Chat} -> Gemini` 三条路线的 T3/T4 一致性定律（Q3/Q4/Q5/S3/S4/S5/S6/S7/S8/M1）、未建模字段普查 `golden/census/gemini_generate_content.txt`、三条路线 `golden/loss` 与 `golden/diff/tool_names.json` 快照更新，以及与旧 Gemini 转换实现的全面差分对账（`diff_ledger.json` 的 `stage3_gemini` 作用域）。
  - `stage3_gemini` 台账分类统计与用量对账结论：`TestLegacyDiffStage3GeminiScenarios` 覆盖三条 Gemini 路线的全部线夹具、真实客户端录制语料、无令牌思考摘要合成流、工具标识符清洗夹具及有状态 `LookupToolCall` 对照 `CallMemory` 场景；新流解码器与 `Exchange.Stream` 的 `ir.Usage.ToTokenUsage()` 相对旧 `ReadGeminiStream` 加 `NativeExtractGeminiUsage` / `NativeUsageProjection` 累加器在全部上游流夹具上计费用量差分为零；`diff_ledger.json` 保持 `stage1_text` 全部 375 条记录零改动，新增 `stage3_gemini` 记录共 158 条（`D2` 66 条，含 `request` 54 与 `stream` 12；`D3` 67 条，含 `request` 1、`stream` 30 与 `stream_flags` 36；`D6` 14 条，均为 `stream`；`D7` 2 条，均为 `request`；`D8` 4 条，含 `request` 1 与 `tool_names` 3；`D11` 1 条，为 `request`；`D12` 4 条，均为 `stream_flags`），每条非 `D5` 请求豁免均强制校验实际命中差异路径（`hits > 0`），每条记录均通过第 6.2.4 节程序化接受条件校验。
  - 局部决策：跨协议发往 Gemini 时省略顶层 `"model"` 字段（同源 Gemini 请求经 `Unknown` 嵌入保留 `"model"` 以守住定律 Q2），标准 Gemini 将纯文本工具结果包裹为 `{"content":...}`（`SchemaAntigravity` 保持 `{"result":...}`）并直接透传非保留键 JSON 对象，在 `deriveNamelessCallID` 中规范化 JSON 参数键序，并在 `FunctionDeclaration` 丢弃工具 `Strict` 标志时记录 `LossSchemaApproximated("strict_dropped")`；`resolveEffectiveDialect` 为跨协议 Gemini 路线默认设置 `ToolSupportFunction | ToolSupportHostedWebSearch`、64 字节工具名上限、`HistoryNone` 与 `SchemaGemini`，在 `ToolMap` 的 `sanitizeTargetToolName` 对数字或连字符开头的跨协议工具名补前导 `'_'` 以满足 `GEM-TOOL-NAME` 且往返无损还原客户端原名；`openCarriers` 在跨协议标准 Gemini 请求上仅当 `CallMemory` 在当前重放方案下命中非空 `UpstreamID` 时保留线级 `functionCall.id` 与 `functionResponse.id`，否则以 `gemcall_` 哨兵省略客户端自造 ID 并保持调用序配对，与旧 `NativeGeminiOptions.LookupToolCall` 在显式 ID、无名带签名调用、无签名并行调用首步占位签名及乱序结果上零差异对齐；在 `ir/essence.go` 与 `testkit/essence.go` 补齐顶层 `service_tier` 的 `extra_dropped` 清理、`strict_dropped` 对齐、无签名调用首步占位签名剥离与关闭思考的 `includeThoughts: false` 提示对齐，并在 `multi_turn_scenario_test.go` 扩展 M1 覆盖 `trc1:gemini` 信封往返、`CallMemory` 跨轮签名与 `UpstreamID` 往返、迟到工具签名尾帧回填、无签名并行调用保持、400 兼容剥离重试与 `legacy_history`；Project Doc 经评估在本批无需改动，留待阶段 3 后续批次随 `googleforward` 接线一并更新。
  - 待确认项：无（原登记的 `P8` 至 `P10` 共 4 个 `pendingID` 已获用户裁决通过，正式纳入第 6.2.4 节 `D5`、`D12`、`D13` 并迁入 `diff_ledger.json`）。

- 2026-11-24，阶段 3 第 3 步 `gateway/session` 封装 `GeminiToolCalls` 实现 `convert.CallMemory`（批次 3.3，提交 `740ac7a7d`）：完成按请求的 API Key、提供商和映射后上游模型隔离作用域的 `GeminiToolCalls.CallMemory` 视图实现及 `-race` 单元测试与 `convert.NewExchange` 往返验证。
  - 局部决策：保持现有 `GeminiToolCalls.Remember` 与 `Lookup` 签名和旧路径行为不变，在 `*GeminiToolCalls` 上新增 `CallMemory(scope GeminiToolScope) convert.CallMemory`，与既有缓存共用同一互斥锁、FIFO 顺序、1 小时有效期、`16384` 条目上限和 `32 MiB` 字符串字节预算（计入 `scope.Model`、`clientID`、`upstreamID`、`signature` 和 `scheme`），零值 `GeminiToolCalls` 直接可用，接收者为 `nil` 或 `scope` 缺少正整数 `APIKeyID`、`ProviderID` 或非空 `Model` 时直接返回无类型 `nil` 供后续 `googleforward.Gemini` 与 `googleforward.Antigravity` 无条件调用；`geminiToolEntry` 按值保存 `scheme`、`tokenProviderID`、`signature` 与 `hasToken`（将 `Token == nil` 与空 `Data` 统一记为无签名），`Recall` 对有签名调用返回独立拷贝的 `&ir.ReplayToken{Scheme, ProviderID, Data}`（覆盖 `gemini`、阶段 4 `antigravity_claude` 与账号绑定方案），对已观测无签名调用返回 `(CallBinding{UpstreamID: ..., Token: nil}, true)` 以区分 `Unsigned = true` 与缓存未命中；支持 `applyLateToolSignatures` 二次调用以及二次 `Remember` 仅携带新令牌且 `UpstreamID` 为空时在锁内保留未过期条目的原非空 `UpstreamID`，并在替换前先校验 `32 MiB` 上限、扣减旧条目字节计数且拒绝过期条目串用旧 ID，同时增加可选测试时钟钩子 `now` 确定性覆盖 1 小时 TTL 边界；同步更新 `gateway/session/doc.go` 中 `gemini_tool_calls.go` 的单行说明，Project Doc 经评估在本批无需改动。
  - 待确认项：无。

- 2026-11-24，阶段 3 第 4 至 5 步 Gemini 网关转换入口与读取函数合并及 `count_tokens` 与文档补充（批次 3.4，提交 `e8fb68638`、`8ad747641`）：完成 `UseIRConversion` 开关下 `googleforward`、`unified_text_executor`（含 OpenCode Gemini）、`upstream/gemini` 与 `/v1/messages/count_tokens` 的合并转换接线，同步更新 `docs/architecture/protocol_conversion.md` 并核验 `docs/architecture/backend_modules.md`。
  - 局部决策：保持生产装配 `UseIRConversion == false` 时旧桥接路径与 `ForwardNative` 原样不变，并在 `textattempt.ForwardPorts` 暴露 `UseGeminiIRConversion` 使 `geminiMessageAttemptBridge.Forward` 仅在开关开启时对 `IsModelAttributeError` 跳过 `reportSchedule`；请求侧新增 `googleforward.Gemini.ForwardConverted` 与 `UnifiedTextExecutor.forwardGemini` 开关分支（单跳兼容带顶层 `input` 与 `reasoning_effort` 的 Cursor Chat 请求，Responses 入口调用无损版 `NormalizeOpenAIResponsesLegacyIngressIR`），按 `(APIKeyID, ProviderID, mappedModel)` 注入 `session.GeminiToolCalls.CallMemory(scope)`，保留编码后的 AI Studio `NativeNormalizeGeminiRequestForAIStudio`、Code Assist OAuth `v1internal` 包装、Vertex `service_account` URL 路由与 OpenCode Bearer/`X-OpenCode-Session` 头适配，客户端语法、空模型与不支持能力错误按客户端协议返回 HTTP 400（`CommittedSSE` 为真时发无计时终态 SSE 错误帧）与 `MODEL_CAPABILITY_UNSUPPORTED`；上游返回签名相关 HTTP 400 时，在 `upstream/gemini` 的 `ExchangeConverted` 中通过 `RepairToolReplay` 以 `Dialect.ReplayScheme = ""` 且不传 `Calls`（保留 `Dialect.StripEffort` 与 `Dialect.PlaceholderToken`）重建兼容请求同号重试一次，响应侧继续复用原 `Exchange` 封装新响应令牌并写入 `CallMemory`；响应侧新增 `ReadConvertedStreaming` 与 `ReadConvertedBuffered`（全部新增 JSON 辅助逻辑采用 `jsontext` 与 `encoding/json/v2` 私有结构体，零 `gjson`/`sjson`/`map[string]any`），在 `ConvertedResponse` 模式下使用 `NewDeferredOutputContext` 暂存前导非语义帧直至首个 `Semantic == true` 帧或合法完成终态到达（空内容合法流在 `c.Complete()` 前先提交 SSE 头），观测到 `finishReason` 后调 `c.Complete()` 停发保活并继续等待最多 5 秒收集稀疏 `usageMetadata` 尾帧、待 `ReadGeminiStream` 返回后再调 `Stream.Close(nil)` 发出最终 `usage` 与 `response_end`，对 `CommittedSSE` 为真时的 HTTP `>= 400`（含 Anthropic Messages `event: error` 帧及 `beginGoogleError` 去重）与流中错误均执行单次健康策略评估和无计时终态 SSE 错误输出，并在已启动流异常中断时优先经 `WriteRawTerminalSSE` 发送 `Stream.Close(cause)` 生成的连续序号终态帧；为 `httpapi.CountTokensHandler` 补齐 `UseIRConversion`（生产默认 `false`，`newProtocolLabRouter` 按 `opts.UseIRConversion` 透传），在 `count_tokens_selected.go` 以清空 `ReplayScheme` 与 `PlaceholderToken` 的 `Exchange.UpstreamRequest` 经 `jsontext` 无损包装 `generateContentRequest` 并保留 OAuth `insufficient_scope` 本地估算回退，同时在 `tools/architecture/catalog.go` 开放 `internal/upstream/gemini` 测试集对 `internal/protocol/testkit` 的依赖。
  - 待确认项：无。

- 2026-11-24，阶段 3 第 6 步与阶段 3 收尾对账（批次 3.5，提交 `ec01eff20`、`0625a77c9`）：完成 `UseIRConversion` 开关下 T5 网关路由测试的 Gemini 全量场景覆盖、T6 客户端实验室全部 Gemini 上游格子（基础与 5.7 节全部 7 类扩展场景）全绿验证，以及阶段 3 旧基线 395 格与 62 条失败缺陷逐条对账报告 `stage3_baseline_resolution.json` 落盘。
  - 阶段 3 旧基线缺陷逐条对账统计（`backend/internal/app/testdata/golden/stage3_baseline_resolution.json`）：`TestProtocolLabStage3BaselineResolution` 对账 `legacy_baseline.json` 中阶段 3 Gemini 上游范围共计 395 格（与阶段 2 的 1,474 格互补覆盖 `legacy_baseline.json` 全部 1,869 格；阶段 3 旧基线通过 257、跳过 76、失败 62），同时核验纯矩阵全部三条 Gemini 转换路线（`messages_to_gemini`、`responses_to_gemini`、`chat_to_gemini`）零失败，阶段 3 新路径回归 0 格、跨协议转换路线遗留失败 0 格；全部 62 条旧 `fail` 缺陷逐条核验结果均写入 `stage3_baseline_resolution.json`，其中 `fixed` 61 条（纯转换矩阵 `entries` 99 格中的 23 条全数修复，T5 网关路由 `gateway_routes` 282 格中的 39 条修复 38 条，T6 客户端实验室 `client_lab` 14 格全部保持通过无回归），保留 `still_present` 1 条（同协议原生透传 `gemini_via_gemini` 上的 `disconnect_after_output|T5_MIDSTREAM_DISCONNECT`，按第 2.2 节与第 6.4 节第 4 步保持 `ForwardNative` 不经过 `convert` 且 `CompletePartialFailure` 仍为 `false`，已记录核实原因）。
  - 局部决策：保持 `UseIRConversion == false` 旧路径、`protocolLabProviderKinds` 与 `legacy_baseline.json` 零改动，且对非 `toggle` Gemini 模型收到显式关闭思考控制（`thinking.type="disabled"`、`reasoning.effort="none"`、`reasoning_effort="none"`）严格保持与旧 `bridge.ResolveGeminiThinking` 一致的 HTTP 400 `"selected model does not support disabling thinking"` 本地拒绝且不扣调度健康分；在 `helpers_test.go` 为 `newProtocolLabRouter` 的 `geminiRuntime` 补齐测试 `GeminiTokenSource` 并将 Gemini `oauth` 纳入 `localEndpointOverride`，使 Code Assist `v1internal` 与 Vertex `service_account` 均落入本地 `httptest.Server`；在 `protocol_lab_scenario_test.go` 新增 `openCodeGeminiLabSpec`（选用内嵌目录已登记 `canonical_id: "google/gemini-3-flash-preview"` 的 `gemini-3-flash` 并在 `makeLabProviderSpec` 注入 `opencode_model_protocols` 以解析 Google 作者身份签发 `trc1:gemini:`）、`vertexGeminiLabSpec`、`vertexGeminiFlashLabSpec` 与 `TestProtocolLabIRGeminiUpstreamMatrix`，覆盖 Google Gemini（API Key、Vertex `service_account`、Code Assist OAuth 流转缓冲折叠）、OpenCode Gemini 三条转换路线与 `/v1/messages/count_tokens`（含 OAuth `insufficient_scope` 本地估算）的 33 个规范场景、客户端与结算用量一致性、5 秒窗口稀疏 `usageMetadata` 尾帧合并、首字节前与 HTTP 200 SSE 前导帧暂存切号、中途断流闭块收尾、签名 400 同号兼容重试（保留 `StripEffort`、64 位大整数与新响应令牌封装）、4 类 `CallMemory` 双轮绑定、Cursor 顶层 `input` Chat 单跳直达 Gemini 以及混合分组转换与原生双向隔离（含 Bedrock EventStream、Vertex、OpenCode、passback、Grok 与 Responses WS）；在 `client_lab_scenario_test.go` 新增 `TestClientLabIRGeminiUpstreamScenarios` 与 `TestClientLabIRStage3GeminiExtendedScenarios`，覆盖全部 7 个 Gemini CLI 格子（`codex`、`claude_bare`、`claude_default`、`pi_chat`、`pi_responses`、`pi_messages` 与原生 `pi_gemini`）及 Vertex、OpenCode Gemini 的基础与 5.7 节全部 7 类扩展场景（非推理子用例将注入关闭思考的 CLI 配对支持 `Toggle: true` 的 `gemini-2.5-flash`，开启推理的 `lab_reasoning_tool` 与 `lab_switch_provider` 覆盖非 `toggle` 的 `gemini-2.5-pro` 与 `gemini-3-flash` 并校验第 2 轮 `CallMemory` 真实签名回放，合成流使用 4 字节对齐合法 Base64 `CpEB` 签名并在末帧附完整 `usageMetadata` 以兼容 `@earendil-works/pi-ai` 原生客户端，`lab_failover` 在 Google Gemini 主账号配置 `529` `temp_unschedulable_rules` 保持真实 HTTP `529` 并首跳切号）；Project Doc 经评估在本批无需改动。
  - 待确认项：无（阶段 3 原登记的 `P8` 至 `P10` 已获用户裁决通过，正式迁入第 6.2.4 节与 `diff_ledger.json`）。

- 2026-11-24，阶段 4 第 1 步 Antigravity `ConversionDialect` 能力映射与 `CallMemory` 挂载（批次 4.1，提交 `0be399120`、`5a427bda1`）：完成 `gateway/provider/model_attributes.go` 中 Antigravity 全模型族与前缀变体的 `ConversionDialect` 推导、`convert/carriers.go` 在 `SchemaAntigravity` 下的 `CallMemory` 调用 ID 与方案守卫，以及 `googleforward.Antigravity` 挂载共享 `session.GeminiToolCalls` 与装配单测验证。
  - 局部决策：保持 `UseIRConversion == false` 时 `provider/modelrules` 与旧 Antigravity 转发路径原样不动，在 `ConversionDialect` 内部通过 `normalizeAntigravityModel` 对 2 段受支持前缀（`models`、`anthropic`、`google`、`gemini`、`google-vertex`、`google-vertex-anthropic`、`antigravity` 且末段非空）、4 段 `publishers/{google|anthropic}/models/{name}` 和 8 段 `projects/{non-empty}/locations/{non-empty}/publishers/{google|anthropic}/models/{name}` 执行单趟精确段数匹配（未知厂商 publisher、嵌套自定义路径、叠加前缀与含空段路径保持原样不落入 `claude-*` 或 `gemini-*` 规则），在缺省 `UpstreamProtocol` 时推导为 `gemini_generate_content` 并产出 `SchemaDialect: convert.SchemaAntigravity`；补齐与 `GeminiThinking` 对齐的 `Reasoning`（`claude-*` 设 `ForceBudget: true`，Opus 4.6/4.7/4.8 及其 `-thinking`/`@version` 变体设 `AdaptiveBudget: 24576`，转换路径设 `BudgetPadding: 1000` 并按 `claude-*`/Chat `64000`、其余转换路径 `65000` 收口 `OutputLimit` 同时保留更小目录上限与 `"config"`/`"provider"` 覆盖，原生 `gemini_generate_content` 客户端保持目录上限与 `0` padding），补齐带前缀 Gemini 推理变体的采样与 `stopSequences` 抑制及 `models/` 前缀的 `StripEffort` 二次匹配，并按模型族拆分 `claude-*`（`ReplayScheme: "antigravity_claude"`，`PlaceholderToken: ""`）与 `gemini-*`（`ReplayScheme: "gemini"`，`PlaceholderToken: bridge.DummyThoughtSignature`，`count_tokens`/`countTokens` 下两者均置空）；在 `convert/carriers.go` 新增 `callBindingMatchesRoute` 拦截 `Dialect.ReplayScheme == ""`（`count_tokens` 或兼容重试）及异族方案的 `CallMemory` 旧绑定，并在 `SchemaAntigravity` 下对未命中或 `UpstreamID == ""` 的调用保留客户端 `functionCall.id` 与 `functionResponse.id`、对命中不同 `UpstreamID` 的调用同步重写并记录 `LossIDRewritten`；在 `internal/app` 新增 `provideGeminiToolCalls` 注入 `provideGeminiForward` 与 `provideAntigravityForward`（同步更新 `wire_gen.go` 与 `newProtocolLabRouter`），使 `Gemini` 与 `Antigravity` 共用单一进程级 `16384` 条 / `32 MiB` 上限并保留结构体内嵌零值 `toolCalls` 兜底，同时暴露 nil 安全的 `(*Antigravity).UseIRConversion()` 与 `(*AntigravityExecutor).UseIRConversion()`；Project Doc 经评估在本批无需改动。
  - 待确认项：无。

- 2026-11-24，阶段 4 第 2 步 Antigravity 三个请求入口合并与双基准全语料差分（批次 4.2，提交 `0b7dcdf65`、`89e3cae1b`、`ed5a06999`、`1cd2e61b9`）：在 `UseIRConversion` 接线开关后将 `antigravity_messages.go` 与 `antigravity_openai.go` 的三个请求入口合并为 `googleforward.Antigravity.ForwardConverted`，补齐 Antigravity 两模型族方言的 T3/T4 定律覆盖，并同时对照 `TransformClaudeToGeminiWithOptions` 与 `NativeConvertClaudeMessagesToGeminiGenerateContent` 完成六分组全语料双基准请求差分与待裁决表落盘。
  - 局部决策：保持 `UseIRConversion == false` 时旧请求转发路径、`ForwardGemini` 原生透传与 `textattempt` 调度扣分原样不变（`antigravityMessageAttemptBridge.Forward` 仅在 `UseAntigravityIRConversion` 为真时对 `IsModelAttributeError` 跳过 `reportSchedule`）；在 `UnifiedTextExecutor.forwardAntigravity` 与 `googleforward.Antigravity` 中将 Messages、Responses（先经无损 `NormalizeOpenAIResponsesLegacyIngressIR` 归一化）与 Chat（含无 `messages` 带顶层 `input` 的 Cursor 请求单跳直达，无需经过 Responses 中转）统一收敛至 `ForwardConverted`，按 `(APIKeyID, ProviderID, mappedModel)` 绑定 `GeminiToolCalls.CallMemory` 并用 `ConversionDialect` 构造 `convert.NewExchange`；在 `upstream/antigravity/payload.go` 新增基于 `jsontext` 与 `encoding/json/v2` 的无损 `InjectIdentityPatchToGeminiRequestWithPrompt` 承接每请求读取的 `s.Gateway.GetIdentityPatchPrompt`，编码后依次执行身份补丁、`CleanGeminiRequest` 与 `WrapV1InternalRequest`，对空模型、非法 JSON、`"ultra"` 档位、非 OAuth 提供商、模型白名单拒绝及 IR 能力拒绝返回客户端协议 HTTP 400/403（`CommittedSSE` 为真时经 `ConvertedOutput.MapAntigravityFailure` 执行错误体日志与 Messages `ApplyErrorPassthroughRule` 后改发无计时终态 SSE 错误帧）与 `MODEL_CAPABILITY_UNSUPPORTED`，同时保留 Messages `PromptTooLongError` 分组回退、Google 项目配置 400 可重试切号、OpenAI 401 凭据失败元数据与粘性会话缓存计费传递；在 `protocol/convert` 扩展 `conformance_scenario_test.go` 与 `multi_turn_scenario_test.go` 覆盖 Antigravity `claude-*` 与 `gemini-*` 两方言变体的定律 Q3、Q4、Q5 与 M1，在 `tools/architecture` 开放 `internal/upstream/antigravity` 测试集导入 `internal/protocol/testkit` 并为 `testkit.VerifyEssence` 补齐 `promoted_google_search` 与 `duplicate_web_search` 校验，在 `upstream/antigravity/dual_baseline_diff_scenario_test.go` 按 `{Messages, Responses, Chat}` 乘 `{claude-*, gemini-*}` 六分组对共 `248` 次评估同步执行定律 Q3/Q4 校验与精确 JSONPath 叶子加值谓词归类（不预先归入 `testdata/diff_ledger.json`，未归类差异 `0` 条，锁定于 `testdata/golden/stage4_dual_baseline_diff.json`），同步更新 `docs/architecture/protocol_conversion.md` 的 `#gateway_integration` 章节。
  - 待确认项：无（原登记的 `AG1` 至 `AG11` 共 11 项双基准差异已获用户裁决全部采用选项 A，正式纳入第 6.2.4 节 `D14` 并迁入 `diff_ledger.json` 与 `stage4_dual_baseline_diff.json`）。

- 2026-11-24，阶段 4 第 3 步 Antigravity 响应读取合并、`recovery_claude` IR 恢复与 `probe.go` 预备（批次 4.3，提交 `337d84569`、`d50447d19`、`7b64275d8`）：在 `UseIRConversion` 接线开关后新增 `upstream/antigravity` 合并流式与缓冲响应读取器并在 `execute.go` 与 `googleforward/antigravity_converted.go` 接通 `ModeConvertedResponse`，补齐 `RecoverClaudeIR` 与 `RecoverGemini` 转换恢复链路，在 `probe.go` 预备 `BuildClaudeTestRequestIR`（生产无开关调用点继续调旧 `BuildClaudeTestRequest`），清理批次 4.2 两项测试审查尾项，并同步更新 `docs/architecture/protocol_conversion.md`。
  - 局部决策：保持 `UseIRConversion == false` 时旧 `response_stream.go`、`response_compat_stream.go`、`response_compat_nonstream.go`、`RecoverClaude` 与 `BuildClaudeTestRequest` 原样不变；在 `upstream/antigravity/response_converted.go` 新增 `ReadConvertedStreaming` 与 `ReadConvertedBuffered`（全部新增 JSON 辅助逻辑采用 `jsontext` 与 `encoding/json/v2` 私有结构体，零 `gjson`/`sjson`/`map[string]any`），经 `wirejson` 守卫校验外层 `v1internal` 与内层 `response` / `usageMetadata` 无歧义后无损解包 `{"response": ...}` 并合并外层 `responseId` 与 `modelVersion`，按模型族 `Dialect.ReplayScheme` 自动封装 `trc1:antigravity_claude` 或 `trc1:gemini` 连续性信封并将工具调用绑定写入 `(APIKeyID, ProviderID, model)` 作用域 `CallMemory`；完整保留 `ReadGeminiStream` 扫描、转换前 `UpstreamResponseModel` 观测、原计费投影路径、观测到 `finishReason` 后立即 `c.Complete()` 停发保活并在最多 5 秒尾帧窗口结束后再调 `Stream.Close(nil)`、客户端断开后继续排空计费用量、debug 级别记录 `Losses()` 计数，以及 3.13 节流控不变量（`NewDeferredOutputContext` 与前导非语义帧本地暂存窗口，`ctx.Err() == nil` 时包裹 `context.DeadlineExceeded` 的出站 `GeminiStreamError` 正常进入单次故障分类以支持前导帧切号与错误写出，执行上下文取消或超时与上游失败在 `CommittedSSE` 为真或流输出已启动时一律经 `WriteRawTerminalSSE` / `WriteLocalTerminalError` 发送无计时终态 SSE 错误帧）；在 `protocol/convert/exchange.go` 新增 `(*Exchange).ReencodeUpstream` 基于深拷贝 `ir.Request` 与 `Dialect` 无损重编上游请求且保留原 `ToolMap` 与 `ReplayScheme`，供 `recovery_claude.go` 的 `RecoverClaudeIR` 依次执行仅剥离思考、剥离思考加工具文本降级及非 `adaptive` 思考预算修正后经 `InjectIdentityPatchToGeminiRequestWithPrompt`、`CleanGeminiRequest` 与 `WrapV1InternalRequest` 重试；`gemini-*` 沿用 `RecoverGemini` 执行基于 `jsontext` 的内层 `thoughtSignature` 占位替换（同模型恢复保持原 `Exchange`）与 404 备用模型回退（回退成功后将 `target.Converted.Exchange` 切换为备用模型的 `Exchange` 以对齐 `ReplayScheme` 与 `fbScope` `CallMemory`）；在 `probe.go` 新增 `BuildClaudeTestRequestIR` 并通过新旧对照单测核验，同时在 `tools/architecture` 登记 `upstream/antigravity` 新手写文件的 `protocol` 依赖许可。
  - 待确认项：无（阶段 4 原登记的 `AG1` 至 `AG11` 已获用户裁决全部采用选项 A，正式纳入第 6.2.4 节 `D14`）。

- 2026-11-24，阶段 4 第 4 步 Antigravity T5 实验室夹具、全场景矩阵验证与新旧路径对账（批次 4.4，提交 `5e600a9ad`、`6f27c8aad`）：在 `internal/app/helpers_test.go` 打通内存态 Antigravity OAuth 凭据源、`project_id` 回填与本地 `v1internal` 测试上游夹具，在 `internal/app/protocol_lab_scenario_test.go` 完成 `TestProtocolLabIRAntigravityUpstreamMatrix` 与 `TestProtocolLabStage4AntigravityLegacyVsIR`（黄金快照锁定于 `testdata/golden/stage4_antigravity_legacy_vs_ir.json`），并同步补充 `docs/architecture/protocol_conversion.md`。
  - 阶段 4 Antigravity 新旧路径逐格对账统计（`backend/internal/app/testdata/golden/stage4_antigravity_legacy_vs_ir.json`）：`TestProtocolLabStage4AntigravityLegacyVsIR` 对 `{Messages, Responses, Chat}` 乘 `{claude-*, gemini-*}` 共 6 个组合乘 71 格（总计 426 格）同时跑通 `UseIRConversion == false` 与 `UseIRConversion == true`；旧路径通过 256 格、跳过 84 格、失败 86 格，新路径通过 342 格、跳过 84 格、失败 0 格，86 个旧路径失败项在新路径下 100% 解决（`fixed = 86`、`still_present = 0`、`regressed = 0`），其中 `messages_via_antigravity_{claude,gemini}` 各修复 5 格、`responses_via_antigravity_{claude,gemini}` 各修复 35 格、`chat_via_antigravity_{claude,gemini}` 各修复 3 格。
  - 局部决策：保持阶段 0 `protocolLabProviders` 与 `legacy_baseline.json` 原样不变，通过局部 `antigravityLabProviderSpecs`（对 `claude-sonnet-4-5` 同时白名单登记 `claude-sonnet-4-5-thinking` 后缀变体，规范矩阵 `T5_FAILOVER` 选用无重试睡眠的 HTTP `401`）与 `antigravityDedicatedMessagesClient`（`/antigravity/v1/messages`）组织测试；在 `helpers_test.go` 新增 `protocolLabSettingsStore`、`protocolLabAccessTokenCache`、绑定 `FillProject` 与 `Persist` 的 `AntigravityTokenSource`、覆盖 Antigravity `oauth` 的 `localEndpointOverride`、向 `provideMessageHTTPBindings` 补齐 `errorRules` 接线，以及基于流式 `jsontext.Decoder` 无损解包请求并保留 SSE 怪癖与 `Fault.TruncateAfterFrames` 底层 TCP 断连的 `protocolLabAntigravityUpstream`；`TestProtocolLabIRAntigravityUpstreamMatrix` 在 `UseIRConversion: true` 下完整覆盖专用与通用四个入口乘两模型族的全部 33 个规范场景、流转缓冲折叠与 `ForcePlatform=antigravity`、5 秒窗口稀疏 `usageMetadata` 尾帧合并、结构化 `429` `RetryInfo` 与项目配置 `400` 及 HTTP 200 SSE 前导非语义帧暂存切号、输出开始后断流与 `DeadlineExceeded` 超时及 `CommittedSSE` 终态错误与 `ApplyErrorPassthroughRule` 覆盖、客户端 400 能力拒绝零调度扣分、`RecoverClaudeIR` 三级同号恢复与 `RecoverGemini` 签名替换及 404 备用模型 `Exchange` 切换跨轮 `CallMemory` 续聊与跨模型族信封封装、多轮 `CallMemory`、Cursor 顶层 `input` Chat 单跳直达、运行时 `identity_patch_prompt` 实时更新，以及混合分组 `trc1:antigravity_claude` 与 `trc1:anthropic` 双向隔离和 `trc1:gemini` 与标准 Gemini 双向复用及落入原生 Messages/Responses（HTTP 与 WS）安全剥离；同步在 `docs/architecture/protocol_conversion.md` 的 `#reasoning_continuity` 补充 Antigravity 两模型族信封方案与跨上游复用和隔离规则。
  - 待确认项：无（阶段 4 原登记的 `AG1` 至 `AG11` 已获用户裁决全部采用选项 A，正式纳入第 6.2.4 节 `D14`）。

- 2026-11-24，阶段 5 第 1 步同协议 Responses 客户端工具轻量适配与旧实现差分验证（批次 5.1，提交 `2cf98954b`、`6b0232ff8`）：核对补齐 `protocol/convert` 的 `NeedsResponsesToolAdaptation`、`AdaptResponsesTools`、`RestoreResponsesTools` 与 `NewResponsesToolStream`，完成全量录制 Codex/pi 请求语料无损扫描预检、WS 预热跨轮续接验证，以及对照旧 `bridge` 同协议客户端工具适配实现的差分对账（`diff_ledger.json` 的 `stage5_responses_tools` 作用域与 `testdata/golden/diff/tool_names.json`）。
  - `stage5_responses_tools` 台账分类统计与语料预检结论：`TestLegacyDiffStage5ResponsesToolsScenarios` 与 `TestRecordedResponsesCorpus_LosslessPrecheckAndFieldPreservation` 覆盖全部 `wire/openai_responses` 夹具、真实客户端录制语料、WS 首轮 `generate: false` 预热及后续省略 `tools` 与 `model` 续接、显式 `tools: []` 重置、空命名空间、`allowed_tools` 命名空间组展开、命名空间内嵌 `custom` 工具与托管工具中间流事件；全部流夹具计费 `TokenUsage` 差分为零，无需降级请求与剥离可降级声明后的 Codex 录制语料 100% 逐字节不变；`diff_ledger.json` 保持 `stage1_text`（375 条）与 `stage3_gemini`（158 条）零改动，新增 `stage5_responses_tools` 记录共 18 条（`D2` 3 条，含 `request` 1 与 `stream` 2；`D3` 11 条，含 `request` 4 与 `stream_flags` 7；`D8` 3 条，含 `request` 1 与 `tool_names` 2；`D12` 1 条，为 `stream_flags`），并将 `responses_same_protocol_tools` 工具名映射报告同步写入 `testdata/golden/diff/tool_names.json`（总计 39 个用例，差异用例 8 个）。
  - 局部决策：请求侧以 `jsontext.AllowInvalidUTF8(true)` 与有序成员切片就地改写 `tools`、`additional_tools`、`tool_choice` 及需降级的历史调用与输出条目，其余顶层与条目级字段（`prompt_cache_breakpoint`、`compaction`、裸 `encrypted_content`、64 位大整数与托管工具声明）原样保留；引入 `rawTopToolEntry` 与 `rawToolOrigin` 按外层槽位跟踪源字节，在 `Dialect` 支持 `ToolSupportNamespace` 时对空命名空间与多子工具命名空间保持逐字节不变，并在 `ToolMap.ScopedCustom` 与 `LookupCustomCall` 中支持保留外层 `namespace` 包装下的组内 `custom` 子工具降级、同组冲突检测与跨轮 `LoweredTools` 还原；提取 `additional_tools` 时记录原始 `origInputIndices` 保持 `LossToolLowered` 源坐标，兼容 Grok Compact 单对象 `input` 形态，继承 `DeclaredNamespaces` 与默认 `"functions"` 作用域函数条目，并在 `tool_choice.allowed_tools` 中将已声明 `"namespace"` 展开为组内扁平化函数引用（空组无其他条目时降级为 `"auto"`）；响应与流侧在 `RestoreResponsesTools` 覆盖 `output`、`response.output`、`data.output` 与 `data.response.output` 四种包络，在 `NewResponsesToolStream` 合成省略 `function_call_arguments.done` 时的 `custom_tool_call_input.delta/done`、保持无 `sequence_number` 的 `ping` 帧与非 JSON 对象 `data:` 保活行透传且不断号，并对未发前置 `Semantic=true` 增量的单帧 `response.output_item.done`（含 `upstream_sse_quirks.sse`）及非空托管工具预览与参数增量标记 `Semantic=true`；Project Doc 经评估在本步无需改动（`docs/architecture/protocol_conversion.md` 已有对应轻量适配说明，网关开关状态留待第 2 至 3 步接线后同步）。
  - 待确认项：无（前序阶段原登记的 `P8` 至 `P10` 与 `AG1` 至 `AG11` 均已获用户裁决通过并正式纳入第 6.2.4 节与 `diff_ledger.json`）。

- 2026-11-25，阶段 5 第 2 至 3 步同协议 Responses 客户端工具降级四个调用点与响应还原接线（批次 5.2，提交 `9328542df`、`caa6c973c`、`e3ecd838f`）：在 `UseIRConversion` 接线开关后完成四个同协议 Responses 请求侧客户端工具降级调用点（`openaiforward/passthrough.go`、`openaiforward/prelude.go`、`grokforward/responses.go` 与 `gateway/ws/httpapi/openai_ws_http_bridge.go`）及流式与缓冲响应侧还原逻辑的接入，补齐 `TestProductionGatewayProvidersKeepUseIRConversionDisabled` 装配断言与各包 `-race` 单元测试，并同步更新 `docs/architecture/protocol_conversion.md`。
  - 局部决策：保持 `UseIRConversion == false` 时四个请求调用点、流式包装与缓冲还原分支逐行不变，并在 `requeststate.ResponseTools` 与 `httpapi/response_tool_restoration.go` 中保留旧 `ResponsesClientToolMapping` 与 `CodexToolNameReverse`、原生命名空间扁平化字段不变，并排新增深拷贝隔离的 OpenAI、Grok 与 WS HTTP 桥 `*convert.ToolMap` 槽位；新增 `upstream.NewResponsesToolStreamBody` 基于管道封装 `convert.NewResponsesToolStream`，支持流结束前逐帧实时刷新、过滤注释与非错误空载荷及 JSON 心跳帧、将多行 JSON 无损压缩为单行 `data:` 帧、透传裸 `event: error` 控制帧与提前关闭唤醒，同时在 `isPermissiveJSONObject` 开启 `jsontext.AllowDuplicateNames(true)` 使重复键流事件进入 `parseOrderedObject` 正常报错；在 `ToolMap` 新增带作用域守卫的 `IsToolSearchProxyCall(namespace, upstreamName)` 并在 `tools_responses.go` 与 `tools_raise.go` 限制仅对空或默认 `"functions"` 命名空间的 `function_call` 执行 `tool_search` 代理与命名空间展开，保证 Grok 流式经 `NewResponsesToolStreamBody` 后再走逐帧 `RestoreResponsesTools` 兜底回调时逐字节幂等；缓冲透传路径在 `RestoreOpenAIResponsesClientToolPayload` 的 IR 分支完成命名空间展开后对非空 `CodexToolNameReverse` 重跑幂等的别名还原，使命名空间内的 `python__tokenrouter` 在缓冲透传下同样还原为 `python`；`openaiforward`（`IRClientToolPassthroughPorts`、`IRClientToolPreludePorts`）与 `grokforward`（`IRClientToolPorts`）采用可选接口扩展，保持 OpenAI API Key 透传在兼容归一化之后、`PrepareNativeReplay` 之前运行，DeepSeek 等兼容 Responses 前奏降级后继续由 `transform.go` 剥离顶层 `prompt_cache_options`，Grok `PatchGrokResponsesBodyWithToolMap` 保留 `additional_tools` 提升、线级字段剥离及紧随其后的 `in.Compact` 摘要回合构造，四个调用点的客户端工具冲突与非法声明统一写本地 HTTP 400（`param: "tools"`）并包装 `MODEL_CAPABILITY_UNSUPPORTED`（WS 映射为 `1008` 策略关闭）以跳过外层切号和调度健康扣分；WS HTTP 桥在 Grok 伴生工具注入前快照会话 `ToolMap`（纯 `function` 工具轮经 `jsontext` 无损提取 `LoweredTools`，显式 `tools: []` 或 `null` 置 `nil`），延迟至 `generate: false` 本地预热写完或真实流下游写出与非切号结算时才写入连接状态，避免首轮 `additional_tools` 切号重复声明并保留跨轮 HTTP 429 切号前的上轮继承映射。
  - 待确认项：无（前序阶段原登记的 `P8` 至 `P10` 与 `AG1` 至 `AG11` 均已获用户裁决通过并正式纳入第 6.2.4 节与 `diff_ledger.json`）。

- 2026-11-25，阶段 5 第 4 步与阶段 5 收尾对账（批次 5.3，提交 `52b01a518`、`affe85eca`、`c1c273522`）：完成 `UseIRConversion` 开关下同协议 Responses 客户端工具降级的 T5 全场景矩阵验证（`protocol_lab_stage5_scenario_test.go`）、T6 真实 Codex CLI `lab_custom_tool` 在 HTTP 与 WebSocket HTTP 桥路线上的脚本验证及双轮录制语料脱敏入库、全量录制 Codex 请求语料无损验证，以及阶段 5 新旧路径 314 格逐条对账报告 `stage5_responses_tools_legacy_vs_ir.json` 落盘。
  - 阶段 5 新旧路径逐格对账统计（`backend/internal/app/testdata/golden/stage5_responses_tools_legacy_vs_ir.json`）：`TestProtocolLabStage5ResponsesToolsLegacyVsIR` 对四条 HTTP 同协议客户端工具降级路线（`responses_via_{openai_apikey_passthrough,grok_oauth,grok_apikey,deepseek_responses}`，各 74 格共 296 格）与三条 Responses WebSocket HTTP 桥路线（`responses_ws_via_{openai_apikey_passthrough,grok_oauth,grok_apikey}`，各 6 格共 18 格）总计 314 格配对运行 `UseIRConversion == false` 与 `UseIRConversion == true`；旧路径通过 237 格、跳过 24 格、失败 53 格，新路径通过 271 格、跳过 24 格、失败 19 格，新路径回归 0 格、客户端工具适配遗留缺陷 0 格（`regressed = 0`、`client_tool_defects_remaining = 0`）；53 条旧路径失败项中 `fixed = 34`（涵盖纯命名空间请求漏判、命名空间内嵌 `custom` 工具在 HTTP 缓冲/流式与 WS 桥下的降级还原、省略 `function_call_arguments.done` 的 `custom` 完成帧合成、Grok WS 首轮预热 `additional_tools` 次轮续接继承、WS 首包前切号隔离、工具名冲突本地 HTTP 400 与 WS `1008` 零切号零扣分门禁，以及 `trc1:` 原生重放解封清理），保留 `still_present = 19`（均为第 2.2 节明确不经过 `convert` 的无降级工具原生 SSE 帧收尾及非 OpenAI 平台 `OpenAIResponseOutput.ResponseError` 默认 HTTP `502` 状态掩码既有行为，均已按实际调用链记录核实根因与结论）。
  - 局部决策：修复 T5 新路径实测捕获的 Grok WS HTTP 桥预热续接边界问题，在 `openai_ws_http_bridge.go` 中将适配前的 `SanitizeGrokResponsesInput` 限定于 `!s.UseIRConversion` 分支（防止首轮预热重放的 `additional_tools` 提前生成顶层 `tools` 覆盖继承映射），并在 `convert/tools_responses.go` 的 `appendInheritedAdditionalTools` 中对省略 `tools` 续接轮重放的同名同构 `additional_tools` 声明去重同时保留新增声明与冲突检测；在 `helpers_test.go` 为 `newProtocolLabRouter` 补齐内存态 `protocolLabKeyRepo`（支撑多轮 WS `Reauthenticate`）、`ws.DefaultParameters()` 轮间空闲超时（保留 `MaxIngressConnectionsPerAPIKey = 0` 兼容内存并发桩）与并发安全的 `ProviderRecord` 健康快照读取；OpenAI API Key 透传夹具显式开启 `openai_passthrough: true` 并绑定 `function-only` 规则精准覆盖 `passthrough.go` 接入点，T6 WebSocket 子用例通过 Codex CLI `-c model_providers.lab.supports_websockets=true` 命令行参数驱动而不改动锁定模板，经 `record` 模式与 `cmd/corpus import`/`verify` 新增 `corpus/clients/codex/0.171.0/lab_custom_tool/{1,2}.request.json` 并刷新派生快照；`TestRecordedResponsesCorpus_LosslessPrecheckAndFieldPreservation` 对无需降级请求与剥离可降级声明的对照报文做 `bytes.Equal` 逐字节断言，对发生工具降级的请求在剥离缩进空白后逐成员核对键序、64 位整数字面量与 `JSONDiff` 零差异；Project Doc 经评估在本批无需改动。
  - 待确认项：无（前序阶段原登记的 `P8` 至 `P10` 与 `AG1` 至 `AG11` 均已获用户裁决通过并正式纳入第 6.2.4 节与 `diff_ledger.json`）。

- 2026-11-25，阶段 3/4 用户裁决落地（批次 4.5，提交 `2623a902d`、`91f314009`、`620da124a`）：完成阶段 3 `P8` 至 `P10`（原 4 个 `pendingID`）与阶段 4 `AG1` 至 `AG11`（全部采用选项 A）正式纳入第 6.2.4 节验收差异清单（扩展 `D5`、`D12` 并新增 `D13`、`D14`），完成 `diff_ledger.json` 与 `stage4_dual_baseline_diff.json` 程序化接受条件校验落盘，清空阶段 3 与阶段 4 全部待确认与待裁决状态。
  - 局部决策：在 `protocol/testkit/diff_ledger.go` 注册 `DiffD13`（跨协议客户端遥测标记清理）与 `DiffD14`（Antigravity 统一单跳 `v1internal` 转换规则）并在 `ValidateDiffLedger` 中限定两者仅用于 `request` 方向；在 `protocol/convert/legacy_diff_scenario_test.go` 删除 `expectedStage3PendingIDs`、`requestDiffExemption.pendingID` 与 `ledgerAccumulator.pending` 等全部内存态待确认通道，将 `P8` 迁入扩展后的 `D5`（校验 `opaque_item_dropped`、定律 Q3/Q4 及剥离旧 `gemini_native.go` 序列化进模型文本的源 `PartOpaque` JSON 后非 `OpaqueItem` `Essence` 零差异），将 `P9` 迁入 `D13`（校验 `client_marker_dropped`、出站不含 `x-anthropic-billing-header:`、定律 Q3/Q4 与剩余系统提示块逐条无损一致），将 `P10` 分别迁入 `D12(a)` 与 `D12(b)`（校验逐块规范工具 JSON 一致及剔除授权增减帧后其余共享帧 `(Semantic, Terminal)` 标记 1 对 1 对齐），更新 `stage3_gemini` 台账（新增 14 条至 172 条）；在 `upstream/antigravity/dual_baseline_diff_scenario_test.go` 将 `AG1` 至 `AG11` 全部绑定至 `DiffD14` 与 `accepted_option_a`，在全语料共 `252` 次六分组评估加 4 条 `AG11` OpenAI 兼容工具直发探针上强制校验定律 Q3/Q4/Q5、外层 `v1internal` 信封、`AG1` 至 `AG11` 选项 A 线级不变量以及 `unclassified_diff_items == 0`、`pending_adjudication_count == 0`，同步落盘 `stage4_dual_baseline_diff.json` 并将 `stage4_antigravity` 作用域（245 条 `D14` 记录）并入共享 `diff_ledger.json`（总计 819 条记录）；核查 `docs/architecture/protocol_conversion.md` 与 `docs/interfaces/antigravity_upstream.md` 确认无待裁决表述且与当前实现一致，`make check-docs` 通过。
  - 待确认项：无。

- 2026-11-25，阶段 4 验收审计缺口修复（批次 4.6，提交 `2f5d6e284`、`56bec9b1d`）：完成响应侧模型生成的 `inlineData` 图片向客户端可见 Markdown 数据 URL 文本渲染与非 `thought` 文本块附带 `thoughtSignature` 重放签名保留，补齐第 6.2.4 节规定的阶段 4 Antigravity 流式/缓冲响应、逐帧 `(Semantic, Terminal)` 标记、计费用量与工具名四个维度的旧新差分核验（纳入 `diff_ledger.json` 的 `stage4_antigravity` 与 `testdata/golden/diff/tool_names.json`），修正 `ir/dialect.go` 与 `convert/dialect.go` 中 `SchemaAntigravity` 常量注释并同步更新 `docs/architecture/protocol_conversion.md`。
  - 局部决策：在 `ir.Part` 与 `ir.EssencePart` 建模 `Token *ReplayToken`（仅允许 `RoleAssistant` 消息或响应/流输出的 `PartText` 携带，其余位置由 `ir/validate.go` 拒绝，并在 `ir.Sequencer` 流式收尾时深拷贝 `endPart.Token`），由 `codec/generatecontent` 在解码非 `thought` 带签名文本部件时保留至 `Part.Token` 并在编码时优先写回 `thoughtSignature`，使同协议 `generatecontent` 往返不改变分块形态从而守住定律 Q2 与 S2；在 `convert/carriers.go` 的 `sealResponseCarriers` 与 `sealEvents` 中仅对非 Gemini 跨协议客户端目标（`Messages`、`Responses`、`Chat`）将模型生成的 base64 `inlineData` 图片转为 `![image](data:<mimeType>;base64,<data>)` 可见文本并记录 `LossMediaUnsupported(Detail: "markdown_data_url")`，同时将带 `Token` 的非 `thought` 文本块拆出合成空签名 `PartReasoning` 经 `trc1:antigravity_claude` 或 `trc1:gemini` 信封承载（`Chat` 与无方案路线记录 `LossReplayTokenDropped`），对剥离令牌后无可见正文与元数据的纯签名空文本（如紧随 `functionCall` 或独立出现的 `{"text": "", "thoughtSignature": "..."}`）省略空 `PartText` 并在流式 `sealEvents` 中延迟 `EventPartStart` 以免产生空 `Responses` `message` 条目或 `Messages` 双重丢弃损失，且在独立调用 `codec/{chat,messages,responses}` 编码带 `PartText.Token` 的合法 IR 时统一记录 `LossReplayTokenDropped` 并同步扩展 `testkit.VerifyEssence` 核验；在 `testkit/diff_ledger.go` 新增按 `(scope, directions)` 分片合并落盘的 `AssertDiffLedgerScopeDirections`，使 `upstream/antigravity`（维护 245 条请求 `D14`）与 `protocol/convert`（`TestLegacyDiffStage4AntigravityScenarios`，新增 242 条非请求记录：其中 `stream` 142 条含 `D2` 22、`D3` 88、`D6` 32，`stream_flags` 94 条含 `D3` 84、`D12` 10，`tool_names` 6 条 `D8`，`stage4_antigravity` 合计 487 条，全库 `diff_ledger.json` 共 1,061 条，`tool_names.json` 扩展至 71 个用例、14 个差异用例）在不跨越架构包边界的前提下协同校验六条 Antigravity 路线的旧流/缓冲响应处理器、兼容流桥接、`observedSink` 帧标记（含 `upstream_sse_quirks.sse` 局部重复工具块裁剪后的逐帧 1 对 1 对齐与 `tool_args_empty` 缓冲缺省 `input` 的显式 `D3` 断言）、计费 `TokenUsage` 零差异与工具名降级，并在 `upstream/antigravity/response_converted_test.go` 与 `internal/app/protocol_lab_scenario_test.go` 补齐图片生成与非 `thought` 签名多轮重放回归测试。
  - 待确认项：无。

- 2026-11-25，阶段 4 第 2 轮验收审计注释缺口修复（批次 4.7，提交 `21933f53f`、`5754ca720`）：修复阶段 4 改动文件中遗漏 Stage 4 与 `stage4_antigravity` 范围的过时注释，并全量复查阶段 4 涉及的 66 个手写 Go 文件补齐关联注释。
  - 局部决策：在 `protocol/convert/legacy_diff_scenario_test.go` 文件总览补入第 6.5 节与 `stage4_antigravity` 非请求方向边界（说明请求侧 `D14` 位于 `upstream/antigravity`），将 `ledgerAccumulator.scope` 改为前置注释覆盖四个阶段分区及 `add` 中的非空覆盖与空值回退 `stage1LedgerScope` 规则，并同步更新 `evalStreamFlagDiffs`、`legacyConvertRequest`、`legacyRunStream`、全部四个 `diffStage*ToolNames` 辅助函数以及 `streamEvalContext.legFrames` 和 `streamFlagAlignmentPlan` 字段注释中的 Antigravity `observedSink` 与 `antigravity_messages_*` 覆盖范围；在 `protocol/convert/multi_turn_scenario_test.go` 区分 9 条阶段 1/3 矩阵路线（遍历 `wire`、录制语料与全部客户端 SDK 变体，并在 `turn2Targets` 检验 Antigravity 双模型族目标）与阶段 4 六条 `{Messages, Responses, Chat} x {claude-*, gemini-*}` 路线的定向双轮合成场景覆盖，同步更新 `conformance_scenario_test.go` 与 `convert/helpers_test.go`；在 `convert/markers.go` 文件总览补入响应与流侧 `remapResponseLosses` 的 `parts[i]` 坐标重映射与合成推理块丢失过滤职责，在 `upstream/antigravity/payload.go` 文件总览区分 `InjectIdentityPatchToGeminiRequestWithPrompt` 的无损 `jsontext` 成员改写与旧可变 `map` 解码函数，在 `upstream/antigravity/dual_baseline_diff_scenario_test.go` 及 `protocol/testkit/diff_ledger{,_test}.go` 对齐 `AssertDiffLedgerScopeDirections` 按方向分片校验语义，并在 `codec/messages/legalize.go`、`codec/responses/{legalize,response}.go`、`convert/dialect.go`、`gateway/provider/model_attributes.go` 补齐 `PartText.Token` 丢失上报与 64 字节工具名上限注释；Project Doc 经评估在本批无需改动。
  - 待确认项：无。

- 2026-11-25，阶段 6 第 1 步 `protocol/qoder` 与上游侧 `codec/qoderchat` 实现及架构规则登记（批次 6.1，提交 `b9614aad9`、`4eab20026`、`26bc59e0d`）：在 `tools/architecture` 登记阶段 6 依赖规则并补齐正反例测试，按第 4.5 节实现 `protocol/qoder` 与仅包含上游侧的 `codec/qoderchat` 并注册至 `protocol/convert`，完成单测、T2 一致性套件与 10 分钟解码器本地 Fuzz 验收，并同步更新 `docs/architecture/backend_modules.md` 与 `docs/architecture/protocol_conversion.md`。
  - 解码器本地 Fuzz 统计（独立临时 `-test.fuzzcachedir`，退出自动清理，仓库零 `testdata/fuzz` 残留）：`FuzzStreamDecode` 10 分钟实测 `601.218s`（18 workers），执行 `156,559,883` 次（新增 interesting 样本 `1,021` 条，总计 `1,029` 条），修改 `stream_decode.go` 后复跑 1 分钟回归 Fuzz `61.474s`，执行 `16,071,956` 次；`FuzzResponseDecode` 1 分钟实测 `61.443s`（执行 `7,639,745` 次）及回归 1 分钟 `61.252s`（执行 `8,304,907` 次），全程 0 崩溃、0 `ir.ValidateStream` / `ir.ValidateResponse` / `ir.Validate` 违规。
  - 局部决策：在 `tools/architecture` 的 `leafDependencies` 登记纯包 `internal/protocol/qoder` 与 `internal/protocol/codec/qoderchat`（均仅用 `encoding/json/v2` 与 `encoding/json/jsontext`，零 `bridge`/`gjson`/`sjson` 导入），同步为 `internal/protocol/convert`、`internal/gateway`、`internal/upstream`、`internal/upstream/qoder` 与 `internal/provider` 开放阶段 6 所需依赖许可；在 `protocol/qoder` 定义归一化的 `PayloadRequest`、离散用户文本块与 `cache_control`、`ToolChoice.AllowedTools` 子集过滤、思考指令优先级推导、`NormalizeBillingHeader` 五位十六进制 `cch` 归一化、支持 `errors.As` 穿透的 `*qoder.RequestError` 与 `*qoder.APIError`，以及流式 `jsontext` 加转义正则双路径的 `RedactErrorText`（覆盖 `logredact` 默认敏感键与 Qoder 专属键并保留 1 至 8 位纯十进制 `"code"` 供应商错误码）；在 `ir.Dialect` 新增 `StatelessResponses` 由 `convert.NewExchange` 仅对 `ProtocolOpenAIResponses` 与带顶层 `input` 的 Cursor Chat 请求置真，使 `qoderchat.EncodeRequest` 仅对 Responses 来源强制 `Store == true`、孤立或无 ID `tool_result` 的 `*qoder.RequestError` 拒绝及未应答调用裁剪，对 Chat 与 Messages 来源忽略 `Store`、按时间序保留独立 `tool` 结果与未应答调用，并将缺失 `tool_use_id` 的 `tool_result` 可读文本前后向并入所属 `user` 轮（记 `LossToolPairingRepaired(Detail: "missing_tool_call_id")` 并在 `testkit.VerifyEssence` 对齐验证）；在 `codec/chat` 解码兼容 `thinking` 开关与预算至 `ir.ReasoningConfig` 同时通过 `Extra` 保持同协议往返无损；在 `qoderchat` 流与缓冲解码中覆盖包裹式状态信封（含省略或空 `body` 的 4xx/5xx 错误信封）、OpenAI choice delta、Anthropic 风格事件信封、XML/JSON/DSML 三种文本编码工具调用、并行槽位漂移与 `{}` 占位符覆盖、按需触发 `wirejson` 用量校验及 `PromptTokensDetails` 缓存拆分合并。
  - 待确认项：无。

- 2026-11-25，阶段 6 第 2 步 Qoder 请求与响应合并接线及连接探针预备（批次 6.2，提交 `764278da5`、`65bb65de9`）：在 `UseIRConversion` 接线开关后把 `upstream/qoder` 的三个请求解析分支与六组流式和缓冲响应写出器替换为基于 `convert.Exchange` 的合并请求转换与一对读取函数，同步在 `gateway/provider` 与 `gateway/httpapi` 接通方言注入、旧式 Responses `messages` 兼容归一化与流控错误分类，为 `provider/provider/qoder_test_execution.go` 和 `test_streams.go` 预备基于 `convert` 的连接探针分支，补齐各包 `-race` 单元测试与新旧对照测试，并更新 `docs/architecture/protocol_conversion.md`。
  - 局部决策：保持 `UseIRConversion == false` 时旧 `ParseQoder*Payload`、`WriteQoder*StreamResponse`、`BuildQoder*` 及无开关连接探针生产入口（`QoderProviderTest.Execute` 与 `TestStreamOutput.Qoder`）原样不动，且 `TestProductionGatewayProvidersKeepUseIRConversionDisabled` 继续守卫生产默认关闭；在 `convert.Options` 新增可选回调 `MapUpstreamToolName`，在 `Exchange.Response`、`Stream.processEvents` 和 `Buffer.foldEvents` 中置于上游令牌打标之后、`raiseTools` 之前，使 `QoderDeclaredToolNameMapper` 先把 Qoder 上游工具别名还原为 `lowerTools` 声明名，再由 `raiseTools` 还原客户端 `custom`、`namespace` 与 `tool_search` 工具身份，并在 `NewExchange` 中对省略顶层 `store` 的无状态 Qoder Responses 请求将回显 `Store` 置为 `false`；在 `upstream/qoder/payload_converted.go` 新增 `PayloadRequestFromIR` 与 `executeConverted`，衔接 `RestrictAllowedTools`、`NormalizedSystem`（`cch=<cch>;` 指纹归一化）、离散 `user` 文本块与标量 `assistant`/`tool` 内容规范、无损 `json.RawMessage` 工具参数、`withEffectiveControls` 推理与工具能力门禁（`attrs.ToolCall == false` 裁剪可选工具并拒绝必选或指定 `tool_choice`）及会话规划，将 `*protocol/qoder.RequestError` 与客户端解码校验错误统一转为 `*upstream/qoder.RequestError`；在 `upstream/qoder/response_converted.go`（及 `tools/architecture/catalog.go` 为 `internal/upstream/qoder` 补充 `internal/protocol/wirejson` 依赖）新增纯 `encoding/json/v2` 与 `jsontext` 实现的 `ReadConvertedStreaming` 和 `ReadConvertedBuffered`，跳过旧 `bridge.CompatOutputMeaning` 二次反解析，以按步受控的 `qoderFrameScanner` 消除同步测试 `ReadCloser` 读写竞态并保留取消时关闭 `resp.Body` 唤醒阻塞读取，完整保留 `wirejson` 守卫的原生计费用量与稀疏尾帧合并、转换前首次 `UpstreamResponseModel` 采集、无令牌推理 `trc1:plain` 信封封装、前导非语义帧暂存窗口、`NewDeferredOutputContext`（同步在 `executionOutput` 转发 `OutputState` 以感知底层预提交状态）、`DeferRetryableCommittedError` 与保活已提交 SSE 流上的无计时终态错误写出、中途断流闭块收尾与部分用量回滚、客户端断开后继续排空计费及 debug 级别 `Losses()` 计数；在 `QoderRuntime.PrepareQoderTarget`、`ForwardQoderAttempt` 与 `UnifiedTextExecutor.forwardQoderConverted` 中接通缓存复用的 `ConversionDialect`（保留映射前模型的 `StripEffort`）、`NormalizeOpenAIResponsesLegacyIngressIR`、同号凭据刷新重试、本地 `*qoder.RequestError` 映射为 HTTP `400` 加 `MODEL_CAPABILITY_UNSUPPORTED`（全协议 `writeQoderCommittedTerminalError` 均调用 `MarkResponseCommitted` 以抑制外层调度扣分与重复兜底）及首包前可切换错误晋升 `UpstreamFailoverError`；在 `qoder_test_execution.go` 与 `test_streams.go` 新增 `BuildQoderTestPayloadIR`、`ExecuteIR` 与 `QoderIR` 供阶段 8 切换。
  - 待确认项：无。

- 2026-11-25，阶段 6 第 3 步 Qoder 三条路线一致性定律与四维旧新差分验证（批次 6.3，提交 `2424d1a26`、`16c30cd1b`、`cee7e36a1`）：在 `protocol/convert` 补齐 `{messages,responses,chat}_to_qoder` 三条路线的 T3/T4 一致性定律（Q3、Q4、Q5、S3 至 S8、多轮重放 M1）与 `golden/loss` 快照，并在 `upstream/qoder` 对照 `ParseQoder*Payload`、`WriteQoder*StreamResponse` 与 `BuildQoder*` 完成请求、流式与缓冲输出、逐帧 `(Semantic, Terminal)` 标记、计费用量与工具名还原的全维度旧新差分验证，记入共享 `diff_ledger.json` 的 `stage6_qoder` 作用域及 `stage6_tool_names.json`。
  - `stage6_qoder` 四维差分台账统计（`backend/internal/protocol/testkit/testdata/diff_ledger.json` 与 `backend/internal/upstream/qoder/testdata/golden/stage6_tool_names.json`）：`stage6_qoder` 作用域共登记 `130` 条已授权差异（全库 `diff_ledger.json` 合计 `1,191` 条），未归类差异 `0` 条，待确认项 `0` 条，流式、缓冲及异常截断场景的计费 `TokenUsage` 差异均为 `0`；按方向拆分为 `request` `22` 条（`D2` 17、`D5` 2、`D4` 1、`D7` 1、`D8` 1）、`stream` `40` 条（`D3` 28、`D2` 6、`D6` 6）、`stream_flags` `63` 条（`D3` 42、`D12` 21，全部 54 个常规路线与流场景组合在按计划旗标执行局部帧裁剪后其余共享帧 `(Semantic, Terminal)` 标记 1 对 1 对齐，且 Chat、Responses、Messages 三种目标协议均通过逐块工具参数规范 JSON 等价校验）、`tool_names` `5` 条（`D8` 3、`D2` 2，`stage6_tool_names.json` 共锁定 29 个含工具用例，其中 24 个常规函数用例零差异，全部 29 个用例在流式与缓冲响应上均通过客户端工具身份无损还原断言）；按类别拆分为 `D3` 70、`D2` 25、`D12` 21、`D6` 6、`D8` 4、`D5` 2、`D4` 1、`D7` 1；按路线拆分为 `responses_to_qoder` 67、`chat_to_qoder` 44、`messages_to_qoder` 19；纯协议层损失快照锁定于 `backend/internal/protocol/convert/testdata/golden/loss/{messages,responses,chat}_to_qoder.json`。
  - 局部决策：保持 `protocol/convert` 纯叶子包边界，将纯协议层 T3/T4 与 M1 定律放在 `protocol/convert`，将依赖 `upstream/qoder` 旧解析器与写出器的四维差分测试放在 `upstream/qoder/legacy_diff_scenario_test.go`，并导出 `convert.DecodeUpstreamRequest` 供包外差分测试对跨协议转换后的 `PayloadRequest` 执行定律 Q4 `VerifyEssence` 核验；在 `convert/dialect.go` 与 `upstream/qoder/payload_converted.go` 对齐四项 Qoder 请求侧兼容细节（`messages_to_qoder` 补齐函数工具默认 `strict: false` 并记 `LossSchemaApproximated("strict")`、`Target.Dialect == nil` 独立调用时默认保留 `AllowBillingHeader: true`、`qoderToolsFromIRPayload` 省略空 `description` 以保持 `ToolsFingerprint` 一致、仅对多块或带缓存标记或全空白的 `user` 文本保留离散 `TextContent`）；在 `codec/responses` 的缓冲与流式响应编码器新增 `resolveResponseToolCallName`，使经 `raiseTools` 还原的非空带标点客户端工具名（如 `"code.exec"`）直接按原名写出，仅对空工具名回退 `"unknown"` 并补记流式 `LossToolLowered` 以维护定律 S4；修复 `extractLegacyStreamComparable` 在 Anthropic `message_delta.usage` 上完整合并 `input_tokens` 与缓存计数的提取逻辑，消除 `messages_to_qoder` 虚构的流式用量差异并将 `responses_to_qoder` 必填 `usage.output_tokens_details` 归入 `D3`（引用 `missing_output_tokens_details` 语法规则并校验 `ToTokenUsage()` 一致），在 `D6` `trc1:plain` 信封场景下继续按 7 个独立维度完整核对流式与缓冲的正文、推理、工具、停止原因及用量；Project Doc 经评估在本批无需改动，`make check-docs` 通过。
  - 待确认项：无。

- 2026-11-25，阶段 6 第 4 步 Qoder 测试夹具接入 T5 全场景矩阵与阶段 6 新旧路径逐格对账（批次 6.4，提交 `e2a893936`、`695cdd77a`、`93ecc6e7f`）：将 `gateway/testkit` 的 `QoderClient` 捕获夹具与 `protocolLabQoderClient` 接入 `backend/internal/app` T5 路由测试，覆盖 `routes.txt` 全部三条 Qoder 转换路线的流式与缓冲转换、无状态 Responses 与 Cursor `input` Chat 本地 400 门禁、工具与思考能力门禁、会话指纹归一化、故障转移与断流计费及混合分组双轮续聊，落盘 `stage6_qoder_legacy_vs_ir.json` 并补足 `qoderRecordingSink.Begin` 注释。
  - `stage6_qoder_legacy_vs_ir.json` 逐格对账统计（`backend/internal/app/testdata/golden/stage6_qoder_legacy_vs_ir.json`）：覆盖 `messages_via_qoder`、`responses_via_qoder`、`chat_via_qoder` 三条路线共 `243` 格（每条路线 `81` 格）配对运行 `UseIRConversion == false` 与 `UseIRConversion == true`；旧路径 `legacy_passed = 131`、`legacy_skipped = 36`、`legacy_failed = 76`，新路径 `ir_passed = 207`、`ir_skipped = 36`、`ir_failed = 0`，合计 `fixed = 76`（`messages_via_qoder` `18` 项、`responses_via_qoder` `46` 项、`chat_via_qoder` `12` 项）、`still_present = 0`、`regressed = 0`；全部 `76` 条旧路径失败项均经 `describeStage6QoderDefect` 的 `42` 个实测命中分支（强制校验零死分支）按真实代码调用链记录单行 `legacy_reason` 与 `resolution`。
  - 局部决策：在 `backend/internal/app/helpers_test.go` 中将 `gateway/testkit.QoderClient` 捕获夹具、按提供商会话隔离的本地 HTTP 端点路由（`targetDoer` 非空时保留 `api1.qoder.sh` 原始域名供 `QoderRequestDoer` 经 `protocolLabTransport.localEndpointOverride` 改写至所选提供商本地 `base_url`）、共享 `scheduler.RuntimeStats` 健康快照、`protocolLabAttributeRepo` 内存分组模型属性覆盖以及 `Transport` 和 `Health` 接入 `newProtocolLabRouter` 与 `protocolLabQoderClient`，保持阶段 0 `legacy_baseline.json` 与生产默认 `UseIRConversion == false` 完全不变；在 `backend/internal/app/protocol_lab_stage6_scenario_test.go` 中新增 `TestProtocolLabIRQoderUpstreamMatrix` 与 `TestProtocolLabStage6QoderLegacyVsIR`，覆盖规范流式 SSE 与 `stream=false` 缓冲 JSON 折叠、`<tool_call>` XML/内嵌 JSON/DSML 文本编码工具调用、Anthropic 事件封套流式与缓冲稀疏 `message_delta` 用量核对、`custom`/`namespace`/`tool_search` 客户端工具降级叠加 `QoderToolNameMapper` 大小写及超 64 字节截断别名还原、无状态 `/v1/responses`（含未持有与同密钥已持有 `previous_response_id`、`store: true`、`conversation`、`item_reference`、未配对 `function_call_output`）及带顶层 `input` 的 Cursor Chat 本地 HTTP 400 拒绝且零切号零扣分（对照标准 Chat `store: true`、独立 `tool_result` 与无状态 Cursor `input` 放行）、`attrs.ToolCall == false` 可选工具裁剪与必选 `tool_choice` 本地 400、强制思考模型 `ultimate` 禁用思考本地 400、Keepalive 提前提交 `CommittedSSE` 后的终态 SSE 错误帧与 `DeferRetryableCommittedError` 跨号故障转移、`x-anthropic-billing-header` 易变 5 位十六进制 `cch` 归一化为 `cch=<cch>;` 的多轮 `session_id` 复用、首包前 HTTP 502 与 HTTP 403 `code=112` 及前导非语义帧加包裹式 503 `APIError` 切号、输出开始后断流闭块收尾与部分计费用量结算（`HasUsage == false` 时不伪造 `UsageLog`），以及 `trc1:plain` 在 Qoder、`passback` Messages、标准 Anthropic Messages、DeepSeek `deepseek-thinking` Chat 与原生 Responses HTTP/WS 间的混合分组双轮续聊；同时将 `backend/internal/upstream/qoder/legacy_diff_scenario_test.go` 中 `qoderRecordingSink.Begin` 注释扩充至 32 词 2 句；Project Doc 经评估在本批无需改动，`make check-docs` 通过。
  - 待确认项：无。

- 2026-11-26，阶段 7 第 1 至 2 步 `protocol/bridge` 全仓导入方核验、`routes.txt` 逐行覆盖补齐及第 4 步 `live` 金丝雀入口准备（批次 7.1，提交 `73a8ca439`、`7c58a4dbe`）：基于 `go/ast` 重新清点全仓 `internal/protocol/bridge` 直接导入方、间接调用方与阶段 8 保留符号去处并落盘 `stage7_bridge_importers.json`，逐行核对并补齐 `routes.txt` 全部 77 条有效路线在 T3、T5 与 T6 脚本模式下的测试缺口并落盘 `stage7_routes_coverage.json`，同时补全第 4 步五组 `TOKENROUTER_CLIENTLAB_MODE=live` 金丝雀的测试入口与无网络装配校验。
  - 10.4 节 `bridge` 导入方与阶段 8 去处清点结论（`backend/internal/app/testdata/golden/stage7_bridge_importers.json`）：除 `internal/protocol/bridge` 自身外，`backend/internal` 共 `100` 个 `.go` 文件导入 `bridge`（`71` 个生产文件加 `29` 个测试文件），另登记 `12` 个不直接导入 `bridge` 的间接调用方与 `2` 个架构白名单文件（`tools/architecture/catalog.go`、`policy_test.go`）；`71` 个生产文件拆分为四类：`34` 个保留文件在阶段 8 删开关与旧分支（`category_a_switch_gated_cutover`，含保留 `RecordMessagesStreamError` 的 `output_openai_messages.go`），`3` 个无开关辅助文件（`input_tokens.go`、`openai_responses_ingress_compat.go`、`tokenestimate/estimate.go`）在阶段 8 第 1 步切向已就绪的 `*IR` 对照函数（`category_b_switchless_counterpart_set`），`20` 个保留原生或共享路径文件在阶段 8 第 4 步将 `18` 个 `bridge.*` 符号迁往 `protocol/openai` 与 `protocol/gemini`（`category_c_native_symbol_migration`），`14` 个旧转换生产文件在阶段 8 第 2 至 3 步整文件删除（`category_d_deleted_or_test_migrated`）。阶段 8 实施注意点：（1）6.9 节第 4 步正文之外新核出保留原生路径 `(*Gemini).ForwardNative`（`googleforward/gemini_attempts.go`）仍调用 `bridge.NativeEnsureGeminiFunctionCallThoughtSignatures`，阶段 8 第 4 步一并迁入 `internal/protocol/gemini`（直接接收 `dummySignature string, body []byte`），同批补迁伴生构造函数与类型 `NewBufferedResponseAccumulator`、`NewResponsesStreamOutputItems`、`ResponsesNamespaceName`（至 `internal/protocol/openai`）及 `NativeGeminiUsage`（至 `internal/protocol/gemini`）；（2）`tools/architecture/catalog.go` 的 `leafDependencies["internal/protocol/gemini"].Production` 须补充 `"internal/protocol"`（供返回 `*protocol.TokenUsage` 的 `NativeUsageProjection` 使用），并在删除 `googleforward/gemini_openai.go` 时同步从 `filePermissions` 移除 `googleforward/gemini_openai_external_test.go`；（3）整文件删除 `antigravity_openai.go`、`upstream/client_tools_stream.go` 与 `antigravity/request_transformer.go` 前，须先将 `antigravityCompatProxyURL` 与 `antigravityCredentialRejectedError` 迁入 `antigravity_converted.go`，将 `DefaultSSELineLimit` 与 `responsesClientToolStreamBody`（含 `Close`）迁入 `upstream/responses_tool_stream.go`，将 `MaxTokensBudgetPadding` 与 `GetDefaultIdentityPatch` 迁入 `antigravity/payload.go`，将 `thinking_request.go` 的 `DummyThoughtSignature` 改指 `internal/protocol/gemini`，并将 `antigravity/probe.go` 的 `BuildClaudeTestRequestIR` 中 `DummyThoughtSignature` 与 `defaultMaxOutputTokens` 分别改用 `protocolgemini.DummyThoughtSignature` 与 `modelrules.AntigravityDefaultOutputLimit`（同步清理 `antigravity_messages.go`、`antigravity_options.go` 的 `getClaudeTransformOptions` 及 `openaiforward/messages_ports.go` 的 `MessagesOptions`）。
  - `routes.txt` 全部 `77` 行 T3/T5/T6 覆盖统计（`backend/internal/app/testdata/golden/stage7_routes_coverage.json`）：`[route_table]` `64` 行与 `[off_table]` `13` 行均绑定经 AST 校验真实存在的 T3 与 T5 测试函数（由 `buildStage7T5RouteIndex` 与 `buildStage7ScriptedT6RouteIndex` 从各测试路线构造函数直接生成精确反向索引，零串挂、零遗漏）；T3 在 `protocol/convert/conformance_scenario_test.go` 新增 `TestStage7OffTableAndDialectLaws`，补齐 Grok `xai` 加 `OmitStoreAndInclude`、DeepSeek `HistoryToolTurns`、Cursor 顶层 `input` 跨目标协议转换及 Gemini `count_tokens` 禁签名预检定律；T5 在 `stage7_verification_scenario_test.go` 新增 `TestProtocolLabStage7RouteMatrixComplete`，于 `UseIRConversion: true` 下端到端跑通全部 `64` 条 `[route_table]` 行与全部 `13` 条 `[off_table]` 行的每个 `platform:provider_type` 作用域；T6 脚本模式在 `client_lab_scenario_test.go` 新增 `TestClientLabIRStage7RouteMatrixScenarios`（`37` 个两轮 `lab_tool` 真实 CLI 子进程格），使全部 `58` 条非 Antigravity、非 Qoder `[route_table]` 行及阶段 5 全部 `5` 条客户端工具降级 `[off_table]` 行（共 `63` 行）具备真实 CLI 脚本模式覆盖，其余 `14` 行（Antigravity `3` 行、Qoder `3` 行及无独立 CLI 开关的 `8` 条离表机制）记录明确豁免说明并由 T3 与 T5 覆盖。
  - 第 4 步 `live` 金丝雀入口准备情况：在 `client_lab_scenario_test.go` 新增 `TestClientLabIRLiveCanaryScenarios`、`parseLiveCanaryConfigFromEnv`、`buildStage7LiveCanaryGroups` 与 `runStage7LiveCanaries`，在 `TOKENROUTER_CLIENTLAB=1`、`TOKENROUTER_CLIENTLAB_MODE=live` 及 `verifyClientLabLiveContainerGate` 一次性容器门禁通过后读取 `OPENAI`、`ANTHROPIC`、`GEMINI`、`GROK`、`DEEPSEEK` 五组线上地址、密钥与可选 `*_MODEL` 覆盖，开启 `UseIRConversion: true` 覆盖第 6.8 节第 4 步全部 5 组（共 7 对路线，每对执行 `lab_reasoning_tool` 与第 2 轮切回客户端原生路径的 `lab_switch_provider`），同步更新 `tools/clientlab/README.md`，并在 `TestClientLabConfigTemplatesAndGuards` 中用内存闭包校验配置解析与网关装配（本次未触发真实联网请求或读取宿主机凭据）。
  - 局部决策：在 `TestStage7BridgeImporterInventoryAndDestinations` 中加入 `verifyDeletedProductionFilesHaveNoUnmovedCallers`、`verifyCitedGoFilenamesExist` 与 `verifyStage7ScenarioOverviewFilesExist` 三项静态防呆断言，确保任何整文件删除候选的未迁出符号引用或备注中不存在的 `*.go` 文件名立即报错；将 `openaiforward/anthropic_converted.go` 与 `messageforward/conversion_ir.go`（仅在 `UseIRConversion == true` 执行）的遗留入口调用对齐到 `NormalizeOpenAIResponsesLegacyIngressIR`，在 `httpapi/count_tokens_selected.go` 的 `platform == "grok"` 分支按 `shouldUseGrokCountIRConversion` 在开关开启时调用 `tokenestimate.AnthropicIR`，在 `helpers_test.go` 的 `localEndpointOverride` 补入 `openai:oauth` 本地重定向，并将 `client_lab_scenario_test.go` 全文件门控统一改为 `clientLabEnabled()` 早返回模式（零 `t.Skip*` 调用），保持生产默认 `UseIRConversion == false` 行为不变；Project Doc 经评估在本批无需改动，`make check-docs` 通过。
  - 待确认项：无。

- 2026-11-26，阶段 7 第 5 步全量编解码器与同协议客户端工具适配路径 2 MiB 请求及 5,000 帧流新旧对照基准测试与 `<= 2.0x` 门槛核验（批次 7.2，提交 `adaa079e4`、`a1098e40c`、`3eebb1134`）：补齐并实测阶段 1 六条文本路线、阶段 3 三条 Gemini `generatecontent` 路线、阶段 4 六条 Antigravity 路线、阶段 5 同协议 Responses 客户端工具轻量适配路线与阶段 6 三条 Qoder `qoderchat` 路线（合计 38 组、76 个子基准）的 2 MiB 多轮请求与精确 5,000 帧上游 SSE 流新旧同源对照基准测试，对新路径 CPU 与分配热点完成无损优化，核验全部 38 组路线耗时均不超过旧实现的 `2.0x`。
  - 全量 38 组新旧对照基准统计（Apple M5 Pro，`darwin/arm64`，`-benchmem -count=3` 算术均值）：38 组中 27 组（`71%`）快于旧实现，全部 38 组 `new/legacy` 耗时比位于 `0.16x` 至 `1.83x` 之间，全部满足 `<= 2.00x` 硬性门槛：（1）阶段 1 六条文本路线（`convert/benchmark_scenario_test.go`）：2 MiB 请求依次为 `messages_to_responses` `0.76x`（`11.10ms` 对 `14.60ms`，`31.54MB/31,269 allocs` 对 `27.03MB/6,251 allocs`）、`messages_to_chat` `0.82x`（`10.40ms` 对 `12.69ms`，`30.19MB/30,867 allocs` 对 `24.57MB/6,734 allocs`）、`responses_to_messages` `0.30x`（`12.38ms` 对 `41.96ms`，`37.61MB/36,761 allocs` 对 `82.05MB/26,493 allocs`）、`responses_to_chat` `0.34x`（`13.08ms` 对 `38.70ms`，`37.62MB/35,689 allocs` 对 `86.04MB/42,940 allocs`）、`chat_to_responses` `0.97x`（`10.08ms` 对 `10.37ms`，`30.17MB/24,785 allocs` 对 `24.62MB/4,127 allocs`）、`chat_to_messages` `0.25x`（`8.91ms` 对 `35.73ms`，`29.05MB/25,549 allocs` 对 `62.64MB/22,606 allocs`），5,000 帧流依次为 `messages_to_responses` `1.17x`（`11.19ms` 对 `9.53ms`）、`messages_to_chat` `0.62x`（`5.84ms` 对 `9.43ms`）、`responses_to_messages` `0.27x`（`4.77ms` 对 `17.95ms`）、`responses_to_chat` `0.35x`（`5.93ms` 对 `17.01ms`）、`chat_to_responses` `1.07x`（`11.97ms` 对 `11.21ms`）、`chat_to_messages` `0.51x`（`5.41ms` 对 `10.60ms`）；（2）阶段 3 三条 Gemini 路线（`convert/benchmark_scenario_test.go`）：2 MiB 请求依次为 `messages_to_gemini` `1.29x`（`9.27ms` 对 `7.17ms`，补丁后单跑复测 `1.25x`，`8.54ms` 对 `6.83ms`）、`responses_to_gemini` `0.23x`（`10.86ms` 对 `47.03ms`）、`chat_to_gemini` `0.18x`（`7.71ms` 对 `41.96ms`），5,000 帧流依次为 `messages_to_gemini` `0.26x`（`6.47ms` 对 `25.38ms`，`7.92MB/111,120 allocs` 对 `73.24MB/348,364 allocs`）、`responses_to_gemini` `0.16x`（`6.60ms` 对 `40.28ms`）、`chat_to_gemini` `0.22x`（`6.89ms` 对 `30.75ms`）；（3）阶段 4 六条 Antigravity 路线（`upstream/antigravity/benchmark_scenario_test.go`）：2 MiB 请求依次为 `antigravity_messages_claude` `1.83x`（`20.21ms` 对 `11.05ms`，补丁后复测 `19.21ms` 对 `10.52ms` 同为 `1.83x`）、`antigravity_messages_gemini` `1.72x`（`18.99ms` 对 `11.03ms`，复测 `1.75x`）、`antigravity_responses_claude` `0.42x`（`20.93ms` 对 `49.75ms`）、`antigravity_responses_gemini` `0.30x`（`21.21ms` 对 `70.47ms`）、`antigravity_chat_claude` `0.39x`（`17.62ms` 对 `44.95ms`）、`antigravity_chat_gemini` `0.27x`（`17.64ms` 对 `66.25ms`），5,000 帧流依次为 `antigravity_messages_claude` `0.97x`（`37.87ms` 对 `38.88ms`，`19.64MB/272,045 allocs` 对 `27.85MB/311,280 allocs`）、`antigravity_messages_gemini` `0.97x`（`38.22ms` 对 `39.53ms`）、`antigravity_responses_claude` `0.62x`（`37.89ms` 对 `61.17ms`）、`antigravity_responses_gemini` `0.62x`（`37.93ms` 对 `61.38ms`）、`antigravity_chat_claude` `0.75x`（`39.70ms` 对 `52.88ms`）、`antigravity_chat_gemini` `0.75x`（`38.89ms` 对 `52.11ms`）；（4）阶段 5 同协议 Responses 客户端工具适配（`convert/benchmark_scenario_test.go`）：2 MiB 含 `custom`/`namespace`/`tool_search` 及 `message`/`reasoning`/`compaction` 大载荷请求 `responses_same_protocol_tools` `1.32x`（`17.52ms` 对 `13.30ms`，`86.30MB/96,942 allocs` 对 `55.21MB/16,235 allocs`；补入 `compaction` 前全量同批为 `1.28x`），5,000 帧流 `1.56x`（`12.30ms` 对 `7.90ms`）；（5）阶段 6 三条 Qoder 路线（`upstream/qoder/benchmark_scenario_test.go`）：2 MiB 请求依次为 `messages_to_qoder` `1.14x`（`13.01ms` 对 `11.42ms`，复测 `1.09x`）、`responses_to_qoder` `0.77x`（`15.37ms` 对 `19.91ms`）、`chat_to_qoder` `1.37x`（`11.92ms` 对 `8.71ms`），5,000 帧流依次为 `messages_to_qoder` `1.29x`（`16.47ms` 对 `12.80ms`）、`responses_to_qoder` `0.94x`（`16.47ms` 对 `17.49ms`）、`chat_to_qoder` `1.12x`（`17.65ms` 对 `15.75ms`）。
  - 性能优化与局部决策：针对 `pprof` 剖析定位的四处新路径分配热点做无损优化且不改动任何 `UseIRConversion == false` 旧生产路径：（a）将 `codec/generatecontent/stream_decode.go` 中 `StreamDecoder.seenText` 与 `seenThinking` 由累积字符串改为可复用 `[]byte` 配合零分配前缀比对，消除 5,000 帧流上的二次方字符串拷贝；（b）在 `codec/generatecontent` 新增 `wireOutPart`、`wireOutContent` 与 `wireOutSystemInstruction`，让 `EncodeRequest` 在顶层单次序列化全部对话部件，省去逐部件 `marshalDeterministic` 后再经 `jsontext.Encoder` 重解析格式化；（c）在 `codec/messages/request_decode.go` 增加首尾 `"type"` 判别键零分配预读（同时覆盖结构体顺序与字典序 JSON）和保留 `jsontext.AllowInvalidUTF8(true)` 的池化 `jsontext.Decoder` 内容数组切片（附块数组 `0xff` 非 UTF-8 回归单测与 `FuzzRequestDecode` 种子），省去逐块 `wireTypeProbe` 双重反序列化并维持 `json/v2` 对每个对象的语法与重复键校验；（d）在 `upstream/antigravity` 对仅新管线使用的 `InjectIdentityPatchToGeminiRequestWithPrompt` 改用已校验原始偏移区间直拼 `systemInstruction`（保留旧管线共用的 `CleanGeminiRequest` 与 `WrapV1InternalRequest` 原样不动），在 `response_converted.go` 引入池化 `jsontext.Decoder`、栈上成员切片与非逃逸 `internAntigravityEnvelopeKey` 并在 `execute.go` 的 `ModeConvertedResponse` `ObserveRaw` 中跳过 `ReadGeminiStream` 已解包帧的重复解包，将 Antigravity 5,000 帧流耗时由初跑 `2.11x`（`82.6ms`、`242.7MB/op`）压至 `0.62x` 至 `0.97x`（`19.6MB` 至 `20.8MB/op`）；严守纯包分层边界将新增基准置于 `upstream/antigravity/benchmark_scenario_test.go` 与 `upstream/qoder/benchmark_scenario_test.go`（零 `protocol/bridge` 导入），每套基准均附带随 `make test-go` 运行的 `TestBenchmark*PayloadsValid` 单测校验体积、帧数、严格请求与流语法规则、计费用量一致性及跨轮切片容量上界，并在 `stage7_verification_scenario_test.go` 与 `stage7_bridge_importers.json` 同步登记 `antigravity/benchmark_scenario_test.go`（`bridge` 导入方总计更新为 `101` 个文件：`71` 个生产文件加 `30` 个测试文件）；Project Doc 经评估在本批无需改动，`make check-docs` 通过。
  - 待确认项：无。

- 2026-11-26，阶段 7 第 3 步与第 6 步全量无缓存重跑、最终差分与基线总对账报告及阶段 8 升级说明要点汇总（批次 7.3，提交 `db96d028c`、`77406b7a9`、`2fe220308`）：以 `-count=1` 无缓存重跑旧路径全部基线（`TestLegacyConversionMatrixBaseline`、T5 旧路径与阶段 4/5/6 配对矩阵、T6 `TestClientLabScenarios`）与新路径全部 T1 至 T6 脚本模式矩阵及全仓 `GOFLAGS=-count=1 make test-go`，完成 `TestStage7FinalVerificationReport` 与 `backend/internal/app/testdata/golden/stage7_final_verification_report.json` 黄金报告落盘，收口 `antigravity/probe.go` 输出上限常量引用与阶段 8 `DummyThoughtSignature` 迁出说明，并在本文末独立小节汇总阶段 8 `docs/operations/upgrade_notes.md` 升级说明全部要点。
  - 最终差分与 2,852 格基线总对账统计（`backend/internal/app/testdata/golden/stage7_final_verification_report.json`）：生产默认装配核验 `production_use_ir_conversion_default = false`；`routes.txt` 全部 `77` 行（`64` 条 `[route_table]` 加 `13` 条 `[off_table]`）T3/T5 100% 绑定并通过 AST 校验（T6 脚本覆盖 `63` 行、豁免 `14` 行，未绑定行 `0`、缺失测试引用 `0`），`bridge` 导入方 `101` 个文件（`71` 个生产文件加 `30` 个测试文件）、`12` 个间接调用方与 `18` 个保留原生符号迁移零遗漏；`diff_ledger.json` 通过 `ValidateDiffLedger` 且内嵌与磁盘零差异，全部 `5` 个作用域共 `1,191` 条记录（`stage1_text` `382`、`stage3_gemini` `174`、`stage4_antigravity` `487`、`stage5_responses_tools` `18`、`stage6_qoder` `130`；按类别为 `D1` `1`、`D2` `185`、`D3` `526`、`D4` `1`、`D5` `8`、`D6` `80`、`D7` `14`、`D8` `20`、`D9` `2`、`D10` `7`、`D11` `22`、`D12` `74`、`D13` `6`、`D14` `245`；按方向为 `request` `475`、`stream` `353`、`stream_flags` `337`、`tool_names` `19`、`native_replay` `7`），未归类差异 `0`、待确认差异 `0`（含 `stage4_dual_baseline_diff.json` 的 `252` 次评估），两份工具名差分黄金（`convert/testdata/golden/diff/tool_names.json` `71` 例与 `stage6_tool_names.json` `29` 例）合计 `100` 例全部满足 `client_names_same == true` 且 `14 + 5 = 19` 个差异用例与 `diff_ledger.json` 的 `19` 条 `tool_names` 记录呈严格 1 对 1 双射，计费用量差分 `0`（含实跑 `19` 组全上游族 `UseIRConversion` 新旧网关缓存计费探针零差异）；基线与配对矩阵总计 `2,852` 格中，`legacy_baseline.json` 全部 `1,869` 格（纯矩阵 `693`、T5 `1,126`、T6 `50`，旧失败 `420`）与 `stage2_baseline_resolution.json`（`1,474` 格：`339` `fixed`、`19` 同协议原生透传 `still_present`）加 `stage3_baseline_resolution.json`（`395` 格：`61` `fixed`、`1` 同协议原生透传 `still_present`）逐格互斥且并集 100% 相等（零遗漏、零重复），叠加阶段 4 Antigravity `426` 格（旧失败 `86` -> `86` `fixed`、`0` `still_present`）、阶段 5 客户端工具 `314` 格（旧失败 `53` -> `34` `fixed`、`19` 原生非工具 `still_present`、`0` 客户端工具遗留缺陷）与阶段 6 Qoder `243` 格（旧失败 `76` -> `76` `fixed`、`0` `still_present`），全量旧失败 `635` 格中 `596` 格已修复（`fixed`）、`39` 格为第 2.2 节不经过 `convert` 的同协议原生非工具既有行为（`still_present`），范围内遗留缺陷 `0` 格、回归 `0` 格。
  - 阶段 8 `upgrade_notes.md` 升级说明要点汇总索引：完整要点已写入本文末归档小节之后的独立小节 `### 阶段 7 第 6 步：阶段 8 upgrade_notes.md 升级说明要点汇总`，覆盖单版本无运行时回退开关整体切换、旧会话裸签名按 `unknown` 本地丢弃与 `legacy_history` 正常续聊边界、`D6` 至 `D10` 及 `D11`/`D13`/`D14` 客户端与运维可感知变化、升级后首轮提示缓存可能未命中一次与后续确定性命中、零数据库迁移与 Redis `reasoning_content:` 旧键自然过期及 `GeminiToolCalls` 重启重建、以及回滚到旧二进制后活跃跨协议 `trc1:` 会话的处理限制；本阶段保持 `docs/operations/upgrade_notes.md` 不变，留待阶段 8 切换时写入。
  - 局部决策：将 `upstream/antigravity/probe.go` 中 `BuildClaudeTestRequestIR` 的 `defaultMaxOutputTokens` 改用已导入的 `modelrules.AntigravityDefaultOutputLimit`，并在 `stage7_verification_scenario_test.go` 与 `stage7_bridge_importers.json` 同步标注阶段 8 第 4 步将 `probe.go` 的 `DummyThoughtSignature` 改指 `internal/protocol/gemini.DummyThoughtSignature`；在 `TestStage7FinalVerificationReport` 中结合 AST 静态扫描（校验全部 6 个差分入口函数存在且零 `pendingID` 豁免标识符）、计划文末进度项扫描与 `19` 组内存网关新旧配对计费探针完成端到端交叉核验；Project Doc 经评估在本批无需改动，`make check-docs` 通过。
  - 待确认项：无。

- 2026-11-26，阶段 7 第 4 步 `TOKENROUTER_CLIENTLAB_MODE=live` 真实凭据一次性容器金丝雀及回归修复（批次 7.4，提交 `99f4c330e`、`5f1baa97c`、`895413281`）：在一次性非特权 Docker 容器（`TOKENROUTER_CLIENTLAB_CONTAINER=1`、`TOKENROUTER_CLIENTLAB_MODE=live`、`UseIRConversion: true`）中以 `-count=1` 实测跑通第 6.8 节第 4 步全部 5 组（7 对路线、14 个真实 CLI 子测试：`lab_reasoning_tool` 与第 2 轮切回原生路径的 `lab_switch_provider`），按第 5.9 节将真实 Codex 分裂回合请求脱敏入库回归语料，收口跨协议回合归并、单协议定律 Q2 不动点与跨协议 Messages 中高档位思考启用逻辑，并刷新 `stage7_final_verification_report.json` 与清理全部临时凭据和连接。
  - 一次性容器 `live` 金丝雀实测摘要（`internal/app:TestClientLabIRLiveCanaryScenarios`，`169.15s` 全绿）：全部 5 组共 7 对路线、14 个子测试 100% 通过：（1）`group1_codex_to_anthropic`（`codex_to_anthropic`：客户端 `codex` / `openai_responses`，第 1 轮转往 `anthropic_messages` 测 `claude-haiku-4-5` 并封入 `anthropic` 信封，第 2 轮 `lab_switch_provider` 切回原生 `openai_responses` 测 `gpt-6-sol`）；（2）`group2_claude_to_openai`（`claude_to_openai`：客户端 `claude-code` / `anthropic_messages`，第 1 轮转往 `openai_responses` 测 `gpt-6-sol` 并封入 `openai` 信封，第 2 轮切回原生 `anthropic_messages` 测 `claude-haiku-4-5`）；（3）`group3_claude_and_codex_to_gemini`（`claude_to_gemini` 与 `codex_to_gemini`：客户端 `claude-code` / `anthropic_messages` 与 `codex` / `openai_responses`，第 1 轮转往 `gemini_generate_content` 测 `gemini-3-flash-preview` 并封入 `gemini` 信封配合 `CallMemory`，第 2 轮分别切回原生 `anthropic_messages` `claude-haiku-4-5` 与原生 `openai_responses` `gpt-6-sol`）；（4）`group4_claude_and_codex_to_grok`（`claude_to_grok` 与 `codex_dialect_lowered_to_grok`：客户端 `claude-code` / `anthropic_messages` 与 `codex` / `openai_responses` 方言工具降级，第 1 轮发往 Grok `openai_responses` 测 `grok-4.7-fast` 并封入 `xai` 信封，第 2 轮分别切回原生 `anthropic_messages` `claude-haiku-4-5` 与原生 `openai_responses` `gpt-6-sol`）；（5）`group5_codex_to_deepseek_chat`（`codex_to_deepseek_chat`：客户端 `codex` / `openai_responses`，第 1 轮转往 DeepSeek `openai_chat_completions` 测 `deepseek-flash` 并封入携带 `reasoning_content` 的 `plain` 信封，第 2 轮切回原生 `openai_responses` 测 `gpt-6-sol`）。全部子测试均验证两轮工具续聊完成、出站上游请求零 `trc1:` 载体泄漏、严格请求校验零违规、网关零 HTTP 5xx 且 `UsageLog` 结算条数与第 2 轮原生落点匹配；测试完成后已立即删除本地临时凭据文件并断开全部 SSH 会话。
  - 回归语料入库与 `stage7_final_verification_report.json` / `diff_ledger.json` 同步统计：按第 5.9 节经 `cmd/corpus import` 脱敏入库真实 Codex 0.171.0 分裂助手回合请求语料 `corpus/clients/codex/0.171.0/lab_reasoning_tool/{meta.json,1.request.json,2.request.json}`（`corpus verify` 校验全部 `41` 个文件通过），同步刷新受影响的黄金快照：`diff_ledger.json` 全部 `5` 个作用域共 `1,205` 条记录（`stage1_text` `388`、`stage3_gemini` `176`、`stage4_antigravity` `491`、`stage5_responses_tools` `18`、`stage6_qoder` `132`；按类别为 `D1` `1`、`D2` `193`、`D3` `527`、`D4` `1`、`D5` `8`、`D6` `80`、`D7` `13`、`D8` `20`、`D9` `3`、`D10` `8`、`D11` `22`、`D12` `74`、`D13` `6`、`D14` `249`；按方向为 `request` `488`、`stream` `353`、`stream_flags` `337`、`tool_names` `19`、`native_replay` `8`），`stage4_dual_baseline_diff.json` 更新为 `256` 次分组评估（对照旧基线 A `2,143` 项、对照旧基线 B `1,046` 项），`convert/testdata/golden/diff/tool_names.json` 更新为 `85` 例（叠加 Qoder `29` 例共 `114` 例全部满足 `client_names_same == true`，`14 + 5 = 19` 个差异用例与台账保持严格 1 对 1 双射），未归类差异 `0`、待确认差异 `0`、计费用量差分 `0`，并在 `stage7_final_verification_report.json` 落盘非敏感 `live_canary` 验收摘要。
  - 局部决策：（1）`codec/chat/legalize.go` 仅在 `dialect.ReasoningHistory != ""`（`convert/dialect.go` 仅对 `route.Client != route.Upstream` 补默认 `HistoryToolTurns`）时将相邻 `RoleAssistant` 回合（如 Codex 在 `function_call` 前拆出的 `phase: "commentary"` 文本消息）归并至工具调用助手消息并前置 `reasoning_content`，单协议 `ir.Dialect{}` 保留连续 `assistant` 消息不动以满足定律 Q2 不动点，归并分支经 `restoreOrDropExtra` 与 `mergeUnknownMaps` 保留同源 Chat `Extra` 字段，并在 `request_rules_chat.go` 增加 `CHAT-THINKING-TOOL-CALL` 严格规则覆盖全量协议夹具；（2）跨协议 `{Responses, Chat} -> Messages` 在 `convert/dialect.go` 对未显式禁用思考且携带 `medium`/`high`/`xhigh`/`max` 档位但未设 `Mode` 与 `BudgetTokens` 的请求补设 `Mode = "enabled"`，并在 `codec/messages/request_encode.go` 覆盖支持 `Budget`（含 `Budget + Efforts` 双控的 `claude-opus-4-6`、`claude-sonnet-4-6`）、未知目录模型（`ControlsKnown == false`）与仅含 `Efforts` 的自适应模型（`claude-opus-4-7`、`claude-sonnet-5` 输出 `thinking.type = "adaptive"` 且不虚增 `max_tokens`），保持单协议 Messages 定律 Q2 与 `claude-haiku-4-5` 出站字节不变；（3）`testkit/essence.go` 的 `coalesceAdjacentMaterializedTurns` 与 `coalesceAdjacentDstToolTurns` 改为按连续同角色段长度对照目标消息定向归并，保留折叠助手回合前未合并的多条连续 `developer`/`user` 消息；`antigravity/dual_baseline_diff_scenario_test.go` 的 `AG6` 谓词补充 `decodedGenerationConfigHasKey(wantDoc, "thinkingConfig")` 校验以恢复纯 `AG9` 探针归因；`gateway/provider/input_tokens_test.go` 按 `trc1:` 载体存在性判定 `D10` 原生输入计数对账；`testkit/corpus_import.go` 纳入 Google `AQ.` 密钥前缀扫描，并在 `tools/clientlab/Dockerfile` 安装 `ca-certificates`、在 `mockupstream/inspect.go` 兼容 OpenAI 顶层 `input` 字符串、在 `client_lab_scenario_test.go` 为 `live` 代理补充瞬态 `429`/`503`/流内并发限制重试传输层，生产默认 `UseIRConversion == false` 行为保持不变；Project Doc 经评估在本批无需改动，`make check-docs` 通过。
  - 待确认项：无。

- 2026-11-26，阶段 8 第 1 至 2 步统一切换至新 IR 转换入口与退役旧转换流程、旧读取器及客户端工具流状态（批次 8.1，提交 `3a440aced`、`21bc692ac`）：删除全仓执行器、运行时 `Options` 与网关分派点中的 `UseIRConversion` 接线开关（含 `UseGeminiIRConversion`、`shouldUseGeminiCountIRConversion`、`shouldUseGrokCountIRConversion`、`UseIRClientToolAdaptation` 等辅助方法）以及生产默认关闭守卫测试 `gateway_ir_conversion_wiring_scenario_test.go`，使全部跨协议分派点、同协议 Responses 客户端工具轻量适配、连接探针与本地/预检计数辅助入口直接调用 `protocol/convert` 与 `protocol/codec/*`；使原生 Messages 全路径（含 `/v1/messages/count_tokens`、Anthropic 各认证类型、Bedrock、Vertex 与兼容平台端点）和原生 Responses 全路径（HTTP、`/v1/responses/input_tokens`、专用 WS、连接池 WS 与 WS HTTP 桥）始终调用 `convert.PrepareNativeReplay`；同时删除第 3.13 节两张表“现有”列的全部旧转换入口与旧读取函数、`gateway/forward.Output` 与 `httpapi.ForwardConversionOutput`、`upstream/client_tools_stream.go` 以及 `ResponsesClientToolMapping` 状态（保留 `CodexToolNameReverse` 与原生命名空间平铺开关逻辑），清理旧配对/差分黄金与 `/legacy` 对照基准并将 `TestClientLabScenarios` 切为直接断言新引擎 100% 通过。
  - 切换与旧流程退役范围摘要：删除 `gateway/forward`（`conversion_output.go`、`conversion_sse.go`、`output.go` 及 `AsChatCompletions`/`AsResponses`）、`openaiforward`（`messages.go`、`messages_ports.go`、`chat.go`、`chat_ports.go`、`raw_fallback.go`、`raw_fallback_ports.go`、`anthropic_output.go` 及按来源协议拆分的单行包装函数）、`messageforward`（`conversion.go` 及 `Runtime.Chat`/`Runtime.Responses`）、`googleforward`（`antigravity_openai.go`、`antigravity_messages.go`、`gemini_openai.go` 及 `Forward`/`ForwardAs*` 门面，同步从 `tools/architecture/catalog.go` 的 `filePermissions` 移除 `gemini_openai_external_test.go`）、`httpapi`（`chat_response_reader.go`、`forward_conversion_output.go`、`openai_cc_response_options.go`、`openai_chat_response_output.go`、`openai_messages_response_output.go`、`output_openai_messages.go`、无生产调用方的 `ForwardAs*` 与 `ForwardQoderAttempt`）、`gateway/provider/model_attributes.go` 的 `ModelConversionOptions`/`GeminiThinking`、`upstream/openai/response_messages.go`、`upstream/gemini`（`exchange_messages.go`、`exchange_openai.go`、`response_openai.go`、`response_completion.go`）、`upstream/antigravity`（`request_transformer.go`、`response_transformer.go`、`stream_transformer.go`、`response_compat_stream.go`、`response_compat_nonstream.go`、`RecoverClaude`、`ClientWriter`、`HandleStreamReadError`）及 `upstream/qoder` 旧 `ParseQoder*Payload`/`WriteQoder*`/`BuildQoder*` 与死辅助函数；严格保留批次 `8.2` 至 `8.5` 范围的 `gateway/session/reasoning_history.go`、`rediscache` `reasoning_content:` 方法、`protocol/bridge`、`claude_types.go`、`testkit/converter.go`、`legacy_baseline.json`、`diff_ledger.json` 与 `protocol/convert/legacy_diff_scenario_test.go` 不变。
  - 局部决策：（1）删文件前先完成同包保留符号迁出：`antigravityCompatProxyURL` 与 `antigravityCredentialRejectedError` 迁入 `googleforward/antigravity_converted.go`，`MaxTokensBudgetPadding` 与 `GetDefaultIdentityPatch` 迁入 `upstream/antigravity/payload.go`（`thinking_request.go` 与 `probe.go` 中 `DummyThoughtSignature` 暂指 `bridge.DummyThoughtSignature`，待批次 `8.3` 迁入 `protocol/gemini`），`DefaultSSELineLimit` 与 `responsesClientToolStreamBody`（含 `Close`）迁入 `upstream/responses_tool_stream.go`，`writeTextResponsesError` 迁入 `httpapi/openai_native_anthropic_ports.go`，原 `exchange_messages_test.go` 中 `ExchangeNative` 单测拆入 `upstream/gemini/exchange_native_test.go`；（2）收敛无开关辅助函数（`gateway/provider/input_tokens.go`、`gateway/tokenestimate/estimate.go`、`gateway/provider/openai_responses_ingress_compat.go`、`upstream/antigravity/probe.go`、`provider/provider/qoder_test_execution.go` 及 `test_streams.go`）并同步刷新 `stage7_verification_scenario_test.go` 与 `stage7_routes_coverage.json` 中的 6 条 T3 测试名；（3）修复单测捕获的两处边界：`openaiforward/anthropic_converted_output.go` 的 `ReadConvertedAnthropicBuffered` 在 `ErrAnthropicStreamIdle` 分支直接写 HTTP 502 并返回 `collector.PartialResult(), scanErr` 以保留 `message_start`/`message_delta` 已观测到的部分 `NativeUsage`；`upstream/grok/request_patch.go` 的 `PatchGrokResponsesBodyWithToolMap` 在非空客户端 `tools` 经 `AdaptResponsesTools` 降为空切片 `[]` 时删除顶层 `"tools"` 键以维持缓存键稳定；（4）按文件布局规则将不足 30 有效行的 `httpapi/openai_protocol_execution_messages.go` 与 `openai_protocol_execution_chat.go` 合并为 `openai_protocol_execution.go`（配对测试合并为 `openai_protocol_execution_test.go`），删除死类型 `ChatFailure` 并将 `CompatResponseResult` 与 `NewCompatStreamHeaderWriter` 并入 `upstream/openai/response_converted.go`（删除 `response_chat.go`），在 `buildClientLabRoutes()` 中将 Gemini 路线模型对齐为可开关思考的 `gemini-2.5-flash`，并同步更新各包 `doc.go` 与 Project Doc（`docs/architecture/{protocol_conversion,gateway_request_lifecycle}.md`、`docs/interfaces/{antigravity_upstream,anthropic_upstream,openai_upstream,gemini_upstream}.md`）。
  - 待确认项：无。

- 2026-11-26，阶段 8 第 3 步退役推理连续性旧缓存机制（批次 8.2，提交 `68edcc852`、`036b1b815`）：删除 `backend/internal/gateway/session/reasoning_history.go`、`session.ReasoningContentCache` 接口、`gateway/rediscache` 中的 `SetReasoningContent`、`GetReasoningContent`、`reasoning_content:` 键前缀常量与 `7d` 默认 TTL 及其单元和集成测试，移除 `provideReasoningHistory` 的 Wire 装配绑定（更新 `wire_gen.go`）、`httpapi.OpenAIResponseOutput.Reasoning` 字段以及 `internal/app`、`gateway/httpapi`、`gateway/ws/httpapi` 测试夹具中的内存桩与断言；Redis 存量 `reasoning_content:` 旧键按原有 TTL 自然过期，不编写清理脚本或迁移。
  - 局部决策：（1）随 `ReasoningContentCache` 与 `ReasoningHistory` 一并删除已无任何调用方的哨兵错误 `session.ErrReasoningContentNotFound`（`gateway/session/stores.go`）与专供 `ReasoningHistory.Warn` 使用的告警回调 `gateway/provider.WarnReasoningCacheFailure`（`gateway/provider/ws_diagnostics.go`），避免遗留死符号；（2）`gateway/session/stores.go` 移除 `ReasoningContentCache` 后为 26 有效行，因其承载包内全部 4 个跨模块网关缓存能力接口（具备明确独立主题且在 `session/doc.go` 独立归组），按后端文件布局规则保留独立文件并更新文件总览；（3）以锁定的 `github.com/google/wire/cmd/wire@v0.7.0` 重生成 `backend/internal/app/wire_gen.go`，避免未带版本后缀的 `go generate` 污染应用 `go.sum`；（4）同步更新 `gateway/session/doc.go`、`openaiforward/raw_fallback_converted{,_test}.go`、`docs/architecture/backend_modules.md`（`gateway/session` 去掉“推理历史”）、`docs/interfaces/openai_upstream.md`（删除 `ReasoningHistory` 段落）与 `docs/architecture/protocol_conversion.md`（去掉表格中“不查 Redis `ReasoningHistory`”的过渡附注，保留 `Dialect.ReasoningHistory` 说明），保持 `docs/operations/upgrade_notes.md` 留待批次 `8.6` 编写。
  - 待确认项：无。

- 2026-11-27，阶段 8 第 4 步迁出原生保留符号并删除 `protocol/bridge`（批次 8.3，提交 `4e4c46d96`、`3ff2b09f6`、`560c2e34a`）：核验 `protocol/openai`（字节级最新轮裁剪与 `TodoGuard`）及 `protocol/anthropic`（完整重放尾部裁剪）中的续接策略均已独立就位且无 `bridge` 残留；将原生流/非流式输出重构与原生命名空间开关及 Compact 仍使用的保留符号连同单元测试迁入 `protocol/openai`，将 Gemini/Antigravity 原生请求规范化、内联图片 MIME 判定、历史 `functionCall` 占位签名补齐、用量提取投影及 JSON Schema 清洗符号连同单元测试迁入 `protocol/gemini`；将全仓 21 个保留生产调用文件与 8 个保留外部测试/基准文件切至 `protocol/openai` 和 `protocol/gemini`，删除 `backend/internal/protocol/convert/legacy_diff_scenario_test.go`、仅由其消费的 `convert/testdata/golden/diff/tool_names.json` 及 `convert/benchmark_scenario_test.go` 中的 `/legacy` 对照子基准，并整目录删除 `backend/internal/protocol/bridge`（`rg 'protocol/bridge"' backend` 为 0 匹配）。
  - 迁出与清理范围摘要：迁入 `protocol/openai` 的符号包括 `BufferedResponseAccumulator`、`NewBufferedResponseAccumulator`、`ResponsesStreamOutputItems`、`NewResponsesStreamOutputItems`、`NormalizeResponsesStreamingTerminalOutput`、`ReconstructResponseOutputFromSSE`（含 `CollectRawResponsesOutputItemsFromSSE`、`BuildResponsesOutputJSON`）以及 `NamespacedToolName`、`ResponsesNamespaceName`、`FlattenResponsesNamespaces`、`FlattenResponsesNamespacesExcept`、`RestoreResponsesNamespaceCalls`、`StripOpenAIResponsesInputNamespaces`、`IsOpenAIResponsesToolCallItemType`；迁入 `protocol/gemini` 的符号包括 `NativeNormalizeGeminiRequestForAIStudio`、`NativeIsGeminiInlineImageMIMEType`、`DummyThoughtSignature`、`NativeEnsureGeminiFunctionCallThoughtSignatures`（签名精简为 `(dummySignature string, body []byte) []byte`，去掉旧 `NativeGeminiOptions` 包装）、`NativeGeminiUsage`、`NativeExtractGeminiUsage`、`NativeUsageProjection`、`CleanJSONSchema` 与 `DeepCleanUndefined`；`upstream/antigravity/execute.go` 原生路径的原 `CompatJSONHasContent` / `CompatOutputMeaning` 调用统一改走 `gemini.ObservePayload`；同步刷新 `gateway/provider/stage7_verification_scenario_test.go` 与 `stage7_routes_coverage.json`（移除已退役的 `legacy_diff_scenario_test.go` 引用，保持 Stage 7 全部路线 T3 覆盖有效），严格保留批次 `8.4` 与 `8.5` 范围的 `protocol/anthropic/claude_types.go`、`protocol/{openai,anthropic}/types.go` 未用线类型、`testkit/converter.go`、`legacy_baseline.json`、`diff_ledger.json` 以及 `tools/architecture` 中的 `protocol/bridge` 许可条目待后续批次清理。
  - 局部决策：（1）`protocol/openai` 按职责新增 `response_reconstruction{,_test}.go` 与 `responses_namespace{,_test}.go`，手写生产源文件增至 21 个，同步在 `protocol/openai/doc.go` 登记新文件分组；（2）在 `tools/architecture/catalog.go` 的 `leafDependencies["internal/protocol/gemini"].Production` 中补充 `"internal/protocol"`（同步补充 `policy_test.go` 断言）以承载返回 `*protocol.TokenUsage` 的 `NativeUsageProjection`；`protocol/gemini` 新增 `aistudio_request{,_test}.go` 与 `schema{,_test}.go`，并将 `NativeIsGeminiInlineImageMIMEType`、用量提取投影、占位签名补齐分别并入同主题的 `images{,_test}.go`、`observation{,_test}.go` 与 `signature_cleaner{,_test}.go`（手写生产源文件共 9 个，无需 `doc.go`）；（3）删除 `convert/legacy_diff_scenario_test.go` 后，按 `helpers_test.go` 仅放多文件共用夹具的布局规则，将仅单文件消费的 `toolCallNameRecord` / `extractUpstreamToolsAndCalls` 迁入 `convert/tools_responses_test.go`，将 `streamEvalContext`（同时移除无读取方的 `scenario` 与 `clientIncludeUsage` 死字段）及流标记/Q4 校验辅助函数迁入 `convert/conformance_scenario_test.go`；（4）同步更新 `docs/architecture/backend_modules.md` 中 `internal/protocol` 的子包列表（移除已删除的 `bridge`，保留实际的 `anthropic`、`codec`、`convert`、`gemini`、`google`、`grok`、`ir`、`openai`、`qoder`、`sse`、`testkit`、`wirejson`），并清理改动调用点与测试中的过时 `bridge`、双引擎及 `GeminiThinking` 注释表述。
  - 待确认项：无。

- 2026-11-27，阶段 8 第 5 步删除只被旧转换器使用的线类型及同包旧转换辅助代码（批次 8.4，提交 `323796799`）：核对 `upstream/antigravity/recovery_claude.go`、`probe.go` 及全仓各构建标签源码后，整文件删除 `backend/internal/protocol/anthropic/claude_types.go`（含 `ClaudeRequest`、`ClaudeMessage`、`ThinkingConfig`、`ClaudeMetadata`、`ClaudeTool`、`CustomToolSpec`、`ClaudeCustomToolSpec`、`SystemBlock`、`ContentBlock`、`ImageSource`、`ClaudeResponse`、`ClaudeContentItem`、`ClaudeUsage`、`ClaudeError`、`ErrorDetail`、`ClaudeModel` 共 16 个类型定义）、仅接收 `*ClaudeRequest` 的 `signature_repair.go` 与 `signature_repair_test.go`，以及仅服务已退役旧流式编码器的 `backend/internal/protocol/openai/responses_stream_event_wire.go`；同步清理 `protocol/anthropic` 与 `protocol/openai` 中零调用方的旧转换专用类型、字段、JSON 序列化方法和辅助函数。
  - 清理与保留范围摘要：`protocol/anthropic` 额外删除 `AnthropicContentBlock.MarshalJSON`、`AnthropicStopReasonPtr`、`AnthropicStopReasonString`（`types.go`）、`NormalizeUsage` 及其单测（`usage_merge{,_test}.go`）、`StripThinkingSignaturesJSON` 及其单测（`signature_json{,_test}.go`，同步移除无用 `wirejson` 导入），手写生产源文件减至 12 个；`protocol/openai` 额外删除 `ChatContentPart`、`ChatImageURL`、`ChatFile`、`ResponsesInputItem.outputRaw` 与 `ResponsesInputItem.RawOutput`、`ChatMessage.ReasoningText`（`types.go`）以及 `ChatCompletionsChunkHasToolCallDelta`（`chat_stream_compat.go`），除 `doc.go` 外手写生产源文件减至 34 个；严格保留批次 `8.5` 范围的 `testkit/converter.go`、`legacy_baseline.json`、`diff_ledger.json` 与 `tools/architecture/catalog.go` 中的 `protocol/bridge` 许可条目待下批清理。
  - 局部决策：（1）删除 `claude_types.go` 前将仍被 `gateway/clientmeta` 与 `upstream/anthropic` 引用的常量 `ClaudeCodeBillingHeaderPrefix` 和 `ClaudeCodeEntrypointMarker` 迁入同包 `protocol/anthropic/types.go`，保持包路径与符号名不变；（2）核查发现 `upstream/openai/response_converted.go` 仍对 `*ResponsesResponse`（内含 `[]ResponsesOutput`）执行 `json.Marshal`，且多处流读取器把含对象参数 `tool_search_call` 的事件反序列化进 `ResponsesStreamEvent`，故保留 `ResponsesOutput` 的对称 `MarshalJSON`/`UnmarshalJSON` 并将 `ToolSearchCallArgumentsJSON` 迁入 `protocol/openai/types.go`；（3）对 `session/anthropic_prompt_cache.go`、`gateway/provider/*`、`gateway/tokenestimate/*`、`gateway/httpapi/*`、`provider/*`、`upstream/openai/*`、`upstream/grok/*` 与 `protocol/codec/*` 仍在使用的 `Anthropic*`、`Responses*`、`Chat*`、`TextProtocol` 线类型原样保留，同步更新 `protocol/openai/doc.go` 文件分组及 `anthropic/types.go`、`anthropic/signature_json.go`、`openai/types.go` 的总览与声明注释。
  - 待确认项：无。

- 2026-11-27，阶段 8 第 6 至 7 步清理旧基线与差分基础设施及移除 `protocol/bridge` 架构许可（批次 8.5，提交 `e767d3458`、`ce18fc5cc`、`f01447a21`）：删除 `backend/internal/protocol/testkit/converter.go`（及 `converter_test.go`）、`diff_ledger.go`（及 `diff_ledger_test.go`）、`testdata/legacy_baseline.json`、`testdata/diff_ledger.json` 以及仅服务于旧矩阵测试的过渡导出（`SSEFrame`、`ConversionLoss`、`GoldenUpdateEnabled`），同步更新 `protocol/testkit/doc.go`（删除后仍含 21 个非测试手写源文件）；从 `tools/architecture/catalog.go` 的 `leafDependencies`、`moduleDependencies`（`internal/gateway`、`internal/upstream`）和五个 `platformDependencies`（`internal/upstream/{antigravity,gemini,grok,openai,qoder}`）彻底移除 `internal/protocol/bridge` 的全部依赖许可，将 `internal/protocol/convert` 的 `Tests` 依赖收紧为 `internal/protocol/testkit`，核验 `filePermissions` 等文件级例外无过时条目，并更新 `tools/architecture/policy_test.go` 正反例断言。
  - 局部决策：（1）按多文件共用测试辅助放入 `helpers_test.go` 的布局规则，在 `backend/internal/app/helpers_test.go` 提取六字段 `labCellResult`（`Client`、`Upstream`、`Scenario`、`Law`、`Status`、`Rules`，移除无读取方的 `Route` 字段）及 `labCellPass`、`labCellFail`、`labCellSkip` 常量，替换 `protocol_lab*_test.go` 与 `client_lab_scenario_test.go` 中的 `LegacyBaselineEntry`、`BaselineStatus*` 与 `ReadEmbeddedLegacyBaseline`（`TestClientLabConfigTemplatesAndGuards` 改为直接校验 25 条 `client_lab` 路线表不变量），将仅单文件使用的 `clientLabTestdataDir` 与 `isStage2TextUpstream` 收回 `client_lab_scenario_test.go`，并同步更新 `protocol_lab*_test.go`、`client_lab_scenario_test.go` 与 `qoder/benchmark_scenario_test.go` 中涉及旧基线的注释；（2）将 `codec/{chat,messages,generatecontent}/conformance_scenario_test.go` 中的 `testkit.SSEFrame` 改为直接引用 `sse.Frame`，将 `policy_test.go` 中的合成夹具路径更新为现存的 `conformance_scenario_test.go` 与 `openaiforward/responses_converted.go`；（3）核对全仓黄金与语料夹具，完整保留 `app/testdata/golden/{routes.txt,stage7_routes_coverage.json}`、`testkit/testdata/{wire,corpus}/`、`convert/testdata/golden/{census,loss}/` 及四个 `codec/*/testdata/golden/ir/`，第 8 至 9 步的工程文档全面收尾、`docs/operations/upgrade_notes.md` 编写与全仓旧关键词清扫留待批次 `8.6`。
  - 待确认项：无。

- 2026-11-27，阶段 8 第 8 至 9 步全面同步线上 Project Doc 工程文档、编写 `upgrade_notes.md` 升级说明、全仓旧关键词与退役符号注释清扫及本地全量验收（批次 8.6，提交 `c7b0dedd4`、`46829c0b1`、`af9b18e0c`）：按第 7 节逐一核对并更新 `docs/architecture/{protocol_conversion,backend_modules,gateway_request_lifecycle}.md`、`docs/interfaces/{protocol_capabilities,anthropic_upstream,openai_upstream,gemini_upstream,antigravity_upstream,grok_upstream,qoder_upstream,opencode_upstream}.md` 与 `docs/operations/development_workflow.md`（保留全部 `<a id="..."></a>` 稳定锚点与 `@project-doc` 对应关系），按阶段 7 第 6 步汇总要点在 `docs/operations/upgrade_notes.md` 新增 `<a id="ir_protocol_conversion"></a>` 未发布升级条目（覆盖单版本无运行时回退开关整体切换、`trc1:` 信封与旧会话裸签名丢弃续聊边界、首轮提示缓存命中行为、`reasoning_content:` 旧键 7 天自然过期、回退限制及 `D6` 至 `D14` 可感知差异摘要），完成全仓旧包名、旧转换符号与阶段过渡注释扫描清理（覆盖 26 个手写 Go 文件与 `tools/clientlab/README.md`，`rg -n "bridge\." backend/internal --type go` 指向旧包的引用为 `0`），并通过本地全量验收。
  - 本地全量验收摘要：`make fmt-check`、`make check-docs`、`make lint`（`tools/architecture`、带 `integration` 标签的 `golangci-lint` `0 issues`、前端与双 SDK 静态检查全绿）、`make test-tools`（Python 与 `declorder` 全绿）、`make test`（后端 `test-go`、前端 470 个测试文件共 3,690 项测试、Go 与 Web 插件 SDK 全绿）、`make test-integration`（全部 44 个容器集成包通过）、`make build`、`make test-embed`、`make test-scripts </dev/null`（排查确认 `deploy/tests/install-github-token-test.sh` 匿名用例假 `curl` 内 `cat` 在交互式打开 PTY 标准输入下等待 EOF，关闭标准输入后全部 9 个部署测试脚本通过）、`make test-installer` 与 `make test-clientlab`（T6 脚本模式全部真实 CLI 矩阵通过）均以退出码 `0` 通过。
  - 局部决策：（1）遵守 Project Doc 精炼易扫与单一事实来源规则，将管线细节集中在 `docs/architecture/protocol_conversion.md`，各接口与生命周期文档以稳定锚点互引并控制新增或改写段落与列表项全部低于 250 字符；（2）在 `docs/interfaces/openai_upstream.md` 同步修正原遗留的“流末聚合 `tool_calls`”描述为实时流式写出 `input_json_delta` 加 `ir.Sequencer` 并行槽位顺序化，在 `docs/interfaces/anthropic_upstream.md` 将原“过滤全部 OpenAI `reasoning`”更新为 `trc1:anthropic` 解封还原、`passback` 工具轮无签名思考保留及 `PrepareNativeReplay` 规则，在 `docs/interfaces/antigravity_upstream.md` 与 `modelrules/antigravity.go` 对齐 `claude-*` 模型和全部 Chat 转换路线 `64000` 上限、非 `claude-*` 的 Messages 与 Responses 转换路线 `65000` 上限以及全部转换请求预留 `1000` token `BudgetPadding` 的精确口径；（3）保留 `tools/architecture/policy_test.go` 中针对 `internal/protocol/bridge` 的 `allowed: false` 禁导反例断言，清理 `protocol/{ir,convert,codec/generatecontent,gemini,testkit/mockupstream}`、`gateway/{session,httpapi,provider/*}`、`upstream/{antigravity,qoder,responses_tool_stream.go}` 与 `app/protocol_routes_scenario_test.go` 中残留的旧 `bridge`、已删除转换函数、裸 `gAAAA` 前缀嗅探对比及阶段过渡注释。
  - 待确认项：无。

### 阶段 3 差异裁决归档（P8 至 P10，已结案）

以下 3 组（原 4 个 `pendingID`）已获用户裁决通过，正式纳入第 6.2.4 节验收差异清单的 `D5`、`D12`、`D13` 类别并迁入 `backend/internal/protocol/testkit/testdata/diff_ledger.json`，`backend/internal/protocol/convert/legacy_diff_scenario_test.go` 中的内存态 `pendingID` 豁免已全部移除并断言待确认项为 `0` 条：

1. P8 `messages_to_gemini` 请求侧异源托管搜索历史块作为 `OpaqueItem` 丢弃（已迁入 `D5`）：旧实现 `bridge.NativeConvertClaudeMessagesToGeminiContents`（`gemini_native.go:731-735`）遇到未知 Anthropic 内容块类型 `server_tool_use` 与 `web_search_tool_result` 时未将其丢弃或合成空壳，却调用 `json.Marshal` 将整段原始块 JSON（含搜索查询、结果 URL、标题与 `encrypted_content`）序列化进发往 Gemini 的 `model` 角色 `text` part；新实现按第 3.4 节 `OpaqueItem` 跨协议丢弃规则丢弃这两个异源专属块并记录 `opaque_item_dropped`，同时完整保留同轮正文 `"Go 1.26 was released in February 2026."`，通过严格请求校验器且满足定律 Q4 与非 `OpaqueItem` 块语义等价校验。复现夹具：`messages_to_gemini` 上的 `wire/anthropic_messages/request/tools_hosted_search.json`。
2. P9 `messages_to_gemini` 请求侧剥离 Claude Code `x-anthropic-billing-header` 客户端遥测标记（已迁入 `D13`）：旧实现 `bridge.NativeExtractClaudeSystemText`（`gemini_native.go:540-564`）缺少 `AnthropicToResponses` 与 `AnthropicToChatCompletionsRequest` 已具备的 `isAnthropicBillingHeaderText` 过滤，将 Claude Code `system[0]` 的 `"x-anthropic-billing-header: cc_version=2.1.330.975; cc_entrypoint=sdk-cli;"` 直接拼入发往 Gemini 的 `systemInstruction.parts[0].text`；新实现按第 3.7 节与第 3.8 节第 3 步统一剥离该客户端遥测标记并记录 `client_marker_dropped`，出站不含 `x-anthropic-billing-header:`，其余系统提示块无损保留并通过定律 Q3 与 Q4 校验（同批离散多块保留部分单独归入 `D2`）。复现夹具：`messages_to_gemini` 上的 `corpus/clients/claude-code/2.1.330/lab_text/1.request.json`、`corpus/clients/claude-code/2.1.330/lab_text_bare/1.request.json`、`corpus/clients/claude-code/2.1.330/lab_tool/1.request.json`、`corpus/clients/claude-code/2.1.330/lab_tool/2.request.json`、`corpus/clients/claude-code/2.1.330/lab_tool_bare/1.request.json`、`corpus/clients/claude-code/2.1.330/lab_tool_bare/2.request.json`。
3. P10 `messages_to_gemini` 流式无 `args` 函数调用补发规范 `input_json_delta("{}")` 与 `trc1:gemini` 连续性 `signature_delta` 帧引起的 `stream_flags` 序列变化（已迁入 `D12(a)` 与 `D12(b)`）：（a）在省略 `args` 字段的无参 Gemini `functionCall` 流上，旧实现 `bridge.NativeGeminiMessagesStream`（`gemini_messages_stream.go:234-246`）仅发出带 `"input":{}` 的 `content_block_start` 而不发 `input_json_delta`，新实现共用第 3.5 节与第 4.1 节的 `codec/messages/stream_encode.go` 在闭合工具块前统一刷出 1 帧 `input_json_delta("{}")`（`Semantic=true, Terminal=false`，块级累计规范 JSON 仍为 `{}`）；（b）在携带 `thoughtSignature` 的 Gemini 思考流上，新实现按第 3.10 节与第 6.2.4 节 `D6` 将签名封入 `trc1:gemini` 信封并在思考块闭合前发出 1 帧 `signature_delta(Semantic=true, Terminal=false)`，旧实现未写签名故无该帧，剔除授权帧后其余全部共享帧的 `(Semantic, Terminal)` 标记逐帧一致。复现夹具：`messages_to_gemini` 上的 `wire/gemini_generate_content/stream/tool_args_empty.sse`，以及 `messages_to_gemini` 上的 `wire/gemini_generate_content/stream/legacy_history.sse`、`wire/gemini_generate_content/stream/reasoning_carrier_roundtrip.sse`、`wire/gemini_generate_content/stream/reasoning_encrypted_only.sse`、`wire/gemini_generate_content/stream/reasoning_summary.sse`、`wire/gemini_generate_content/stream/reasoning_token_foreign.sse`、`wire/gemini_generate_content/stream/reasoning_tool_replay.sse`。

### 阶段 4 第 2 步 Antigravity 双基准差分裁决归档（AG1 至 AG11，已结案）

按第 6.5 节第 2 步与用户裁决，`AG1` 至 `AG11` 共 11 项差异已全部采用选项 A（即当前统一单跳新实现行为），作为第 6.2.4 节 `D14` 验收类别正式生效，同步迁入 `backend/internal/protocol/testkit/testdata/diff_ledger.json`（`stage4_antigravity` 作用域，共 `245` 条记录）与 `backend/internal/upstream/antigravity/testdata/golden/stage4_dual_baseline_diff.json`（`pending_adjudication_count = 0`、`unclassified_diff_items = 0`）。`TestStage4AntigravityDualBaselineCorpusDiff` 在全量规范线夹具、客户端录制语料（含阶段 5 新增的 `lab_custom_tool` 语料）与 `probe/*` 探针共 `252` 次分组评估（加 4 条 `AG11` OpenAI 兼容网关透传探针）上同步执行定律 Q3、Q4（`VerifyEssence`）、Q5（确定性）及 `D14` 选项 A 线级不变量核验，对照 `TransformClaudeToGeminiWithOptions` 命中 `2,096` 项精确叶子路径差异，对照 `NativeConvertClaudeMessagesToGeminiGenerateContent` 命中 `1,002` 项精确叶子路径差异，未归类差异 `0` 条，待裁决项已清空。

#### 1. 来源协议乘模型族六分组概览（含旧生产网关实际选用路径）

| 分组 ID | 来源协议 | 模型族（代表模型） | 旧生产网关（`UseIRConversion == false`）实际选用管线 | 评估夹具数 | 对照旧基线 A 零差异数 | 对照旧基线 B 零差异数 | 相对旧生产实际管线变化维度 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `messages_claude` | `anthropic_messages` | `claude-*`（`claude-sonnet-4-5`、`claude-opus-4-6-thinking`） | 旧基线 A：`antigravity.TransformClaudeToGeminiWithOptions`（`antigravity_messages.go:127`） | 45（其中 6 个 `thinking: disabled` 语料三方一致拒绝） | 0 / 39 | 15 / 39 | `AG1`、`AG2`、`AG3`、`AG4`、`AG5`、`AG6`、`AG7`、`AG9`、`AG10`（`AG8` 与旧生产一致） |
| `messages_gemini` | `anthropic_messages` | `gemini-*`（`gemini-2.5-flash`、`gemini-3.1-pro-high`） | 旧基线 A：`antigravity.TransformClaudeToGeminiWithOptions`（`antigravity_messages.go` 未按 `gemini-*` 分支） | 45 | 0 / 45 | 15 / 45 | `AG1`、`AG2`、`AG3`、`AG4`、`AG5`、`AG6`、`AG7`、`AG8`、`AG9`、`AG10` |
| `responses_claude` | `openai_responses` | `claude-*`（`claude-sonnet-4-5`、`claude-opus-4-6-thinking`） | `AdaptResponsesClientToolsForAnthropic` -> `ResponsesToAnthropicRequest` -> `prepareAntigravityCompatTools` -> 旧基线 A（`antigravity_openai.go:504`） | 47（其中 5 个 `effort: none` 语料三方一致拒绝） | 0 / 42 | 14 / 42 | `AG1`、`AG2`、`AG3`、`AG4`、`AG5`、`AG6`、`AG7`、`AG9`、`AG10`、`AG11`（`AG8` 与旧生产一致） |
| `responses_gemini` | `openai_responses` | `gemini-*`（`gemini-2.5-flash`、`gemini-3.1-pro-high`） | `AdaptResponsesClientToolsForAnthropic` -> `ResponsesToAnthropicRequest` -> `prepareAntigravityCompatTools` -> 旧基线 B 加 Antigravity 后处理（`antigravity_openai.go:463-490`） | 47 | 0 / 47 | 15 / 47 | `AG1`、`AG2`、`AG3`、`AG4`、`AG6`、`AG7`、`AG8`、`AG9`、`AG10`、`AG11`（`AG5` 与旧生产一致） |
| `chat_claude` | `openai_chat_completions` | `claude-*`（`claude-sonnet-4-5`、`claude-opus-4-6-thinking`） | `ChatCompletionsToResponses` -> `ResponsesToAnthropicRequest` 加 `PreserveChatCompletionTokenLimit` -> `prepareAntigravityCompatTools` -> 旧基线 A（`antigravity_openai.go:504`） | 34 | 0 / 34 | 13 / 34 | `AG1`、`AG2`、`AG3`、`AG4`、`AG5`、`AG6`、`AG7`、`AG9`、`AG10`、`AG11`（`AG8` 与旧生产一致） |
| `chat_gemini` | `openai_chat_completions` | `gemini-*`（`gemini-2.5-flash`、`gemini-3.1-pro-high`） | `ChatCompletionsToResponses` -> `ResponsesToAnthropicRequest` 加 `PreserveChatCompletionTokenLimit` -> `prepareAntigravityCompatTools` -> 旧基线 B 加 Antigravity 后处理（`antigravity_openai.go:463-490`） | 34 | 0 / 34 | 14 / 34 | `AG1`、`AG2`、`AG3`、`AG4`、`AG6`、`AG7`、`AG8`、`AG9`、`AG10`、`AG11`（`AG5` 与旧生产一致） |

#### 2. 逐维度双基准对比与用户裁决结果（全部采纳选项 A，已纳入第 6.2.4 节 D14）

| 编号 | 差异维度 | 旧基线 A（`TransformClaudeToGeminiWithOptions`） | 旧基线 B（`NativeConvertClaudeMessagesToGeminiGenerateContent` 加后处理） | 当前新实现（`convert.NewExchange` 加身份补丁、`CleanGeminiRequest`、`WrapV1InternalRequest`） | 典型复现夹具 | 用户裁决结果（已纳入 `D14`） |
| --- | --- | --- | --- | --- | --- | --- |
| `AG1` | `functionResponse.response` 载荷键（`result` 对比 `content` 对比 `error`/JSON 对象）与工具结果图片 | 始终包为 `{"result": "<string>"}`；空串无论 `is_error` 与否均替换为 `"Command executed successfully."` 或 `"Tool execution failed with no output."`；`is_error: true` 仍用 `"result"` 键；JSON 字符串不解包为对象；工具结果内嵌图片抽为同轮 `inlineData` | 始终包为 `{"content": "<string>"}`（JSON 对象字符串与错误结果同样包成字符串）；静默丢弃 `tool_result` 中的图片块 | 在 `SchemaAntigravity` 下纯文本包为 `{"result": "<string>"}`（空串保留 `{"result": ""}`），`IsError == true` 包为 `{"error": "<string>"}`，无保留键冲突的结构化 JSON 对象直接作为 `response` 对象透传，工具结果媒体拆为相邻 `inlineData` / `fileData` 部件（命中 A `241` 项、B `302` 项，涉及 71 个分组夹具） | `probe/*/call_memory_json_and_empty_results`、`wire/*/request/tool_result_error.json`、`wire/*/request/tool_result_image.json`、`corpus/clients/pi/1.6.0/lab_tool_*/2.request.json` | 已裁决采用选项 A：保持 `SchemaAntigravity` 的 `{"result": ...}` / `{"error": ...}` / 结构化 JSON 对象直传与媒体保留，不注入英文空串占位句 |
| `AG2` | `functionCall.id` 与 `functionResponse.id` 保留及 `CallMemory` 重写 | 无条件原样写出客户端 `id`，不查询 `CallMemory` | 仅在 `LookupToolCall` 命中非空上游 ID 时写出 `id`，未命中时一律剥离客户端 `id`（且旧 `antigravity_openai.go` 未向 `NativeConvert` 注入 `LookupToolCall`，实际始终剥离全部 `id`） | 在 `SchemaAntigravity` 下，`CallMemory` 未命中或记录的 `UpstreamID == ""` 时保留客户端 `id`（避免 Claude 模型报缺 `id`）；`CallMemory` 在同 `ReplayScheme` 下命中不同 `UpstreamID` 时同步重写 `functionCall.id` 与 `functionResponse.id` 并记 `LossIDRewritten`；跨方案条目忽略（命中 A `24` 项、B `172` 项，涉及 70 个分组夹具） | `probe/*/call_memory_json_and_empty_results`、`corpus/clients/claude-code/2.1.330/lab_tool/2.request.json`、`corpus/clients/codex/0.171.0/lab_tool/2.request.json` | 已裁决采用选项 A：两模型族统一在未命中时保留客户端 `id`，命中同方案不同 `UpstreamID` 时重写并记 `id_rewritten` |
| `AG3` | `request.toolConfig` 强制 `VALIDATED` 对比客户端 `tool_choice` 映射 | 只要 `!OmitEmptyToolConfig \|\| len(tools) > 0` 即无条件注入 `functionCallingConfig: {"mode": "VALIDATED"}`，完全忽略客户端显式 `tool_choice`（`none`、`any`/`required`、指定工具、`allowed_tools`） | 完全省略 `functionCallingConfig`（同样忽略客户端 `tool_choice`），仅在搜索工具与普通函数并存时经 `EnableMixedGeminiToolInvocations` 写 `includeServerSideToolInvocations: true` | 客户端未传 `tool_choice` 时省略 `functionCallingConfig`；显式传入时忠实映射为 `AUTO`、`NONE` 或 `ANY`（附 `allowedFunctionNames`）；函数工具与 `googleSearch` 并存时同样补 `includeServerSideToolInvocations: true`（命中 A `246` 项、B `63` 项，涉及 238 个分组夹具） | `probe/*/premarked_identity_and_tool_choice_any`、`wire/*/request/tool_single.json`、`corpus/clients/*` | 已裁决采用选项 A：按客户端显式 `tool_choice` 映射并在未指定时省略 `functionCallingConfig`，不强制注入 `VALIDATED` |
| `AG4` | `systemInstruction` 包装、运行时自定义 `identity_patch_prompt`、`filterOpenCodePrompt`、`mcpXMLProtocol`、`SYSTEM_PROMPT_END` 与计费头剥离 | 写 `systemInstruction.role = "user"`；读取管理员每请求设置 `identity_patch_prompt`（空串回退内置默认提示词），未含 `"You are Antigravity"` 时注入身份提示词加反泄露免责声明段；用 `filterOpenCodePrompt` 截断 `"You are an interactive CLI tool"` 开头的提示词；存在 `mcp__*` 工具时追加 `mcpXMLProtocol`；末尾追加 `"\n--- [SYSTEM_PROMPT_END] ---"`；Messages 路线未剥离 `x-anthropic-billing-header:` | 不写 `role`；`InjectIdentityPatchToGeminiRequest` 固定前置内置默认 `antigravityIdentity`（忽略管理员 `identity_patch_prompt` 设置）；多个系统块以 `"\n"` 合并为单块；Messages 路线未剥离 `x-anthropic-billing-header:`；无免责声明、`filterOpenCodePrompt`、`mcpXMLProtocol` 与 `SYSTEM_PROMPT_END` | 不写 `role`；`ForwardConverted` 每请求读取 `s.Gateway.GetIdentityPatchPrompt` 并经 `InjectIdentityPatchToGeminiRequestWithPrompt` 无损前置自定义或默认身份提示词（已含 `"You are Antigravity"` 标记时幂等跳过）；剥离 `x-anthropic-billing-header:` 并记 `client_marker_dropped`；保留离散系统块与移位的 `system`/`developer` 消息原文（不截断 CLI 提示词）；不注入免责声明、`mcpXMLProtocol` 与 `SYSTEM_PROMPT_END`（命中 A `795` 项、B `58` 项，涉及 241 个分组夹具） | `probe/*/custom_identity_patch_prompt`、`probe/*/mcp_opencode_metadata_schema`、`probe/anthropic_messages/claude_code_billing_header_and_tools`、`wire/*/request/system_roles.json` | 已裁决采用选项 A：保持 `InjectIdentityPatchToGeminiRequestWithPrompt`（两模型族统一生效管理员自定义 `identity_patch_prompt`）加离散系统块保留与计费头剥离 |
| `AG5` | `request.sessionId` 派生与外层 `v1internal` `requestType` | 填充 `request.sessionId`（优先取非空 `metadata.user_id` 原文，否则对首个 `user` 轮首个文本部件算 SHA-256 取 63 位十进制负数 `-<n>`，无文本时取随机负数）；含网页搜索工具时外层 `requestType` 设为 `"web_search"`，其余为 `"agent"` | `WrapV1InternalRequest` 不写 `request.sessionId`，外层 `requestType` 固定为 `"agent"` | 调用 `WrapV1InternalRequest`，不写 `request.sessionId`，外层 `requestType` 固定为 `"agent"`（与旧基线 B 零差异，命中旧基线 A `256` 项，涉及 241 个分组夹具） | `probe/*/mcp_opencode_metadata_schema`、`probe/*/web_search_and_code_execution`、`wire/*/request/tools_hosted_search.json` | 已裁决采用选项 A：保持 `WrapV1InternalRequest` 现行契约（不发 `request.sessionId`，外层 `requestType` 固定 `"agent"`） |
| `AG6` | 历史思考块、`trc1:` 信封、无签名降级（含清空 `thinkingConfig`）与 `skip_thought_signature_validator` | 原样透传裸客户端签名；`claude-*` 将无签名思考降级为文本并清空当轮 `generationConfig.thinkingConfig`；`gemini-*` 给无签名思考填 `skip_thought_signature_validator` 并在末尾 `model` 轮补 `"Thinking..."`；丢弃 `redacted_thinking`；OpenAI 桥将 Chat `reasoning_content` 拼成 `<thinking>...</thinking>` 正文标签 | 丢弃全部历史思考块且仍保留当轮 `generationConfig.thinkingConfig`；`gemini-*` 对并行步骤中每个无签名 `functionCall` 都填 `skip_thought_signature_validator`；OpenAI 桥同样将 Chat `reasoning_content` 拼成 `<thinking>` 正文标签 | 解封同方案 `trc1:` 信封（`claude-*` 用 `antigravity_claude` 同步还原签名与原文，`gemini-*` 用 `gemini`），丢弃裸令牌与异族信封；`claude-*` 将无令牌思考降级为可见文本并清空当轮 `generationConfig.thinkingConfig`；`gemini-*` 省略无令牌历史思考（不拼 `<thinking>` 标签），仅对每步首调用或未知无签名调用填 `skip_thought_signature_validator`，保持 `CallMemory` 已知无签名并行兄弟调用无签名（命中 A `155` 项、B `171` 项，涉及 45 个分组夹具） | `probe/*/call_memory_json_and_empty_results`、`wire/*/request/legacy_history.json`、`wire/*/request/reasoning_carrier_roundtrip.json`、`wire/*/request/reasoning_carrier_native.json`、`wire/openai_chat_completions/request/reasoning_tool_replay.json` | 已裁决采用选项 A：执行第 3.10 节与第 4.4 节的 `trc1:` 信封解封、`claude-*` 无签名降级加 `thinkingConfig` 清空，以及 `gemini-*` 步骤首调用占位规则 |
| `AG7` | 托管 `web_search`、旧式搜索函数、`code_execution` 与带版本后缀 `code_execution_20250522` 工具形态 | `type: "web_search*"` 与旧式函数 `name: "web_search"` 均转为 `googleSearch: {"enhancedContent": {"imageSearch": {"maxResultCount": 5}}}`；精确 `type: "code_execution"` 转为 `codeExecution: {}`；Anthropic 带版本后缀的 `{"type": "code_execution_20250522", "name": "code_execution"}` 未命中精确匹配，被误当作空 schema 客户端 `functionDeclaration` 写出 | 仅识别 `type: "web_search*"` 输出 `googleSearch: {}`；将旧式函数 `name: "web_search"` 与带版本后缀的 `code_execution_20250522` 留作客户端 `functionDeclaration`，并丢弃无 `name` 的 `type: "code_execution"` | 三协议统一将托管 `web_search*` 与旧式搜索函数名提升为单个 `googleSearch: {}`，将精确 `type: "code_execution"` 转为 `codeExecution: {}`；对带版本后缀的 `code_execution_20250522` 作为不支持异源托管工具丢弃并记 `LossHostedToolDropped`，不伪造空 schema 客户端函数（命中 A `17` 项、B `14` 项，涉及 15 个分组夹具） | `probe/*/web_search_and_code_execution`、`wire/*/request/tools_hosted_search.json`、`corpus/clients/codex/0.171.0/*` | 已裁决采用选项 A：保持标准 `googleSearch: {}`、精确 `type: "code_execution"` 转 `codeExecution: {}`，并丢弃未知版本托管工具记 `hosted_tool_dropped` |
| `AG8` | 工具 JSON Schema 清洗顺序、约束提示、空对象 `reason` 与客户端工具降级 | 直接调 `CleanJSONSchema`（小写 `type: "object"`，将 `minLength` 等迁入 `description` 的 `[Constraint: ...]`，对空 `properties` 对象注入 `reason` 占位属性；先经 `map[string]any` 反序列化） | 先跑 `NativeCleanToolSchema` 将 `type` 转为大写 `"OBJECT"` 且直接删掉 `minLength`/`pattern`/`format` 不留约束提示，导致后续 `CleanGeminiRequest` 因大小写不匹配跳过空对象 `reason` 属性注入 | 两模型族统一经 `CleanAntigravitySchema` 从原始 `jsontext.Value` 一次无损清洗（保留小写类型、`[Constraint: ...]` 提示与空对象 `reason` 注入，随后 `CleanGeminiRequest` 幂等通过），并经 `convert` 统一降级 `custom`/`namespace`/`tool_search` 与清洗非法起始字符工具名（命中 A `24` 项且仅在 `messages_gemini` 的 `claude-code` 语料出现、命中 B `103` 项，涉及 23 个分组夹具） | `probe/*/mcp_opencode_metadata_schema`、`wire/*/request/tool_args_empty.json`、`corpus/clients/claude-code/2.1.330/*`、`corpus/clients/codex/0.171.0/*` | 已裁决采用选项 A：两模型族统一使用 `CleanAntigravitySchema` 加 `convert` 工具降级与标识符清洗 |
| `AG9` | `generationConfig` 默认停止词、`maxOutputTokens` 默认值与思考预算余量抬升、禁用思考控制与结构化输出 | 未设 `OmitStopSequences` 时硬编码注入 `DefaultStopSequences`（`["<\|user\|>", "<\|endoftext\|>", "<\|end_of_turn\|>", "\n\nHuman:"]`）并覆盖忽略客户端 `stop_sequences`；客户端未传上限时默认写 `maxOutputTokens: 64000`；客户端显式传入上限且低于思考预算时抬升至 `thinkingBudget + 1000`（OpenAI 来源因中间 `ResponsesToAnthropicRequest` 先叠加 `+4096` 产生二次膨胀，如 `4096 -> 14336`）；思考禁用时省略 `thinkingConfig`；丢弃结构化输出 schema | 透传客户端 `stop_sequences`（OpenAI 串联桥接丢失部分 `stop`）；OpenAI 来源未传上限时继承中间 Anthropic 默认值 `maxOutputTokens: 8192`；`applyGeminiModelParameters` 未对显式 `maxOutputTokens` 执行 `thinkingBudget + 1000` 余量抬升（保留 `4096`）；思考禁用时省略 `thinkingConfig`；丢弃结构化输出 schema | 仅在客户端显式提供且 `!OmitStopSequences` 时写出 `stopSequences`；客户端未传上限时省略 `maxOutputTokens`，显式传入正上限时直接按 `max(maxOutputTokens, thinkingBudget + 1000)` 抬升并受 `64000`/`65000` 与目录上限封顶（无中间桥 `+4096` 膨胀，如 `4096 -> 11240`）；`Toggle: true` 模型显式关闭思考时写 `{"includeThoughts": false, "thinkingBudget": 0}`；完整保留 `responseMimeType` 与 `responseSchema`/`responseJsonSchema`（命中 A `280` 项、B `57` 项，涉及 239 个分组夹具） | `wire/*/request/stop_sequence.json`、`wire/*/request/structured_output.json`、`probe/*/opus_4_6_adaptive_budget`、`probe/*/gemini_3_1_pro_high_controls`、`corpus/clients/*` | 已裁决采用选项 A：仅透传客户端 `stopSequences`、未传输出上限时省略 `maxOutputTokens`、显式上限直接按 `+1000` 余量抬升并保留结构化输出 |
| `AG10` | 多模态 URL/PDF、拒答内联、`OpaqueItem` 清理、Cursor `input` Chat 与回合归并 | 丢弃远程图片 URL、OpenAI 桥上的 PDF 文档与单跳 `refusal`；Cursor 无 `messages` 带顶层 `input` 的 Chat 请求丢失全部输入产出 `contents: null`；OpenAI 桥用 `"\n\n"` 合并同轮多段文本 | 丢弃远程图片 URL 与 PDF 文档；将未知 Anthropic 块（`server_tool_use`、`web_search_tool_result`）序列化为模型原始 JSON 文本；Cursor 顶层 `input` Chat 产出空字符串用户消息；OpenAI 桥用 `"\n\n"` 合并多段文本 | 保留远程图片 URL 为 `fileData`、base64 PDF 为 `inlineData`、拒答内联为文本、单跳保留 Cursor 顶层 `input` Chat 内容与离散文本分块；跨协议干净丢弃 `OpaqueItem`；修复孤儿工具调用/结果配对与相邻同角色回合归并（命中 A `58` 项、B `62` 项，涉及 22 个分组夹具） | `probe/openai_chat_completions/cursor_input_without_messages`、`wire/*/request/{image_input,document_input,refusal,tools_hosted_search,system_roles}.json` | 已裁决采用选项 A：保留第 4.4 节无损多模态、拒答、Cursor 单跳 Chat、`OpaqueItem` 丢弃与回合合法化语义 |
| `AG11` | 网关层 OpenAI 兼容入口 `prepareAntigravityCompatTools` 工具名混淆改写 | `antigravity_messages.go` 不执行该改写；`antigravity_openai.go` 在 `Responses` 与 `Chat` 入口调用 `anthropic.BuildToolNameRewriteFromBody` 将 `sessions_*` 改写为 `cc_sess_*` 并在工具数超过 5 个时注入哈希前缀别名，响应侧再经 `attempt.ToolNames` 反解 | 同左（位于 `antigravity_openai.go:166,229` 网关前置步骤，在进入旧基线 A 或 B 之前执行） | 统一入口 `Antigravity.ForwardConverted` 不再对 Antigravity（Gemini `v1internal` 上游，非 Anthropic OAuth 上游）套用 Anthropic OAuth 专用的 `cc_sess_*` 工具名混淆，保持原工具名直发并由 `convert` 的 `ToolMap` 负责客户端工具降级与还原（已由 `dual_baseline_diff_scenario_test.go` 与 `googleforward/antigravity_converted_external_test.go` 验证；`UseIRConversion == false` 旧入口原样保留） | `probe/gateway_openai_compat/sessions_lookup_and_multi_tool_passthrough`、`googleforward/antigravity_converted_external_test.go` | 已裁决采用选项 A：Antigravity 新路径不再执行 Anthropic OAuth 专用的 `BuildToolNameRewriteFromBody` 工具名混淆 |

### 阶段 7 第 6 步：阶段 8 upgrade_notes.md 升级说明要点汇总

阶段 8 切换时，按 `docs/operations/upgrade_notes.md` 现有体例在 `## 未发布与近期升级` 下新增锚点 `<a id="ir_protocol_conversion"></a>` 与条目 `### 未发布：协议转换引擎统一为 IR 管线（无数据库迁移）`。以下六个方面覆盖阶段 8 编写升级说明所需的全部事实。

#### 1. 转换引擎整体替换与无运行时回退开关

四个文本协议（`anthropic_messages`、`openai_responses`、`openai_chat_completions`、`gemini_generate_content`）之间的跨协议转换，Antigravity 与 Qoder 上游转换，以及同协议 OpenAI Responses 客户端工具降级（OpenAI API Key、Grok、DeepSeek 与 Responses WebSocket HTTP 桥上的 `custom`、`namespace`、`tool_search`）在同一发布版本内统一切换至 `internal/protocol/convert` 与各协议编解码器。旧 `internal/protocol/bridge` 及逐对串联转换代码随同一版本删除。

本次切换不设运行时配置项、环境变量或请求头引擎开关，生产环境不同时保留新旧两套转换实现。升级与回退均按单实例前后端同版本整体切换执行。

#### 2. 升级边界与存量会话兼容后果

升级前由旧引擎写入客户端会话历史的裸签名或密文（例如曾写入 Messages `thinking.signature` 的 Grok 或 OpenAI `encrypted_content`）不含 `trc1:` 信封前缀。升级后首轮跨协议请求会将这类非信封令牌识别为 `unknown` 并在本地丢弃（记 `replay_token_dropped`），同时按 `legacy_history` 合法化规则补齐消息结构，请求照常完成，不会触发崩溃或上游 HTTP 400，但跨升级边界当轮会丢失该段历史推理连续性。不涉及信封的同协议透传路径继续原样转发裸令牌。

升级前生成且客户端仅保留 `encrypted_content`（`summary` 为空）的历史 Responses 推理条目，在升级后走 Responses 转 Chat 路线时无法再从 Redis 取回旧明文推理，表现与升级前 Redis 缓存未命中相同。新生成的无令牌推理条目改由客户端侧 `trc1:plain` 信封自包含携带。

客户端若将包含 `trc1:` 信封的网关会话历史直接绕过网关发往官方上游 API，官方 API 会因无法识别网关信封拒绝校验，这一边界与旧引擎向客户端历史写入异源裸令牌时的行为一致。

#### 3. 客户端与运维可感知的验收差异（6.2.4 节）

| 类别 | 可感知行为变化 | 客户端与运维影响 |
| --- | --- | --- |
| `D6` | 跨协议转换统一在客户端不透明令牌字段（Messages 的 `thinking.signature` 与 `redacted_thinking.data`、Responses 的 `reasoning.encrypted_content`）写入无填充 `base64url` 编码的 `trc1:<scheme>:<payload>` 信封，方案枚举为 `anthropic`、`antigravity_claude`、`openai`、`xai`、`gemini`、`plain`。Gemini 推理签名与无令牌明文推理（Chat `reasoning_content`、无签名 Gemini 思考摘要、`passback` Messages 思考）同样封装写出 | 客户端可见正文、工具调用与计费用量不变；Claude Code、Codex 与 `pi` 原样保存并在下一轮带回信封，Messages 客户端不再收到空串 `signature` |
| `D7` | 出站令牌不再凭 `gAAAA` 前缀猜测归属，也不再把 Messages 历史裸签名写进 Responses `encrypted_content`。账号绑定的 `openai` 与 `xai` 信封携带签发提供商 ID `p`，同分组切到其他提供商账号时在网关本地直接剥离；`passback` Messages 上游（`deepseek-*`、`kimi-*`、`glm-*`、`minimax-m*`、`qwen*-thinking`）在工具轮回传无签名明文 `thinking` 块 | 消除跨账号切换时必败的上游 `400 invalid_encrypted_content` 往返；同方案同账号续聊完整保留上游推理状态 |
| `D8` | `custom`、`namespace`、`tool_search` 降级函数名及非法起始字符、超长工具名的清洗结果在跨版本升级时可能变化一次 | 同版本内相同工具声明与历史生成的名字确定稳定，响应侧始终按 `ToolMap` 向客户端还原原始工具名与工具类型 |
| `D9` | Responses 客户端转 Chat 上游时直接从 `reasoning.encrypted_content` 中的 `trc1:plain` 信封恢复历史 `reasoning_content` | 取消服务端 Redis 推理文本存取依赖，支持 `deepseek-thinking` 等强制工具轮携带 `reasoning_content` 的上游 |
| `D10` | 原生 Messages 路径（含 Anthropic 各认证类型、Bedrock、Vertex、兼容端点与 `/v1/messages/count_tokens`）和原生 Responses 路径（HTTP、`/v1/responses/input_tokens`、专用 WS、连接池 WS 与 WS HTTP 桥）在协议令牌路径含 `trc1:` 信封时执行 `PrepareNativeReplay`，解封同方案同账号令牌并剥离异源或跨账号信封 | 混合分组在转换路径与原生路径之间切号时无需客户端干预即可续聊；协议令牌路径不含 `trc1:` 的原生请求（含用户或工具正文出现字面量 `"trc1:"` 的请求）保持逐字节不变 |
| `D11` | Messages 显式关闭思考（`thinking.type == "disabled"`、`budget_tokens == 0` 或 `effort == "none"`）转往 Responses 或 Chat 时按 `Dialect.Reasoning` 模型能力裁决（支持 `"none"` 时发 `effort="none"` 且省略 `summary`，支持 `"minimal"` 时回退 `"minimal"`，纯推理模型取最低档位，仅开关控制、非推理或未知目录模型直接省略努力度）；Chat 转往 `HistoryNone` 上游时丢弃无令牌历史 `reasoning_content` | 避免向不支持禁用思考或努力度参数的上游误发 `effort` 引发 400 或意外开启思考，也不再向历史助手正文拼接 `<thinking>` 标签 |
| `D13` | 跨协议发往 `!AllowBillingHeader` 上游（含 Gemini、Antigravity、OpenAI 兼容平台；Qoder 仍保留并归一化 `cch=<cch>;`）时统一剥离 `x-anthropic-billing-header:` 系统提示块 | 避免 Claude Code 客户端内部计费遥测头泄漏进非 Anthropic 上游的 `systemInstruction` 或 `developer` 指令 |
| `D14` | Antigravity 将 Messages、Responses、Chat 合并为单跳 `v1internal` 管线（`AG1` 至 `AG11` 选项 A）：两模型族统一生效管理员自定义 `identity_patch_prompt` 与 `CleanAntigravitySchema`，按客户端显式 `tool_choice` 与 `stop_sequences` 映射，未传输出上限时省略 `maxOutputTokens`，省略 `request.sessionId` 并固定 `requestType: "agent"`，OpenAI 兼容入口保持原工具名直发（取消 Anthropic OAuth 专用的 `cc_sess_*` 混淆），并拆分 `trc1:antigravity_claude` 与 `trc1:gemini` 信封方案 | 消除旧双管线在 `claude-*` 与 `gemini-*` 之间的参数膨胀、强制 `VALIDATED` 工具模式与停止词覆盖差异，同分组下 Antigravity `gemini-*` 与标准 Gemini 可安全互通重放签名，且与 `claude-*` 严格隔离 |

#### 4. 升级后首轮提示缓存命中行为

存量长会话在升级后的首轮请求中，若命中降级工具名调整、消息块顺序保留或确定性 JSON 成员序列化变化，上游前缀可能出现一次提示缓存未命中。进入新版本后的后续轮次对相同输入生成逐字节确定的请求体，提示缓存恢复稳定命中。

#### 5. 数据库、Redis 与进程内状态影响

1. 数据库与配置：本次变更无 PostgreSQL schema 迁移，不改变 `config.yaml` 配置版本与字段，计费用量（`TokenUsage` 与 `usage_logs` 各缓存分档列）结算口径保持不变。
2. Redis 遗留键：旧引擎写入的 `reasoning_content:` 前缀键（原 TTL 为 7 天）在升级后停止读写，无需执行人工清理脚本，随原有 TTL 在 7 天内自然过期。
3. 进程内状态：`gateway/session.GeminiToolCalls`（承载 Gemini 与 Antigravity 工具调用 ID 及 `thoughtSignature` 的进程内 `CallMemory`，1 小时有效期）与 Responses WebSocket 连接内 `ToolMap` 在升级重启后自然清空，随新请求自动建立。

#### 6. 回退限制（2.4 节）

本次升级不涉及数据库结构变更，需要回退时停止新版本进程并切回上一版本前后端二进制即可。旧版本二进制不识别 `trc1:` 信封格式；升级运行期间已经把 `trc1:` 信封写入客户端历史的活跃跨协议或混合分组会话，在回退到旧版本后会被当作无法校验的裸签名或密文处理，按各上游既有容错逻辑触发一次签名剥离重试或需由客户端新开会话，不会造成 PostgreSQL 或 Redis 持久化数据损坏。
