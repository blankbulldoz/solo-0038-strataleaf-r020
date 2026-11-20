# 数据与兼容边界

物理表包括 meta、collections、versions、commits、receipts、index_defs、index_values、pins、consumers、rule_versions、rule_registry。versions 的主键为集合、键、修订；body 的 SQL NULL 表示删除，JSON null 只能作为文档字段。collections 保存当前模式；index_values 是当前 head 的投影。commits 保存完整的操作序列；receipts 保存不受历史回收影响的请求摘要与结果。幂等键不能被不同操作复用，回放不重新执行前置条件。

rule_versions 保存规则的全部不可变版本（active/superseded/disabled），rule_registry 只指向每条规则的当前活动版本。规则目录用独立的 rule_epoch 标识，与模式的 catalog_epoch 平行；规则注册、替换、停用各让 rule_epoch 前进一步，即使没有文档提交也会前进。历史版本解释当时提交使用的规则定义：commits 负载中的每个派生操作都带 origin，记录规则名、源集合/键、事件、深度与上游 cause，旧规则版本不被删除。

触发器在批次的同一个 IMMEDIATE 事务内、任何 versions 行写入之前执行。活动规则按源集合分组，用户操作以深度 0 进入工作队列；匹配事件经谓词过滤后投影为 upsert/delete/patch 效果，效果再作为下一轮事件传播，直到队列定点。深度达到 32 判定为环或过深，派生操作达到 10000 判定为扇出失控，均整批回滚。同一提交内规则不得写调用方正在写的文档；同键的多个规则效果只允许 patch 在 upsert 之后按规则名顺序折叠，整文档写撞键是歧义错误。patch 效果要求目标文档已存在，否则失败关闭。所有派生文档与用户文档走同一套 schema 校验与唯一索引约束。

模板是受边界约束的纯投影：叶子 `{"$field": "a.b"}` 取源文档字段（缺失投影为 null），`{"$key": true}` 取源文档键；目标键支持 `${a.b}` 与 `${key}` 标量插值，最多 8 个占位符；模板最多 256 个 JSON 节点、8 层深度、每层 32 个字段。删除事件的源文档为空对象，只有源键可用。规则不执行任意代码，投影结果必须是 JSON 对象。

commits 负载扩展为含 user_count、derived 计数、trace 因果链，operations 中每条派生操作带 action 与 origin；旧负载缺少这些键时按零派生处理。副本复制 rule_versions、rule_registry 与 rule_epoch，但只重放负载里已物化的操作，从不运行规则引擎，所以重放同一包不会重复生成派生记录。副本在分段追赶未到 head 时把 catalog_epoch/rule_epoch 暂置为 -1，与索引时序一致；纯元数据包（through 等于 head、无新提交）在任一 epoch 更新时仍会安装目录。


floor 是仍可读取的最早修订，不代表每条物理版本的最小修订；每个键在 floor 之前的最后版本可能是构造 floor 读视图的基准，必须保留。commits 仅需保存大于 floor 的提交，消费者从自己的 ack 之后读取。pin 与消费者位置不能低于现存 floor。

模式包含 fields、required、additional，支持点路径类型；additional=false 限制顶层未知根字段，不递归禁止已声明对象中的子字段。字段不允许 null，除非该字段未声明类型；缺失可通过 required 控制。integer 排除 bool，number 接受整数及有限浮点。JSON 对象不允许非字符串键；浮点必须有限。索引忽略缺失字段与对象/数组，空值作为可索引标量，数值 1 和 1.0 共用索引值，true 与 1 不共用。

游标携带数据库身份、谓词摘要、读版本和最后键，并由数据库私有密钥签名。它不是权限凭据，不冻结模式，也不是保留租约。需要稳定的长时间读取，调用者自行管理具名保留点。

副本包是内部可信通道数据，校验和只用于检测传输损坏。正常备份恢复新主库保留逻辑内容而更换身份；副本初始化保留上游身份。分段追赶未完成时暂不提供当前索引，避免最新唯一约束错误约束中间历史视图。目录 epoch 可在没有文档提交时前进，最终空提交包仍能传递新的模式与索引。
