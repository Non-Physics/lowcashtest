# 股票策略系统说明

本项目当前不是一个“全自动实盘系统”，而是一个从可信回测逐步走向半自动模拟盘验证的平台。  
核心目标是先保证策略时序正确、信号可复现、交易链路可验证，再决定是否继续自动化下单。

## 当前结论

- 可信策略版本是 [股票策略回测_基线版.py](/mnt/d/量化/同花顺/pythonProject/股票策略回测_基线版.py)
- 基线时序是：
  - 收盘后生成信号
  - 下一交易日开盘执行
  - 先卖后买
  - 同批买单共用开盘前权益快照
- 数据链路已打通：
  - `Tushare -> 本地 CSV -> 指标 -> 基线信号`
- 交易链路已部分打通：
  - 可连接同花顺模拟盘
  - 可稳定读取余额
  - 可生成订单预览
  - 持仓/成交表已经出现成功读取
  - 委托/成交/持仓读取仍需继续观察稳定性

## 目录角色

### 主策略与研究

- [股票策略回测_基线版.py](/mnt/d/量化/同花顺/pythonProject/股票策略回测_基线版.py)
  当前可信基线回测入口
- [股票策略参数优化.py](/mnt/d/量化/同花顺/pythonProject/股票策略参数优化.py)
  基于基线策略的参数优化入口
- [股票策略回测.py](/mnt/d/量化/同花顺/pythonProject/股票策略回测.py)
  基础回测实现和指标函数

### 数据入口

- [下载股票数据.py](/mnt/d/量化/同花顺/pythonProject/下载股票数据.py)
  下载并清洗本地股票数据
- [检查股票数据.py](/mnt/d/量化/同花顺/pythonProject/检查股票数据.py)
  检查本地数据完整性
- [构建股票分集.py](/mnt/d/量化/同花顺/pythonProject/构建股票分集.py)
  研究用股票分集构建

### 交易主入口

- [交易_一键入口.py](/mnt/d/量化/同花顺/pythonProject/交易_一键入口.py)
  推荐的人机交互入口
- [交易_半自动流程.py](/mnt/d/量化/同花顺/pythonProject/交易_半自动流程.py)
  收盘后/盘前/复核三阶段入口
- [交易_稳定性巡检.py](/mnt/d/量化/同花顺/pythonProject/交易_稳定性巡检.py)
  模拟盘读取稳定性巡检入口
- [同花顺控件诊断.py](/mnt/d/量化/同花顺/pythonProject/同花顺控件诊断.py)
  交易端控件树和表格候选控件诊断入口
- [同花顺表格抓取诊断.py](/mnt/d/量化/同花顺/pythonProject/同花顺表格抓取诊断.py)
  对 `CVirtualGridCtrl` 做聚焦与剪贴板抓取的诊断入口
- [同花顺表格截图诊断.py](/mnt/d/量化/同花顺/pythonProject/同花顺表格截图诊断.py)
  对目标表格截图并 OCR，判断画面里是否真的显示了数据
- [股票策略交易主流程.py](/mnt/d/量化/同花顺/pythonProject/股票策略交易主流程.py)
  交易执行总控脚本
- [股票策略同花顺适配.py](/mnt/d/量化/同花顺/pythonProject/股票策略同花顺适配.py)
  同花顺 GUI / easytrader 适配层
- [stock_trading](/mnt/d/量化/同花顺/pythonProject/stock_trading)
  交易状态、信号服务、纸面执行组件

### 历史参考与诊断

- `股票策略回测_旧版参考.py`
- `股票策略回测_旧版反演.py`
- 各类 `诊断.py`

这些文件当前保留用于回溯问题和对照，不作为主入口。

## 推荐运行方式

### 研究环境

- WSL / Linux Python
- 用于数据下载、回测、参数研究

### 交易环境

- Windows 32 位 Python
- 用于 `easytrader / pywinauto / 同花顺客户端`
- 推荐单独虚拟环境：`.venv-trader32`

## 推荐日常流程

### 1. 收盘后生成信号

```powershell
python 交易_半自动流程.py eod --signal-date 2026-04-03
```

### 2. 次日盘前读取账户并预览

```powershell
python 交易_半自动流程.py preopen --trade-date 2026-04-07 --sync-cash
```

### 3. 人工在模拟盘执行后复核

```powershell
python 交易_半自动流程.py postcheck --trade-date 2026-04-07 --manual-status 已成交 --manual-note "手工买入 XXX，验证成交链路"
```

如果人工成交后本地状态仍为空，但券商快照已经读到持仓，可以显式回填本地状态：

```powershell
python 交易_半自动流程.py sync-state --trade-date 2026-04-07 --replace-cash
```

注意：
- `sync-state` 的首要目标是让 `cash / code / shares` 与券商侧重新对齐
- 若本次快照只来自 OCR 兜底，则 `entry_price / cost_basis` 只按当前市价做保守近似，不能视为真实成交成本

### 4. 懒人模式

```powershell
python 交易_一键入口.py
```

### 5. 稳定性巡检

```powershell
python 交易_稳定性巡检.py --date 2025-03-07 --rounds 3
```

### 6. 控件诊断

当同花顺模拟盘的持仓、委托、成交读取为空时，用这个工具导出控件树：

```powershell
python 同花顺控件诊断.py --exe-path "D:\量化\同花顺\同花顺\xiadan.exe" --menu-path "查询[F4],资金股票"
```

进一步测试表格是否可通过剪贴板导出：

```powershell
python 同花顺表格抓取诊断.py --exe-path "D:\量化\同花顺\同花顺\xiadan.exe" --menu-path "查询[F4],资金股票"
```

如果复制为空，再继续做截图和 OCR：

```powershell
python 同花顺表格截图诊断.py --exe-path "D:\量化\同花顺\同花顺\xiadan.exe" --menu-path "查询[F4],资金股票"
```

## 当前已知问题

### 同花顺模拟盘读取问题

最新验证显示：

- `balance` 和 `raw_balance` 可读
- `raw_positions` 已成功读到真实持仓
- `raw_today_trades` 已成功读到真实成交
- 适配层现已把券商裸代码进一步归一到标准代码格式
- 32 位 Python 解决了兼容性问题，当前瓶颈转为“查询读取是否每次都稳定”
- OCR 兜底已足够用于确认持仓股数，但不足以可靠恢复真实成交成本

这说明问题已经从“完全读不到”进入了“可读但需要稳健化”的阶段，更像是：

- 同花顺查询页加载、验证码、窗口焦点会影响读表成功率
- `easytrader` 默认 grid 读取对当前模拟盘界面的适配度一般
- OCR 仍适合作为兜底，而不是主路径

因此当前系统当前定位是：

- 自动出信号
- 自动预览订单
- 自动读取余额、持仓、成交快照
- 手工下单
- 手工成交确认
- 程序辅助复核和留痕
- 本地成本字段仅在读到原始持仓/成交时可信

## 后续攻关方向

- 整理旧脚本、统一命名、沉淀项目文档
- 继续分析同花顺模拟盘持仓/成交表控件结构
- 判断是否需要绕过 `easytrader` 默认 grid 读取逻辑
- 在读取稳定前，不把自动提交作为核心依赖

## 更多说明

- [docs/项目导览.md](/mnt/d/量化/同花顺/pythonProject/docs/项目导览.md)
- [docs/同花顺模拟盘问题记录.md](/mnt/d/量化/同花顺/pythonProject/docs/同花顺模拟盘问题记录.md)
