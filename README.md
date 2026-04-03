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
  - 持仓/委托/成交表读取仍不稳定

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

### 4. 懒人模式

```powershell
python 交易_一键入口.py
```

### 5. 稳定性巡检

```powershell
python 交易_稳定性巡检.py --date 2025-03-07 --rounds 3
```

## 当前已知问题

### 同花顺模拟盘读取问题

当前验证显示：

- `balance` 和 `raw_balance` 可读
- `股票市值` 会变化，说明模拟盘交易状态确实发生变化
- 但 `raw_positions / raw_today_entrusts / raw_today_trades` 经常为空
- 32 位 Python 解决了兼容性和 `pywin32` 依赖问题，但没有解决空表问题

这说明当前问题不再是简单的位数或依赖错误，而更像是：

- `easytrader` 默认 grid 读取不适配当前同花顺模拟盘表格
- 或者模拟盘查询页控件结构和默认预期不同

因此当前系统定位是：

- 自动出信号
- 自动预览订单
- 自动读取余额
- 手工下单
- 手工成交确认
- 程序辅助复核和留痕

## 后续攻关方向

- 整理旧脚本、统一命名、沉淀项目文档
- 继续分析同花顺模拟盘持仓/成交表控件结构
- 判断是否需要绕过 `easytrader` 默认 grid 读取逻辑
- 在读取稳定前，不把自动提交作为核心依赖

## 更多说明

- [docs/项目导览.md](/mnt/d/量化/同花顺/pythonProject/docs/项目导览.md)
- [docs/同花顺模拟盘问题记录.md](/mnt/d/量化/同花顺/pythonProject/docs/同花顺模拟盘问题记录.md)
