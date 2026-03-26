# ETF Factor Data Management

这个项目只做一件事：

- 从 `etf-data/data/etf_daily.duckdb` 读取 ETF 日线数据
- 计算全部因子
- 把结果写进自己的 DuckDB 表 `etf_factor`

项目文件只保留：

- `main.py`
- `factors.py`
- `etf-data/`

## 环境

```bash
conda activate multifactor-etf
```

## 运行

```bash
python main.py
```

默认行为：

- 自动检查 `etf_factor` 表是否存在，不存在就创建
- 检查上游 `etf_daily` 的最新交易日
- 检查每个因子在 `etf_factor` 中是否已更新到该日期
- 只重算落后的因子
- 已是最新的因子直接跳过

## 可选参数

```bash
python main.py --force
```

- `--force`：忽略最新日期检查，强制重算并覆盖全部因子

## 表结构

只有一张表：`etf_factor`

字段：

- `trade_date`
- `symbol`
- `factor_key`
- `factor_name`
- `params_json`
- `value`

## 说明

- 不修改 `etf-data` 中的任何内容
- 当前是否需要更新，只看因子表最大 `trade_date` 是否等于上游最大 `trade_date`
- 如果上游历史数据被修订但最新日期没变，用 `--force` 重算
