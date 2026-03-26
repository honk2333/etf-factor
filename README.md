# ETF Factor Data Management

这个项目只做一件事：

- 从 `etf-data/data/etf_daily.duckdb` 读取 ETF 日线数据
- 计算因子
- 把结果写进自己的 DuckDB 表 `etf_factor`

项目文件尽量收缩，只保留：

- `main.py`：主程序
- `factors.py`：因子定义和注册表
- `etf-data/`：只读上游 submodule

## 环境

```bash
conda activate multifactor-etf
```

## 初始化因子库

```bash
python main.py init-db
```

默认会创建 `data/etf_factor.duckdb`。

这一步会清理旧版遗留的：

- `factor_values`
- `factor_ic_daily`
- `factor_metrics`
- `factor_definitions`

## 更新因子

```bash
python main.py update
```

默认行为：

- 检查上游 `etf_daily` 的最新交易日
- 检查每个因子在 `etf_factor` 中是否已更新到该日期
- 只重算落后的因子
- 已是最新的因子直接跳过

## 常用参数

```bash
python main.py update --factor-name momentum
python main.py update --factor-name momentum --factor-name rsi
python main.py update --force
```

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
