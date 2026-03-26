# ETF Factor Data Management

这个项目用于管理 ETF 因子数据，设计原则如下：

- `etf-data/` 是只读上游日线数据源，以 git submodule 方式接入
- 本项目只负责因子定义、因子计算、因子值存储、IC 评估
- 原始日线数据和因子数据分库存放，避免职责混乱
- 因子结果统一写入 DuckDB，方便筛选、研究和回测复用

## 目录结构

```text
etf-factor/
├── etf-data/                 # 上游只读 submodule
├── sql/schema.sql            # 因子库 schema
├── src/etf_factor/
│   ├── cli.py                # 命令行入口
│   ├── config.py             # 路径配置
│   ├── db.py                 # DuckDB 连接和 schema 初始化
│   ├── factors.py            # 因子定义与注册表
│   ├── market_data.py        # 从 etf-data 读取行情并整理
│   ├── compute.py            # 因子计算主流程
│   └── storage.py            # 因子入库
├── environment.yml
└── tests/
```

## 环境

```bash
conda env create -f environment.yml
conda activate multifactor-etf
```

当前默认建议直接使用 `scripts/` 入口，不依赖 `pip install -e .`。

## 初始化因子库

```bash
python scripts/init_factor_db.py
```

默认会创建：`data/etf_factor.duckdb`

## 计算因子

```bash
python scripts/compute_factors.py \
  --start 2022-01-01 \
  --pred-days 3
```

默认行为：

- 从 `etf-data/data/etf_daily.duckdb` 读取 `etf_daily`
- 计算注册表中的全部因子
- 将因子值写入 `factor_values`
- 将按日 Rank IC 写入 `factor_ic_daily`
- 将汇总指标写入 `factor_metrics`

## 关键表

- `factor_definitions`: 因子定义和参数快照
- `factor_values`: 长表格式的每日因子值
- `factor_ic_daily`: 每日 Rank IC
- `factor_metrics`: 因子汇总统计

## 说明

- `etf-data` 中的任何内容都不应在本项目内被修改
- 当前振幅 `amplitude` 为派生字段：`(high - low) / prev_close`
- 当前 `pct_chg` 为派生字段：`close.pct_change()`
- 如后续你要加行业中性化、分层 IC、换手约束，建议继续在本项目内扩展，不要回写到 `etf-data`
