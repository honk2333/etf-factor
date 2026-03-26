# ETF Factor Data Management

这个项目用于管理 ETF 因子数据，设计原则如下：

- `etf-data/` 是只读上游日线数据源，以 git submodule 方式接入
- 本项目只负责因子计算和因子落库
- 因子结果统一写入单表 `etf_factor`
- 每次运行主程序都会检查上游最新交易日，只有未更新到最新的因子才会重算并覆盖

## 目录结构

```text
etf-factor/
├── etf-data/                 # 上游只读 submodule
├── sql/schema.sql            # 因子库 schema
├── scripts/
│   ├── init_factor_db.py     # 初始化因子库
│   └── compute_factors.py    # 计算并更新因子
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

默认建议直接使用 `scripts/` 入口，不依赖 `pip install -e .`。

## 初始化因子库

```bash
python scripts/init_factor_db.py
```

默认会创建：`data/etf_factor.duckdb`

这一步会清理旧版遗留的 `factor_values`、`factor_ic_daily`、`factor_metrics`、`factor_definitions` 表。

## 更新因子

```bash
python scripts/compute_factors.py
```

默认行为：

- 从 `etf-data/data/etf_daily.duckdb` 读取 `etf_daily`
- 检查上游 `etf_daily` 的最新交易日
- 检查每个因子在 `etf_factor` 中是否已更新到该日期
- 仅对未更新到最新的因子执行重算并覆盖写入

## 可选参数

```bash
python scripts/compute_factors.py --factor-name momentum
python scripts/compute_factors.py --factor-name momentum --factor-name rsi
python scripts/compute_factors.py --force
```

- `--factor-name`：仅重算指定因子，可重复传入
- `--force`：忽略最新日期检查，强制重算并覆盖

## 表结构

唯一业务表：`etf_factor`

关键字段：

- `trade_date`
- `symbol`
- `factor_key`
- `factor_name`
- `params_json`
- `value`

## 说明

- `etf-data` 中的任何内容都不应在本项目内被修改
- 当前振幅 `amplitude` 为派生字段：`(high - low) / prev_close`
- 当前 `pct_chg` 为派生字段：`close.pct_change()`
- 当前“是否需要更新”的判断标准是因子表最大 `trade_date` 是否等于上游最大 `trade_date`
- 如果上游历史数据被回补或修订，但最新交易日不变，当前策略不会自动识别；这种情况用 `--force` 重算
