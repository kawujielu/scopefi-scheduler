# scopefi-scheduler

Docker 定时任务：每天北京时间 **02:00** 过滤活跃地址，**05:00** 打分写入多空比。

## 任务

| 时间 | 脚本 | 说明 |
|------|------|------|
| 02:00 | `strategy-data-main/ch_filter_active_addresses.py` | 读 `active_perp_addresses`，查 ScopeFi 资金过滤，写 `pro.active_addresses` |
| 05:00 | `scopefi-score/ch_score_to_long_short_ratio.py --write` | 读 `user_fill_v2` 打分，写 `pro.long_short_ratio` |

## 目录

```
scopefi-scheduler/
├── scopefi/query_wallet_balance.py   # ScopeFi 查资金（读 scopefi/.env）
├── strategy-data-main/ch_filter_active_addresses.py
├── scopefi-score/ch_score_to_long_short_ratio.py
├── scopefi-score/score_fills_by_symbol.py
├── scripts/run_*.sh
├── crontab
├── Dockerfile
└── docker-compose.yml
```

## 快速开始

```powershell
cd D:\scopefi-scheduler

# 1. 配置 ScopeFi API
copy scopefi\.env.example scopefi\.env
# 编辑 scopefi\.env，填入 SCOPEFI_TRACK_BASE_URL

# 2. 构建并启动
docker compose build
docker compose up -d

# 3. 查看日志
docker compose logs -f
```

## 手动执行

```powershell
docker compose run --rm scheduler /app/scripts/run_filter_active.sh
docker compose run --rm scheduler /app/scripts/run_score.sh
```

## 部署到 Ubuntu

```bash
scp -r scopefi-scheduler user@server:/opt/
ssh user@server
cd /opt/scopefi-scheduler
cp scopefi/.env.example scopefi/.env   # 填入生产配置
docker compose up -d --build
```

## 验收

```sql
SELECT count(), max(version) FROM pro.active_addresses;
SELECT coin, timestamp FROM pro.long_short_ratio ORDER BY timestamp DESC LIMIT 10;
```

## 说明

- ClickHouse 连接信息在业务脚本内配置（`writer_pro` / `192.168.112.239`）
- CSV 输出持久化：`data/filter/`、`data/score/` 已挂载到容器
- 需确保服务器能访问 ClickHouse 与 ScopeFi API
