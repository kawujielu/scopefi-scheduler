# scopefi-scheduler

Docker 定时任务：每天北京时间 **02:00** 过滤活跃地址，**05:00** 打分写入多空比，**每小时整点** 剔除零资金地址并更新 `long_short_ratio`。

## 任务

| 时间 | 脚本 | 说明 |
|------|------|------|
| 02:00 | `strategy-data-main/ch_filter_active_addresses.py` | 读 `active_perp_addresses`，查 ScopeFi 资金过滤，写 `pro.active_addresses` |
| 05:00 | `scopefi-score/ch_score_to_long_short_ratio.py --write` | 读 `user_fill_v2` 打分，写 `pro.long_short_ratio` |
| 每小时 :00 | `scopefi-score/prune_long_short_ratio_zero_balance.py` | 读最新 `long_short_ratio`，剔除 balance=0 地址，**默认 INSERT 新快照** |

## 目录

```
scopefi-scheduler/
├── scopefi/query_wallet_balance.py   # ScopeFi 查资金（读 scopefi/.env）
├── strategy-data-main/ch_filter_active_addresses.py
├── scopefi-score/ch_score_to_long_short_ratio.py
├── scopefi-score/prune_long_short_ratio_zero_balance.py
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
docker compose run --rm scheduler /app/scripts/run_prune_zero_balance.sh
docker compose run --rm scheduler /app/scripts/run_prune_zero_balance.sh --dry-run
```

## 部署到 Ubuntu

```bash
# 1. 上传项目
scp -r scopefi-scheduler user@server:/opt/

# 2. SSH 登录
ssh user@server
cd /opt/scopefi-scheduler

# 3. 配置 ScopeFi（必做）
cp scopefi/.env.example scopefi/.env
# 编辑 scopefi/.env，至少确认 SCOPEFI_TRACK_BASE_URL、SCOPEFI_POSITION_PATH

# 4. 日志目录（挂载用，git 已含 logs/.gitkeep）
mkdir -p logs

# 5. 构建并启动（需已安装 Docker + Compose）
docker compose up -d --build

# 6. 首次建议手动跑一遍验证
docker compose exec scheduler /app/scripts/run_filter_active.sh
docker compose exec scheduler /app/scripts/run_score.sh
docker compose exec scheduler /app/scripts/run_prune_zero_balance.sh

# 7. 查看日志
tail -f logs/ch_filter_active_addresses.log
docker compose logs -f scheduler
```

### 网络要求

- 服务器能访问 `scopefi/.env` 中的 ClickHouse 与 ScopeFi API
- 容器时区 `TZ=Asia/Shanghai`，crontab 按北京时间执行

## 验收

```sql
SELECT count(), max(version) FROM pro.active_addresses;
SELECT coin, timestamp FROM pro.long_short_ratio ORDER BY timestamp DESC LIMIT 10;
```

## 说明

- ClickHouse / ScopeFi 均在 `scopefi/.env` 配置（docker compose 挂载进容器）
- 日志目录：`logs/` 挂载到容器 `/app/logs`，每个任务对应一个文件：
  - `ch_filter_active_addresses.log`
  - `ch_score_to_long_short_ratio.log`
  - `prune_long_short_ratio_zero_balance.log`
- 需确保服务器能访问 ClickHouse 与 ScopeFi API
