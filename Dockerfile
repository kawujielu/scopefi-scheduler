# scopefi-scheduler — 定时跑 active_addresses 过滤 + long_short_ratio 打分
#
# 构建: docker compose build
# 运行: docker compose up -d

FROM python:3.11-slim-bookworm

ARG SUPERCRONIC_VERSION=v0.2.33
ARG TARGETARCH=amd64

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fsSLO "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-${TARGETARCH}" \
    && chmod +x "supercronic-linux-${TARGETARCH}" \
    && mv "supercronic-linux-${TARGETARCH}" /usr/local/bin/supercronic \
    && apt-get purge -y curl \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY crontab ./
COPY scripts ./scripts
COPY scopefi ./scopefi
COPY strategy-data-main ./strategy-data-main
COPY scopefi-score ./scopefi-score

RUN chmod +x /app/scripts/*.sh \
    && mkdir -p /app/logs

ENV TZ=Asia/Shanghai \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

CMD ["supercronic", "/app/crontab"]
