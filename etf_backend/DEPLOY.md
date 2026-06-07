# ETF Backend — 修改部署规范

## 核心原则

每次修改遵循：**Pull → Edit → Commit → Push → Restart → Verify**

---

## 标准修改流程

### Step 1：改前先从VPS同步到本地
```bash
# 以 sentiment_v2.py 为例
sshpass -p 'bexmlj@-Zogrog-xycni5' scp -o StrictHostKeyChecking=no \
  root@124.222.87.177:/opt/etf_backend/sentiment_v2.py \
  /Users/malijun/WorkBuddy/2026-05-26-19-28-01/etf_backend/sentiment_v2.py
```

### Step 2：本地修改（用 Edit 工具，保证有读取记录）

### Step 3：上传到VPS
```bash
sshpass -p 'bexmlj@-Zogrog-xycni5' scp -o StrictHostKeyChecking=no \
  /Users/malijun/WorkBuddy/2026-05-26-19-28-01/etf_backend/sentiment_v2.py \
  root@124.222.87.177:/opt/etf_backend/sentiment_v2.py
```

### Step 4：VPS上 git commit（保留历史，可回滚）
```bash
sshpass -p 'bexmlj@-Zogrog-xycni5' ssh -o StrictHostKeyChecking=no root@124.222.87.177 "
  cd /opt/etf_backend
  git add sentiment_v2.py
  git commit -m 'fix: <描述修改内容>'
  git log --oneline -5
"
```

### Step 5：重启服务
```bash
sshpass -p 'bexmlj@-Zogrog-xycni5' ssh -o StrictHostKeyChecking=no root@124.222.87.177 \
  "systemctl restart etf-backtest && sleep 2 && systemctl is-active etf-backtest"
```

### Step 6：验证
```bash
curl -s 'http://127.0.0.1:5000/api/sentiment/v2' | python3 -c "import sys,json; d=json.load(sys.stdin); print('ok:', d.get('all_modules_ok'))"
```

---

## 回滚操作

```bash
# 查看历史
git log --oneline

# 回滚到上一个commit（保留工作区）
git revert HEAD

# 强制回滚到某个commit（危险：丢弃之后所有改动）
git checkout <commit_hash> -- sentiment_v2.py
```

---

## 文件清单

| 文件 | 说明 |
|------|------|
| `api_server.py` | Flask API + judgment_v2 计算 + LLM curl调用 |
| `sentiment_v2.py` | 6模块情绪引擎（K线/ADR/涨跌停/偏离/搜索/资金） |
| `daily_predict.py` | 凌晨1点cron：拉数据→写DB→AI预测 |
| `sentiment_storage.py` | MySQL持久层（sentiment_score表） |
| `import_etf_monthly.py` | 每月1号凌晨2点：ETF月K线导入 |

前端: `/var/www/etf/sentiment.html`（有独立git仓库）

---

## 关键配置

- **VPS**: 124.222.87.177, user: root
- **服务**: `systemctl {start|stop|restart|status} etf-backtest`
- **Python**: `/usr/bin/python3.6`（有SSL）或 `/usr/local/bin/python3`（3.10，无SSL）
- **MySQL**: host=127.0.0.1, port=32306, user=root, pwd=root@2024, db=etf_backtest
- **LLM**: api.scnet.cn / DeepSeek-V4-Flash（用curl调用，绕过Python无SSL问题）

---

## 已知注意事项

1. **不要用 `/usr/local/bin/python3` 做HTTPS请求** — 无_ssl模块，改用curl subprocess
2. **交易日判断**：`_market_closed_today()` 判断hour>=15，15点前不算今日已完成
3. **TDX K线过滤**：返回数据会含当日占位bar，需过滤 date == today（15点前）
4. **ADR/limits 历史**：快照+DB合并逻辑，不要改回"快照优先就跳过DB"的旧逻辑
