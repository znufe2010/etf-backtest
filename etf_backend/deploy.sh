#!/bin/bash
# ============================================
# ETF Backtest Deployment Script v3 - MySQL
# Server: 124.222.87.177 (root)
# ============================================
set -e

SERVER="root@124.222.87.177"
PASS="bexmlj@-Zogrog-xycni5"
REMOTE_DIR="/opt/etf_backend"
LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)"
PYMYSQL_WHEEL="/tmp/pymysql_wheel_old/PyMySQL-0.10.1-py2.py3-none-any.whl"

echo "=== ETF Backend Deploy v3 (MySQL) ==="
echo "Source: $LOCAL_DIR"
echo "Target: $SERVER:$REMOTE_DIR"
echo ""

# 1. Upload all files
echo "[1/6] Uploading backend files..."
sshpass -p "$PASS" ssh -o StrictHostKeyChecking=no "$SERVER" "mkdir -p $REMOTE_DIR/cache"
sshpass -p "$PASS" scp -o StrictHostKeyChecking=no -r \
    "$LOCAL_DIR/api_server.py" \
    "$LOCAL_DIR/sentiment_v2.py" \
    "$LOCAL_DIR/sentiment_storage.py" \
    "$LOCAL_DIR/daily_predict.py" \
    "$LOCAL_DIR/migrate_to_db.py" \
    "$LOCAL_DIR/requirements.txt" \
    "$SERVER:$REMOTE_DIR/"

# 2. Upload pymysql wheel and install
echo ""
echo "[2/6] Installing pymysql (offline wheel)..."
if [ -f "$PYMYSQL_WHEEL" ]; then
    sshpass -p "$PASS" scp -o StrictHostKeyChecking=no "$PYMYSQL_WHEEL" "$SERVER:/tmp/PyMySQL-0.10.1-py2.py3-none-any.whl"
fi
sshpass -p "$PASS" ssh -o StrictHostKeyChecking=no "$SERVER" \
    "pip3 install --no-index --find-links=/tmp /tmp/PyMySQL-0.10.1-py2.py3-none-any.whl 2>/dev/null || pip3 install --no-index --find-links=/tmp PyMySQL-0.10.1-py2.py3-none-any.whl 2>/dev/null || echo 'pymysql may already be installed'"

# 3. Create systemd service
echo ""
echo "[3/6] Configuring systemd service..."
sshpass -p "$PASS" ssh -o StrictHostKeyChecking=no "$SERVER" "cat > /etc/systemd/system/etf-backtest.service << 'ENDOFSERVICE'
[Unit]
Description=ETF Backtest API Service v3 (MySQL)
After=network.target mysqld.service

[Service]
Type=simple
User=root
WorkingDirectory=/opt/etf_backend
ExecStart=/usr/bin/python3 /opt/etf_backend/api_server.py
Restart=always
RestartSec=5
Environment=PORT=5000
Environment=MYSQL_HOST=127.0.0.1
Environment=MYSQL_PORT=32306
Environment=MYSQL_USER=root
Environment=MYSQL_PASSWORD=root@2024
Environment=MYSQL_DATABASE=etf_backtest
Environment=LLM_API_URL=https://api.moonshot.cn/v1/chat/completions
Environment=LLM_API_KEY=sk-x01UXnDclLoj5hq7aO8cjZ9WaW43t4KHyR975DnK2AL7hDcZ
Environment=LLM_MODEL=moonshot-v1-8k
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
ENDOFSERVICE"

# 4. Reload & restart
echo ""
echo "[4/6] Starting service..."
sshpass -p "$PASS" ssh -o StrictHostKeyChecking=no "$SERVER" \
    "systemctl daemon-reload && systemctl enable etf-backtest && systemctl stop etf-backtest 2>/dev/null; systemctl start etf-backtest"

# 4.5. Setup daily prediction cron
echo ""
echo "[4.5/6] Setting up daily prediction cron (00:01 daily)..."
CRON_LINE='1 0 * * * MYSQL_HOST=127.0.0.1 MYSQL_PORT=32306 MYSQL_USER=root MYSQL_PASSWORD=root@2024 MYSQL_DATABASE=etf_backtest LLM_API_URL=https://api.moonshot.cn/v1/chat/completions LLM_API_KEY=sk-x01UXnDclLoj5hq7aO8cjZ9WaW43t4KHyR975DnK2AL7hDcZ LLM_MODEL=moonshot-v1-8k /usr/bin/python3 /opt/etf_backend/daily_predict.py >> /var/log/etf_predict.log 2>&1'
sshpass -p "$PASS" ssh -o StrictHostKeyChecking=no "$SERVER" \
    "(crontab -l 2>/dev/null | grep -v 'daily_predict.py'; echo '$CRON_LINE') | crontab -"
echo "Cron installed."

# 5. Wait and verify
echo ""
echo "[5/6] Verifying health..."
sleep 3
SERVICE_STATUS=$(sshpass -p "$PASS" ssh -o StrictHostKeyChecking=no "$SERVER" "systemctl is-active etf-backtest")
echo "Service: $SERVICE_STATUS"

# 6. API test
echo ""
echo "[6/6] Testing API..."
sshpass -p "$PASS" ssh -o StrictHostKeyChecking=no "$SERVER" \
    "curl -s http://127.0.0.1:5000/api/health"
echo ""

# Test backtest endpoint
sshpass -p "$PASS" ssh -o StrictHostKeyChecking=no "$SERVER" \
    "curl -s 'http://127.0.0.1:5000/api/backtest?pattern=up2&start=2009-10&end=2026-05' | python3 -c '
import sys,json
d=json.load(sys.stdin)
if \"error\" in d:
    print(\"ERROR: \" + d[\"error\"])
else:
    print(\"Pattern: \" + d[\"patternLabel\"] + \", ETFs: \" + str(len(d[\"etfs\"])))
    for e in d[\"etfs\"]:
        s=e[\"stats\"]
        print(\"  %s: t=%d y=%.1f%%\" % (e[\"name\"], s[\"total\"], s[\"yin_prob\"]))
' 2>/dev/null || echo 'API test failed - check logs'"

echo ""
echo "=== Deploy Complete ==="
echo "API: http://znufe.ltd/api/etf/backtest?pattern=up2&start=2009-10&end=2026-05"
echo "Page: http://znufe.ltd/etf/report.html"
echo ""
echo "=== Frontend Note ==="
echo "Frontend HTML files must be copied to /var/www/etf/ (nginx root):"
echo "  scp ../etf_backtest/*.html root@124.222.87.177:/var/www/etf/"
