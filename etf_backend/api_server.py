#!/usr/bin/env python3
"""
ETF月线回测API服务 v3 - MySQL数据源
支持6种模式：2/3/4连阳 + 2/3/4连阴
支持时间区间筛选
数据从MySQL读取，按月线K线实时计算回测结果
"""

import json
import os
import time
import threading
import urllib.request
import urllib.error
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import pymysql
from flask import Flask, request, jsonify

app = Flask(__name__)


# ===== CORS =====
@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


# ===== MySQL 配置 =====
MYSQL_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("MYSQL_PORT", "32306")),
    "user": os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", "root@2024"),
    "database": os.environ.get("MYSQL_DATABASE", "etf_backtest"),
    "charset": "utf8mb4",
    "connect_timeout": 5,
    "cursorclass": pymysql.cursors.DictCursor,
}


def get_db():
    """获取数据库连接"""
    return pymysql.connect(**MYSQL_CONFIG)


# ===== ETF 列表 =====
ETFS = {
    "sh000001": "上证指数",
    "sh510300": "沪深300ETF",
    "sh510050": "上证50ETF",
    "sz159915": "创业板ETF",
    "sh510500": "中证500ETF",
    "sh588000": "科创50ETF",
}

# ===== 模式定义 =====
PATTERNS = {
    # ---- 精确筛选（K线形态） ----
    "up2":    {"n": 2, "dir": "up",   "label": "2连阳后第3个月",   "target": "阴线", "type": "exact"},
    "up3":    {"n": 3, "dir": "up",   "label": "3连阳后第4个月",   "target": "阴线", "type": "exact"},
    "up4":    {"n": 4, "dir": "up",   "label": "4连阳后第5个月",   "target": "阴线", "type": "exact"},
    "down2":  {"n": 2, "dir": "down", "label": "2连阴后第3个月",   "target": "阳线", "type": "exact"},
    "down3":  {"n": 3, "dir": "down", "label": "3连阴后第4个月",   "target": "阳线", "type": "exact"},
    "down4":  {"n": 4, "dir": "down", "label": "4连阴后第5个月",   "target": "阳线", "type": "exact"},
    # ---- 模糊筛选（涨跌幅阈值 ≥1%） ----
    "fuzzy_up2":   {"n": 2, "dir": "up",   "label": "2连阳(≥1%)后第3个月", "target": "阴线", "type": "fuzzy", "threshold": 1},
    "fuzzy_up3":   {"n": 3, "dir": "up",   "label": "3连阳(≥1%)后第4个月", "target": "阴线", "type": "fuzzy", "threshold": 1},
    "fuzzy_down2": {"n": 2, "dir": "down", "label": "2连阴(≤-1%)后第3个月", "target": "阳线", "type": "fuzzy", "threshold": -1},
    "fuzzy_down3": {"n": 3, "dir": "down", "label": "3连阴(≤-1%)后第4个月", "target": "阳线", "type": "fuzzy", "threshold": -1},
}


# ===== 数据查询层 =====
def _row_to_dict(row: Dict) -> Dict:
    """将MySQL行转为回测引擎使用的字典格式"""
    return {
        "d": str(row["trade_date"])[:7],  # YYYY-MM
        "o": float(row["open_price"]),
        "c": float(row["close_price"]),
        "h": float(row["high_price"]),
        "l": float(row["low_price"]),
        "t": row["k_type"],
        "p": float(row["change_pct"] or 0),
        "v": int(row["volume"] or 0),
    }


def get_kline(symbol: str) -> List[Dict]:
    """从MySQL获取月线K线数据，按日期升序"""
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT trade_date, open_price, close_price, high_price, low_price,
                       k_type, change_pct, volume
                FROM etf_kline
                WHERE symbol = %s
                ORDER BY trade_date ASC
            """
            cursor.execute(sql, (symbol,))
            rows = cursor.fetchall()
            return [_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def get_kline_filtered(symbol: str, start_date: str, end_date: str) -> List[Dict]:
    """从MySQL获取指定时间区间的月线K线"""
    conn = get_db()
    try:
        # start_date 格式: "2009-10" → "2009-10-01"
        # end_date 格式:   "2026-05" → "2026-05-31"
        start_dt = start_date + "-01"
        end_dt = end_date + "-31"
        with conn.cursor() as cursor:
            sql = """
                SELECT trade_date, open_price, close_price, high_price, low_price,
                       k_type, change_pct, volume
                FROM etf_kline
                WHERE symbol = %s
                  AND trade_date >= %s
                  AND trade_date <= %s
                ORDER BY trade_date ASC
            """
            cursor.execute(sql, (symbol, start_dt, end_dt))
            rows = cursor.fetchall()
            return [_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def get_all_etfs_data(start_date: str, end_date: str) -> Dict[str, List[Dict]]:
    """一次性获取全部ETF数据"""
    conn = get_db()
    try:
        start_dt = start_date + "-01"
        end_dt = end_date + "-31"
        etf_list = list(ETFS.keys())
        placeholders = ",".join(["%s"] * len(etf_list))
        with conn.cursor() as cursor:
            sql = f"""
                SELECT symbol, trade_date, open_price, close_price, high_price, low_price,
                       k_type, change_pct, volume
                FROM etf_kline
                WHERE symbol IN ({placeholders})
                  AND trade_date >= %s
                  AND trade_date <= %s
                ORDER BY symbol, trade_date ASC
            """
            cursor.execute(sql, tuple(etf_list) + (start_dt, end_dt))
            rows = cursor.fetchall()

        result = {k: [] for k in ETFS}
        for row in rows:
            result[row["symbol"]].append(_row_to_dict(row))
        return result
    finally:
        conn.close()


# ===== 回测引擎 (不变) =====
def scan_pattern(months: List[Dict], n: int, direction: str) -> List[Dict]:
    """扫描连续N根同向K线后的次月表现"""
    target_type = "阳线" if direction == "up" else "阴线"
    cases = []

    for i in range(n, len(months)):
        match = True
        for j in range(1, n + 1):
            if months[i - j]["t"] != target_type:
                match = False
                break
        if match:
            next_month = months[i]
            trigger_start = months[i - n]
            trigger_end = months[i - 1]
            cum_ret = 0.0
            if trigger_start["o"] != 0:
                cum_ret = round((trigger_end["c"] - trigger_start["o"]) / trigger_start["o"] * 100, 2)

            cases.append({
                "trigger": trigger_end["d"],
                "triggerStart": trigger_start["d"],
                "nextDate": next_month["d"],
                "nextOpen": next_month["o"],
                "nextClose": next_month["c"],
                "nextType": next_month["t"],
                "nextChg": next_month["p"],
                "cumRet": cum_ret,
            })

    return cases


def scan_pattern_threshold(months: List[Dict], n: int, direction: str, threshold: float) -> List[Dict]:
    """模糊筛选：扫描连续N个月月内涨跌幅均超过阈值后的次月表现
    
    注意：使用 open/close 实时计算月内涨跌幅 (close-open)/open*100，
    不依赖数据库中的 change_pct 字段（历史数据该字段可能存的是开盘缺口）。
    """
    cases = []

    for i in range(n, len(months)):
        match = True
        for j in range(1, n + 1):
            m = months[i - j]
            # 实时计算月内涨跌幅，避免依赖可能存在的数据问题
            if m["o"] != 0:
                monthly_ret = round((m["c"] - m["o"]) / m["o"] * 100, 2)
            else:
                monthly_ret = 0.0

            if direction == "up" and monthly_ret < threshold:
                match = False
                break
            if direction == "down" and monthly_ret > threshold:
                match = False
                break
        if match:
            next_month = months[i]
            trigger_start = months[i - n]
            trigger_end = months[i - 1]
            cum_ret = 0.0
            if trigger_start["o"] != 0:
                cum_ret = round((trigger_end["c"] - trigger_start["o"]) / trigger_start["o"] * 100, 2)

            cases.append({
                "trigger": trigger_end["d"],
                "triggerStart": trigger_start["d"],
                "nextDate": next_month["d"],
                "nextOpen": next_month["o"],
                "nextClose": next_month["c"],
                "nextType": next_month["t"],
                "nextChg": next_month["p"],
                "cumRet": cum_ret,
            })

    return cases


def aggregate_cases(cases: List[Dict]) -> Dict:
    """汇总案例统计"""
    yin = [c for c in cases if c["nextType"] == "阴线"]
    yang = [c for c in cases if c["nextType"] == "阳线"]
    doji = [c for c in cases if c["nextType"] == "十字星"]
    total = len(cases)

    return {
        "total": total,
        "yin_cnt": len(yin),
        "yang_cnt": len(yang),
        "doji_cnt": len(doji),
        "yin_prob": round(len(yin) / total * 100, 1) if total > 0 else 0,
        "yang_prob": round(len(yang) / total * 100, 1) if total > 0 else 0,
        "doji_prob": round(len(doji) / total * 100, 1) if total > 0 else 0,
        "cases": cases,
    }


def run_backtest(pattern: str, start_date: str, end_date: str, full: bool = False) -> Dict:
    """执行回测 - 使用批量查询优化"""
    pd_info = PATTERNS.get(pattern)
    if not pd_info:
        raise ValueError(f"Unknown pattern: {pattern}")

    # 批量获取全部ETF数据（一次SQL查询）
    all_data = get_all_etfs_data(start_date, end_date)

    result = {
        "pattern": pattern,
        "patternLabel": pd_info["label"],
        "patternDir": pd_info["dir"],
        "patternTarget": pd_info["target"],
        "dateRange": {"start": start_date, "end": end_date},
        "etfs": [],
    }

    for code, name in ETFS.items():
        months = all_data.get(code, [])
        if len(months) < pd_info["n"] + 1:
            continue

        cases = scan_pattern(months, pd_info["n"], pd_info["dir"]) if pd_info.get("type") != "fuzzy" \
            else scan_pattern_threshold(months, pd_info["n"], pd_info["dir"], pd_info["threshold"])
        stats = aggregate_cases(cases)

        date_range_str = f"{months[0]['d']} ~ {months[-1]['d']}" if months else "N/A"

        result["etfs"].append({
            "code": code,
            "name": name,
            "dateRange": date_range_str,
            "monthCount": len(months),
            "stats": stats,
            "cases": cases if full else cases[:12],
        })

    return result


def run_backtest_advanced(
    pattern: str, start_date: str, end_date: str,
    stop_loss: float = -8.0, take_profit: float = 15.0
) -> Dict:
    """增强回测 — 带止损止盈参数，输出最大回撤、夏普比率、持有期收益分布。

    Args:
        pattern:     PATTERNS 中的 key
        start_date:  开始月份，格式 YYYY-MM
        end_date:    结束月份，格式 YYYY-MM
        stop_loss:   止损触发阈值（负值，如 -8.0 表示跌超8%止损），0 表示不止损
        take_profit: 止盈触发阈值（正值，如 15.0 表示涨超15%止盈），0 表示不止盈
    """
    pd_info = PATTERNS.get(pattern)
    if not pd_info:
        raise ValueError(f"Unknown pattern: {pattern}")

    all_data = get_all_etfs_data(start_date, end_date)

    def _apply_sl_tp(ret: float, sl: float, tp: float) -> tuple:
        """模拟止损/止盈后的实际收益与触发类型。"""
        if tp > 0 and ret >= tp:
            return tp, "take_profit"
        if sl < 0 and ret <= sl:
            return sl, "stop_loss"
        return ret, "hold"

    def _calc_metrics(rets: List[float]) -> Dict:
        """从月度收益率列表计算统计指标。"""
        if not rets:
            return {}
        n = len(rets)
        avg = sum(rets) / n
        # 最大回撤（以等额资金，每次独立持有）
        max_dd = min(rets) if rets else 0.0
        # 胜率
        wins = [r for r in rets if r > 0]
        win_rate = round(len(wins) / n * 100, 1) if n else 0.0
        # 平均盈利 / 平均亏损
        losers = [r for r in rets if r <= 0]
        avg_win  = round(sum(wins) / len(wins), 2) if wins else 0.0
        avg_loss = round(sum(losers) / len(losers), 2) if losers else 0.0
        # 盈亏比
        profit_ratio = round(avg_win / abs(avg_loss), 2) if avg_loss != 0 else float('inf')
        # 夏普比（月度，无风险利率 0）
        import math
        std = math.sqrt(sum((r - avg) ** 2 for r in rets) / n) if n > 1 else 0.0
        sharpe = round((avg / std) * (12 ** 0.5), 2) if std > 0 else 0.0  # 年化
        # 期望收益
        expectancy = round(win_rate / 100 * avg_win + (1 - win_rate / 100) * avg_loss, 2)
        return {
            "avg_ret": round(avg, 2),
            "win_rate": win_rate,
            "max_single_dd": round(max_dd, 2),
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "profit_ratio": profit_ratio,
            "sharpe_annualized": sharpe,
            "expectancy": expectancy,
            "n": n,
        }

    result = {
        "pattern": pattern,
        "patternLabel": pd_info["label"],
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "dateRange": {"start": start_date, "end": end_date},
        "etfs": [],
    }

    for code, name in ETFS.items():
        months = all_data.get(code, [])
        if len(months) < pd_info["n"] + 1:
            continue

        cases = scan_pattern(months, pd_info["n"], pd_info["dir"]) if pd_info.get("type") != "fuzzy" \
            else scan_pattern_threshold(months, pd_info["n"], pd_info["dir"], pd_info["threshold"])

        if not cases:
            continue

        # 对每个 case 应用止损/止盈
        enhanced_cases = []
        raw_rets = []
        adj_rets = []
        sl_cnt = 0
        tp_cnt = 0

        for c in cases:
            raw_ret = c["nextChg"]  # 原始次月涨跌幅
            adj_ret, trigger_type = _apply_sl_tp(raw_ret, stop_loss, take_profit)
            raw_rets.append(raw_ret)
            adj_rets.append(adj_ret)
            if trigger_type == "stop_loss":   sl_cnt += 1
            if trigger_type == "take_profit": tp_cnt += 1
            enhanced_cases.append({**c, "adjChg": round(adj_ret, 2), "trigger": trigger_type})

        raw_metrics = _calc_metrics(raw_rets)
        adj_metrics = _calc_metrics(adj_rets)

        result["etfs"].append({
            "code": code,
            "name": name,
            "total_cases": len(cases),
            "stop_loss_hits": sl_cnt,
            "take_profit_hits": tp_cnt,
            "raw_metrics": raw_metrics,
            "adj_metrics": adj_metrics,
            "cases": enhanced_cases[:15],  # 最近15条
        })

    return result


def run_backtest_all(start_date: str, end_date: str) -> Dict:
    """执行全部6种模式回测 - 批量查询"""
    all_data = get_all_etfs_data(start_date, end_date)
    all_patterns = {}

    for key, pd_info in PATTERNS.items():
        result = {
            "pattern": key,
            "patternLabel": pd_info["label"],
            "patternDir": pd_info["dir"],
            "patternTarget": pd_info["target"],
            "etfs": [],
        }

        for code, name in ETFS.items():
            months = all_data.get(code, [])
            if len(months) < pd_info["n"] + 1:
                continue

            cases = scan_pattern(months, pd_info["n"], pd_info["dir"]) if pd_info.get("type") != "fuzzy" \
                else scan_pattern_threshold(months, pd_info["n"], pd_info["dir"], pd_info["threshold"])
            stats = aggregate_cases(cases)

            result["etfs"].append({
                "code": code,
                "name": name,
                "dateRange": f"{months[0]['d']} ~ {months[-1]['d']}" if months else "N/A",
                "monthCount": len(months),
                "stats": stats,
                "cases": cases[:12],
            })

        all_patterns[key] = result

    return {
        "dateRange": {"start": start_date, "end": end_date},
        "patterns": all_patterns,
    }


# ===== 市场区间识别 =====
def detect_market_regimes():
    """基于上证指数月线数据，用 SMA6 + 斜率对每个月做四分类"""
    months = get_kline("sh000001")
    if len(months) < 7:
        return {"periods": [], "monthly": {}}

    # 计算 SMA6（6月简单移动均线）
    sma6 = []
    for i in range(len(months)):
        if i >= 5:
            avg = sum(months[j]["c"] for j in range(i - 5, i + 1)) / 6.0
        else:
            window = months[: i + 1]
            avg = sum(m["c"] for m in window) / len(window)
        sma6.append(avg)

    # 逐月分类
    classifications = []
    for i in range(len(months)):
        m = months[i]
        close = m["c"]
        s6 = sma6[i]

        # SMA6 斜率（3个月变化率，%）
        if i >= 8:
            s6_3m_ago = sma6[i - 3]
            slope = round((s6 - s6_3m_ago) / s6_3m_ago * 100, 2) if s6_3m_ago > 0 else 0.0
        else:
            slope = 0.0

        price_pos = round((close - s6) / s6 * 100, 2) if s6 > 0 else 0.0

        # 四分类规则
        if close > s6 * 1.05 and slope > 0:
            regime = "强牛市"
        elif close > s6 and slope > 0:
            regime = "牛市"
        elif close < s6 and slope < 0:
            regime = "熊市"
        else:
            regime = "震荡市"

        classifications.append({
            "date": m["d"],
            "regime": regime,
        })

    # 合并连续同区间 → periods（至少3个月）
    periods = []
    i = 0
    while i < len(classifications):
        regime = classifications[i]["regime"]
        j = i
        while j < len(classifications) and classifications[j]["regime"] == regime:
            j += 1
        if j - i >= 3:  # 至少连续3个月才能形成有效区间段
            start_m = months[i]
            end_m = months[j - 1]
            ret = round((end_m["c"] - start_m["c"]) / start_m["c"] * 100, 2) if start_m["c"] > 0 else 0
            periods.append({
                "regime": regime,
                "start": classifications[i]["date"],
                "end": classifications[j - 1]["date"],
                "months": j - i,
                "return_pct": ret,
            })
        i = j

    # 月度字典
    monthly = {c["date"]: c["regime"] for c in classifications}

    return {
        "periods": periods,
        "monthly": monthly,
    }


# ===== 数据库统计端点 =====
@app.route("/api/etfs")
def api_etfs():
    """获取ETF列表及数据概览"""
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT symbol, MIN(trade_date) as min_date, MAX(trade_date) as max_date,
                       COUNT(*) as cnt
                FROM etf_kline
                GROUP BY symbol
                ORDER BY symbol
            """
            cursor.execute(sql)
            rows = cursor.fetchall()

        etfs_list = []
        for row in rows:
            code = row["symbol"]
            etfs_list.append({
                "code": code,
                "name": ETFS.get(code, code),
                "startDate": str(row["min_date"])[:7],
                "endDate": str(row["max_date"])[:7],
                "recordCount": row["cnt"],
            })
        return jsonify({"etfs": etfs_list})
    finally:
        conn.close()


# ===== 市场区间端点 =====
@app.route("/api/regime")
def api_regime():
    """市场区间分类"""
    try:
        result = detect_market_regimes()
        return jsonify(result)
    except Exception as e:
        print(f"[Error] Regime detection failed: {e}", flush=True)
        return jsonify({"error": str(e)}), 500


# ===== API 端点 =====
@app.route("/api/health")
def health():
    """健康检查 + 数据库连接状态"""
    try:
        conn = get_db()
        conn.close()
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {e}"

    return jsonify({
        "status": "ok",
        "db": db_status,
        "time": datetime.now().isoformat(),
    })


@app.route("/api/backtest")
def api_backtest():
    """
    回测接口
    GET /api/backtest?pattern=up2&start=2009-10&end=2026-05
    """
    pattern = request.args.get("pattern", "up2")
    start = request.args.get("start", "2009-10")
    end = request.args.get("end", "2026-05")
    full = request.args.get("full", "").lower() == "true"

    if pattern not in PATTERNS:
        return jsonify({"error": f"Unknown pattern: {pattern}. Valid: {list(PATTERNS.keys())}"}), 400

    try:
        result = run_backtest(pattern, start, end, full=full)
        return jsonify(result)
    except Exception as e:
        print(f"[Error] Backtest failed: {e}", flush=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/backtest/all")
def api_backtest_all():
    """
    全量回测接口（一次性返回6种模式）
    GET /api/backtest/all?start=2009-10&end=2026-05
    """
    start = request.args.get("start", "2009-10")
    end = request.args.get("end", "2026-05")

    try:
        result = run_backtest_all(start, end)
        return jsonify(result)
    except Exception as e:
        print(f"[Error] Backtest all failed: {e}", flush=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/backtest/advanced")
def api_backtest_advanced():
    """
    增强回测接口 — 带止损止盈参数、输出夏普比/最大回撤/盈亏比
    GET /api/backtest/advanced?pattern=up2&start=2009-10&end=2026-05&stop_loss=-8&take_profit=15
    """
    pattern     = request.args.get("pattern", "up2")
    start       = request.args.get("start", "2009-10")
    end         = request.args.get("end", "2026-05")
    stop_loss   = float(request.args.get("stop_loss", -8.0))
    take_profit = float(request.args.get("take_profit", 15.0))

    if pattern not in PATTERNS:
        return jsonify({"error": f"Unknown pattern: {pattern}. Valid: {list(PATTERNS.keys())}"}), 400

    try:
        result = run_backtest_advanced(pattern, start, end, stop_loss=stop_loss, take_profit=take_profit)
        return jsonify(result)
    except Exception as e:
        print(f"[Error] Advanced backtest failed: {e}", flush=True)
        return jsonify({"error": str(e)}), 500


# ===== ETF K线数据端点 =====
@app.route("/api/kline")
def api_etf_kline():
    """
    获取指定ETF的原始月线K线数据（含candle_type）
    GET /api/etf/kline?symbol=sh000001&start=1993-02&end=2026-05
    """
    symbol = request.args.get("symbol", "")
    start = request.args.get("start", "2009-10")
    end = request.args.get("end", "2026-05")

    if not symbol or symbol not in ETFS:
        return jsonify({"error": f"Unknown symbol: {symbol}. Valid: {list(ETFS.keys())}"}), 400

    try:
        rows = get_kline_filtered(symbol, start, end)

        # 计算candle_type并转为前端格式
        data = []
        for r in rows:
            close = r["c"]
            open_p = r["o"]
            if close > open_p:
                candle_type = "阳线"
            elif close < open_p:
                candle_type = "阴线"
            else:
                candle_type = "十字星"

            data.append({
                "date": r["d"],
                "open": round(open_p, 3),
                "close": round(close, 3),
                "high": round(r["h"], 3),
                "low": round(r["l"], 3),
                "volume": r["v"],
                "amount": 0.0,  # MySQL表中无amount字段
                "pct_chg": round(r["p"], 2),
                "candle_type": candle_type,
            })

        date_range = f"{data[0]['date']} ~ {data[-1]['date']}" if data else "N/A"

        return jsonify({
            "symbol": symbol,
            "name": ETFS.get(symbol, symbol),
            "count": len(data),
            "date_range": date_range,
            "data": data,
        })
    except Exception as e:
        print(f"[Error] ETF kline failed: {e}", flush=True)
        return jsonify({"error": str(e)}), 500


# ===== 市场情绪 API (T-1 隔日数据 · 混合缓存方案) =====
# 策略：
#   - 非交易时段（盘后/周末）: clist API 实时拉取 → 存持久化文件缓存 + 设 trade_date = T-1
#   - 交易时段（9:30-15:30）: 从持久化文件缓存读取 → data_source = "T-1隔日(缓存)"
#   - 这样确保任何时候返回的都是上一次收盘数据，符合"隔日"要求

import os as _os

SENTIMENT_CACHE_FILE = "/opt/etf_backend/sentiment_cache.json"

# 内存级短期缓存（30秒），避免文件I/O过于频繁
_sentiment_mem_cache: Dict[str, Any] = {}
_sentiment_mem_lock = threading.Lock()
MEM_CACHE_TTL = 30

ALL_A_STOCKS_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
API_CLIST = "https://push2.eastmoney.com/api/qt/clist/get"
API_KLINE_IDX = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

# 指数成分股分类（从 sentiment_v2 引入）
INDEX_LABELS = ["sz50", "hs300", "zz500", "zz1000", "cyb", "kcb"]
INDEX_NAMES = {"sz50": "上证50", "hs300": "沪深300", "zz500": "中证500",
               "zz1000": "中证1000", "cyb": "创业板", "kcb": "科创板"}

def _get_index_classifier():
    """懒加载指数成分股分类器（从 sentiment_v2 借调）"""
    try:
        from sentiment_v2 import _load_index_classifier
        return _load_index_classifier()
    except Exception:
        return None

def _classify_stock_code(code, classifier):
    """按代码前缀+成分股映射分类到指数"""
    labels = set()
    code_clean = code.lstrip("sh").lstrip("sz").lstrip("bj")
    if code_clean.startswith("30"):
        labels.add("cyb")
    if code_clean.startswith("688"):
        labels.add("kcb")
    if classifier:
        for lbl in ["sz50", "hs300", "zz500", "zz1000"]:
            if code in classifier.get(lbl, set()):
                labels.add(lbl)
    return labels

# 全市场A股约数（用于外推涨跌停）
TOTAL_A_STOCKS = 5500

_SENTIMENT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.eastmoney.com/",
}


def _is_market_data_stale() -> bool:
    """判断当前是否在A股交易时段内。
    交易时段 9:30-15:30 期间，clist 返回的是实时盘中数据，
    此时应使用持久化缓存返回隔日（T-1收盘）数据。
    """
    import datetime as _dt
    now = _dt.datetime.now()
    weekday = now.weekday()
    if weekday >= 5:
        return False  # 周末，clist 返回的就是 T-1 收盘数据
    t = now.hour * 60 + now.minute
    return 570 <= t < 930  # 9:30 = 570, 15:30 = 930


def _get_latest_trading_day() -> str:
    """最近已完成交易日 (T-1，跳过周末和中国法定假日)"""
    import datetime as _dt
    today = _dt.date.today()
    day = today - _dt.timedelta(days=1)
    # 回退直到找到真正的交易日
    for _ in range(30):  # 最多30天，足以覆盖春节等长假
        if _is_trading_day(day):
            return day.strftime("%Y%m%d")
        day -= _dt.timedelta(days=1)
    # 极端兜底：返回昨天的日期
    return (today - _dt.timedelta(days=1)).strftime("%Y%m%d")


def _sentiment_http_get(url: str, timeout: int = 10, retries: int = 2) -> Optional[Dict]:
    """情绪数据HTTP请求，带重试"""
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, headers=_SENTIMENT_HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                text = raw.decode("utf-8-sig") if raw[:3] == b'\xef\xbb\xbf' else raw.decode("utf-8")
                return json.loads(text)
        except (urllib.error.URLError, urllib.error.HTTPError,
                ConnectionError, TimeoutError, OSError, json.JSONDecodeError):
            if attempt < retries:
                time.sleep((attempt + 1) * 1.0)
    return None


def _sentiment_safe_float(val, default=0.0):
    if val is None or val == "-" or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _load_persistent_cache() -> Optional[Dict]:
    """加载持久化文件缓存"""
    try:
        if _os.path.exists(SENTIMENT_CACHE_FILE):
            with open(SENTIMENT_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"[Sentiment] 加载缓存失败: {e}", flush=True)
    return None


def _save_persistent_cache(data: Dict) -> None:
    """保存持久化文件缓存（去除 _ts 内部字段）"""
    try:
        to_save = {k: v for k, v in data.items() if k != "_ts"}
        _dirname = _os.path.dirname(SENTIMENT_CACHE_FILE)
        if _dirname:
            _os.makedirs(_dirname, exist_ok=True)
        with open(SENTIMENT_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(to_save, f, ensure_ascii=False)
    except Exception as e:
        print(f"[Sentiment] 保存缓存失败: {e}", flush=True)


def _fetch_all_dimensions() -> Dict:
    """使用 clist API 拉取全部维度数据（非交易时段调用）
    
    维度:
      1. ADR (涨跌比) - 从全市场涨跌家数计算
      2. 涨跌停家数 - 从 clist 分页统计
      3. 成交额偏离度 - 从指数 K线历史计算
    
    返回:
      { "trade_date": "2026-05-26", "adr": {...}, "limits": {...}, 
        "volume_deviation": {...}, "northbound_flow": {...}, "margin_balance": {...} }
    """
    trade_date = _get_latest_trading_day()
    trade_date_fmt = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
    errors = []

    # === 维度1+2: ADR & 涨跌停（clist 实时行情直接拿涨跌幅） ===
    adr_result = {"up": 0, "down": 0, "flat": 0, "adr": 0.0, "sample_size": 0, "status": "error", "error": None}
    limits_result = {"limit_up": 0, "limit_down": 0, "sample_up": 0, "sample_down": 0,
                      "extrapolated": False, "status": "error", "error": None}

    try:
        up = down = flat = 0
        lu = ld = 0
        valid = 0
        # 指数涨跌停归类统计
        idx_clf = _get_index_classifier()
        idx_lu = {lbl: 0 for lbl in INDEX_LABELS}
        idx_ld = {lbl: 0 for lbl in INDEX_LABELS}
        # 一次拉取全量A股（上证+深证+科创+创业板，约5500只）
        url = (f"{API_CLIST}?pn=1&pz=6000&po=1&np=1&fltt=2&invt=2"
               f"&fid=f12&fs={ALL_A_STOCKS_FS}"
               f"&fields=f2,f3,f4,f12,f14")
        data = _sentiment_http_get(url, timeout=15, retries=2)
        if data and data.get("data") and data["data"].get("diff"):
            for s in data["data"]["diff"]:
                pct = _sentiment_safe_float(s.get("f3"), 0)  # 涨跌幅%
                if abs(pct) < 0.01:
                    continue  # 停牌或无效
                valid += 1
                if pct > 0: up += 1
                elif pct < 0: down += 1
                else: flat += 1
                # 区分涨跌停阈值：主板10%、科创(sh688)/创业(sz30) 20%
                code = s.get("f12", "")
                is_20 = code.startswith("sh688") or code.startswith("sz30")
                is_lu = is_ld = False
                if is_20:
                    if pct >= 19.8: is_lu = True; lu += 1
                    if pct <= -19.8: is_ld = True; ld += 1
                else:
                    if pct >= 9.8: is_lu = True; lu += 1
                    if pct <= -9.8: is_ld = True; ld += 1
                # 涨跌停股票按指数归类
                if is_lu or is_ld:
                    stock_labels = _classify_stock_code(code, idx_clf)
                    for lbl in stock_labels:
                        if is_lu: idx_lu[lbl] += 1
                        if is_ld: idx_ld[lbl] += 1

        adr_result["up"], adr_result["down"], adr_result["flat"] = up, down, flat
        adr_result["sample_size"] = valid
        adr_v = round(up / max(down, 1), 2) if valid > 0 else 1.0
        adr_result["adr"] = adr_v

        if adr_v >= 3.0: adr_result["status"] = "extreme_bullish"
        elif adr_v >= 2.0: adr_result["status"] = "bullish"
        elif adr_v >= 1.2: adr_result["status"] = "slightly_bullish"
        elif adr_v >= 0.8: adr_result["status"] = "neutral"
        elif adr_v >= 0.5: adr_result["status"] = "slightly_bearish"
        elif adr_v >= 0.3: adr_result["status"] = "bearish"
        else: adr_result["status"] = "extreme_bearish"

        # 涨跌停：全量数据，不再外推
        limits_result["limit_up"] = lu
        limits_result["limit_down"] = ld
        limits_result["sample_up"] = lu
        limits_result["sample_down"] = ld
        limits_result["extrapolated"] = False
        # 指数分类涨跌停（供前端 M5b 卡片柱状图使用）
        limits_result["index_breakdown"] = {
            lbl: {
                "name": INDEX_NAMES.get(lbl, lbl),
                "limit_up": idx_lu.get(lbl, 0),
                "limit_down": idx_ld.get(lbl, 0),
            }
            for lbl in INDEX_LABELS
        }

        l_u, l_d = limits_result["limit_up"], limits_result["limit_down"]
        if l_u == 0 and l_d == 0: limits_result["status"] = "neutral"
        elif l_u >= 100 and l_d <= 10: limits_result["status"] = "extreme_bullish"
        elif l_u >= 50 and l_d <= 20: limits_result["status"] = "bullish"
        elif l_d >= 100 and l_u <= 10: limits_result["status"] = "extreme_bearish"
        elif l_d >= 50 and l_u <= 20: limits_result["status"] = "bearish"
        elif l_u >= l_d * 2: limits_result["status"] = "slightly_bullish"
        elif l_d >= l_u * 2: limits_result["status"] = "slightly_bearish"
        else: limits_result["status"] = "neutral"

    except Exception as e:
        adr_result["error"] = str(e)
        limits_result["error"] = str(e)
        errors.append(f"ADR/涨跌停: {e}")

    # === 维度3: 成交额偏离度（从指数K线历史获取） ===
    volume_result = {"today_amount": 0.0, "avg20_amount": 0.0, "deviation_pct": 0.0, "status": "error", "error": None}
    try:
        total_today = 0.0
        total_avg20 = 0.0
        for secid in ["1.000001", "0.399001"]:
            url = (f"{API_KLINE_IDX}?secid={secid}"
                   f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57"
                   f"&klt=101&fqt=1&end=20500101&lmt=25")
            data = _sentiment_http_get(url, timeout=8, retries=1)
            if not data or not data.get("data") or not data["data"].get("klines"):
                continue
            klines = data["data"]["klines"]

            # 找 trade_date 对应 K线
            target_idx = -1
            for i, k_str in enumerate(klines):
                if k_str[:10] == trade_date_fmt:
                    target_idx = i
                    break
            if target_idx >= 0:
                k = klines[target_idx].split(",")
                if len(k) >= 7:
                    total_today += max(0, _sentiment_safe_float(k[6], 0))
                prev_amounts = []
                for j in range(max(0, target_idx - 20), target_idx):
                    parts = klines[j].split(",")
                    if len(parts) >= 7:
                        prev_amounts.append(max(0, _sentiment_safe_float(parts[6], 0)))
                if prev_amounts:
                    total_avg20 += sum(prev_amounts) / len(prev_amounts)
            time.sleep(0.15)

        volume_result["today_amount"] = round(total_today / 1e8, 2)
        volume_result["avg20_amount"] = round(total_avg20 / 1e8, 2)

        if total_today > 0 and total_avg20 > 0:
            dev = (total_today - total_avg20) / total_avg20 * 100
            volume_result["deviation_pct"] = round(dev, 2)
            if dev >= 80: volume_result["status"] = "extreme_high"
            elif dev >= 40: volume_result["status"] = "high"
            elif dev >= 15: volume_result["status"] = "slightly_high"
            elif dev >= -15: volume_result["status"] = "normal"
            elif dev >= -35: volume_result["status"] = "slightly_low"
            elif dev >= -55: volume_result["status"] = "low"
            else: volume_result["status"] = "extreme_low"
        else:
            volume_result["error"] = "指数成交额数据不足"
    except Exception as e:
        volume_result["error"] = str(e)
        errors.append(f"成交额: {e}")

    # === 维度4+5: 北向 & 融资（暂不实时获取，保持 neutral） ===
    northbound = {"status": "pending", "note": "需通过 westock-data 或 neodata 插件补充"}
    margin = {"status": "pending", "note": "需通过 westock-data 或 web_search 补充"}

    return {
        "trade_date": trade_date_fmt,
        "adr": adr_result,
        "limits": limits_result,
        "volume_deviation": volume_result,
        "northbound_flow": northbound,
        "margin_balance": margin,
        "errors": errors,
    }


def _calc_sentiment_score(adr: Dict, limits: Dict, volume: Dict) -> Dict:
    """计算综合情绪评分（0-10）"""
    scores = {}
    # ADR
    adr_v = adr.get("adr", 1.0)
    if adr_v >= 3.0: scores["adr"] = 10.0
    elif adr_v >= 2.0: scores["adr"] = 8.0 + (adr_v - 2.0) / 1.0 * 2.0
    elif adr_v >= 1.5: scores["adr"] = 6.0 + (adr_v - 1.5) / 0.5 * 2.0
    elif adr_v >= 1.2: scores["adr"] = 5.0 + (adr_v - 1.2) / 0.3 * 1.0
    elif adr_v >= 0.8: scores["adr"] = 4.5 + (adr_v - 0.8) / 0.4 * 0.5
    elif adr_v >= 0.5: scores["adr"] = 2.0 + (adr_v - 0.5) / 0.3 * 2.5
    elif adr_v >= 0.3: scores["adr"] = 1.0 + (adr_v - 0.3) / 0.2 * 1.0
    else: scores["adr"] = max(0.0, adr_v / 0.3 * 1.0)

    # 涨停跌停
    lu, ld = limits.get("limit_up", 0), limits.get("limit_down", 0)
    if lu == 0 and ld == 0: scores["limits"] = 5.0
    elif lu >= 150 and ld <= 5: scores["limits"] = 10.0
    elif lu >= 100 and ld <= 10: scores["limits"] = 9.0
    elif lu >= 50 and ld <= 15: scores["limits"] = 7.5
    elif lu >= 30 and ld <= 20: scores["limits"] = 6.5
    elif lu >= ld * 3: scores["limits"] = 7.0
    elif lu >= ld * 2: scores["limits"] = 6.0
    elif lu >= ld: scores["limits"] = 5.0
    elif ld >= lu * 3: scores["limits"] = 1.0
    elif ld >= lu * 2: scores["limits"] = 3.0
    elif ld >= 100: scores["limits"] = 0.5
    elif ld >= 50: scores["limits"] = 2.0
    else: scores["limits"] = 4.0

    # 成交额偏离
    dev = volume.get("deviation_pct", 0)
    if dev >= 80: scores["volume"] = 9.0
    elif dev >= 40: scores["volume"] = 7.0 + (dev - 40) / 40 * 2.0
    elif dev >= 15: scores["volume"] = 6.0 + (dev - 15) / 25 * 1.0
    elif dev >= -15: scores["volume"] = 5.0 + dev / 15 * 1.0
    elif dev >= -35: scores["volume"] = 3.0 + (dev + 35) / 20 * 2.0
    elif dev >= -55: scores["volume"] = 1.0 + (dev + 55) / 20 * 2.0
    else: scores["volume"] = max(0.0, 1.0 + dev / 55 * 1.0)

    # 北向 & 融资（无数据时中性 5.0）
    scores["northbound"] = 5.0
    scores["margin"] = 5.0

    weights = {"adr": 0.25, "limits": 0.25, "volume": 0.15, "northbound": 0.20, "margin": 0.15}
    for k in scores:
        scores[k] = round(max(0.0, min(10.0, scores[k])), 1)
    composite = round(sum(scores[k] * weights[k] for k in weights), 1)

    if composite >= 8.0: category, label = "extreme_greed", "极度贪婪"
    elif composite >= 6.5: category, label = "greed", "乐观"
    elif composite >= 4.5: category, label = "neutral", "中性"
    elif composite >= 2.5: category, label = "fear", "恐慌"
    else: category, label = "extreme_fear", "极度恐慌"

    return {"scores": scores, "composite": composite, "category": category, "label": label, "weights": weights}


def _build_result(dimensions: Dict, data_source: str, is_stale: bool) -> Dict:
    """将原始维度数据组装为最终 API 响应"""
    trade_date = dimensions.get("trade_date", _get_latest_trading_day())
    adr = dimensions.get("adr", {})
    limits = dimensions.get("limits", {})
    volume = dimensions.get("volume_deviation", {})

    result = {
        "success": False,
        "trade_date": trade_date,
        "data_source": data_source,
        "is_stale": is_stale,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": {
            "advance_decline": adr,
            "limit_counts": limits,
            "volume_deviation": volume,
            "northbound_flow": dimensions.get("northbound_flow", {"status": "pending"}),
            "margin_balance": dimensions.get("margin_balance", {"status": "pending"}),
        },
        "summary": _calc_sentiment_score(adr, limits, volume),
        "errors": dimensions.get("errors", []),
        "pending_dimensions": 2,
    }

    valid = sum(1 for k in ["advance_decline", "limit_counts", "volume_deviation"]
                if not result["data"].get(k, {}).get("error"))
    result["success"] = valid >= 2
    result["valid_dimensions"] = valid
    return result


def fetch_sentiment_data() -> Dict:
    """获取完整市场情绪数据（T-1隔日，混合缓存方案）
    
    策略：
    - 非交易时段: 实时拉取(此时 clist 返回的就是 T-1 收盘数据) → 更新文件缓存
    - 交易时段: 从文件缓存读取 → 确保返回隔日数据
    """
    global _sentiment_mem_cache
    now = time.time()

    # 内存缓存（30秒）- 减轻文件I/O压力
    with _sentiment_mem_lock:
        if _sentiment_mem_cache and (now - _sentiment_mem_cache.get("_ts", 0)) < MEM_CACHE_TTL:
            return _sentiment_mem_cache

    is_stale = _is_market_data_stale()

    if is_stale:
        # === 交易时段: 从文件缓存读取 ===
        cached = _load_persistent_cache()
        if cached and cached.get("trade_date"):
            result = cached.copy()
            result["data_source"] = "T-1隔日(缓存)"
            result["is_stale"] = True
            result["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            result["_ts"] = now
            with _sentiment_mem_lock:
                _sentiment_mem_cache = result
            print(f"[Sentiment] 交易时段，从缓存返回 (trade_date={result.get('trade_date')})", flush=True)
            return result

        # 缓存为空（首次部署或缓存文件丢失），降级实时拉取但标记为 stale
        print("[Sentiment] 交易时段但缓存为空，降级为实时拉取", flush=True)
        dimensions = _fetch_all_dimensions()
        result = _build_result(dimensions, "T-1隔日(降级实时)", is_stale=True)
        # 顺便保存缓存供后续使用
        cache_data = {k: v for k, v in result.items() if k != "_ts"}
        _save_persistent_cache(cache_data)
        result["_ts"] = now
        with _sentiment_mem_lock:
            _sentiment_mem_cache = result
        return result

    # === 非交易时段: 实时拉取 + 更新文件缓存 ===
    dimensions = _fetch_all_dimensions()
    result = _build_result(dimensions, "T-1隔日(clist盘后)", is_stale=False)

    # 保存到文件缓存
    cache_data = {k: v for k, v in result.items() if k != "_ts"}
    _save_persistent_cache(cache_data)

    result["_ts"] = now
    with _sentiment_mem_lock:
        _sentiment_mem_cache = result

    print(f"[Sentiment] 非交易时段，实时拉取 (trade_date={result.get('trade_date')}, "
          f"adr={result['data']['advance_decline'].get('adr')}, "
          f"volume={result['data']['volume_deviation'].get('deviation_pct')}%)", flush=True)
    return result


@app.route("/api/sentiment")
def api_sentiment():
    """
    市场情绪指标 API
    GET /api/sentiment
    返回 T-1 隔日收盘数据（交易时段从缓存读取，非交易时段实时拉取）
    """
    try:
        result = fetch_sentiment_data()

        # 自动持久化到 MySQL（非交易时段 + 缓存首次加载才写，避免重复）
        if result.get("success") and not result.get("is_stale"):
            try:
                db_saved = save_sentiment_v1(result)
                result["db_saved"] = db_saved
            except Exception as e:
                print("[Sentiment] DB save skipped: {}".format(e), flush=True)

        return jsonify(result)
    except Exception as e:
        print(f"[Error] Sentiment fetch failed: {e}", flush=True)
        return jsonify({"error": str(e)}), 500



# ===== 情绪 V2 =====
from sentiment_v2 import get_sentiment_v2

def _calc_judgment_v2(modules):
    """基于 v2 模块数据计算情绪综合研判（规则引擎）。
    返回 judgment 字段，结构与前端 renderJudgment 期望一致：
      { composite, category, label, verdict, scores_detail }
    scores_detail keys: m1_kline, m4_adr, m5_limits, m6_voldev
    """
    scores_detail = {}

    # --- M4: ADR 涨跌家数 ---
    adr_mod = modules.get("adr", {})
    adr_data = adr_mod.get("data", {}) or {}
    adr_val = float(adr_data.get("adr") or 0)
    up = int(adr_data.get("up") or 0)
    down = int(adr_data.get("down") or 0)
    flat = int(adr_data.get("flat") or 0)
    if adr_val >= 3.0:    s_adr = 9.0
    elif adr_val >= 2.0:  s_adr = 8.0
    elif adr_val >= 1.5:  s_adr = 7.0
    elif adr_val >= 1.0:  s_adr = 5.5
    elif adr_val >= 0.7:  s_adr = 4.0
    elif adr_val >= 0.5:  s_adr = 2.5
    else:                 s_adr = 1.0
    s_adr = round(min(10.0, max(0.0, s_adr)), 1)
    scores_detail["m4_adr"] = {
        "label": "涨跌家数(ADR)",
        "score": s_adr,
        "value": "ADR {:.2f} (涨{}跌{})".format(adr_val, up, down),
    }

    # --- M5: 涨跌停 ---
    lim_mod = modules.get("limits", {})
    lim_data = lim_mod.get("data", {}) or {}
    lu = int(lim_data.get("limit_up") or 0)
    ld = int(lim_data.get("limit_down") or 0)
    if lu >= ld * 3 and lu >= 50: s_lim = 9.0
    elif lu >= ld * 2 and lu >= 30: s_lim = 7.5
    elif lu >= ld: s_lim = 5.5
    elif ld >= lu * 3: s_lim = 1.0
    elif ld >= lu * 2: s_lim = 2.5
    else: s_lim = 4.0
    s_lim = round(min(10.0, max(0.0, s_lim)), 1)
    scores_detail["m5_limits"] = {
        "label": "涨跌停",
        "score": s_lim,
        "value": "涨停{}家 跌停{}家".format(lu, ld),
    }

    # --- M6: 成交额偏离度 ---
    vol_mod = modules.get("volume_dev", {})
    vol_data = (vol_mod.get("history") or [{}])[-1] if vol_mod.get("history") else {}
    dev = float(vol_data.get("deviation") or vol_data.get("deviation_pct") or 0)
    if dev >= 80:   s_vol = 9.0
    elif dev >= 40: s_vol = 7.0 + (dev - 40) / 40 * 2.0
    elif dev >= 15: s_vol = 6.0 + (dev - 15) / 25 * 1.0
    elif dev >= -15: s_vol = 5.0 + dev / 15 * 1.0
    elif dev >= -35: s_vol = 3.0 + (dev + 35) / 20 * 2.0
    elif dev >= -55: s_vol = 1.0 + (dev + 55) / 20 * 2.0
    else:           s_vol = max(0.0, 1.0 + dev / 55 * 1.0)
    s_vol = round(min(10.0, max(0.0, s_vol)), 1)
    scores_detail["m6_voldev"] = {
        "label": "成交额偏离",
        "score": s_vol,
        "value": "{:+.1f}%".format(dev),
    }

    # --- M1: K线趋势（基于最近收盘价方向） ---
    sh_mod = modules.get("sh_volume", {})
    kbars = sh_mod.get("data") or []
    sh_bars = [x for x in kbars if x.get("code") == "000001" or "sh" in x.get("code","").lower()]
    if not sh_bars:
        sh_bars = kbars  # fallback
    # 取最后2根判断方向
    if len(sh_bars) >= 2:
        last_close = float((sh_bars[-1] or {}).get("close") or 0)
        prev_close = float((sh_bars[-2] or {}).get("close") or 0)
        pct = (last_close - prev_close) / prev_close * 100 if prev_close else 0
    else:
        pct = 0
    if pct >= 2.0:    s_kline = 8.5
    elif pct >= 1.0:  s_kline = 7.5
    elif pct >= 0.3:  s_kline = 6.5
    elif pct >= -0.3: s_kline = 5.5
    elif pct >= -1.0: s_kline = 4.0
    elif pct >= -2.0: s_kline = 2.5
    else:             s_kline = 1.0
    s_kline = round(min(10.0, max(0.0, s_kline)), 1)
    scores_detail["m1_kline"] = {
        "label": "K线趋势(上证)",
        "score": s_kline,
        "value": "{:+.2f}%".format(pct),
    }

    # --- 综合加权 ---
    weights = {"m1_kline": 0.20, "m4_adr": 0.30, "m5_limits": 0.25, "m6_voldev": 0.25}
    composite = round(sum(
        scores_detail[k]["score"] * weights[k] for k in weights if k in scores_detail
    ), 1)

    if composite >= 8.0:   category, label = "extreme_greed", "极度贪婪"
    elif composite >= 6.5: category, label = "greed", "乐观"
    elif composite >= 4.5: category, label = "neutral", "中性"
    elif composite >= 2.5: category, label = "fear", "恐慌"
    else:                  category, label = "extreme_fear", "极度恐慌"

    # --- 文字总结（verdict）---
    verdict_map = {
        "extreme_greed": "市场情绪极度亢奋，ADR 与涨停占优，成交放量明显。短期注意高位风险，可适当控仓。",
        "greed":         "市场整体偏多，上涨股票占优，情绪较为乐观。可保持正常仓位，关注板块轮动机会。",
        "neutral":       "市场情绪均衡，多空力量基本持平。建议继续持股观望，等待方向性突破信号。",
        "fear":          "市场情绪偏空，下跌股票较多，资金活跃度偏低。建议控制仓位，关注超跌反弹机会。",
        "extreme_fear":  "市场情绪极度悲观，大面积下跌，跌停较多。可关注底部信号，但须防范继续下探风险。",
    }
    verdict = verdict_map.get(category, "市场数据不足，无法评估。")

    return {
        "composite": composite,
        "category": category,
        "label": label,
        "verdict": verdict,
        "scores_detail": scores_detail,
    }


@app.route("/api/sentiment/v2")
def api_sentiment_v2():
    """V2 情绪 - 3个5日趋势模块 + 情绪研判"""
    try:
        result = get_sentiment_v2()
        # 将情绪综合研判附加到返回结果
        try:
            result["judgment"] = _calc_judgment_v2(result.get("modules", {}))
        except Exception as je:
            print("[SentimentV2] judgment calc error: {}".format(je), flush=True)
            result["judgment"] = None
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ===== 情绪数据持久化 & 历史查询 =====
from sentiment_storage import save_sentiment_v2, save_sentiment_v1, query_history
from sentiment_storage import save_prediction, get_latest_prediction, get_prediction_by_date, save_feedback, get_my_feedback, get_my_feedback_list


@app.route("/api/sentiment/history")
def api_sentiment_history():
    """
    查询历史情绪数据
    GET /api/sentiment/history?table=sh_index&days=30
    GET /api/sentiment/history?table=score&days=30
    """
    table = request.args.get("table", "sh_index")
    days = int(request.args.get("days", "30"))
    try:
        rows = query_history(table, days)
        return jsonify({"table": table, "days": days, "count": len(rows), "data": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ===== AI预测 + 用户反馈 =====

LLM_API_URL = os.environ.get("LLM_API_URL", "https://api.scnet.cn/api/llm/v1/chat/completions")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_MODEL   = os.environ.get("LLM_MODEL", "DeepSeek-V4-Flash")


def _build_prediction_prompt(sentiment_data):
    """根据情绪数据构建发给LLM的prompt
    预测逻辑：
      - 不预测成交量，只预测上午/下午走势
      - 广泛收集市场多维度信息，综合分析
    """
    modules  = sentiment_data.get("modules", {})
    sh      = modules.get("sh_volume", {})
    si      = modules.get("search_index", {})
    cf      = modules.get("capital_flow", {})
    adr     = modules.get("adr", {})
    limits  = modules.get("limits", {})
    vdev    = modules.get("volume_dev", {})
    judgment = sentiment_data.get("judgment", {}) or {}

    lines = [
        "你是一位资深A股日内交易分析师。请根据以下所有市场维度信息，"
        "分别预测【今日上午(9:30-11:30)】和【今日下午(13:00-15:00)】的走势，"
        "并给出可操作的盘中建议。"
    ]
    lines.append("")

    # ===== 1. 指数K线（方向、力度）=====
    if sh.get("success") and sh.get("data"):
        bars = sh["data"]
        lines.append("【近5日K线（上证指数）】")
        for d in bars:
            lines.append("  {}  开:{:.2f}  高:{:.2f}  低:{:.2f}  收:{:.2f}".format(
                d.get("date",""),
                float(d.get("open",0)), float(d.get("high",0)),
                float(d.get("low",0)),  float(d.get("close",0))))
        if len(bars) >= 2:
            pct = (float(bars[-1]["close"]) - float(bars[0]["close"])) / float(bars[0]["close"]) * 100
            lines.append("  → 5日累计: {:.2f}%".format(pct))
        if len(bars) >= 2:
            chg = (float(bars[-1]["close"]) - float(bars[-2]["close"])) / float(bars[-2]["close"]) * 100
            lines.append("  → 最新一日: {:+.2f}%  {}".format(
                chg, "阳线↑" if chg > 0 else "阴线↓"))

    # ===== 2. 涨跌家数 ADR =====
    if adr.get("success") and adr.get("data"):
        a = adr["data"]
        lines.append("")
        lines.append("【涨跌家数（全市场）】")
        lines.append("  上涨:{}家  下跌:{}家  平盘:{}家  ADR={:.2f}  {}".format(
            a.get("up",0), a.get("down",0), a.get("flat",0),
            float(a.get("adr",0)), a.get("status","")))

    # ===== 3. 涨跌停 =====
    if limits.get("success") and limits.get("data"):
        l = limits["data"]
        lines.append("")
        lines.append("【涨跌停统计】 涨停:{}家  跌停:{}家  涨停/跌停比:{}  {}".format(
            l.get("limit_up",0), l.get("limit_down",0),
            l.get("ratio",""), l.get("status","")))

    # ===== 4. 成交额偏离 =====
    if vdev.get("success") and vdev.get("data"):
        v = vdev["data"]
        lines.append("")
        lines.append("【成交额偏离度】今日:{:.0f}亿  20日均:{:.0f}亿  偏离:{:.1f}%  {}".format(
            float(v.get("today_amount",0)), float(v.get("avg20_amount",0)),
            float(v.get("deviation_pct",0)), v.get("status","")))

    # ===== 5. 搜索人气 =====
    if si.get("success") and si.get("data"):
        lines.append("")
        lines.append("【搜索人气指数（沪深全市场）】")
        for d in si["data"]:
            lines.append("  {}  人气总值:{:.0f}  5日均:{:.0f}".format(
                d.get("date",""), float(d.get("total_heat",0)), float(d.get("avg_heat",0))))

    # ===== 6. 主力/散户资金流向 =====
    if cf.get("success") and cf.get("data"):
        lines.append("")
        lines.append("【主力 vs 散户资金流向（亿元）】")
        for d in cf["data"]:
            lines.append("  {}  主力净流入:{:.1f}  散户净流入:{:.1f}".format(
                d.get("date",""),
                float(d.get("main_net_yi",0)), float(d.get("retail_net_yi",0))))

    # ===== 7. 情绪综合研判 =====
    if judgment:
        lines.append("")
        lines.append("【情绪综合研判】")
        lines.append("  综合评分: {}/10  市场状态: {}".format(
            judgment.get("composite",""), judgment.get("label","")))
        lines.append("  研判结论: {}".format(judgment.get("verdict","")))
        sd = judgment.get("scores_detail", {})
        if sd:
            for k, v in sd.items():
                lines.append("    · {}: 评分{}  ({})".format(
                    v.get("label",""), v.get("score",""), v.get("value","")))

    # ===== 8. 补充信息要求 =====
    lines.append("")
    lines.append("【补充信息要求】")
    lines.append("  请结合以下维度综合判断（可在回答中引用）：")
    lines.append("    - 近期宏观政策（货币政策、监管动向）")
    lines.append("    - 外盘影响（美股隔夜、港股早盘、人民币汇率）")
    lines.append("    - 板块轮动（当日强势/弱势板块）")
    lines.append("    - 北向/南向资金动态（如有数据）")

    # ===== 预测输出要求 =====
    lines.append("")
    lines.append("【预测输出格式（严格按此格式）】")
    lines.append("")
    lines.append("▌ 上午走势预测 (9:30-11:30)")
    lines.append("  开盘方向：【高开 / 平开 / 低开】")
    lines.append("  主要趋势：【震荡向上 / 单边上行 / 震荡向下 / 单边下行 / 横盘震荡】")
    lines.append("  关键支撑位：_____点（上证）")
    lines.append("  关键压力位：_____点（上证）")
    lines.append("  变盘时间点：_____（如10:00 / 11:00附近）")
    lines.append("")
    lines.append("▌ 下午走势预测 (13:00-15:00)")
    lines.append("  上午惯性：【延续上午方向 / 逆转 / 独立走势】")
    lines.append("  主要趋势：【震荡向上 / 单边上行 / 震荡向下 / 单边下行 / 横盘震荡】")
    lines.append("  尾盘(14:30-15:00)：【有拉升可能 / 有跳水可能 / 平稳收盘】")
    lines.append("")
    lines.append("▌ 盘中操作建议")
    lines.append("  适合买入时段：_____（如上探后回踩确认）")
    lines.append("  参考买入价位：_____（上证点位或ETF价格）")
    lines.append("  止损信号：_____（如破位XXXX点）")
    lines.append("  止盈信号：_____（如冲高放量回落）")
    lines.append("  风险提示：_____（政策/外盘/板块风险）")
    lines.append("")
    lines.append("要求：")
    lines.append("  - 总字数不超过600字")
    lines.append("  - 观点明确，不用'可能''或许'等模糊词")
    lines.append("  - 上午/下午分别给出明确方向判断")
    lines.append("  - 支撑/压力位给出具体点位（不要区间）")

    return "\n".join(lines)


def _call_llm(prompt):
    """调用LLM API（DeepSeek/OpenAI兼容格式）。
    使用 curl subprocess 发送请求，绕过 Python 3.10 无 _ssl 模块的问题。
    """
    if not LLM_API_KEY:
        return None, "LLM_API_KEY not configured"

    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": "你是一位专业的A股市场分析师，擅长根据技术指标和情绪数据做出简洁、明确的短期市场预测。请用中文回答。"},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 600,
        "temperature": 0.7,
    }

    req_body = json.dumps(payload, ensure_ascii=False)
    try:
        import subprocess
        cmd = [
            "curl", "-s", "-X", "POST", LLM_API_URL,
            "-H", "Content-Type: application/json",
            "-H", "Authorization: Bearer " + LLM_API_KEY,
            "-H", "User-Agent: ETF-Backtest/1.0",
            "--data", req_body,
            "--max-time", "40",
            "--connect-timeout", "10",
        ]
        proc = subprocess.run(cmd, capture_output=True, timeout=45)
        if proc.returncode != 0:
            return None, "curl error: " + proc.stderr.decode("utf-8", errors="ignore")[:200]
        raw = proc.stdout.decode("utf-8")
        result = json.loads(raw)
        # 检查 API 层错误
        if "error" in result:
            return None, "API error: " + str(result["error"])[:200]
        content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
        return content.strip(), None
    except subprocess.TimeoutExpired:
        return None, "LLM request timeout (45s)"
    except Exception as e:
        return None, str(e)


def _do_predict_and_save():
    """执行预测并存入数据库（供 API 和 cron 共用）"""
    from sentiment_v2 import get_sentiment_v2
    sentiment = get_sentiment_v2()

    prompt = _build_prediction_prompt(sentiment)
    pred_text, err = _call_llm(prompt)

    if pred_text is None:
        raise RuntimeError("LLM调用失败: " + (err or "未知错误"))

    # 判断方向（排除否定句式如"不会上涨""防止下跌"）
    direction = "neutral"
    # 先检查否定+看涨词的组合 → 实际是 bearish
    negated_bullish = any(nw + bw in pred_text for nw in ["不会", "难以", "不再", "无法", "缺乏"]
                          for bw in ["上涨", "看涨", "反弹", "走强", "上攻"])
    negated_bearish = any(nw + bw in pred_text for nw in ["不会", "难以", "不再", "无法", "缺乏"]
                          for bw in ["下跌", "看跌", "回调", "走弱", "下探"])

    if negated_bullish:
        direction = "bearish"
    elif negated_bearish:
        direction = "bullish"
    else:
        for w in ["上涨", "看涨", "反弹", "走强", "上攻"]:
            if w in pred_text:
                direction = "bullish"
                break
        else:
            for w in ["下跌", "看跌", "回调", "走弱", "下探"]:
                if w in pred_text:
                    direction = "bearish"
                    break

    # 确定预测日期（下一个交易日）
    today = date.today()
    predicted_date = today
    for _ in range(7):
        predicted_date = predicted_date + timedelta(days=1)
        if _is_trading_day(predicted_date):
            break

    # 存入数据库
    pid = save_prediction(
        trade_date=today.isoformat(),
        predicted_date=predicted_date.isoformat(),
        prediction_text=pred_text,
        direction=direction,
        llm_model=LLM_MODEL,
        prompt_raw=prompt,
    )

    return {
        "success": True,
        "prediction_id": pid,
        "trade_date": today.isoformat(),
        "predicted_date": predicted_date.isoformat(),
        "prediction_text": pred_text,
        "direction": direction,
        "llm_model": LLM_MODEL,
        "from_db": False,
    }


@app.route("/api/sentiment/predict")
def api_sentiment_predict():
    """
    获取AI对下一个交易日的预测
    策略：优先从数据库读取今日预测，没有才调用LLM（每日仅一次）
    GET /api/sentiment/predict
    返回: { success, prediction_text, direction, prediction_id, from_db }
    """
    today_str = date.today().isoformat()

    try:
        # 第一步：查数据库，今天是否已有预测
        cached = get_prediction_by_date(today_str)
        if cached:
            return jsonify({
                "success": True,
                "prediction_id": cached["id"],
                "trade_date": cached["trade_date"],
                "predicted_date": cached["predicted_date"],
                "prediction_text": cached["prediction_text"],
                "direction": cached.get("direction", "neutral"),
                "llm_model": cached.get("llm_model", LLM_MODEL),
                "from_db": True,
                "created_at": cached.get("created_at", ""),
            })

        # 第二步：今天还没有预测，调用 LLM 生成
        print("[Predict] No prediction for {} — calling LLM...".format(today_str), flush=True)
        result = _do_predict_and_save()
        return jsonify(result)

    except RuntimeError as e:
        return jsonify({"success": False, "error": str(e)}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/sentiment/feedback", methods=["POST"])
def api_sentiment_feedback():
    """
    提交用户对AI预测的反馈（多用户隔离，基于Cookie user_id）
    POST /api/sentiment/feedback
    Body: { prediction_id, agree, comment }
    """
    try:
        # 从 Cookie 读取 user_id，没有则返回错误
        user_id = request.cookies.get("etf_user_id", "").strip()
        if not user_id:
            return jsonify({"success": False, "error": "无法识别用户，请刷新页面后重试"}), 400

        data = request.get_json(force=True)
        pid = int(data.get("prediction_id", 0))
        agree = data.get("agree", None)
        comment = data.get("comment", "")

        if not pid or agree is None:
            return jsonify({"success": False, "error": "缺少参数: prediction_id, agree"}), 400

        # 检查该用户是否已有该预测的反馈（防止重复提交）
        existing = get_my_feedback(pid, user_id)
        if existing:
            return jsonify({"success": False, "error": "您已对此预测提交过反馈", "feedback_id": existing["id"]}), 409

        user_agent = request.headers.get("User-Agent", "")
        fid = save_feedback(pid, user_id, agree, comment, user_agent)
        return jsonify({"success": True, "feedback_id": fid})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/sentiment/feedback/my")
def api_sentiment_feedback_my():
    """
    查询当前用户的反馈历史（多用户隔离）
    GET /api/sentiment/feedback/my?limit=50
    """
    try:
        user_id = request.cookies.get("etf_user_id", "").strip()
        if not user_id:
            return jsonify({"success": False, "error": "无法识别用户"}), 400

        limit = int(request.args.get("limit", 50))
        rows = get_my_feedback_list(user_id, limit)
        return jsonify({"success": True, "count": len(rows), "data": rows})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/sentiment/feedback/check")
def api_sentiment_feedback_check():
    """
    检查当前用户是否对某个预测已提交反馈
    GET /api/sentiment/feedback/check?prediction_id=123
    """
    try:
        user_id = request.cookies.get("etf_user_id", "").strip()
        if not user_id:
            return jsonify({"success": True, "has_feedback": False})
        pid = int(request.args.get("prediction_id", 0))
        if not pid:
            return jsonify({"success": False, "error": "缺少 prediction_id"}), 400
        existing = get_my_feedback(pid, user_id)
        if existing:
            return jsonify({"success": True, "has_feedback": True,
                            "agree": bool(existing["agree"]),
                            "comment": existing["comment"],
                            "feedback_id": existing["id"]})
        return jsonify({"success": True, "has_feedback": False})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ===== ETF期权交割日 =====

# 中国法定节假日（不含周末，周末已在工作日判断中排除）
# 需要维护的假期列表（格式: (月, 日)）
CN_HOLIDAYS_2026 = {
    (1, 1), (1, 2),   # 元旦
    (2, 16), (2, 17), (2, 18), (2, 19), (2, 20), (2, 23), (2, 24),  # 春节 (2/17除夕)
    (4, 6),            # 清明节
    (5, 1), (5, 4), (5, 5),  # 劳动节
    (6, 19),           # 端午节
    (9, 25),           # 中秋节 (2026-09-25, 农历八月十五)
    (10, 1), (10, 2), (10, 5), (10, 6), (10, 7),  # 国庆节
}

CN_HOLIDAYS_2027 = {
    (1, 1),            # 元旦
    (2, 5), (2, 8), (2, 9), (2, 10), (2, 11), (2, 12),  # 春节 (2027-02-06除夕)
    (3, 26),           # 清明节 (2027-03-26)
    (5, 3), (5, 4),    # 劳动节
    (6, 7),            # 端午节 (2027-06-07, 农历五月初三) - approximate
    (9, 13),           # 中秋节 (2027-09-13, 农历八月十四) - approximate
    (10, 1), (10, 4), (10, 5), (10, 6), (10, 7),  # 国庆节
}


def _is_trading_day(d: date) -> bool:
    """判断是否为A股交易日（简化版：排除周末和已知法定假日）"""
    if d.weekday() >= 5:  # 周六周日
        return False
    key = (d.month, d.day)
    if d.year == 2026 and key in CN_HOLIDAYS_2026:
        return False
    if d.year == 2027 and key in CN_HOLIDAYS_2027:
        return False
    return True


def _next_trading_day(d: date, direction: str = "forward") -> date:
    """获取下一个（或上一个）交易日"""
    delta = 1 if direction == "forward" else -1
    for _ in range(10):
        d = d + timedelta(days=delta)
        if _is_trading_day(d):
            return d
    return d  # fallback


def _prev_trading_day(d: date) -> date:
    """获取前一个交易日"""
    return _next_trading_day(d, direction="backward")


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    """计算某年第n个指定星期几"""
    first = date(year, month, 1)
    # 找到第一个目标星期几
    days_ahead = (weekday - first.weekday()) % 7
    first_target = first + timedelta(days=days_ahead)
    return first_target + timedelta(weeks=n - 1)


def _calculate_expiry_dates(year: int) -> list:
    """
    计算某年所有 ETF 期权交割日（每月第4个周三；若遇非交易日则顺延至前一交易日）
    返回 [(月份, 原始周三, 实际交割日)]
    """
    expiries = []
    for month in range(1, 13):
        raw = _nth_weekday_of_month(year, month, 2, 4)  # 周二=1, 周三=2
        actual = raw
        # 如果不是交易日，往前找最近的交易日
        while not _is_trading_day(actual):
            actual = actual - timedelta(days=1)
        expiry_obj = {
            "month": month,
            "raw_wed": raw.isoformat(),
            "expiry_date": actual.isoformat(),
            "expiry_weekday": actual.strftime("%A"),
        }
        expiries.append(expiry_obj)
    return expiries


def get_next_expiry_warning() -> dict:
    """
    获取下一个ETF期权交割日及预警信息
    - 如果在交割日前3个交易日内（含当日），返回 warning=True
    - 如果今天就是交割日，返回 expiry_today=True
    """
    today = date.today()
    current_year = today.year

    # 计算今年和明年的交割日
    all_expiries = _calculate_expiry_dates(current_year)
    if today.month >= 10:
        all_expiries += _calculate_expiry_dates(current_year + 1)

    # 找到最近的下一个交割日（包括今天之后的）
    next_expiry = None
    for exp in all_expiries:
        exp_date = datetime.strptime(exp["expiry_date"], "%Y-%m-%d").date()
        if exp_date >= today:
            next_expiry = exp
            next_expiry["expiry_date_obj"] = exp_date
            break

    if next_expiry is None:
        return {"error": "无法确定下一个交割日"}

    exp_date = next_expiry["expiry_date_obj"]
    days_until = (exp_date - today).days

    # 计算还剩多少个交易日
    trading_days_left = 0
    cursor = today + timedelta(days=1)
    while cursor <= exp_date:
        if _is_trading_day(cursor):
            trading_days_left += 1
        cursor += timedelta(days=1)

    # 今天是否就是交割日
    expiry_today = (today == exp_date)

    # 是否在预警窗口内（交割日前3个交易日内，含当日）
    in_warning = trading_days_left <= 3 and trading_days_left >= 0

    result = {
        "today": today.isoformat(),
        "next_expiry": next_expiry["expiry_date"],
        "next_expiry_raw": next_expiry["raw_wed"],
        "month": next_expiry["month"],
        "days_until": days_until,
        "trading_days_left": trading_days_left,
        "expiry_today": expiry_today,
        "in_warning": in_warning,
        "warning_level": _warning_level(trading_days_left, expiry_today),
        "warning_message": _warning_message(trading_days_left, expiry_today, next_expiry["expiry_date"]),
        "all_expiries_current_year": [e for e in all_expiries if datetime.strptime(e["expiry_date"], "%Y-%m-%d").date().year == current_year],
    }

    return result


def _warning_level(trading_days_left: int, expiry_today: bool) -> str:
    if expiry_today:
        return "critical"
    if trading_days_left == 0:
        return "critical"  # 明天就是交割日
    if trading_days_left == 1:
        return "high"
    if trading_days_left == 2:
        return "elevated"
    return "none"


def _warning_message(trading_days_left: int, expiry_today: bool, expiry_date_str: str) -> str:
    if expiry_today:
        return f"⚡ 今天（{expiry_date_str}）就是ETF期权交割日！市场波动可能加剧，请密切关注持仓风险。"
    if trading_days_left == 0:
        return f"🔴 明天（{expiry_date_str}）就是ETF期权交割日！请立即检查持仓和风险敞口。"
    if trading_days_left == 1:
        return f"🔶 距ETF期权交割日（{expiry_date_str}）仅剩1个交易日！波动率可能上升。"
    if trading_days_left == 2:
        return f"🔸 距ETF期权交割日（{expiry_date_str}）还有2个交易日，请注意仓位管理。"
    if trading_days_left == 3:
        return f"ℹ 距ETF期权交割日（{expiry_date_str}）还有3个交易日。"
    return ""


@app.route("/api/options-expiry")
def api_options_expiry():
    """ETF期权交割日预警 API"""
    try:
        result = get_next_expiry_warning()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ===== AI预测准确率面板 =====
@app.route("/api/etf/accuracy")
def api_accuracy():
    """AI预测准确率统计面板
    统计过去N天的预测命中率、方向准确率、平均误差
    """
    days = request.args.get("days", 30, type=int)
    try:
        db = get_db()
        cur = db.cursor()

        # 统计预测记录
        cur.execute("""
            SELECT predict_date, direction, confidence, actual_direction,
                   sh_close_pred, sh_close_actual,
                   sz_close_pred, sz_close_actual
            FROM sentiment_predictions
            WHERE predict_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
              AND actual_direction IS NOT NULL
            ORDER BY predict_date DESC
        """, (days,))

        rows = cur.fetchall()
        db.close()

        if not rows:
            return jsonify({"success": True, "data": {"total": 0, "message": "暂无足够回测数据"}, "history": []})

        total = len(rows)
        correct = 0
        details = []

        for r in rows:
            is_correct = r["direction"] == r["actual_direction"]
            if is_correct:
                correct += 1

            sh_err = abs(float(r.get("sh_close_pred", 0) or 0) - float(r.get("sh_close_actual", 0) or 0))
            sz_err = abs(float(r.get("sz_close_pred", 0) or 0) - float(r.get("sz_close_actual", 0) or 0))

            details.append({
                "date": str(r["predict_date"])[:10],
                "pred": r["direction"],
                "actual": r["actual_direction"],
                "correct": is_correct,
                "confidence": float(r.get("confidence", 0) or 0),
                "sh_err": round(sh_err, 2),
                "sz_err": round(sz_err, 2),
            })

        accuracy = round(correct / total * 100, 1) if total > 0 else 0

        # 按置信度分层统计
        high_conf = [d for d in details if d["confidence"] >= 0.7]
        low_conf = [d for d in details if d["confidence"] < 0.7]
        high_acc = round(sum(1 for d in high_conf if d["correct"]) / max(len(high_conf), 1) * 100, 1)
        low_acc = round(sum(1 for d in low_conf if d["correct"]) / max(len(low_conf), 1) * 100, 1)

        # 方向细分
        dir_stats = {}
        for d in details:
            pred = d["pred"]
            if pred not in dir_stats:
                dir_stats[pred] = {"total": 0, "correct": 0}
            dir_stats[pred]["total"] += 1
            if d["correct"]:
                dir_stats[pred]["correct"] += 1

        return jsonify({
            "success": True,
            "data": {
                "total_predictions": total,
                "correct": correct,
                "accuracy_pct": accuracy,
                "high_conf_accuracy": high_acc,
                "low_conf_accuracy": low_acc,
                "high_conf_count": len(high_conf),
                "low_conf_count": len(low_conf),
                "direction_stats": {k: {
                    "total": v["total"],
                    "correct": v["correct"],
                    "accuracy": round(v["correct"] / v["total"] * 100, 1),
                } for k, v in dir_stats.items()},
            },
            "history": details,
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ===== ETF对比工具 =====
@app.route("/api/etf/compare")
def api_etf_compare():
    """多ETF并排对比: 收益率、波动率、夏普比、最大回撤、资金流
    Query params: codes=510050,510300,510500&period=60 (days)
    """
    codes_str = request.args.get("codes", "510050,510300,510500")
    period = request.args.get("period", 60, type=int)
    codes = [c.strip() for c in codes_str.split(",") if c.strip()]

    if len(codes) < 2 or len(codes) > 5:
        return jsonify({"error": "请选择2-5只ETF"}), 400

    ETF_NAMES = {
        "510050": "上证50", "510300": "沪深300", "510500": "中证500",
        "159915": "创业板", "588000": "科创50", "510880": "红利ETF",
        "512880": "证券ETF", "512690": "酒ETF", "515790": "光伏ETF",
    }

    try:
        results = []
        for code in codes:
            market = 1 if code.startswith("51") else 0
            secid = "{}.{}".format(market, code)
            url = ("https://push2his.eastmoney.com/api/qt/stock/kline/get?"
                   "secid={}&fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55,f56,f57"
                   "&klt=101&fqt=1&lmt={}".format(secid, period))
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://quote.eastmoney.com/",
            })
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = json.loads(resp.read().decode("utf-8"))
            klines = raw.get("data", {}).get("klines", [])

            if not klines or len(klines) < 5:
                results.append({"code": code, "name": ETF_NAMES.get(code, code), "error": "insufficient data"})
                continue

            closes = []
            volumes = []
            for line in klines:
                parts = line.split(",")
                closes.append(float(parts[2]))
                volumes.append(float(parts[5]) if len(parts) > 5 else 0)

            # 收益率指标
            ret_1m = round((closes[-1] / closes[-min(22, len(closes))] - 1) * 100, 2)
            ret_3m = round((closes[-1] / closes[-min(66, len(closes))] - 1) * 100, 2) if len(closes) >= 66 else None
            ret_ytd = round((closes[-1] / closes[0] - 1) * 100, 2)

            # 日收益率序列
            daily_rets = [round((closes[i] / closes[i-1] - 1) * 100, 4) for i in range(1, len(closes))]
            avg_ret = round(sum(daily_rets) / len(daily_rets), 4)
            std_ret = round((sum((r - avg_ret) ** 2 for r in daily_rets) / len(daily_rets)) ** 0.5, 4)

            # 夏普比（年化，无风险利率=0.02）
            sharpe = round((avg_ret * 252 - 0.02) / (std_ret * (252 ** 0.5)), 2) if std_ret > 0 else 0

            # 最大回撤
            peak = closes[0]
            max_dd = 0
            for c in closes:
                if c > peak: peak = c
                dd = (peak - c) / peak * 100
                if dd > max_dd: max_dd = dd
            max_dd = round(max_dd, 2)

            # 波动率(年化)
            volatility = round(std_ret * (252 ** 0.5), 2)

            # 涨跌天数
            up_days = sum(1 for r in daily_rets if r > 0)
            down_days = sum(1 for r in daily_rets if r < 0)

            results.append({
                "code": code, "name": ETF_NAMES.get(code, code),
                "latest_price": round(closes[-1], 3),
                "ret_1m": ret_1m, "ret_3m": ret_3m, "ret_ytd": ret_ytd,
                "volatility": volatility, "sharpe": sharpe, "max_drawdown": max_dd,
                "up_days": up_days, "down_days": down_days, "win_rate": round(up_days / (up_days + down_days) * 100, 1),
                "avg_vol_yi": round(sum(volumes) / len(volumes) / 100000000, 2),
            })

        return jsonify({"success": True, "data": results, "period_days": period})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ===== 舆情热词云 =====
@app.route("/api/wordcloud")
def api_wordcloud():
    """市场舆情热词云数据
    来源: 东方财富热搜关键词
    返回 top 50 关键词及搜索热度
    """
    try:
        import sys as _sys
        _sys.path.insert(0, "/opt/etf_backend")
        from sentiment_v2 import _em_hot_keywords

        keywords = _em_hot_keywords()
        if not keywords:
            return jsonify({"success": False, "error": "获取热词失败", "data": []})

        words = [{"word": k["name"], "code": k["code"], "weight": 1.0} for k in keywords[:50]]
        return jsonify({"success": True, "data": words, "count": len(words)})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ===== 龙虎榜监控 =====
@app.route("/api/longhu/today")
def api_longhu_today():
    """当日龙虎榜机构净买入 Top 10
    来源: 东方财富龙虎榜API
    """
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        url = ("https://datacenter.eastmoney.com/securities/api/data/v1/get?"
               "reportName=RPT_DAILYBILLBOARD_DETAILSNEW"
               "&columns=SECURITY_CODE,SECURITY_NAME_ABBR,TRADE_DATE,CLOSE_PRICE,CHANGE_RATE,"
               "TOTAL_NETAMT,BILLBOARD_NET_AMT,ORG_NET_BUY_AMT,JG_NET_BUY_AMT,"
               "SD_NET_BUY_AMT,SEC_NET_BUY_AMT,FREE_MARKET_CAP"
               "&pageSize=20&pageNumber=1&sortColumns=TRADE_DATE,ORG_NET_BUY_AMT"
               "&sortTypes=-1,-1&source=WEB&client=WEB")

        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://data.eastmoney.com/",
        })
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        items = data.get("result", {}).get("data", [])
        if not items:
            # Fallback: 使用 webstock API
            url2 = ("https://push2.eastmoney.com/api/qt/clist/get?"
                    "pn=1&pz=10&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281"
                    "&fltt=2&invt=2&fid=f184&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
                    "&fld=f184>10000000&fields=f2,f3,f12,f14,f62,f64,f66,f124,f184")
            req2 = urllib.request.Request(url2, headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://quote.eastmoney.com/",
            })
            with urllib.request.urlopen(req2, timeout=10) as resp2:
                data2 = json.loads(resp2.read().decode("utf-8"))
            items_raw = data2.get("data", {}).get("diff", [])
            items = []
            for item in items_raw:
                items.append({
                    "SECURITY_CODE": item.get("f12", ""),
                    "SECURITY_NAME_ABBR": item.get("f14", ""),
                    "CLOSE_PRICE": item.get("f2", 0),
                    "CHANGE_RATE": item.get("f3", 0),
                    "ORG_NET_BUY_AMT": item.get("f184", 0),
                    "TOTAL_NETAMT": item.get("f62", 0),
                })

        result = []
        for item in items:
            org_net = float(item.get("ORG_NET_BUY_AMT", 0) or 0)
            if org_net == 0:
                continue
            result.append({
                "code": item.get("SECURITY_CODE", ""),
                "name": item.get("SECURITY_NAME_ABBR", ""),
                "price": float(item.get("CLOSE_PRICE", 0) or 0),
                "pct_chg": float(item.get("CHANGE_RATE", 0) or 0),
                "org_net_yi": round(org_net / 100000000, 2),
                "total_net_yi": round(float(item.get("TOTAL_NETAMT", 0) or 0) / 100000000, 2),
            })

        # 按机构净买入排序 Top 10
        result.sort(key=lambda x: abs(x["org_net_yi"]), reverse=True)
        result = result[:10]

        return jsonify({
            "success": True,
            "data": result,
            "count": len(result),
            "date": today,
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e), "data": []}), 500


# ===== 启动 =====
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("DEBUG", "false").lower() == "true"
    print(f"[ETF Backtest API v3] Starting on port {port} (MySQL @ {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']})...", flush=True)
    app.run(host="0.0.0.0", port=port, debug=debug)
