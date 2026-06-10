#!/usr/bin/env python3
"""
sentiment_v2.py - VPS-deployed market sentiment trend module
Data sources: ifzq.gtimg.cn (K-line), push2delay.eastmoney.com (clist/fflow)
Python 3.6 compatible
"""

import json, os, re, time, subprocess
import urllib.request, urllib.error
from datetime import datetime, timedelta

# TDX (通达信) pytdx integration — 替代 ifzq.gtimg.cn 获取上证指数K线
_TDX_AVAILABLE = False
try:
    from pytdx.hq import TdxHq_API
    _TDX_SERVERS = [
        ('115.238.56.198', 7709),
        ('119.147.212.81', 7709),
        ('218.75.126.9', 7709),
    ]
    _TDX_AVAILABLE = True
except ImportError:
    _TDX_SERVERS = []

# SSL detect — Python 3.10 compiled without _ssl needs curl fallback
_HAS_SSL = True
try:
    import ssl
    ssl.create_default_context()
except Exception:
    _HAS_SSL = False

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sentiment_v2_cache")
os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_TTL = 600
ETF_VAL_CACHE_TTL = 60  # etf_val 缓存60秒，价格敏感模块单独设置

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
REF_EM = "https://quote.eastmoney.com/"
REF_QQ = "https://gu.qq.com/"

def http_get(url, timeout=10, referer=REF_EM):
    """HTTP GET — curl primary (works without Python SSL), urllib fallback."""
    # If SSL available and url is http, use urllib directly (faster)
    if _HAS_SSL and url.startswith("http://"):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": referer})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="ignore")
        except Exception:
            pass
    # curl path — works for both http and https without Python SSL
    try:
        proc = subprocess.run(
            ["curl", "-s", "--max-time", str(timeout),
             "-H", "User-Agent: {}".format(UA),
             "-H", "Referer: {}".format(referer),
             url],
            capture_output=True, text=True, timeout=timeout + 2
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout
    except Exception:
        pass
    # urllib fallback (last resort)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": referer})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        raise e

def _market_closed_today():
    """判断今日是否已收盘（15:00后 + 当天为工作日才视为交易日）"""
    now = datetime.now()
    return now.weekday() < 5 and now.hour >= 15


def _is_trading_day():
    """判断今天是否为交易日（A股：周一至周五，且非节假日）"""
    return datetime.now().weekday() < 5


def _is_weekend():
    """判断今天是否为周末（周六=5, 周日=6）"""
    return datetime.now().weekday() >= 5


def trading_days(n=5):
    """返回最近 n 个**已完成**交易日（排除当日15:00前及周末）。"""
    days = []
    d = datetime.now().date()
    # 如果当天在15:00之前（市场未收盘或未开盘），从昨天开始
    if not _market_closed_today():
        d -= timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y-%m-%d"))
        d -= timedelta(days=1)
    return list(reversed(days))

def load_cache(key):
    path = os.path.join(CACHE_DIR, key + ".json")
    # etf_val 价格敏感，使用单独的短 TTL
    ttl = ETF_VAL_CACHE_TTL if key == "etf_val" else CACHE_TTL
    if os.path.exists(path) and (time.time() - os.path.getmtime(path)) < ttl:
        try:
            with open(path) as fh:
                return json.load(fh)
        except:
            pass
    return None

def save_cache(key, data):
    path = os.path.join(CACHE_DIR, key + ".json")
    with open(path, "w") as fh:
        json.dump(data, fh, ensure_ascii=False)

P2D = "https://push2delay.eastmoney.com/api/qt"
# fs 参数说明:
#   m:0+t:6   = 深市主板A股
#   m:0+t:80  = 深市创业板 (30xxxx)
#   m:1+t:2   = 沪市主板A股
#   m:1+t:23  = 沪市科创板 (688xxx)
# 注意：北交所(83xxxx)通过 BSE_FS 单独扫描并合并
#       避免把6854只新三板全量加入ALL_A_STOCKS_FS，大幅增加分页数
ALL_A_STOCKS_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
BSE_FS = "m:0+t:81"   # 股转系统+北交所: 交易日有f3的83xxxx就是北交所上市，新三板f3="-"自动过滤
TOTAL_A_STOCKS = 5800


def safe_float(val, default=0.0):
    if val is None or val == "-" or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _tdx_get_etf_kline(etf_code, count=260):
    """Use pytdx to fetch ETF daily K-line data.
    etf_code: 6-digit code, e.g. '510050'
    Returns list of dicts: [{date, open, close, high, low, volume_shou}, ...] or None on failure.
    """
    if not _TDX_AVAILABLE:
        return None
    # Determine market: shanghai=1 for 51xxxx/58xxxx, shenzhen=0 for 159xxx
    market = 1 if etf_code.startswith('51') or etf_code.startswith('58') else 0
    for ip, port in _TDX_SERVERS:
        api = None
        try:
            api = TdxHq_API()
            if not api.connect(ip, port, time_out=5):
                continue
            # category=4 for daily K-line of stocks/ETFs
            raw = api.get_security_bars(4, market, etf_code, 0, count)
            if not raw or len(raw) == 0:
                api.disconnect()
                continue
            result = []
            for item in raw:
                close = float(item.get('close', 0))
                if close <= 0:
                    continue
                date_str = str(item.get('datetime', ''))[:10]
                result.append({
                    'date': date_str,
                    'open': float(item.get('open', 0)),
                    'close': close,
                    'high': float(item.get('high', 0)),
                    'low': float(item.get('low', 0)),
                    'volume_shou': int(item.get('vol', 0) or 0) // 100,
                })
            api.disconnect()
            if result:
                return result
        except Exception as e:
            print("[TDX ETF] {}:{} error: {}".format(ip, port, e), flush=True)
            try:
                if api: api.disconnect()
            except:
                pass
    return None


def _tdx_get_index_kline(index_code='000001', count=10):
    """Use pytdx to fetch index K-line data.
    amount_yi is computed from TDX 'amount' field (actual turnover in yuan).
    Returns list of dicts: [{date, open, close, high, low, volume_shou, amount_yi}, ...] or None on failure.
    """
    if not _TDX_AVAILABLE:
        return None
    market = 1 if (index_code == '000001' or index_code.startswith('000') or index_code.startswith('60')) else 0
    for ip, port in _TDX_SERVERS:
        api = None
        try:
            api = TdxHq_API()
            if not api.connect(ip, port, time_out=5):
                continue
            raw = api.get_index_bars(4, market, index_code, 0, count)
            if not raw or len(raw) == 0:
                api.disconnect()
                continue
            result = []
            for item in raw:
                amt = float(item.get('amount', 0) or 0)
                vol = float(item.get('vol', 0) or 0)
                close = float(item.get('close', 0))
                if close <= 0 or amt <= 0:
                    continue
                amount_yi = round(amt / 100_000_000, 2)
                vol_shou = int(vol) // 100 if vol > 0 else 0
                date_str = str(item.get('datetime', ''))[:10]
                result.append({
                    'date': date_str,
                    'open': float(item.get('open', 0)),
                    'close': close,
                    'high': float(item.get('high', 0)),
                    'low': float(item.get('low', 0)),
                    'volume_shou': vol_shou,
                    'amount_yi': amount_yi,
                })
            api.disconnect()
            # 过滤当日未完成的K线
            if result and not _market_closed_today():
                today_str = datetime.now().strftime("%Y-%m-%d")
                result = [r for r in result if r.get('date','') != today_str]
            if result:
                return result
        except Exception as e:
            print("[TDX] {}:{} error: {}".format(ip, port, e), flush=True)
            try:
                if api: api.disconnect()
            except:
                pass
    return None


def _tdx_get_index_kline_raw(index_code='000001', count=50):
    """Use pytdx to fetch raw index K-line data for volume deviation calculation.
    Returns list of (date_str, amount_yi) tuples, or None on failure.
    """
    if not _TDX_AVAILABLE:
        return None
    market = 1 if (index_code == '000001' or index_code.startswith('000') or index_code.startswith('60')) else 0
    for ip, port in _TDX_SERVERS:
        api = None
        try:
            api = TdxHq_API()
            if not api.connect(ip, port, time_out=5):
                continue
            raw = api.get_index_bars(4, market, index_code, 0, count)
            if not raw or len(raw) == 0:
                api.disconnect()
                continue
            result = []
            for item in raw:
                # 使用 TDX 'amount' 字段（实际成交额，元），而非 vol * close
                amt = float(item.get('amount', 0) or 0)
                if amt <= 0:
                    continue
                amount_yi = round(amt / 100_000_000, 2)
                date_str = str(item.get('datetime', ''))[:10]
                result.append((date_str, amount_yi))
            api.disconnect()
            # 过滤当日未完成的K线（15:00前TDX可能返回占位数据）
            if result and not _market_closed_today():
                today_str = datetime.now().strftime("%Y-%m-%d")
                result = [r for r in result if r[0] != today_str]
            if result:
                return result
        except Exception as e:
            print("[TDX] raw {}:{} error: {}".format(ip, port, e), flush=True)
            try:
                if api: api.disconnect()
            except:
                pass
    return None


def _tdx_get_index_amount_raw(index_code='000001', count=50):
    """Fetch TDX index K-line using the 'amount' field (actual yuan turnover).
    Returns list of (date_str, amount_yi) tuples, or None on failure.
    amount field in TDX is total turnover in yuan, divided by 1e8 to get 亿元.
    """
    if not _TDX_AVAILABLE:
        return None
    market = 1 if (index_code == '000001' or index_code.startswith('000') or index_code.startswith('60')) else 0
    for ip, port in _TDX_SERVERS:
        api = None
        try:
            api = TdxHq_API()
            if not api.connect(ip, port, time_out=5):
                continue
            raw = api.get_index_bars(4, market, index_code, 0, count)
            if not raw or len(raw) == 0:
                api.disconnect()
                continue
            result = []
            for item in raw:
                amt = float(item.get('amount', 0) or 0)
                if amt <= 0:
                    continue
                date_str = str(item.get('datetime', ''))[:10]
                result.append((date_str, round(amt / 100_000_000, 2)))
            api.disconnect()
            # 过滤当日未完成的K线
            if result and not _market_closed_today():
                today_str = datetime.now().strftime("%Y-%m-%d")
                result = [r for r in result if r[0] != today_str]
            if result:
                return result
        except Exception as e:
            print("[TDX] amount {}:{} error: {}".format(ip, port, e), flush=True)
            try:
                if api: api.disconnect()
            except:
                pass
    return None


def _tdx_get_combined_amounts(count=50):
    """Fetch BOTH Shanghai (000001) and Shenzhen (399001) index amounts and sum by date.
    Uses TDX 'amount' field (actual turnover in yuan).
    Returns list of (date_str, combined_amount_yi) tuples, or None on failure.
    """
    sh_raw = _tdx_get_index_amount_raw('000001', count)
    sz_raw = _tdx_get_index_amount_raw('399001', count)
    if not sh_raw:
        return sz_raw
    if not sz_raw:
        return sh_raw

    sz_map = {d: amt for d, amt in sz_raw}
    combined = []
    for date_str, sh_amt in sh_raw:
        sz_amt = sz_map.get(date_str, 0)
        combined.append((date_str, round(sh_amt + sz_amt, 2)))
    return combined if combined else None


def _tdx_get_combined_kline(count=10):
    """Fetch SH (000001) K-line with combined SH+SZ amounts using TDX 'amount' field.
    Returns list of dicts with SH OHLC but combined amount_yi, or None on failure.
    """
    sh_kline = _tdx_get_index_kline('000001', count)
    sz_amounts = _tdx_get_index_amount_raw('399001', count)
    if not sh_kline:
        return None
    if sz_amounts:
        sz_map = {d: amt for d, amt in sz_amounts}
        for item in sh_kline:
            sz_amt = sz_map.get(item['date'], 0)
            item['amount_yi'] = round(item['amount_yi'] + sz_amt, 2)
    return sh_kline


# ===== M4+M5: ADR & Limits (push2delay clist, shared fetch) =====
def _fetch_clist_adr_limits():
    """使用 Eastmoney clist 全量A股+北交所分页扫描，统计 ADR + 涨跌停家数。
    
    来源: push2delay.eastmoney.com API
    覆盖: 沪市+深市(ALL_A_STOCKS_FS) + 北交所(BSE_FS独立扫描)
    
    北交所处理:
      - BSE_FS (m:0+t:81) 包含北交所(83xxxx)和新三板，总量约6854只
      - 交易日北交所上市股票(83xxxx)有 f3 涨跌幅数据，新三板股票 f3="-" 自动过滤
      - 独立扫描避免将6854只股票加入主循环，保持扫描效率
    
    采用日期感知缓存（当天首次拉取后缓存 10 分钟），避免同一分钟内 m4/m5 重复请求。
    
    Returns: dict {up, down, flat, lu_sample, ld_sample, valid, total, halted}
    """
    today_str = datetime.now().strftime("%Y%m%d")
    raw_cache_key = "clist_adr_{}".format(today_str)
    cached = load_cache(raw_cache_key)
    if cached:
        print("[SentimentV2] clist_adr: using cached result for {}".format(today_str), flush=True)
        return cached

    stats = [0, 0, 0, 0, 0, 0]  # up, down, flat, lu, ld, valid
    total_stocks = 0

    def _is_20pct_board(code):
        """判断是否为20%涨跌停板块（科创板688/创业板30）"""
        return (code.startswith("688") or code.startswith("30"))

    def _is_30pct_board(code):
        """判断是否为30%涨跌停板块（北交所 83xxxx）"""
        return code.startswith("83")

    def _count(diffs):
        for s in diffs:
            f3_val = s.get("f3")
            # f3 为 None/空/'-' 视为停牌或数据缺失 → halted（不算 flat）
            if f3_val is None or f3_val == "-" or f3_val == "":
                continue  # 停牌股不计入任何统计
            pct = safe_float(f3_val, None)
            if pct is None:
                continue
            stats[5] += 1  # valid (有效交易)
            if pct > 0:    stats[0] += 1  # up
            elif pct < 0:  stats[1] += 1  # down
            else:          stats[2] += 1  # flat (零涨跌)
            # 区分涨跌停阈值：北交所±30%、科创/创业板±20%、主板±10%
            code = s.get("f12", "")
            if _is_30pct_board(code):
                if pct >= 29.95:  stats[3] += 1  # lu (30%板)
                if pct <= -29.95: stats[4] += 1  # ld (30%板)
            elif _is_20pct_board(code):
                if pct >= 19.95:  stats[3] += 1  # lu (20%板)
                if pct <= -19.95: stats[4] += 1  # ld (20%板)
            else:
                if pct >= 9.95:   stats[3] += 1  # lu (10%板)
                if pct <= -9.95:  stats[4] += 1  # ld (10%板)

    def _fetch_page(pn, retries=2):
        """拉取单页，支持重试"""
        url = (P2D + "/clist/get?pn=" + str(pn) + "&pz=100&po=1&np=1&fltt=2&invt=2"
               "&fid=f12&fs=" + ALL_A_STOCKS_FS + "&fields=f2,f3,f4,f12,f14")
        for attempt in range(retries):
            try:
                data = json.loads(http_get(url, timeout=15))
                # 验证返回数据完整性
                diffs = data.get("data", {}).get("diff")
                if diffs is not None:
                    return diffs
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(0.5)
                    continue
                raise e
        return []

    # 第1页：获取 total + 计票
    try:
        diffs = _fetch_page(1)
        total_stocks = 0
        # 从第一页额外获取 total（需要一次完整请求）
        url = (P2D + "/clist/get?pn=1&pz=1&po=1&np=1&fltt=2&invt=2"
               "&fid=f12&fs=" + ALL_A_STOCKS_FS + "&fields=f2,f3,f4,f12,f14")
        try:
            data = json.loads(http_get(url, timeout=10))
            total_stocks = data.get("data", {}).get("total", 0)
        except Exception:
            total_stocks = TOTAL_A_STOCKS  # 回退默认值
        total_pages = (total_stocks + 99) // 100 if total_stocks else 0
        if diffs:
            _count(diffs)
        print("[SentimentV2] clist_adr: total={}, pages={}, page1={} items".format(
            total_stocks, total_pages, len(diffs)), flush=True)
    except Exception as e:
        print("[SentimentV2] clist_adr page 1 error: {}".format(e), flush=True)
        total_pages = 0

    # 后续页面
    page_errors = 0
    failed_pages = []
    for page in range(2, total_pages + 1):
        try:
            diffs = _fetch_page(page)
            if diffs:
                _count(diffs)
        except Exception as e:
            page_errors += 1
            failed_pages.append(page)
            if page_errors <= 3:
                print("[SentimentV2] clist_adr page {} error: {}".format(page, e), flush=True)

    # 失败页重试（最多重试整个失败列表一次）
    if failed_pages:
        print("[SentimentV2] clist_adr: retrying {} failed pages...".format(len(failed_pages)), flush=True)
        retry_failures = []
        for page in failed_pages:
            try:
                diffs = _fetch_page(page, retries=3)
                if diffs:
                    _count(diffs)
                    page_errors -= 1
            except Exception:
                retry_failures.append(page)
        if retry_failures:
            print("[SentimentV2] clist_adr: {} pages still failed after retry: {}".format(
                len(retry_failures), retry_failures[:10]), flush=True)

    # ===== 第2步：补上北交所数据（BSE_FS = m:0+t:81 独立扫描）=====
    # 东方财富 m:0+t:81 包含北交所(83xxxx)和新三板，总量约 6854 只
    # 北交所上市股票(83xxxx) 交易日有 f3 涨跌幅数据，新三板股票 f3="-" 自动被过滤
    # 通过独立请求避免将6854只股票全部加入主循环，保持主循环高效
    #
    # 快照 SH+SZ 的有效/停牌数（BSE 添加前），用于 halted 正确计算
    shsz_valid = stats[5]
    shsz_halted = max(0, total_stocks - shsz_valid)
    bse_valid = 0
    try:
        # 第一次请求获取总数
        bse_url0 = (P2D + "/clist/get?pn=1&pz=1&po=1&np=1&fltt=2&invt=2"
                    "&fid=f3&fs=" + BSE_FS + "&fields=f3,f12")
        bse_data0 = json.loads(http_get(bse_url0, timeout=10))
        bse_total_raw = bse_data0.get("data", {}).get("total", 0)
        bse_pages = (bse_total_raw + 99) // 100 if bse_total_raw else 0
        print("[SentimentV2] clist_adr: BSE total={}, pages={}".format(bse_total_raw, bse_pages), flush=True)

        for bse_pn in range(1, bse_pages + 1):
            bse_url = (P2D + "/clist/get?pn=" + str(bse_pn) + "&pz=100&po=1&np=1&fltt=2&invt=2"
                       "&fid=f3&fs=" + BSE_FS + "&fields=f3,f12,f14")
            try:
                bse_d = json.loads(http_get(bse_url, timeout=12))
                bse_diffs = bse_d.get("data", {}).get("diff") or []
                for s in bse_diffs:
                    code = s.get("f12", "")
                    # 只统计北交所上市股票: 代码以 83 开头 (830xxx-836xxx 等)
                    # 87xxxx/92xxxx/43xxxx 是新三板（基础层/创新层），不统计
                    if not str(code).startswith("83"):
                        continue
                    f3_val = s.get("f3")
                    if f3_val is None or f3_val == "-" or f3_val == "":
                        continue  # 停牌/无数据，跳过
                    pct = safe_float(f3_val, None)
                    if pct is None:
                        continue
                    bse_valid += 1
                    stats[5] += 1  # valid
                    if pct > 0:    stats[0] += 1
                    elif pct < 0:  stats[1] += 1
                    else:          stats[2] += 1
                    # 北交所 ±30% 涨跌停
                    if pct >= 29.95:  stats[3] += 1
                    if pct <= -29.95: stats[4] += 1
            except Exception as bse_e:
                print("[SentimentV2] BSE page {} error: {}".format(bse_pn, bse_e), flush=True)
    except Exception as e:
        print("[SentimentV2] clist_adr: BSE fetch error: {}".format(e), flush=True)

    print("[SentimentV2] clist_adr: BSE valid stocks counted: {}".format(bse_valid), flush=True)

    # ===== 汇总结果 =====
    # stats 统计已包含 SH+SZ (ALL_A_STOCKS_FS) + 北交所 (BSE_FS独立扫描)
    # halted: SH+SZ 部分停牌数（bse_valid 已自动过滤无数据的新三板股票，不存在BSE停牌概念）
    # total: SH+SZ 总量 + BSE 有效数量（含在 valid 中）
    halted = shsz_halted
    display_total = total_stocks + bse_valid
    result = {
        "up": stats[0], "down": stats[1], "flat": stats[2],
        "lu_sample": stats[3], "ld_sample": stats[4],
        "valid": stats[5],
        "total": total_stocks,           # SH+SZ 总量（不含BSE，用于停牌计算）
        "total_incl_bse": display_total, # SH+SZ + BSE 有效（用于展示）
        "halted": halted,
        "bse_valid": bse_valid,
    }
    print("[SentimentV2] clist_adr done: up={}, down={}, flat={}, valid={}, lu={}, ld={}, halted={}, errors={}".format(
        stats[0], stats[1], stats[2], stats[5], stats[3], stats[4], halted, page_errors), flush=True)
    save_cache(raw_cache_key, result)
    return result


# ===== [DEPRECATED] M4+M5: TDX 全市场 ADR/涨跌停扫描 =====
# 已于 2026-06-09 弃用：TDX get_security_list(market=0) 只返回深市 ~1494 只股票，
# market=1 返回空，无法覆盖沪市。改为 Eastmoney clist (_fetch_clist_adr_limits)。
# 保留代码仅供参考，不再被任何函数调用。
def _tdx_fetch_adr_limits():
    """[DEPRECATED] 使用 TDX (通达信) 扫描全A股涨跌家数和涨跌停家数。
    
    通过 get_security_list 获取沪市+深市股票列表，
    再用 get_security_quotes 每批80只批量获取实时行情，
    统计涨/跌/平/涨停/跌停数量。
    
    优势：与通达信客户端同步，数据准确度远高于 Eastmoney clist 分页方案；
          单连接批量处理，无 HTTP 限流和分页丢数据问题。
    
    Returns: dict {up, down, flat, lu, ld, valid} 或 None (TDX 不可用时)
    """
    if not _TDX_AVAILABLE:
        return None

    server_name = ""
    stats = None
    
    for ip, port in _TDX_SERVERS:
        stats = {"up": 0, "down": 0, "flat": 0, "lu": 0, "ld": 0, "valid": 0, "halted": 0}
        api = None
        try:
            api = TdxHq_API()
            if not api.connect(ip, port, time_out=8):
                continue
            server_name = "{}:{}".format(ip, port)
            
            start_t = time.time()
            batches = 0
            
            for market in [1, 0]:   # 先沪市(1) 后深市(0)
                offset = 0
                while True:
                    try:
                        stock_list = api.get_security_list(market, offset)
                    except Exception:
                        stock_list = None
                    
                    if not stock_list or len(stock_list) == 0:
                        break
                    
                    # get_security_list 可能一次返回上千只股票。
                    # get_security_quotes 单次限制约 80 只，所以对返回列表分 chunk 处理。
                    chunk_size = 80
                    for chunk_start in range(0, len(stock_list), chunk_size):
                        chunk = stock_list[chunk_start:chunk_start + chunk_size]
                        codes = []
                        for item in chunk:
                            if isinstance(item, (tuple, list)):
                                code = str(item[0]).zfill(6)
                            elif isinstance(item, dict):
                                code = str(item.get('code', '')).zfill(6)
                            else:
                                continue
                            if code and code != '0' and len(code) >= 6:
                                codes.append((market, code))

                        if not codes:
                            continue

                        try:
                            quotes = api.get_security_quotes(codes)
                        except Exception:
                            quotes = None

                        if quotes:
                            for q in quotes:
                                price = safe_float(q.get('price'), 0)
                                last_close = safe_float(q.get('last_close'), 0)
                                if last_close <= 0:
                                    stats["halted"] += 1
                                    continue

                                # 检测无交易数据：开盘价/最高价/价格均为0 → 停牌或数据缺失
                                open_val = safe_float(q.get('open'), 0)
                                high_val = safe_float(q.get('high'), 0)
                                if open_val <= 0 and high_val <= 0 and price <= 0:
                                    stats["halted"] += 1
                                    continue

                                pct = round((price - last_close) / last_close * 100, 2)
                                stats["valid"] += 1
                                code_str = str(q.get('code', ''))

                                if pct > 0.01:
                                    stats["up"] += 1
                                elif pct < -0.01:
                                    stats["down"] += 1
                                else:
                                    stats["flat"] += 1

                                # 区分涨跌停阈值：科创板(688)/创业板(30) → 20%, 其他 → 10%
                                is_20pct = code_str.startswith('688') or code_str.startswith('30')
                                if is_20pct:
                                    if pct >= 19.90:   stats["lu"] += 1
                                    if pct <= -19.90:  stats["ld"] += 1
                                else:
                                    if pct >= 9.90:    stats["lu"] += 1
                                    if pct <= -9.90:   stats["ld"] += 1

                            batches += 1

                        # chunk 间微小延迟
                        time.sleep(0.02)

                    offset += len(stock_list)
            
            api.disconnect()
            elapsed = time.time() - start_t
            
            print("[TDX ADR] {}: {} stocks valid, {} batches, {:.1f}s".format(
                server_name, stats["valid"], batches, elapsed), flush=True)
            
            # 至少2000只有效交易才认为数据可信
            if stats["valid"] >= 2000:
                return stats
                
        except Exception as e:
            print("[TDX ADR] {} error: {}".format(server_name or "{}:{}".format(ip, port), e), flush=True)
            try:
                if api: api.disconnect()
            except Exception:
                pass

    # 所有服务器都失败，返回已部分收集的统计（若有一定数据量）
    if stats and stats["valid"] >= 1000:
        print("[TDX ADR] partial result with {} valid stocks".format(stats["valid"]), flush=True)
        return stats

    return None


def _get_adr_limits_snap_file():
    return os.path.join(CACHE_DIR, "adr_limits_snapshots.json")

def _load_adr_limits_snaps():
    """加载 ADR/涨跌停快照文件，返回 dict: {date_str -> {up,down,flat,lu,ld}}"""
    path = _get_adr_limits_snap_file()
    if os.path.exists(path):
        try:
            with open(path) as fh:
                return json.load(fh)
        except Exception:
            pass
    return {}

def _save_adr_limits_snap(date_str, up, down, flat, lu, ld):
    """将当日 ADR/涨跌停数据写入快照文件（仅在非零时写入）"""
    if up == 0 and down == 0:
        return
    snaps = _load_adr_limits_snaps()
    snaps[date_str] = {"up": up, "down": down, "flat": flat, "lu": lu, "ld": ld}
    # 只保留最近 30 天
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    snaps = {k: v for k, v in snaps.items() if k >= cutoff}
    try:
        with open(_get_adr_limits_snap_file(), "w") as fh:
            json.dump(snaps, fh, ensure_ascii=False)
    except Exception as e:
        print("[SentimentV2] adr_limits snap save error: {}".format(e), flush=True)


def m4_adr():
    cached = load_cache("adr")
    if cached:
        return cached
    
    raw = None
    source_label = ""
    
    # === 数据源: Eastmoney clist 全市场分页扫描 ===
    # 周末也调用：Eastmoney API 周末返回周五收盘数据，数据正确
    # TDX 已弃用：get_security_list(market=0) 只返回深市 ~1494 只股票
    try:
        raw = _fetch_clist_adr_limits()
        total_shsz = raw.get("total", TOTAL_A_STOCKS)   # SH+SZ 总量
        bse_v = raw.get("bse_valid", 0)
        if bse_v > 0:
            source_label = "Eastmoney clist (SH+SZ {}只 + BSE {}只)".format(total_shsz, bse_v)
        else:
            source_label = "Eastmoney clist (SH+SZ {}只)".format(total_shsz)
        print("[SentimentV2] ADR: clist OK, valid={}, up={}, down={}, lu={}, ld={}, bse={}".format(
            raw["valid"], raw["up"], raw["down"], raw["lu_sample"], raw["ld_sample"], bse_v), flush=True)
    except Exception as e:
        print("[SentimentV2] ADR: clist error: {}".format(e), flush=True)
        raw = {"up": 0, "down": 0, "flat": 0, "lu_sample": 0, "ld_sample": 0, "valid": 0}
        source_label = "error"

    try:
        valid = raw["valid"]
        up, down, flat = raw["up"], raw["down"], raw["flat"]
        lu, ld = raw["lu_sample"], raw["ld_sample"]

        # 当天有效数据：仅在收盘后写入快照（确保数据完整），避免盘中数据污染历史快照
        if valid > 0 and _market_closed_today():
            snap_date = trading_days(1)[-1]  # 最近一个已完成交易日（收盘后=今天）
            _save_adr_limits_snap(snap_date, up, down, flat, lu, ld)

        # 非交易时段（valid==0），优先从快照文件读取昨日收盘数据
        if valid == 0:
            snaps = _load_adr_limits_snaps()
            # 找最近一个有数据的交易日快照
            recent_date = None
            for d in sorted(snaps.keys(), reverse=True):
                if snaps[d].get("up", 0) or snaps[d].get("down", 0):
                    recent_date = d
                    break
            if recent_date:
                snap = snaps[recent_date]
                up = snap.get("up", 0)
                down = snap.get("down", 0)
                flat = snap.get("flat", 0)
                adr_v = round(up / max(down, 1), 2) if (up or down) else 1.0
                out = {
                    "success": True,
                    "data": {
                        "up": up, "down": down, "flat": flat,
                        "adr": adr_v, "sample_size": up + down + flat,
                        "status": _adr_status(adr_v),
                        "is_closing": True,
                    },
                    "source": "snapshot ({} 收盘)".format(recent_date),
                }
                save_cache("adr", out)
                return out

            # 快照也没有，回退 DB 收盘统计
            if _get_db_closing:
                db_row = _get_db_closing()
                if db_row:
                    up = int(db_row.get("adr_up") or 0)
                    down = int(db_row.get("adr_down") or 0)
                    flat = int(db_row.get("adr_flat") or 0)
                    adr_v = float(db_row.get("adr") or 0)
                    if not adr_v and (up or down):
                        adr_v = round(up / max(down, 1), 2)
                    sample = int(db_row.get("adr_sample") or 0)
                    out = {
                        "success": True,
                        "data": {
                            "up": up, "down": down, "flat": flat,
                            "adr": adr_v, "sample_size": sample or (up + down + flat),
                            "status": _adr_status(adr_v),
                            "is_closing": True,
                        },
                        "source": "DB ({} 收盘)".format(db_row.get("trade_date", "")),
                    }
                    save_cache("adr", out)
                    return out

            out = {
                "success": False,
                "error": "非交易时段，暂无历史收盘数据",
                "data": {"up": 0, "down": 0, "flat": 0, "adr": 1.0, "sample_size": 0, "status": "no_data"},
            }
            save_cache("adr", out)
            return out

        adr_v = round(up / max(down, 1), 2) if valid > 0 else 1.0

        out = {
            "success": True,
            "data": {
                "up": up, "down": down, "flat": flat,
                "adr": adr_v, "sample_size": valid,
                "halted": raw.get("halted", 0),
                "status": _adr_status(adr_v),
            },
            "source": source_label,
        }
    except Exception as e:
        out = {"success": False, "error": str(e), "data": {}}
    save_cache("adr", out)
    return out


def _adr_status(adr_val):
    if adr_val >= 3.0: return "extreme_bullish"
    if adr_val >= 2.0: return "bullish"
    if adr_val >= 1.2: return "slightly_bullish"
    if adr_val >= 0.8: return "neutral"
    if adr_val >= 0.5: return "slightly_bearish"
    if adr_val >= 0.3: return "bearish"
    return "extreme_bearish"


def _limits_status(lu, ld):
    """纯函数：根据涨跌停家数返回状态标签"""
    if lu == 0 and ld == 0: return "neutral"
    if lu >= 100 and ld <= 10: return "extreme_bullish"
    if lu >= 50 and ld <= 20: return "bullish"
    if ld >= 100 and lu <= 10: return "extreme_bearish"
    if ld >= 50 and lu <= 20: return "bearish"
    if lu >= ld * 2: return "slightly_bullish"
    if ld >= lu * 2: return "slightly_bearish"
    return "neutral"


def m5_limits():
    cached = load_cache("limits")
    if cached:
        return cached
    
    raw = None
    source_label = ""
    
    # === 数据源: Eastmoney clist 全市场分页扫描 ===
    # 周末也调用：Eastmoney API 周末返回周五收盘数据，数据正确
    # TDX 已弃用：原因同 m4_adr()，get_security_list 缺失沪市数据。
    try:
        raw = _fetch_clist_adr_limits()
        total_shsz = raw.get("total", TOTAL_A_STOCKS)
        bse_v = raw.get("bse_valid", 0)
        if bse_v > 0:
            source_label = "Eastmoney clist (SH+SZ {}只 + BSE {}只)".format(total_shsz, bse_v)
        else:
            source_label = "Eastmoney clist (SH+SZ {}只)".format(total_shsz)
        print("[SentimentV2] limits: clist OK, valid={}, lu={}, ld={}, bse={}".format(
            raw["valid"], raw["lu_sample"], raw["ld_sample"], bse_v), flush=True)
    except Exception as e:
        print("[SentimentV2] limits: clist error: {}".format(e), flush=True)
        raw = {"up": 0, "down": 0, "flat": 0, "lu_sample": 0, "ld_sample": 0, "valid": 0}
        source_label = "error"

    try:
        valid = raw["valid"]
        lu_sample, ld_sample = raw["lu_sample"], raw["ld_sample"]
        # 收盘后写入快照（与 m4_adr 共享同一快照文件）
        if valid > 0 and _market_closed_today():
            snap_date = trading_days(1)[-1]
            up, down, flat = raw["up"], raw["down"], raw["flat"]
            _save_adr_limits_snap(snap_date, up, down, flat, lu_sample, ld_sample)

        # 非交易时段（valid==0），优先从快照文件读取
        if valid == 0:
            snaps = _load_adr_limits_snaps()
            recent_date = None
            for d in sorted(snaps.keys(), reverse=True):
                if snaps[d].get("lu", 0) or snaps[d].get("ld", 0):
                    recent_date = d
                    break
            if recent_date:
                snap = snaps[recent_date]
                limit_up = snap.get("lu", 0)
                limit_down = snap.get("ld", 0)
                out = {
                    "success": True,
                    "data": {
                        "limit_up": limit_up, "limit_down": limit_down,
                        "sample_up": 0, "sample_down": 0,
                        "extrapolated": False,
                        "status": _limits_status(limit_up, limit_down),
                        "ratio": round(limit_up / max(limit_down, 1), 1),
                        "is_closing": True,
                    },
                    "source": "snapshot ({} 收盘)".format(recent_date),
                }
                save_cache("limits", out)
                return out

            # 快照也没有，回退 DB 收盘统计
            if _get_db_closing:
                db_row = _get_db_closing()
                if db_row:
                    limit_up = int(db_row.get("limit_up") or 0)
                    limit_down = int(db_row.get("limit_down") or 0)
                    out = {
                        "success": True,
                        "data": {
                            "limit_up": limit_up, "limit_down": limit_down,
                            "sample_up": 0, "sample_down": 0,
                            "extrapolated": False, "status": _limits_status(limit_up, limit_down),
                            "ratio": round(limit_up / max(limit_down, 1), 1),
                            "is_closing": True,
                        },
                        "source": "DB ({} 收盘)".format(db_row.get("trade_date", "")),
                    }
                    save_cache("limits", out)
                    return out

            out = {
                "success": False,
                "error": "非交易时段，暂无历史收盘数据",
                "data": {"limit_up": 0, "limit_down": 0, "ratio": 0, "status": "no_data"},
            }
            save_cache("limits", out)
            return out

        if valid > 0:
            limit_up = lu_sample
            limit_down = ld_sample
        else:
            limit_up = limit_down = 0

        out = {
            "success": True,
            "data": {
                "limit_up": limit_up, "limit_down": limit_down,
                "sample_up": lu_sample, "sample_down": ld_sample,
                "extrapolated": False, "status": _limits_status(limit_up, limit_down),
                "ratio": round(limit_up / max(limit_down, 1), 1),
                "halted": raw.get("halted", 0),
            },
            "source": source_label,
        }
    except Exception as e:
        out = {"success": False, "error": str(e), "data": {}}
    save_cache("limits", out)
    return out


# ===== M6: Volume Deviation (TDX primary, ifzq fallback) =====
def m6_volume_dev():
    cached = load_cache("vol_dev")
    if cached:
        return cached
    out = None

    # 优先尝试 TDX 两市合计
    tdx_raw = _tdx_get_combined_amounts(count=50)
    if tdx_raw and len(tdx_raw) >= 2:
        dates = [x[0] for x in tdx_raw]
        amounts = [x[1] for x in tdx_raw]

        today_amount = amounts[-1]
        prev_amounts = amounts[-21:-1] if len(amounts) >= 21 else amounts[:-1]
        avg20_amount = round(sum(prev_amounts) / len(prev_amounts), 2) if prev_amounts else today_amount

        deviation = round((today_amount - avg20_amount) / avg20_amount * 100, 2) if avg20_amount > 0 else 0.0

        if deviation >= 80:       status = "extreme_high"
        elif deviation >= 40:     status = "high"
        elif deviation >= 15:     status = "slightly_high"
        elif deviation >= -15:    status = "normal"
        elif deviation >= -35:    status = "slightly_low"
        elif deviation >= -55:    status = "low"
        else:                     status = "extreme_low"

        # 计算最近5个交易日的偏离度历史
        dev_history = []
        for offset in range(4, -1, -1):
            idx = -1 - offset
            if len(amounts) < 21 + offset:
                continue
            day_amount = amounts[idx]
            day_prev = amounts[idx - 20:idx]
            day_avg = sum(day_prev) / len(day_prev) if day_prev else day_amount
            day_dev = round((day_amount - day_avg) / day_avg * 100, 2) if day_avg > 0 else 0.0
            dev_history.append({
                "date": dates[idx],
                "today_amount": round(day_amount, 2),
                "avg20_amount": round(day_avg, 2),
                "deviation_pct": day_dev,
            })

        out = {
            "success": True,
            "data": {
                "today_amount": today_amount,
                "avg20_amount": avg20_amount,
                "deviation_pct": deviation,
                "diff_yi": round(today_amount - avg20_amount, 2),
                "status": status,
            },
            "history": dev_history,
            "source": "pytdx (通达信, 上证+深证合计)",
        }

    if out is None:
        # 兜底: ifzq.gtimg.cn
        try:
            raw = http_get(
                "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
                "?_var=kline_day&param=sh000001,day,,,50,qfq",
                referer=REF_QQ
            )
            raw = raw.replace("kline_day=", "").strip()
            data = json.loads(raw)
            klines = data["data"]["sh000001"]["day"]

            amounts = []
            dates_list = []
            for item in klines:
                amounts.append(float(item[5]) * float(item[2]) / 1e8)
                dates_list.append(item[0])

            if len(amounts) < 2:
                raise Exception("K-line data too short")

            today_amount = round(amounts[-1], 2)
            prev_amounts = amounts[-21:-1] if len(amounts) >= 21 else amounts[:-1]
            avg20_amount = round(sum(prev_amounts) / len(prev_amounts), 2)

            deviation = round((today_amount - avg20_amount) / avg20_amount * 100, 2) if avg20_amount > 0 else 0.0

            if deviation >= 80:       status = "extreme_high"
            elif deviation >= 40:     status = "high"
            elif deviation >= 15:     status = "slightly_high"
            elif deviation >= -15:    status = "normal"
            elif deviation >= -35:    status = "slightly_low"
            elif deviation >= -55:    status = "low"
            else:                     status = "extreme_low"

            # 计算最近5个交易日的偏离度历史
            dev_history = []
            for offset in range(4, -1, -1):
                idx = -1 - offset
                if len(amounts) < 21 + offset:
                    continue
                day_amount = amounts[idx]
                day_prev = amounts[idx - 20:idx] if idx != -1 else amounts[-21:-1]
                day_avg = sum(day_prev) / len(day_prev) if day_prev else day_amount
                day_dev = round((day_amount - day_avg) / day_avg * 100, 2) if day_avg > 0 else 0.0
                dev_history.append({
                    "date": dates_list[idx],
                    "today_amount": round(day_amount, 2),
                    "avg20_amount": round(day_avg, 2),
                    "deviation_pct": day_dev,
                })

            out = {
                "success": True,
                "data": {
                    "today_amount": today_amount,
                    "avg20_amount": avg20_amount,
                    "deviation_pct": deviation,
                    "diff_yi": round(today_amount - avg20_amount, 2),
                    "status": status,
                },
                "history": dev_history,
                "source": "ifzq.gtimg.cn",
            }
        except Exception as e:
            out = {"success": False, "error": str(e), "data": {}}

    save_cache("vol_dev", out)
    return out


# ===== M7: North-bound Capital Flow 5D (沪深港通北向资金) =====
def _backfill_nbsb_from_kline(snap_file, save_func, is_northbound=True, min_days=5):
    """回填北向/南向资金历史快照（当快照不足时调用）

    注意：push2his kamt.kline 接口的 f52(dayNetAmtIn) 字段是"当日买入规模"（接近额度上限），
    并非真实净买入（买-卖差）。真实净买入只能从 push2delay kamt 的 netBuyAmt 字段获取。
    由于历史净买入无法通过单一API批量获取，此函数作为占位符，
    历史数据通过每日调用 m7_northbound/m10_southbound 逐渐积累。
    """
    # push2his kamt.kline 不提供真实净买入历史，跳过
    print("[Backfill] {} skipped - no reliable historical net buy API available".format(
        "NB" if is_northbound else "SB"), flush=True)
    return


def m7_northbound():
    """北向资金（沪股通+深股通）近5日净买入额。

    数据来源：东方财富 push2delay.eastmoney.com
    - API: push2delay/api/qt/kamt/get — 实时快照（万元单位）
      - hk2sh = 北向沪股通（港资→A股上海），hk2sz = 北向深股通（港资→A股深圳）
    - 历史数据通过快照文件积累（每日收盘写入）
    """
    cached = load_cache("northbound")
    if cached:
        return cached

    SNAP_FILE = os.path.join(CACHE_DIR, "northbound_snaps.json")

    def _load_nb_snaps():
        if os.path.exists(SNAP_FILE):
            try:
                with open(SNAP_FILE) as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_nb_snap(date_str, sh_net, sz_net, total):
        snaps = _load_nb_snaps()
        print("[M7 NB] _save_nb_snap: date={}, sh_net={}, sz_net={}, total={}".format(
            date_str, sh_net, sz_net, total), flush=True)
        print("[M7 NB] _save_nb_snap: existing keys={}".format(list(snaps.keys())), flush=True)
        snaps[date_str] = {
            "date": date_str,
            "sh_net_yi": sh_net, "sz_net_yi": sz_net, "total_yi": total,
            # 兼容前端读取 total 字段
            "sh_net": sh_net, "sz_net": sz_net, "total": total,
        }
        keys = sorted(snaps.keys(), reverse=True)
        snaps = {k: snaps[k] for k in keys[:10]}
        print("[M7 NB] _save_nb_snap: saving keys={}".format(list(snaps.keys())), flush=True)
        try:
            with open(SNAP_FILE, "w") as f:
                json.dump(snaps, f, ensure_ascii=False)
            print("[M7 NB] _save_nb_snap: file saved successfully", flush=True)
        except Exception as e:
            print("[M7 NB] _save_nb_snap: ERROR saving file: {}".format(e), flush=True)

    out = None
    try:
        # 拉取今日实时数据（万元单位）
        # fields2 需包含 f59-f64 以获取 buyAmt/sellAmt/netBuyAmt 真实净买入字段
        # 注意：dayNetAmtIn 字段含义是"今日已买入净额/额度"，与 netBuyAmt 不同
        #       netBuyAmt = buyAmt - sellAmt，才是真正的净买入(买卖差)
        url = ("https://push2delay.eastmoney.com/api/qt/kamt/get?"
               "fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64"
               "&klt=101&lmt=1&ut=b2884a393a59ad64002292a3e90d46a5")
        raw = http_get(url, timeout=12)
        data = json.loads(raw)
        d = data.get("data", {})
        sh_north = d.get("hk2sh", {})   # 北向沪股通（港资买上海A股）
        sz_north = d.get("hk2sz", {})   # 北向深股通（港资买深圳A股）
        # 用 netBuyAmt (买-卖差) 作为真实净买入，单位万元
        sh_net_wan = float(sh_north.get("netBuyAmt", 0) or 0)
        sz_net_wan = float(sz_north.get("netBuyAmt", 0) or 0)
        sh_net = round(sh_net_wan / 10000, 2)   # 亿元
        sz_net = round(sz_net_wan / 10000, 2)   # 亿元
        total  = round(sh_net + sz_net, 2)

        # 使用 API 返回的 date2 字段作为准确交易日期（格式 YYYY-MM-DD）
        # 非交易日 date2 可能不存在，兜底用最近已完成交易日
        # 只要有有效数据就立即写入快照（以 snap_date 为 key，天然去重）
        snap_date = (sh_north.get("date2") or sz_north.get("date2")
                     or trading_days(1)[-1])

        # 智能判断是否有真实数据：buyAmt+sellAmt>0 说明市场有交易活动
        # 如果 buyAmt/sellAmt/netBuyAmt 全部为0 → 盘前重置数据，不应保存快照
        sh_buy_sell = abs(float(sh_north.get("buyAmt", 0) or 0)) + abs(float(sh_north.get("sellAmt", 0) or 0))
        sz_buy_sell = abs(float(sz_north.get("buyAmt", 0) or 0)) + abs(float(sz_north.get("sellAmt", 0) or 0))
        is_pre_market = (sh_buy_sell == 0 and sz_buy_sell == 0 and abs(sh_net_wan) == 0 and abs(sz_net_wan) == 0)
        has_data = bool(sh_north.get("date2") or sz_north.get("date2"))
        print("[M7 NB] buySell: sh={:.0f}, sz={:.0f}, is_pre_market={}".format(
            sh_buy_sell, sz_buy_sell, is_pre_market), flush=True)

        if is_pre_market:
            # 盘前数据全零 → 从快照取前一天真实数据用于展示
            snaps_existing = _load_nb_snaps()
            if snaps_existing:
                latest_snap = sorted(snaps_existing.values(), key=lambda x: x.get("date", ""))[-1]
                sh_net = latest_snap.get("sh_net_yi", 0)
                sz_net = latest_snap.get("sz_net_yi", 0)
                total = latest_snap.get("total_yi", 0)
                print("[M7 NB] Pre-market, using snapshot from {}: total={:.2f}亿".format(
                    latest_snap.get("date"), total), flush=True)
        elif has_data:
            _save_nb_snap(snap_date, sh_net, sz_net, total)

        # 从快照构建历史（最近5日）；历史数据通过每日调用自动积累
        snaps = _load_nb_snaps()
        history = sorted(snaps.values(), key=lambda x: x.get("date", ""))[-5:]
        if len(history) < 5:
            print("[M7 NB] Warning: only {} days in history, need {} more days to reach 5".format(
                len(history), 5 - len(history)), flush=True)

        # 盘中补充逻辑（已写入快照，history 已含当日，无需再补）

        if total >= 30:       nb_status = "大幅流入"
        elif total >= 10:     nb_status = "净流入"
        elif total >= 0:      nb_status = "小幅流入"
        elif total >= -10:    nb_status = "小幅流出"
        elif total >= -30:    nb_status = "净流出"
        else:                 nb_status = "大幅流出"

        out = {
            "success": True,
            "data": {
                "total_yi": total,
                "sh_net_yi": sh_net,
                "sz_net_yi": sz_net,
                "status": nb_status,
            },
            "history": history,
            "source": "eastmoney kamt",
        }
    except Exception as e:
        # 接口失败时，从历史快照降级
        try:
            snaps = _load_nb_snaps()
            history = sorted(snaps.values(), key=lambda x: x.get("date", ""))[-5:]
            if len(history) < 5:
                print("[M7 NB] Warning: only {} days in history, need {} more days to reach 5".format(
                    len(history), 5 - len(history)), flush=True)
            if history:
                last = history[-1]
                t = last["total_yi"]
                if t >= 30:       nb_status = "大幅流入"
                elif t >= 10:     nb_status = "净流入"
                elif t >= 0:      nb_status = "小幅流入"
                elif t >= -10:    nb_status = "小幅流出"
                elif t >= -30:    nb_status = "净流出"
                else:             nb_status = "大幅流出"
                out = {
                    "success": True,
                    "data": {
                        "total_yi": last["total_yi"],
                        "sh_net_yi": last["sh_net_yi"],
                        "sz_net_yi": last["sz_net_yi"],
                        "status": nb_status,
                    },
                    "history": history,
                    "source": "northbound snapshot (API fallback)",
                }
        except Exception:
            pass

    if out is None:
        out = {"success": False, "error": "北向资金数据获取失败", "data": {}, "history": []}

    save_cache("northbound", out)
    return out


# ===== M1: SH Volume 5D (TDX primary, ifzq fallback) =====
def m1_sh_volume():
    cached = load_cache("sh_vol")
    if cached:
        return cached
    out = None
    # 优先尝试 TDX — 取两市合计成交额
    tdx_data = _tdx_get_combined_kline(count=10)
    if tdx_data:
        result = tdx_data[-5:] if len(tdx_data) >= 5 else tdx_data
        out = {"success": True, "data": result, "source": "pytdx (通达信, 上证+深证合计)"}
    else:
        # 兜底: ifzq.gtimg.cn（仅上证，标注为仅沪市）
        try:
            raw = http_get(
                "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
                "?_var=kline_day&param=sh000001,day,,,10,qfq",
                referer=REF_QQ
            )
            raw = raw.replace("kline_day=", "").strip()
            data = json.loads(raw)
            klines = data["data"]["sh000001"]["day"]
            result = []
            for item in klines[-5:]:
                result.append({
                    "date": item[0],
                    "open": float(item[1]),
                    "close": float(item[2]),
                    "high": float(item[3]),
                    "low": float(item[4]),
                    "volume_shou": int(float(item[5]) / 100),  # 股->手
                    "amount_yi": round(float(item[5]) * float(item[2]) / 100000000, 2),
                })
            out = {"success": True, "data": result, "source": "ifzq.gtimg.cn (仅沪市)"}
        except Exception as e:
            out = {"success": False, "error": str(e), "data": []}
    save_cache("sh_vol", out)
    return out


# ===== M2: Search Index 5D (push2delay f165 + 东财热搜关键词) =====
def _em_hot_keywords():
    """Fetch Eastmoney (东方财富) hot search keywords count as supplementary search heat indicator.
    Returns list of hot keyword dicts, or None on failure.
    """
    try:
        url = ("https://searchapi.eastmoney.com/api/suggest/get?input=&type=14"
               "&token=D43BF722C8E33BDC906FB84D85E326E8&count=50")
        data = json.loads(http_get(url, timeout=10, referer=REF_EM))
        items = data.get("QuotationCodeTable", {}).get("Data", [])
        if not items:
            return None
        # Extract stock-related keywords only
        keywords = []
        for item in items:
            name = item.get("Name", "")
            code = item.get("Code", "")
            mkt = item.get("MktNum", "")
            if name and code:
                keywords.append({"name": name, "code": code, "market": mkt})
        return keywords
    except Exception as e:
        print("[SentimentV2] EM hot keywords error: {}".format(e), flush=True)
        return None


def _search_snapshot():
    """Take a snapshot of market search heat: push2delay f165 aggregate + EM hot keyword count."""
    result = {
        "stock_count": 0,
        "total_heat": 0,
        "avg_heat": 0,
        "hot_keyword_count": 0,
        "hot_keywords": [],
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    # 1. push2delay f164/f165: A-share stock-level search heat aggregate
    try:
        url = (P2D + "/clist/get?pn=1&pz=500&po=1&np=1&fltt=2&invt=2"
               "&fid=f164&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
               "&fields=f2,f12,f14,f164,f165")
        data = json.loads(http_get(url, timeout=15))
        diffs = data.get("data", {}).get("diff", [])
        if diffs:
            total_heat = 0
            for item in diffs:
                total_heat += (item.get("f165") or 0)
            result["stock_count"] = len(diffs)
            result["total_heat"] = round(total_heat, 2)
            result["avg_heat"] = round(total_heat / len(diffs), 2)
    except Exception as e:
        print("[SentimentV2] push2delay f165 snapshot error: {}".format(e), flush=True)

    # 2. 东方财富热搜关键词: market-level search interest
    kw = _em_hot_keywords()
    if kw:
        result["hot_keyword_count"] = len(kw)
        result["hot_keywords"] = [k["name"] for k in kw[:10]]  # top 10 for display

    return result if result["stock_count"] > 0 or result["hot_keyword_count"] > 0 else None


def m2_search_index():
    cached = load_cache("search_idx")
    if cached:
        return cached
    snap_file = os.path.join(CACHE_DIR, "search_snapshots.json")
    snaps = {}
    if os.path.exists(snap_file):
        try:
            with open(snap_file) as fh:
                snaps = json.load(fh)
        except:
            pass
    today = datetime.now().strftime("%Y-%m-%d")
    snap = _search_snapshot()
    if snap:
        snaps[today] = snap
        cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        snaps = {k: v for k, v in snaps.items() if k >= cutoff}
        with open(snap_file, "w") as fh:
            json.dump(snaps, fh, ensure_ascii=False)
    result = []
    for d in trading_days(5):
        if d in snaps:
            entry = dict(snaps[d])
            entry["date"] = d
            result.append(entry)
    out = {
        "success": True,
        "data": result,
        "source": "push2delay f165 (curl)",
        "snapshots": len(snaps),
    }
    save_cache("search_idx", out)
    return out


# ===== M8: Sector Flow Top 10 (板块资金净流入) =====
def _get_sector_flow_snap_file():
    return os.path.join(CACHE_DIR, "sector_flow_snaps.json")

def _load_sector_flow_snaps():
    path = _get_sector_flow_snap_file()
    if os.path.exists(path):
        try:
            with open(path) as fh:
                return json.load(fh)
        except Exception:
            pass
    return {}

def _save_sector_flow_snap(date_str, sectors):
    snaps = _load_sector_flow_snaps()
    snaps[date_str] = sectors
    keys = sorted(snaps.keys(), reverse=True)[:10]
    snaps = {k: snaps[k] for k in sorted(keys)}
    try:
        with open(_get_sector_flow_snap_file(), "w") as fh:
            json.dump(snaps, fh, ensure_ascii=False)
    except Exception as e:
        print("[SentimentV2] sector_flow snap save error: {}".format(e), flush=True)


# ===== M8: TDX 板块行情 (通达信行业板块实时涨跌幅) =====
# TDX 行业板块指数代码 → 中文名称映射
_TDX_SECTOR_CODES = [
    ("880001", "农林牧渔"), ("880002", "采掘"), ("880005", "有色金属"),
    ("880007", "建筑装饰"), ("880008", "电气设备"), ("880009", "机械设备"),
    ("880011", "计算机"), ("880012", "通信"), ("880015", "食品饮料"),
    ("880017", "轻工制造"), ("880018", "医药生物"), ("880019", "公用事业"),
    ("880031", "电力"), ("880032", "运输设备"), ("880035", "互联网"),
    ("880037", "造纸"), ("880038", "纺织服饰"), ("880039", "环保"),
    ("880041", "化工原料"), ("880042", "化纤"), ("880045", "建材"),
    ("880047", "运输服务"), ("880048", "水务"), ("880049", "工业机械"),
]
# 每批查询的板块数量（TDX get_security_quotes 板块模式限制较小批次）
_TDX_SECTOR_BATCH = len(_TDX_SECTOR_CODES)  # 24个一次查询


def _tdx_fetch_sector_ranking():
    """使用 TDX (通达信) 获取行业板块实时涨跌幅排名。
    
    通过 pytdx 批量查询预定义的行业板块指数代码，
    返回按涨跌幅排序的板块列表。
    
    Returns: list of dicts [{code, name, pct_chg, price, main_net_yi: 0}, ...] 或 None
    """
    if not _TDX_AVAILABLE:
        return None
    
    for ip, port in _TDX_SERVERS:
        api = None
        try:
            api = TdxHq_API()
            if not api.connect(ip, port, time_out=8):
                continue
            
            tdx_codes = [(1, code) for code, name in _TDX_SECTOR_CODES]
            quotes = api.get_security_quotes(tdx_codes)
            api.disconnect()
            
            if not quotes:
                continue
            
            sectors = []
            name_map = dict(_TDX_SECTOR_CODES)
            for q in quotes:
                code = str(q.get("code", ""))
                price = safe_float(q.get("price"))
                last_close = safe_float(q.get("last_close"))
                if last_close <= 0 or price <= 0:
                    continue
                pct_chg = round((price - last_close) / last_close * 100, 2)
                sectors.append({
                    "code": code,
                    "name": name_map.get(code, code),
                    "pct_chg": pct_chg,
                    "price": round(price, 2),
                    "main_net_yi": 0,  # TDX 不提供资金流，填充0保持结构兼容
                    "super_large_yi": 0,
                    "large_yi": 0,
                })
            
            if sectors:
                # 按涨跌幅排序
                sectors.sort(key=lambda s: s["pct_chg"], reverse=True)
                print("[TDX Sector] {} sectors from {}, top: {} ({:+.2f}%)".format(
                    len(sectors), ip, sectors[0]["name"], sectors[0]["pct_chg"]), flush=True)
                return sectors
                
        except Exception as e:
            print("[TDX Sector] {}:{} error: {}".format(ip, port, e), flush=True)
            try:
                if api: api.disconnect()
            except Exception:
                pass
    
    return None


def m8_sector_flow():
    """板块涨跌排行 Top 10 + 最弱 Top 5 + 5日趋势
    数据来源: 优先 TDX (通达信) 板块指数实时涨跌幅，回退 Eastmoney push2delay
    """
    cached = load_cache("sector_flow")
    if cached:
        return cached

    out = None
    is_stale = False
    fallback_mode = None
    source_label = "tdx (通达信板块指数)"

    # === Layer 1: TDX 板块指数（优先）===
    tdx_sectors = _tdx_fetch_sector_ranking()
    if tdx_sectors and len(tdx_sectors) >= 10:
        sectors_sorted = tdx_sectors
        source_label = "pytdx (通达信板块指数, {}行业)".format(len(tdx_sectors))

        # TDX 数据不含资金流，按涨跌幅排序：in=最强Top10，out=最弱（负涨幅）Top5
        in_data = sectors_sorted[:10]
        out_data = [s for s in reversed(sectors_sorted) if s.get("pct_chg", 0) < 0][:5]
        total_net = round(sum(s.get("main_net_yi", 0) for s in in_data), 2)

        # 构建5日历史（复用快照系统）
        snaps = _load_sector_flow_snaps()
        history = []
        for d in sorted(snaps.keys()):
            s_list = snaps[d]
            d_total = sum(x.get("main_net_yi", 0) for x in s_list[:10])
            history.append({"date": d, "total_net_yi": round(d_total, 2), "top3": [x["name"] for x in s_list[:3]]})

        out = {
            "success": True,
            "data": in_data,
            "out_data": out_data,
            "total_net_yi": total_net,
            "history": history[-5:],
            "is_stale": False,
            "fallback_mode": None,
            "source": source_label,
        }
        # TDX 路径也保存快照（收盘后）
        if _market_closed_today():
            snap_date = trading_days(1)[-1]
            _save_sector_flow_snap(snap_date, sectors_sorted)
    else:
        # === Layer 2: Eastmoney push2delay（回退）===
        source = "eastmoney sector flow"

        try:
            # 拉取足够多的板块数据（pz=30），后续分别提取流入Top10和流出Top5
            # po=1 降序（流入最多排前）；pz=30 确保覆盖大多数行业板块
            url = (
                P2D + "/clist/get?pn=1&pz=30&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281"
                "&fltt=2&invt=2&fid=f62&fs=m:90+t:2"
                "&fields=f2,f3,f4,f12,f14,f62,f66,f104,f105,f128,f140,f141,f136,f152,f184"
            )
            raw = http_get(url, timeout=12)
            data = json.loads(raw)
            diffs = data.get("data", {}).get("diff", [])

            sectors = []
            f62_stale_count = 0
            for item in diffs:
                f62_val = item.get("f62")
                if f62_val is None or f62_val == "-" or f62_val == "":
                    f62_stale_count += 1
                net_in = safe_float(f62_val, 0)
                net_in_yi = round(net_in / 100000000, 2)
                sectors.append({
                    "code": item.get("f12", ""),
                    "name": item.get("f14", ""),
                    "pct_chg": safe_float(item.get("f3"), 0),
                    "main_net_yi": net_in_yi,
                    "super_large_yi": round(safe_float(item.get("f64", 0)) / 100000000, 2),
                    "large_yi": round(safe_float(item.get("f66", 0)) / 100000000, 2),
                })

            # 检测 f62 空值比例：超过50%板块的f62为空 → 资金流数据不可用
            if sectors and f62_stale_count > len(sectors) // 2:
                is_stale = True
                # 优先回退昨日快照
                snaps = _load_sector_flow_snaps()
                if snaps:
                    snap_dates = sorted(snaps.keys(), reverse=True)
                    yesterday_sectors = snaps.get(snap_dates[0], [])
                    if yesterday_sectors:
                        sectors = yesterday_sectors
                        fallback_mode = "snap"
                        source = "昨日快照回退 (P2D f62 为空)"
                # 快照也无 → 按涨幅降级
                if not fallback_mode:
                    sectors.sort(key=lambda s: abs(s["pct_chg"]), reverse=True)
                    fallback_mode = "pct_chg"
                    source = "按涨跌幅降级 (P2D f62 为空)"

            # 按 main_net_yi 排序：流入 Top10（降序）和流出 Top5（升序，最负的排前）
            sectors_sorted = sorted(sectors, key=lambda s: s.get("main_net_yi", 0), reverse=True)
            in_data = sectors_sorted[:10]      # 流入最多的前10
            out_data = []
            for s in reversed(sectors_sorted): # 从最流出开始
                if s.get("main_net_yi", 0) < 0:
                    out_data.append(s)
                if len(out_data) >= 5:
                    break

            # 只在有有效资金流数据时才保存快照（保存完整排序后数据，便于历史回放）
            if sectors_sorted and not is_stale and _market_closed_today():
                snap_date = trading_days(1)[-1]
                _save_sector_flow_snap(snap_date, sectors_sorted)

            total_net = round(sum(s.get("main_net_yi", 0) for s in in_data), 2)

            # 构建5日历史
            snaps = _load_sector_flow_snaps()
            history = []
            for d in sorted(snaps.keys()):
                s_list = snaps[d]
                d_total = sum(x.get("main_net_yi", 0) for x in s_list[:10])
                history.append({"date": d, "total_net_yi": round(d_total, 2), "top3": [x["name"] for x in s_list[:3]]})

            out = {
                "success": True,
                "data": in_data,           # 流入 Top10（向后兼容）
                "out_data": out_data,      # 流出 Top5（新增）
                "total_net_yi": total_net,
                "history": history[-5:],
                "is_stale": is_stale,
                "fallback_mode": fallback_mode,
                "source": source,
            }
        except Exception as e:
            # API 完全失败：尝试回退昨日快照
            snaps = _load_sector_flow_snaps()
            if snaps:
                snap_dates = sorted(snaps.keys(), reverse=True)
                yesterday_sectors = snaps.get(snap_dates[0], [])
                if yesterday_sectors:
                    hist = []
                    for d in sorted(snaps.keys()):
                        s_list = snaps[d]
                        d_total = sum(x.get("main_net_yi", 0) for x in s_list[:10])
                        hist.append({"date": d, "total_net_yi": round(d_total, 2), "top3": [x["name"] for x in s_list[:3]]})
                    # 从快照数据里也提取流出 Top5
                    snap_sorted = sorted(yesterday_sectors, key=lambda s: s.get("main_net_yi", 0), reverse=True)
                    snap_out = [s for s in reversed(snap_sorted) if s.get("main_net_yi", 0) < 0][:5]
                    out = {
                        "success": True,
                        "data": snap_sorted[:10],
                        "out_data": snap_out,
                        "total_net_yi": round(sum(x.get("main_net_yi", 0) for x in snap_sorted[:10]), 2),
                        "history": hist[-5:],
                        "is_stale": True,
                        "fallback_mode": "snap",
                        "source": "昨日快照回退 (API异常: {})".format(str(e)[:50]),
                    }
                else:
                    out = {"success": False, "error": str(e), "data": [], "out_data": [], "history": []}
            else:
                out = {"success": False, "error": str(e), "data": [], "out_data": [], "history": []}

    # 过期数据缩短缓存（60s），正常数据常规缓存（600s）
    if out.get("is_stale"):
        path = os.path.join(CACHE_DIR, "sector_flow.json")
        try:
            with open(path, "w") as fh:
                json.dump(out, fh, ensure_ascii=False)
        except:
            pass
    else:
        save_cache("sector_flow", out)
    return out


# ===== M9: Market Breadth (市场宽度) =====
def m9_market_breadth():
    """市场宽度综合指标：涨跌比/涨停/跌停/新高新低/量比
    数据来源: 复用 ADR/Limits 模块聚合 + 主力资金流
    """
    cached = load_cache("breadth")
    if cached:
        return cached

    try:
        # 1. 从 ADR 获取涨跌家数 + 涨跌停数
        adr = m4_adr()
        limits = m5_limits()
        adr_data = adr.get("data", {}) if adr.get("success") else {}
        lim_data = limits.get("data", {}) if limits.get("success") else {}

        up = int(adr_data.get("up", 0))
        down = int(adr_data.get("down", 0))
        flat = int(adr_data.get("flat", 0))
        lu = int(lim_data.get("limit_up", 0))
        ld = int(lim_data.get("limit_down", 0))

        up_ratio = round(up / max(down, 1), 2)
        breadth_pct = round(up / max(up + down, 1) * 100, 1)
        total_stocks = up + down + flat

        # 2. 从主力资金流获取全市场主力净流入（Eastmoney fflow — 唯一可靠的主力/散户分类数据源）
        cf = m3_capital_flow()
        cf_data = cf.get("data", [])
        main_net_today = 0
        main_net_5d = []
        for item in cf_data:
            mn = item.get("main_net_yi", 0)
            main_net_5d.append(mn)
            if item.get("date", "") == cf_data[-1].get("date", "") if cf_data else "":
                main_net_today = mn
        if not main_net_today and main_net_5d:
            main_net_today = main_net_5d[-1]

        # 2b. TDX 总成交额补充（M1 数据，更准确）
        sh_vol = m1_sh_volume()
        vol_data = sh_vol.get("data", []) if sh_vol.get("success") else []
        total_turnover_yi = 0
        if vol_data:
            # M1 data 格式: [{"date": "2026-06-06", "amount_yi": 14500.5, "volume_yi": 920.3}, ...]
            today_vol = vol_data[-1] if vol_data else {}
            total_turnover_yi = today_vol.get("amount_yi", 0) or today_vol.get("amount", 0)

        # 3. 计算综合breadth评分 (0-100)
        score = 50.0
        # 涨跌比贡献
        if up_ratio >= 3: score += 25
        elif up_ratio >= 2: score += 15
        elif up_ratio >= 1.2: score += 5
        elif up_ratio >= 0.8: score += 0
        elif up_ratio >= 0.5: score -= 5
        elif up_ratio >= 0.3: score -= 15
        else: score -= 25
        # 涨停家数贡献
        if lu >= 100: score += 15
        elif lu >= 50: score += 8
        elif lu >= 30: score += 2
        if ld >= 100: score -= 15
        elif ld >= 50: score -= 8
        elif ld >= 30: score -= 2
        # 主力资金贡献
        if main_net_today > 100: score += 10
        elif main_net_today > 30: score += 5
        elif main_net_today < -100: score -= 10
        elif main_net_today < -30: score -= 5

        score = max(0, min(100, round(score, 1)))

        if score >= 75: status, label = "extreme_bullish", "极度乐观"
        elif score >= 60: status, label = "bullish", "偏乐观"
        elif score >= 45: status, label = "neutral", "中性"
        elif score >= 30: status, label = "bearish", "偏悲观"
        else: status, label = "extreme_bearish", "极度悲观"

        # 构建5日历史
        snaps = _load_adr_limits_snaps()
        history = []
        for d in sorted(snaps.keys())[-5:]:
            s = snaps[d]
            h_up = int(s.get("up", 0))
            h_down = int(s.get("down", 0))
            h_pct = round(h_up / max(h_up + h_down, 1) * 100, 1)
            history.append({"date": d, "up_pct": h_pct, "up": h_up, "down": h_down, "lu": s.get("lu", 0), "ld": s.get("ld", 0)})

        out = {
            "success": True,
            "data": {
                "score": score, "status": status, "label": label,
                "breadth_pct": breadth_pct, "up_ratio": up_ratio,
                "up": up, "down": down, "flat": flat,
                "limit_up": lu, "limit_down": ld,
                "main_net_yi": round(main_net_today, 2),
                "total_turnover_yi": round(total_turnover_yi, 2),
                "total_stocks": total_stocks,
            },
            "history": history,
            "source": "composite (ADR/Limits→Eastmoney clist+BSE, CapitalFlow→Eastmoney fflow, Turnover→pytdx)",
        }
    except Exception as e:
        out = {"success": False, "error": str(e), "data": {}, "history": []}

    save_cache("breadth", out)
    return out


# ===== M10: South-bound Capital Flow (港股通南向资金) =====
def m10_southbound():
    """港股通南向资金近5日净买入额。
    数据来源：东方财富 kamt API klt=105 (港股通)
    - sh2hk = 港股通(沪), sz2hk = 港股通(深)
    - dayNetAmtIn 单位为万元
    """
    cached = load_cache("southbound")
    if cached:
        return cached

    SNAP_FILE = os.path.join(CACHE_DIR, "southbound_snaps.json")

    def _load_sb_snaps():
        if os.path.exists(SNAP_FILE):
            try:
                with open(SNAP_FILE) as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_sb_snap(date_str, sh_net, sz_net, total):
        snaps = _load_sb_snaps()
        print("[M10 SB] _save_sb_snap: date={}, sh_net={}, sz_net={}, total={}".format(
            date_str, sh_net, sz_net, total), flush=True)
        print("[M10 SB] _save_sb_snap: existing keys={}".format(list(snaps.keys())), flush=True)
        snaps[date_str] = {
            "date": date_str,
            "sh_net_yi": sh_net, "sz_net_yi": sz_net, "total_yi": total,
            # 兼容前端读取 total 字段
            "sh_net": sh_net, "sz_net": sz_net, "total": total,
        }
        keys = sorted(snaps.keys(), reverse=True)
        snaps = {k: snaps[k] for k in keys[:10]}
        print("[M10 SB] _save_sb_snap: saving keys={}".format(list(snaps.keys())), flush=True)
        try:
            with open(SNAP_FILE, "w") as f:
                json.dump(snaps, f, ensure_ascii=False)
            print("[M10 SB] _save_sb_snap: file saved successfully", flush=True)
        except Exception as e:
            print("[M10 SB] _save_sb_snap: ERROR saving file: {}".format(e), flush=True)

    out = None
    try:
        # fields2 需包含 f59-f64 以获取 buyAmt/sellAmt/netBuyAmt 真实净买入字段
        # 注意：dayNetAmtIn 字段含义是"今日港股通买入规模/额度"，不是净买入
        #       netBuyAmt = buyAmt - sellAmt，才是真正的净买入(买卖差)
        url = ("https://push2delay.eastmoney.com/api/qt/kamt/get?"
               "fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64"
               "&klt=101&lmt=1&ut=b2884a393a59ad64002292a3e90d46a5")
        raw = http_get(url, timeout=12)
        data = json.loads(raw)
        d = data.get("data", {})
        sh_south = d.get("sh2hk", {})
        sz_south = d.get("sz2hk", {})
        # 用 netBuyAmt (买-卖差) 作为真实净买入，单位万元
        sh_net_wan = float(sh_south.get("netBuyAmt", 0) or 0)
        sz_net_wan = float(sz_south.get("netBuyAmt", 0) or 0)
        sh_net = round(sh_net_wan / 10000, 2)
        sz_net = round(sz_net_wan / 10000, 2)
        total = round(sh_net + sz_net, 2)
        print("[M10 SB] netBuyAmt: sh={:.2f}亿, sz={:.2f}亿, total={:.2f}亿".format(
            sh_net, sz_net, total), flush=True)

        snap_date = (sh_south.get("date2") or sz_south.get("date2")
                     or trading_days(1)[-1])
        print("[M10 SB] snap_date={}, sh_south.date2={}, sz_south.date2={}".format(
            snap_date, sh_south.get("date2"), sz_south.get("date2")), flush=True)

        # 智能判断是否有真实数据：buyAmt+sellAmt>0 说明市场有交易活动
        sh_buy_sell = abs(float(sh_south.get("buyAmt", 0) or 0)) + abs(float(sh_south.get("sellAmt", 0) or 0))
        sz_buy_sell = abs(float(sz_south.get("buyAmt", 0) or 0)) + abs(float(sz_south.get("sellAmt", 0) or 0))
        is_pre_market = (sh_buy_sell == 0 and sz_buy_sell == 0 and abs(sh_net_wan) == 0 and abs(sz_net_wan) == 0)
        print("[M10 SB] buySell: sh={:.0f}, sz={:.0f}, is_pre_market={}".format(
            sh_buy_sell, sz_buy_sell, is_pre_market), flush=True)

        if is_pre_market:
            # 盘前数据全零 → 从快照取前一天真实数据用于展示
            snaps_existing = _load_sb_snaps()
            if snaps_existing:
                latest_snap = sorted(snaps_existing.values(), key=lambda x: x.get("date", ""))[-1]
                sh_net = latest_snap.get("sh_net_yi", 0)
                sz_net = latest_snap.get("sz_net_yi", 0)
                total = latest_snap.get("total_yi", 0)
                print("[M10 SB] Pre-market, using snapshot from {}: total={:.2f}亿".format(
                    latest_snap.get("date"), total), flush=True)
        else:
            _save_sb_snap(snap_date, sh_net, sz_net, total)
            print("[M10 SB] Snapshot saved for date={}".format(snap_date), flush=True)

        snaps = _load_sb_snaps()
        history = sorted(snaps.values(), key=lambda x: x.get("date", ""))[-5:]
        # 注意：_backfill_nbsb_from_kline 已废弃（push2his 无真实净买入历史）
        # 历史数据将通过每日调用自动积累
        if len(history) < 5:
            print("[M10 SB] Warning: only {} days in history, need {} more days to reach 5".format(
                len(history), 5 - len(history)), flush=True)

        if total >= 30:       sb_status = "大幅南下"
        elif total >= 10:     sb_status = "净流入港股"
        elif total >= 0:      sb_status = "小幅南下"
        elif total >= -10:    sb_status = "小幅回流"
        elif total >= -30:    sb_status = "净回流A股"
        else:                 sb_status = "大幅回流"

        out = {
            "success": True,
            "data": {
                "total_yi": total, "sh_net_yi": sh_net, "sz_net_yi": sz_net, "status": sb_status,
            },
            "history": history,
            "source": "eastmoney kamt (港股通)",
        }
    except Exception as e:
        try:
            snaps = _load_sb_snaps()
            print("[M10 SB] Fallback: snaps keys={}, count={}".format(
                list(snaps.keys()), len(snaps)), flush=True)
            history = sorted(snaps.values(), key=lambda x: x.get("date", ""))[-5:]
            if len(history) < 5:
                _backfill_nbsb_from_kline(SNAP_FILE, None, is_northbound=False, min_days=5)
                snaps = _load_sb_snaps()
                history = sorted(snaps.values(), key=lambda x: x.get("date", ""))[-5:]
            print("[M10 SB] Fallback history len={}".format(len(history)), flush=True)
            if history:
                last = history[-1]
                t = last["total_yi"]
                if t >= 30: sb_status = "大幅南下"
                elif t >= 10: sb_status = "净流入港股"
                elif t >= 0: sb_status = "小幅南下"
                elif t >= -10: sb_status = "小幅回流"
                elif t >= -30: sb_status = "净回流A股"
                else: sb_status = "大幅回流"
                out = {
                    "success": True,
                    "data": {"total_yi": last["total_yi"], "sh_net_yi": last["sh_net_yi"],
                             "sz_net_yi": last["sz_net_yi"], "status": sb_status},
                    "history": history, "source": "southbound snapshot (API fallback)",
                }
        except Exception:
            pass

    if out is None:
        out = {"success": False, "error": "南向资金数据获取失败", "data": {}, "history": []}

    save_cache("southbound", out)
    return out


# ===== M11: Multi-Timeframe Resonance (多周期技术信号共振) =====
def m11_tf_resonance():
    """日线+周线+月线 KDJ/MACD/RSI 共振评级
    数据来源: TDX K-line data
    输出: 五档评级 (strong_bullish/bullish/neutral/bearish/strong_bearish)
    """
    cached = load_cache("tf_resonance")
    if cached:
        return cached

    out = None
    try:
        if not _TDX_AVAILABLE:
            raise Exception("pytdx not available")

        # 获取不同周期K线
        daily = _tdx_get_index_kline('000001', count=60)   # 日线60天
        weekly = _tdx_get_weekly_kline('000001', count=30)  # 周线30周
        monthly = _tdx_get_monthly_kline('000001', count=24) # 月线24月

        if not daily or len(daily) < 30:
            raise Exception("insufficient daily data")

        # 计算各周期信号
        def _calc_signals(klines, name=""):
            """对K线序列计算 KDJ/MACD/RSI 信号"""
            closes = [k["close"] for k in klines]
            highs = [k["high"] for k in klines]
            lows = [k["low"] for k in klines]
            n = len(closes)
            if n < 14:
                return {"kdj": 0, "macd": 0, "rsi": 0, "score": 0}

            # RSI(14)
            gains, losses = [], []
            for i in range(1, n):
                chg = closes[i] - closes[i-1]
                gains.append(max(chg, 0))
                losses.append(max(-chg, 0))
            avg_gain = sum(gains[-14:]) / 14
            avg_loss = sum(losses[-14:]) / 14
            rs = avg_gain / avg_loss if avg_loss > 0 else 100
            rsi = round(100 - 100 / (1 + rs), 1)
            rsi_sig = 1 if rsi > 60 else (-1 if rsi < 40 else 0)

            # MACD(12,26,9)
            def _ema(data, period):
                k = 2 / (period + 1)
                ema = [data[0]]
                for i in range(1, len(data)):
                    ema.append(data[i] * k + ema[-1] * (1 - k))
                return ema
            ema12 = _ema(closes, 12)
            ema26 = _ema(closes, 26)
            dif = [ema12[i] - ema26[i] for i in range(len(closes))]
            dea = _ema(dif, 9)
            macd = [(dif[i] - dea[i]) * 2 for i in range(len(closes))]
            macd_sig = 1 if macd[-1] > 0 and macd[-1] > macd[-2] else (-1 if macd[-1] < 0 and macd[-1] < macd[-2] else 0)

            # KDJ(9,3,3)
            low9 = min(lows[-9:])
            high9 = max(highs[-9:])
            rsv = (closes[-1] - low9) / (high9 - low9) * 100 if high9 > low9 else 50
            k = rsv * 1/3 + 50 * 2/3  # 简化K值
            d = k * 1/3 + 50 * 2/3    # 简化D值
            kdj_sig = 1 if k > 60 else (-1 if k < 40 else 0)

            score = rsi_sig + macd_sig + kdj_sig  # -3 to +3
            return {
                "rsi": rsi, "rsi_sig": rsi_sig,
                "macd": round(macd[-1], 4), "macd_sig": macd_sig,
                "k": round(k, 1), "d": round(d, 1), "kdj_sig": kdj_sig,
                "score": score, "close": closes[-1],
            }

        d_sig = _calc_signals(daily, "daily")
        w_sig = _calc_signals(weekly, "weekly") if weekly else {"score": 0}
        m_sig = _calc_signals(monthly, "monthly") if monthly else {"score": 0}

        # 加权共振: 日线50% + 周线30% + 月线20%
        resonance = round(d_sig["score"] * 0.5 + w_sig["score"] * 0.3 + m_sig["score"] * 0.2, 2)

        if resonance >= 2.0: level, label = "strong_bullish", "强多 ★★★"
        elif resonance >= 1.0: level, label = "bullish", "偏多 ★★"
        elif resonance >= -1.0: level, label = "neutral", "震荡 ★"
        elif resonance >= -2.0: level, label = "bearish", "偏空 ▼▼"
        else: level, label = "strong_bearish", "强空 ▼▼▼"

        out = {
            "success": True,
            "data": {
                "resonance": resonance, "level": level, "label": label,
                "daily": d_sig,
                "weekly": w_sig if weekly else None,
                "monthly": m_sig if monthly else None,
            },
            "source": "pytdx (TDX K-line)",
        }
    except Exception as e:
        out = {"success": False, "error": str(e), "data": {}}

    save_cache("tf_resonance", out)
    return out


def _tdx_get_weekly_kline(index_code='000001', count=30):
    """获取周K线数据"""
    if not _TDX_AVAILABLE:
        return None
    market = 1 if index_code == '000001' else 0
    for ip, port in _TDX_SERVERS:
        api = None
        try:
            api = TdxHq_API()
            if not api.connect(ip, port, time_out=5):
                continue
            raw = api.get_index_bars(5, market, index_code, 0, count)  # category=5 → weekly
            if not raw or len(raw) == 0:
                api.disconnect()
                continue
            result = []
            for item in raw:
                close = float(item.get('close', 0))
                result.append({
                    'date': str(item.get('datetime', ''))[:10],
                    'open': float(item.get('open', 0)),
                    'close': close,
                    'high': float(item.get('high', 0)),
                    'low': float(item.get('low', 0)),
                })
            api.disconnect()
            if result:
                return result
        except Exception:
            try:
                if api: api.disconnect()
            except:
                pass
    return None


def _tdx_get_monthly_kline(index_code='000001', count=24):
    """获取月K线数据"""
    if not _TDX_AVAILABLE:
        return None
    market = 1 if index_code == '000001' else 0
    for ip, port in _TDX_SERVERS:
        api = None
        try:
            api = TdxHq_API()
            if not api.connect(ip, port, time_out=5):
                continue
            raw = api.get_index_bars(6, market, index_code, 0, count)  # category=6 → monthly
            if not raw or len(raw) == 0:
                api.disconnect()
                continue
            result = []
            for item in raw:
                close = float(item.get('close', 0))
                result.append({
                    'date': str(item.get('datetime', ''))[:10],
                    'open': float(item.get('open', 0)),
                    'close': close,
                    'high': float(item.get('high', 0)),
                    'low': float(item.get('low', 0)),
                })
            api.disconnect()
            if result:
                return result
        except Exception:
            try:
                if api: api.disconnect()
            except:
                pass
    return None


# ===== M12: ETF Valuation Thermometer (ETF 52周温度计) =====
# ETF列表及其对应指数代码（用于K线查询）
_ETF_VAL_CONFIG = [
    {"code": "510050", "name": "上证50",   "index": "000016", "market": 1},
    {"code": "510300", "name": "沪深300",  "index": "000300", "market": 1},
    {"code": "510500", "name": "中证500",  "index": "000905", "market": 1},
    {"code": "159915", "name": "创业板",   "index": "399006", "market": 0},
    {"code": "588000", "name": "科创50",   "index": "000688", "market": 1},
]

def m12_etf_valuation():
    """5只核心ETF的52周温度计
    方法：腾讯 qt.gtimg.cn HTTP接口获取实时价格 + TDX get_security_bars 获取ETF日线计算52周高低
    当前价格在52周高低区间内的百分比位置：0%=冰点, 100%=沸点
    """
    cached = load_cache("etf_val")
    if cached:
        return cached

    result = []
    overall_temp = 0.0
    valid_count = 0

    # 1. 获取ETF实时价格 —— 腾讯 qt.gtimg.cn 主用，逐只fallback防限流
    # 策略：批量请求优先；若部分ETF缺失，逐只请求（间隔1秒，降低限流概率）
    prices = {}

    # --- 方式1：批量请求 ---
    try:
        tencent_codes = ["sh510050", "sh510300", "sh510500", "sz159915", "sh588000"]
        url = "http://qt.gtimg.cn/q=" + ",".join(tencent_codes)
        raw = http_get(url, timeout=10)
        for line in raw.strip().splitlines():
            if not line.startswith("v_"):
                continue
            m = re.search(r'"(.+)"', line)
            if not m:
                continue
            fields = m.group(1).split("~")
            if len(fields) < 33:
                continue
            code_full = fields[2]
            price = safe_float(fields[3], 0)
            chg_pct = safe_float(fields[32], 0)
            if price > 0:
                prices[code_full] = {"price": price, "chg_pct": chg_pct}
        print("[ETF_Val] Tencent batch: {} ETFs".format(len(prices)), flush=True)
    except Exception as e:
        print("[ETF_Val] Tencent batch error: {}, trying one-by-one...".format(e), flush=True)

    # --- 方式2：逐只请求（降低限流概率）---
    if len(prices) < len(_ETF_VAL_CONFIG):
        fallback_codes = [
            ("510050", "sh510050"),
            ("510300", "sh510300"),
            ("510500", "sh510500"),
            ("159915", "sz159915"),
            ("588000", "sh588000"),
        ]
        for code, tcode in fallback_codes:
            if code in prices:
                continue
            try:
                url2 = "http://qt.gtimg.cn/q=" + tcode
                raw2 = http_get(url2, timeout=8)
                for line in raw2.strip().splitlines():
                    if not line.startswith("v_"):
                        continue
                    m = re.search(r'"(.+)"', line)
                    if not m:
                        continue
                    fields = m.group(1).split("~")
                    if len(fields) < 33:
                        continue
                    price = safe_float(fields[3], 0)
                    chg_pct = safe_float(fields[32], 0)
                    if price > 0:
                        prices[code] = {"price": price, "chg_pct": chg_pct}
                        print("[ETF_Val] Tencent single fallback: {} price={}".format(code, price), flush=True)
                        break
            except Exception as e2:
                print("[ETF_Val] Tencent single error for {}: {}".format(code, e2), flush=True)
            time.sleep(1)  # 逐只间隔1秒，降低限流概率

    print("[ETF_Val] Final prices ({} ETFs): {}".format(len(prices), prices), flush=True)

    # 2. 逐只计算52周温度
    for cfg in _ETF_VAL_CONFIG:
        code = cfg["code"]
        name = cfg["name"]

        try:
            info = prices.get(code, {})
            price = info.get("price", 0)
            chg_pct = info.get("chg_pct", 0)

            wk52_high = 0.0
            wk52_low = 0.0

            # 优先：用TDX获取ETF自身日线K线计算52周高低
            if _TDX_AVAILABLE and price > 0:
                try:
                    etf_klines = _tdx_get_etf_kline(code, count=260)
                    if etf_klines and len(etf_klines) >= 50:
                        closes = [k["close"] for k in etf_klines if k.get("close", 0) > 0]
                        if len(closes) >= 2:
                            wk52_high = round(max(closes), 4)
                            wk52_low = round(min(closes), 4)
                except Exception as e2:
                    print("[ETF_Val] TDX ETF kline error for {}: {}".format(code, e2), flush=True)

            # ETF自身K线无数据 → 用指数K线的百分比映射到ETF价格
            if (wk52_high <= wk52_low or wk52_high <= 0) and _TDX_AVAILABLE and price > 0:
                try:
                    idx_code = cfg["index"]
                    idx_klines = _tdx_get_index_kline(idx_code, count=260)
                    if idx_klines and len(idx_klines) >= 50:
                        idx_closes = [k["close"] for k in idx_klines if k.get("close", 0) > 0]
                        if len(idx_closes) >= 2:
                            idx_hi = max(idx_closes)
                            idx_lo = min(idx_closes)
                            idx_cur = idx_closes[-1]
                            if idx_hi > idx_lo and idx_cur > 0:
                                # 用指数百分比位置映射到ETF价格范围
                                idx_pct = (idx_cur - idx_lo) / (idx_hi - idx_lo)
                                # ETF价格范围 = 当前价 / idx_pct 到 当前价 / idx_pct * (idx_hi/idx_lo)
                                wk52_high = round(price * (idx_hi / idx_cur), 4)
                                wk52_low = round(price * (idx_lo / idx_cur), 4)
                except Exception as e3:
                    print("[ETF_Val] Index mapping error for {}: {}".format(code, e3), flush=True)

            # 最终兜底：用price ±20%估算
            if (wk52_high <= wk52_low or wk52_high <= 0) and price > 0:
                wk52_high = round(price * 1.20, 4)
                wk52_low = round(price * 0.80, 4)

            # 温度计算: 0%=冰点(52周最低), 100%=沸点(52周最高)
            temp_pct = 50.0
            if wk52_high > wk52_low and price > 0 and wk52_high > 0:
                temp_pct = round((price - wk52_low) / (wk52_high - wk52_low) * 100, 1)
                temp_pct = max(0.0, min(100.0, temp_pct))

            if price <= 0:         label, level = "无数据", "unknown"
            elif temp_pct >= 80:   label, level = "沸点", "overheat"
            elif temp_pct >= 65:   label, level = "偏热", "warm"
            elif temp_pct >= 45:   label, level = "适中", "neutral"
            elif temp_pct >= 25:   label, level = "偏冷", "cool"
            else:                  label, level = "冰点", "cold"

            if price > 0:
                overall_temp += temp_pct
                valid_count += 1

            result.append({
                "code": code, "name": name,
                "price": round(price, 4), "chg_pct": round(chg_pct, 2),
                "week52_high": wk52_high, "week52_low": wk52_low,
                "temp_pct": temp_pct, "label": label, "level": level,
            })

        except Exception as e:
            print("[ETF_Val] {} proc error: {}".format(code, e), flush=True)
            result.append({
                "code": code, "name": name,
                "price": 0, "temp_pct": 50.0,
                "label": "错误", "level": "unknown",
            })

    avg_temp = round(overall_temp / max(valid_count, 1), 1)
    if avg_temp >= 70:   mkt_label, mkt_status = "市场偏热", "warm"
    elif avg_temp >= 50: mkt_label, mkt_status = "市场中性", "neutral"
    elif avg_temp >= 30: mkt_label, mkt_status = "市场偏冷", "cool"
    else:                mkt_label, mkt_status = "市场冰冷", "cold"

    out = {
        "success": valid_count > 0,
        "data": result,
        "summary": {
            "avg_temp": avg_temp, "mkt_label": mkt_label,
            "mkt_status": mkt_status, "valid_count": valid_count,
        },
        "source": "Tencent qt.gtimg.cn + TDX get_security_bars",
    }
    save_cache("etf_val", out)
    return out


# ===== M13: Nasdaq Trend (近5日纳斯达克收盘走势) =====
def _backfill_nasdaq_from_yahoo(snap_file, min_days=5):
    """从 Yahoo Finance 回填纳斯达克 K-line 数据（当快照不足时调用）"""
    try:
        url = ("https://query1.finance.yahoo.com/v8/finance/chart/%5EIXIC?"
               "range={}d&interval=1d").format(min_days + 2)
        raw = http_get(url, timeout=12)
        chart = json.loads(raw).get("chart", {}).get("result", [])
        if not chart:
            return
        quotes = chart[0].get("indicators", {}).get("quote", [{}])[0]
        timestamps = chart[0].get("timestamp", [])
        if not timestamps:
            return

        snaps = []
        if os.path.exists(snap_file):
            try:
                with open(snap_file) as f:
                    snaps = json.load(f)
            except Exception:
                pass

        existing_dates = {s.get("date", "") for s in snaps}
        backfilled = 0
        for i, ts in enumerate(timestamps):
            dt = datetime.fromtimestamp(ts)
            date_str = dt.strftime("%Y-%m-%d")
            if date_str in existing_dates:
                continue
            o = safe_float(quotes.get("open", [None])[i], None)
            c = safe_float(quotes.get("close", [None])[i], None)
            h = safe_float(quotes.get("high", [None])[i], None)
            l = safe_float(quotes.get("low", [None])[i], None)
            if not all(v is not None and v > 0 for v in [o, c, h, l]):
                continue
            prev_close = snaps[-1]["close"] if snaps else o
            chg_pct = round((c - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0
            snaps.append({
                "date": date_str, "open": o, "close": c,
                "high": h, "low": l, "prev_close": prev_close, "chg_pct": chg_pct,
            })
            existing_dates.add(date_str)
            backfilled += 1

        if backfilled > 0:
            snaps.sort(key=lambda s: s.get("date", ""))
            snaps = snaps[-10:]
            try:
                with open(snap_file, "w", encoding="utf-8") as f:
                    json.dump(snaps, f, ensure_ascii=False)
                print("[Backfill] Nasdaq {} days from Yahoo Finance".format(backfilled), flush=True)
            except Exception:
                pass
    except Exception as e:
        print("[Backfill] Yahoo Finance Nasdaq fallback failed: {}".format(e), flush=True)


def m13_nasdaq_trend():
    """纳斯达克综合指数（IXIC）近5个交易日走势
    数据获取策略：腾讯 qt.gtimg.cn HTTP接口（us.IXIC）获取当日快照，
    累积到本地文件 nasdaq_snaps.json，保留最近10条不同交易日的记录。
    用于展示隔日美股对A股情绪的参考。
    """
    NASDAQ_SNAPS = os.path.join(CACHE_DIR, "nasdaq_snaps.json")

    # ---- 加载历史快照 ----
    snaps = []
    if os.path.exists(NASDAQ_SNAPS):
        try:
            with open(NASDAQ_SNAPS, "r", encoding="utf-8") as f:
                snaps = json.load(f)
        except Exception:
            snaps = []

    # ---- 快照不足时从 Yahoo Finance 回填 ----
    if len(snaps) < 5:
        _backfill_nasdaq_from_yahoo(NASDAQ_SNAPS, min_days=5)
        if os.path.exists(NASDAQ_SNAPS):
            try:
                with open(NASDAQ_SNAPS, "r", encoding="utf-8") as f:
                    snaps = json.load(f)
            except Exception:
                pass

    # ---- 获取当日最新数据 ----
    today_snap = None
    try:
        # 腾讯 us.IXIC：VPS 验证可用，纯 HTTP 无需 SSL
        url = "http://qt.gtimg.cn/q=us.IXIC"
        raw = http_get(url, timeout=8)
        for line in raw.strip().splitlines():
            if not line.startswith("v_"):
                continue
            m = re.search(r'"(.+)"', line)
            if not m:
                continue
            fields = m.group(1).split("~")
            # 腾讯美股格式字段：0=类型 1=中文名 2=代码 3=当前价 4=开盘 5=前收 6=成交量 ...28=日期时间 29=涨跌额 30=涨跌幅
            if len(fields) < 30:
                continue
            price = safe_float(fields[3], 0)
            prev_close = safe_float(fields[5], 0)   # 昨收
            high = safe_float(fields[33], 0) if len(fields) > 33 else 0  # 今日最高
            low  = safe_float(fields[34], 0) if len(fields) > 34 else 0  # 今日最低
            if price <= 0:
                continue
            # 日期：字段28 格式 "2026-06-04 17:15:59"
            date_raw = fields[28] if len(fields) > 28 else ""
            date_str = date_raw[:10] if len(date_raw) >= 10 else datetime.now().strftime("%Y-%m-%d")
            # 美股通常北京时间次日凌晨收盘，日期字段是美东日期
            chg_pct = round((price - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0
            today_snap = {
                "date": date_str,
                "open": round(safe_float(fields[4], price), 2),
                "close": round(price, 2),
                "high": round(high if high > 0 else price, 2),
                "low":  round(low if low > 0 else price, 2),
                "prev_close": round(prev_close, 2),
                "chg_pct": chg_pct,
            }
            print("[NASDAQ] Today snap: date={} close={} chg={}%".format(
                date_str, price, chg_pct), flush=True)
            break
    except Exception as e:
        print("[NASDAQ] Tencent fetch error: {}".format(e), flush=True)

    # ---- 追加/更新快照（按日期去重，同一天覆盖以获取最新价格）
    # 策略：以 date_str 所在周历判断，若为周末则回退到最近周五
    # （腾讯美股API在周末返回北京时间周末日期，但实际是周五收盘数据）
    if today_snap and today_snap.get("close", 0) > 0:
        snap_date_str = today_snap.get("date", "")
        try:
            _snap_dt = datetime.strptime(snap_date_str, "%Y-%m-%d")
            _snap_weekday = _snap_dt.weekday()
        except Exception:
            _snap_weekday = -1
        # 周末回退到最近周五（美国最后一个交易日）
        if _snap_weekday >= 5:
            from datetime import timedelta
            _original_date = snap_date_str
            _adjusted_dt = _snap_dt - timedelta(days=_snap_weekday - 4)
            snap_date_str = _adjusted_dt.strftime("%Y-%m-%d")
            today_snap["date"] = snap_date_str
            print("[NASDAQ] Weekend adjustment: {} -> {}".format(
                _original_date, snap_date_str), flush=True)
        # 写入快照（工作日数据，已做日期修正）
        snaps = [s for s in snaps if s.get("date") != snap_date_str]
        snaps.append(today_snap)
        snaps.sort(key=lambda s: s.get("date", ""))
        snaps = snaps[-10:]
        try:
            with open(NASDAQ_SNAPS, "w", encoding="utf-8") as f:
                json.dump(snaps, f, ensure_ascii=False)
            print("[NASDAQ] Snapshot saved: date={} (today={})".format(
                snap_date_str, datetime.now().strftime("%Y-%m-%d")), flush=True)
        except Exception as e2:
            print("[NASDAQ] Save snaps error: {}".format(e2), flush=True)

    # ---- 取最近5条 ----
    # 兜底：若 snaps 为空但 today_snap 有效，直接使用 today_snap 作为单条结果
    if not snaps and today_snap and today_snap.get("close", 0) > 0:
        result = [today_snap]
        print("[NASDAQ] Using today_snap as result (snaps empty, date={})".format(
            today_snap.get("date", "")), flush=True)
    else:
        result = snaps[-5:]

    if not result:
        out = {
            "success": False,
            "error": "暂无数据，将在每次页面刷新时自动累积历史记录",
            "data": [],
            "index_name": "纳斯达克综合指数",
        }
        return out

    # ---- 补充相对涨跌幅（每日相对前一日收盘）----
    for i, item in enumerate(result):
        if "chg_pct" not in item:
            if i == 0:
                item["chg_pct"] = 0.0
            else:
                prev = result[i - 1]["close"]
                item["chg_pct"] = round((item["close"] - prev) / prev * 100, 2) if prev > 0 else 0.0

    latest = result[-1]
    chg_latest = latest.get("chg_pct", 0)
    if chg_latest >= 1.5:    trend_label, trend_level = "强劲上涨", "bull_strong"
    elif chg_latest >= 0.3:  trend_label, trend_level = "温和上涨", "bull"
    elif chg_latest >= -0.3: trend_label, trend_level = "横盘震荡", "neutral"
    elif chg_latest >= -1.5: trend_label, trend_level = "温和下跌", "bear"
    else:                    trend_label, trend_level = "明显下跌", "bear_strong"

    # 5日累计涨跌幅
    first_close = result[0].get("close", 0)
    last_close = latest.get("close", 0)
    five_day_chg = round((last_close - first_close) / first_close * 100, 2) if first_close > 0 else 0.0

    out = {
        "success": True,
        "data": result,
        "latest_close": round(last_close, 2),
        "latest_chg_pct": chg_latest,
        "five_day_chg": five_day_chg,
        "trend_label": trend_label,
        "trend_level": trend_level,
        "index_name": "纳斯达克综合指数",
        "data_days": len(result),
        "source": "Tencent qt.gtimg.cn us.IXIC + local snaps",
    }
    # nasdaq_trend 不走普通缓存（快照机制已处理），但设一个短暂缓存避免同秒重复调用
    save_cache("nasdaq_trend", out)
    return out


# ===== M3: Capital Flow 5D (push2delay fflow) =====
def m3_capital_flow():
    cached = load_cache("cap_flow")
    if cached:
        return cached
    try:
        # 注意：fflow kline 必须用 push2his（push2delay 返回空 klines）
        url = ("https://push2his.eastmoney.com/api/qt/stock/fflow/kline/get?secid=1.000001"
               "&fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55,f56,f57"
               "&lmt=10&klt=101")
        data = json.loads(http_get(url, timeout=10))
        klines = data.get("data", {}).get("klines", [])
        result = []
        for line in klines[-5:]:
            p = line.split(",")
            # push2delay fflow: f51=date, f52=tot_net, f53=超大单, f54=大单, f55=中单, f56=小单; 主力=f53+f54, 散户=f55+f56
            # 主力 = 超大单+大单(f53+f54), 散户 = 中单+小单(f55+f56); f52=total net, 单位: 元
            if len(p) >= 6:
                main_net = float(p[2]) + float(p[3])  # f53超大单+f54大单 = 主力净流入
                retail_net = float(p[4]) + float(p[5])  # f55中单 + f56小单 = 散户
                result.append({
                    "date": p[0],
                    "main_net_yi": round(main_net / 100000000, 2),
                    "retail_net_yi": round(retail_net / 100000000, 2),
                })
        out = {"success": len(result) > 0, "data": result, "source": "push2his fflow + pytdx turnover"}

        # === 补充 TDX 两市总成交额（按日期匹配）===
        try:
            tdx_kline = _tdx_get_combined_kline(count=6)
            if tdx_kline:
                tdx_map = {item["date"]: item.get("amount_yi", 0) for item in tdx_kline}
                matched = 0
                for r in result:
                    td = r.get("date", "")
                    if td in tdx_map and tdx_map[td] > 0:
                        r["total_turnover_yi"] = round(tdx_map[td], 2)
                        matched += 1
                if matched > 0:
                    out["tdx_turnover_days"] = matched
        except Exception:
            pass  # TDX 补充失败不阻塞主流程
    except Exception as e:
        out = {"success": False, "error": str(e), "data": []}
    save_cache("cap_flow", out)
    return out


# ===== 持久化导入 =====
try:
    from sentiment_storage import save_sentiment_v2 as _save_v2_to_db
    from sentiment_storage import query_adr_limits_volume_history as _query_score_history
    from sentiment_storage import get_latest_closing_adr_limits as _get_db_closing
    from sentiment_storage import query_capital_flow_history as _query_cf_history
    from sentiment_storage import query_search_heat_history as _query_sh_history
except ImportError:
    _save_v2_to_db = None
    _query_score_history = None
    _get_db_closing = None
    _query_cf_history = None
    _query_sh_history = None


def _enrich_with_history(result):
    """为 M4/M5/M6 模块补充最近5个交易日的历史数据
    优先从快照文件读取 ADR/涨跌停，DB 作为补充兜底
    """
    days5 = trading_days(5)  # 最近5个交易日列表

    # ===== 优先从快照文件构建 ADR/涨跌停历史 =====
    snaps = _load_adr_limits_snaps()
    adr_hist_from_snap = []
    limits_hist_from_snap = []
    for d in days5:
        if d in snaps:
            s = snaps[d]
            up = int(s.get("up", 0))
            down = int(s.get("down", 0))
            flat = int(s.get("flat", 0))
            lu = int(s.get("lu", 0))
            ld = int(s.get("ld", 0))
            if up or down or flat:
                adr_hist_from_snap.append({
                    "date": d, "up": up, "down": down, "flat": flat,
                    "adr": round(up / max(down, 1), 2),
                })
            if lu or ld:
                limits_hist_from_snap.append({
                    "date": d, "limit_up": lu, "limit_down": ld,
                    "ratio": round(lu / max(ld, 1), 1),
                })

    # ===== 从 DB 拉取历史（与快照合并，DB 有效数据优先补充快照缺少的日期）=====
    # 快照文件只在当日市场开放时写入，凌晨cron运行时无快照，
    # 因此改为：snap + DB 合并，以 date 去重，保留最终5天
    db_adr_by_date = {}
    db_limits_by_date = {}
    if _query_score_history:
        try:
            history_rows = _query_score_history(days=7)  # 多取几天防节假日断档
            for r in (history_rows or []):
                td = r.get("trade_date", "")
                if not td:
                    continue
                adr_up = int(r.get("adr_up", 0) or 0)
                adr_down = int(r.get("adr_down", 0) or 0)
                adr_flat = int(r.get("adr_flat", 0) or 0)
                lu = int(r.get("limit_up", 0) or 0)
                ld = int(r.get("limit_down", 0) or 0)
                if adr_up or adr_down or adr_flat:
                    db_adr_by_date[td] = {
                        "date": td, "up": adr_up, "down": adr_down, "flat": adr_flat,
                        "adr": round(adr_up / max(adr_down, 1), 2),
                    }
                if lu or ld:
                    db_limits_by_date[td] = {
                        "date": td, "limit_up": lu, "limit_down": ld,
                        "ratio": round(lu / max(ld, 1), 1),
                    }
        except Exception as e:
            print("[SentimentV2] enrich history DB error: {}".format(e), flush=True)

    # M4: ADR历史 = 快照 + DB 合并，取最近5天（有效数据）
    adr_merged = {}
    for item in adr_hist_from_snap:
        adr_merged[item["date"]] = item
    for date_key, item in db_adr_by_date.items():
        if date_key not in adr_merged:  # 快照优先，DB 仅补缺
            adr_merged[date_key] = item
    # 按日期排序取最近5天
    adr_hist_final = sorted(adr_merged.values(), key=lambda x: x["date"])[-5:]
    result["modules"]["adr"]["history"] = adr_hist_final

    # M5: 涨跌停历史 = 快照 + DB 合并，取最近5天
    limits_merged = {}
    for item in limits_hist_from_snap:
        limits_merged[item["date"]] = item
    for date_key, item in db_limits_by_date.items():
        if date_key not in limits_merged:
            limits_merged[date_key] = item
    limits_hist_final = sorted(limits_merged.values(), key=lambda x: x["date"])[-5:]
    result["modules"]["limits"]["history"] = limits_hist_final

    # M6: 成交额偏离历史（优先使用 K 线 API 实时计算的 history）
    voldev_m = result["modules"].get("volume_dev", {})
    if isinstance(voldev_m, dict) and voldev_m.get("history") and len(voldev_m.get("history", [])) >= 2:
        # K线API已经内置了5日历史，直接用
        pass
    elif _query_score_history:
        try:
            history_rows = _query_score_history(days=5)
            voldev_hist = []
            for r in (history_rows[-5:] if history_rows else []):
                today_amt = float(r.get("volume_today_yi", 0) or 0)
                avg20_amt = float(r.get("volume_avg20_yi", 0) or 0)
                dev_pct = float(r.get("volume_dev_pct", 0) or 0)
                if today_amt > 0:
                    voldev_hist.append({
                        "date": r.get("trade_date", ""),
                        "today_amount": round(today_amt, 2),
                        "avg20_amount": round(avg20_amt, 2),
                        "deviation_pct": round(dev_pct, 2),
                    })
            if "volume_dev" in result["modules"]:
                result["modules"]["volume_dev"]["history"] = voldev_hist
        except Exception as e:
            print("[SentimentV2] enrich voldev DB fallback error: {}".format(e), flush=True)

    # 标记历史数据是否存在
    has_any = (
        bool(result["modules"]["adr"].get("history"))
        or bool(result["modules"]["limits"].get("history"))
        or bool(result["modules"]["volume_dev"].get("history"))
    )
    if has_any:
        result["has_history"] = True

    # ===== M3: capital_flow - 补充 DB 历史（API 返回不足5天时）=====
    cf_mod = result["modules"].get("capital_flow", {})
    cf_data = cf_mod.get("data", [])
    if len(cf_data) < 5 and _query_cf_history:
        try:
            cf_rows = _query_cf_history(days=5)
            # 构建日期索引，API 数据优先（覆盖 DB）
            cf_by_date = {}
            for r in cf_rows:
                d = r.get("trade_date", "")
                if d:
                    cf_by_date[d] = {
                        "date": d,
                        "main_net_yi": float(r.get("main_net_yi", 0) or 0),
                        "retail_net_yi": float(r.get("retail_net_yi", 0) or 0),
                    }
            for item in cf_data:
                d = item.get("date", "")
                if d:
                    cf_by_date[d] = item  # API 数据覆盖 DB
            merged = sorted(cf_by_date.values(), key=lambda x: x.get("date", ""))
            if len(merged) > len(cf_data):
                result["modules"]["capital_flow"]["data"] = merged
                result["modules"]["capital_flow"]["source"] += "+DB"
                # 修复：数据补充后更新 success 标记
                result["modules"]["capital_flow"]["success"] = True
            # TDX 成交额补充（DB 数据不含此字段，合并后需重新补齐）
            try:
                tdx_kline = _tdx_get_combined_kline(count=6)
                if tdx_kline:
                    tdx_map = {item["date"]: item.get("amount_yi", 0) for item in tdx_kline}
                    for r in result["modules"]["capital_flow"]["data"]:
                        td = r.get("date", "")
                        if td in tdx_map and tdx_map[td] > 0:
                            r["total_turnover_yi"] = round(tdx_map[td], 2)
            except Exception:
                pass
        except Exception as e:
            print("[SentimentV2] enrich capital_flow DB error: {}".format(e), flush=True)

    # ===== M2: search_index - 补充 DB 历史（快照不足5天时）=====
    si_mod = result["modules"].get("search_index", {})
    si_data = si_mod.get("data", [])
    if len(si_data) < 5 and _query_sh_history:
        try:
            sh_rows = _query_sh_history(days=5)
            si_by_date = {}
            for r in sh_rows:
                d = r.get("trade_date", "")
                if d:
                    si_by_date[d] = {
                        "date": d,
                        "stock_count": int(r.get("stock_count", 0) or 0),
                        "total_heat": float(r.get("total_heat", 0) or 0),
                        "avg_heat": float(r.get("avg_heat", 0) or 0),
                    }
            for item in si_data:
                d = item.get("date", "")
                if d:
                    si_by_date[d] = item  # 当前数据优先
            merged = sorted(si_by_date.values(), key=lambda x: x.get("date", ""))
            if len(merged) > len(si_data):
                result["modules"]["search_index"]["data"] = merged
                result["modules"]["search_index"]["source"] += "+DB"
        except Exception as e:
            print("[SentimentV2] enrich search_index DB error: {}".format(e), flush=True)


# ===== V2 Aggregation =====
def _filter_modules_to_recent5(result):
    """过滤所有模块：去掉未完成交易日的数据，保留最近5条（按日期排序），矫正 trade_date

    截止日期规则：
    - 15:00 之前（未收盘）：截止日期 = 昨天（最近已完成交易日），今天数据不展示
    - 15:00 之后（已收盘）：截止日期 = 今天（允许当日收盘数据展示）
    """
    if _market_closed_today():
        cutoff = datetime.now().strftime("%Y-%m-%d")  # 已收盘，允许今天
    else:
        # 未收盘，截止到昨天的最后一个有效交易日
        cutoff = trading_days(1)[-1]
    all_dates = []

    for mod_name, mod in result.get("modules", {}).items():
        for key in ("data", "history"):
            lst = mod.get(key)
            if not isinstance(lst, list) or not lst:
                continue
            # 1. 去掉截止日期之后的数据（未收盘时今天数据不显示）
            lst = [x for x in lst if isinstance(x, dict) and x.get("date", "") <= cutoff]
            if not lst:
                mod[key] = []
                continue
            # 2. 按日期排序，保留最近5条
            lst.sort(key=lambda x: x.get("date", ""))
            lst = lst[-5:]
            mod[key] = lst
            # 收集日期
            for item in lst:
                if item.get("date"):
                    all_dates.append(item["date"])

    # 3. 矫正 trade_date 为数据中最新的日期
    if all_dates:
        all_dates.sort()
        result["trade_date"] = all_dates[-1]


def get_sentiment_v2():
    # 周末/节假日检测：非交易日时标记为 non_trading，前端展示提示
    is_trading = _is_trading_day()
    result = {
        "success": True,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trading_day": is_trading,
        "modules": {
            "sh_volume": m1_sh_volume(),
            "search_index": m2_search_index(),
            "capital_flow": m3_capital_flow(),
            "adr": m4_adr(),
            "limits": m5_limits(),
            "volume_dev": m6_volume_dev(),
            "northbound": m7_northbound(),
            "sector_flow": m8_sector_flow(),
            "breadth": m9_market_breadth(),
            "southbound": m10_southbound(),
            "tf_resonance": m11_tf_resonance(),
            "etf_val": m12_etf_valuation(),
            "nasdaq_trend": m13_nasdaq_trend(),
        }
    }
    ok_count = sum(1 for m in result["modules"].values() if m.get("success"))
    result["all_modules_ok"] = ok_count == len(result["modules"])
    result["ok_count"] = ok_count

    # 推断数据对应的交易日（非系统时间）
    # 优先从资金流数据中取最新日期（fflow API 返回实际交易日）
    cf_data = result["modules"]["capital_flow"].get("data", [])
    if cf_data and cf_data[-1].get("date"):
        result["trade_date"] = cf_data[-1]["date"]
    else:
        # 兜底：从 ADR source 推断
        adr_src = result["modules"]["adr"].get("source", "")
        if "DB" in str(adr_src):
            import re
            m = re.search(r'(\d{4}-\d{2}-\d{2})', str(adr_src))
            if m:
                result["trade_date"] = m.group(1)

    # 统一过滤：过滤掉今天及以后的数据，保留最近5个交易日
    # 不偏移 trade_date：广告和数据直接反映实际的交易日
    _filter_modules_to_recent5(result)

    # 自动持久化到 MySQL
    if _save_v2_to_db:
        try:
            saved = _save_v2_to_db(result)
            result["db_saved"] = saved
        except Exception as e:
            print("[SentimentV2] DB save failed: {}".format(e), flush=True)
            result["db_saved"] = 0

    # 补充 M4/M5/M6 的5日历史数据
    _enrich_with_history(result)
    # 再次过滤：确保 history 也只保留今天前的数据
    _filter_modules_to_recent5(result)

    return result
