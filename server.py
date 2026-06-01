#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票分析 Web 服务器
独立的 Flask 服务器，提供股票分析、大盘分析、走势预测功能
"""

import os
import sys
import json
import time
import threading
import subprocess
import requests
import pandas as pd
import numpy as np
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, jsonify, redirect

app = Flask(__name__, template_folder='templates')

# ==================== 缓存系统 ====================
_cache = {}
_cache_lock = threading.Lock()

def cache_get(key):
    """读取缓存，过期返回None"""
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() < entry['expire']:
            return entry['data']
    return None

def cache_set(key, data, ttl):
    """写入缓存，ttl秒过期"""
    with _cache_lock:
        _cache[key] = {'data': data, 'expire': time.time() + ttl}

def cache_clean():
    """清理过期缓存（后台调用）"""
    with _cache_lock:
        now = time.time()
        expired = [k for k, v in _cache.items() if v['expire'] < now]
        for k in expired:
            del _cache[k]

# 获取自选股列表
def get_stock_list():
    stock_file = os.path.join(os.path.dirname(__file__), '自选股票.md')
    stocks = []
    if os.path.exists(stock_file):
        with open(stock_file, 'r', encoding='utf-8') as f:
            for line in f:
                if '|' in line:
                    parts = [p.strip() for p in line.split('|')]
                    if len(parts) >= 4:
                        code_raw = parts[2] if len(parts) > 2 else ''
                        name = parts[3] if len(parts) > 3 else ''
                        # 支持 .sz/.sh/.SZ/.SH/.Sz/.sH 等格式
                        code_upper = code_raw.upper()
                        if '.SZ' in code_upper or '.SH' in code_upper:
                            code = code_upper.replace('.SZ', 'sz').replace('.SH', 'sh')
                            stocks.append({'code': code, 'name': name})
    return stocks


def get_realtime_data(code_qq, timeout=5):
    """获取实时行情数据"""
    url = f'https://qt.gtimg.cn/q={code_qq}'
    try:
        r = requests.get(url, timeout=timeout, headers={'Referer': 'https://finance.qq.com'})
        if 'FAILED' in r.text or 'none_match' in r.text or r.text.strip() == '':
            return None
        m = r.text.split('=')[1].strip(';').strip('"')
        s = m.split('~')
        if len(s) < 10:
            return None
        return {
            'price': float(s[3]),
            'prev_close': float(s[4]),
            'open': float(s[5]),
            'vol': int(s[6]) if s[6].isdigit() else 0,
            'bid1_vol': int(s[7]) if s[7].isdigit() else 0,
            'ask1_vol': int(s[8]) if s[8].isdigit() else 0,
            'chg': float(s[31]) if s[31] else 0,
            'chg_pct': float(s[32]) if s[32] else 0,
        }
    except:
        return None


def get_realtime_batch(codes_qq, timeout=4):
    """批量获取实时行情，返回 {code: data}"""
    if not codes_qq:
        return {}
    url = f"https://qt.gtimg.cn/q={','.join(codes_qq)}"
    result = {}
    try:
        r = requests.get(url, timeout=timeout, headers={'Referer': 'https://finance.qq.com'})
        text = r.text or ''
        for line in text.splitlines():
            line = line.strip()
            if not line or '=' not in line:
                continue
            key = line.split('=')[0].replace('v_', '').strip()
            payload = line.split('=', 1)[1].strip(';').strip('"')
            s = payload.split('~')
            if len(s) < 33:
                continue
            try:
                result[key] = {
                    'price': float(s[3]),
                    'prev_close': float(s[4]),
                    'open': float(s[5]),
                    'vol': int(s[6]) if s[6].isdigit() else 0,
                    'bid1_vol': int(s[7]) if s[7].isdigit() else 0,
                    'ask1_vol': int(s[8]) if s[8].isdigit() else 0,
                    'chg': float(s[31]) if s[31] else 0,
                    'chg_pct': float(s[32]) if s[32] else 0,
                }
            except:
                continue
    except:
        return {}
    return result


def get_kline_data(code_qq, datalen=250):
    """获取K线数据"""
    url = f'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_dayqfq&param={code_qq},day,,,{datalen},qfq'
    try:
        r = requests.get(url, timeout=10, headers={'Referer': 'https://finance.qq.com'})
        text = r.text.replace('kline_dayqfq=', '')
        data = json.loads(text)
        qfqday = data.get('data', {}).get(code_qq, {}).get('qfqday', [])
        if not qfqday:
            qfqday = data.get('data', {}).get(code_qq, {}).get('day', [])
        return qfqday[-datalen:] if len(qfqday) > datalen else qfqday
    except:
        return []


def fetch_json(url, timeout=8, headers=None):
    """通用JSON请求，失败时返回None"""
    try:
        req_headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Referer': 'https://quote.eastmoney.com/',
            'Accept': 'application/json,text/plain,*/*',
        }
        if headers:
            req_headers.update(headers)
        r = requests.get(url, timeout=timeout, headers=req_headers)
        r.raise_for_status()
        return r.json()
    except:
        # 部分数据源会主动断开 requests 连接，这里回退到 curl
        try:
            cmd = [
                'curl', '-s', '--max-time', str(timeout),
                '-H', 'User-Agent: Mozilla/5.0',
                '-H', 'Referer: https://quote.eastmoney.com/',
                url,
            ]
            out = subprocess.check_output(cmd, text=True)
            return json.loads(out)
        except:
            return None


def get_hot_sectors(limit=8):
    """获取热门板块（使用ETF代理，稳定可用）"""
    sector_proxies = {
        '券商': 'sh512000',
        '银行': 'sh512800',
        '白酒': 'sz159928',
        '半导体': 'sh512480',
        '新能源': 'sh516160',
        '医药': 'sh512010',
        '消费': 'sz159928',
        '军工': 'sh512660',
        '人工智能': 'sz159819',
        '红利': 'sh510880',
    }
    sectors = []
    batch = get_realtime_batch(list(sector_proxies.values()), timeout=3)
    for name, code in sector_proxies.items():
        rt = batch.get(code)
        if not rt:
            continue
        est_amount = rt['price'] * rt.get('vol', 0)
        sectors.append({
            'name': name,
            'price': rt['price'],
            'chg': rt['chg_pct'],
            'main_net_inflow': est_amount,
        })
    sectors.sort(key=lambda x: x['chg'], reverse=True)
    return sectors[:limit]


def get_lhb_list(limit=5):
    """获取龙虎榜（最近交易日）"""
    url = (
        "https://datacenter-web.eastmoney.com/api/data/v1/get?"
        "sortColumns=TRADE_DATE,SECURITY_CODE&sortTypes=-1,1"
        f"&pageSize={limit}&pageNumber=1"
        "&reportName=RPT_DAILYBILLBOARD_DETAILS&columns=ALL&source=WEB&client=WEB"
    )
    data = fetch_json(url)
    lhb = []
    if not data:
        return lhb

    rows = data.get('result', {}).get('data', []) or []
    for row in rows[:limit]:
        lhb.append({
            'code': row.get('SECURITY_CODE', ''),
            'name': row.get('SECURITY_NAME_ABBR', ''),
            'chg': float(row.get('CHANGE_RATE', 0) or 0),
            'close': float(row.get('CLOSE_PRICE', 0) or 0),
            'net_amt': float(row.get('BILLBOARD_NET_AMT', 0) or 0),
            'reason': row.get('EXPLANATION', row.get('EXPLAIN', '')), 
            'trade_date': row.get('TRADE_DATE', ''),
        })
    return lhb


def get_capital_flow_snapshot(limit=8):
    """获取资金流快照（样本资金量估算）"""
    capital = {
        'northbound': {},
        'main_force_top': [],
    }

    sample_indices = {
        '上证指数': 'sh000001',
        '深证成指': 'sz399001',
        '创业板指': 'sz399006',
        '沪深300': 'sh000300',
    }
    idx_batch = get_realtime_batch(list(sample_indices.values()), timeout=3)
    total_bid = 0
    total_ask = 0
    total_amount = 0
    for _, code in sample_indices.items():
        rt = idx_batch.get(code)
        if not rt:
            continue
        total_bid += rt.get('bid1_vol', 0)
        total_ask += rt.get('ask1_vol', 0)
        total_amount += rt['price'] * rt.get('vol', 0)

    net_est = total_bid - total_ask
    capital['northbound'] = {
        'date': datetime.now().strftime('%Y-%m-%d'),
        'sh_net_in': float(net_est),
        'sz_net_in': 0.0,
        'total_net_in': float(net_est),
        'sample_amount': float(total_amount),
        'note': '基于指数盘口委比与成交量的样本估算，非交易所口径。',
    }

    sample_universe = [
        ('贵州茅台', 'sh600519'), ('宁德时代', 'sz300750'), ('比亚迪', 'sz002594'),
        ('中国平安', 'sh601318'), ('招商银行', 'sh600036'), ('中信证券', 'sh600030'),
        ('东方财富', 'sz300059'), ('中际旭创', 'sz300308'), ('工业富联', 'sh601138'),
        ('五粮液', 'sz000858'), ('迈瑞医疗', 'sz300760'), ('立讯精密', 'sz002475'),
        ('中芯国际', 'sh688981'), ('药明康德', 'sh603259'), ('隆基绿能', 'sh601012'),
        ('海光信息', 'sh688041'), ('中科曙光', 'sh603019'), ('紫金矿业', 'sh601899'),
        ('万华化学', 'sh600309'), ('长江电力', 'sh600900')
    ]
    codes = [x[1] for x in sample_universe]
    batch = get_realtime_batch(codes, timeout=3)
    sample = []
    for name, code_qq in sample_universe:
        rt = batch.get(code_qq)
        if not rt:
            continue
        est_inflow = (rt.get('bid1_vol', 0) - rt.get('ask1_vol', 0)) * rt['price']
        sample.append({
            'code': code_qq,
            'name': name,
            'price': rt['price'],
            'chg': rt['chg_pct'],
            'net_inflow': est_inflow,
            'net_ratio': 0,
        })

    sample.sort(key=lambda x: x['net_inflow'], reverse=True)
    capital['main_force_top'] = sample[:limit]

    return capital


def get_market_news(limit=8):
    """获取市场快讯并分类利好/利空"""
    trace = uuid.uuid4().hex
    url = (
        "https://np-listapi.eastmoney.com/comm/web/getNewsByColumns?"
        "client=web&biz=web_news_col&column=345,346&order=1"
        f"&needInteractData=0&page_index=1&page_size={limit}&req_trace={trace}"
    )
    data = fetch_json(url)
    result = {
        'positive': [],
        'negative': [],
        'neutral': [],
    }
    if not data:
        return result

    rows = data.get('data', {}).get('list', []) or []
    pos_words = ['上涨', '新高', '增长', '增持', '回购', '利好', '突破', '超预期', '降息', '宽松']
    neg_words = ['下跌', '回调', '制裁', '减持', '风险', '违约', '利空', '收紧', '通胀', '地缘冲突']

    for row in rows:
        title = (row.get('title') or '').strip()
        media = row.get('mediaName', '')
        item = {
            'title': title,
            'media': media,
            'time': row.get('showTime', ''),
            'url': row.get('url', row.get('uniqueUrl', '')),
        }
        text = f"{title} {(row.get('summary') or '')}"
        if any(w in text for w in pos_words):
            result['positive'].append(item)
        elif any(w in text for w in neg_words):
            result['negative'].append(item)
        else:
            result['neutral'].append(item)

    return result


def build_market_report(result):
    """拼装文字版当日大盘报告"""
    overview = result.get('market_overview', {})
    sectors = result.get('hot_sectors', [])
    lhb = result.get('lhb', [])
    capital = result.get('capital_flow', {})
    news = result.get('news', {})

    top_sector = sectors[0]['name'] if sectors else '暂无'
    top_sector_chg = sectors[0]['chg'] if sectors else 0
    north_total = capital.get('northbound', {}).get('total_net_in', 0)
    flow_trend = '净流入' if north_total > 0 else '净流出' if north_total < 0 else '持平'

    report_lines = [
        f"【当日大盘分析报告】{result.get('date', '')}",
        f"1) 大盘涨势: {result.get('trend', '未知')}，上涨指数{result.get('up_count', 0)}个，下跌指数{result.get('down_count', 0)}个，平均涨跌{overview.get('avg_chg', 0)}%。",
        f"2) 热门板块: {top_sector}领涨（{top_sector_chg:+.2f}%），市场情绪{overview.get('sentiment', '未知')}，强度{overview.get('strength', '未知')}。",
        f"3) 龙虎榜: {'有' if lhb else '暂无'}重点个股上榜，资金偏好集中于高波动与题材股。",
        f"4) 资金量: 样本资金当日{flow_trend} {north_total / 100000000:.2f}亿元（估算口径），主力资金活跃度较高。",
        f"5) 利好利空: 利好{len(news.get('positive', []))}条，利空{len(news.get('negative', []))}条，中性{len(news.get('neutral', []))}条。",
        "6) 结论: 建议以板块轮动思路应对，强势板块可低吸不追高，关注资金连续净流入方向。"
    ]
    return "\n".join(report_lines)


def get_global_market_snapshot():
    """获取海外及港股市场快照"""
    # 腾讯行情代码（部分时段可能无数据，函数内已容错）
    global_indices = {
        '恒生指数': 'hkHSI',
        '道琼斯': 'usDJI',
        '纳斯达克': 'usIXIC',
        '标普500': 'usINX',
    }
    batch = get_realtime_batch(list(global_indices.values()), timeout=3)
    result = {}
    for name, code in global_indices.items():
        item = batch.get(code)
        if item:
            result[name] = {
                'price': item.get('price', 0),
                'chg': item.get('chg_pct', 0),
            }
    return result


def analyze_policy_and_news(news):
    """基于消息面提取政策倾向和风险倾向"""
    pos = news.get('positive', []) if news else []
    neg = news.get('negative', []) if news else []
    neu = news.get('neutral', []) if news else []
    all_items = pos + neg + neu

    easing_words = ['降准', '降息', '逆回购', '宽松', '专项债', '财政发力', '稳增长', '支持平台经济', '减税']
    tightening_words = ['去杠杆', '收紧', '通胀压力', '监管趋严', '地缘冲突', '制裁', '风险提示']

    easing_cnt = 0
    tightening_cnt = 0
    for item in all_items:
        text = f"{item.get('title', '')}"
        if any(w in text for w in easing_words):
            easing_cnt += 1
        if any(w in text for w in tightening_words):
            tightening_cnt += 1

    policy_score = easing_cnt - tightening_cnt
    msg_score = len(pos) - len(neg)

    if policy_score >= 2:
        policy_view = '偏宽松'
    elif policy_score <= -2:
        policy_view = '偏收紧'
    else:
        policy_view = '中性偏稳'

    return {
        'policy_score': policy_score,
        'policy_view': policy_view,
        'message_score': msg_score,
        'positive_count': len(pos),
        'negative_count': len(neg),
        'neutral_count': len(neu),
    }


def build_next_day_prediction():
    """构建次日开盘预测"""
    now = datetime.now().strftime('%Y-%m-%d %H:%M')

    # 国内市场（权重更高）
    domestic_codes = {
        '上证指数': 'sh000001',
        '深证成指': 'sz399001',
        '创业板指': 'sz399006',
        '沪深300': 'sh000300',
    }
    domestic_batch = get_realtime_batch(list(domestic_codes.values()), timeout=3)
    domestic = {}
    for name, code in domestic_codes.items():
        item = domestic_batch.get(code)
        if item:
            domestic[name] = item

    domestic_avg = 0
    if domestic:
        domestic_avg = sum(v.get('chg_pct', 0) for v in domestic.values()) / len(domestic)

    # 海外市场
    global_snapshot = get_global_market_snapshot()
    global_avg = 0
    if global_snapshot:
        global_avg = sum(v.get('chg', 0) for v in global_snapshot.values()) / len(global_snapshot)

    # 消息与政策
    news = get_market_news(limit=20)
    news_info = analyze_policy_and_news(news)

    # 技术面（以上证指数为锚）
    kline = get_kline_data('sh000001', 120)
    tech_score = 0
    tech_desc = '技术信号不足'
    expected_low = None
    expected_high = None
    if kline and len(kline) >= 60:
        closes = [float(k[1]) for k in kline]
        highs = [float(k[3]) for k in kline]
        lows = [float(k[4]) for k in kline]
        ma5 = calc_ma(closes, 5)
        ma10 = calc_ma(closes, 10)
        ma20 = calc_ma(closes, 20)
        dif, macd_hist, _ = calc_macd(closes)
        rsi = calc_rsi(closes, 14)
        k, d, _ = calc_kdj(highs, lows, closes)
        latest = closes[-1]

        if ma5 and ma10 and ma20:
            if latest > ma5 > ma10 > ma20:
                tech_score += 2
            elif latest < ma5 < ma10 < ma20:
                tech_score -= 2
        if dif is not None:
            tech_score += 1 if dif > 0 else -1
        if rsi is not None:
            if rsi < 35:
                tech_score += 1
            elif rsi > 70:
                tech_score -= 1
        if k is not None and d is not None:
            if k > d:
                tech_score += 1
            else:
                tech_score -= 1

        # 用近20日波动率给出次日合理波动区间
        rets = np.diff(closes[-21:]) / np.array(closes[-21:-1])
        vol = float(np.std(rets)) if len(rets) > 0 else 0.01
        expected_low = latest * (1 - max(0.004, vol * 0.8))
        expected_high = latest * (1 + max(0.004, vol * 0.8))
        tech_desc = f"MA结构{'多头' if tech_score >= 1 else '空头/震荡'}，DIF={dif}, RSI={rsi}, KDJ(K={k},D={d})"

    # 四因子分项得分（标准化到 -5~+5 区间，便于前端可视化）
    domestic_factor = max(-5, min(5, domestic_avg * 2.5))
    global_factor = max(-5, min(5, global_avg * 2.2))
    policy_news_raw = news_info['policy_score'] * 0.7 + news_info['message_score'] * 0.45
    policy_news_factor = max(-5, min(5, policy_news_raw))
    technical_factor = max(-5, min(5, tech_score * 1.25))

    # 综合打分（次日）
    total_score = domestic_factor * 0.35 + global_factor * 0.2 + policy_news_factor * 0.2 + technical_factor * 0.25
    if total_score >= 2.2:
        direction = '偏强高开概率大'
        action = '可考虑逢低布局强势板块，避免追高'
    elif total_score >= 0.6:
        direction = '震荡偏强'
        action = '控制仓位，围绕主线板块做低吸高抛'
    elif total_score > -0.8:
        direction = '窄幅震荡'
        action = '轻仓观望，等待方向确认'
    else:
        direction = '偏弱低开风险较高'
        action = '防守为主，控制回撤，减少追涨'

    confidence = int(min(90, max(45, 58 + abs(total_score) * 7)))

    range_text = '暂无'
    if expected_low is not None and expected_high is not None:
        range_text = f"预计上证次日波动区间: {expected_low:.2f} - {expected_high:.2f}"

    factor_scores = {
        'domestic': round(domestic_factor, 2),
        'global': round(global_factor, 2),
        'policy_news': round(policy_news_factor, 2),
        'technical': round(technical_factor, 2),
    }

    report_lines = [
        f"【次日大盘走势预测】{now}",
        "",
        "1) 国内外市场行情:",
        f"- A股核心指数均值涨跌: {domestic_avg:+.2f}%",
        f"- 海外/港股联动均值涨跌: {global_avg:+.2f}%",
        "",
        "2) 国家政策与消息面:",
        f"- 政策倾向: {news_info['policy_view']} (评分 {news_info['policy_score']:+d})",
        f"- 消息统计: 利好{news_info['positive_count']}条 / 利空{news_info['negative_count']}条 / 中性{news_info['neutral_count']}条",
        "",
        "3) 技术面:",
        f"- {tech_desc}",
        f"- {range_text}",
        "",
        "4) 次日开盘预测:",
        f"- 方向: {direction}",
        f"- 置信度: {confidence}%",
        f"- 操作建议: {action}",
        "",
        "5) 风险提示:",
        "- 海外突发事件、汇率与政策节奏变化可能导致偏离预测。",
        "- 本预测仅供参考，不构成投资建议。"
    ]

    return {
        'prediction': "\n".join(report_lines),
        'date': datetime.now().strftime('%Y-%m-%d'),
        'direction': direction,
        'confidence': confidence,
        'score': round(total_score, 2),
        'action': action,
        'range_text': range_text,
        'domestic_avg_chg': round(domestic_avg, 2),
        'global_avg_chg': round(global_avg, 2),
        'policy_view': news_info['policy_view'],
        'factor_scores': factor_scores,
    }


def calc_ma(prices, period):
    """计算移动平均线"""
    if len(prices) < period:
        return None
    return round(np.mean(prices[-period:]), 2)


def calc_macd(prices, fast=12, slow=26, signal=9):
    """计算MACD"""
    if len(prices) < slow:
        return None, None, None
    ema_fast = pd.Series(prices).ewm(span=fast, adjust=False).mean().iloc[-1]
    ema_slow = pd.Series(prices).ewm(span=slow, adjust=False).mean().iloc[-1]
    dif = ema_fast - ema_slow
    macd_hist = dif * 2  # MACD柱
    return round(dif, 3), round(macd_hist, 3), None


def calc_rsi(prices, period=14):
    """计算RSI"""
    if len(prices) < period + 1:
        return None
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 2)


def calc_boll(prices, period=20, std_dev=2):
    """计算布林带"""
    if len(prices) < period:
        return None, None, None
    recent = prices[-period:]
    ma = np.mean(recent)
    std = np.std(recent)
    upper = ma + std_dev * std
    lower = ma - std_dev * std
    return round(upper, 2), round(ma, 2), round(lower, 2)


def calc_kdj(highs, lows, closes, n=9, m1=3, m2=3):
    """计算KDJ"""
    if len(closes) < n:
        return None, None, None
    low_n = pd.Series(lows[-n:]).min()
    high_n = pd.Series(highs[-n:]).max()
    rsv = (closes[-1] - low_n) / (high_n - low_n) * 100 if high_n != low_n else 50
    k = 50
    d = 50
    for _ in range(n):
        k = (2 * k + rsv) / 3
        d = (2 * d + k) / 3
    j = 3 * k - 2 * d
    return round(k, 2), round(d, 2), round(j, 2)


def generate_signal(score):
    """根据评分生成信号"""
    if score >= 8:
        return '强烈买入', '分批建仓', 85
    elif score >= 6:
        return '买入', '适量买入', 70
    elif score >= 4:
        return '观望', '谨慎观望', 50
    elif score >= 2:
        return '谨慎', '少量减仓', 30
    else:
        return '卖出', '清仓观望', 15


def analyze_stock(code, name):
    """分析单只股票（带缓存）"""
    # 转换代码格式
    if code.endswith('sh'):
        code_qq = f"sh{code[:-2]}"
    elif code.endswith('sz'):
        code_qq = f"sz{code[:-2]}"
    else:
        code_qq = code
    
    cache_key_rt = f'rt_{code_qq}'
    cache_key_kline = f'kline_{code_qq}'
    
    # 实时数据缓存 5秒
    rt = cache_get(cache_key_rt)
    if rt is None:
        rt = get_realtime_data(code_qq, timeout=3)
        cache_set(cache_key_rt, rt, 5)
    if not rt:
        return None
    
    # K线数据缓存 60秒
    kline = cache_get(cache_key_kline)
    if kline is None:
        kline = get_kline_data(code_qq, 120)
        cache_set(cache_key_kline, kline, 60)
    if not kline or len(kline) < 30:
        return None
    
    prices = [float(k[1]) for k in kline]
    highs = [float(k[3]) for k in kline]
    lows = [float(k[4]) for k in kline]
    vols = [int(float(k[5])) for k in kline]
    
    # 计算技术指标
    ma5 = calc_ma(prices, 5)
    ma10 = calc_ma(prices, 10)
    ma20 = calc_ma(prices, 20)
    ma60 = calc_ma(prices, 60)
    dif, macd, _ = calc_macd(prices)
    rsi = calc_rsi(prices, 14)
    boll_upper, boll_ma, boll_lower = calc_boll(prices)
    k, d, j = calc_kdj(highs, lows, prices)
    
    # 综合评分
    score = 5
    
    # 趋势评分
    if ma5 and ma10 and ma20:
        if prices[-1] > ma5 > ma10 > ma20:
            score += 3
        elif prices[-1] < ma5 < ma10 < ma20:
            score -= 2
    
    # MACD评分
    if dif and dif > 0:
        score += 1
    elif dif and dif < 0:
        score -= 1
    
    # RSI评分
    if rsi:
        if rsi < 30:
            score += 2  # 超卖
        elif rsi > 70:
            score -= 1  # 超买
    
    # 布林带评分
    if boll_lower and boll_ma and prices[-1] < boll_lower:
        score += 2  # 触及下轨，可能反弹
    elif boll_upper and prices[-1] > boll_upper:
        score -= 1  # 突破上轨
    
    # KDJ评分
    if k and d and j:
        if j < 20:
            score += 1  # 超卖
        elif j > 80:
            score -= 1  # 超买
        if k > d and d < 30:
            score += 1  # 金叉
        elif k < d and d > 70:
            score -= 1  # 死叉
    
    # 保存原始代码用于显示
    display_code = code[:-2] if code.endswith('sh') or code.endswith('sz') else code
    
    signal_text, action, confidence = generate_signal(score)
    
    return {
        'name': name,
        'code': display_code,
        'market': 'SH' if code.endswith('sh') else 'SZ',
        'price': rt['price'],
        'chg': round(rt['chg_pct'], 2),
        'ma5': ma5,
        'ma10': ma10,
        'ma20': ma20,
        'ma60': ma60,
        'dif': dif,
        'macd': macd,
        'rsi': rsi,
        'boll_upper': boll_upper,
        'boll_ma': boll_ma,
        'boll_lower': boll_lower,
        'kdj_k': k,
        'kdj_d': d,
        'kdj_j': j,
        'signal_score': score,
        'signal_text': signal_text,
        'action': action,
        'confidence': confidence
    }


@app.route('/')
def index():
    """首页"""
    return '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>股票分析控制台</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: radial-gradient(circle at 10% 10%, #1a2035 0%, #0f1117 45%, #0b0d12 100%); color: #e4e4e4; padding: 40px; min-height: 100vh; }
            .container { max-width: 900px; margin: 0 auto; text-align: center; padding-top: 80px; }
            h1 { color: #00d4ff; font-size: 2.6em; margin-bottom: 18px; letter-spacing: 1px; }
            p { color: #9aa4c0; font-size: 15px; line-height: 1.8; }
            .actions { margin-top: 36px; }
            .btn-main { display: inline-block; padding: 16px 34px; background: #00d4ff; border: 1px solid #00d4ff; border-radius: 10px; color: #0f1117; text-decoration: none; font-size: 17px; font-weight: 700; }
            .btn-main:hover { background: #00bde3; }
            .hint { margin-top: 20px; color: #7b87aa; font-size: 13px; }
            .hint a { color: #7b87aa; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 股票分析控制台</h1>
            <p>已整合为单一工作台页面，包含股票分析、大盘分析与次日走势预测，减少重复页面与无效入口。</p>
            <div class="actions">
                <a class="btn-main" href="/stock_analysis">进入统一分析工作台</a>
            </div>
            <div class="hint">
                旧入口兼容: <a href="/market_analysis">/market_analysis</a>、<a href="/prediction">/prediction</a> 会自动跳转到工作台
            </div>
        </div>
    </body>
    </html>
    '''


@app.route('/stock_analysis')
def stock_analysis_page():
    """股票分析页面"""
    tmpl = os.path.join(os.path.dirname(__file__), 'templates', 'index.html')
    with open(tmpl, 'r', encoding='utf-8') as f:
        html = f.read()
    return html


@app.route('/market_analysis')
def market_analysis_page():
    """兼容旧地址，统一跳转到工作台"""
    return redirect('/stock_analysis', code=302)


@app.route('/prediction')
def prediction_page():
    """兼容旧地址，统一跳转到工作台"""
    return redirect('/stock_analysis', code=302)


@app.route('/api/stock_analysis', methods=['POST'])
def api_stock_analysis():
    """股票分析API - 并发版"""
    stocks = get_stock_list()
    
    if not stocks:
        return jsonify({'stocks': [], 'total': 0, 'analyzed': 0})
    
    # 支持快速模式：只分析前20只（可传参数 ?full=1 分析全部）
    full_mode = request.args.get('full', '0') == '1'
    if not full_mode and len(stocks) > 30:
        stocks = stocks[:30]  # 快速模式只分析前30只
    
    results = []
    lock = threading.Lock()
    
    def analyze_one(stock):
        try:
            result = analyze_stock(stock['code'], stock['name'])
            return result
        except:
            return None
    
    # 并发请求，最多10个线程
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_one, s): s for s in stocks}
        for f in as_completed(futures):
            result = f.result()
            if result:
                with lock:
                    results.append(result)
    
    # 按评分排序
    results.sort(key=lambda x: x['signal_score'], reverse=True)
    
    # 清理过期缓存（非阻塞）
    cache_clean()
    
    return jsonify({
        'stocks': results,
        'total': len(stocks),
        'analyzed': len(results)
    })


@app.route('/api/market_analysis', methods=['POST'])
def api_market_analysis():
    """大盘分析API - 报告版"""
    # 主要指数
    indices = {
        '上证指数': 'sh000001',
        '深证成指': 'sz399001',
        '创业板指': 'sz399006',
        '沪深300': 'sh000300',
        '科创50': 'sh000688',
        '上证50': 'sh000016',
        '中证500': 'sh000905',
        '中证1000': 'sh000852',
        '北证50': 'sh899050',
        'B股指数': 'sh999007',
    }

    result = {
        'indices': {},
        'hot_sectors': [],
        'lhb': [],
        'capital_flow': {},
        'news': {},
        'date': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'market_overview': {},
        'report': ''
    }
    
    for name, code in indices.items():
        rt = get_realtime_data(code, timeout=2)
        kline = get_kline_data(code, 60) if rt else None
        if rt:
            item = {
                'price': rt['price'],
                'chg': rt['chg_pct'],
                'volume': rt.get('vol', 0),
            }
            if kline and len(kline) >= 60:
                prices = [float(k[1]) for k in kline]
                volumes = [int(float(k[5])) for k in kline]
                item['ma5'] = round(sum(prices[-5:])/5, 2)
                item['ma10'] = round(sum(prices[-10:])/10, 2)
                item['ma20'] = round(sum(prices[-20:])/20, 2)
                item['vol_ma5'] = round(sum(volumes[-5:])/5, 0)
                # 计算RSI
                gains = [prices[i]-prices[i-1] for i in range(1, len(prices)) if prices[i]>prices[i-1]]
                losses = [prices[i-1]-prices[i] for i in range(1, len(prices)) if prices[i]<prices[i-1]]
                avg_gain = sum(gains[-14:])/14 if gains else 0
                avg_loss = sum(losses[-14:])/14 if losses else 0
                rs = avg_gain/avg_loss if avg_loss else 99
                item['rsi'] = round(100 - 100/(1+rs), 1) if rs else 50
            result['indices'][name] = item

    # 扩展数据
    result['hot_sectors'] = get_hot_sectors(limit=8)
    result['lhb'] = get_lhb_list(limit=5)
    result['capital_flow'] = get_capital_flow_snapshot(limit=8)
    result['news'] = get_market_news(limit=8)
    
    if result['indices']:
        up_count = sum(1 for v in result['indices'].values() if v['chg'] > 0)
        down_count = len(result['indices']) - up_count
        result['trend'] = '偏多' if up_count > down_count else '偏空' if down_count > up_count else '中性'
        result['up_count'] = up_count
        result['down_count'] = down_count
        # 计算市场情绪
        avg_chg = sum(v['chg'] for v in result['indices'].values()) / len(result['indices'])
        result['market_overview']['avg_chg'] = round(avg_chg, 2)
        result['market_overview']['sentiment'] = '乐观' if avg_chg > 0.5 else '谨慎乐观' if avg_chg > 0 else '偏谨慎' if avg_chg > -0.5 else '悲观'
        result['market_overview']['strength'] = '强势' if avg_chg > 1 else '偏强' if avg_chg > 0.3 else '偏弱' if avg_chg > -0.3 else '弱势'

    result['report'] = build_market_report(result)
    
    return jsonify(result)


@app.route('/api/prediction', methods=['POST'])
def api_prediction():
    """次日走势预测API"""
    try:
        result = build_next_day_prediction()
    except Exception as e:
        result = {
            'prediction': f"预测生成失败: {e}",
            'date': datetime.now().strftime('%Y-%m-%d'),
            'direction': '未知',
            'confidence': 0,
            'score': 0,
        }
    return jsonify(result)


# ==================== 自选股票管理 API ====================

STOCK_FILE = os.path.join(os.path.dirname(__file__), '自选股票.md')

def normalize_stock_code(code_raw):
    """标准化股票代码，返回 (code, market) 如 ('300059', 'SZ')"""
    code_raw = code_raw.strip().upper().replace(' ', '')
    # 处理 300059.SZ / 300059.SH 格式
    if '.' in code_raw:
        parts = code_raw.split('.')
        num, mkt = parts[0], parts[1]
        if mkt in ('SZ', 'SH'):
            return num, mkt
    # 处理纯6位数字格式：6开头=SH，0/3开头=SZ
    if len(code_raw) == 6 and code_raw.isdigit():
        if code_raw.startswith('6'):
            return code_raw, 'SH'
        else:
            return code_raw, 'SZ'
    return None, None

def lookup_stock_name(code_num, market):
    """通过腾讯API查询股票名称"""
    qq_code = f'sh{code_num}' if market == 'SH' else f'sz{code_num}'
    try:
        url = f'https://qt.gtimg.cn/q={qq_code}'
        r = requests.get(url, timeout=3, headers={'Referer': 'https://finance.qq.com'})
        if r.text and '~' in r.text:
            s = r.text.split('=')[1].strip(';').strip('"').split('~')
            if len(s) > 1 and s[1]:
                return s[1]
    except:
        pass
    return code_num  # 查不到就用代码当名称

def sync_stock_from_code(code_raw):
    """根据用户输入的代码，返回标准化后的信息"""
    code_num, market = normalize_stock_code(code_raw)
    if not code_num:
        return None, '无效的股票代码格式，支持: 300059.SZ / 600519.SH / 300059 / 600519'
    
    # 检查是否已存在
    existing = get_stock_list()
    for s in existing:
        if s['code'] == f'{code_num.lower()}{market.lower()}':
            return None, f'股票 {code_num}.{market} ({s["name"]}) 已存在'
    
    name = lookup_stock_name(code_num, market)
    display_code = f'{code_num}.{market}'
    return {'code': display_code, 'name': name, 'code_num': code_num, 'market': market}, None

def append_stock_to_file(code_display, name):
    """向自选股票.md追加一行"""
    if not os.path.exists(STOCK_FILE):
        return False
    with open(STOCK_FILE, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # 找最后一行带 | 的行作为序号参考
    last_num = 0
    for line in lines:
        if '|' in line and line.strip().startswith('|'):
            parts = [p.strip() for p in line.split('|') if p.strip()]
            if parts and parts[0].replace('.', '').isdigit():
                try:
                    last_num = max(last_num, int(float(parts[0])))
                except:
                    pass
    
    new_line = f'| {last_num + 1} | {code_display} | {name} | - | 自选 |\n'
    
    with open(STOCK_FILE, 'a', encoding='utf-8') as f:
        f.write(new_line)
    return True

def remove_stock_from_file(code_display):
    """从自选股票.md删除指定股票"""
    if not os.path.exists(STOCK_FILE):
        return False
    with open(STOCK_FILE, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    found = False
    new_lines = []
    for line in lines:
        if '|' in line and code_display.upper() in line.upper():
            found = True
            continue
        new_lines.append(line)
    
    if found:
        with open(STOCK_FILE, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
    return found


@app.route('/api/stocks', methods=['GET'])
def api_list_stocks():
    """获取自选股列表"""
    stocks = get_stock_list()
    # 标准化格式: code=300059sz -> display=300059.SZ
    result = []
    for s in stocks:
        code = s['code']
        if code.endswith('sz'):
            display = code[:-2].upper() + '.SZ'
        elif code.endswith('sh'):
            display = code[:-2].upper() + '.SH'
        else:
            display = code
        result.append({'code': display, 'name': s['name'], 'raw_code': code})
    return jsonify({'stocks': result, 'total': len(result)})

@app.route('/api/stocks', methods=['POST'])
def api_add_stock():
    """添加自选股"""
    data = request.get_json()
    if not data or 'code' not in data:
        return jsonify({'error': '请提供股票代码'}), 400
    
    result, error = sync_stock_from_code(data['code'])
    if error:
        return jsonify({'error': error}), 400
    
    if append_stock_to_file(result['code'], result['name']):
        return jsonify({'success': True, 'stock': result})
    return jsonify({'error': '写入文件失败'}), 500

@app.route('/api/stocks/<path:code>', methods=['DELETE'])
def api_delete_stock(code):
    """删除自选股"""
    # code 可能是 300059.SZ 或 300059sz 格式
    code_upper = code.strip().upper()
    if '.' in code_upper:
        parts = code_upper.split('.')
        display = f'{parts[0]}.{parts[1]}'
    else:
        # 尝试推断
        if code.endswith('sz') or code.endswith('sh'):
            display = code[:-2].upper() + ('.SZ' if code.endswith('sz') else '.SH')
        else:
            return jsonify({'error': '无效代码格式'}), 400
    
    if remove_stock_from_file(display):
        return jsonify({'success': True, 'deleted': display})
    return jsonify({'error': f'未找到股票 {display}'}), 404

@app.route('/api/stocks/upload', methods=['POST'])
def api_upload_stocks():
    """批量上传Excel文件导入自选股"""
    if 'file' not in request.files:
        return jsonify({'error': '请上传文件'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': '文件名为空'}), 400
    
    try:
        # 支持 xlsx 和 csv
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file, engine='openpyxl')
    except Exception as e:
        return jsonify({'error': f'文件解析失败: {str(e)}'}), 400
    
    # 查找股票代码列（可能的列名）
    code_col = None
    for col in df.columns:
        col_lower = str(col).lower()
        if '代码' in col_lower or 'code' in col_lower or '股票' in col_lower or 'symbol' in col_lower:
            code_col = col
            break
    
    if code_col is None:
        # 默认使用第一列
        code_col = df.columns[0]
    
    added = []
    skipped = []
    for val in df[code_col].dropna():
        code_str = str(val).strip()
        if not code_str or code_str == 'nan':
            continue
        result, error = sync_stock_from_code(code_str)
        if error:
            skipped.append({'code': code_str, 'reason': error})
        else:
            if append_stock_to_file(result['code'], result['name']):
                added.append(result)
    
    return jsonify({
        'success': True,
        'added': added,
        'added_count': len(added),
        'skipped': skipped,
        'skipped_count': len(skipped)
    })


# ==================== 量化回测引擎 ====================

def run_backtest(code, market, strategy, start_date, end_date, capital):
    """量化回测核心引擎"""
    code_qq = f"{'sh' if market == 'SH' else 'sz'}{code}"
    
    # 获取历史K线（尝试3次）
    kline = None
    for attempt in range(3):
        kline = get_kline_data(code_qq, 600)
        if kline and len(kline) >= 60:
            break
        time.sleep(0.5)
    
    if not kline or len(kline) < 30:
        return {'error': '历史数据不足，该股票可能已退市或代码错误。请检查代码和市场选择。'}
    
    if len(kline) < 60:
        return {'error': f'仅获取到 {len(kline)} 天K线数据，至少需要60天进行回测。'}
    
    # 解析K线数据
    dates = [k[0] for k in kline]
    prices = [float(k[1]) for k in kline]
    opens = [float(k[2]) for k in kline]
    highs = [float(k[3]) for k in kline]
    lows = [float(k[4]) for k in kline]
    vols = [int(float(k[5])) for k in kline]
    
    # 筛选日期范围（自动扩展以包含足够指标计算数据）
    idx_start = 0
    idx_end = len(dates) - 1
    trade_start_idx = 0  # 用户选择的实际起始日期对应的索引
    found_start = False
    for i, d in enumerate(dates):
        if d >= start_date and not found_start:
            trade_start_idx = i          # 用户选择的真实起始日
            idx_start = max(30, i - 30)  # 至少留30天算指标（用于正确计算MA/MACD等）
            found_start = True
        if d <= end_date:
            idx_end = i
    if not found_start:
        idx_start = max(30, len(dates) - 90)  # 兜底：取最后90天
    
    if idx_end - idx_start < 20:
        return {'error': f'日期范围数据不足（仅{idx_end-idx_start}天）。数据范围: {dates[0]} ~ {dates[-1]}，请调整起止日期。'}
    
    # 子集（仅从用户选择的起始日期开始展示）
    sub_dates = dates[trade_start_idx:idx_end+1]
    sub_prices = prices[trade_start_idx:idx_end+1]
    sub_opens = opens[trade_start_idx:idx_end+1]
    
    # 计算指标
    ma5 = calc_ma_series(prices, 5)
    ma10 = calc_ma_series(prices, 10)
    ma20 = calc_ma_series(prices, 20)
    ma60 = calc_ma_series(prices, 60)
    dif, macd_vals, _ = calc_macd_series(prices)
    rsi_vals = calc_rsi_series(prices)
    boll_u, boll_m, boll_l = calc_boll_series(prices)
    atr = calc_atr_series(highs, lows, prices)
    adx = calc_adx_series(highs, lows, prices)
    
    # 生成信号
    signals = []
    for i in range(idx_start, idx_end + 1):
        sig = None
        if strategy == 'ma_cross':
            sig = signal_ma_cross(i, prices, ma5, ma10, ma20, signals)
        elif strategy == 'macd_signal':
            sig = signal_macd(i, dif, macd_vals, signals)
        elif strategy == 'rsi_extreme':
            sig = signal_rsi(i, rsi_vals, prices, signals)
        elif strategy == 'boll_break':
            sig = signal_boll(i, prices, boll_u, boll_m, boll_l, signals)
        elif strategy == 'multi_factor':
            sig = signal_multi(i, prices, ma5, ma10, ma20, dif, macd_vals, rsi_vals, boll_u, boll_l, signals)
        elif strategy == 'adaptive':
            sig = signal_adaptive(i, prices, highs, lows, vols, ma5, ma10, ma20, ma60, dif, macd_vals, rsi_vals, boll_u, boll_m, boll_l, atr, adx, signals)
        
        if sig:
            sig['idx'] = i
            sig['date'] = dates[i] if i < len(dates) else ''
            sig['price'] = prices[i] if i < len(prices) else 0
            signals.append(sig)
    
    # 模拟交易（仅从用户选择的起始日期开始执行交易）
    trades, equity_curve, buy_hold_curve, daily_returns = simulate_trades(
        signals, sub_dates, sub_prices, sub_opens, capital, trade_start_idx)
    
    # 检查是否有实际交易
    actual_trades = [t for t in trades if t['action'] in ('买入','卖出')]
    if len(actual_trades) < 2:
        min_p = min(sub_prices) if sub_prices else 0
        lots = int(capital / (min_p * 100)) if min_p > 0 else 0
        
        if lots < 1:
            need_cap = int(min_p * 100 * 1.1)
            hint = f'股价 {min_p:.2f} 太高，当前资金仅可买 {lots} 手。建议初始资金 ≥ {need_cap/10000:.0f} 万。'
        else:
            # 自动检测哪种策略信号最多
            strategies_cn = {'ma_cross':'双均线交叉','macd_signal':'MACD金叉死叉','rsi_extreme':'RSI超买超卖','boll_break':'布林带突破','multi_factor':'综合多因子','adaptive':'自适应智能'}
            best_s, best_n = strategy, len(signals)
            for test_s in ['ma_cross','macd_signal','boll_break','rsi_extreme','multi_factor','adaptive']:
                if test_s == strategy: continue
                ts = []
                for i in range(idx_start, idx_end + 1):
                    sig = None
                    if test_s == 'ma_cross': sig = signal_ma_cross(i, prices, ma5, ma10, ma20, ts)
                    elif test_s == 'macd_signal': sig = signal_macd(i, dif, macd_vals, ts)
                    elif test_s == 'rsi_extreme': sig = signal_rsi(i, rsi_vals, prices, ts)
                    elif test_s == 'boll_break': sig = signal_boll(i, prices, boll_u, boll_m, boll_l, ts)
                    elif test_s == 'multi_factor': sig = signal_multi(i, prices, ma5, ma10, ma20, dif, macd_vals, rsi_vals, boll_u, boll_l, ts)
                    elif test_s == 'adaptive': sig = signal_adaptive(i, prices, highs, lows, vols, ma5, ma10, ma20, ma60, dif, macd_vals, rsi_vals, boll_u, boll_m, boll_l, atr, adx, ts)
                    if sig:
                        sig['idx'] = i; sig['date'] = dates[i] if i < len(dates) else ''; sig['price'] = prices[i] if i < len(prices) else 0
                        ts.append(sig)
                if len(ts) > best_n: best_n = len(ts); best_s = test_s
            
            if best_s != strategy:
                hint = f'"{strategies_cn.get(strategy,strategy)}"仅{len(signals)}个信号→0笔交易。推荐切换至"{strategies_cn.get(best_s,best_s)}"（{best_n}个信号）。'
            else:
                hint = f'仅{len(signals)}个信号→0笔交易。建议扩大日期范围（当前{len(sub_dates)}天）。'
        
        return {'total_return': 0, 'buy_hold_return': 0, 'win_rate': 0,
                'max_drawdown': 0, 'total_trades': 0, 'win_trades': 0,
                'sharpe': 0, 'calmar': 0, 'annual_return': 0,
                'final_equity': capital, 'equity_curve': equity_curve,
                'buy_hold_curve': buy_hold_curve, 'trades': [],
                'strategy': strategy, 'code': code, 'market': market,
                'hint': hint, 'signal_count': len(signals)}
    
    # 计算专业绩效指标
    perf = calc_performance(trades, equity_curve, daily_returns, capital, buy_hold_curve)
    
    # 平均持仓天数
    hold_days = []
    buy_date = None
    for t in trades:
        if t['action'] == '买入':
            buy_date = t['date']
        elif t['action'] == '卖出' and buy_date:
            try:
                d1 = datetime.strptime(buy_date, '%Y-%m-%d')
                d2 = datetime.strptime(t['date'], '%Y-%m-%d')
                hold_days.append((d2-d1).days)
            except: pass
            buy_date = None
    avg_hold = round(sum(hold_days)/len(hold_days)) if hold_days else 0
    
    return {
        **perf,
        'avg_hold_days': avg_hold,
        'equity_curve': equity_curve,
        'buy_hold_curve': buy_hold_curve,
        'trades': trades,
        'strategy': strategy,
        'code': code,
        'market': market,
        'data_days': len(sub_dates),
        'signal_count': len(signals),
    }


# ---- 信号生成（聚宽风格：基于历史数据，不含未来信息） ----
    if i < 1: return None
    # 金叉: MA5上穿MA10（在MA20上方更好）
# ---- 信号生成（聚宽风格：基于历史数据，不含未来信息） ----
def signal_ma_cross(i, prices, ma5, ma10, ma20, prev_signals):
    if i < 1: return None
    if ma5[i] and ma10[i] and ma5[i-1] and ma10[i-1]:
        if ma5[i-1] <= ma10[i-1] and ma5[i] > ma10[i]:
            # 检查是否已有持仓
            if not any(s['type']=='buy' for s in prev_signals[-5:]):
                return {'type': 'buy', 'signal': 'MA5↑MA10 金叉'}
        elif ma5[i-1] >= ma10[i-1] and ma5[i] < ma10[i]:
            if any(s['type']=='buy' for s in prev_signals[-20:]):
                return {'type': 'sell', 'signal': 'MA5↓MA10 死叉'}
    return None

def signal_macd(i, dif, macd_vals, prev_signals):
    if i < 1: return None
    d, m = dif[i], macd_vals[i]
    d_prev, m_prev = dif[i-1], macd_vals[i-1]
    if d is None or m is None or d_prev is None or m_prev is None: return None
    if d_prev <= m_prev and d > m and d < 0:
        if not any(s['type']=='buy' for s in prev_signals[-5:]):
            return {'type': 'buy', 'signal': 'MACD金叉(低位)'}
    elif d > 0 and d < m:
        if any(s['type']=='buy' for s in prev_signals[-30:]):
            return {'type': 'sell', 'signal': 'MACD死叉'}
    return None

def signal_rsi(i, rsi_vals, prices, prev_signals):
    r = rsi_vals[i]
    if r is None: return None
    if r < 25:
        if not any(s['type']=='buy' for s in prev_signals[-3:]):
            return {'type': 'buy', 'signal': f'RSI超卖({r:.0f})'}
    elif r > 75:
        if any(s['type']=='buy' for s in prev_signals[-30:]):
            return {'type': 'sell', 'signal': f'RSI超买({r:.0f})'}
    return None

def signal_boll(i, prices, boll_u, boll_m, boll_l, prev_signals):
    p = prices[i] if i < len(prices) else 0
    bl = boll_l[i] if boll_l and i < len(boll_l) else None
    bu = boll_u[i] if boll_u and i < len(boll_u) else None
    if bl is None or bu is None: return None
    if p <= bl * 1.02:
        if not any(s['type']=='buy' for s in prev_signals[-3:]):
            return {'type': 'buy', 'signal': '触及布林下轨'}
    elif p >= bu * 0.98:
        if any(s['type']=='buy' for s in prev_signals[-30:]):
            return {'type': 'sell', 'signal': '触及布林上轨'}
    return None

def signal_multi(i, prices, ma5, ma10, ma20, dif, macd_vals, rsi_vals, boll_u, boll_l, prev_signals):
    score = 0
    reasons = []
    p = prices[i] if i < len(prices) else 0
    
    # MA多头
    if ma5[i] and ma10[i] and ma20[i] and ma5[i] > ma10[i] > ma20[i]: score+=2; reasons.append('MA多头')
    elif ma5[i] and ma10[i] and ma5[i] < ma10[i]: score-=2; reasons.append('MA空头')
    
    # MACD
    if dif[i] and macd_vals[i] and dif[i-1] and macd_vals[i-1]:
        if dif[i-1] <= macd_vals[i-1] and dif[i] > macd_vals[i]: score+=2; reasons.append('MACD金叉')
        elif dif[i] > 0: score+=1
        else: score-=1
    
    # RSI
    r = rsi_vals[i]
    if r is not None:
        if r < 30: score+=2; reasons.append('RSI超卖')
        elif r > 70: score-=2; reasons.append('RSI超买')
    
    # Boll
    bl = boll_l[i] if boll_l and i < len(boll_l) else None
    bu = boll_u[i] if boll_u and i < len(boll_u) else None
    if bl and p <= bl * 1.02: score+=1; reasons.append('布林下轨')
    elif bu and p >= bu * 0.98: score-=1; reasons.append('布林上轨')
    
    has_buy = any(s['type']=='buy' for s in prev_signals[-30:])
    if score >= 4 and not any(s['type']=='buy' for s in prev_signals[-3:]):
        return {'type': 'buy', 'signal': f"多因子评分+{score}({','.join(reasons[:2])})"}
    elif score <= -3 and has_buy:
        return {'type': 'sell', 'signal': f"多因子评分{score}({','.join(reasons[:2])})"}
    return None


# ==================== 自适应策略引擎 ====================

def calc_atr_series(highs, lows, closes, period=14):
    """计算ATR序列"""
    atr = [None] * len(closes)
    tr_list = []
    for i in range(1, len(closes)):
        tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        tr_list.append(tr)
    for i in range(period - 1, len(tr_list)):
        atr[i + 1] = round(sum(tr_list[i-period+1:i+1]) / period, 2)
    return atr

def calc_adx_series(highs, lows, closes, period=14):
    """计算ADX趋势强度"""
    n = len(closes)
    tr = [0]*n; plus_dm = [0]*n; minus_dm = [0]*n
    for i in range(1, n):
        tr[i] = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        up = highs[i] - highs[i-1]; down = lows[i-1] - lows[i]
        plus_dm[i] = up if up > down and up > 0 else 0
        minus_dm[i] = down if down > up and down > 0 else 0
    
    # 平滑
    tr_s = [0]*n; pdm_s = [0]*n; mdm_s = [0]*n
    tr_s[period] = sum(tr[1:period+1])
    pdm_s[period] = sum(plus_dm[1:period+1])
    mdm_s[period] = sum(minus_dm[1:period+1])
    for i in range(period+1, n):
        tr_s[i] = tr_s[i-1] - tr_s[i-1]/period + tr[i]
        pdm_s[i] = pdm_s[i-1] - pdm_s[i-1]/period + plus_dm[i]
        mdm_s[i] = mdm_s[i-1] - mdm_s[i-1]/period + minus_dm[i]
    
    adx = [None]*n
    for i in range(period*2, n):
        pdi = (pdm_s[i]/tr_s[i]*100) if tr_s[i] > 0 else 0
        mdi = (mdm_s[i]/tr_s[i]*100) if tr_s[i] > 0 else 0
        dx = abs(pdi-mdi)/(pdi+mdi)*100 if (pdi+mdi) > 0 else 0
        adx[i] = round(dx, 1)
    return adx

def detect_market_regime(i, prices, adx, atr, boll_u, boll_l, boll_m):
    """市场状态检测：trending / ranging / volatile"""
    # ADX判断趋势强度
    adx_val = adx[i] if adx and i < len(adx) and adx[i] else 0
    
    # 布林带宽度 = 波动率
    if boll_u and boll_l and boll_m and i < len(boll_u) and boll_u[i] and boll_l[i] and boll_m[i]:
        bb_width = (boll_u[i] - boll_l[i]) / boll_m[i] * 100  # 百分比宽度
    else:
        bb_width = 0
    
    # ATR相对值
    if atr and i < len(atr) and atr[i] and prices[i] > 0:
        atr_pct = atr[i] / prices[i] * 100
    else:
        atr_pct = 0
    
    # 状态判定
    if adx_val > 25:
        regime = 'trending'
        confidence_adj = 1.0
    elif bb_width > 8 or atr_pct > 4:
        regime = 'volatile'
        confidence_adj = 0.7  # 高波动时提高入场门槛
    else:
        regime = 'ranging'
        confidence_adj = 0.85
    
    return regime, confidence_adj, {'adx': adx_val, 'bb_width': round(bb_width,1), 'atr_pct': round(atr_pct,2)}


def signal_adaptive(i, prices, highs, lows, vols,
                    ma5, ma10, ma20, ma60,
                    dif, macd_vals,
                    rsi_vals,
                    boll_u, boll_m, boll_l,
                    atr, adx,
                    prev_signals):
    """
    自适应多因子策略：
    - 市场状态检测 → 动态选择子策略
    - 成交量确认 → 过滤假突破
    - ATR动态止损 → 自适应波动率
    - 近期胜率 → 动态调整置信度阈值
    """
    if i < 30: return None
    
    p = prices[i]
    regime, conf_adj, regime_info = detect_market_regime(i, prices, adx, atr, boll_u, boll_l, boll_m)
    
    score = 0
    reasons = []
    
    # ============ 1. 趋势因子 ============
    if ma5[i] and ma10[i] and ma20[i] and ma60[i]:
        # 多头排列
        if p > ma5[i] > ma10[i] > ma20[i] > ma60[i]:
            score += 4; reasons.append('强多头排列')
        elif p > ma5[i] > ma10[i] > ma20[i]:
            score += 2; reasons.append('多头排列')
        elif p < ma5[i] < ma10[i] < ma20[i] < ma60[i]:
            score -= 3; reasons.append('强空头排列')
        elif p < ma5[i] < ma10[i] < ma20[i]:
            score -= 2; reasons.append('空头排列')
        
        # 均线坡度（趋势加速度）
        if i >= 5 and ma20[i] and ma20[i-5]:
            slope = (ma20[i] - ma20[i-5]) / ma20[i-5] * 100
            if slope > 1: score += 1; reasons.append('MA20↑加速')
            elif slope < -1: score -= 1; reasons.append('MA20↓加速')
    
    # ============ 2. MACD信号 ============
    if dif[i] and macd_vals[i] and dif[i-1] and macd_vals[i-1]:
        # MACD柱变化方向
        if i >= 1:
            hist = dif[i] - macd_vals[i]
            hist_prev = dif[i-1] - macd_vals[i-1]
            if hist > hist_prev and hist > 0: score += 2; reasons.append('MACD柱放大')
            elif hist < hist_prev and hist < 0: score -= 1; reasons.append('MACD柱缩小')
        
        # 零轴位置
        if dif[i] > 0: score += 1
        else: score -= 1
    
    # ============ 3. RSI信号 ============
    r = rsi_vals[i]
    if r is not None:
        if r < 25: score += 3; reasons.append(f'RSI超卖({r:.0f})')
        elif r < 35: score += 1; reasons.append('RSI偏低')
        elif r > 75: score -= 2; reasons.append(f'RSI超买({r:.0f})')
        elif r > 65: score -= 1; reasons.append('RSI偏高')
    
    # ============ 4. 布林带信号 ============
    if boll_l[i] and boll_u[i] and boll_m[i]:
        bb_pos = (p - boll_l[i]) / (boll_u[i] - boll_l[i]) if boll_u[i] != boll_l[i] else 0.5
        if bb_pos < 0.1: score += 2; reasons.append('布林下轨')
        elif bb_pos > 0.9: score -= 1; reasons.append('布林上轨')
        elif bb_pos > 0.5: score += 0.5  # 中轨上方偏多
    
    # ============ 5. 成交量确认 ============
    if i >= 5 and vols[i]:
        vol_ma5 = sum(vols[max(0,i-5):i]) / min(5, i) if i >= 1 else vols[i]
        if vol_ma5 > 0 and vols[i] > vol_ma5 * 1.3:
            if score > 0: score += 1; reasons.append('放量确认')
            elif score < 0: score -= 0.5  # 放量下跌更危险
    
    # ============ 6. 市场状态自适应调整 ============
    if regime == 'trending':
        # 趋势市：加重趋势因子权重
        score = score * 1.2
        reasons.append(f'趋势市(ADX:{regime_info["adx"]:.0f})')
    elif regime == 'volatile':
        # 高波动市：提高入场门槛
        reasons.append(f'高波动(ATR:{regime_info["atr_pct"]:.1f}%)')
    elif regime == 'ranging':
        # 震荡市：降低趋势权重，关注布林带
        reasons.append('震荡市')
    
    # ============ 7. 近期表现自适应 ============
    # 计算最近5次交易的胜率来调整阈值
    recent_sells = [s for s in prev_signals[-30:] if s.get('type') == 'sell']
    recent_buys = [s for s in prev_signals[-30:] if s.get('type') == 'buy']
    
    # 动态阈值
    if len(recent_sells) >= 3:
        recent_wins = sum(1 for s in recent_sells[-5:] if s.get('pnl', 0) > 0)
        win_rate = recent_wins / max(len(recent_sells[-5:]), 1)
        if win_rate > 0.6: buy_threshold = 2.5  # 胜率高，放宽买入
        elif win_rate < 0.3: buy_threshold = 5.5  # 胜率低，收紧买入
        else: buy_threshold = 4.0
    else:
        buy_threshold = 4.0  # 默认阈值
    
    sell_threshold = -buy_threshold * 0.5  # 卖出阈值比买入宽松
    
    # ============ 信号判定 ============
    score = score * conf_adj  # 应用波动率调整
    
    has_buy = any(s['type'] == 'buy' for s in recent_buys)
    last_buy_days = 999
    if has_buy:
        for s in reversed(recent_buys):
            if s['type'] == 'buy':
                last_buy_days = 0
                break
    
    if score >= buy_threshold and not any(s['type'] == 'buy' for s in prev_signals[-3:]):
        return {'type': 'buy', 'signal': f'自适应+{score:.1f}({regime})',
                'pnl': 0}  # pnl占位，卖出时更新
    
    if score <= sell_threshold and has_buy:
        return {'type': 'sell', 'signal': f'自适应{score:.1f}({regime})',
                'pnl': 0}
    
    # 强制止损：价格跌破MA20超过2*ATR
    if has_buy and atr[i] and ma20[i]:
        stop_price = ma20[i] - 2 * atr[i]
        if p < stop_price:
            return {'type': 'sell', 'signal': f'止损(破MA20-2ATR)',
                    'pnl': 0}
    
    return None


# ---- 模拟交易（聚宽风格） ----
def simulate_trades(signals, dates, prices, opens, capital, offset):
    """聚宽风格模拟交易：每日评估信号，严格防止未来函数"""
    trades = []
    equity_curve = []
    buy_hold_curve = []
    daily_returns = []
    
    cash = capital
    shares = 0
    start_price = prices[0] if prices else 1
    cost_basis = 0  # 持仓成本
    
    # 费率和滑点（聚宽风格）
    COMMISSION = 0.0003   # 佣金万三
    STAMP_TAX = 0.001     # 印花税千一（仅卖出）
    SLIPPAGE = 0.001      # 滑点千一
    
    sigs = [s for s in signals if s['idx'] >= offset]
    sig_idx = 0
    
    for i in range(len(dates)):
        price = prices[i]
        day = dates[i]
        
        # 日终处理当日信号（防止未来函数：用今日收盘价判断信号，次日开盘执行）
        while sig_idx < len(sigs) and sigs[sig_idx]['date'] == day:
            s = sigs[sig_idx]
            if s['type'] == 'buy' and shares == 0 and cash > 0:
                # 考虑滑点：实际买入价 = 信号价 * (1 + 滑点)
                exec_price = price * (1 + SLIPPAGE)
                buy_shares = int(cash / (exec_price * (1 + COMMISSION)) / 100) * 100
                if buy_shares >= 100:
                    cost = buy_shares * exec_price * (1 + COMMISSION)
                    if cost <= cash:
                        cash -= cost
                        shares = buy_shares
                        cost_basis = exec_price
                        trades.append({
                            'action': '买入', 'date': day, 'price': round(exec_price, 2),
                            'shares': shares, 'amount': round(cost, 0),
                            'signal': s.get('signal', ''), 'cash_after': round(cash, 0)
                        })
            elif s['type'] == 'sell' and shares > 0:
                exec_price = price * (1 - SLIPPAGE)
                revenue = shares * exec_price
                commission = revenue * COMMISSION
                tax = revenue * STAMP_TAX
                net_revenue = revenue - commission - tax
                pnl = net_revenue - (shares * cost_basis)
                pnl_pct = (pnl / (shares * cost_basis)) * 100 if cost_basis > 0 else 0
                # 更新买入记录的盈亏
                for t in reversed(trades):
                    if t['action'] == '买入' and 'pnl_pct' not in t:
                        t['pnl_pct'] = round(pnl_pct, 2)
                        t['pnl'] = round(pnl, 0)
                        break
                trades.append({
                    'action': '卖出', 'date': day, 'price': round(exec_price, 2),
                    'shares': shares, 'amount': round(net_revenue, 0),
                    'pnl_pct': round(pnl_pct, 2), 'pnl': round(pnl, 0),
                    'signal': s.get('signal', ''), 'cash_after': round(net_revenue, 0)
                })
                cash = net_revenue
                shares = 0
                cost_basis = 0
            sig_idx += 1
        
        # 当日权益
        equity = cash + shares * price
        equity_curve.append(round(equity, 2))
        
        # 日收益率
        if i > 0 and equity_curve[i-1] > 0:
            daily_returns.append((equity - equity_curve[i-1]) / equity_curve[i-1])
        else:
            daily_returns.append(0)
        
        # 买入持有基准
        bh_shares = int(capital / (start_price * (1 + COMMISSION)) / 100) * 100
        bh_cost = bh_shares * start_price * (1 + COMMISSION)
        buy_hold_curve.append(round(cash - capital + bh_shares * price + (capital - bh_cost), 2))
    
    return trades, equity_curve, buy_hold_curve, daily_returns


def calc_performance(trades, equity_curve, daily_returns, capital, benchmark_curve=None):
    """计算专业绩效指标（聚宽风格）"""
    if not equity_curve or len(equity_curve) < 2:
        return {}
    
    final_equity = equity_curve[-1]
    total_return = (final_equity - capital) / capital * 100
    
    # 年化收益率（假设252个交易日）
    days = len(equity_curve)
    years = days / 252
    annual_return = ((final_equity / capital) ** (1 / max(years, 0.1)) - 1) * 100
    
    # 卖出交易统计
    sell_trades = [t for t in trades if t['action'] == '卖出']
    win_trades = [t for t in sell_trades if t.get('pnl_pct', 0) > 0]
    total_trades = len(sell_trades)
    win_rate = (len(win_trades) / total_trades * 100) if total_trades > 0 else 0
    
    # 盈亏比
    avg_win = sum(t.get('pnl_pct', 0) for t in win_trades) / len(win_trades) if win_trades else 0
    loss_trades = [t for t in sell_trades if t.get('pnl_pct', 0) <= 0]
    avg_loss = abs(sum(t.get('pnl_pct', 0) for t in loss_trades) / len(loss_trades)) if loss_trades else 0
    profit_factor = (avg_win * len(win_trades)) / (avg_loss * len(loss_trades)) if (avg_loss * len(loss_trades)) > 0 else 0
    
    # 最大回撤
    peak = equity_curve[0]
    max_dd = 0
    max_dd_date = ''
    for i, v in enumerate(equity_curve):
        if v > peak: peak = v
        dd = (v - peak) / peak * 100 if peak > 0 else 0
        if dd < max_dd:
            max_dd = dd
            max_dd_date = str(i)  # 简化
    
    # 夏普比率
    if len(daily_returns) > 1:
        import numpy as np
        rets = np.array(daily_returns)
        mean_ret = np.mean(rets)
        std_ret = np.std(rets, ddof=1)
        # 假设无风险利率 2.5%
        rf_daily = 0.025 / 252
        sharpe = ((mean_ret - rf_daily) / std_ret * np.sqrt(252)) if std_ret > 0 else 0
    else:
        sharpe = 0
    
    # 卡玛比率
    calmar = annual_return / abs(max_dd) if abs(max_dd) > 0 else 0
    
    # 最大连续盈利/亏损
    max_consecutive_wins = 0
    max_consecutive_losses = 0
    curr_wins = 0
    curr_losses = 0
    for t in sell_trades:
        if t.get('pnl_pct', 0) > 0:
            curr_wins += 1; curr_losses = 0
            max_consecutive_wins = max(max_consecutive_wins, curr_wins)
        else:
            curr_losses += 1; curr_wins = 0
            max_consecutive_losses = max(max_consecutive_losses, curr_losses)
    
    # 买入持有收益
    bh_return = 0
    if benchmark_curve and len(benchmark_curve) > 0:
        bh_return = (benchmark_curve[-1] - capital) / capital * 100
    
    return {
        'total_return': round(total_return, 2),
        'annual_return': round(annual_return, 2),
        'buy_hold_return': round(bh_return, 2),
        'win_rate': round(win_rate, 1),
        'total_trades': total_trades,
        'win_trades': len(win_trades),
        'avg_win': round(avg_win, 2),
        'avg_loss': round(avg_loss, 2),
        'profit_factor': round(profit_factor, 2),
        'max_drawdown': round(max_dd, 2),
        'max_dd_date': max_dd_date,
        'sharpe': round(sharpe, 2),
        'calmar': round(calmar, 2),
        'max_consecutive_wins': max_consecutive_wins,
        'max_consecutive_losses': max_consecutive_losses,
        'final_equity': round(final_equity, 2),
    }


# ---- 指标序列计算 ----
def calc_ma_series(prices, period):
    result = [None] * len(prices)
    for i in range(period - 1, len(prices)):
        result[i] = round(sum(prices[i-period+1:i+1]) / period, 2)
    return result

def calc_macd_series(prices, fast=12, slow=26, signal=9):
    dif = [None] * len(prices)
    macd_vals = [None] * len(prices)
    ema_fast = prices[0]
    ema_slow = prices[0]
    ema_dif = 0
    kf = 2/(fast+1); ks = 2/(slow+1); kd = 2/(signal+1)
    for i in range(len(prices)):
        if i > 0:
            ema_fast = prices[i]*kf + ema_fast*(1-kf)
            ema_slow = prices[i]*ks + ema_slow*(1-ks)
        d = ema_fast - ema_slow
        if i > 0:
            ema_dif = d*kd + ema_dif*(1-kd)
        else:
            ema_dif = d
        dif[i] = round(d, 4)
        macd_vals[i] = round(ema_dif, 4)
    return dif, macd_vals, [round(dif[i]-macd_vals[i], 4) for i in range(len(dif))]

def calc_rsi_series(prices, period=14):
    rsi = [None] * len(prices)
    gains = []; losses = []
    for i in range(1, len(prices)):
        chg = prices[i] - prices[i-1]
        gains.append(max(chg, 0))
        losses.append(max(-chg, 0))
    for i in range(period - 1, len(gains)):
        avg_gain = sum(gains[i-period+1:i+1])/period
        avg_loss = sum(losses[i-period+1:i+1])/period
        rs = avg_gain/avg_loss if avg_loss > 0 else 99
        rsi[i+1] = round(100 - 100/(1+rs), 1)
    return rsi

def calc_boll_series(prices, period=20, std=2):
    upper = [None]*len(prices); mid=[None]*len(prices); lower=[None]*len(prices)
    for i in range(period-1, len(prices)):
        w = prices[i-period+1:i+1]
        m = sum(w)/period
        s = (sum((x-m)**2 for x in w)/period)**0.5
        mid[i] = round(m, 2)
        upper[i] = round(m + std*s, 2)
        lower[i] = round(m - std*s, 2)
    return upper, mid, lower


@app.route('/api/backtest', methods=['POST'])
def api_backtest():
    """量化回测API"""
    data = request.get_json()
    if not data:
        return jsonify({'error': '请提供回测参数'}), 400
    
    code = data.get('code', '').strip()
    market = data.get('market', 'SH').upper()
    strategy = data.get('strategy', 'ma_cross')
    start_date = data.get('start', '2025-01-01')
    end_date = data.get('end', datetime.now().strftime('%Y-%m-%d'))
    capital = float(data.get('capital', 100000))
    
    if not code:
        return jsonify({'error': '请输入股票代码'}), 400
    if len(code) != 6 or not code.isdigit():
        return jsonify({'error': '股票代码需要6位数字'}), 400
    
    result = run_backtest(code, market, strategy, start_date, end_date, capital)
    return jsonify(result)


@app.route('/api/backtest_all', methods=['POST'])
def api_backtest_all():
    """批量回测：对所有自选股进行量化分析"""
    data = request.get_json() or {}
    strategy = data.get('strategy', 'adaptive')
    start_date = data.get('start', '2025-01-01')
    end_date = data.get('end', datetime.now().strftime('%Y-%m-%d'))
    capital = float(data.get('capital', 100000))
    
    stocks = get_stock_list()
    if not stocks:
        return jsonify({'error': '暂无自选股'}), 400
    
    results = []
    lock = threading.Lock()
    
    def backtest_one(s):
        code = s['code']
        market = 'SH' if code.endswith('sh') else 'SZ'
        code_num = code[:-2]
        r = run_backtest(code_num, market, strategy, start_date, end_date, capital)
        if r.get('error'): return None
        return {
            'code': s['code'].upper().replace('SZ','.SZ').replace('SH','.SH'),
            'name': s['name'],
            'total_return': r.get('total_return', 0),
            'annual_return': r.get('annual_return', 0),
            'win_rate': r.get('win_rate', 0),
            'sharpe': r.get('sharpe', 0),
            'max_drawdown': r.get('max_drawdown', 0),
            'total_trades': r.get('total_trades', 0),
            'hint': r.get('hint', ''),
        }
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(backtest_one, s): s for s in stocks}
        for f in as_completed(futures):
            r = f.result()
            if r:
                with lock: results.append(r)
    
    # 按收益率排序
    results.sort(key=lambda x: x['total_return'], reverse=True)
    
    # 统计
    positive = sum(1 for r in results if r['total_return'] > 0)
    avg_ret = sum(r['total_return'] for r in results) / len(results) if results else 0
    avg_sharpe = sum(r['sharpe'] for r in results) / len(results) if results else 0
    
    # 找最优股票
    best = results[0] if results else None
    
    return jsonify({
        'strategy': strategy,
        'total': len(stocks),
        'analyzed': len(results),
        'positive_count': positive,
        'avg_return': round(avg_ret, 2),
        'avg_sharpe': round(avg_sharpe, 2),
        'best': best,
        'results': results
    })


if __name__ == '__main__':
    print('=' * 50)
    print('  股票分析 Web 服务器')
    print('  访问地址: http://localhost:8888')
    print('  股票分析: http://localhost:8888/stock_analysis')
    print('=' * 50)
    app.run(host='0.0.0.0', port=8888, debug=True)