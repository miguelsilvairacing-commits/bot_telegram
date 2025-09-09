# bot_volume_clean.py - VERSÃO SEM ERROS
import os
import time
import math
from collections import defaultdict
from datetime import datetime, timezone
import requests
import ccxt
import numpy as np

# =========================
#   CONFIGURAÇÃO
# =========================
EXCHANGES = os.getenv("EXCHANGES", "binance,bingx").split(",")
QUOTE_FILTER = os.getenv("QUOTE_FILTER", "USDT").split(",")

QV24H_MIN_USD = float(os.getenv("QV24H_MIN_USD", "200000"))
QV24H_MAX_USD = float(os.getenv("QV24H_MAX_USD", "20000000"))

SYMBOLS_BLACKLIST = set([
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "ADA/USDT", "SOL/USDT", 
    "DOT/USDT", "AVAX/USDT", "MATIC/USDT", "LINK/USDT", "UNI/USDT",
    "LTC/USDT", "BCH/USDT", "XRP/USDT", "DOGE/USDT", "ATOM/USDT"
] + [s.strip() for s in os.getenv("SYMBOLS_BLACKLIST", "").split(",") if s.strip()])

TOP_N_BY_VOLUME = int(os.getenv("TOP_N_BY_VOLUME", "30"))
TIMEFRAME = os.getenv("TIMEFRAME", "1m")
LOOKBACK = int(os.getenv("LOOKBACK", "8"))
THRESHOLD = float(os.getenv("THRESHOLD", "3.0"))

MIN_RISK_SCORE = int(os.getenv("MIN_RISK_SCORE", "4"))
MIN_PRICE_CHANGE = float(os.getenv("MIN_PRICE_CHANGE", "0.04"))
HIGH_CONFIDENCE_ONLY = os.getenv("HIGH_CONFIDENCE_ONLY", "false").lower() == "true"

RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
RSI_OVERBOUGHT = float(os.getenv("RSI_OVERBOUGHT", "80"))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "20"))

SH_WICK_BODY_RATIO = float(os.getenv("SH_WICK_BODY_RATIO", "2.0"))
SH_WICK_RANGE_PCT = float(os.getenv("SH_WICK_RANGE_PCT", "0.5"))
SH_MIN_RETRACE_PCT = float(os.getenv("SH_MIN_RETRACE_PCT", "0.4"))
SH_USE_VOLUME = os.getenv("SH_USE_VOLUME", "true").lower() == "true"
SH_VOL_MULT = float(os.getenv("SH_VOL_MULT", "3.0"))

SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "30"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "25"))

ALERT_HOURS_STR = os.getenv("ALERT_HOURS", "")
ALERT_HOURS = [int(x.strip()) for x in ALERT_HOURS_STR.split(",") if x.strip()] if ALERT_HOURS_STR else []
WEEKEND_QUIET_MODE = os.getenv("WEEKEND_QUIET_MODE", "false").lower() == "true"

DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"
TG_TOKEN = os.getenv("TG_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

# =========================
#   UTILITÁRIOS
# =========================
def send_telegram(msg: str):
    if not TG_TOKEN or not TG_CHAT_ID:
        print("[Telegram] Não configurado. Mensagem seria:")
        print(msg)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=20
        )
    except Exception as e:
        print(f"[Telegram] Falha: {e}")

def short_num(x: float) -> str:
    if x is None or math.isnan(x):
        return "-"
    for unit in ["","K","M","B","T","P"]:
        if abs(x) < 1000.0:
            return f"{x:,.2f}{unit}"
        x /= 1000.0
    return f"{x:.2f}E"

def build_exchange(name: str):
    name = name.strip().lower()
    if not hasattr(ccxt, name):
        raise ValueError(f"Exchange '{name}' não existe no CCXT.")
    
    config = {
        "enableRateLimit": True,
        "options": {"adjustForTimeDifference": True}
    }
    
    if name == "bingx":
        config.update({
            "timeout": 30000,
            "rateLimit": 2000,
            "options": {
                "adjustForTimeDifference": True,
                "recvWindow": 60000,
            }
        })
        print("[BingX] Configuração específica aplicada")
        
    ex = getattr(ccxt, name)(config)
    
    try:
        print(f"[{name}] Carregando mercados...")
        ex.load_markets()
        market_count = len(ex.markets) if hasattr(ex, 'markets') else 0
        print(f"[{name}] ✅ {market_count} mercados carregados")
    except Exception as e:
        print(f"[{name}] ❌ Erro: {e}")
        raise
    
    return ex

def safe_rsi_format(rsi_value):
    if rsi_value is None:
        return "N/A"
    try:
        return f"{float(rsi_value):.1f}"
    except:
        return "N/A"

def calculate_rsi(prices, period=14):
    try:
        if not prices or len(prices) < period + 1:
            return None
        
        clean_prices = []
        for p in prices[-(period+1):]:
            try:
                clean_prices.append(float(p))
            except:
                continue
        
        if len(clean_prices) < period + 1:
            return None
        
        prices_array = np.array(clean_prices)
        deltas = np.diff(prices_array)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return float(rsi)
        
    except Exception as e:
        if DEBUG_MODE:
            print(f"[DEBUG] Erro RSI: {e}")
        return None

def is_alert_hour():
    if not ALERT_HOURS:
        return True
    
    utc_now = datetime.now(timezone.utc)
    current_hour = utc_now.hour
    
    if WEEKEND_QUIET_MODE and utc_now.weekday() >= 5:
        return current_hour in [8, 9, 14, 15, 22]
    
    return current_hour in ALERT_HOURS

# =========================
#   FETCH OHLCV
# =========================
def fetch_ohlcv_safe(ex, symbol, timeframe, limit):
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            if ex.id.lower() == 'bingx':
                actual_limit = min(limit, 100)
                result = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=actual_limit)
                return result
            else:
                return ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                
        except ccxt.NetworkError as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 3
                if DEBUG_MODE:
                    print(f"[DEBUG] {ex.id} {symbol}: Network retry {attempt + 1} in {wait_time}s")
                time.sleep(wait_time)
                continue
            else:
                return None
                
        except ccxt.ExchangeError as e:
            error_str = str(e).lower()
            if any(x in error_str for x in ["invalid symbol", "not found"]):
                return None
            elif "rate limit" in error_str:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    time.sleep(wait_time)
                    continue
                else:
                    return None
            else:
                return None
                
        except Exception as e:
            if DEBUG_MODE:
                print(f"[DEBUG] {ex.id} {symbol}: Error - {e}")
            return None
    
    return None

# =========================
#   SELEÇÃO DE SÍMBOLOS
# =========================
def pick_symbols_by_24h_volume(ex, top_n=30, quotes=None):
    if quotes is None:
        quotes = QUOTE_FILTER
        
    exchange_name = ex.id.lower()
    
    try:
        markets = ex.load_markets()
        active_markets = {k: v for k, v in markets.items() if v.get('active', True)}
            
        try:
            print(f"[{exchange_name}] Buscando tickers...")
            tickers = ex.fetch_tickers()
            print(f"[{exchange_name}] ✅ {len(tickers)} tickers obtidos")
        except Exception as e:
            print(f"[{exchange_name}] ❌ Erro tickers: {e}")
            
            symbols = []
            for s, m in active_markets.items():
                if any(s.endswith("/" + q) for q in quotes) and s not in SYMBOLS_BLACKLIST:
                    symbols.append(s)
            
            return symbols[:top_n]

        rows = []
        for sym, t in tickers.items():
            if not any(sym.endswith("/" + q) for q in quotes):
                continue
            if sym in SYMBOLS_BLACKLIST:
                continue
            if sym not in active_markets:
                continue

            vol_q = None
            if exchange_name == 'bingx':
                vol_q = (t.get("quoteVolume") or t.get("info", {}).get("volume"))
            else:
                vol_q = t.get("quoteVolume")
            
            try:
                vol_q = float(vol_q) if vol_q is not None else None
            except:
                vol_q = None
                
            if vol_q is None or vol_q <= 0:
                continue

            if vol_q < QV24H_MIN_USD or vol_q > QV24H_MAX_USD:
                continue

            rows.append((sym, vol_q))

        rows.sort(key=lambda x: x[1], reverse=True)
        selected = [sym for sym, _ in rows[:top_n]]
        
        print(f"[{exchange_name}] ✅ {len(selected)} pares selecionados")
        
        if DEBUG_MODE and selected:
            print(f"[DEBUG] {exchange_name}: Top 5: {selected[:5]}")
        
        return selected
        
    except Exception as e:
        print(f"[ERROR] {exchange_name}: Erro seleção - {e}")
        return []

# =========================
#   DETECÇÃO
# =========================
def detect_pump_pattern(ohlcv):
    if len(ohlcv) < 8:
        return False, None, {}
    
    last_8 = ohlcv[-8:]
    volumes = [c[5] for c in last_8]
    prices = [c[4] for c in last_8]
    
    recent_vol = sum(volumes[-3:])
    early_vol = sum(volumes[:3])
    vol_trend = recent_vol / early_vol if early_vol > 0 else 0
    
    price_change = (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0
    
    if vol_trend > 3 and price_change > 0.06:
        return True, "ACCUMULATION_PUMP", {
            "vol_trend": vol_trend,
            "price_change": price_change,
            "confidence": "HIGH" if vol_trend > 5 else "MEDIUM"
        }
    
    max_vol = max(volumes)
    avg_vol = sum(volumes[:-1]) / len(volumes[:-1]) if len(volumes) > 1 else 0
    
    if avg_vol > 0 and max_vol > 6 * avg_vol and price_change > 0.04:
        return True, "COORDINATED_PUMP", {
            "vol_spike": max_vol / avg_vol,
            "price_change": price_change,
            "confidence": "HIGH"
        }
    
    return False, None, {}

def calculate_risk_score(volume_mult, price_change, rsi, market_cap_usd):
    score = 0
    
    if volume_mult > 15: score += 4
    elif volume_mult > 10: score += 3
    elif volume_mult > 6: score += 2
    elif volume_mult > 3: score += 1
    
    abs_change = abs(price_change)
    if abs_change > 0.20: score += 3
    elif abs_change > 0.12: score += 2
    elif abs_change > 0.06: score += 1
    
    if rsi is not None:
        try:
            rsi_val = float(rsi)
            if price_change > 0:
                if 50 <= rsi_val <= 75: score += 2
                elif 40 <= rsi_val <= 80: score += 1
            else:
                if rsi_val < 30: score += 2
                elif rsi_val < 45: score += 1
        except:
            pass
    
    if 500_000 <= market_cap_usd <= 15_000_000: score += 1
    
    return min(score, 10)

def get_pump_stage(rsi, price_change_pct, volume_mult):
    if rsi is None:
        return "UNKNOWN"
    
    try:
        rsi_val = float(rsi)
        abs_change = abs(price_change_pct)
        
        if rsi_val < 60 and abs_change < 12 and volume_mult > 4:
            return "EARLY_PUMP"
        elif 60 <= rsi_val <= 78 and abs_change < 20:
            return "MID_PUMP"
        elif rsi_val > 78 or abs_change > 25:
            return "LATE_PUMP"
        elif rsi_val < 35 and price_change_pct < -8:
            return "DUMP_PHASE"
        else:
            return "UNKNOWN"
    except:
        return "UNKNOWN"

def candle_parts(c):
    o, h, l, cl, v = c[1], c[2], c[3], c[4], c[5]
    rng = max(1e-12, h - l)
    body = abs(cl - o)
    upper = h - max(o, cl)
    lower = min(o, cl) - l
    return o, h, l, cl, v, rng, body, upper, lower

def detect_stop_hunt(ohlcv):
    if len(ohlcv) < max(LOOKBACK, 3):
        return False, None, {}

    last = ohlcv[-1]
    o, h, l, cl, v, rng, body, upper, lower = candle_parts(last)

    vols = [c[5] for c in ohlcv[-(LOOKBACK+1):-1]]
    vol_avg = sum(vols)/len(vols) if vols else 0.0
    vol_ok = (not SH_USE_VOLUME) or (vol_avg > 0 and v >= SH_VOL_MULT * vol_avg)

    retrace_from_low = (cl - l) / rng if rng > 0 else 0.0
    retrace_from_high = (h - cl) / rng if rng > 0 else 0.0

    cond_down = (lower >= SH_WICK_BODY_RATIO * body) and (lower >= SH_WICK_RANGE_PCT * rng)
    cond_up = (upper >= SH_WICK_BODY_RATIO * body) and (upper >= SH_WICK_RANGE_PCT * rng)

    found = False
    side = None

    if cond_down and (retrace_from_low >= SH_MIN_RETRACE_PCT) and vol_ok:
        found, side = True, "down"
    if cond_up and (retrace_from_high >= SH_MIN_RETRACE_PCT) and vol_ok:
        found, side = True, "up"

    info = {
        "vol_mult": (v/vol_avg) if vol_avg else None,
        "side": side
    }
    return found, side, info

# =========================
#   ALERTAS
# =========================
last_alert_ts = defaultdict(lambda: 0.0)

def can_alert(key: str, now_ts: float) -> bool:
    last = last_alert_ts[key]
    if (now_ts - last) >= COOLDOWN_MINUTES * 60:
        last_alert_ts[key] = now_ts
        return True
    return False

def should_send_alert(risk_score, confidence):
    if risk_score < MIN_RISK_SCORE:
        return False
    
    if HIGH_CONFIDENCE_ONLY and confidence != "HIGH":
        return False
    
    if ALERT_HOURS and not is_alert_hour():
        return False
    
    return True

def send_enhanced_alert(symbol, alert_type, data, exchange_name):
    risk_score = data.get('risk_score', 0)
    confidence = data.get('confidence', 'MEDIUM')
    
    if not should_send_alert(risk_score, confidence):
        return
    
    price_change = data.get('price_change', 0)
    volume_mult = data.get('volume_multiple', 0)
    rsi = data.get('rsi')
    pump_stage = data.get('pump_stage', alert_type)
    
    exchange_emojis = {
        'binance': '🟡',
        'bingx': '🔵'
    }
    
    stage_emojis = {
        'EARLY_PUMP': '🚀',
        'MID_PUMP': '⚡',
        'LATE_PUMP': '⚠️',
        'DUMP_PHASE': '🔴',
        'VOLUME_SPIKE': '💥',
        'STOP_HUNT': '🩸'
    }
    
    exchange_emoji = exchange_emojis.get(exchange_name.lower(), '⚪')
    stage_emoji = stage_emojis.get(pump_stage, '⚠️')
    
    rsi_text = safe_rsi_format(rsi)
    
    msg = f"""{stage_emoji} <b>{exchange_name.upper()} SIGNAL</b> {exchange_emoji}
━━━━━━━━━━━━━━━━━━━━━━━━━━

<b>Par:</b> {symbol}
<b>Stage:</b> {pump_stage}
<b>Quality:</b> {risk_score}/10 ({confidence})

<b>📊 Metrics:</b>
• Price: {price_change:+.1f}%
• Volume: {volume_mult:.1f}x
• RSI: {rsi_text}
• Time: {datetime.now(timezone.utc).strftime('%H:%M UTC')}"""

    if pump_stage == 'EARLY_PUMP':
        msg += "\n\n<b>💡 Action:</b> 🟢 ENTRY ZONE\n• Target: +20-35%\n• Stop: -6%"
    elif pump_stage == 'MID_PUMP':
        msg += "\n\n<b>💡 Action:</b> 🟡 MONITOR\n• Partial profit\n• Trail stops"
    elif pump_stage == 'LATE_PUMP':
        msg += "\n\n<b>💡 Action:</b> 🔴 EXIT WARNING\n• Prepare exits"
    elif pump_stage == 'DUMP_PHASE':
        msg += "\n\n<b>💡 Action:</b> 🔴 DUMP ACTIVE\n• Exit now"
    
    if exchange_name.lower() == 'binance':
        binance_symbol = symbol.replace('/', '')
        msg += f"\n\n<b>🔗 Trade:</b> binance.com/trade/{binance_symbol}"
    elif exchange_name.lower() == 'bingx':
        bingx_symbol = symbol.replace('/', '-')
        msg += f"\n\n<b>🔗 Trade:</b> bingx.com/spot/{bingx_symbol}"
    
    send_telegram(msg)

# =========================
#   MAIN
# =========================
def main():
    print("🎯 Bot Multi-Exchange Limpo!")
    
    exchanges = {}
    watchlist = {}

    for name in EXCHANGES:
        name = name.strip()
        if not name:
            continue
        try:
            print(f"\n[{name.upper()}] Inicializando...")
            ex = build_exchange(name)
            exchanges[name] = ex
            
            syms = pick_symbols_by_24h_volume(ex, TOP_N_BY_VOLUME, QUOTE_FILTER)
            watchlist[name] = syms
            
            if syms:
                print(f"[{name.upper()}] ✅ {len(syms)} pares")
            else:
                print(f"[{name.upper()}] ⚠️ Nenhum par")
                
        except Exception as e:
            print(f"[{name.upper()}] ❌ Falha: {e}")
            continue

    if not exchanges:
        raise SystemExit("❌ Nenhuma exchange inicializada.")

    active_exchanges = list(exchanges.keys())
    total_pairs = sum(len(watchlist.get(ex, [])) for ex in active_exchanges)
    
    print(f"\n🚀 INICIALIZAÇÃO COMPLETA!")
    print(f"🏦 Exchanges: {', '.join(active_exchanges)}")
    print(f"📊 Total pares: {total_pairs}")
    
    send_telegram(f"✅ Bot Online!\n🏦 Exchanges: {', '.join(active_exchanges)}\n📊 Pares: {total_pairs}\n🎯 Filtros: Risk≥{MIN_RISK_SCORE}, Price≥{MIN_PRICE_CHANGE*100:.0f}%")

    print("\n🔄 LOOP INICIADO...")

    while True:
        loop_start = time.time()
        
        for exchange_name, ex in exchanges.items():
            symbols = watchlist.get(exchange_name, [])
            
            if not symbols:
                continue
            
            for sym in symbols:
                try:
                    ohlcv = fetch_ohlcv_safe(ex, sym, TIMEFRAME, limit=LOOKBACK + 15)
                    if not ohlcv or len(ohlcv) < LOOKBACK + 1:
                        continue

                    *hist, last = ohlcv
                    volumes = [c[5] for c in hist[-LOOKBACK:]]
                    vol_avg = (sum(volumes) / len(volumes)) if volumes else 0.0
                    vol_last = last[5]
                    close_last = last[4]

                    vol_multiple = vol_last / vol_avg if vol_avg > 0 else 0
                    
                    price_change = 0
                    if len(hist) > 0:
                        prev_close = hist[-1][4]
                        price_change = (close_last - prev_close) / prev_close if prev_close > 0 else 0

                    if abs(price_change) < MIN_PRICE_CHANGE:
                        continue

                    prices = [c[4] for c in ohlcv]
                    rsi = calculate_rsi(prices, RSI_PERIOD)
                    market_cap_est = close_last * 1_000_000

                    risk_score = calculate_risk_score(vol_multiple, price_change, rsi, market_cap_est)
                    
                    if risk_score < MIN_RISK_SCORE:
                        continue

                    pump_stage = get_pump_stage(rsi, price_change * 100, vol_multiple)

                    if DEBUG_MODE and vol_multiple > 2:
                        rsi_display = safe_rsi_format(rsi)
                        print(f"[DEBUG] {exchange_name} {sym}: {vol_multiple:.1f}x, {price_change:+.1f}%, RSI {rsi_display}, Risk {risk_score}, {pump_stage}")

                    # VOLUME SPIKE
                    if vol_avg > 0 and vol_multiple >= THRESHOLD:
                        key = f"SPIKE:{exchange_name}:{sym}"
                        if can_alert(key, time.time()):
                            send_enhanced_alert(sym, "VOLUME_SPIKE", {
                                "volume_multiple": vol_multiple,
                                "price_change": price_change * 100,
                                "risk_score": risk_score,
                                "rsi": rsi,
                                "pump_stage": pump_stage,
                                "confidence": "HIGH" if vol_multiple > 10 else "MEDIUM"
                            }, exchange_name)

                    # PUMP PATTERNS
                    pump_found, pump_type, pump_data = detect_pump_pattern(ohlcv)
                    if pump_found:
                        key = f"PUMP:{pump_type}:{exchange_name}:{sym}"
                        if can_alert(key, time.time()):
                            send_enhanced_alert(sym, pump_type, {
                                "volume_multiple": vol_multiple,
                                "price_change": price_change * 100,
                                "risk_score": risk_score,
                                "rsi": rsi,
                                "pump_stage": pump_stage,
                                "confidence": pump_data.get("confidence", "MEDIUM")
                            }, exchange_name)

                    # RSI EXTREMES
                    if rsi is not None:
                        try:
                            rsi_val = float(rsi)
                            
                            if rsi_val > RSI_OVERBOUGHT and vol_multiple > 2:
                                key = f"DUMP:{exchange_name}:{sym}"
                                if can_alert(key, time.time()):
                                    send_enhanced_alert(sym, "DUMP_WARNING", {
                                        "volume_multiple": vol_multiple,
                                        "price_change": price_change * 100,
                                        "risk_score": risk_score,
                                        "rsi": rsi,
                                        "pump_stage": "DUMP_WARNING",
                                        "confidence": "HIGH"
                                    }, exchange_name)
                            
                            elif rsi_val < RSI_OVERSOLD and vol_multiple > 2 and price_change < -0.05:
                                key = f"OVERSOLD:{exchange_name}:{sym}"
                                if can_alert(key, time.time()):
                                    send_enhanced_alert(sym, "OVERSOLD_BOUNCE", {
                                        "volume_multiple": vol_multiple,
                                        "price_change": price_change * 100,
                                        "risk_score": risk_score,
                                        "rsi": rsi,
                                        "pump_stage": "OVERSOLD_BOUNCE",
                                        "confidence": "MEDIUM"
                                    }, exchange_name)
                        except:
                            pass

                    # STOP HUNTING
                    sh_found, sh_side, sh = detect_stop_hunt(ohlcv)
                    if sh_found:
                        key = f"STOPHUNT:{sh_side}:{exchange_name}:{sym}"
                        if can_alert(key, time.time()):
                            sh_risk = calculate_risk_score(sh.get('vol_mult', 0), price_change, rsi, market_cap_est)
                            if sh_risk >= MIN_RISK_SCORE:
                                send_enhanced_alert(sym, "STOP_HUNT", {
                                    "volume_multiple": sh.get('vol_mult', 0),
                                    "price_change": price_change * 100,
                                    "risk_score": sh_risk,
                                    "rsi": rsi,
                                    "pump_stage": "STOP_HUNT",
                                    "confidence": "MEDIUM"
                                }, exchange_name)

                except ccxt.NetworkError:
                    continue
                except ccxt.ExchangeError as e:
                    if "rate limit" in str(e).lower():
                        time.sleep(2)
                    continue
                except Exception as e:
                    if DEBUG_MODE:
                        print(f"[ERROR] {exchange_name} {sym}: {e}")
                    continue

        elapsed = time.time() - loop_start
        sleep_time = max(0, SLEEP_SECONDS - elapsed)
        time.sleep(sleep_time)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Bot encerrado.")
        send_telegram("👋 Bot encerrado pelo utilizador.")
    except Exception as e:
        error_msg = f"❌ Bot crashed: {e}"
        print(error_msg)
        send_telegram(error_msg)
