# bot_volume_with_filters.py
import os
import time
import math
from collections import defaultdict
from datetime import datetime, timezone
import requests
import ccxt
import numpy as np

# =========================
#   CONFIGURAÇÃO AVANÇADA
# =========================
EXCHANGES = os.getenv("EXCHANGES", "binance").split(",")
QUOTE_FILTER = os.getenv("QUOTE_FILTER", "USDT,BTC").split(",")

# Market cap filtering
QV24H_MIN_USD = float(os.getenv("QV24H_MIN_USD", "2000000"))      # $2M mínimo
QV24H_MAX_USD = float(os.getenv("QV24H_MAX_USD", "20000000"))     # $20M máximo

# Blacklist
SYMBOLS_BLACKLIST = set([
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "ADA/USDT", "SOL/USDT", 
    "DOT/USDT", "AVAX/USDT", "MATIC/USDT", "LINK/USDT", "UNI/USDT",
    "LTC/USDT", "BCH/USDT", "XRP/USDT", "DOGE/USDT", "ATOM/USDT"
] + [s.strip() for s in os.getenv("SYMBOLS_BLACKLIST", "").split(",") if s.strip()])

TOP_N_BY_VOLUME = int(os.getenv("TOP_N_BY_VOLUME", "30"))

# Detection thresholds
TIMEFRAME = os.getenv("TIMEFRAME", "1m")
LOOKBACK = int(os.getenv("LOOKBACK", "10"))
THRESHOLD = float(os.getenv("THRESHOLD", "5.0"))                 # Volume threshold

# 🚨 NOVOS FILTROS DE QUALIDADE
MIN_RISK_SCORE = int(os.getenv("MIN_RISK_SCORE", "6"))           # ⭐ NOVO: Só alertas com risk score 6+
MIN_PRICE_CHANGE = float(os.getenv("MIN_PRICE_CHANGE", "0.08"))  # ⭐ NOVO: Mínimo 8% price movement
HIGH_CONFIDENCE_ONLY = os.getenv("HIGH_CONFIDENCE_ONLY", "false").lower() == "true"  # ⭐ NOVO

# RSI settings
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
RSI_OVERBOUGHT = float(os.getenv("RSI_OVERBOUGHT", "80"))        # ⭐ NOVO: RSI threshold para dump warning
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "20"))            # ⭐ NOVO

# Stop hunting (mais restritivo)
SH_WICK_BODY_RATIO = float(os.getenv("SH_WICK_BODY_RATIO", "2.0"))  # Mais restritivo
SH_WICK_RANGE_PCT = float(os.getenv("SH_WICK_RANGE_PCT", "0.5"))    
SH_MIN_RETRACE_PCT = float(os.getenv("SH_MIN_RETRACE_PCT", "0.4"))   
SH_USE_VOLUME = os.getenv("SH_USE_VOLUME", "true").lower() == "true"
SH_VOL_MULT = float(os.getenv("SH_VOL_MULT", "3.0"))               # Mais restritivo

# Timing
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "20"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "30"))

# 🕐 NOVO: Filtros de horário
ALERT_HOURS = [int(x.strip()) for x in os.getenv("ALERT_HOURS", "0,1,2,8,9,10,14,15,16,22,23").split(",") if x.strip()]
WEEKEND_QUIET_MODE = os.getenv("WEEKEND_QUIET_MODE", "false").lower() == "true"

# Debug
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"

# Telegram
TG_TOKEN = os.getenv("TG_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

# =========================
#   UTILITÁRIOS
# =========================
def send_telegram(msg: str):
    """Envia texto para o Telegram (HTML permitido)."""
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
        print(f"[Telegram] Falha ao enviar: {e}")

def ts_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

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
    ex = getattr(ccxt, name)({
        "enableRateLimit": True,
        "options": {"adjustForTimeDifference": True}
    })
    ex.load_markets()
    return ex

def safe_rsi_format(rsi_value):
    """Formatação segura do RSI"""
    if rsi_value is None:
        return "N/A"
    try:
        return f"{float(rsi_value):.1f}"
    except:
        return "N/A"

def calculate_rsi(prices, period=14):
    """Calcula RSI"""
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

# 🕐 NOVO: Verificações de timing
def is_alert_hour():
    """Verifica se está num horário válido para alertas"""
    if not ALERT_HOURS:
        return True
    
    utc_now = datetime.now(timezone.utc)
    current_hour = utc_now.hour
    
    # Weekend quiet mode
    if WEEKEND_QUIET_MODE and utc_now.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return current_hour in [8, 9, 14, 15, 22]  # Só horários prime no fim de semana
    
    return current_hour in ALERT_HOURS

def is_manipulation_hour():
    """Horários de maior atividade suspeita"""
    utc_now = datetime.now(timezone.utc)
    hour = utc_now.hour
    high_activity_hours = [0, 1, 2, 8, 9, 10, 14, 15, 16, 22, 23]
    return hour in high_activity_hours

# =========================
#   FILTROS DE SYMBOLS
# =========================
def pick_symbols_by_24h_volume(ex, top_n=30, quotes=None):
    """Seleção de símbolos otimizada"""
    if quotes is None:
        quotes = QUOTE_FILTER
        
    markets = ex.load_markets()
    try:
        tickers = ex.fetch_tickers()
    except Exception as e:
        print(f"[ERROR] Falha ao buscar tickers: {e}")
        symbols = [s for s, m in markets.items() if m.get("active")]
        symbols = [s for s in symbols if any(s.endswith("/" + q) for q in quotes)]
        symbols = [s for s in symbols if s not in SYMBOLS_BLACKLIST]
        return symbols[:top_n]

    rows = []
    for sym, t in tickers.items():
        if not any(sym.endswith("/" + q) for q in quotes):
            continue
            
        if sym in SYMBOLS_BLACKLIST:
            continue

        vol_q = t.get("quoteVolume") or (t.get("info") or {}).get("quoteVolume")
        try:
            vol_q = float(vol_q) if vol_q is not None else None
        except Exception:
            vol_q = None
        if vol_q is None:
            continue

        if vol_q < QV24H_MIN_USD or vol_q > QV24H_MAX_USD:
            continue

        rows.append((sym, vol_q))

    rows.sort(key=lambda x: x[1], reverse=True)
    selected = [sym for sym, _ in rows[:top_n]]
    
    if DEBUG_MODE:
        print(f"[DEBUG] Selecionados {len(selected)} pares: {selected[:8]}...")
    
    return selected

def fetch_ohlcv_safe(ex, symbol, timeframe, limit):
    try:
        return ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        if DEBUG_MODE:
            print(f"[DEBUG] Erro OHLCV {symbol}: {e}")
        return None

# =========================
#   DETECÇÃO MELHORADA
# =========================
def detect_pump_pattern(ohlcv):
    """Detecta padrões de pump"""
    if len(ohlcv) < 10:
        return False, None, {}
    
    last_10 = ohlcv[-10:]
    volumes = [c[5] for c in last_10]
    prices = [c[4] for c in last_10]
    
    # Padrão accumulation
    vol_trend = sum(volumes[-3:]) / sum(volumes[:3]) if sum(volumes[:3]) > 0 else 0
    price_change = (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0
    
    if vol_trend > 4 and price_change > 0.08:  # Mais restritivo
        return True, "ACCUMULATION_PUMP", {
            "vol_trend": vol_trend,
            "price_change": price_change,
            "confidence": "HIGH" if vol_trend > 6 else "MEDIUM"
        }
    
    # Padrão coordinated
    max_vol = max(volumes)
    avg_vol = sum(volumes[:-1]) / len(volumes[:-1]) if len(volumes) > 1 else 0
    
    if avg_vol > 0 and max_vol > 8 * avg_vol and price_change > 0.05:  # Mais restritivo
        return True, "COORDINATED_PUMP", {
            "vol_spike": max_vol / avg_vol,
            "price_change": price_change,
            "confidence": "HIGH"
        }
    
    return False, None, {}

def calculate_risk_score(volume_mult, price_change, rsi, market_cap_usd):
    """🚨 RISK SCORE MELHORADO (1-10)"""
    score = 0
    
    # Volume severity (0-4)
    if volume_mult > 20: score += 4
    elif volume_mult > 15: score += 3
    elif volume_mult > 10: score += 2
    elif volume_mult > 5: score += 1
    
    # Price movement (0-3)
    abs_change = abs(price_change)
    if abs_change > 0.25: score += 3      # >25%
    elif abs_change > 0.15: score += 2    # >15%
    elif abs_change > 0.08: score += 1    # >8%
    
    # RSI context (0-2)
    if rsi is not None:
        try:
            rsi_val = float(rsi)
            # Para pumps: RSI muito alto = risco de dump
            # Para dumps: RSI muito baixo = oportunidade
            if price_change > 0:  # Pump
                if 60 <= rsi_val <= 75: score += 2  # Sweet spot
                elif 50 <= rsi_val <= 85: score += 1
            else:  # Dump
                if rsi_val < 25: score += 2  # Oversold opportunity
                elif rsi_val < 40: score += 1
        except:
            pass
    
    # Market cap sweet spot (0-1)
    if 2_000_000 <= market_cap_usd <= 15_000_000: score += 1
    
    return min(score, 10)

def get_pump_stage(rsi, price_change_pct, volume_mult):
    """🎯 NOVO: Determina o estágio do pump"""
    if rsi is None:
        return "UNKNOWN"
    
    try:
        rsi_val = float(rsi)
        abs_change = abs(price_change_pct)
        
        if rsi_val < 60 and abs_change < 15 and volume_mult > 5:
            return "EARLY_PUMP"      # Bom para entry
        elif 60 <= rsi_val <= 80 and abs_change < 25:
            return "MID_PUMP"        # Monitor closely
        elif rsi_val > 80 or abs_change > 30:
            return "LATE_PUMP"       # Warning - prepare exit
        elif rsi_val < 30 and price_change_pct < -10:
            return "DUMP_PHASE"      # Exit / Short opportunity
        else:
            return "UNKNOWN"
    except:
        return "UNKNOWN"

# =========================
#   STOP HUNTING
# =========================
def candle_parts(c):
    o, h, l, cl, v = c[1], c[2], c[3], c[4], c[5]
    rng = max(1e-12, h - l)
    body = abs(cl - o)
    upper = h - max(o, cl)
    lower = min(o, cl) - l
    return o, h, l, cl, v, rng, body, upper, lower

def detect_stop_hunt(ohlcv):
    """Stop hunting detection (mais restritivo)"""
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
#   ALERTAS COM FILTROS
# =========================
last_alert_ts = defaultdict(lambda: 0.0)

def can_alert(key: str, now_ts: float) -> bool:
    last = last_alert_ts[key]
    if (now_ts - last) >= COOLDOWN_MINUTES * 60:
        last_alert_ts[key] = now_ts
        return True
    return False

def should_send_alert(risk_score, confidence):
    """🚨 NOVO: Filtros de qualidade antes de enviar alert"""
    
    # Risk score filter
    if risk_score < MIN_RISK_SCORE:
        if DEBUG_MODE:
            print(f"[FILTER] Risk score {risk_score} < {MIN_RISK_SCORE} - skipped")
        return False
    
    # Confidence filter
    if HIGH_CONFIDENCE_ONLY and confidence != "HIGH":
        if DEBUG_MODE:
            print(f"[FILTER] Confidence {confidence} não é HIGH - skipped")
        return False
    
    # Timing filter
    if not is_alert_hour():
        if DEBUG_MODE:
            print(f"[FILTER] Fora do horário de alertas - skipped")
        return False
    
    return True

def send_enhanced_alert(symbol, alert_type, data):
    """🚨 ALERTAS MELHORADOS com staging e targets"""
    
    risk_score = data.get('risk_score', 0)
    confidence = data.get('confidence', 'MEDIUM')
    
    # Aplicar filtros de qualidade
    if not should_send_alert(risk_score, confidence):
        return
    
    price_change = data.get('price_change', 0)
    volume_mult = data.get('volume_multiple', 0)
    rsi = data.get('rsi')
    pump_stage = data.get('pump_stage', 'UNKNOWN')
    
    # Emojis e cores por stage
    stage_emojis = {
        'EARLY_PUMP': '🚀',
        'MID_PUMP': '⚡',
        'LATE_PUMP': '⚠️',
        'DUMP_PHASE': '🔴',
        'VOLUME_SPIKE': '💥',
        'STOP_HUNT': '🩸'
    }
    
    emoji = stage_emojis.get(pump_stage, stage_emojis.get(alert_type, '⚠️'))
    
    # Formatação segura
    rsi_text = safe_rsi_format(rsi)
    
    msg = f"""{emoji} <b>BINANCE SIGNAL</b>
━━━━━━━━━━━━━━━━━━━━━━

<b>Par:</b> {symbol}
<b>Stage:</b> {pump_stage or alert_type}
<b>Quality:</b> {risk_score}/10 ({confidence})

<b>📊 Metrics:</b>
• Price: {price_change:+.1f}%
• Volume: {volume_mult:.1f}x
• RSI: {rsi_text}
• Time: {datetime.now(timezone.utc).strftime('%H:%M UTC')}"""

    # Recomendações por stage
    if pump_stage == 'EARLY_PUMP':
        msg += f"\n\n<b>💡 Action:</b> 🟢 ENTRY ZONE\n• Target: +25-40%\n• Stop: -8%"
    elif pump_stage == 'MID_PUMP':
        msg += f"\n\n<b>💡 Action:</b> 🟡 MONITOR\n• Take some profit\n• Trail stop"
    elif pump_stage == 'LATE_PUMP':
        msg += f"\n\n<b>💡 Action:</b> 🔴 EXIT WARNING\n• Prepare to exit\n• Very risky entry"
    elif pump_stage == 'DUMP_PHASE':
        msg += f"\n\n<b>💡 Action:</b> 🔴 DUMP ACTIVE\n• Exit immediately\n• Short opportunity"
    
    # Link
    binance_symbol = symbol.replace('/', '')
    msg += f"\n\n<b>🔗 Trade:</b> binance.com/trade/{binance_symbol}"
    
    send_telegram(msg)

# =========================
#   MAIN LOOP
# =========================
def main():
    print("🎯 Bot CONCLUSIVO para Pumps/Dumps - Quality over Quantity!")
    print(f"📊 Filtros ativos: Risk Score ≥{MIN_RISK_SCORE}, Price Change ≥{MIN_PRICE_CHANGE*100:.0f}%")
    
    exchanges = {}
    watchlist = {}

    for name in EXCHANGES:
        name = name.strip()
        if not name:
            continue
        try:
            ex = build_exchange(name)
            exchanges[name] = ex
            syms = pick_symbols_by_24h_volume(ex, TOP_N_BY_VOLUME, QUOTE_FILTER)
            watchlist[name] = syms
            print(f"[{name}] 📊 Monitorizando {len(syms)} pares de qualidade")
        except Exception as e:
            print(f"[{name}] ❌ Falha: {e}")

    if not exchanges:
        raise SystemExit("❌ Nenhuma exchange configurada.")

    send_telegram(f"✅ Bot CONCLUSIVO iniciado!\n🎯 Target: 10-20 alertas/dia\n📊 Min Risk Score: {MIN_RISK_SCORE}/10\n💥 Min Price Change: {MIN_PRICE_CHANGE*100:.0f}%")

    print(f"🔄 Loop: scan a cada {SLEEP_SECONDS}s, cooldown {COOLDOWN_MINUTES}min")

    while True:
        loop_start = time.time()
        
        for name, ex in exchanges.items():
            symbols = watchlist.get(name, [])
            
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

                    # 🚨 FILTRO DE PRICE CHANGE
                    if abs(price_change) < MIN_PRICE_CHANGE:
                        continue  # Skip se movimento muito pequeno

                    prices = [c[4] for c in ohlcv]
                    rsi = calculate_rsi(prices, RSI_PERIOD)
                    market_cap_est = close_last * 1_000_000

                    # Calcular risk score
                    risk_score = calculate_risk_score(vol_multiple, price_change, rsi, market_cap_est)
                    
                    # 🚨 FILTRO DE RISK SCORE
                    if risk_score < MIN_RISK_SCORE:
                        continue  # Skip alertas de baixa qualidade

                    pump_stage = get_pump_stage(rsi, price_change * 100, vol_multiple)

                    if DEBUG_MODE and vol_multiple > 3:
                        rsi_display = safe_rsi_format(rsi)
                        print(f"[DEBUG] {sym}: {vol_multiple:.1f}x, {price_change:+.1f}%, RSI {rsi_display}, Risk {risk_score}/10, Stage {pump_stage}")

                    # ---------- VOLUME SPIKE ----------
                    if vol_avg > 0 and vol_multiple >= THRESHOLD:
                        key = f"SPIKE:{name}:{sym}"
                        if can_alert(key, time.time()):
                            send_enhanced_alert(sym, "VOLUME_SPIKE", {
                                "volume_multiple": vol_multiple,
                                "price_change": price_change * 100,
                                "risk_score": risk_score,
                                "rsi": rsi,
                                "pump_stage": pump_stage,
                                "confidence": "HIGH" if vol_multiple > 15 else "MEDIUM"
                            })

                    # ---------- PUMP PATTERNS ----------
                    pump_found, pump_type, pump_data = detect_pump_pattern(ohlcv)
                    if pump_found:
                        key = f"PUMP:{pump_type}:{name}:{sym}"
                        if can_alert(key, time.time()):
                            send_enhanced_alert(sym, pump_type, {
                                "volume_multiple": vol_multiple,
                                "price_change": price_change * 100,
                                "risk_score": risk_score,
                                "rsi": rsi,
                                "pump_stage": pump_stage,
                                "confidence": pump_data.get("confidence", "MEDIUM")
                            })

                    # ---------- RSI EXTREMES ----------
                    if rsi is not None:
                        try:
                            rsi_val = float(rsi)
                            if (rsi_val > RSI_OVERBOUGHT and vol_multiple > 3) or (rsi_val < RSI_OVERSOLD and vol_multiple > 2):
                                alert_type = "DUMP_WARNING" if rsi_val > RSI_OVERBOUGHT else "OVERSOLD_BOUNCE"
                                key = f"RSI:{alert_type}:{name}:{sym}"
                                if can_alert(key, time.time()):
                                    send_enhanced_alert(sym, alert_type, {
                                        "volume_multiple": vol_multiple,
                                        "price_change": price_change * 100,
                                        "risk_score": risk_score,
                                        "rsi": rsi,
                                        "pump_stage": pump_stage,
                                        "confidence": "HIGH" if abs(rsi_val - 50) > 35 else "MEDIUM"
                                    })
                        except:
                            pass

                    # ---------- STOP HUNTING ----------
                    sh_found, sh_side, sh = detect_stop_hunt(ohlcv)
                    if sh_found:
                        key = f"STOPHUNT:{sh_side}:{name}:{sym}"
                        if can_alert(key, time.time()):
                            sh_risk = calculate_risk_score(sh.get('vol_mult', 0), price_change, rsi, market_cap_est)
                            if sh_risk >= MIN_RISK_SCORE:  # Aplicar filtro também
                                send_enhanced_alert(sym, "STOP_HUNT", {
                                    "volume_multiple": sh.get('vol_mult', 0),
                                    "price_change": price_change * 100,
                                    "risk_score": sh_risk,
                                    "rsi": rsi,
                                    "pump_stage": "STOP_HUNT",
                                    "confidence": "MEDIUM"
                                })

                except ccxt.NetworkError:
                    continue
                except Exception as e:
                    if DEBUG_MODE:
                        print(f"[ERROR] {name} {sym}: {e}")
                    continue

        elapsed = time.time() - loop_start
        sleep_time = max(0, SLEEP_SECONDS - elapsed)
        time.sleep(sleep_time)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Bot encerrado.")
    except Exception as e:
        print(f"❌ Erro fatal: {e}")
        send_telegram(f"❌ Bot crashed: {e}")
