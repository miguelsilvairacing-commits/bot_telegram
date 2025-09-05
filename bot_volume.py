# bot_volume_binance_optimized.py
import os
import time
import math
from collections import defaultdict
from datetime import datetime, timezone
import requests
import ccxt
import numpy as np

# =========================
#   CONFIGURAÇÃO OPTIMIZADA BINANCE
# =========================
EXCHANGES = os.getenv("EXCHANGES", "binance").split(",")
QUOTE_FILTER = os.getenv("QUOTE_FILTER", "USDT,BTC").split(",")

# Filtro OPTIMIZADO para micro caps Binance
QV24H_MIN_USD = float(os.getenv("QV24H_MIN_USD", "1000000"))      # $1M mínimo
QV24H_MAX_USD = float(os.getenv("QV24H_MAX_USD", "30000000"))     # $30M máximo

# Blacklist de majors (não interessam para manipulação)
SYMBOLS_BLACKLIST = set([
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "ADA/USDT", "SOL/USDT", 
    "DOT/USDT", "AVAX/USDT", "MATIC/USDT", "LINK/USDT", "UNI/USDT",
    "LTC/USDT", "BCH/USDT", "XRP/USDT", "DOGE/USDT", "ATOM/USDT"
] + os.getenv("SYMBOLS_BLACKLIST", "").split(","))

TOP_N_BY_VOLUME = int(os.getenv("TOP_N_BY_VOLUME", "50"))         # Mais pares

# TIMEFRAMES CURTOS para apanhar manipulações rápidas
TIMEFRAME = os.getenv("TIMEFRAME", "1m")                          # 1m em vez de 4h!
LOOKBACK = int(os.getenv("LOOKBACK", "10"))                       # Menos lookback
THRESHOLD = float(os.getenv("THRESHOLD", "3.0"))                  # 3x em vez de 10x

# Multi-timeframe analysis
TIMEFRAMES = ["1m", "5m"]                                         # Verificar ambos
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))                   # Para dump warnings

# Stop hunting MAIS SENSÍVEL
SH_WICK_BODY_RATIO = float(os.getenv("SH_WICK_BODY_RATIO", "1.5"))  # Era 2.5
SH_WICK_RANGE_PCT  = float(os.getenv("SH_WICK_RANGE_PCT", "0.4"))   # Era 0.6  
SH_MIN_RETRACE_PCT = float(os.getenv("SH_MIN_RETRACE_PCT", "0.3"))   # Era 0.5
SH_USE_VOLUME      = os.getenv("SH_USE_VOLUME", "true").lower() == "true"
SH_VOL_MULT        = float(os.getenv("SH_VOL_MULT", "2.0"))          # Era 3.0

# Ciclo MAIS RÁPIDO
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "15"))               # Era 30
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "10"))         # Era 30

# DEBUG mode
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
        print("[Telegram] Não configurado. Mensagem seria:\n", msg)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=20
        )
    except Exception as e:
        print("[Telegram] Falha ao enviar:", e)

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

def calculate_rsi(prices, period=14):
    """Calcula RSI simples"""
    try:
        if len(prices) < period + 1:
            return None
        
        # Converte para numpy array para evitar problemas
        prices = np.array([float(p) for p in prices[-period-1:]])
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return float(rsi)
    except Exception as e:
        if DEBUG_MODE:
            print(f"[DEBUG] Erro RSI: {e}")
        return None

def is_manipulation_hour():
    """Horários onde manipulações são mais comuns (UTC)"""
    utc_now = datetime.now(timezone.utc)
    hour = utc_now.hour
    
    # Horários de maior atividade suspeita:
    # 0-2h: Ásia acordando
    # 8-10h: Europa acordando  
    # 14-16h: EUA acordando
    # 22-23h: Final do dia EUA
    high_activity_hours = [0, 1, 2, 8, 9, 10, 14, 15, 16, 22, 23]
    return hour in high_activity_hours

# ========= Filtro OPTIMIZADO para Binance =========
def pick_symbols_by_24h_volume(ex, top_n=50, quotes=None):
    """
    Escolhe pares OPTIMIZADO para detectar manipulações na Binance
    """
    if quotes is None:
        quotes = QUOTE_FILTER
        
    markets = ex.load_markets()
    try:
        tickers = ex.fetch_tickers()
    except Exception as e:
        print(f"[ERROR] Falha ao buscar tickers: {e}")
        # fallback: usa mercados ativos
        symbols = [s for s, m in markets.items() if m.get("active")]
        symbols = [s for s in symbols if any(s.endswith("/" + q) for q in quotes)]
        symbols = [s for s in symbols if s not in SYMBOLS_BLACKLIST]
        return symbols[:top_n]

    rows = []
    for sym, t in tickers.items():
        # Filtra por quote currency
        if not any(sym.endswith("/" + q) for q in quotes):
            continue
            
        if sym in SYMBOLS_BLACKLIST:
            continue

        # Volume 24h
        vol_q = t.get("quoteVolume") or (t.get("info") or {}).get("quoteVolume")
        try:
            vol_q = float(vol_q) if vol_q is not None else None
        except Exception:
            vol_q = None
        if vol_q is None:
            continue

        # Filtro por intervalo (micro/small caps)
        if vol_q < QV24H_MIN_USD or vol_q > QV24H_MAX_USD:
            continue

        rows.append((sym, vol_q))

    # Ordenar por volume e cortar
    rows.sort(key=lambda x: x[1], reverse=True)
    selected = [sym for sym, _ in rows[:top_n]]
    
    if DEBUG_MODE:
        print(f"[DEBUG] Selecionados {len(selected)} pares: {selected[:10]}...")
    
    return selected

def fetch_ohlcv_safe(ex, symbol, timeframe, limit):
    try:
        return ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        if DEBUG_MODE:
            print(f"[DEBUG] Erro OHLCV {symbol}: {e}")
        return None

# =========================
#   DETECÇÃO DE MANIPULAÇÃO MELHORADA
# =========================
def detect_pump_pattern(ohlcv):
    """Detecta padrões de pump específicos da Binance"""
    if len(ohlcv) < 10:
        return False, None, {}
    
    last_10 = ohlcv[-10:]
    volumes = [c[5] for c in last_10]
    prices = [c[4] for c in last_10]
    
    # Padrão 1: Volume crescente + preço crescente (acumulação)
    vol_trend = sum(volumes[-3:]) / sum(volumes[:3]) if sum(volumes[:3]) > 0 else 0
    price_change = (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0
    
    if vol_trend > 3 and price_change > 0.05:  # Volume 3x + preço +5%
        return True, "ACCUMULATION_PUMP", {
            "vol_trend": vol_trend,
            "price_change": price_change,
            "confidence": "HIGH" if vol_trend > 5 else "MEDIUM"
        }
    
    # Padrão 2: Spike súbito (coordenação)
    max_vol = max(volumes)
    avg_vol = sum(volumes[:-1]) / len(volumes[:-1]) if len(volumes) > 1 else 0
    
    if avg_vol > 0 and max_vol > 5 * avg_vol and price_change > 0.03:
        return True, "COORDINATED_PUMP", {
            "vol_spike": max_vol / avg_vol,
            "price_change": price_change,
            "confidence": "HIGH"
        }
    
    return False, None, {}

def calculate_risk_score(volume_mult, price_change, rsi, market_cap_usd):
    """Calcula risk score de 1-10 para a manipulação"""
    score = 0
    
    # Volume spike severity (0-4 pontos)
    if volume_mult > 20: score += 4
    elif volume_mult > 10: score += 3
    elif volume_mult > 5: score += 2
    elif volume_mult > 3: score += 1
    
    # Price movement (0-3 pontos)
    abs_change = abs(price_change)
    if abs_change > 0.2: score += 3      # >20%
    elif abs_change > 0.1: score += 2    # >10%
    elif abs_change > 0.05: score += 1   # >5%
    
    # RSI extremes (0-2 pontos)
    if rsi is not None:
        if rsi > 85 or rsi < 15: score += 2
        elif rsi > 75 or rsi < 25: score += 1
    
    # Market cap (smaller = higher risk) (0-1 pontos)
    if market_cap_usd < 5_000_000: score += 1
    
    return min(score, 10)

# =========================
#   STOP HUNTING DETECTOR (melhorado)
# =========================
def candle_parts(c):
    o, h, l, cl, v = c[1], c[2], c[3], c[4], c[5]
    rng = max(1e-12, h - l)
    body = abs(cl - o)
    upper = h - max(o, cl)
    lower = min(o, cl) - l
    return o, h, l, cl, v, rng, body, upper, lower

def detect_stop_hunt(ohlcv,
                     wick_body_ratio=SH_WICK_BODY_RATIO,
                     wick_range_pct=SH_WICK_RANGE_PCT,
                     min_retrace_pct=SH_MIN_RETRACE_PCT,
                     use_volume=SH_USE_VOLUME,
                     vol_mult=SH_VOL_MULT,
                     lookback_for_vol=LOOKBACK):
    """Detecta stop hunting (mais sensível)"""
    if len(ohlcv) < max(lookback_for_vol, 3):
        return False, None, {}

    last = ohlcv[-1]
    o, h, l, cl, v, rng, body, upper, lower = candle_parts(last)

    vols = [c[5] for c in ohlcv[-(lookback_for_vol+1):-1]]
    vol_avg = sum(vols)/len(vols) if vols else 0.0
    vol_ok = (not use_volume) or (vol_avg > 0 and v >= vol_mult * vol_avg)

    # Retracções
    retrace_from_low = (cl - l) / rng if rng > 0 else 0.0
    retrace_from_high = (h - cl) / rng if rng > 0 else 0.0

    # Condições (mais sensíveis)
    cond_down = (lower >= wick_body_ratio * body) and (lower >= wick_range_pct * rng)
    cond_up = (upper >= wick_body_ratio * body) and (upper >= wick_range_pct * rng)

    found = False
    side = None

    if cond_down and (retrace_from_low >= min_retrace_pct) and vol_ok:
        found, side = True, "down"
    if cond_up and (retrace_from_high >= min_retrace_pct) and vol_ok:
        found, side = True, "up"

    info = {
        "o": o, "h": h, "l": l, "c": cl, "v": v,
        "range": rng, "body": body, "upper": upper, "lower": lower,
        "retrace_from_low": retrace_from_low,
        "retrace_from_high": retrace_from_high,
        "vol_avg": vol_avg, "vol_mult": (v/vol_avg) if vol_avg else None
    }
    return found, side, info

# =========================
#   ALERTAS MELHORADOS
# =========================
last_alert_ts = defaultdict(lambda: 0.0)

def can_alert(key: str, now_ts: float) -> bool:
    last = last_alert_ts[key]
    if (now_ts - last) >= COOLDOWN_MINUTES * 60:
        last_alert_ts[key] = now_ts
        return True
    return False

def send_manipulation_alert(symbol, alert_type, data):
    """Alerta específico para manipulações"""
    confidence = data.get('confidence', 'MEDIUM')
    price_change = data.get('price_change', 0)
    volume_mult = data.get('volume_multiple', 0)
    risk_score = data.get('risk_score', 0)
    rsi = data.get('rsi')
    
    # Emojis por tipo
    emoji_map = {
        'VOLUME_SPIKE': '🚀' if confidence == 'HIGH' else '⚡',
        'ACCUMULATION_PUMP': '🔥',
        'COORDINATED_PUMP': '💥',
        'DUMP_WARNING': '🔴',
        'STOP_HUNT': '🩸' if data.get('side') == 'down' else '🧨'
    }
    
    emoji = emoji_map.get(alert_type, '⚠️')
    
    msg = f"""
{emoji} <b>BINANCE MANIPULATION DETECTED</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━

<b>Par:</b> {symbol}
<b>Tipo:</b> {alert_type}
<b>Risk Score:</b> {risk_score}/10
<b>Confiança:</b> {confidence}

<b>📊 Métricas:</b>
• Preço: {price_change:+.2f}%
• Volume: {volume_mult:.1f}x média
• Timeframe: {TIMEFRAME}
• Hora: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"""

    if rsi is not None:
        msg += f"\n• RSI: {rsi:.1f}"
        if rsi > 80:
            msg += " ⚠️ OVERBOUGHT"
        elif rsi < 20:
            msg += " ⚠️ OVERSOLD"

    # Recomendação baseada no tipo
    if alert_type == 'ACCUMULATION_PUMP':
        msg += "\n\n<b>💡 Recomendação:</b> 🟢 Possível ENTRY zone"
    elif alert_type == 'DUMP_WARNING' or (rsi and rsi > 80):
        msg += "\n\n<b>💡 Recomendação:</b> 🔴 DUMP risk - Monitor exits"
    elif alert_type == 'COORDINATED_PUMP':
        msg += "\n\n<b>💡 Recomendação:</b> 🟡 Quick pump - Cuidado"
    
    # Link direto para Binance
    binance_symbol = symbol.replace('/', '')
    msg += f"\n\n<b>🔗 Binance:</b> https://www.binance.com/en/trade/{binance_symbol}"
    
    send_telegram(msg)

# =========================
#   MAIN LOOP OPTIMIZADO
# =========================
def main():
    print("🚀 Iniciando bot OPTIMIZADO para detectar manipulações na Binance...")
    
    # Instanciar Binance
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
            print(f"[{name}] 📊 Monitorizando {len(syms)} pares")
            if DEBUG_MODE:
                print(f"[{name}] Exemplos: {syms[:8]}")
        except Exception as e:
            print(f"[{name}] ❌ Falha: {e}")

    if not exchanges:
        raise SystemExit("❌ Nenhuma exchange configurada.")

    send_telegram("✅ Bot de MANIPULAÇÃO BINANCE iniciado!\n🎯 Foco: Small/micro caps\n⏱️ Timeframe: 1m\n🔍 Threshold: 3x volume")

    print(f"🔄 Loop iniciado - scan a cada {SLEEP_SECONDS}s")
    if DEBUG_MODE:
        print("🐛 DEBUG MODE ativo - logs detalhados")

    while True:
        loop_start = time.time()
        
        # Check se é horário de alta atividade
        high_activity = is_manipulation_hour()
        if DEBUG_MODE and high_activity:
            print(f"[DEBUG] ⏰ Horário de alta atividade: {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
        
        for name, ex in exchanges.items():
            symbols = watchlist.get(name, [])
            
            for sym in symbols:
                try:
                    # Buscar dados para análise
                    ohlcv = fetch_ohlcv_safe(ex, sym, TIMEFRAME, limit=LOOKBACK + 15)
                    if not ohlcv or len(ohlcv) < LOOKBACK + 1:
                        continue

                    *hist, last = ohlcv
                    volumes = [c[5] for c in hist[-LOOKBACK:]]
                    vol_avg = (sum(volumes) / len(volumes)) if volumes else 0.0
                    vol_last = last[5]
                    close_last = last[4]
                    ts_last = last[0]

                    # Calcular múltiplo de volume
                    vol_multiple = vol_last / vol_avg if vol_avg > 0 else 0
                    
                    # Preço change
                    price_change = 0
                    if len(hist) > 0:
                        prev_close = hist[-1][4]
                        price_change = (close_last - prev_close) / prev_close if prev_close > 0 else 0

                    # RSI
                    prices = [c[4] for c in ohlcv]
                    rsi = calculate_rsi(prices, RSI_PERIOD)

                    # Market cap estimate (aproximado)
                    market_cap_est = close_last * 1_000_000  # Rough estimate

                    # DEBUG logs para volume activity
                    if DEBUG_MODE and vol_multiple > 2:
                        print(f"[DEBUG] {sym}: {vol_multiple:.1f}x volume, preço {price_change:+.2f}%, RSI {rsi:.1f if rsi else 'N/A'}")

                    # ---------- ALERTA 1: VOLUME SPIKE ----------
                    if vol_avg > 0 and vol_multiple >= THRESHOLD:
                        risk_score = calculate_risk_score(vol_multiple, price_change, rsi, market_cap_est)
                        
                        key = f"SPIKE:{name}:{sym}"
                        if can_alert(key, time.time()):
                            send_manipulation_alert(sym, "VOLUME_SPIKE", {
                                "volume_multiple": vol_multiple,
                                "price_change": price_change * 100,
                                "risk_score": risk_score,
                                "rsi": rsi,
                                "confidence": "HIGH" if vol_multiple > 10 else "MEDIUM"
                            })

                    # ---------- ALERTA 2: PUMP PATTERNS ----------
                    pump_found, pump_type, pump_data = detect_pump_pattern(ohlcv)
                    if pump_found:
                        key = f"PUMP:{pump_type}:{name}:{sym}"
                        if can_alert(key, time.time()):
                            risk_score = calculate_risk_score(vol_multiple, price_change, rsi, market_cap_est)
                            send_manipulation_alert(sym, pump_type, {
                                "volume_multiple": vol_multiple,
                                "price_change": price_change * 100,
                                "risk_score": risk_score,
                                "rsi": rsi,
                                "confidence": pump_data.get("confidence", "MEDIUM")
                            })

                    # ---------- ALERTA 3: DUMP WARNING ----------
                    if rsi and rsi > 80 and vol_multiple > 2:
                        key = f"DUMP:{name}:{sym}"
                        if can_alert(key, time.time()):
                            risk_score = calculate_risk_score(vol_multiple, price_change, rsi, market_cap_est)
                            send_manipulation_alert(sym, "DUMP_WARNING", {
                                "volume_multiple": vol_multiple,
                                "price_change": price_change * 100,
                                "risk_score": risk_score,
                                "rsi": rsi,
                                "confidence": "HIGH"
                            })

                    # ---------- ALERTA 4: STOP HUNTING ----------
                    sh_found, sh_side, sh = detect_stop_hunt(ohlcv)
                    if sh_found:
                        key = f"STOPHUNT:{sh_side}:{name}:{sym}"
                        if can_alert(key, time.time()):
                            send_manipulation_alert(sym, "STOP_HUNT", {
                                "side": sh_side,
                                "volume_multiple": sh.get('vol_mult', 0),
                                "price_change": price_change * 100,
                                "risk_score": calculate_risk_score(sh.get('vol_mult', 0), price_change, rsi, market_cap_est),
                                "rsi": rsi,
                                "confidence": "MEDIUM"
                            })

                except ccxt.NetworkError:
                    continue  # Rate limit, ignora
                except Exception as e:
                    if DEBUG_MODE:
                        print(f"[ERROR] {name} {sym}: {e}")
                    continue

        # Pausa respeitando rate limits
        elapsed = time.time() - loop_start
        sleep_time = max(0, SLEEP_SECONDS - elapsed)
        if DEBUG_MODE and sleep_time < 5:
            print(f"[DEBUG] ⚡ Loop rápido: {elapsed:.1f}s")
        time.sleep(sleep_time)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Bot encerrado pelo utilizador.")
    except Exception as e:
        print(f"❌ Erro fatal: {e}")
        send_telegram(f"❌ Bot crashed: {e}")
