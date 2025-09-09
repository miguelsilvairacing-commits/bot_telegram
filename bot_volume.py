# bot_volume_bingx_fixed.py
import os
import time
import math
from collections import defaultdict
from datetime import datetime, timezone
import requests
import ccxt
import numpy as np

# =========================
#   CONFIGURAÇÃO MULTI-EXCHANGE
# =========================
EXCHANGES = os.getenv("EXCHANGES", "binance,bingx").split(",")
QUOTE_FILTER = os.getenv("QUOTE_FILTER", "USDT").split(",")

# Market cap filtering
QV24H_MIN_USD = float(os.getenv("QV24H_MIN_USD", "200000"))       # $200K mínimo
QV24H_MAX_USD = float(os.getenv("QV24H_MAX_USD", "20000000"))     # $20M máximo

# Blacklist de majors
SYMBOLS_BLACKLIST = set([
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "ADA/USDT", "SOL/USDT", 
    "DOT/USDT", "AVAX/USDT", "MATIC/USDT", "LINK/USDT", "UNI/USDT",
    "LTC/USDT", "BCH/USDT", "XRP/USDT", "DOGE/USDT", "ATOM/USDT"
] + [s.strip() for s in os.getenv("SYMBOLS_BLACKLIST", "").split(",") if s.strip()])

TOP_N_BY_VOLUME = int(os.getenv("TOP_N_BY_VOLUME", "30"))

# Detection thresholds
TIMEFRAME = os.getenv("TIMEFRAME", "1m")
LOOKBACK = int(os.getenv("LOOKBACK", "8"))                        # Reduzido para BingX
THRESHOLD = float(os.getenv("THRESHOLD", "3.0"))

# Filtros de qualidade
MIN_RISK_SCORE = int(os.getenv("MIN_RISK_SCORE", "4"))
MIN_PRICE_CHANGE = float(os.getenv("MIN_PRICE_CHANGE", "0.04"))   # 4%
HIGH_CONFIDENCE_ONLY = os.getenv("HIGH_CONFIDENCE_ONLY", "false").lower() == "true"

# RSI settings
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
RSI_OVERBOUGHT = float(os.getenv("RSI_OVERBOUGHT", "80"))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "20"))

# Stop hunting
SH_WICK_BODY_RATIO = float(os.getenv("SH_WICK_BODY_RATIO", "2.0"))
SH_WICK_RANGE_PCT = float(os.getenv("SH_WICK_RANGE_PCT", "0.5"))    
SH_MIN_RETRACE_PCT = float(os.getenv("SH_MIN_RETRACE_PCT", "0.4"))   
SH_USE_VOLUME = os.getenv("SH_USE_VOLUME", "true").lower() == "true"
SH_VOL_MULT = float(os.getenv("SH_VOL_MULT", "3.0"))

# Timing - ajustado para multi-exchange
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "30"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "25"))

# Horários
ALERT_HOURS_STR = os.getenv("ALERT_HOURS", "")
ALERT_HOURS = [int(x.strip()) for x in ALERT_HOURS_STR.split(",") if x.strip()] if ALERT_HOURS_STR else []
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
    """Envia texto para o Telegram"""
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
        print(f"\n🚀 INICIALIZAÇÃO COMPLETA!")
    print(f"🏦 Exchanges ativas: {', '.join(active_exchanges)}")
    print(f"📊 Total de pares: {total_pairs}")
    print(f"⏱️ Scan interval: {SLEEP_SECONDS}s")
    print(f"🔄 Cooldown: {COOLDOWN_MINUTES}min")
    
    # Notificar Telegram
    send_telegram(f"✅ Bot Multi-Exchange ONLINE!\n🏦 Exchanges: {', '.join(active_exchanges)}\n📊 Pares: {total_pairs}\n🎯 Target: 30-80 alertas/dia\n🔧 Filtros: Risk≥{MIN_RISK_SCORE}, Price≥{MIN_PRICE_CHANGE*100:.0f}%")

    print(f"\n🔄 LOOP PRINCIPAL INICIADO...")
    if DEBUG_MODE:
        print("🐛 DEBUG MODE: Logs detalhados ativados")

    # Stats para monitorização
    loop_count = 0
    total_symbols_checked = 0
    alerts_sent = 0

    while True:
        loop_start = time.time()
        loop_count += 1
        
        if DEBUG_MODE and loop_count % 10 == 0:
            print(f"\n[STATS] Loop #{loop_count}, Symbols checked: {total_symbols_checked}, Alerts sent: {alerts_sent}")
        
        for exchange_name, ex in exchanges.items():
            symbols = watchlist.get(exchange_name, [])
            
            if not symbols:
                if DEBUG_MODE:
                    print(f"[{exchange_name}] Sem símbolos para verificar")
                continue
            
            exchange_symbols_checked = 0
            
            for sym in symbols:
                try:
                    # Fetch OHLCV data
                    ohlcv = fetch_ohlcv_safe(ex, sym, TIMEFRAME, limit=LOOKBACK + 15)
                    if not ohlcv or len(ohlcv) < LOOKBACK + 1:
                        continue

                    exchange_symbols_checked += 1
                    total_symbols_checked += 1

                    # Calcular métricas básicas
                    *hist, last = ohlcv
                    volumes = [c[5] for c in hist[-LOOKBACK:]]
                    vol_avg = (sum(volumes) / len(volumes)) if volumes else 0.0
                    vol_last = last[5]
                    close_last = last[4]

                    # Volume multiple
                    vol_multiple = vol_last / vol_avg if vol_avg > 0 else 0
                    
                    # Price change
                    price_change = 0
                    if len(hist) > 0:
                        prev_close = hist[-1][4]
                        price_change = (close_last - prev_close) / prev_close if prev_close > 0 else 0

                    # Filtro de price change ANTES de calcular RSI (otimização)
                    if abs(price_change) < MIN_PRICE_CHANGE:
                        continue

                    # RSI calculation
                    prices = [c[4] for c in ohlcv]
                    rsi = calculate_rsi(prices, RSI_PERIOD)
                    
                    # Market cap estimate
                    market_cap_est = close_last * 1_000_000

                    # Risk score
                    risk_score = calculate_risk_score(vol_multiple, price_change, rsi, market_cap_est)
                    
                    # Filtro de risk score ANTES de outros cálculos
                    if risk_score < MIN_RISK_SCORE:
                        continue

                    # Pump stage detection
                    pump_stage = get_pump_stage(rsi, price_change * 100, vol_multiple)

                    # Debug logs para atividade interessante
                    if DEBUG_MODE and vol_multiple > 2:
                        rsi_display = safe_rsi_format(rsi)
                        print(f"[DEBUG] {exchange_name} {sym}: {vol_multiple:.1f}x vol, {price_change:+.1f}%, RSI {rsi_display}, Risk {risk_score}/10, {pump_stage}")

                    # ========== ALERTAS ==========

                    # 1. VOLUME SPIKE
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
                            alerts_sent += 1

                    # 2. PUMP PATTERNS
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
                            alerts_sent += 1

                    # 3. RSI EXTREMES
                    if rsi is not None:
                        try:
                            rsi_val = float(rsi)
                            
                            # Dump warning (RSI overbought + volume)
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
                                    alerts_sent += 1
                            
                            # Oversold bounce opportunity
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
                                    alerts_sent += 1
                                    
                        except:
                            pass

                    # 4. STOP HUNTING
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
                                alerts_sent += 1

                except ccxt.NetworkError:
                    # Rate limit ou network issues - continuar
                    continue
                    
                except ccxt.ExchangeError as e:
                    if "rate limit" in str(e).lower():
                        if DEBUG_MODE:
                            print(f"[DEBUG] {exchange_name}: Rate limit hit")
                        time.sleep(2)  # Pausa curta
                    continue
                    
                except Exception as e:
                    if DEBUG_MODE:
                        print(f"[ERROR] {exchange_name} {sym}: {e}")
                    continue
            
            # Stats por exchange
            if DEBUG_MODE and exchange_symbols_checked > 0:
                print(f"[DEBUG] {exchange_name}: Checked {exchange_symbols_checked} symbols this loop")

        # Controlo de timing
        elapsed = time.time() - loop_start
        sleep_time = max(0, SLEEP_SECONDS - elapsed)
        
        if DEBUG_MODE and elapsed > SLEEP_SECONDS * 0.8:
            print(f"[DEBUG] ⚡ Slow loop: {elapsed:.1f}s (target: {SLEEP_SECONDS}s)")
        
        time.sleep(sleep_time)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Bot encerrado pelo utilizador.")
        send_telegram("👋 Bot Multi-Exchange encerrado pelo utilizador.")
    except Exception as e:
        error_msg = f"❌ Bot crashed: {e}"
        print(error_msg)
        send_telegram(error_msg)
        print(f"[Telegram] Falha: {e}")

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
    """Build exchange com configurações específicas para cada API"""
    name = name.strip().lower()
    if not hasattr(ccxt, name):
        raise ValueError(f"Exchange '{name}' não existe no CCXT.")
    
    # Configurações base
    config = {
        "enableRateLimit": True,
        "options": {"adjustForTimeDifference": True}
    }
    
    # Configurações específicas por exchange
    if name == "bingx":
        config.update({
            "sandbox": False,
            "timeout": 30000,  # 30s timeout (BingX pode ser lento)
            "rateLimit": 2000,  # Mais conservativo
            "options": {
                "adjustForTimeDifference": True,
                "recvWindow": 60000,  # BingX precisa de mais tempo
            }
        })
        print("[BingX] Configuração específica aplicada")
        
    elif name == "binance":
        config.update({
            "timeout": 20000,
            "rateLimit": 1200,
        })
        print("[Binance] Configuração padrão aplicada")
    
    ex = getattr(ccxt, name)(config)
    
    try:
        print(f"[{name}] Carregando mercados...")
        ex.load_markets()
        market_count = len(ex.markets) if hasattr(ex, 'markets') else 0
        print(f"[{name}] ✅ {market_count} mercados carregados")
    except Exception as e:
        print(f"[{name}] ❌ Erro ao carregar mercados: {e}")
        raise
    
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
    """Calcula RSI com tratamento robusto de erros"""
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
    """Verifica se está num horário válido para alertas"""
    if not ALERT_HOURS:
        return True
    
    utc_now = datetime.now(timezone.utc)
    current_hour = utc_now.hour
    
    if WEEKEND_QUIET_MODE and utc_now.weekday() >= 5:
        # Weekend: só horários prime
        return current_hour in [8, 9, 14, 15, 22]
    
    return current_hour in ALERT_HOURS

def is_manipulation_hour():
    """Horários de maior atividade suspeita"""
    utc_now = datetime.now(timezone.utc)
    hour = utc_now.hour
    high_activity_hours = [0, 1, 2, 8, 9, 10, 14, 15, 16, 22, 23]
    return hour in high_activity_hours

# =========================
#   FETCH OHLCV COM CORREÇÕES BINGX
# =========================
def fetch_ohlcv_safe(ex, symbol, timeframe, limit):
    """OHLCV fetch com retry e configurações específicas por exchange"""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            # Configurações específicas BingX
            if ex.id.lower() == 'bingx':
                # BingX limites
                actual_limit = min(limit, 100)  # BingX max 100 candles
                
                # Mapear timeframes se necessário
                tf_map = {
                    '1m': '1m',
                    '5m': '5m', 
                    '15m': '15m',
                    '1h': '1h',
                    '4h': '4h',
                    '1d': '1d'
                }
                actual_tf = tf_map.get(timeframe, timeframe)
                
                # Extra params para BingX
                params = {}
                
                result = ex.fetch_ohlcv(symbol, timeframe=actual_tf, limit=actual_limit, params=params)
                
                if DEBUG_MODE and attempt > 0:
                    print(f"[DEBUG] BingX {symbol}: Success on attempt {attempt + 1}")
                
                return result
                
            else:
                # Outras exchanges (Binance, etc) - sem mudanças
                return ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                
        except ccxt.NetworkError as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 3  # 3, 6, 9 seconds
                if DEBUG_MODE:
                    print(f"[DEBUG] {ex.id} {symbol}: Network error, retry {attempt + 1}/{max_retries} in {wait_time}s - {str(e)[:100]}")
                time.sleep(wait_time)
                continue
            else:
                if DEBUG_MODE:
                    print(f"[DEBUG] {ex.id} {symbol}: Max retries exceeded - {e}")
                return None
                
        except ccxt.ExchangeError as e:
            error_str = str(e).lower()
            if any(x in error_str for x in ["invalid symbol", "symbol not found", "not found", "does not exist"]):
                if DEBUG_MODE:
                    print(f"[DEBUG] {ex.id} {symbol}: Symbol not found")
                return None
            elif "rate limit" in error_str or "too many requests" in error_str:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5  # 5, 10, 15 seconds para rate limit
                    if DEBUG_MODE:
                        print(f"[DEBUG] {ex.id} {symbol}: Rate limit, wait {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    if DEBUG_MODE:
                        print(f"[DEBUG] {ex.id} {symbol}: Rate limit - skipping")
                    return None
            else:
                if DEBUG_MODE:
                    print(f"[DEBUG] {ex.id} {symbol}: Exchange error - {e}")
                return None
                
        except Exception as e:
            if DEBUG_MODE:
                print(f"[DEBUG] {ex.id} {symbol}: Unexpected error - {e}")
            return None
    
    return None

# =========================
#   SELEÇÃO DE SÍMBOLOS MULTI-EXCHANGE
# =========================
def pick_symbols_by_24h_volume_multi_exchange(ex, top_n=30, quotes=None):
    """Seleção otimizada com handling específico por exchange"""
    if quotes is None:
        quotes = QUOTE_FILTER
        
    exchange_name = ex.id.lower()
    
    try:
        markets = ex.load_markets()
        
        # Filtrar mercados ativos
        if exchange_name == 'bingx':
            # BingX: filtros específicos
            active_markets = {}
            for k, v in markets.items():
                if (v.get('active', True) and 
                    v.get('spot', True) and 
                    v.get('type') == 'spot'):
                    active_markets[k] = v
            
            if DEBUG_MODE:
                print(f"[DEBUG] BingX: {len(active_markets)}/{len(markets)} mercados ativos")
        else:
            # Binance e outras
            active_markets = {k: v for k, v in markets.items() if v.get('active', True)}
            
        # Tentar buscar tickers
        tickers = None
        try:
            print(f"[{exchange_name}] Buscando tickers...")
            tickers = ex.fetch_tickers()
            print(f"[{exchange_name}] ✅ {len(tickers)} tickers obtidos")
        except Exception as e:
            print(f"[{exchange_name}] ❌ Erro ao buscar tickers: {e}")
            
            # Fallback: usar mercados ativos
            symbols = []
            for s, m in active_markets.items():
                if any(s.endswith("/" + q) for q in quotes) and s not in SYMBOLS_BLACKLIST:
                    symbols.append(s)
            
            print(f"[{exchange_name}] Fallback: {len(symbols)} símbolos selecionados")
            return symbols[:top_n]

        # Processar tickers
        rows = []
        processed = 0
        
        for sym, t in tickers.items():
            processed += 1
            
            # Filtro quote currency
            if not any(sym.endswith("/" + q) for q in quotes):
                continue
                
            # Blacklist
            if sym in SYMBOLS_BLACKLIST:
                continue
                
            # Deve estar nos mercados ativos
            if sym not in active_markets:
                continue

            # Extrair volume 24h
            vol_q = None
            
            if exchange_name == 'bingx':
                # BingX estrutura pode ser diferente
                vol_q = (t.get("quoteVolume") or 
                        t.get("info", {}).get("volume") or
                        t.get("info", {}).get("quoteVolume"))
            else:
                # Binance e outras
                vol_q = (t.get("quoteVolume") or 
                        t.get("info", {}).get("quoteVolume"))
            
            # Converter para float
            try:
                vol_q = float(vol_q) if vol_q is not None else None
            except (ValueError, TypeError):
                vol_q = None
                
            if vol_q is None or vol_q <= 0:
                continue

            # Filtros de volume
            if vol_q < QV24H_MIN_USD or vol_q > QV24H_MAX_USD:
                continue

            rows.append((sym, vol_q))

        # Debug info
        if DEBUG_MODE:
            print(f"[DEBUG] {exchange_name}: Processed {processed} tickers, found {len(rows)} válidos")

        # Ordenar por volume e selecionar top N
        rows.sort(key=lambda x: x[1], reverse=True)
        selected = [sym for sym, _ in rows[:top_n]]
        
        print(f"[{exchange_name}] ✅ Selected {len(selected)} pares de {len(rows)} válidos")
        
        if DEBUG_MODE and selected:
            print(f"[DEBUG] {exchange_name}: Top 5: {selected[:5]}")
            if len(rows) > 0:
                top_volumes = [short_num(vol) for _, vol in rows[:3]]
                print(f"[DEBUG] {exchange_name}: Top volumes: {top_volumes}")
        
        return selected
        
    except Exception as e:
        print(f"[ERROR] {exchange_name}: Erro na seleção de símbolos - {e}")
        return []

# =========================
#   DETECÇÃO DE PADRÕES
# =========================
def detect_pump_pattern(ohlcv):
    """Detecta padrões de pump"""
    if len(ohlcv) < 8:  # Reduzido para trabalhar com menos dados
        return False, None, {}
    
    last_8 = ohlcv[-8:]
    volumes = [c[5] for c in last_8]
    prices = [c[4] for c in last_8]
    
    # Padrão accumulation
    recent_vol = sum(volumes[-3:])
    early_vol = sum(volumes[:3])
    vol_trend = recent_vol / early_vol if early_vol > 0 else 0
    
    price_change = (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0
    
    if vol_trend > 3 and price_change > 0.06:  # Ajustado para ser menos restritivo
        return True, "ACCUMULATION_PUMP", {
            "vol_trend": vol_trend,
            "price_change": price_change,
            "confidence": "HIGH" if vol_trend > 5 else "MEDIUM"
        }
    
    # Padrão coordinated
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
    """Risk score calculation"""
    score = 0
    
    # Volume severity (0-4)
    if volume_mult > 15: score += 4
    elif volume_mult > 10: score += 3
    elif volume_mult > 6: score += 2
    elif volume_mult > 3: score += 1
    
    # Price movement (0-3)
    abs_change = abs(price_change)
    if abs_change > 0.20: score += 3      # >20%
    elif abs_change > 0.12: score += 2    # >12%
    elif abs_change > 0.06: score += 1    # >6%
    
    # RSI context (0-2)
    if rsi is not None:
        try:
            rsi_val = float(rsi)
            if price_change > 0:  # Pump
                if 50 <= rsi_val <= 75: score += 2  # Sweet spot
                elif 40 <= rsi_val <= 80: score += 1
            else:  # Dump
                if rsi_val < 30: score += 2  # Oversold
                elif rsi_val < 45: score += 1
        except:
            pass
    
    # Market cap sweet spot (0-1)
    if 500_000 <= market_cap_usd <= 15_000_000: score += 1
    
    return min(score, 10)

def get_pump_stage(rsi, price_change_pct, volume_mult):
    """Determina o estágio do pump"""
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
    """Stop hunting detection"""
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
#   SISTEMA DE ALERTAS
# =========================
last_alert_ts = defaultdict(lambda: 0.0)

def can_alert(key: str, now_ts: float) -> bool:
    last = last_alert_ts[key]
    if (now_ts - last) >= COOLDOWN_MINUTES * 60:
        last_alert_ts[key] = now_ts
        return True
    return False

def should_send_alert(risk_score, confidence):
    """Filtros de qualidade"""
    
    # Risk score filter
    if risk_score < MIN_RISK_SCORE:
        if DEBUG_MODE:
            print(f"[FILTER] Risk score {risk_score} < {MIN_RISK_SCORE}")
        return False
    
    # Confidence filter
    if HIGH_CONFIDENCE_ONLY and confidence != "HIGH":
        if DEBUG_MODE:
            print(f"[FILTER] Confidence {confidence} não é HIGH")
        return False
    
    # Timing filter
    if ALERT_HOURS and not is_alert_hour():
        if DEBUG_MODE:
            print(f"[FILTER] Fora do horário de alertas")
        return False
    
    return True

def send_enhanced_alert(symbol, alert_type, data, exchange_name):
    """Alertas melhorados com info da exchange"""
    
    risk_score = data.get('risk_score', 0)
    confidence = data.get('confidence', 'MEDIUM')
    
    # Aplicar filtros
    if not should_send_alert(risk_score, confidence):
        return
    
    price_change = data.get('price_change', 0)
    volume_mult = data.get('volume_multiple', 0)
    rsi = data.get('rsi')
    pump_stage = data.get('pump_stage', alert_type)
    
    # Emojis por exchange
    exchange_emojis = {
        'binance': '🟡',
        'bingx': '🔵',
        'kucoin': '🟢',
        'gate': '🟣'
    }
    
    # Emojis por stage
    stage_emojis = {
        'EARLY_PUMP': '🚀',
        'MID_PUMP': '⚡',
        'LATE_PUMP': '⚠️',
        'DUMP_PHASE': '🔴',
        'VOLUME_SPIKE': '💥',
        'STOP_HUNT': '🩸'
    }
    
    exchange_emoji = exchange_emojis.get(exchange_name.lower(), '⚪')
    stage_emoji = stage_emojis.get(pump_stage, stage_emojis.get(alert_type, '⚠️'))
    
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

    # Recomendações por stage
    if pump_stage == 'EARLY_PUMP':
        msg += f"\n\n<b>💡 Action:</b> 🟢 ENTRY ZONE\n• Target: +20-35%\n• Stop: -6%"
    elif pump_stage == 'MID_PUMP':
        msg += f"\n\n<b>💡 Action:</b> 🟡 MONITOR\n• Partial profit taking\n• Trail stops"
    elif pump_stage == 'LATE_PUMP':
        msg += f"\n\n<b>💡 Action:</b> 🔴 EXIT WARNING\n• Prepare exits\n• Risky entry"
    elif pump_stage == 'DUMP_PHASE':
        msg += f"\n\n<b>💡 Action:</b> 🔴 DUMP ACTIVE\n• Exit immediately\n• Short opportunity"
    
    # Links por exchange
    if exchange_name.lower() == 'binance':
        binance_symbol = symbol.replace('/', '')
        msg += f"\n\n<b>🔗 Trade:</b> binance.com/trade/{binance_symbol}"
    elif exchange_name.lower() == 'bingx':
        bingx_symbol = symbol.replace('/', '-')
        msg += f"\n\n<b>🔗 Trade:</b> bingx.com/spot/{bingx_symbol}"
    
    send_telegram(msg)

# =========================
#   MAIN LOOP
# =========================
def main():
    print("🎯 Bot Multi-Exchange com BingX FIXED!")
    print(f"🔧 Configuração: Risk≥{MIN_RISK_SCORE}, Price≥{MIN_PRICE_CHANGE*100:.0f}%, Volume≥{THRESHOLD}x")
    
    # Inicializar exchanges
    exchanges = {}
    watchlist = {}

    for name in EXCHANGES:
        name = name.strip()
        if not name:
            continue
        try:
            print(f"\n[{name.upper()}] 🔄 Inicializando...")
            ex = build_exchange(name)
            exchanges[name] = ex
            
            print(f"[{name.upper()}] 📊 Selecionando símbolos...")
            syms = pick_symbols_by_24h_volume_multi_exchange(ex, TOP_N_BY_VOLUME, QUOTE_FILTER)
            watchlist[name] = syms
            
            if syms:
                print(f"[{name.upper()}] ✅ {len(syms)} pares selecionados")
                if DEBUG_MODE:
                    print(f"[{name.upper()}] Top 5: {syms[:5]}")
            else:
                print(f"[{name.upper()}] ⚠️ Nenhum par selecionado")
                
        except Exception as e:
            print(f"[{name.upper()}] ❌ Falha: {e}")
            continue

    if not exchanges:
        raise SystemExit("❌ Nenhuma exchange inicializada.")

    # Stats iniciais
    active_exchanges = list(exchanges.keys())
    total_pairs = sum(len(watchlist.get(ex, [])) for ex in active_exchanges)
    
    print(f
