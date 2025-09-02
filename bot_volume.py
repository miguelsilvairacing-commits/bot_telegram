# bot_volume.py
import os
import time
import math
from collections import defaultdict
from datetime import datetime, timezone
import requests
import ccxt

# =========================
#   CONFIGURAÇÃO (ENV)
# =========================
EXCHANGES = os.getenv("EXCHANGES", "binance,bybit,okx").split(",")
QUOTE_FILTER = os.getenv("QUOTE_FILTER", "USDT")     # "" = todos

# Filtro por volume 24h (para apanhar small/micro caps e cortar majors)
QV24H_MIN_USD = float(os.getenv("QV24H_MIN_USD", "0"))          # ex.: 5_000_000
QV24H_MAX_USD = float(os.getenv("QV24H_MAX_USD", "1e18"))       # ex.: 200_000_000
SYMBOLS_BLACKLIST = set(
    s.strip() for s in os.getenv("SYMBOLS_BLACKLIST", "").split(",") if s.strip()
)

TOP_N_BY_VOLUME = int(os.getenv("TOP_N_BY_VOLUME", "30"))

TIMEFRAME = os.getenv("TIMEFRAME", "4h")             # ex.: 1m,5m,15m,1h,4h,1d
LOOKBACK = int(os.getenv("LOOKBACK", "20"))          # média de volume
THRESHOLD = float(os.getenv("THRESHOLD", "10"))      # múltiplo p/ spike (ex.: 10×)

# Stop hunting
SH_WICK_BODY_RATIO = float(os.getenv("SH_WICK_BODY_RATIO", "2.5"))
SH_WICK_RANGE_PCT  = float(os.getenv("SH_WICK_RANGE_PCT", "0.6"))
SH_MIN_RETRACE_PCT = float(os.getenv("SH_MIN_RETRACE_PCT", "0.5"))
SH_USE_VOLUME      = os.getenv("SH_USE_VOLUME", "true").lower() == "true"
SH_VOL_MULT        = float(os.getenv("SH_VOL_MULT", "3.0"))

# Ciclo e cooldowns
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "30"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "30"))

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

# ========= Filtro por INTERVALO de volume 24h (small/micro caps) =========
def pick_symbols_by_24h_volume(ex, top_n=30, quote=QUOTE_FILTER):
    """
    Escolhe pares pela janela de volume 24h, com filtros:
    - quote (ex.: .../USDT)
    - intervalo de volume 24h [QV24H_MIN_USD, QV24H_MAX_USD]
    - blacklist de majors
    - limita a top_n após o filtro
    """
    markets = ex.load_markets()
    try:
        tickers = ex.fetch_tickers()
    except Exception:
        # fallback: usa mercados activos e aplica só quote e blacklist
        symbols = [s for s, m in markets.items() if m.get("active")]
        if quote:
            symbols = [s for s in symbols if s.endswith("/" + quote)]
        symbols = [s for s in symbols if s not in SYMBOLS_BLACKLIST]
        return symbols[:top_n]

    rows = []
    for sym, t in tickers.items():
        if quote and not sym.endswith("/" + quote):
            continue
        if sym in SYMBOLS_BLACKLIST:
            continue

        # quoteVolume pode vir em t['quoteVolume'] ou t['info']['quoteVolume']
        vol_q = t.get("quoteVolume") or (t.get("info") or {}).get("quoteVolume")
        try:
            vol_q = float(vol_q) if vol_q is not None else None
        except Exception:
            vol_q = None
        if vol_q is None:
            continue

        # filtro por intervalo (evita majors e micros ilíquidas)
        if vol_q < QV24H_MIN_USD or vol_q > QV24H_MAX_USD:
            continue

        rows.append((sym, vol_q))

    # ordenar por volume (dentro do intervalo) e cortar a top_n
    rows.sort(key=lambda x: x[1], reverse=True)
    return [sym for sym, _ in rows[:top_n]]

def fetch_ohlcv_safe(ex, symbol, timeframe, limit):
    return ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)

# =========================
#   STOP HUNTING DETECTOR
# =========================
def candle_parts(c):
    # ccxt OHLCV: [ts, o, h, l, c, v]
    o, h, l, cl, v = c[1], c[2], c[3], c[4], c[5]
    rng  = max(1e-12, h - l)
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
    """
    Devolve (found: bool, side: 'down'|'up'|None, info: dict)
    Usa a última vela e a anterior para confirmar retracção/absorção.
    """
    if len(ohlcv) < max(lookback_for_vol, 3):
        return False, None, {}

    prev = ohlcv[-2]
    last = ohlcv[-1]

    o, h, l, cl, v, rng, body, upper, lower = candle_parts(last)
    po, ph, pl, pcl, pv, prng, pbody, pupper, plower = candle_parts(prev)

    vols = [c[5] for c in ohlcv[-(lookback_for_vol+1):-1]]
    vol_avg = sum(vols)/len(vols) if vols else 0.0
    vol_ok = (not use_volume) or (vol_avg > 0 and v >= vol_mult * vol_avg)

    # Retracções (0..1)
    retrace_from_low  = (cl - l) / rng if rng > 0 else 0.0   # 1 = fechou perto do topo
    retrace_from_high = (h - cl) / rng if rng > 0 else 0.0   # 1 = fechou perto do fundo

    # Condições geométricas
    cond_down = (lower >= wick_body_ratio * body) and (lower >= wick_range_pct * rng)
    cond_up   = (upper >= wick_body_ratio * body) and (upper >= wick_range_pct * rng)

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
#   ALERT COOLDOWNS
# =========================
last_alert_ts = defaultdict(lambda: 0.0)  # chave: tipo:name:symbol

def can_alert(key: str, now_ts: float) -> bool:
    last = last_alert_ts[key]
    if (now_ts - last) >= COOLDOWN_MINUTES * 60:
        last_alert_ts[key] = now_ts
        return True
    return False

# =========================
#   MAIN LOOP
# =========================
def main():
    print("A iniciar bot…")
    # Instanciar exchanges e watchlists
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
            print(f"[{name}] A monitorizar {len(syms)} pares. Ex.: {syms[:8]}")
        except Exception as e:
            print(f"[{name}] Falha na preparação: {e}")

    if not exchanges:
        raise SystemExit("Nenhuma exchange válida configurada.")

    send_telegram("✅ Bot de volume iniciado. Vou avisar sobre spikes e possíveis stop hunts.")

    while True:
        loop_start = time.time()
        for name, ex in exchanges.items():
            symbols = watchlist.get(name, [])
            for sym in symbols:
                try:
                    # Buscar N+1 velas para média + última
                    ohlcv = fetch_ohlcv_safe(ex, sym, TIMEFRAME, limit=LOOKBACK + 1)
                    if not ohlcv or len(ohlcv) < LOOKBACK + 1:
                        continue

                    *hist, last = ohlcv
                    volumes = [c[5] for c in hist[-LOOKBACK:]]
                    vol_avg = (sum(volumes) / len(volumes)) if volumes else 0.0
                    vol_last = last[5]
                    close_last = last[4]
                    ts_last = last[0]

                    # ---------- Alerta: SPIKE DE VOLUME ----------
                    if vol_avg > 0 and vol_last >= THRESHOLD * vol_avg:
                        multiple = vol_last / vol_avg
                        key = f"SPIKE:{name}:{sym}"
                        if can_alert(key, time.time()):
                            msg = (
                                f"🚨 <b>Volume spike</b>\n"
                                f"Exchange: <b>{name}</b>\n"
                                f"Par: <b>{sym}</b> • TF: <b>{TIMEFRAME}</b>\n"
                                f"Hora vela: <b>{ts_iso(ts_last)}</b>\n"
                                f"Preço fecho: <b>{close_last}</b>\n"
                                f"Volume: <b>{short_num(vol_last)}</b> "
                                f"(~<b>{multiple:.2f}×</b> acima da média {LOOKBACK})"
                            )
                            send_telegram(msg)

                    # ---------- Alerta: STOP HUNTING ----------
                    sh_found, sh_side, sh = detect_stop_hunt(
                        ohlcv,
                        wick_body_ratio=SH_WICK_BODY_RATIO,
                        wick_range_pct=SH_WICK_RANGE_PCT,
                        min_retrace_pct=SH_MIN_RETRACE_PCT,
                        use_volume=SH_USE_VOLUME,
                        vol_mult=SH_VOL_MULT,
                        lookback_for_vol=LOOKBACK
                    )
                    if sh_found:
                        key = f"STOPHUNT:{sh_side}:{name}:{sym}"
                        if can_alert(key, time.time()):
                            emoji = "🩸" if sh_side == "down" else "🧨"
                            msg = (
                                f"{emoji} <b>Possível STOP HUNT</b>\n"
                                f"Exchange: <b>{name}</b>\n"
                                f"Par: <b>{sym}</b> • TF: <b>{TIMEFRAME}</b>\n"
                                f"Hora vela: <b>{ts_iso(ohlcv[-1][0])}</b>\n"
                                f"Direcção: <b>{'para baixo (pavio inferior)' if sh_side=='down' else 'para cima (pavio superior)'}</b>\n"
                                f"Fecho: <b>{sh['c']}</b> | High/Low: <b>{sh['h']}/{sh['l']}</b>\n"
                                f"Pavio sup/inf: <b>{sh['upper']:.6f}/{sh['lower']:.6f}</b> | Corpo: <b>{sh['body']:.6f}</b>\n"
                                f"Retracção: <b>{(sh['retrace_from_low']*100):.0f}%</b> (de baixo) / "
                                f"<b>{(sh['retrace_from_high']*100):.0f}%</b> (de cima)\n"
                                + (f"Volume: <b>{short_num(sh['v'])}</b> (~<b>{sh['vol_mult']:.2f}×</b> média) " if sh.get('vol_avg') else "")
                            )
                            send_telegram(msg)

                except ccxt.NetworkError:
                    # Intermitências da API: ignora e segue
                    continue
                except Exception as e:
                    print(f"[{name}] Erro em {sym}: {e}")
                    continue

        # Pausa respeitando rate limits
        elapsed = time.time() - loop_start
        time.sleep(max(0, SLEEP_SECONDS - elapsed))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Encerrado.")
