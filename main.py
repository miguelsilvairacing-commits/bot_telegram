# =========================================================================================
#   CRYPTO ML BOT v3.6 - MANIPULATION FILTER / CLOSED CANDLE
# =========================================================================================
#
# IMPORTANTE:
# - Usa candle FECHADO por defeito para reduzir falsos positivos.
# - Adiciona filtro de manipulação: wick ratio + close position + confirmação da candle seguinte.
# - Mantém Binance-only como base.
# - Mantém cooldown dinâmico por reputação do símbolo.
#
# VARIÁVEIS NOVAS OPCIONAIS NO RAILWAY:
# USE_CLOSED_CANDLE="true"
# ENABLE_MANIPULATION_FILTER="true"
# MAX_WICK_RATIO="0.65"
# MIN_CLOSE_POSITION_PUMP="0.60"
# MAX_CLOSE_POSITION_DUMP="0.40"
# REQUIRE_NEXT_CANDLE_CONFIRMATION="true"
# NEXT_CANDLE_MAX_REVERSAL="0.50"
#
# =========================================================================================

import os
import time
import json
import ccxt
import requests
import numpy as np
from datetime import datetime
from collections import defaultdict, deque
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
import statistics
import threading

# =========================
#   CONFIGURATION
# =========================

BOT_VERSION = "3.6"

MAX_ALERTS_PER_SYMBOL_PER_DAY = int(os.getenv("MAX_ALERTS_PER_DAY", "2"))
MIN_ALERTS_FOR_DYNAMIC_COOLDOWN = 5

# Blacklist via Railway
env_blacklist = os.getenv("SYMBOLS_BLACKLIST", "")
SYMBOLS_BLACKLIST = set()

if env_blacklist:
    parsed = [s.strip() for s in env_blacklist.split(",") if s.strip()]
    SYMBOLS_BLACKLIST.update(parsed)

# Blacklist crítica fixa
SYMBOLS_BLACKLIST_CRITICAL = {
    "DCR/USDT",
    "XVG/USDT",
    "TURTLE/USDT",
}
SYMBOLS_BLACKLIST.update(SYMBOLS_BLACKLIST_CRITICAL)

print(f"⛔ Blacklist: {len(SYMBOLS_BLACKLIST)} símbolos bloqueados")

# =========================
#   SYMBOL REPUTATION SYSTEM
# =========================

class SymbolReputationSystem:
    """
    Sistema de reputação por símbolo.

    Mantém:
    - histórico de resultados por símbolo
    - histórico de volumes de spike por símbolo
    - alertas diários por símbolo

    Decide:
    - cooldown dinâmico
    - se o volume atual é excecional para o símbolo
    - se já atingiu o cap diário
    """

    def __init__(self, data_dir="pattern_data"):
        self.data_dir = data_dir
        self.symbol_history = defaultdict(list)
        self.symbol_volume_history = defaultdict(list)
        self.today_alerts = defaultdict(list)
        self._load()
        print(f"Symbol Reputation System initialized (v{BOT_VERSION})")

    def _load(self):
        try:
            path = os.path.join(self.data_dir, "symbol_reputation.json")
            if os.path.exists(path):
                with open(path, "r") as f:
                    data = json.load(f)
                    self.symbol_history = defaultdict(list, data.get("history", {}))
                    self.symbol_volume_history = defaultdict(list, data.get("vol_history", {}))
                    self.today_alerts = defaultdict(list, data.get("today_alerts", {}))
                total = sum(len(v) for v in self.symbol_history.values())
                print(f"[Reputation] Loaded {len(self.symbol_history)} symbols, {total} records")
        except Exception as e:
            print(f"[Reputation] Load error: {e}")

    def _save(self):
        try:
            if not os.path.exists(self.data_dir):
                os.makedirs(self.data_dir)

            path = os.path.join(self.data_dir, "symbol_reputation.json")
            with open(path, "w") as f:
                json.dump({
                    "history": dict(self.symbol_history),
                    "vol_history": dict(self.symbol_volume_history),
                    "today_alerts": dict(self.today_alerts),
                }, f)
        except Exception as e:
            print(f"[Reputation] Save error: {e}")

    def record_alert(self, symbol: str, volume_x: float):
        today = datetime.now().date().isoformat()
        self.today_alerts[symbol].append({
            "ts": int(time.time()),
            "date": today,
        })

        if volume_x:
            vols = self.symbol_volume_history[symbol]
            vols.append(volume_x)
            if len(vols) > 50:
                self.symbol_volume_history[symbol] = vols[-50:]

        self._save()

    def record_validation(self, symbol: str, result: str, volume_x: float = None):
        self.symbol_history[symbol].append({
            "ts": int(time.time()),
            "result": result,
            "sustained": result in ["SUSTAINED_PUMP", "SUSTAINED_DUMP"],
        })

        if len(self.symbol_history[symbol]) > 30:
            self.symbol_history[symbol] = self.symbol_history[symbol][-30:]

        self._save()

    def get_symbol_accuracy(self, symbol: str) -> tuple:
        history = self.symbol_history.get(symbol, [])
        if len(history) < MIN_ALERTS_FOR_DYNAMIC_COOLDOWN:
            return None, len(history)

        sustained = sum(1 for h in history if h.get("sustained"))
        acc = sustained / len(history) * 100
        return acc, len(history)

    def get_dynamic_cooldown(self, symbol: str) -> int:
        acc, n = self.get_symbol_accuracy(symbol)

        if acc is None:
            return 30

        if acc < 15:
            return 240
        elif acc < 30:
            return 120
        else:
            return 30

    def is_volume_exceptional(self, symbol: str, current_vol: float) -> bool:
        vols = self.symbol_volume_history.get(symbol, [])

        if len(vols) < 10:
            return True

        median_vol = statistics.median(vols)
        threshold = median_vol * 1.5
        is_exceptional = current_vol > threshold

        if not is_exceptional:
            print(
                f"[VOL NORM] {symbol}: vol={current_vol:.1f}x não excecional "
                f"(mediana histórica={median_vol:.1f}x, threshold={threshold:.1f}x)"
            )

        return is_exceptional

    def check_daily_cap(self, symbol: str) -> bool:
        today = datetime.now().date().isoformat()

        self.today_alerts[symbol] = [
            a for a in self.today_alerts[symbol]
            if a.get("date") == today
        ]

        count_today = len(self.today_alerts[symbol])

        if count_today >= MAX_ALERTS_PER_SYMBOL_PER_DAY:
            print(f"[DAILY CAP] {symbol}: {count_today}/{MAX_ALERTS_PER_SYMBOL_PER_DAY} alertas hoje — bloqueado")
            self._save()
            return False

        self._save()
        return True

    def get_reputation_summary(self) -> str:
        lines = []
        blocked_4h = []
        medium = []
        good = []

        for sym in self.symbol_history:
            acc, n = self.get_symbol_accuracy(sym)
            if acc is None:
                continue

            cooldown = self.get_dynamic_cooldown(sym)
            clean = sym.replace("/USDT", "")

            if cooldown >= 240:
                blocked_4h.append(f"{clean} ({acc:.0f}%)")
            elif cooldown >= 120:
                medium.append(f"{clean} ({acc:.0f}%)")
            elif acc >= 30:
                good.append(f"{clean} ({acc:.0f}%)")

        if blocked_4h:
            lines.append(f"🔴 Cooldown 4h: {', '.join(blocked_4h[:5])}")
        if medium:
            lines.append(f"🟡 Cooldown 2h: {', '.join(medium[:5])}")
        if good:
            lines.append(f"🟢 Símbolos bons ≥30%: {', '.join(good[:5])}")

        return "\n".join(lines) if lines else "Dados insuficientes ainda"

# =========================
#   VALIDATION SYSTEM
# =========================

class AlertValidationSystem:
    """
    Validação honesta:
    - Só SUSTAINED_PUMP / SUSTAINED_DUMP contam como acerto.
    - WEAK_CONTINUATION é neutro.
    """

    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.pending_validations = []
        self.validation_results = []
        self.validation_lock = threading.Lock()
        self.last_daily_report = 0

        self._load_existing_data()

        self.validation_thread = threading.Thread(target=self._validation_loop, daemon=True)
        self.validation_thread.start()

        print(f"Alert Validation System initialized (v{BOT_VERSION})")

    def register_alert(self, alert_data: dict):
        validation_record = {
            "alert_id": f"{alert_data['symbol']}_{int(time.time())}",
            "timestamp": int(time.time()),
            "exchange": alert_data["exchange"],
            "symbol": alert_data["symbol"],
            "event_type": alert_data["event_type"],
            "initial_price": alert_data.get("price", 0),
            "volume_multiple": alert_data.get("volume_multiple", 0),
            "strength": alert_data.get("event_strength", 0),
            "price_change_pct": alert_data.get("price_change_pct", 0),
            "rsi": alert_data.get("rsi", None),
            "pre_trend": alert_data.get("pre_trend", "UNKNOWN"),
            "close_position": alert_data.get("close_position", 0),
            "wick_ratio": alert_data.get("wick_ratio", 0),
            "hour_utc": datetime.fromtimestamp(int(time.time())).hour,
            "day_of_week": datetime.fromtimestamp(int(time.time())).weekday(),
            "correlations_count": alert_data.get("correlations_count", 0),
            "cascade_risk": alert_data.get("cascade_risk", 0),
            "market_regime": alert_data.get("market_regime", "UNKNOWN"),
            "btc_price": self.bot.btc_data["last_price"],
            "btc_change_5m": self.bot.btc_data["change_5m"],
            "btc_change_1h": self.bot.btc_data["change_1h"],
            "btc_change_4h": self.bot.btc_data["change_4h"],
            "btc_change_24h": self.bot.btc_data["change_24h"],
            "btc_trend_micro": self.bot.btc_data["trend_micro"],
            "btc_trend_macro": self.bot.btc_data["trend_macro"],
            "btc_volume_spike": self.bot.btc_data.get("volume_spike", 1.0),
            "btc_data_valid": self.bot.btc_data.get("data_valid", False),
            "price_vs_btc_4h": alert_data.get("price_change_pct", 0) - self.bot.btc_data["change_4h"],
            "is_btc_follower": abs(alert_data.get("price_change_pct", 0) - self.bot.btc_data["change_4h"]) < 2.0,
            "movement_type": self._classify_movement(alert_data, self.bot.btc_data),
            "validations": {
                "4h": {"checked": False, "price": None, "result": None, "price_change": None},
                "24h": {"checked": False, "price": None, "result": None, "price_change": None},
            },
        }

        with self.validation_lock:
            self.pending_validations.append(validation_record)

        self._save_pending_validations()
        print(f"[ML-DATA] Alert registered: {alert_data['symbol']} {alert_data['event_type']}")

    def _classify_movement(self, alert_data: dict, btc_data: dict) -> str:
        if not btc_data.get("data_valid", False):
            return "UNKNOWN"

        alert_change = alert_data.get("price_change_pct", 0)
        btc_change_4h = btc_data["change_4h"]
        btc_trend = btc_data["trend_macro"]

        if btc_trend == "LATERAL":
            return "INDEPENDENT"

        if (alert_change > 0 and btc_change_4h > 0) or (alert_change < 0 and btc_change_4h < 0):
            if abs(alert_change) > abs(btc_change_4h) * 1.5:
                return "BTC_OUTPERFORM"
            elif abs(alert_change) > abs(btc_change_4h) * 0.5:
                return "BTC_FOLLOW"
            else:
                return "BTC_UNDERPERFORM"
        else:
            return "BTC_COUNTER"

    def _validation_loop(self):
        while True:
            try:
                time.sleep(300)
                self._check_pending_validations()
                self._check_daily_report()
            except Exception as e:
                print(f"[VALIDATION] Error in loop: {e}")

    def _check_pending_validations(self):
        current_time = int(time.time())

        with self.validation_lock:
            for record in self.pending_validations[:]:
                alert_time = record["timestamp"]

                if not record["validations"]["4h"]["checked"] and current_time >= alert_time + 14400:
                    self._validate_alert(record, "4h", notify=True)

                if not record["validations"]["24h"]["checked"] and current_time >= alert_time + 86400:
                    notify_24h = record["strength"] >= 7
                    self._validate_alert(record, "24h", notify=notify_24h)

                    self.validation_results.append(record)
                    self.pending_validations.remove(record)
                    self._save_results()
                    self._save_pending_validations()

    def _validate_alert(self, record: dict, timeframe: str, notify: bool = True):
        try:
            exchange_name = record["exchange"]
            symbol = record["symbol"]

            if exchange_name not in self.bot.exchanges:
                return

            ex = self.bot.exchanges[exchange_name]
            ticker = ex.fetch_ticker(symbol)
            current_price = ticker["last"]

            initial_price = record["initial_price"]
            price_change_pct = ((current_price - initial_price) / initial_price) * 100 if initial_price > 0 else 0

            validation = record["validations"][timeframe]
            validation["btc_price"] = self.bot.btc_data["last_price"]
            validation["btc_change_4h"] = self.bot.btc_data["change_4h"]
            validation["btc_trend_macro"] = self.bot.btc_data["trend_macro"]
            validation["checked"] = True
            validation["price"] = current_price
            validation["price_change"] = price_change_pct
            validation["validated_at"] = int(time.time())

            result = self._classify_result(record["event_type"], price_change_pct)
            validation["result"] = result

            if timeframe == "4h":
                self.bot.reputation.record_validation(
                    symbol=record["symbol"],
                    result=result,
                    volume_x=record.get("volume_multiple"),
                )

            if notify:
                self._send_validation_report(record, timeframe)

            self._save_pending_validations()

        except Exception as e:
            print(f"[VALIDATION] Error validating {record['symbol']}: {e}")

    def _classify_result(self, event_type: str, price_change_pct: float) -> str:
        if event_type == "PUMP":
            if price_change_pct > 5:
                return "SUSTAINED_PUMP"
            elif price_change_pct > -5:
                return "WEAK_CONTINUATION"
            else:
                return "DUMP_REVERSAL"
        else:
            if price_change_pct < -5:
                return "SUSTAINED_DUMP"
            elif price_change_pct < 5:
                return "WEAK_CONTINUATION"
            else:
                return "PUMP_REVERSAL"

    def _send_validation_report(self, record: dict, timeframe: str):
        validation = record["validations"][timeframe]
        result = validation["result"]

        result_emojis = {
            "SUSTAINED_PUMP": "✅",
            "SUSTAINED_DUMP": "✅",
            "WEAK_CONTINUATION": "⚪",
            "DUMP_REVERSAL": "❌",
            "PUMP_REVERSAL": "❌",
        }

        result_labels = {
            "SUSTAINED_PUMP": "ACERTO — Pump confirmado",
            "SUSTAINED_DUMP": "ACERTO — Dump confirmado",
            "WEAK_CONTINUATION": "NEUTRO — Sem movimento real",
            "DUMP_REVERSAL": "FALHA — Reverteu para baixo",
            "PUMP_REVERSAL": "FALHA — Reverteu para cima",
        }

        emoji = result_emojis.get(result, "⚪")
        label = result_labels.get(result, result)

        msg = f"""📊 <b>VALIDAÇÃO [{timeframe}]</b>

{emoji} <b>{label}</b>

🎯 {record['symbol']} ({record['exchange'].upper()})
📊 {record['event_type']} | ⚡ {record['strength']}/10
💹 {record['volume_multiple']:.1f}x

💰 ${record['initial_price']:.6f} → ${validation['price']:.6f}
📈 {validation['price_change']:+.2f}%"""

        if record.get("movement_type") and record.get("btc_data_valid", False):
            msg += f"\n₿ Movimento: {record['movement_type']}"

        if record.get("pre_trend") and record["pre_trend"] != "UNKNOWN":
            msg += f"\n📉 Tendência prévia: {record['pre_trend']}"

        if record.get("wick_ratio"):
            msg += f"\n🕯️ Wick: {record['wick_ratio']:.2f}"
            msg += f"\n📍 Close position: {record['close_position']:.2f}"

        msg += f"\n\n⏰ Alerta enviado há {timeframe}"

        self.bot.send_telegram(msg)

    def _check_daily_report(self):
        current_time = int(time.time())
        current_hour = datetime.fromtimestamp(current_time).hour

        if (current_time - self.last_daily_report) >= 86400 and current_hour == 10:
            self._send_daily_report()
            self.last_daily_report = current_time

    def _send_daily_report(self):
        cutoff = int(time.time()) - 86400

        # Corrigido: usa validações 4h já feitas, mesmo que ainda estejam pendentes da validação 24h.
        records = list(self.validation_results) + list(self.pending_validations)
        recent = []

        for r in records:
            val_4h = r.get("validations", {}).get("4h", {})
            if val_4h.get("checked") and val_4h.get("validated_at", 0) > cutoff:
                recent.append(r)

        if len(recent) < 3:
            return

        acertos = 0
        neutros = 0
        falhas = 0
        total = 0
        pump_acertos = pump_total = 0
        dump_acertos = dump_total = 0

        for record in recent:
            val_4h = record["validations"].get("4h", {})
            result = val_4h.get("result", "UNKNOWN")
            event_type = record.get("event_type", "UNKNOWN")
            total += 1

            if result in ["SUSTAINED_PUMP", "SUSTAINED_DUMP"]:
                acertos += 1
                if event_type == "PUMP":
                    pump_acertos += 1
                elif event_type == "DUMP":
                    dump_acertos += 1
            elif result == "WEAK_CONTINUATION":
                neutros += 1
            else:
                falhas += 1

            if event_type == "PUMP":
                pump_total += 1
            elif event_type == "DUMP":
                dump_total += 1

        if total == 0:
            return

        acc_real = acertos / total * 100
        pump_acc = (pump_acertos / pump_total * 100) if pump_total > 0 else 0
        dump_acc = (dump_acertos / dump_total * 100) if dump_total > 0 else 0
        total_dataset = len(self.validation_results)

        msg = f"""📊 <b>RELATÓRIO DIÁRIO v{BOT_VERSION}</b>

<b>🎯 Accuracy REAL 4h:</b>
✅ Acertos: {acertos}/{total} = {acc_real:.1f}%
⚪ Neutros: {neutros}/{total} = {neutros/total*100:.1f}%
❌ Falhas:  {falhas}/{total} = {falhas/total*100:.1f}%

<b>Por tipo:</b>
- Pumps: {pump_acc:.1f}% ({pump_acertos}/{pump_total})
- Dumps: {dump_acc:.1f}% ({dump_acertos}/{dump_total})

<b>💾 Dataset ML:</b>
- Total validações concluídas: {total_dataset}
- Binance only ✅
- Candle fechado ✅
- Manipulation filter ✅"""

        rep_summary = self.bot.reputation.get_reputation_summary()
        if rep_summary:
            msg += f"\n\n<b>🎖️ Reputação:</b>\n{rep_summary}"

        self.bot.send_telegram(msg)

    def _save_pending_validations(self):
        try:
            validation_file = os.path.join(self.bot.db.data_dir, "pending_validations.json")
            with open(validation_file, "w") as f:
                json.dump(self.pending_validations, f, indent=2)
        except Exception as e:
            print(f"[VALIDATION] Error saving pending: {e}")

    def _save_results(self):
        try:
            results_file = os.path.join(self.bot.db.data_dir, "validation_results.json")
            with open(results_file, "w") as f:
                json.dump(self.validation_results, f, indent=2)
        except Exception as e:
            print(f"[VALIDATION] Error saving results: {e}")

    def _load_existing_data(self):
        try:
            validation_file = os.path.join(self.bot.db.data_dir, "pending_validations.json")
            if os.path.exists(validation_file):
                with open(validation_file, "r") as f:
                    self.pending_validations = json.load(f)

            results_file = os.path.join(self.bot.db.data_dir, "validation_results.json")
            if os.path.exists(results_file):
                with open(results_file, "r") as f:
                    self.validation_results = json.load(f)

            print(f"[VALIDATION] Loaded {len(self.pending_validations)} pending, {len(self.validation_results)} completed")
        except Exception as e:
            print(f"[VALIDATION] Error loading: {e}")

# =========================
#   DATABASE
# =========================

class FileBasedPatternDB:
    def __init__(self, data_dir="pattern_data"):
        self.data_dir = data_dir
        self.ensure_data_dir()
        self.events_buffer = deque(maxlen=1000)
        self.correlations = {}
        self.sessions = {}
        self._load_existing_data()
        print(f"Pattern database initialized (v{BOT_VERSION})")

    def ensure_data_dir(self):
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

    def _load_existing_data(self):
        try:
            events_file = os.path.join(self.data_dir, "recent_events.json")
            if os.path.exists(events_file):
                with open(events_file, "r") as f:
                    events_data = json.load(f)
                    for event in events_data[-1000:]:
                        self.events_buffer.append(event)

            corr_file = os.path.join(self.data_dir, "correlations.json")
            if os.path.exists(corr_file):
                with open(corr_file, "r") as f:
                    self.correlations = json.load(f)

            print(f"Loaded {len(self.events_buffer)} events, {len(self.correlations)} correlations")
        except Exception as e:
            print(f"Error loading data: {e}")

    def _save_data_periodically(self):
        try:
            events_file = os.path.join(self.data_dir, "recent_events.json")
            with open(events_file, "w") as f:
                json.dump(list(self.events_buffer), f, indent=2)

            corr_file = os.path.join(self.data_dir, "correlations.json")
            with open(corr_file, "w") as f:
                json.dump(self.correlations, f, indent=2)
        except Exception as e:
            print(f"Error saving data: {e}")

    def log_market_event(self, event_data: Dict) -> str:
        timestamp = event_data["timestamp"]
        window_start = (timestamp // 1800) * 1800
        session_id = f"SESSION_{window_start}"

        event_data["session_id"] = session_id
        event_data["logged_at"] = int(time.time())
        self.events_buffer.append(event_data)

        if session_id not in self.sessions:
            self.sessions[session_id] = {
                "session_id": session_id,
                "start_time": window_start,
                "events": [],
                "total_events": 0,
            }

        self.sessions[session_id]["events"].append(event_data)
        self.sessions[session_id]["total_events"] += 1

        if len(self.events_buffer) % 10 == 0:
            self._save_data_periodically()

        return session_id

    def update_symbol_correlation(self, symbol1: str, symbol2: str, correlation_type: str, strength: float, delay: int):
        key = f"{symbol1}|{symbol2}|{correlation_type}"

        if key not in self.correlations:
            self.correlations[key] = {
                "symbol_1": symbol1,
                "symbol_2": symbol2,
                "correlation_type": correlation_type,
                "correlation_strength": strength,
                "time_delay_minutes": delay,
                "sample_size": 1,
                "last_updated": int(time.time()),
            }
        else:
            existing = self.correlations[key]
            existing["correlation_strength"] = (existing["correlation_strength"] + strength) / 2
            existing["time_delay_minutes"] = (existing["time_delay_minutes"] + delay) / 2
            existing["sample_size"] += 1
            existing["last_updated"] = int(time.time())

# =========================
#   CORRELATION ENGINE
# =========================

@dataclass
class MarketEvent:
    timestamp: int
    exchange: str
    symbol: str
    event_type: str
    volume_multiple: float
    price_change_pct: float
    rsi: Optional[float]
    event_strength: int
    pre_trend: str = "UNKNOWN"
    close_position: float = 0.0
    wick_ratio: float = 0.0

@dataclass
class CorrelationPattern:
    symbol_pair: Tuple[str, str]
    correlation_type: str
    strength: float
    time_delay: int
    confidence: float
    sample_size: int

class PatternCorrelationEngine:
    def __init__(self, db: FileBasedPatternDB):
        self.db = db
        self.active_events = deque(maxlen=500)
        self.correlation_threshold = 0.7
        self.time_window_minutes = 30
        print(f"Correlation Engine initialized (v{BOT_VERSION})")

    def process_new_event(self, event: MarketEvent) -> Dict:
        self.active_events.append(event)
        event_data = asdict(event)
        session_id = self.db.log_market_event(event_data)
        correlations_found = self._analyze_correlations(event)
        cascade_risk = self._detect_cascade_risk(event)
        regime = self._assess_market_regime()

        return {
            "session_id": session_id,
            "correlations_found": correlations_found,
            "cascade_risk": cascade_risk["cascade_risk_score"],
            "market_regime": regime["regime"],
        }

    def _analyze_correlations(self, new_event: MarketEvent) -> List[CorrelationPattern]:
        correlations = []
        time_threshold = new_event.timestamp - (self.time_window_minutes * 60)
        recent_by_symbol = defaultdict(list)

        for event in self.active_events:
            if event.timestamp > time_threshold and event.symbol != new_event.symbol:
                recent_by_symbol[event.symbol].append(event)

        for symbol, events in recent_by_symbol.items():
            if len(events) == 0:
                continue

            same_direction = [e for e in events if e.event_type == new_event.event_type]
            if not same_direction:
                continue

            closest = min(same_direction, key=lambda x: abs(x.timestamp - new_event.timestamp))
            time_delay = abs(new_event.timestamp - closest.timestamp) // 60
            time_score = max(0, 1 - (time_delay / self.time_window_minutes))

            if max(new_event.volume_multiple, closest.volume_multiple) > 0:
                volume_score = min(new_event.volume_multiple, closest.volume_multiple) / max(new_event.volume_multiple, closest.volume_multiple)
            else:
                volume_score = 0

            strength = (time_score * 0.6 + volume_score * 0.4)

            if strength >= self.correlation_threshold:
                corr_type = f"{new_event.event_type}_FOLLOW"
                correlations.append(CorrelationPattern(
                    symbol_pair=(closest.symbol, new_event.symbol),
                    correlation_type=corr_type,
                    strength=strength,
                    time_delay=int(time_delay),
                    confidence=strength,
                    sample_size=1,
                ))
                self.db.update_symbol_correlation(new_event.symbol, symbol, corr_type, strength, int(time_delay))

        return correlations

    def _detect_cascade_risk(self, new_event: MarketEvent) -> Dict:
        recent_threshold = new_event.timestamp - 3600
        recent_events = [e for e in self.active_events if e.timestamp > recent_threshold]
        pumps_count = len([e for e in recent_events if e.event_type == "PUMP"])
        dumps_count = len([e for e in recent_events if e.event_type == "DUMP"])
        cascade_risk_score = 0.0

        if pumps_count >= 5:
            cascade_risk_score += 0.4
        if dumps_count >= 5:
            cascade_risk_score += 0.4
        if new_event.event_type == "DUMP" and dumps_count >= 2:
            cascade_risk_score += 0.2

        return {
            "cascade_risk_score": min(cascade_risk_score, 1.0),
            "recent_pumps": pumps_count,
            "recent_dumps": dumps_count,
            "risk_level": "HIGH" if cascade_risk_score > 0.7 else "MEDIUM" if cascade_risk_score > 0.4 else "LOW",
        }

    def _assess_market_regime(self) -> Dict:
        recent_events = list(self.active_events)[-50:]
        if len(recent_events) < 10:
            return {"regime": "INSUFFICIENT_DATA", "confidence": 0.0}

        pumps = [e for e in recent_events if e.event_type == "PUMP"]
        pump_ratio = len(pumps) / len(recent_events)

        if pump_ratio > 0.7:
            regime = "PUMP_MANIPULATION"
        elif pump_ratio < 0.3:
            regime = "DUMP_CASCADE"
        else:
            regime = "MIXED_SIGNALS"

        return {"regime": regime, "confidence": 0.7}

# =========================
#   TRADING BOT
# =========================

class AdvancedPatternTradingBot:
    def __init__(self):
        self.db = FileBasedPatternDB()
        self.correlation_engine = PatternCorrelationEngine(self.db)
        self.exchanges = {}

        configured_exchanges = os.getenv("EXCHANGES", "binance").lower()
        if "bingx" in configured_exchanges:
            print("[CONFIG] ⚠️ EXCHANGES inclui bingx — v3.6 usa apenas Binance.")
        if "binance" not in configured_exchanges:
            print(f"[CONFIG] ⚠️ EXCHANGES='{configured_exchanges}' não inclui binance — a usar binance na mesma.")

        self._initialize_binance()

        self.btc_data = {
            "last_price": 0,
            "change_5m": 0,
            "change_1h": 0,
            "change_4h": 0,
            "change_24h": 0,
            "trend_micro": "LATERAL",
            "trend_macro": "LATERAL",
            "last_update": 0,
            "history": deque(maxlen=800),
            "volume_spike": 1.0,
            "last_volume": 0,
            "data_valid": False,
            "warmup_time": 300,
            "start_time": time.time(),
        }

        self.reputation = SymbolReputationSystem(self.db.data_dir)
        self.validation_system = AlertValidationSystem(self)

        self.watchlist = {}
        self.last_alert_ts = defaultdict(lambda: 0.0)

        self.quote_filter = os.getenv("QUOTE_FILTER", "USDT").split(",")
        self.top_n_by_volume = int(os.getenv("TOP_N_BY_VOLUME", "60"))
        self.timeframe = os.getenv("TIMEFRAME", "1m")

        self.threshold = float(os.getenv("THRESHOLD", "1.8"))
        self.min_price_change = float(os.getenv("MIN_PRICE_CHANGE", "0.015"))
        self.sleep_seconds = int(os.getenv("SLEEP_SECONDS", "20"))
        self.cooldown_minutes = int(os.getenv("COOLDOWN_MINUTES", "30"))
        self.min_strength = int(os.getenv("MIN_STRENGTH", "5"))
        self.debug_mode = os.getenv("DEBUG_MODE", "true").lower() == "true"
        self.btc_adjust_strength = os.getenv("BTC_ADJUST_STRENGTH", "true").lower() == "true"
        self.btc_filter_followers = os.getenv("BTC_FILTER_FOLLOWERS", "false").lower() == "true"

        # Manipulation filter v3.6
        self.use_closed_candle = os.getenv("USE_CLOSED_CANDLE", "true").lower() == "true"
        self.enable_manipulation_filter = os.getenv("ENABLE_MANIPULATION_FILTER", "true").lower() == "true"
        self.max_wick_ratio = float(os.getenv("MAX_WICK_RATIO", "0.65"))
        self.min_close_position_pump = float(os.getenv("MIN_CLOSE_POSITION_PUMP", "0.60"))
        self.max_close_position_dump = float(os.getenv("MAX_CLOSE_POSITION_DUMP", "0.40"))
        self.require_next_candle_confirmation = os.getenv("REQUIRE_NEXT_CANDLE_CONFIRMATION", "true").lower() == "true"
        self.next_candle_max_reversal = float(os.getenv("NEXT_CANDLE_MAX_REVERSAL", "0.50"))

        self.tg_token = os.getenv("TG_TOKEN", "")
        self.tg_chat_id = os.getenv("TG_CHAT_ID", "")
        self.tg_log_chat_id = os.getenv("TG_LOG_CHAT_ID", "")

        self.force_test_alerts = os.getenv("FORCE_TEST_ALERTS", "false").lower() == "true"
        self.test_alert_interval = int(os.getenv("TEST_ALERT_INTERVAL", "300"))
        self.last_test_alert = 0

        self.stats = {
            "alerts_sent": 0,
            "alerts_filtered_rsi": 0,
            "alerts_filtered_price": 0,
            "alerts_filtered_pretrend": 0,
            "alerts_filtered_manipulation": 0,
            "alerts_filtered_next_candle": 0,
            "start_time": time.time(),
        }

        self.btc_thread = threading.Thread(target=self._btc_tracker_loop, daemon=True)
        self.btc_thread.start()

        self.btc_snapshot_thread = threading.Thread(target=self._btc_snapshot_loop, daemon=True)
        self.btc_snapshot_thread.start()

        print(f"Bot v{BOT_VERSION} initialized — Manipulation Filter")

    def _initialize_binance(self):
        try:
            config = {
                "enableRateLimit": True,
                "timeout": 20000,
                "rateLimit": 1000,
                "options": {"adjustForTimeDifference": True},
            }
            ex = ccxt.binance(config)
            ex.load_markets()
            self.exchanges["binance"] = ex
            print("✅ Binance initialized")
        except Exception as e:
            print(f"❌ Failed to initialize Binance: {e}")

    def _validate_btc_change(self, change: float, timeframe: str) -> bool:
        max_change = {
            "5m": 3.0,
            "1h": 8.0,
            "4h": 15.0,
            "24h": 25.0,
        }
        threshold = max_change.get(timeframe, 50.0)
        if abs(change) > threshold:
            print(f"[BTC SANITY] {timeframe}: {change:+.1f}% INVALID (> ±{threshold}%)")
            return False
        return True

    def _btc_snapshot_loop(self):
        while True:
            try:
                time.sleep(1800)

                if not self.btc_data.get("data_valid", False):
                    continue

                snapshot = {
                    "ts": int(time.time()),
                    "price": self.btc_data["last_price"],
                    "c5m": self.btc_data["change_5m"],
                    "c1h": self.btc_data["change_1h"],
                    "c4h": self.btc_data["change_4h"],
                    "c24h": self.btc_data["change_24h"],
                    "macro": self.btc_data["trend_macro"],
                }

                try:
                    snap_file = os.path.join(self.db.data_dir, "btc_snapshot.json")
                    with open(snap_file, "w") as f:
                        json.dump(snapshot, f)
                except Exception:
                    pass

                if self.tg_log_chat_id and self.tg_token:
                    msg = (
                        f"📸 BTC_SNAP|{snapshot['ts']}|{snapshot['price']:.0f}|"
                        f"{snapshot['c5m']:.2f}|{snapshot['c1h']:.2f}|{snapshot['c4h']:.2f}|"
                        f"{snapshot['c24h']:.2f}|{snapshot['macro']}"
                    )
                    try:
                        requests.post(
                            f"https://api.telegram.org/bot{self.tg_token}/sendMessage",
                            json={"chat_id": self.tg_log_chat_id, "text": msg},
                            timeout=10,
                        )
                    except Exception:
                        pass
            except Exception as e:
                print(f"[BTC Snapshot] Error: {e}")

    def _restore_btc_from_snapshot(self):
        try:
            snap_file = os.path.join(self.db.data_dir, "btc_snapshot.json")
            if not os.path.exists(snap_file):
                return False

            with open(snap_file, "r") as f:
                snap = json.load(f)

            age_minutes = (int(time.time()) - snap["ts"]) / 60

            if age_minutes < 120:
                self.btc_data["last_price"] = snap["price"]
                self.btc_data["change_5m"] = snap["c5m"]
                self.btc_data["change_1h"] = snap["c1h"]
                self.btc_data["change_4h"] = snap["c4h"]
                self.btc_data["change_24h"] = snap["c24h"]
                self.btc_data["trend_macro"] = snap["macro"]
                self.btc_data["data_valid"] = True
                print(f"[BTC] Restored from snapshot ({age_minutes:.0f}min ago): ${snap['price']:.0f}")
                return True
            else:
                print(f"[BTC] Snapshot too old ({age_minutes:.0f}min), will wait for fresh data")
                return False
        except Exception as e:
            print(f"[BTC] Snapshot restore error: {e}")
            return False

    def _btc_tracker_loop(self):
        print(f"[BTC Tracker v{BOT_VERSION}] Starting...")
        self._restore_btc_from_snapshot()

        attempts = 0
        while "binance" not in self.exchanges or not self.exchanges.get("binance"):
            time.sleep(2)
            attempts += 1
            if attempts > 30:
                print("[BTC Tracker] ERROR: Binance not available!")
                return

        try:
            ex = self.exchanges["binance"]
            ticker = ex.fetch_ticker("BTC/USDT")
            if self.btc_data["last_price"] == 0:
                self.btc_data["last_price"] = ticker["last"]
            print(f"[BTC Tracker] BTC: ${ticker['last']:.0f}")
        except Exception as e:
            print(f"[BTC Tracker] Init error: {e}")

        while True:
            try:
                ex = self.exchanges["binance"]
                ticker = ex.fetch_ticker("BTC/USDT")
                current_price = ticker["last"]
                current_time = int(time.time())
                current_volume = ticker.get("quoteVolume", 0)

                elapsed = current_time - self.btc_data["start_time"]
                if elapsed >= self.btc_data["warmup_time"]:
                    self.btc_data["data_valid"] = True

                try:
                    ohlcv = ex.fetch_ohlcv("BTC/USDT", "1m", limit=10)
                    if len(ohlcv) >= 5:
                        volumes = [c[5] for c in ohlcv[-5:]]
                        avg_volume = sum(volumes) / len(volumes)
                        last_volume = ohlcv[-1][5]
                        self.btc_data["volume_spike"] = last_volume / avg_volume if avg_volume > 0 else 1.0
                except Exception:
                    self.btc_data["volume_spike"] = 1.0

                self.btc_data["history"].append({
                    "price": current_price,
                    "timestamp": current_time,
                    "volume": current_volume,
                })

                history = list(self.btc_data["history"])

                if len(history) >= 15:
                    lookback_5m = min(15, len(history) - 1)  # 15 pontos x 20s ≈ 5min
                    price_5m_ago = history[-lookback_5m]["price"]
                    change_5m = ((current_price - price_5m_ago) / price_5m_ago) * 100
                    if self._validate_btc_change(change_5m, "5m"):
                        self.btc_data["change_5m"] = change_5m

                if len(history) >= 180:
                    lookback_1h = min(180, len(history) - 1)  # 180 pontos x 20s ≈ 1h
                    price_1h_ago = history[-lookback_1h]["price"]
                    change_1h = ((current_price - price_1h_ago) / price_1h_ago) * 100
                    if self._validate_btc_change(change_1h, "1h"):
                        self.btc_data["change_1h"] = change_1h

                try:
                    resp = requests.get(
                        "https://api.binance.com/api/v3/klines",
                        params={"symbol": "BTCUSDT", "interval": "4h", "limit": 2},
                        timeout=10,
                    )
                    klines = resp.json()
                    if isinstance(klines, list) and len(klines) >= 2:
                        closed_candle = klines[-2]
                        price_4h = float(closed_candle[4])
                        change_4h = ((current_price - price_4h) / price_4h) * 100
                        if self._validate_btc_change(change_4h, "4h"):
                            self.btc_data["change_4h"] = change_4h
                except Exception:
                    if len(history) >= 720:
                        lookback_4h = min(720, len(history) - 1)
                        price_4h_ago = history[-lookback_4h]["price"]
                        change_4h = ((current_price - price_4h_ago) / price_4h_ago) * 100
                        if self._validate_btc_change(change_4h, "4h"):
                            self.btc_data["change_4h"] = change_4h

                try:
                    if ticker.get("percentage") is not None:
                        change_24h = ticker["percentage"]
                        if self._validate_btc_change(change_24h, "24h"):
                            self.btc_data["change_24h"] = change_24h
                except Exception:
                    pass

                if self.btc_data["change_5m"] > 0.3:
                    self.btc_data["trend_micro"] = "UP"
                elif self.btc_data["change_5m"] < -0.3:
                    self.btc_data["trend_micro"] = "DOWN"
                else:
                    self.btc_data["trend_micro"] = "LATERAL"

                change_4h = self.btc_data["change_4h"]
                change_24h = self.btc_data["change_24h"]

                if change_4h > 1.5 or change_24h > 2.0:
                    self.btc_data["trend_macro"] = "STRONG_UP"
                elif change_4h > 0.5 or change_24h > 1.0:
                    self.btc_data["trend_macro"] = "UP"
                elif change_4h < -1.5 or change_24h < -2.0:
                    self.btc_data["trend_macro"] = "STRONG_DOWN"
                elif change_4h < -0.5 or change_24h < -1.0:
                    self.btc_data["trend_macro"] = "DOWN"
                else:
                    self.btc_data["trend_macro"] = "LATERAL"

                self.btc_data["last_price"] = current_price
                self.btc_data["last_update"] = current_time
                self.btc_data["last_volume"] = current_volume

                time.sleep(20)

            except Exception as e:
                print(f"[BTC Tracker] Error: {e}")
                time.sleep(30)

    def should_process_symbol(self, symbol: str) -> bool:
        clean_symbol = symbol.split(":")[0] if ":" in symbol else symbol
        return clean_symbol not in SYMBOLS_BLACKLIST

    def send_telegram(self, msg: str):
        if not self.tg_token or not self.tg_chat_id:
            return
        try:
            requests.post(
                f"https://api.telegram.org/bot{self.tg_token}/sendMessage",
                json={
                    "chat_id": self.tg_chat_id,
                    "text": msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=20,
            )
        except Exception as e:
            print(f"[Telegram] Error: {e}")

    def get_symbols_for_exchange(self, ex, limit: int = 60):
        try:
            tickers = ex.fetch_tickers()
            volume_pairs = []

            for symbol, ticker in tickers.items():
                if not symbol.endswith("/USDT"):
                    continue
                if not self.should_process_symbol(symbol):
                    continue

                volume = ticker.get("quoteVolume")
                if volume and 100_000 <= volume <= 500_000_000:
                    volume_pairs.append((symbol, volume))

            volume_pairs.sort(key=lambda x: x[1], reverse=True)
            return [symbol for symbol, _ in volume_pairs[:limit]]
        except Exception as e:
            print(f"Error getting symbols: {e}")
            return []

    def calculate_rsi(self, prices: List[float], period: int = 14) -> Optional[float]:
        try:
            if len(prices) < period + 1:
                return None

            prices_array = np.array(prices[-(period + 1):])
            deltas = np.diff(prices_array)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            avg_gain = np.mean(gains[-period:])
            avg_loss = np.mean(losses[-period:])

            if avg_loss == 0:
                return 100.0

            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))
            return max(0, min(100, rsi))
        except Exception:
            return None

    def calculate_pre_trend(self, ohlcv: list) -> str:
        try:
            if len(ohlcv) < 5:
                return "UNKNOWN"

            recent = ohlcv[-5:-2]
            ups = 0
            downs = 0

            for candle in recent:
                open_p, close_p = candle[1], candle[4]
                if close_p > open_p * 1.001:
                    ups += 1
                elif close_p < open_p * 0.999:
                    downs += 1

            if ups >= 2:
                return "BULLISH"
            elif downs >= 2:
                return "BEARISH"
            else:
                return "CHOPPY"
        except Exception:
            return "UNKNOWN"

    def calculate_strength_v3(self, vol_multiple: float, price_change_pct: float) -> int:
        if vol_multiple >= 50:
            vol_score = 5
        elif vol_multiple >= 20:
            vol_score = 4
        elif vol_multiple >= 10:
            vol_score = 3
        elif vol_multiple >= 5:
            vol_score = 2
        elif vol_multiple >= 1.8:
            vol_score = 1
        else:
            vol_score = 0

        abs_price = abs(price_change_pct)
        if abs_price >= 8:
            price_score = 5
        elif abs_price >= 5:
            price_score = 4
        elif abs_price >= 3:
            price_score = 3
        elif abs_price >= 2:
            price_score = 2
        elif abs_price >= 1.5:
            price_score = 1
        else:
            price_score = 0

        return vol_score + price_score

    def analyze_candle_quality(self, candle: list, event_type: str) -> tuple:
        """
        Retorna:
        (is_good, reason, close_position, wick_ratio)

        close_position:
        - 1.0 = fechou no topo da candle
        - 0.0 = fechou no fundo da candle

        wick_ratio:
        - percentagem da candle composta por wicks
        """
        try:
            open_p = candle[1]
            high_p = candle[2]
            low_p = candle[3]
            close_p = candle[4]

            candle_range = high_p - low_p
            if candle_range <= 0:
                return False, "invalid_range", 0, 1

            close_position = (close_p - low_p) / candle_range
            upper_wick = high_p - max(open_p, close_p)
            lower_wick = min(open_p, close_p) - low_p
            total_wick = max(0, upper_wick) + max(0, lower_wick)
            wick_ratio = total_wick / candle_range

            if wick_ratio > self.max_wick_ratio:
                return False, "wick_too_large", close_position, wick_ratio

            if event_type == "PUMP" and close_position < self.min_close_position_pump:
                return False, "pump_closed_weak", close_position, wick_ratio

            if event_type == "DUMP" and close_position > self.max_close_position_dump:
                return False, "dump_closed_weak", close_position, wick_ratio

            return True, "good_candle", close_position, wick_ratio
        except Exception as e:
            return False, f"candle_quality_error:{e}", 0, 1

    def fetch_ohlcv_safe(self, ex, symbol: str, timeframe: str, limit: int):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            except ccxt.NetworkError:
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 2)
                    continue
                return None
            except ccxt.ExchangeError as e:
                if "rate limit" in str(e).lower():
                    time.sleep(3)
                    continue
                return None
            except Exception:
                return None
        return None

    def can_alert(self, symbol: str, exchange: str, now_ts: float, volume_x: float = None) -> bool:
        key = f"{exchange}:{symbol}"
        dynamic_cooldown = self.reputation.get_dynamic_cooldown(symbol)
        elapsed = now_ts - self.last_alert_ts[key]

        if elapsed < dynamic_cooldown * 60:
            remaining = (dynamic_cooldown * 60 - elapsed) / 60
            if self.debug_mode and dynamic_cooldown >= 120:
                print(f"[COOLDOWN] {symbol}: {dynamic_cooldown}min cooldown, {remaining:.0f}min restantes")
            return False

        if volume_x is not None and not self.reputation.is_volume_exceptional(symbol, volume_x):
            return False

        if not self.reputation.check_daily_cap(symbol):
            return False

        self.last_alert_ts[key] = now_ts
        return True

    def generate_alert(self, event: MarketEvent, analysis: Dict) -> str:
        trend_emoji = {
            "BULLISH": "📈",
            "BEARISH": "📉",
            "CHOPPY": "〰️",
            "UNKNOWN": "",
        }.get(event.pre_trend, "")

        msg = f"""🚨 <b>{event.event_type} DETECTADO</b>

🎯 <b>{event.symbol}</b> (BINANCE)
⚡ <b>Strength: {event.event_strength}/10</b>
💹 Volume: {event.volume_multiple:.1f}x médio
📈 Preço: {event.price_change_pct:+.1f}%"""

        if event.rsi is not None:
            msg += f"\n📊 RSI: {event.rsi:.0f}"

        if event.pre_trend != "UNKNOWN":
            msg += f"\n{trend_emoji} Tendência prévia: {event.pre_trend}"

        if event.wick_ratio:
            msg += f"\n🕯️ Wick: {event.wick_ratio:.2f}"
            msg += f"\n📍 Close position: {event.close_position:.2f}"

        if self.btc_data.get("data_valid", False):
            btc_1h = self.btc_data["change_1h"]
            btc_4h = self.btc_data["change_4h"]
            btc_24h = self.btc_data["change_24h"]
            trend_macro = self.btc_data["trend_macro"]

            btc_lines = []
            if abs(btc_24h) > 3.0:
                btc_lines.append(f"₿ BTC Dia: {btc_24h:+.1f}%")
            if abs(btc_4h) > 1.5:
                btc_lines.append(f"₿ BTC 4h: {btc_4h:+.1f}%")
            elif abs(btc_1h) > 0.8:
                btc_lines.append(f"₿ BTC 1h: {btc_1h:+.1f}%")

            if trend_macro != "LATERAL" and btc_lines:
                btc_lines.append(f"📊 Trend: {trend_macro}")

            relative_to_4h = event.price_change_pct - btc_4h
            if abs(relative_to_4h) > 2.0:
                if relative_to_4h > 0:
                    btc_lines.append(f"💪 +{abs(relative_to_4h):.1f}% vs BTC")
                else:
                    btc_lines.append(f"⚠️ {relative_to_4h:.1f}% vs BTC")

            if btc_lines:
                msg += "\n\n" + "\n".join(btc_lines)

        msg += f"\n🕐 {datetime.fromtimestamp(event.timestamp).strftime('%H:%M:%S')}"

        correlations = analysis.get("correlations_found", [])
        if correlations:
            msg += f"\n🔗 Correlação: {correlations[0].symbol_pair[0]}"

        cascade = analysis.get("cascade_risk", 0)
        if cascade > 0.5:
            msg += "\n⚠️ Cascade Risk: HIGH"

        return msg

    def run(self):
        try:
            print("🏦 Initializing Binance...")

            for exchange_name, ex in self.exchanges.items():
                symbols = self.get_symbols_for_exchange(ex, self.top_n_by_volume)
                self.watchlist[exchange_name] = symbols
                print(f"✅ {exchange_name}: {len(symbols)} symbols")

            if not self.exchanges:
                raise SystemExit("❌ No exchanges")

            total_symbols = sum(len(s) for s in self.watchlist.values())
            blacklisted = len(SYMBOLS_BLACKLIST)

            startup_msg = f"""🚀 <b>BOT v{BOT_VERSION} — BINANCE ONLY</b>

<b>Manipulation Filter activo</b>

📊 {total_symbols} pares USDT
⛔ {blacklisted} blacklist

<b>🔧 Filtros:</b>
• Volume threshold ≥ {self.threshold:.1f}x
• Price change ≥ {self.min_price_change*100:.1f}%
• Strength ≥ {self.min_strength}
• RSI filter activo
• Pre-trend filter activo
• Candle fechado: {'✅' if self.use_closed_candle else '❌'}
• Manipulation filter: {'✅' if self.enable_manipulation_filter else '❌'}
• Max wick ratio: {self.max_wick_ratio:.2f}
• Pump close position ≥ {self.min_close_position_pump:.2f}
• Dump close position ≤ {self.max_close_position_dump:.2f}
• Next candle confirmation: {'✅' if self.require_next_candle_confirmation else '❌'}

<b>📈 Accuracy honesta:</b>
• Só SUSTAINED conta como acerto
• WEAK_CONTINUATION = Neutro

₿ BTC: {'Snapshot restaurado ✅' if self.btc_data['data_valid'] else 'Aguarda 5min ⏳'}"""

            self.send_telegram(startup_msg)
            time.sleep(5)
            self.run_detection_loop()
        except KeyboardInterrupt:
            print("\n👋 Bot stopped")
        except Exception as e:
            error_msg = f"❌ Bot crashed: {e}"
            print(error_msg)
            self.send_telegram(error_msg)
            raise

    def run_detection_loop(self):
        print(f"🔬 Starting detection v{BOT_VERSION}...")
        loop_count = 0

        while True:
            loop_start = time.time()
            loop_count += 1

            if self.force_test_alerts:
                current_time = time.time()
                if current_time - self.last_test_alert >= self.test_alert_interval:
                    self._send_test_alert()
                    self.last_test_alert = current_time

            if self.debug_mode and loop_count % 50 == 0:
                uptime = (time.time() - self.stats["start_time"]) / 3600
                total_alerts = len(self.validation_system.validation_results) + len(self.validation_system.pending_validations)
                btc_price = self.btc_data["last_price"]
                trend = self.btc_data["trend_macro"]
                valid = "✅" if self.btc_data["data_valid"] else "⏳"
                print(f"[STATS v{BOT_VERSION}] Loop #{loop_count} | Uptime {uptime:.1f}h | Alerts: {total_alerts} | BTC: ${btc_price:.0f} ({trend}) {valid}")
                print(
                    f"  Filtered: RSI={self.stats['alerts_filtered_rsi']} "
                    f"Price={self.stats['alerts_filtered_price']} "
                    f"PreTrend={self.stats['alerts_filtered_pretrend']} "
                    f"Manip={self.stats['alerts_filtered_manipulation']} "
                    f"Next={self.stats['alerts_filtered_next_candle']}"
                )

            for exchange_name, ex in self.exchanges.items():
                symbols = self.watchlist.get(exchange_name, [])

                for symbol in symbols:
                    if not self.should_process_symbol(symbol):
                        continue

                    try:
                        ohlcv = self.fetch_ohlcv_safe(ex, symbol, self.timeframe, 30)
                        if not ohlcv or len(ohlcv) < 16:
                            continue

                        if self.use_closed_candle:
                            # last = última candle fechada; next_candle = candle atual em formação
                            last = ohlcv[-2]
                            next_candle = ohlcv[-1]
                            hist = ohlcv[:-2]
                        else:
                            *hist, last = ohlcv
                            next_candle = None

                        if len(hist) < 10:
                            continue

                        volumes = [c[5] for c in hist[-8:]]
                        vol_avg = sum(volumes) / len(volumes) if volumes else 0
                        vol_last = last[5]
                        close_last = last[4]
                        vol_multiple = vol_last / vol_avg if vol_avg > 0 else 0

                        if vol_multiple > 200:
                            continue

                        prev_close = hist[-1][4]
                        price_change_pct = (close_last - prev_close) / prev_close if prev_close > 0 else 0

                        if vol_multiple < self.threshold:
                            continue

                        if abs(price_change_pct) < self.min_price_change:
                            self.stats["alerts_filtered_price"] += 1
                            continue

                        prices = [c[4] for c in ohlcv]
                        rsi = self.calculate_rsi(prices)
                        event_type = "PUMP" if price_change_pct > 0 else "DUMP"

                        close_position = 0.0
                        wick_ratio = 0.0

                        # Manipulation filter v3.6
                        if self.enable_manipulation_filter:
                            candle_ok, candle_reason, close_position, wick_ratio = self.analyze_candle_quality(last, event_type)

                            if not candle_ok:
                                self.stats["alerts_filtered_manipulation"] += 1
                                if self.debug_mode:
                                    print(
                                        f"[MANIP FILTER] {symbol}: {event_type} blocked "
                                        f"reason={candle_reason} close_pos={close_position:.2f} wick={wick_ratio:.2f}"
                                    )
                                continue

                            if self.require_next_candle_confirmation and next_candle is not None:
                                next_close = next_candle[4]
                                signal_close = last[4]
                                next_move_pct = ((next_close - signal_close) / signal_close) * 100 if signal_close > 0 else 0

                                if event_type == "PUMP" and next_move_pct < -self.next_candle_max_reversal:
                                    self.stats["alerts_filtered_next_candle"] += 1
                                    if self.debug_mode:
                                        print(f"[NEXT CANDLE] {symbol}: PUMP rejected, next candle reversed {next_move_pct:.2f}%")
                                    continue

                                if event_type == "DUMP" and next_move_pct > self.next_candle_max_reversal:
                                    self.stats["alerts_filtered_next_candle"] += 1
                                    if self.debug_mode:
                                        print(f"[NEXT CANDLE] {symbol}: DUMP rejected, next candle bounced {next_move_pct:.2f}%")
                                    continue

                        # RSI filter
                        if rsi is not None:
                            if event_type == "PUMP" and rsi > 75:
                                self.stats["alerts_filtered_rsi"] += 1
                                if self.debug_mode:
                                    print(f"[RSI FILTER] {symbol}: PUMP bloqueado RSI={rsi:.0f}>75")
                                continue

                            if event_type == "DUMP" and rsi < 25:
                                self.stats["alerts_filtered_rsi"] += 1
                                if self.debug_mode:
                                    print(f"[RSI FILTER] {symbol}: DUMP bloqueado RSI={rsi:.0f}<25")
                                continue

                        pre_trend = self.calculate_pre_trend(ohlcv)

                        if event_type == "PUMP" and pre_trend == "BEARISH":
                            self.stats["alerts_filtered_pretrend"] += 1
                            if self.debug_mode:
                                print(f"[TREND FILTER] {symbol}: PUMP bloqueado, pre-trend BEARISH")
                            continue

                        if event_type == "DUMP" and pre_trend == "BULLISH":
                            self.stats["alerts_filtered_pretrend"] += 1
                            if self.debug_mode:
                                print(f"[TREND FILTER] {symbol}: DUMP bloqueado, pre-trend BULLISH")
                            continue

                        base_strength = self.calculate_strength_v3(vol_multiple, price_change_pct * 100)
                        event_strength = base_strength

                        if rsi is not None and 45 <= rsi <= 65:
                            event_strength = max(0, event_strength - 1)

                        if self.btc_adjust_strength and self.btc_data.get("data_valid", False):
                            trend_macro = self.btc_data["trend_macro"]
                            btc_4h = self.btc_data["change_4h"]

                            if trend_macro in ["UP", "STRONG_UP"] and event_type == "PUMP":
                                if abs(price_change_pct * 100 - btc_4h) < 2:
                                    event_strength = int(base_strength * 0.7)
                            elif trend_macro in ["DOWN", "STRONG_DOWN"] and event_type == "PUMP":
                                event_strength = min(10, int(base_strength * 1.3))
                            elif trend_macro in ["DOWN", "STRONG_DOWN"] and event_type == "DUMP":
                                if abs(price_change_pct * 100 - btc_4h) < 2:
                                    event_strength = int(base_strength * 0.7)

                        if self.btc_filter_followers and self.btc_data.get("data_valid", False):
                            btc_4h = self.btc_data["change_4h"]
                            if abs(price_change_pct * 100 - btc_4h) < 1.0:
                                if self.debug_mode:
                                    print(f"[BTC FOLLOWER] {symbol}: blocked as BTC follower")
                                continue

                        if event_strength < self.min_strength:
                            if not hasattr(self, "_strength_debug_count"):
                                self._strength_debug_count = 0
                            if self._strength_debug_count < 5:
                                self._strength_debug_count += 1
                                print(
                                    f"[STRENGTH] {symbol}: {event_type} vol={vol_multiple:.1f}x "
                                    f"price={price_change_pct*100:+.2f}% base_S={base_strength} "
                                    f"final_S={event_strength} < {self.min_strength} (blocked)"
                                )
                            continue

                        event = MarketEvent(
                            timestamp=int(time.time()),
                            exchange=exchange_name,
                            symbol=symbol,
                            event_type=event_type,
                            volume_multiple=vol_multiple,
                            price_change_pct=price_change_pct * 100,
                            rsi=rsi,
                            event_strength=event_strength,
                            pre_trend=pre_trend,
                            close_position=close_position,
                            wick_ratio=wick_ratio,
                        )

                        analysis = self.correlation_engine.process_new_event(event)

                        if self.can_alert(symbol, exchange_name, time.time(), vol_multiple):
                            alert_message = self.generate_alert(event, analysis)
                            self.send_telegram(alert_message)
                            self.stats["alerts_sent"] += 1
                            self.reputation.record_alert(symbol, vol_multiple)

                            alert_data = {
                                "symbol": event.symbol,
                                "exchange": event.exchange,
                                "event_type": event.event_type,
                                "price": close_last,
                                "volume_multiple": vol_multiple,
                                "event_strength": event_strength,
                                "price_change_pct": price_change_pct * 100,
                                "rsi": rsi,
                                "pre_trend": pre_trend,
                                "close_position": close_position,
                                "wick_ratio": wick_ratio,
                                "correlations_count": len(analysis["correlations_found"]),
                                "cascade_risk": analysis["cascade_risk"],
                                "market_regime": analysis["market_regime"],
                            }
                            self.validation_system.register_alert(alert_data)

                            if self.debug_mode:
                                cd = self.reputation.get_dynamic_cooldown(symbol)
                                acc, n = self.reputation.get_symbol_accuracy(symbol)
                                acc_str = f"{acc:.0f}%" if acc is not None else "novo"
                                rsi_str = f"{rsi:.0f}" if rsi is not None else "N/A"
                                print(
                                    f"[ALERT v{BOT_VERSION}] {symbol}: {event_type} S{event_strength}/10 "
                                    f"vol={vol_multiple:.1f}x rsi={rsi_str} trend={pre_trend} "
                                    f"wick={wick_ratio:.2f} close_pos={close_position:.2f} "
                                    f"acc={acc_str} cd={cd}min"
                                )

                    except Exception as e:
                        if self.debug_mode and "rate limit" not in str(e).lower():
                            print(f"Error: {exchange_name} {symbol}: {e}")
                        continue

            elapsed = time.time() - loop_start
            sleep_time = max(0, self.sleep_seconds - elapsed)
            time.sleep(sleep_time)

    def _send_test_alert(self):
        btc_price = self.btc_data["last_price"]
        btc_4h = self.btc_data["change_4h"]
        btc_24h = self.btc_data["change_24h"]
        trend_macro = self.btc_data["trend_macro"]
        data_valid = self.btc_data["data_valid"]

        msg = f"""🧪 <b>TEST v{BOT_VERSION} — Binance Only</b>

₿ ${btc_price:.0f}
4h: {btc_4h:+.2f}% | 24h: {btc_24h:+.2f}%
Trend: {trend_macro}
Valid: {'✅' if data_valid else '⏳ Warming up...'}

<b>Filtros:</b>
🚫 RSI: {self.stats['alerts_filtered_rsi']}
🚫 Price: {self.stats['alerts_filtered_price']}
🚫 Pre-trend: {self.stats['alerts_filtered_pretrend']}
🚫 Manipulation: {self.stats['alerts_filtered_manipulation']}
🚫 Next candle: {self.stats['alerts_filtered_next_candle']}
✅ Alertas enviados: {self.stats['alerts_sent']}

{datetime.now().strftime('%H:%M:%S')}"""

        self.send_telegram(msg)

# =========================
#   MAIN
# =========================

def main():
    print(f"🚀 Bot v{BOT_VERSION} Starting — Manipulation Filter")
    print("🔧 Filtros: candle fechado, wick ratio, close position, next candle confirmation")
    print("📊 Cooldown: acc<15%→4h, acc15-30%→2h, acc≥30%→30min")
    print("📈 Accuracy honesta: só SUSTAINED conta como acerto")

    bot = AdvancedPatternTradingBot()
    bot.run()

if __name__ == "__main__":
    main()
