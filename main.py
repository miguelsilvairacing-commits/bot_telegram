# =========================================================================================
#   CRYPTO ML BOT v2.3.1 - HOTFIX BTC CALCULATIONS! 🔧
# =========================================================================================
# 
# ✅ HOTFIX v2.3.1:
#
# 🔧 BTC SANITY CHECKS (CRITICAL FIX):
#    - Validação: Se BTC > ±50% em qualquer timeframe = INVÁLIDO
#    - Fallback seguro: Usa ticker['percentage'] da exchange
#    - Delay de inicialização: 5min antes de mostrar contexto BTC
#    - Debug logging: Mostra origem dos valores
#
# 🐛 BUG CORRIGIDO:
#    Antes: "₿ BTC Dia: +2154.1%" (impossível!)
#    Agora: Valida e usa fontes confiáveis
#
# 📊 MELHORIAS:
#    - Usa ticker['percentage'] para 24h (mais confiável)
#    - OHLCV só como backup
#    - Marca BTC como "warming up" nos primeiros 5min
#    - Não mostra contexto BTC se dados não confiáveis
#
# =========================================================================================

import os
import time
import json
import ccxt
import requests
import numpy as np
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
import statistics
import threading

# =========================
#   CONFIGURATION
# =========================

env_blacklist = os.getenv("SYMBOLS_BLACKLIST", "")
SYMBOLS_BLACKLIST = set()

if env_blacklist:
    parsed = [s.strip() for s in env_blacklist.split(",") if s.strip()]
    SYMBOLS_BLACKLIST.update(parsed)

SYMBOLS_BLACKLIST_CRITICAL = {
    'SUT/USDT',
    'YB/USDT',
}
SYMBOLS_BLACKLIST.update(SYMBOLS_BLACKLIST_CRITICAL)

print(f"⛔ Blacklist: {len(SYMBOLS_BLACKLIST)} símbolos bloqueados")

# =========================
#   VALIDATION SYSTEM
# =========================
class AlertValidationSystem:
    """Sistema de validação optimizado para colecta de dados ML com BTC multi-timeframe"""
    
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.pending_validations = []
        self.validation_results = []
        self.validation_lock = threading.Lock()
        self.last_daily_report = 0
        
        self.validation_thread = threading.Thread(target=self._validation_loop, daemon=True)
        self.validation_thread.start()
        
        self._load_existing_data()
        
        print("Alert Validation System initialized (v2.3.1 - Hotfix)")
    
    def register_alert(self, alert_data: dict):
        """Registra alerta com TODOS os dados necessários para ML incluindo BTC multi-timeframe"""
        validation_record = {
            'alert_id': f"{alert_data['symbol']}_{int(time.time())}",
            'timestamp': int(time.time()),
            'exchange': alert_data['exchange'],
            'symbol': alert_data['symbol'],
            'event_type': alert_data['event_type'],
            'initial_price': alert_data.get('price', 0),
            'volume_multiple': alert_data.get('volume_multiple', 0),
            'strength': alert_data.get('event_strength', 0),
            'price_change_pct': alert_data.get('price_change_pct', 0),
            'rsi': alert_data.get('rsi', None),
            'hour_utc': datetime.fromtimestamp(int(time.time())).hour,
            'day_of_week': datetime.fromtimestamp(int(time.time())).weekday(),
            'correlations_count': alert_data.get('correlations_count', 0),
            'cascade_risk': alert_data.get('cascade_risk', 0),
            'market_regime': alert_data.get('market_regime', 'UNKNOWN'),
            
            # Bitcoin multi-timeframe context
            'btc_price': self.bot.btc_data['last_price'],
            'btc_change_5m': self.bot.btc_data['change_5m'],
            'btc_change_1h': self.bot.btc_data['change_1h'],
            'btc_change_4h': self.bot.btc_data['change_4h'],
            'btc_change_24h': self.bot.btc_data['change_24h'],
            'btc_trend_micro': self.bot.btc_data['trend_micro'],
            'btc_trend_macro': self.bot.btc_data['trend_macro'],
            'btc_volume_spike': self.bot.btc_data.get('volume_spike', 1.0),
            'btc_data_valid': self.bot.btc_data.get('data_valid', False),
            
            'price_vs_btc_4h': alert_data.get('price_change_pct', 0) - self.bot.btc_data['change_4h'],
            'is_btc_follower': abs(alert_data.get('price_change_pct', 0) - self.bot.btc_data['change_4h']) < 2.0,
            'movement_type': self._classify_movement(alert_data, self.bot.btc_data),
            
            'validations': {
                '1h': {'checked': False, 'price': None, 'result': None, 'price_change': None},
                '4h': {'checked': False, 'price': None, 'result': None, 'price_change': None},
                '24h': {'checked': False, 'price': None, 'result': None, 'price_change': None}
            }
        }
        
        with self.validation_lock:
            self.pending_validations.append(validation_record)
        
        self._save_pending_validations()
        print(f"[ML-DATA] Alert registered: {alert_data['symbol']} {alert_data['event_type']} (BTC: {self.bot.btc_data['trend_macro']})")
    
    def _classify_movement(self, alert_data: dict, btc_data: dict) -> str:
        """Classifica o movimento em relação ao BTC MACRO trend"""
        if not btc_data.get('data_valid', False):
            return 'UNKNOWN'
            
        alert_change = alert_data.get('price_change_pct', 0)
        btc_change_4h = btc_data['change_4h']
        btc_trend = btc_data['trend_macro']
        
        if btc_trend == 'LATERAL':
            return 'INDEPENDENT'
        
        if (alert_change > 0 and btc_change_4h > 0) or (alert_change < 0 and btc_change_4h < 0):
            if abs(alert_change) > abs(btc_change_4h) * 1.5:
                return 'BTC_OUTPERFORM'
            elif abs(alert_change) > abs(btc_change_4h) * 0.5:
                return 'BTC_FOLLOW'
            else:
                return 'BTC_UNDERPERFORM'
        else:
            return 'BTC_COUNTER'
    
    def _validation_loop(self):
        """Loop de validação"""
        while True:
            try:
                time.sleep(300)
                self._check_pending_validations()
                self._check_daily_report()
            except Exception as e:
                print(f"[VALIDATION] Error in loop: {e}")
    
    def _check_pending_validations(self):
        """Verifica alertas pendentes"""
        current_time = int(time.time())
        
        with self.validation_lock:
            for record in self.pending_validations[:]:
                alert_time = record['timestamp']
                
                if not record['validations']['1h']['checked'] and current_time >= alert_time + 3600:
                    self._validate_alert(record, '1h', notify=False)
                
                if not record['validations']['4h']['checked'] and current_time >= alert_time + 14400:
                    self._validate_alert(record, '4h', notify=True)
                
                if not record['validations']['24h']['checked'] and current_time >= alert_time + 86400:
                    notify_24h = record['strength'] >= 7
                    self._validate_alert(record, '24h', notify=notify_24h)
                    
                    self.validation_results.append(record)
                    self.pending_validations.remove(record)
                    self._save_results()
    
    def _validate_alert(self, record: dict, timeframe: str, notify: bool = True):
        """Valida alerta"""
        try:
            exchange_name = record['exchange']
            symbol = record['symbol']
            
            if exchange_name not in self.bot.exchanges:
                return
            
            ex = self.bot.exchanges[exchange_name]
            ticker = ex.fetch_ticker(symbol)
            current_price = ticker['last']
            
            initial_price = record['initial_price']
            price_change_pct = ((current_price - initial_price) / initial_price) * 100 if initial_price > 0 else 0
            
            record['validations'][timeframe]['btc_price'] = self.bot.btc_data['last_price']
            record['validations'][timeframe]['btc_change_1h'] = self.bot.btc_data['change_1h']
            record['validations'][timeframe]['btc_change_4h'] = self.bot.btc_data['change_4h']
            record['validations'][timeframe]['btc_trend_macro'] = self.bot.btc_data['trend_macro']
            
            record['validations'][timeframe]['checked'] = True
            record['validations'][timeframe]['price'] = current_price
            record['validations'][timeframe]['price_change'] = price_change_pct
            
            event_type = record['event_type']
            result = self._classify_result(event_type, price_change_pct, timeframe)
            record['validations'][timeframe]['result'] = result
            record['validations'][timeframe]['validated_at'] = int(time.time())
            
            if notify:
                self._send_validation_report(record, timeframe)
            
            self._save_pending_validations()
            
        except Exception as e:
            print(f"[VALIDATION] Error validating {record['symbol']}: {e}")
    
    def _classify_result(self, event_type: str, price_change_pct: float, timeframe: str) -> str:
        """Classifica resultado da validação"""
        
        if event_type == "PUMP":
            if price_change_pct > 5:
                return "SUSTAINED_PUMP"
            elif price_change_pct > 0:
                return "WEAK_CONTINUATION"
            elif price_change_pct > -5:
                return "SMALL_REVERSAL"
            else:
                return "DUMP_REVERSAL"
        else:
            if price_change_pct < -5:
                return "SUSTAINED_DUMP"
            elif price_change_pct < 0:
                return "WEAK_CONTINUATION"
            elif price_change_pct < 5:
                return "SMALL_REVERSAL"
            else:
                return "PUMP_REVERSAL"
    
    def _send_validation_report(self, record: dict, timeframe: str):
        """Envia relatório de validação"""
        
        validation = record['validations'][timeframe]
        
        result_emojis = {
            'SUSTAINED_PUMP': '✅',
            'SUSTAINED_DUMP': '✅',
            'WEAK_CONTINUATION': '🟡',
            'SMALL_REVERSAL': '🟠',
            'DUMP_REVERSAL': '❌',
            'PUMP_REVERSAL': '❌'
        }
        
        result = validation['result']
        emoji = result_emojis.get(result, '⚪')
        
        msg = f"""📊 <b>VALIDAÇÃO [{timeframe}]</b>

{emoji} <b>{result.replace('_', ' ')}</b>

🎯 {record['symbol']} ({record['exchange'].upper()})
📊 {record['event_type']} | ⚡ {record['strength']}/10
💹 {record['volume_multiple']:.1f}x

💰 ${record['initial_price']:.6f} → ${validation['price']:.6f}
📈 {validation['price_change']:+.2f}%"""

        if record.get('movement_type') and record.get('btc_data_valid', False):
            msg += f"\n\n₿ Movimento: {record['movement_type']}"
            
            if abs(record.get('btc_change_4h', 0)) > 1.5:
                msg += f"\n📊 BTC 4h no alerta: {record['btc_change_4h']:+.1f}%"
            elif abs(record.get('btc_change_1h', 0)) > 0.5:
                msg += f"\n📊 BTC 1h no alerta: {record['btc_change_1h']:+.1f}%"
        
        if record.get('correlations_count', 0) > 0:
            msg += f"\n🔗 Correlações: {record['correlations_count']}"
        
        if record.get('cascade_risk', 0) > 0.5:
            msg += f"\n⚠️ Cascade risk: {record['cascade_risk']:.2f}"
        
        msg += f"\n\n⏰ Alerta enviado há {timeframe}"
        
        self.bot.send_telegram(msg)
    
    def _check_daily_report(self):
        """Verifica se deve enviar relatório diário"""
        current_time = int(time.time())
        current_hour = datetime.fromtimestamp(current_time).hour
        
        if (current_time - self.last_daily_report) >= 86400 and current_hour == 10:
            self._send_daily_report()
            self.last_daily_report = current_time
    
    def _send_daily_report(self):
        """Envia relatório diário"""
        
        cutoff = int(time.time()) - 86400
        recent = [r for r in self.validation_results if r['timestamp'] > cutoff]
        
        if len(recent) < 5:
            return
        
        pump_correct = 0
        pump_total = 0
        dump_correct = 0
        dump_total = 0
        
        btc_followers = 0
        btc_counter = 0
        independent = 0
        
        total_alerts = len(self.validation_results)
        
        for record in recent:
            val_4h = record['validations'].get('4h', {})
            
            if not val_4h.get('checked', False):
                continue
            
            result = val_4h.get('result', 'UNKNOWN')
            event_type = record.get('event_type', 'UNKNOWN')
            
            movement = record.get('movement_type', 'UNKNOWN')
            if movement == 'BTC_FOLLOW':
                btc_followers += 1
            elif movement == 'BTC_COUNTER':
                btc_counter += 1
            elif movement == 'INDEPENDENT':
                independent += 1
            
            if event_type == 'PUMP':
                pump_total += 1
                if result in ['SUSTAINED_PUMP', 'WEAK_CONTINUATION']:
                    pump_correct += 1
            elif event_type == 'DUMP':
                dump_total += 1
                if result in ['SUSTAINED_DUMP', 'WEAK_CONTINUATION']:
                    dump_correct += 1
        
        pump_acc = (pump_correct / pump_total * 100) if pump_total > 0 else 0
        dump_acc = (dump_correct / dump_total * 100) if dump_total > 0 else 0
        overall = ((pump_correct + dump_correct) / (pump_total + dump_total) * 100) if (pump_total + dump_total) > 0 else 0
        
        ml_target = 150
        ml_progress = (total_alerts / ml_target) * 100
        days_remaining = max(0, 14 - (total_alerts / 15))
        
        msg = f"""📊 <b>RELATÓRIO DIÁRIO v2.3.1</b>

<b>🎯 Accuracy 24h:</b>
- Overall: {overall:.1f}% ({pump_correct + dump_correct}/{pump_total + dump_total})
- Pumps: {pump_acc:.1f}% ({pump_correct}/{pump_total})
- Dumps: {dump_acc:.1f}% ({dump_correct}/{dump_total})

<b>₿ BTC Analysis:</b>
- BTC Followers: {btc_followers}
- Counter-trend: {btc_counter}
- Independent: {independent}

<b>📈 Progresso ML:</b>
- Alertas colectados: {total_alerts}/{ml_target}
- Progresso: {ml_progress:.1f}%
- Estimativa: ~{days_remaining:.0f} dias restantes

<b>💾 Dataset Status:</b>
✅ BTC multi-timeframe tracking
✅ Dados ML-ready
✅ Sanity checks implementados"""
        
        if total_alerts >= ml_target:
            msg += f"\n\n🎉 <b>META ATINGIDA!</b>"
        
        self.bot.send_telegram(msg)
    
    def _save_pending_validations(self):
        try:
            validation_file = os.path.join(self.bot.db.data_dir, "pending_validations.json")
            with open(validation_file, 'w') as f:
                json.dump(self.pending_validations, f, indent=2)
        except Exception as e:
            print(f"[VALIDATION] Error saving: {e}")
    
    def _save_results(self):
        try:
            results_file = os.path.join(self.bot.db.data_dir, "validation_results.json")
            with open(results_file, 'w') as f:
                json.dump(self.validation_results, f, indent=2)
        except Exception as e:
            print(f"[VALIDATION] Error saving: {e}")
    
    def _load_existing_data(self):
        try:
            validation_file = os.path.join(self.bot.db.data_dir, "pending_validations.json")
            if os.path.exists(validation_file):
                with open(validation_file, 'r') as f:
                    self.pending_validations = json.load(f)
            
            results_file = os.path.join(self.bot.db.data_dir, "validation_results.json")
            if os.path.exists(results_file):
                with open(results_file, 'r') as f:
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
        
        print("Pattern database initialized (v2.3.1)")
    
    def ensure_data_dir(self):
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def _load_existing_data(self):
        try:
            events_file = os.path.join(self.data_dir, "recent_events.json")
            if os.path.exists(events_file):
                with open(events_file, 'r') as f:
                    events_data = json.load(f)
                    for event in events_data[-1000:]:
                        self.events_buffer.append(event)
            
            corr_file = os.path.join(self.data_dir, "correlations.json")
            if os.path.exists(corr_file):
                with open(corr_file, 'r') as f:
                    self.correlations = json.load(f)
            
            print(f"Loaded {len(self.events_buffer)} events, {len(self.correlations)} correlations")
            
        except Exception as e:
            print(f"Error loading data: {e}")
    
    def _save_data_periodically(self):
        try:
            events_file = os.path.join(self.data_dir, "recent_events.json")
            with open(events_file, 'w') as f:
                json.dump(list(self.events_buffer), f, indent=2)
            
            corr_file = os.path.join(self.data_dir, "correlations.json")
            with open(corr_file, 'w') as f:
                json.dump(self.correlations, f, indent=2)
            
        except Exception as e:
            print(f"Error saving data: {e}")
    
    def log_market_event(self, event_data: Dict) -> str:
        timestamp = event_data['timestamp']
        window_start = (timestamp // 1800) * 1800
        session_id = f"SESSION_{window_start}"
        
        event_data['session_id'] = session_id
        event_data['logged_at'] = int(time.time())
        
        self.events_buffer.append(event_data)
        
        if session_id not in self.sessions:
            self.sessions[session_id] = {
                'session_id': session_id,
                'start_time': window_start,
                'events': [],
                'total_events': 0
            }
        
        self.sessions[session_id]['events'].append(event_data)
        self.sessions[session_id]['total_events'] += 1
        
        if len(self.events_buffer) % 10 == 0:
            self._save_data_periodically()
        
        return session_id
    
    def update_symbol_correlation(self, symbol1: str, symbol2: str, 
                                 correlation_type: str, strength: float, delay: int):
        key = f"{symbol1}|{symbol2}|{correlation_type}"
        
        if key not in self.correlations:
            self.correlations[key] = {
                'symbol_1': symbol1,
                'symbol_2': symbol2,
                'correlation_type': correlation_type,
                'correlation_strength': strength,
                'time_delay_minutes': delay,
                'sample_size': 1,
                'last_updated': int(time.time())
            }
        else:
            existing = self.correlations[key]
            existing['correlation_strength'] = (existing['correlation_strength'] + strength) / 2
            existing['time_delay_minutes'] = (existing['time_delay_minutes'] + delay) / 2
            existing['sample_size'] += 1
            existing['last_updated'] = int(time.time())

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
        
        print("Correlation Engine initialized (v2.3.1)")
    
    def process_new_event(self, event: MarketEvent) -> Dict:
        self.active_events.append(event)
        
        event_data = asdict(event)
        session_id = self.db.log_market_event(event_data)
        
        correlations_found = self._analyze_correlations(event)
        cascade_risk = self._detect_cascade_risk(event)
        regime = self._assess_market_regime()
        
        return {
            'session_id': session_id,
            'correlations_found': correlations_found,
            'cascade_risk': cascade_risk['cascade_risk_score'],
            'market_regime': regime['regime']
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
            
            if new_event.event_type == "PUMP":
                recent_pumps = [e for e in events if e.event_type == "PUMP"]
                if recent_pumps:
                    closest = min(recent_pumps, key=lambda x: abs(x.timestamp - new_event.timestamp))
                    time_delay = abs(new_event.timestamp - closest.timestamp) // 60
                    
                    time_score = max(0, 1 - (time_delay / self.time_window_minutes))
                    volume_score = min(new_event.volume_multiple, closest.volume_multiple) / max(new_event.volume_multiple, closest.volume_multiple)
                    strength = (time_score * 0.6 + volume_score * 0.4)
                    
                    if strength >= self.correlation_threshold:
                        correlations.append(CorrelationPattern(
                            symbol_pair=(closest.symbol, new_event.symbol),
                            correlation_type="PUMP_FOLLOW",
                            strength=strength,
                            time_delay=int(time_delay),
                            confidence=strength,
                            sample_size=1
                        ))
                        self.db.update_symbol_correlation(
                            new_event.symbol, symbol, "PUMP_FOLLOW", strength, int(time_delay)
                        )
        
        return correlations
    
    def _detect_cascade_risk(self, new_event: MarketEvent) -> Dict:
        recent_threshold = new_event.timestamp - 3600
        recent_events = [e for e in self.active_events if e.timestamp > recent_threshold]
        
        pumps_count = len([e for e in recent_events if e.event_type == "PUMP"])
        dumps_count = len([e for e in recent_events if e.event_type == "DUMP"])
        
        cascade_risk_score = 0.0
        
        if pumps_count >= 5:
            cascade_risk_score += 0.4
        
        if new_event.event_type == "DUMP" and dumps_count >= 2:
            cascade_risk_score += 0.4
        
        return {
            'cascade_risk_score': min(cascade_risk_score, 1.0),
            'recent_pumps': pumps_count,
            'recent_dumps': dumps_count,
            'risk_level': 'HIGH' if cascade_risk_score > 0.7 else 'MEDIUM' if cascade_risk_score > 0.4 else 'LOW'
        }
    
    def _assess_market_regime(self) -> Dict:
        recent_events = list(self.active_events)[-50:]
        
        if len(recent_events) < 10:
            return {'regime': 'INSUFFICIENT_DATA', 'confidence': 0.0}
        
        pumps = [e for e in recent_events if e.event_type == "PUMP"]
        pump_ratio = len(pumps) / len(recent_events)
        
        if pump_ratio > 0.7:
            regime = "PUMP_MANIPULATION"
        elif pump_ratio < 0.3:
            regime = "DUMP_CASCADE"
        else:
            regime = "MIXED_SIGNALS"
        
        return {'regime': regime, 'confidence': 0.7}

# =========================
#   TRADING BOT v2.3.1 - HOTFIX
# =========================
class AdvancedPatternTradingBot:
    """Bot v2.3.1 com BTC Multi-Timeframe + Sanity Checks"""
    
    def __init__(self):
        self.db = FileBasedPatternDB()
        self.correlation_engine = PatternCorrelationEngine(self.db)
        
        self.exchanges = {}
        self.exchanges_list = os.getenv("EXCHANGES", "binance,bingx").split(",")
        
        self._initialize_exchanges()
        
        # BTC tracking data - v2.3.1 com sanity checks
        self.btc_data = {
            'last_price': 0,
            'change_5m': 0,
            'change_1h': 0,
            'change_4h': 0,
            'change_24h': 0,
            'trend_micro': 'LATERAL',
            'trend_macro': 'LATERAL',
            'last_update': 0,
            'history': deque(maxlen=200),
            'volume_spike': 1.0,
            'last_volume': 0,
            'data_valid': False,  # NOVO - marca se dados são confiáveis
            'warmup_time': 300,   # NOVO - 5min de warmup
            'start_time': time.time()
        }
        
        self.validation_system = AlertValidationSystem(self)
        
        self.watchlist = {}
        self.last_alert_ts = defaultdict(lambda: 0.0)
        
        self.quote_filter = os.getenv("QUOTE_FILTER", "USDT").split(",")
        self.top_n_by_volume = int(os.getenv("TOP_N_BY_VOLUME", "40"))
        self.timeframe = os.getenv("TIMEFRAME", "1m")
        self.threshold = float(os.getenv("THRESHOLD", "1.8"))
        self.min_price_change = float(os.getenv("MIN_PRICE_CHANGE", "0.015"))
        self.sleep_seconds = int(os.getenv("SLEEP_SECONDS", "20"))
        self.cooldown_minutes = int(os.getenv("COOLDOWN_MINUTES", "20"))
        self.min_strength = int(os.getenv("MIN_STRENGTH", "5"))
        self.debug_mode = os.getenv("DEBUG_MODE", "true").lower() == "true"
        
        self.btc_adjust_strength = os.getenv("BTC_ADJUST_STRENGTH", "true").lower() == "true"
        self.btc_filter_followers = os.getenv("BTC_FILTER_FOLLOWERS", "false").lower() == "true"
        
        self.tg_token = os.getenv("TG_TOKEN", "")
        self.tg_chat_id = os.getenv("TG_CHAT_ID", "")
        
        self.force_test_alerts = os.getenv("FORCE_TEST_ALERTS", "false").lower() == "true"
        self.test_alert_interval = int(os.getenv("TEST_ALERT_INTERVAL", "300"))
        self.last_test_alert = 0
        
        if self.force_test_alerts:
            print(f"🧪 TEST ALERTS ENABLED")
        
        self.stats = {
            'alerts_sent': 0,
            'btc_followers_filtered': 0,
            'start_time': time.time()
        }
        
        self.btc_thread = threading.Thread(target=self._btc_tracker_loop, daemon=True)
        self.btc_thread.start()
        
        print(f"Bot v2.3.1 initialized - Hotfix Applied")
    
    def _initialize_exchanges(self):
        """Inicializa exchanges"""
        for exchange_name in self.exchanges_list:
            exchange_name = exchange_name.strip()
            if not exchange_name:
                continue
            
            try:
                ex = self.build_exchange(exchange_name)
                self.exchanges[exchange_name] = ex
                print(f"✅ Exchange {exchange_name} initialized")
            except Exception as e:
                print(f"❌ Failed to initialize {exchange_name}: {e}")
    
    def _validate_btc_change(self, change: float, timeframe: str) -> bool:
        """
        SANITY CHECK - v2.3.1
        Valida se a mudança de preço BTC faz sentido
        """
        # Thresholds de sanidade por timeframe
        max_change = {
            '5m': 3.0,    # Max 3% em 5min (extremo mas possível)
            '1h': 8.0,    # Max 8% em 1h
            '4h': 15.0,   # Max 15% em 4h
            '24h': 25.0   # Max 25% em 24h
        }
        
        threshold = max_change.get(timeframe, 50.0)
        
        if abs(change) > threshold:
            print(f"[BTC SANITY] {timeframe}: {change:+.1f}% INVALID (> ±{threshold}%)")
            return False
        
        return True
    
    def _btc_tracker_loop(self):
        """
        Monitora BTC/USDT - v2.3.1 HOTFIX
        Com validação de sanidade e fallbacks
        """
        print("[BTC Tracker v2.3.1] Starting with Sanity Checks...")
        
        attempts = 0
        while 'binance' not in self.exchanges or not self.exchanges.get('binance'):
            time.sleep(2)
            attempts += 1
            if attempts > 30:
                print("[BTC Tracker] ERROR: Binance not available!")
                return
        
        print("[BTC Tracker] Binance ready!")
        
        # Inicializa
        try:
            ex = self.exchanges['binance']
            ticker = ex.fetch_ticker('BTC/USDT')
            self.btc_data['last_price'] = ticker['last']
            print(f"[BTC Tracker] Initial BTC: ${ticker['last']:.0f}")
            print(f"[BTC Tracker] Warming up for {self.btc_data['warmup_time']}s...")
        except Exception as e:
            print(f"[BTC Tracker] Init error: {e}")
        
        while True:
            try:
                if 'binance' in self.exchanges:
                    ex = self.exchanges['binance']
                    
                    ticker = ex.fetch_ticker('BTC/USDT')
                    current_price = ticker['last']
                    current_time = int(time.time())
                    current_volume = ticker.get('quoteVolume', 0)
                    
                    # Verifica se passou warmup
                    elapsed = current_time - self.btc_data['start_time']
                    if elapsed >= self.btc_data['warmup_time']:
                        self.btc_data['data_valid'] = True
                    
                    # Volume spike
                    try:
                        ohlcv = ex.fetch_ohlcv('BTC/USDT', '1m', 10)
                        if len(ohlcv) >= 5:
                            volumes = [c[5] for c in ohlcv[-5:]]
                            avg_volume = sum(volumes) / len(volumes)
                            last_volume = ohlcv[-1][5] if ohlcv else current_volume
                            self.btc_data['volume_spike'] = last_volume / avg_volume if avg_volume > 0 else 1.0
                    except Exception:
                        self.btc_data['volume_spike'] = 1.0
                    
                    # Adiciona à história
                    self.btc_data['history'].append({
                        'price': current_price,
                        'timestamp': current_time,
                        'volume': current_volume
                    })
                    
                    # ========================================
                    # CÁLCULOS COM SANITY CHECKS - v2.3.1
                    # ========================================
                    
                    if len(self.btc_data['history']) >= 2:
                        history = list(self.btc_data['history'])
                        
                        # 5 minutos
                        if len(history) >= 5:
                            lookback_5m = min(15, len(history) - 1)
                            price_5m_ago = history[-lookback_5m]['price']
                            change_5m = ((current_price - price_5m_ago) / price_5m_ago) * 100
                            
                            if self._validate_btc_change(change_5m, '5m'):
                                self.btc_data['change_5m'] = change_5m
                            else:
                                self.btc_data['change_5m'] = 0  # Fallback seguro
                        
                        # 1 hora
                        if len(history) >= 20:
                            lookback_1h = min(60, len(history) - 1)
                            price_1h_ago = history[-lookback_1h]['price']
                            change_1h = ((current_price - price_1h_ago) / price_1h_ago) * 100
                            
                            if self._validate_btc_change(change_1h, '1h'):
                                self.btc_data['change_1h'] = change_1h
                            else:
                                self.btc_data['change_1h'] = 0
                        
                        # 4 horas (usa história acumulada)
                        if len(history) >= 60:
                            lookback_4h = min(120, len(history) - 1)
                            price_4h_ago = history[-lookback_4h]['price']
                            change_4h = ((current_price - price_4h_ago) / price_4h_ago) * 100
                            
                            if self._validate_btc_change(change_4h, '4h'):
                                self.btc_data['change_4h'] = change_4h
                            else:
                                # Fallback: tenta OHLCV
                                try:
                                    ohlcv_1h = ex.fetch_ohlcv('BTC/USDT', '1h', 5)
                                    if len(ohlcv_1h) >= 4:
                                        price_4h = ohlcv_1h[-4][4]
                                        change_4h_alt = ((current_price - price_4h) / price_4h) * 100
                                        if self._validate_btc_change(change_4h_alt, '4h'):
                                            self.btc_data['change_4h'] = change_4h_alt
                                        else:
                                            self.btc_data['change_4h'] = 0
                                except Exception:
                                    self.btc_data['change_4h'] = 0
                        
                        # 24 horas - USA TICKER PERCENTAGE (mais confiável!)
                        try:
                            # Primeira opção: ticker percentage (CONFIÁVEL)
                            if 'percentage' in ticker and ticker['percentage'] is not None:
                                change_24h = ticker['percentage']
                                
                                if self._validate_btc_change(change_24h, '24h'):
                                    self.btc_data['change_24h'] = change_24h
                                    if self.debug_mode and current_time % 60 == 0:
                                        print(f"[BTC] 24h from ticker: {change_24h:+.2f}%")
                                else:
                                    print(f"[BTC SANITY] Ticker 24h invalid: {change_24h:+.1f}%")
                                    self.btc_data['change_24h'] = 0
                            else:
                                # Fallback: OHLCV
                                ohlcv_1h = ex.fetch_ohlcv('BTC/USDT', '1h', 25)
                                if len(ohlcv_1h) >= 24:
                                    price_24h_ago = ohlcv_1h[-24][4]
                                    change_24h = ((current_price - price_24h_ago) / price_24h_ago) * 100
                                    
                                    if self._validate_btc_change(change_24h, '24h'):
                                        self.btc_data['change_24h'] = change_24h
                                    else:
                                        self.btc_data['change_24h'] = 0
                                else:
                                    self.btc_data['change_24h'] = 0
                                    
                        except Exception as e:
                            if self.debug_mode:
                                print(f"[BTC] 24h calc error: {e}")
                            self.btc_data['change_24h'] = 0
                        
                        # Trend MICRO (5min)
                        if self.btc_data['change_5m'] > 0.3:
                            self.btc_data['trend_micro'] = 'UP'
                        elif self.btc_data['change_5m'] < -0.3:
                            self.btc_data['trend_micro'] = 'DOWN'
                        else:
                            self.btc_data['trend_micro'] = 'LATERAL'
                        
                        # Trend MACRO (4h + 24h)
                        change_4h = self.btc_data['change_4h']
                        change_24h = self.btc_data['change_24h']
                        
                        if abs(change_4h) > 2.0 or abs(change_24h) > 3.0:
                            if change_4h > 1.5 or change_24h > 2.0:
                                self.btc_data['trend_macro'] = 'STRONG_UP'
                            elif change_4h > 0.5 or change_24h > 1.0:
                                self.btc_data['trend_macro'] = 'UP'
                            elif change_4h < -1.5 or change_24h < -2.0:
                                self.btc_data['trend_macro'] = 'STRONG_DOWN'
                            elif change_4h < -0.5 or change_24h < -1.0:
                                self.btc_data['trend_macro'] = 'DOWN'
                            else:
                                self.btc_data['trend_macro'] = 'LATERAL'
                        else:
                            self.btc_data['trend_macro'] = 'LATERAL'
                    
                    self.btc_data['last_price'] = current_price
                    self.btc_data['last_update'] = current_time
                    self.btc_data['last_volume'] = current_volume
                    
                    # Log detalhado
                    if self.debug_mode and len(self.btc_data['history']) % 30 == 0:
                        print(f"[BTC] ${current_price:.0f} | Valid: {self.btc_data['data_valid']}")
                        print(f"  5m: {self.btc_data['change_5m']:+.2f}% | 1h: {self.btc_data['change_1h']:+.2f}%")
                        print(f"  4h: {self.btc_data['change_4h']:+.2f}% | 24h: {self.btc_data['change_24h']:+.2f}%")
                        print(f"  Macro: {self.btc_data['trend_macro']}")
                
                time.sleep(20)
                
            except Exception as e:
                print(f"[BTC Tracker] Error: {e}")
                time.sleep(30)
    
    def should_process_symbol(self, symbol: str) -> bool:
        """Verifica blacklist"""
        clean_symbol = symbol.split(':')[0] if ':' in symbol else symbol
        
        if clean_symbol in SYMBOLS_BLACKLIST:
            if self.debug_mode:
                print(f"⛔ {clean_symbol} BLOCKED")
            return False
        return True
    
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
                    "disable_web_page_preview": True
                },
                timeout=20
            )
        except Exception as e:
            print(f"[Telegram] Error: {e}")
    
    def _send_test_alert(self):
        """Test alert v2.3.1"""
        
        btc_price = self.btc_data['last_price']
        btc_5m = self.btc_data['change_5m']
        btc_1h = self.btc_data['change_1h']
        btc_4h = self.btc_data['change_4h']
        btc_24h = self.btc_data['change_24h']
        trend_macro = self.btc_data['trend_macro']
        data_valid = self.btc_data['data_valid']
        
        msg = f"""🧪 <b>TEST v2.3.1 - Hotfix</b>

₿ ${btc_price:.0f}

5m: {btc_5m:+.2f}%
1h: {btc_1h:+.2f}%
4h: {btc_4h:+.2f}%
24h: {btc_24h:+.2f}%

Trend: {trend_macro}
Valid: {'✅' if data_valid else '⏳ Warming up...'}

{datetime.now().strftime('%H:%M:%S')}"""
        
        self.send_telegram(msg)
        print(f"[TEST] Alert sent")
    
    def build_exchange(self, name: str):
        name = name.strip().lower()
        if not hasattr(ccxt, name):
            raise ValueError(f"Exchange '{name}' not found")
        
        config = {"enableRateLimit": True, "options": {"adjustForTimeDifference": True}}
        
        if name == "bingx":
            config.update({"timeout": 30000, "rateLimit": 1500})
        elif name == "binance":
            config.update({"timeout": 20000, "rateLimit": 1000})
        
        ex = getattr(ccxt, name)(config)
        ex.load_markets()
        return ex
    
    def get_symbols_for_exchange(self, ex, limit: int = 40):
        try:
            tickers = ex.fetch_tickers()
            volume_pairs = []
            
            for symbol, ticker in tickers.items():
                if not any(symbol.endswith("/" + q) for q in self.quote_filter):
                    continue
                
                if not self.should_process_symbol(symbol):
                    continue
                    
                volume = ticker.get("quoteVolume")
                if volume and 100_000 <= volume <= 100_000_000:
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
            
            prices_array = np.array(prices[-(period+1):])
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
    
    def fetch_ohlcv_safe(self, ex, symbol: str, timeframe: str, limit: int):
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                if ex.id.lower() == 'bingx':
                    actual_limit = min(limit, 100)
                    return ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=actual_limit)
                else:
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
    
    def can_alert(self, key: str, now_ts: float) -> bool:
        if (now_ts - self.last_alert_ts[key]) >= self.cooldown_minutes * 60:
            self.last_alert_ts[key] = now_ts
            return True
        return False
    
    def generate_alert(self, event: MarketEvent, analysis: Dict) -> str:
        """Alerta v2.3.1 com validação"""
        
        msg = f"""🚨 <b>{event.event_type} DETECTADO</b>

🎯 <b>{event.symbol}</b> ({event.exchange.upper()})
⚡ <b>Strength: {event.event_strength}/10</b>
💹 Volume: {event.volume_multiple:.1f}x médio
📈 Preço: {event.price_change_pct:+.1f}%"""

        # SÓ MOSTRA BTC SE DADOS VÁLIDOS - v2.3.1
        if self.btc_data.get('data_valid', False):
            btc_5m = self.btc_data['change_5m']
            btc_1h = self.btc_data['change_1h']
            btc_4h = self.btc_data['change_4h']
            btc_24h = self.btc_data['change_24h']
            trend_macro = self.btc_data['trend_macro']
            
            show_btc = False
            btc_lines = []
            
            # Decide o que mostrar
            if abs(btc_24h) > 3.0:
                show_btc = True
                btc_lines.append(f"₿ BTC Dia: {btc_24h:+.1f}%")
                if abs(btc_4h) > 1.5:
                    btc_lines.append(f"₿ BTC 4h: {btc_4h:+.1f}%")
            elif abs(btc_4h) > 1.5:
                show_btc = True
                btc_lines.append(f"₿ BTC 4h: {btc_4h:+.1f}%")
            elif abs(btc_1h) > 0.8:
                show_btc = True
                btc_lines.append(f"₿ BTC 1h: {btc_1h:+.1f}%")
            
            if trend_macro != 'LATERAL' and show_btc:
                btc_lines.append(f"📊 Trend: {trend_macro}")
            
            relative_to_4h = event.price_change_pct - btc_4h
            
            if show_btc:
                msg += "\n"
                for line in btc_lines:
                    msg += f"\n{line}"
                
                if abs(relative_to_4h) > 2.0:
                    if relative_to_4h > 0:
                        msg += f"\n💪 Acima BTC (+{abs(relative_to_4h):.1f}%)"
                    else:
                        msg += f"\n⚠️ Abaixo BTC ({relative_to_4h:.1f}%)"
                elif abs(event.price_change_pct - btc_4h) < 1.0:
                    msg += f"\n📊 Segue BTC"
        
        msg += f"\n🕐 {datetime.fromtimestamp(event.timestamp).strftime('%H:%M:%S')}"

        correlations = analysis.get('correlations_found', [])
        if correlations:
            msg += f"\n🔗 Correlação: {correlations[0].symbol_pair[0]}"

        cascade = analysis.get('cascade_risk', 0)
        if cascade > 0.5:
            msg += f"\n⚠️ Cascade Risk: HIGH"

        return msg
    
    def run(self):
        try:
            print("🏦 Initializing exchanges...")
            
            for exchange_name, ex in self.exchanges.items():
                symbols = self.get_symbols_for_exchange(ex, self.top_n_by_volume)
                self.watchlist[exchange_name] = symbols
                print(f"✅ {exchange_name}: {len(symbols)} symbols")
            
            if not self.exchanges:
                raise SystemExit("❌ No exchanges")
            
            total_symbols = sum(len(s) for s in self.watchlist.values())
            blacklisted = len(SYMBOLS_BLACKLIST)
            
            startup_msg = f"""🚀 <b>BOT v2.3.1 - HOTFIX</b> 🔧

<b>Sanity Checks Implementados!</b>

🏦 {', '.join(self.exchanges.keys())}
📊 {total_symbols} moedas
⛔ {blacklisted} blacklist

₿ <b>BTC v2.3.1:</b>
   • Validação de valores
   • Fallback seguro
   • 5min warmup
   • Dados confiáveis

Aguarda 5min para contexto BTC! ⏳"""
            
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
        """Loop de detecção v2.3.1"""
        print("🔬 Starting detection v2.3.1...")
        
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
                uptime = (time.time() - self.stats['start_time']) / 3600
                total_alerts = len(self.validation_system.validation_results) + len(self.validation_system.pending_validations)
                btc_price = self.btc_data['last_price']
                trend = self.btc_data['trend_macro']
                valid = '✅' if self.btc_data['data_valid'] else '⏳'
                print(f"[STATS] Loop #{loop_count} | Alerts: {total_alerts} | BTC: ${btc_price:.0f} ({trend}) {valid}")
            
            for exchange_name, ex in self.exchanges.items():
                symbols = self.watchlist.get(exchange_name, [])
                
                for symbol in symbols:
                    if not self.should_process_symbol(symbol):
                        continue
                    
                    try:
                        ohlcv = self.fetch_ohlcv_safe(ex, symbol, self.timeframe, 20)
                        if not ohlcv or len(ohlcv) < 10:
                            continue
                        
                        *hist, last = ohlcv
                        volumes = [c[5] for c in hist[-8:]]
                        vol_avg = sum(volumes) / len(volumes) if volumes else 0
                        vol_last = last[5]
                        close_last = last[4]
                        
                        vol_multiple = vol_last / vol_avg if vol_avg > 0 else 0
                        
                        if vol_multiple > 100:
                            continue
                        
                        price_change_pct = 0
                        if len(hist) > 0:
                            prev_close = hist[-1][4]
                            price_change_pct = (close_last - prev_close) / prev_close if prev_close > 0 else 0
                        
                        if vol_multiple < self.threshold or abs(price_change_pct) < self.min_price_change:
                            continue
                        
                        prices = [c[4] for c in ohlcv]
                        rsi = self.calculate_rsi(prices)
                        
                        event_type = "PUMP" if price_change_pct > 0 else "DUMP"
                        
                        base_strength = min(int((vol_multiple / 2 + abs(price_change_pct) * 20)), 10)
                        
                        event_strength = base_strength
                        
                        # Ajusta com BTC se dados válidos
                        if self.btc_adjust_strength and self.btc_data.get('data_valid', False):
                            trend_macro = self.btc_data['trend_macro']
                            btc_4h = self.btc_data['change_4h']
                            
                            if trend_macro in ['UP', 'STRONG_UP'] and event_type == 'PUMP':
                                if abs(price_change_pct*100 - btc_4h) < 2:
                                    event_strength = int(base_strength * 0.7)
                                    
                            elif trend_macro in ['DOWN', 'STRONG_DOWN'] and event_type == 'PUMP':
                                event_strength = min(10, int(base_strength * 1.3))
                            
                            elif trend_macro in ['DOWN', 'STRONG_DOWN'] and event_type == 'DUMP':
                                if abs(price_change_pct*100 - btc_4h) < 2:
                                    event_strength = int(base_strength * 0.7)
                        
                        if event_strength < self.min_strength:
                            continue
                        
                        event = MarketEvent(
                            timestamp=int(time.time()),
                            exchange=exchange_name,
                            symbol=symbol,
                            event_type=event_type,
                            volume_multiple=vol_multiple,
                            price_change_pct=price_change_pct * 100,
                            rsi=rsi,
                            event_strength=event_strength
                        )
                        
                        analysis = self.correlation_engine.process_new_event(event)
                        
                        alert_key = f"{exchange_name}:{symbol}"
                        
                        if self.can_alert(alert_key, time.time()):
                            alert_message = self.generate_alert(event, analysis)
                            self.send_telegram(alert_message)
                            self.stats['alerts_sent'] += 1
                            
                            alert_data = {
                                'symbol': event.symbol,
                                'exchange': event.exchange,
                                'event_type': event.event_type,
                                'price': close_last,
                                'volume_multiple': vol_multiple,
                                'event_strength': event_strength,
                                'price_change_pct': price_change_pct * 100,
                                'rsi': rsi,
                                'correlations_count': len(analysis['correlations_found']),
                                'cascade_risk': analysis['cascade_risk'],
                                'market_regime': analysis['market_regime']
                            }
                            self.validation_system.register_alert(alert_data)
                            
                            if self.debug_mode:
                                print(f"[ALERT] {symbol}: {event_type} {event_strength}/10")
                    
                    except Exception as e:
                        if self.debug_mode and "rate limit" not in str(e).lower():
                            print(f"Error: {exchange_name} {symbol}: {e}")
                        continue
            
            elapsed = time.time() - loop_start
            sleep_time = max(0, self.sleep_seconds - elapsed)
            time.sleep(sleep_time)

# =========================
#   MAIN
# =========================
def main():
    print("🚀 Bot v2.3.1 Starting - HOTFIX Applied!")
    print("🔧 Sanity checks on BTC calculations")
    print("✅ Validation: Max ±25% in 24h")
    print("📊 Fallback: ticker percentage")
    print("⏳ Warmup: 5min before showing BTC context")
    
    bot = AdvancedPatternTradingBot()
    bot.run()

if __name__ == "__main__":
    main()
