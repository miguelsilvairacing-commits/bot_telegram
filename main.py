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
import hashlib
import statistics
import threading

# =========================
#   AUTO-VALIDATION SYSTEM (OPTIMIZED)
# =========================
class AlertValidationSystem:
    """Sistema que valida automaticamente alertas e envia resumos diários"""
    
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.pending_validations = []
        self.validation_results = []
        self.validation_lock = threading.Lock()
        self.last_daily_report = 0
        
        self.validation_thread = threading.Thread(target=self._validation_loop, daemon=True)
        self.validation_thread.start()
        
        self._load_existing_data()
        
        print("Alert Validation System initialized (Optimized)")
    
    def register_alert(self, alert_data: dict):
        """Registra um alerta para validação futura"""
        validation_record = {
            'alert_id': f"{alert_data['symbol']}_{int(time.time())}",
            'timestamp': int(time.time()),
            'exchange': alert_data['exchange'],
            'symbol': alert_data['symbol'],
            'event_type': alert_data['event_type'],
            'initial_price': alert_data.get('price', 0),
            'volume_multiple': alert_data.get('volume_multiple', 0),
            'strength': alert_data.get('event_strength', 0),
            'prediction': alert_data.get('prediction'),
            'validations': {
                '1h': {'checked': False, 'price': None, 'result': None},
                '4h': {'checked': False, 'price': None, 'result': None},
                '24h': {'checked': False, 'price': None, 'result': None}
            }
        }
        
        with self.validation_lock:
            self.pending_validations.append(validation_record)
        
        self._save_pending_validations()
    
    def _validation_loop(self):
        """Loop que verifica periodicamente alertas pendentes"""
        while True:
            try:
                time.sleep(300)
                self._check_pending_validations()
                self._check_daily_report()
            except Exception as e:
                print(f"[VALIDATION] Error in loop: {e}")
    
    def _check_pending_validations(self):
        """Verifica alertas que precisam de validação"""
        current_time = int(time.time())
        
        with self.validation_lock:
            for record in self.pending_validations[:]:
                alert_time = record['timestamp']
                
                if not record['validations']['1h']['checked'] and current_time >= alert_time + 3600:
                    self._validate_alert(record, '1h', notify=False)
                
                if not record['validations']['4h']['checked'] and current_time >= alert_time + 14400:
                    notify_4h = record['strength'] >= 7
                    self._validate_alert(record, '4h', notify=notify_4h)
                
                if not record['validations']['24h']['checked'] and current_time >= alert_time + 86400:
                    notify_24h = record['strength'] >= 6
                    self._validate_alert(record, '24h', notify=notify_24h)
                    
                    self.validation_results.append(record)
                    self.pending_validations.remove(record)
                    self._save_results()
    
    def _validate_alert(self, record: dict, timeframe: str, notify: bool = True):
        """Valida um alerta específico em determinado timeframe"""
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
            
            record['validations'][timeframe]['checked'] = True
            record['validations'][timeframe]['price'] = current_price
            record['validations'][timeframe]['price_change'] = price_change_pct
            
            event_type = record['event_type']
            result = self._classify_result(event_type, price_change_pct, timeframe)
            record['validations'][timeframe]['result'] = result
            
            if notify:
                self._send_validation_report(record, timeframe)
            
            self._save_pending_validations()
            
        except Exception as e:
            print(f"[VALIDATION] Error validating {record['symbol']}: {e}")
    
    def _classify_result(self, event_type: str, price_change_pct: float, timeframe: str) -> str:
        """Classifica o resultado da validação"""
        
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
        """Envia relatório de validação individual"""
        
        validation = record['validations'][timeframe]
        
        result_emojis = {
            'SUSTAINED_PUMP': '✅ 🚀',
            'SUSTAINED_DUMP': '✅ 📉',
            'WEAK_CONTINUATION': '🟡 ⚖️',
            'SMALL_REVERSAL': '🟠 🔄',
            'DUMP_REVERSAL': '❌ 💥',
            'PUMP_REVERSAL': '❌ 💥'
        }
        
        result = validation['result']
        emoji = result_emojis.get(result, '⚪')
        
        accuracy = self._calculate_accuracy()
        
        success = result in ['SUSTAINED_PUMP', 'SUSTAINED_DUMP', 'WEAK_CONTINUATION']
        
        msg = f"""📊 <b>VALIDAÇÃO [{timeframe}]</b>

{'✅ CONFIRMADO' if success else '❌ FALHOU'}

<b>🎯 {record['symbol']} ({record['exchange'].upper()})</b>
📊 {record['event_type']} | ⚡ {record['strength']}/10
💹 {record['volume_multiple']:.1f}x

{emoji} <b>{result.replace('_', ' ')}</b>
${record['initial_price']:.6f} → ${validation['price']:.6f}
{validation['price_change']:+.2f}%

📈 Accuracy: {accuracy['overall']:.1f}%"""
        
        self.bot.send_telegram(msg)
    
    def _check_daily_report(self):
        """Verifica se deve enviar relatório diário"""
        current_time = int(time.time())
        current_hour = datetime.fromtimestamp(current_time).hour
        
        if (current_time - self.last_daily_report) >= 86400 and current_hour == 10:
            self._send_daily_report()
            self.last_daily_report = current_time
    
    def _send_daily_report(self):
        """Envia relatório diário consolidado"""
        
        cutoff = int(time.time()) - 86400
        recent_validations = [r for r in self.validation_results if r['timestamp'] > cutoff]
        
        if len(recent_validations) < 3:
            return
        
        pump_correct = 0
        pump_total = 0
        dump_correct = 0
        dump_total = 0
        
        best_alerts = []
        worst_alerts = []
        
        for record in recent_validations:
            val_4h = record['validations'].get('4h', {})
            
            if not val_4h.get('checked', False):
                continue
            
            result = val_4h.get('result', 'UNKNOWN')
            event_type = record.get('event_type', 'UNKNOWN')
            price_change = val_4h.get('price_change', 0)
            
            if event_type == 'PUMP':
                pump_total += 1
                if result in ['SUSTAINED_PUMP', 'WEAK_CONTINUATION']:
                    pump_correct += 1
                    if price_change > 10:
                        best_alerts.append((record, price_change))
                else:
                    if price_change < -10:
                        worst_alerts.append((record, price_change))
            elif event_type == 'DUMP':
                dump_total += 1
                if result in ['SUSTAINED_DUMP', 'WEAK_CONTINUATION']:
                    dump_correct += 1
                    if price_change < -10:
                        best_alerts.append((record, price_change))
                else:
                    if price_change > 10:
                        worst_alerts.append((record, price_change))
        
        pump_accuracy = (pump_correct / pump_total * 100) if pump_total > 0 else 0
        dump_accuracy = (dump_correct / dump_total * 100) if dump_total > 0 else 0
        overall = ((pump_correct + dump_correct) / (pump_total + dump_total) * 100) if (pump_total + dump_total) > 0 else 0
        
        msg = f"""📊 <b>RELATÓRIO DIÁRIO</b>

<b>🎯 Accuracy 24h:</b>
• Overall: {overall:.1f}% ({pump_correct + dump_correct}/{pump_total + dump_total})
• Pumps: {pump_accuracy:.1f}% ({pump_correct}/{pump_total})
• Dumps: {dump_accuracy:.1f}% ({dump_correct}/{dump_total})

<b>📈 Total:</b> {len(self.validation_results)} alertas"""
        
        if best_alerts:
            best_alerts.sort(key=lambda x: abs(x[1]), reverse=True)
            msg += "\n\n<b>✅ TOP 3:</b>"
            for record, change in best_alerts[:3]:
                msg += f"\n• {record['symbol']}: {change:+.1f}%"
        
        if worst_alerts:
            worst_alerts.sort(key=lambda x: abs(x[1]), reverse=True)
            msg += "\n\n<b>❌ PIORES:</b>"
            for record, change in worst_alerts[:3]:
                msg += f"\n• {record['symbol']}: {change:+.1f}%"
        
        self.bot.send_telegram(msg)
    
    def _calculate_accuracy(self) -> dict:
        """Calcula accuracy geral"""
        
        if len(self.validation_results) < 5:
            return {
                'overall': 0.0,
                'pump': 0.0,
                'dump': 0.0,
                'total_validated': len(self.validation_results)
            }
        
        pump_correct = 0
        pump_total = 0
        dump_correct = 0
        dump_total = 0
        
        for record in self.validation_results:
            val_4h = record['validations']['4h']
            
            if not val_4h['checked']:
                continue
            
            result = val_4h['result']
            event_type = record['event_type']
            
            if event_type == "PUMP":
                pump_total += 1
                if result in ['SUSTAINED_PUMP', 'WEAK_CONTINUATION']:
                    pump_correct += 1
            else:
                dump_total += 1
                if result in ['SUSTAINED_DUMP', 'WEAK_CONTINUATION']:
                    dump_correct += 1
        
        pump_accuracy = (pump_correct / pump_total * 100) if pump_total > 0 else 0
        dump_accuracy = (dump_correct / dump_total * 100) if dump_total > 0 else 0
        overall_accuracy = ((pump_correct + dump_correct) / (pump_total + dump_total) * 100) if (pump_total + dump_total) > 0 else 0
        
        return {
            'overall': overall_accuracy,
            'pump': pump_accuracy,
            'dump': dump_accuracy,
            'total_validated': len(self.validation_results)
        }
    
    def _save_pending_validations(self):
        try:
            validation_file = os.path.join(self.bot.db.data_dir, "pending_validations.json")
            with open(validation_file, 'w') as f:
                json.dump(self.pending_validations, f, indent=2)
        except Exception as e:
            print(f"[VALIDATION] Error saving pending: {e}")
    
    def _save_results(self):
        try:
            results_file = os.path.join(self.bot.db.data_dir, "validation_results.json")
            with open(results_file, 'w') as f:
                json.dump(self.validation_results, f, indent=2)
        except Exception as e:
            print(f"[VALIDATION] Error saving results: {e}")
    
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
            print(f"[VALIDATION] Error loading data: {e}")

# =========================
#   FILE-BASED DATABASE
# =========================
class FileBasedPatternDB:
    def __init__(self, data_dir="pattern_data"):
        self.data_dir = data_dir
        self.ensure_data_dir()
        
        self.events_buffer = deque(maxlen=1000)
        self.correlations = {}
        self.sessions = {}
        self.predictions = deque(maxlen=100)
        
        self._load_existing_data()
        
        print("File-based pattern database initialized")
    
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
            
            print(f"Loaded {len(self.events_buffer)} events and {len(self.correlations)} correlations")
            
        except Exception as e:
            print(f"Error loading existing data: {e}")
    
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
    
    def get_recent_events(self, hours: int = 2) -> List[Dict]:
        since = int((datetime.now() - timedelta(hours=hours)).timestamp())
        
        recent = []
        for event in self.events_buffer:
            if event.get('timestamp', 0) > since:
                recent.append(event)
        
        return recent
    
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
    
    def get_symbol_correlations(self, symbol: str, min_strength: float = 0.7) -> List[Dict]:
        correlations = []
        
        for key, corr in self.correlations.items():
            if (corr['symbol_1'] == symbol or corr['symbol_2'] == symbol) and \
               corr['correlation_strength'] >= min_strength:
                
                other_symbol = corr['symbol_2'] if corr['symbol_1'] == symbol else corr['symbol_1']
                correlations.append({
                    'other_symbol': other_symbol,
                    'correlation_type': corr['correlation_type'],
                    'strength': corr['correlation_strength'],
                    'delay_minutes': corr['time_delay_minutes'],
                    'sample_size': corr['sample_size']
                })
        
        return sorted(correlations, key=lambda x: x['strength'], reverse=True)

# =========================
#   PATTERN CORRELATION ENGINE
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
        
        print("Pattern Correlation Engine initialized")
    
    def process_new_event(self, event: MarketEvent) -> Dict:
        self.active_events.append(event)
        
        event_data = asdict(event)
        session_id = self.db.log_market_event(event_data)
        
        correlations_found = self._analyze_event_correlations(event)
        cascade_risk = self._detect_cascade_risk(event)
        regime = self._assess_market_regime()
        predictions = self._generate_predictions(event, correlations_found, cascade_risk)
        
        return {
            'session_id': session_id,
            'correlations_found': correlations_found,
            'cascade_risk': cascade_risk,
            'market_regime': regime,
            'predictions': predictions
        }
    
    def _analyze_event_correlations(self, new_event: MarketEvent) -> List[CorrelationPattern]:
        correlations = []
        time_threshold = new_event.timestamp - (self.time_window_minutes * 60)
        
        recent_by_symbol = defaultdict(list)
        for event in self.active_events:
            if event.timestamp > time_threshold and event.symbol != new_event.symbol:
                recent_by_symbol[event.symbol].append(event)
        
        for symbol, events in recent_by_symbol.items():
            if len(events) == 0:
                continue
            
            pump_correlation = self._check_pump_follow_pattern(new_event, events)
            if pump_correlation:
                correlations.append(pump_correlation)
                self.db.update_symbol_correlation(
                    new_event.symbol, symbol, 
                    "PUMP_FOLLOW", pump_correlation.strength, pump_correlation.time_delay
                )
            
            dump_correlation = self._check_dump_follow_pattern(new_event, events)
            if dump_correlation:
                correlations.append(dump_correlation)
                self.db.update_symbol_correlation(
                    new_event.symbol, symbol,
                    "DUMP_FOLLOW", dump_correlation.strength, dump_correlation.time_delay
                )
        
        return correlations
    
    def _check_pump_follow_pattern(self, new_event: MarketEvent, recent_events: List[MarketEvent]) -> Optional[CorrelationPattern]:
        if new_event.event_type != "PUMP":
            return None
        
        recent_pumps = [e for e in recent_events if e.event_type == "PUMP"]
        if not recent_pumps:
            return None
        
        closest_pump = min(recent_pumps, key=lambda x: abs(x.timestamp - new_event.timestamp))
        time_delay = abs(new_event.timestamp - closest_pump.timestamp) // 60
        
        time_score = max(0, 1 - (time_delay / self.time_window_minutes))
        volume_score = min(new_event.volume_multiple, closest_pump.volume_multiple) / max(new_event.volume_multiple, closest_pump.volume_multiple)
        strength_score = min(new_event.event_strength, closest_pump.event_strength) / max(new_event.event_strength, closest_pump.event_strength)
        
        correlation_strength = (time_score * 0.4 + volume_score * 0.3 + strength_score * 0.3)
        
        if correlation_strength >= self.correlation_threshold:
            return CorrelationPattern(
                symbol_pair=(closest_pump.symbol, new_event.symbol),
                correlation_type="PUMP_FOLLOW",
                strength=correlation_strength,
                time_delay=int(time_delay),
                confidence=correlation_strength,
                sample_size=1
            )
        
        return None
    
    def _check_dump_follow_pattern(self, new_event: MarketEvent, recent_events: List[MarketEvent]) -> Optional[CorrelationPattern]:
        if new_event.event_type != "DUMP":
            return None
        
        recent_dumps = [e for e in recent_events if e.event_type == "DUMP"]
        if not recent_dumps:
            return None
        
        closest_dump = min(recent_dumps, key=lambda x: abs(x.timestamp - new_event.timestamp))
        time_delay = abs(new_event.timestamp - closest_dump.timestamp) // 60
        
        time_score = max(0, 1 - (time_delay / self.time_window_minutes))
        volume_score = min(new_event.volume_multiple, closest_dump.volume_multiple) / max(new_event.volume_multiple, closest_dump.volume_multiple)
        
        correlation_strength = (time_score * 0.6 + volume_score * 0.4)
        
        if correlation_strength >= self.correlation_threshold:
            return CorrelationPattern(
                symbol_pair=(closest_dump.symbol, new_event.symbol),
                correlation_type="DUMP_FOLLOW",
                strength=correlation_strength,
                time_delay=int(time_delay),
                confidence=correlation_strength,
                sample_size=1
            )
        
        return None
    
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
        
        hour = datetime.fromtimestamp(new_event.timestamp).hour
        if hour in [22, 23, 0, 1, 2]:
            cascade_risk_score += 0.2
        
        return {
            'cascade_risk_score': min(cascade_risk_score, 1.0),
            'recent_pumps': pumps_count,
            'recent_dumps': dumps_count,
            'risk_level': 'HIGH' if cascade_risk_score > 0.7 else 'MEDIUM' if cascade_risk_score > 0.4 else 'LOW',
            'estimated_cascade_time': new_event.timestamp + (45 * 60) if pumps_count >= 3 else None
        }
    
    def _assess_market_regime(self) -> Dict:
        recent_events = list(self.active_events)[-50:]
        
        if len(recent_events) < 10:
            return {'regime': 'INSUFFICIENT_DATA', 'confidence': 0.0}
        
        pumps = [e for e in recent_events if e.event_type == "PUMP"]
        dumps = [e for e in recent_events if e.event_type == "DUMP"]
        
        pump_ratio = len(pumps) / len(recent_events)
        avg_volume_multiple = statistics.mean(e.volume_multiple for e in recent_events)
        
        if pump_ratio > 0.7 and avg_volume_multiple > 8:
            regime = "PUMP_MANIPULATION"
            confidence = 0.9
        elif pump_ratio < 0.3 and len(dumps) > 5:
            regime = "DUMP_CASCADE"
            confidence = 0.8
        elif avg_volume_multiple > 15:
            regime = "HIGH_MANIPULATION"
            confidence = 0.85
        else:
            regime = "MIXED_SIGNALS"
            confidence = 0.6
        
        return {
            'regime': regime,
            'confidence': confidence,
            'pump_ratio': pump_ratio,
            'avg_volume': avg_volume_multiple,
            'coordination_score': self._calculate_coordination_score(recent_events)
        }
    
    def _calculate_coordination_score(self, events: List[MarketEvent]) -> float:
        if len(events) < 5:
            return 0.0
        
        time_windows = defaultdict(list)
        for event in events:
            window = (event.timestamp // 300) * 300
            time_windows[window].append(event)
        
        coordinated_windows = [w for w, evs in time_windows.items() if len(evs) >= 3]
        coordination_score = len(coordinated_windows) / len(time_windows) if time_windows else 0
        
        return min(coordination_score, 1.0)
    
    def _generate_predictions(self, event: MarketEvent, correlations: List[CorrelationPattern], 
                            cascade_risk: Dict) -> List[Dict]:
        predictions = []
        
        for corr in correlations:
            if corr.correlation_type == "PUMP_FOLLOW":
                predictions.append({
                    'type': 'CORRELATION_PUMP',
                    'target_symbol': corr.symbol_pair[1],
                    'predicted_time': event.timestamp + (corr.time_delay * 60),
                    'confidence': corr.confidence,
                    'reason': f"Follows {corr.symbol_pair[0]} pump pattern"
                })
        
        if cascade_risk['cascade_risk_score'] > 0.6:
            predictions.append({
                'type': 'MARKET_CASCADE',
                'target_symbol': 'MULTIPLE',
                'predicted_time': cascade_risk.get('estimated_cascade_time', event.timestamp + 2700),
                'confidence': cascade_risk['cascade_risk_score'],
                'reason': f"High cascade risk ({cascade_risk['recent_pumps']} recent pumps)"
            })
        
        return predictions

# =========================
#   ENHANCED TRADING BOT (SUPER SIMPLIFICADO)
# =========================
class AdvancedPatternTradingBot:
    """Trading bot com alertas SIMPLES - strength >= 5 = ALERTA!"""
    
    def __init__(self):
        self.db = FileBasedPatternDB()
        self.correlation_engine = PatternCorrelationEngine(self.db)
        self.validation_system = AlertValidationSystem(self)
        
        self.exchanges = {}
        self.watchlist = {}
        self.last_alert_ts = defaultdict(lambda: 0.0)
        
        self.exchanges_list = os.getenv("EXCHANGES", "binance,bingx").split(",")
        self.quote_filter = os.getenv("QUOTE_FILTER", "USDT").split(",")
        self.top_n_by_volume = int(os.getenv("TOP_N_BY_VOLUME", "50"))
        self.timeframe = os.getenv("TIMEFRAME", "1m")
        self.threshold = float(os.getenv("THRESHOLD", "2.0"))
        self.min_price_change = float(os.getenv("MIN_PRICE_CHANGE", "0.02"))
        self.sleep_seconds = int(os.getenv("SLEEP_SECONDS", "20"))
        self.cooldown_minutes = int(os.getenv("COOLDOWN_MINUTES", "8"))
        self.min_strength = int(os.getenv("MIN_STRENGTH", "5"))
        self.debug_mode = os.getenv("DEBUG_MODE", "true").lower() == "true"
        
        self.tg_token = os.getenv("TG_TOKEN", "")
        self.tg_chat_id = os.getenv("TG_CHAT_ID", "")
        
        self.stats = {
            'events_logged': 0,
            'correlations_found': 0,
            'alerts_sent': 0,
            'start_time': time.time()
        }
        
        print(f"Bot initialized - Threshold: {self.threshold}, MinStrength: {self.min_strength}")
        print("🎯 MODO SIMPLES: Alerta TUDO com strength >= 5")
    
    def send_telegram(self, msg: str):
        if not self.tg_token or not self.tg_chat_id:
            if self.debug_mode:
                print("[Telegram] Not configured")
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
    
    def get_symbols_for_exchange(self, ex, limit: int = 50):
        try:
            tickers = ex.fetch_tickers()
            volume_pairs = []
            
            for symbol, ticker in tickers.items():
                if not any(symbol.endswith("/" + q) for q in self.quote_filter):
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
    
    def generate_simple_alert(self, event: MarketEvent, analysis: Dict) -> str:
        """Alerta SIMPLES - foco no que importa"""
        
        msg = f"""🚨 <b>{event.event_type} DETECTADO</b>

🎯 <b>{event.symbol}</b> ({event.exchange.upper()})
⚡ <b>Strength: {event.event_strength}/10</b>
💹 Volume: {event.volume_multiple:.1f}x médio
📈 Preço: {event.price_change_pct:+.1f}%
🕐 {datetime.fromtimestamp(event.timestamp).strftime('%H:%M:%S')}"""

        # Só adiciona extras se existirem
        correlations = analysis.get('correlations_found', [])
        if correlations:
            msg += f"\n\n🔗 Correlação com {correlations[0].symbol_pair[0]}"

        cascade = analysis.get('cascade_risk', {})
        if cascade.get('cascade_risk_score', 0) > 0.5:
            msg += f"\n⚠️ Cascade Risk: {cascade['risk_level']}"

        return msg
    
    def run(self):
        try:
            print("🏦 Initializing exchanges...")
            for exchange_name in self.exchanges_list:
                exchange_name = exchange_name.strip()
                if not exchange_name:
                    continue
                
                try:
                    ex = self.build_exchange(exchange_name)
                    self.exchanges[exchange_name] = ex
                    
                    symbols = self.get_symbols_for_exchange(ex, self.top_n_by_volume)
                    self.watchlist[exchange_name] = symbols
                    
                    print(f"✅ {exchange_name}: {len(symbols)} symbols")
                    
                except Exception as e:
                    print(f"❌ Failed to initialize {exchange_name}: {e}")
            
            if not self.exchanges:
                raise SystemExit("❌ No exchanges initialized")
            
            total_symbols = sum(len(symbols) for symbols in self.watchlist.values())
            
            startup_msg = f"""🚨 <b>BOT SIMPLIFICADO ONLINE</b>

🏦 {', '.join(self.exchanges.keys())}
📊 {total_symbols} moedas
🎯 <b>Regra: Strength >= {self.min_strength} = ALERTA!</b>

Vais receber TODOS os pumps/dumps relevantes! 🔥"""
            
            self.send_telegram(startup_msg)
            
            self.run_simple_loop()
            
        except KeyboardInterrupt:
            print("\n👋 Bot stopped")
            
            uptime_hours = (time.time() - self.stats['start_time']) / 3600
            shutdown_msg = f"""👋 <b>BOT OFFLINE</b>

⏱️ {uptime_hours:.1f}h runtime
📊 {self.stats['alerts_sent']} alertas enviados"""
            
            self.send_telegram(shutdown_msg)
            
        except Exception as e:
            error_msg = f"❌ Bot crashed: {e}"
            print(error_msg)
            self.send_telegram(error_msg)
            raise
    
    def run_simple_loop(self):
        """Loop SIMPLIFICADO - alerta tudo relevante"""
        print("🔬 Starting SIMPLE alert system...")
        
        loop_count = 0
        
        while True:
            loop_start = time.time()
            loop_count += 1
            
            if self.debug_mode and loop_count % 20 == 0:
                uptime = (time.time() - self.stats['start_time']) / 3600
                print(f"[STATS] Loop #{loop_count}, {uptime:.1f}h | Alertas: {self.stats['alerts_sent']}")
            
            for exchange_name, ex in self.exchanges.items():
                symbols = self.watchlist.get(exchange_name, [])
                
                for symbol in symbols:
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
                        
                        price_change_pct = 0
                        if len(hist) > 0:
                            prev_close = hist[-1][4]
                            price_change_pct = (close_last - prev_close) / prev_close if prev_close > 0 else 0
                        
                        # Thresholds básicos
                        if vol_multiple < self.threshold or abs(price_change_pct) < self.min_price_change:
                            continue
                        
                        prices = [c[4] for c in ohlcv]
                        rsi = self.calculate_rsi(prices)
                        
                        event_type = "PUMP" if price_change_pct > 0 else "DUMP"
                        event_strength = min(int((vol_multiple / 2 + abs(price_change_pct) * 20)), 10)
                        
                        # FILTRO: Só processa se strength >= min_strength
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
                        self.stats['events_logged'] += 1
                        
                        # 🎯 REGRA SIMPLES: Strength >= min_strength = ALERTA!
                        alert_key = f"{exchange_name}:{symbol}"
                        
                        if self.can_alert(alert_key, time.time()):
                            alert_message = self.generate_simple_alert(event, analysis)
                            self.send_telegram(alert_message)
                            self.stats['alerts_sent'] += 1
                            
                            # Registar para validação
                            alert_data = {
                                'symbol': event.symbol,
                                'exchange': event.exchange,
                                'event_type': event.event_type,
                                'price': close_last,
                                'volume_multiple': vol_multiple,
                                'event_strength': event_strength,
                                'prediction': {
                                    'direction': 'UP' if event.event_type == 'PUMP' else 'DOWN',
                                    'cascade_risk': analysis['cascade_risk']['cascade_risk_score'],
                                    'correlations_count': len(analysis['correlations_found'])
                                }
                            }
                            self.validation_system.register_alert(alert_data)
                            
                            if self.debug_mode:
                                print(f"[ALERT #{self.stats['alerts_sent']}] {symbol}: {event_type} {event_strength}/10")
                    
                    except Exception as e:
                        if self.debug_mode:
                            print(f"Error: {exchange_name} {symbol}: {e}")
                        continue
            
            elapsed = time.time() - loop_start
            sleep_time = max(0, self.sleep_seconds - elapsed)
            time.sleep(sleep_time)

# =========================
#   MAIN
# =========================
def main():
    print("🚨 Simple Alert Bot Starting...")
    
    bot = AdvancedPatternTradingBot()
    bot.run()

if __name__ == "__main__":
    main()
