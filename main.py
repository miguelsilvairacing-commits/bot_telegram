# =========================================================================================
#   CRYPTO ML BOT v2.2.1 - TEST ALERTS + BTC TRACKING! 🚀🧪
# =========================================================================================
# 
# ✅ NOVIDADES v2.2.1:
#
# 🧪 TEST ALERTS SYSTEM:
#    - Envia alertas de TESTE ao Telegram a cada X minutos
#    - Mostra status BTC atual + formato de alerta esperado
#    - Valida que mensagens incluem contexto BTC corretamente
#    - Controle via Railway variables (liga/desliga facilmente)
#
# 🔧 RAILWAY VARIABLES:
#    FORCE_TEST_ALERTS="true"      # Ativa alertas de teste
#    TEST_ALERT_INTERVAL="300"     # Intervalo em segundos (300 = 5 min)
#
# ✅ CORREÇÕES APLICADAS (v2.1 → v2.2):
# 
# 1. 🚫 BLACKLIST DO RAILWAY:
#    - Lê SYMBOLS_BLACKLIST do Railway (flexibilidade total!)
#    - Fallback: SUT/USDT, YB/USDT sempre bloqueados (scam confirmado)
#    - Edita Railway em 30 segundos, sem commits no git!
#
# 2. 🎯 BTC TRACKING MELHORADO:
#    - Threshold UP/DOWN: 0.8% → 0.3% (MUITO mais sensível!)
#    - Threshold INDEPENDENT: 0.5% → 0.2% (captar mais movimentos)
#    - Inicialização rápida: Usa dados parciais (5+ pontos em vez de 15+)
#    - Agora detecta movimentos reais de BTC (não só "LATERAL")!
#
# 3. 📊 IMPACTO ESPERADO:
#    - BTC trend será UP/DOWN com mais frequência (antes: ~5%, agora: ~40%)
#    - Classificação INDEPENDENT será mais precisa
#    - Alertas terão contexto BTC real
#    - ML accuracy esperada: 40% → 60-70%
#
# 4. 🔧 RAILWAY VARIABLES NECESSÁRIAS:
#    SYMBOLS_BLACKLIST="FTT/USDT,ENSO/USDT,BNLIFE/USDT,COLS/USDT,FORM/USDT"
#    (SUT/USDT e YB/USDT já incluídos no código como fallback)
#
# 5. 🔍 PRÓXIMOS PASSOS:
#    - [ ] Após 150 alertas: Análise de performance
#    - [ ] Implementar ML com features BTC
#    - [ ] (Opcional) Rate limit por moeda (4 alertas/dia/moeda)
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

# Blacklist de símbolos problemáticos
# FONTE PRINCIPAL: Railway environment variable SYMBOLS_BLACKLIST
# FALLBACK: Moedas scam confirmadas (sempre bloqueadas)

# Lê blacklist do Railway
env_blacklist = os.getenv("SYMBOLS_BLACKLIST", "")
SYMBOLS_BLACKLIST = set()

if env_blacklist:
    # Parse e limpa a string do Railway
    parsed = [s.strip() for s in env_blacklist.split(",") if s.strip()]
    SYMBOLS_BLACKLIST.update(parsed)

# SEMPRE adiciona moedas scam confirmadas (proteção mínima)
SYMBOLS_BLACKLIST_CRITICAL = {
    'SUT/USDT',  # Scam confirmado - 53% reversals
    'YB/USDT',   # Scam confirmado - sempre reversals
}
SYMBOLS_BLACKLIST.update(SYMBOLS_BLACKLIST_CRITICAL)

# Log para confirmação
print(f"⛔ Blacklist: {len(SYMBOLS_BLACKLIST)} símbolos bloqueados")
if len(SYMBOLS_BLACKLIST) > 2:
    print(f"   Railway: {len(SYMBOLS_BLACKLIST) - 2} moedas")
    print(f"   Hardcoded: 2 moedas críticas (SUT, YB)")
else:
    print(f"   ⚠️  Apenas fallback ativo (SUT, YB)")

# =========================
#   VALIDATION SYSTEM - OPTIMIZED FOR ML DATA COLLECTION
# =========================
class AlertValidationSystem:
    """Sistema de validação optimizado para colecta de dados ML com BTC context"""
    
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.pending_validations = []
        self.validation_results = []
        self.validation_lock = threading.Lock()
        self.last_daily_report = 0
        
        self.validation_thread = threading.Thread(target=self._validation_loop, daemon=True)
        self.validation_thread.start()
        
        self._load_existing_data()
        
        print("Alert Validation System initialized (ML-Ready with BTC)")
    
    def register_alert(self, alert_data: dict):
        """Registra alerta com TODOS os dados necessários para ML incluindo BTC"""
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
            
            # NOVO: Bitcoin context
            'btc_price': self.bot.btc_data['last_price'],
            'btc_change_5m': self.bot.btc_data['change_5m'],
            'btc_change_15m': self.bot.btc_data['change_15m'],
            'btc_trend': self.bot.btc_data['trend'],
            'btc_volume_spike': self.bot.btc_data.get('volume_spike', 1.0),
            
            # NOVO: Relação com BTC
            'price_vs_btc': alert_data.get('price_change_pct', 0) - self.bot.btc_data['change_5m'],
            'is_btc_follower': abs(alert_data.get('price_change_pct', 0) - self.bot.btc_data['change_5m']) < 1.5,
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
        print(f"[ML-DATA] Alert registered: {alert_data['symbol']} {alert_data['event_type']} (BTC: {self.bot.btc_data['trend']})")
    
    def _classify_movement(self, alert_data: dict, btc_data: dict) -> str:
        """Classifica o movimento em relação ao BTC"""
        alert_change = alert_data.get('price_change_pct', 0)
        btc_change = btc_data['change_5m']
        
        # Se BTC está lateral, movimento é independente
        if abs(btc_change) < 0.2:  # FIXED: 0.5 → 0.2 para captar mais movimentos
            return 'INDEPENDENT'
        
        # Mesma direção que BTC
        if (alert_change > 0 and btc_change > 0) or (alert_change < 0 and btc_change < 0):
            # Verifica força relativa
            if abs(alert_change) > abs(btc_change) * 1.5:
                return 'BTC_OUTPERFORM'
            elif abs(alert_change) > abs(btc_change) * 0.5:
                return 'BTC_FOLLOW'
            else:
                return 'BTC_UNDERPERFORM'
        else:
            # Movimento contrário ao BTC
            return 'BTC_COUNTER'
    
    def _validation_loop(self):
        """Loop de validação"""
        while True:
            try:
                time.sleep(300)  # A cada 5min
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
                
                # Validação 1h (silenciosa - só guarda dados)
                if not record['validations']['1h']['checked'] and current_time >= alert_time + 3600:
                    self._validate_alert(record, '1h', notify=False)
                
                # Validação 4h (SEMPRE notifica - dados para ML)
                if not record['validations']['4h']['checked'] and current_time >= alert_time + 14400:
                    self._validate_alert(record, '4h', notify=True)
                
                # Validação 24h (notifica se forte)
                if not record['validations']['24h']['checked'] and current_time >= alert_time + 86400:
                    notify_24h = record['strength'] >= 7
                    self._validate_alert(record, '24h', notify=notify_24h)
                    
                    # Move para resultados finais
                    self.validation_results.append(record)
                    self.pending_validations.remove(record)
                    self._save_results()
    
    def _validate_alert(self, record: dict, timeframe: str, notify: bool = True):
        """Valida alerta e guarda TODOS os dados incluindo BTC atual"""
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
            
            # NOVO: Guarda BTC context no momento da validação
            record['validations'][timeframe]['btc_price'] = self.bot.btc_data['last_price']
            record['validations'][timeframe]['btc_change'] = self.bot.btc_data['change_5m']
            
            # Atualiza registro
            record['validations'][timeframe]['checked'] = True
            record['validations'][timeframe]['price'] = current_price
            record['validations'][timeframe]['price_change'] = price_change_pct
            
            event_type = record['event_type']
            result = self._classify_result(event_type, price_change_pct, timeframe)
            record['validations'][timeframe]['result'] = result
            
            # Guarda timestamp da validação
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
        """Envia relatório de validação com contexto BTC"""
        
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
        
        success = result in ['SUSTAINED_PUMP', 'SUSTAINED_DUMP', 'WEAK_CONTINUATION']
        
        msg = f"""📊 <b>VALIDAÇÃO [{timeframe}]</b>

{emoji} <b>{result.replace('_', ' ')}</b>

🎯 {record['symbol']} ({record['exchange'].upper()})
📊 {record['event_type']} | ⚡ {record['strength']}/10
💹 {record['volume_multiple']:.1f}x

💰 ${record['initial_price']:.6f} → ${validation['price']:.6f}
📈 {validation['price_change']:+.2f}%"""

        # Adiciona contexto BTC
        if record.get('movement_type'):
            msg += f"\n\n₿ Movimento: {record['movement_type']}"
            msg += f"\n📊 BTC no alerta: {record['btc_change_5m']:+.1f}%"
        
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
        """Envia relatório diário com stats ML-ready e BTC analysis"""
        
        cutoff = int(time.time()) - 86400
        recent = [r for r in self.validation_results if r['timestamp'] > cutoff]
        
        if len(recent) < 5:
            return
        
        # Stats básicas
        pump_correct = 0
        pump_total = 0
        dump_correct = 0
        dump_total = 0
        
        # Stats por movimento BTC
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
            
            # Contabiliza movimento vs BTC
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
        
        # Progresso para ML
        ml_target = 150
        ml_progress = (total_alerts / ml_target) * 100
        days_remaining = max(0, 14 - (total_alerts / 15))  # Estimativa
        
        msg = f"""📊 <b>RELATÓRIO DIÁRIO</b>

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
✅ BTC tracking activo
✅ Dados ML-ready
✅ Blacklist implementada"""
        
        if total_alerts >= ml_target:
            msg += f"\n\n🎉 <b>META ATINGIDA!</b>\n✅ Dataset completo para ML!"
        
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
        
        print("Pattern database initialized (ML-Ready with BTC)")
    
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
        
        print("Correlation Engine initialized with BTC awareness")
    
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
#   TRADING BOT - ML DATA COLLECTION MODE WITH BTC
# =========================
class AdvancedPatternTradingBot:
    """Bot optimizado para colecta de dados ML com BTC tracking"""
    
    def __init__(self):
        self.db = FileBasedPatternDB()
        self.correlation_engine = PatternCorrelationEngine(self.db)
        
        # Inicializa exchanges primeiro
        self.exchanges = {}
        self.exchanges_list = os.getenv("EXCHANGES", "binance,bingx").split(",")
        
        # Precisa inicializar exchanges antes do validation system
        self._initialize_exchanges()
        
        # BTC tracking data
        self.btc_data = {
            'last_price': 0,
            'change_5m': 0,
            'change_15m': 0,
            'trend': 'LATERAL',  # UP/DOWN/LATERAL
            'last_update': 0,
            'history': deque(maxlen=50),  # 50 data points (~15min)
            'volume_spike': 1.0,
            'last_volume': 0
        }
        
        # Agora pode inicializar validation system
        self.validation_system = AlertValidationSystem(self)
        
        self.watchlist = {}
        self.last_alert_ts = defaultdict(lambda: 0.0)
        
        self.quote_filter = os.getenv("QUOTE_FILTER", "USDT").split(",")
        self.top_n_by_volume = int(os.getenv("TOP_N_BY_VOLUME", "50"))
        self.timeframe = os.getenv("TIMEFRAME", "1m")
        self.threshold = float(os.getenv("THRESHOLD", "1.8"))
        self.min_price_change = float(os.getenv("MIN_PRICE_CHANGE", "0.02"))
        self.sleep_seconds = int(os.getenv("SLEEP_SECONDS", "20"))
        self.cooldown_minutes = int(os.getenv("COOLDOWN_MINUTES", "8"))
        self.min_strength = int(os.getenv("MIN_STRENGTH", "5"))
        self.debug_mode = os.getenv("DEBUG_MODE", "true").lower() == "true"
        
        # NOVO: Configurações BTC
        self.btc_adjust_strength = os.getenv("BTC_ADJUST_STRENGTH", "true").lower() == "true"
        self.btc_filter_followers = os.getenv("BTC_FILTER_FOLLOWERS", "false").lower() == "true"
        
        self.tg_token = os.getenv("TG_TOKEN", "")
        self.tg_chat_id = os.getenv("TG_CHAT_ID", "")
        
        # NOVO v2.2.1: Test Alerts System
        self.force_test_alerts = os.getenv("FORCE_TEST_ALERTS", "false").lower() == "true"
        self.test_alert_interval = int(os.getenv("TEST_ALERT_INTERVAL", "300"))  # 5 min default
        self.last_test_alert = 0
        
        if self.force_test_alerts:
            print(f"🧪 TEST ALERTS ENABLED - Interval: {self.test_alert_interval}s ({self.test_alert_interval//60} min)")
        
        self.stats = {
            'alerts_sent': 0,
            'btc_followers_filtered': 0,
            'start_time': time.time()
        }
        
        # Inicia BTC tracker thread
        self.btc_thread = threading.Thread(target=self._btc_tracker_loop, daemon=True)
        self.btc_thread.start()
        
        print(f"Bot initialized - ML Data Collection Mode with BTC Tracking")
        print(f"Config: Threshold={self.threshold}, MinStrength={self.min_strength}, BTC_Adjust={self.btc_adjust_strength}")
    
    def _initialize_exchanges(self):
        """Inicializa exchanges antes de outros componentes"""
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
    
    def _btc_tracker_loop(self):
        """Monitora BTC/USDT continuamente"""
        print("[BTC Tracker] Starting...")
        
        # Aguarda exchanges estarem prontas
        attempts = 0
        while 'binance' not in self.exchanges or not self.exchanges.get('binance'):
            time.sleep(2)
            attempts += 1
            if attempts > 30:
                print("[BTC Tracker] ERROR: Binance not available!")
                return
            if attempts % 5 == 0:
                print(f"[BTC Tracker] Waiting for Binance... ({attempts*2}s)")
        
        print("[BTC Tracker] Binance ready, starting monitoring...")
        
        # Inicializa com dados reais
        try:
            ex = self.exchanges['binance']
            ticker = ex.fetch_ticker('BTC/USDT')
            self.btc_data['last_price'] = ticker['last']
            print(f"[BTC Tracker] Initial BTC price: ${ticker['last']:.0f}")
        except Exception as e:
            print(f"[BTC Tracker] Init error: {e}")
        
        while True:
            try:
                if 'binance' in self.exchanges:
                    ex = self.exchanges['binance']
                    
                    # Fetch ticker para preço
                    ticker = ex.fetch_ticker('BTC/USDT')
                    current_price = ticker['last']
                    current_time = int(time.time())
                    
                    # CORREÇÃO: Usar 'quoteVolume' em vez de 'quoteVolume24h'
                    current_volume = ticker.get('quoteVolume', 0)  # Campo correto!
                    
                    # Fetch OHLCV para volume spike
                    try:
                        ohlcv = ex.fetch_ohlcv('BTC/USDT', '1m', 10)
                        
                        # Calcula volume spike
                        if len(ohlcv) >= 5:
                            volumes = [c[5] for c in ohlcv[-5:]]
                            avg_volume = sum(volumes) / len(volumes)
                            last_volume = ohlcv[-1][5] if ohlcv else current_volume
                            self.btc_data['volume_spike'] = last_volume / avg_volume if avg_volume > 0 else 1.0
                    except Exception as e:
                        # Se falhar OHLCV, continua sem volume spike
                        self.btc_data['volume_spike'] = 1.0
                    
                    # Adiciona à história
                    self.btc_data['history'].append({
                        'price': current_price,
                        'timestamp': current_time,
                        'volume': current_volume
                    })
                    
                    # Calcula mudanças apenas se tiver história suficiente
                    if len(self.btc_data['history']) >= 2:
                        # 5min ago - usa o que tiver disponível (mínimo 5 pontos)
                        if len(self.btc_data['history']) >= 5:
                            lookback_5m = min(15, len(self.btc_data['history']) - 1)
                            price_5m_ago = self.btc_data['history'][-lookback_5m]['price']
                            self.btc_data['change_5m'] = ((current_price - price_5m_ago) / price_5m_ago) * 100
                        
                        # 15min ago - usa o que tiver disponível (mínimo 15 pontos)
                        if len(self.btc_data['history']) >= 15:
                            lookback_15m = min(45, len(self.btc_data['history']) - 1)
                            price_15m_ago = self.btc_data['history'][-lookback_15m]['price']
                            self.btc_data['change_15m'] = ((current_price - price_15m_ago) / price_15m_ago) * 100
                        
                        # Determina trend baseado em 5min (FIXED: 0.8 → 0.3)
                        if self.btc_data['change_5m'] > 0.3:  # FIXED
                            self.btc_data['trend'] = 'UP'
                        elif self.btc_data['change_5m'] < -0.3:  # FIXED
                            self.btc_data['trend'] = 'DOWN'
                        else:
                            self.btc_data['trend'] = 'LATERAL'
                    
                    self.btc_data['last_price'] = current_price
                    self.btc_data['last_update'] = current_time
                    self.btc_data['last_volume'] = current_volume
                    
                    # Log a cada 5min em debug
                    if self.debug_mode and len(self.btc_data['history']) % 15 == 0:
                        print(f"[BTC] ${current_price:.0f} | 5m: {self.btc_data['change_5m']:+.2f}% | Trend: {self.btc_data['trend']}")
                
                time.sleep(20)  # Update a cada 20s
                
            except Exception as e:
                print(f"[BTC Tracker] Error: {e}")
                time.sleep(30)
    
    def should_process_symbol(self, symbol: str) -> bool:
        """Verifica se o símbolo deve ser processado (blacklist check)"""
        # Remove exchange suffix para comparação
        clean_symbol = symbol.split(':')[0] if ':' in symbol else symbol
        
        if clean_symbol in SYMBOLS_BLACKLIST:
            if self.debug_mode:
                print(f"⛔ {clean_symbol} BLOCKED (blacklist)")
            return False
        return True
    
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
    
    def _send_test_alert(self):
        """Envia alerta de TESTE com BTC context atual - v2.2.1"""
        
        btc_price = self.btc_data['last_price']
        btc_5m = self.btc_data['change_5m']
        btc_15m = self.btc_data['change_15m']
        btc_trend = self.btc_data['trend']
        history_points = len(self.btc_data['history'])
        
        # Determina movimento simulado baseado no BTC real
        if abs(btc_5m) < 0.2:
            movement_type = "INDEPENDENT"
            movement_emoji = "🎯"
            movement_desc = "Movimento independente de BTC"
        elif btc_5m > 0.3:
            movement_type = "FOLLOWING BTC"
            movement_emoji = "📊"
            movement_desc = "Seguindo pump do BTC"
        elif btc_5m < -0.3:
            movement_type = "COUNTER TO BTC"
            movement_emoji = "⚔️"
            movement_desc = "Contra movimento do BTC"
        else:
            movement_type = "INDEPENDENT"
            movement_emoji = "🎯"
            movement_desc = "BTC lateral, movimento próprio"
        
        # Simula preço de alerta baseado no BTC
        simulated_price_change = btc_5m + 1.5  # Sempre +1.5% acima do BTC
        
        msg = f"""🧪 <b>ALERTA DE TESTE - BTC TRACKER v2.2.1</b>

━━━━━━━━━━━━━━━━━━━━━━━━
<b>₿ BTC STATUS ATUAL:</b>
━━━━━━━━━━━━━━━━━━━━━━━━

💰 Preço: ${btc_price:.0f}
📊 5min: {btc_5m:+.2f}%
📊 15min: {btc_15m:+.2f}%
🎯 Trend: <b>{btc_trend}</b>
📈 Dados: {history_points} pontos

━━━━━━━━━━━━━━━━━━━━━━━━
<b>📝 FORMATO DE ALERTA REAL:</b>
━━━━━━━━━━━━━━━━━━━━━━━━

Se receberes um PUMP agora, virá assim:

🚨 <b>PUMP DETECTADO</b>

🎯 <b>ETH/USDT</b> (BINANCE)
⚡ <b>Strength: 8/10</b>
💹 Volume: 15.5x médio
📈 Preço: {simulated_price_change:+.1f}%"""

        # Adiciona contexto BTC SE BTC estiver a mover
        if abs(btc_5m) > 0.3:
            relative_strength = simulated_price_change - btc_5m
            msg += f"\n\n₿ BTC: {btc_5m:+.1f}% ({btc_trend})"
            
            if abs(relative_strength) > 1:
                if relative_strength > 0:
                    msg += f"\n💪 Outperforming BTC! (+{abs(relative_strength):.1f}%)"
                else:
                    msg += f"\n⚠️ Underperforming BTC ({relative_strength:.1f}%)"
            else:
                msg += f"\n📊 Following BTC"
        else:
            msg += f"\n\n{movement_emoji} <b>{movement_type}</b>"
            msg += f"\n💭 {movement_desc}"
        
        msg += f"\n🕐 {datetime.now().strftime('%H:%M:%S')}"
        
        msg += f"""

━━━━━━━━━━━━━━━━━━━━━━━━
<b>🔍 INTERPRETAÇÃO:</b>
━━━━━━━━━━━━━━━━━━━━━━━━

• BTC {btc_trend}: {'Mercado volátil' if abs(btc_5m) > 0.3 else 'Mercado calmo'}
• Contexto BTC: {'VISÍVEL nos alertas' if abs(btc_5m) > 0.3 else 'Não aparece (BTC lateral)'}
• Movimentos <0.3%: Considerados LATERAL
• Próximo teste: {self.test_alert_interval//60} minutos

━━━━━━━━━━━━━━━━━━━━━━━━
🧪 Isto é um TESTE automático
⏰ Para desativar: FORCE_TEST_ALERTS=false"""
        
        self.send_telegram(msg)
        print(f"[TEST] Alerta de teste enviado | BTC: {btc_5m:+.2f}% ({btc_trend})")
    
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
                
                # BLACKLIST CHECK
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
        """Alerta com contexto BTC"""
        
        msg = f"""🚨 <b>{event.event_type} DETECTADO</b>

🎯 <b>{event.symbol}</b> ({event.exchange.upper()})
⚡ <b>Strength: {event.event_strength}/10</b>
💹 Volume: {event.volume_multiple:.1f}x médio
📈 Preço: {event.price_change_pct:+.1f}%"""

        # NOVO: Adiciona contexto BTC
        btc_change = self.btc_data['change_5m']
        btc_trend = self.btc_data['trend']
        
        # Calcula força relativa ao BTC
        relative_strength = event.price_change_pct - btc_change
        
        if abs(btc_change) > 0.3:  # BTC está a mover
            msg += f"\n\n₿ BTC: {btc_change:+.1f}% ({btc_trend})"
            
            if abs(relative_strength) > 1:
                if relative_strength > 0:
                    msg += f"\n💪 Outperforming BTC! (+{abs(relative_strength):.1f}%)"
                else:
                    msg += f"\n⚠️ Underperforming BTC ({relative_strength:.1f}%)"
            elif abs(event.price_change_pct - btc_change) < 0.5:
                msg += f"\n📊 Following BTC"
        
        # Adiciona hora
        msg += f"\n🕐 {datetime.fromtimestamp(event.timestamp).strftime('%H:%M:%S')}"

        correlations = analysis.get('correlations_found', [])
        if correlations:
            msg += f"\n\n🔗 Correlação com {correlations[0].symbol_pair[0]}"

        cascade = analysis.get('cascade_risk', 0)
        if cascade > 0.5:
            msg += f"\n⚠️ Cascade Risk: HIGH"

        return msg
    
    def run(self):
        try:
            print("🏦 Initializing exchanges...")
            
            # Já inicializadas no __init__
            for exchange_name, ex in self.exchanges.items():
                symbols = self.get_symbols_for_exchange(ex, self.top_n_by_volume)
                self.watchlist[exchange_name] = symbols
                print(f"✅ {exchange_name}: {len(symbols)} symbols (after blacklist)")
            
            if not self.exchanges:
                raise SystemExit("❌ No exchanges")
            
            total_symbols = sum(len(s) for s in self.watchlist.values())
            blacklisted = len(SYMBOLS_BLACKLIST)
            
            startup_msg = f"""🚀 <b>BOT INICIADO - ML DATA COLLECTION v2.2.1</b> 🧪

🏦 {', '.join(self.exchanges.keys())}
📊 {total_symbols} moedas monitorizadas
⛔ {blacklisted} símbolos na blacklist

₿ <b>BTC Tracking:</b> ACTIVO
🧪 <b>Test Alerts:</b> {'ACTIVO' if self.force_test_alerts else 'DESATIVADO'}
🎯 <b>Objectivo:</b> 150-200 alertas limpos
📊 <b>Sistema:</b> Alertas + BTC context + Validações 4h
📈 <b>Próximo:</b> Machine Learning com features BTC

Aguarda validações para ML! 🔥"""
            
            self.send_telegram(startup_msg)
            
            # Aguarda BTC tracker inicializar
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
        """Loop de detecção com BTC awareness"""
        print("🔬 Starting ML data collection with BTC tracking...")
        
        loop_count = 0
        
        while True:
            loop_start = time.time()
            loop_count += 1
            
            # NOVO v2.2.1: Test Alerts System
            if self.force_test_alerts:
                current_time = time.time()
                if current_time - self.last_test_alert >= self.test_alert_interval:
                    self._send_test_alert()
                    self.last_test_alert = current_time
            
            if self.debug_mode and loop_count % 50 == 0:
                uptime = (time.time() - self.stats['start_time']) / 3600
                total_alerts = len(self.validation_system.validation_results) + len(self.validation_system.pending_validations)
                btc_price = self.btc_data['last_price']
                print(f"[STATS] Loop #{loop_count}, {uptime:.1f}h | Alerts: {total_alerts} | BTC: ${btc_price:.0f}")
            
            for exchange_name, ex in self.exchanges.items():
                symbols = self.watchlist.get(exchange_name, [])
                
                for symbol in symbols:
                    # BLACKLIST CHECK
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
                        
                        # Filtro volume extremo
                        if vol_multiple > 100:  # Volume suspeito
                            if self.debug_mode:
                                print(f"[SKIP] {symbol}: Volume extremo {vol_multiple:.1f}x")
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
                        
                        # Calcula strength base
                        base_strength = min(int((vol_multiple / 2 + abs(price_change_pct) * 20)), 10)
                        
                        # NOVO: Ajusta strength baseado em BTC
                        event_strength = base_strength
                        btc_change = self.btc_data['change_5m']
                        
                        if self.btc_adjust_strength:
                            if self.btc_data['trend'] == 'UP' and event_type == 'PUMP':
                                # Pump durante BTC pump
                                if abs(price_change_pct*100 - btc_change) < 2:
                                    event_strength = int(base_strength * 0.7)  # Reduz se só segue BTC
                                    
                            elif self.btc_data['trend'] == 'DOWN' and event_type == 'PUMP':
                                # Pump durante BTC dump = muito forte!
                                event_strength = min(10, int(base_strength * 1.3))
                            
                            elif self.btc_data['trend'] == 'DOWN' and event_type == 'DUMP':
                                # Dump durante BTC dump
                                if abs(price_change_pct*100 - btc_change) < 2:
                                    event_strength = int(base_strength * 0.7)
                        
                        if event_strength < self.min_strength:
                            continue
                        
                        # NOVO: Filtro opcional de BTC followers
                        is_btc_follower = abs(price_change_pct*100 - btc_change) < 1.5
                        
                        if self.btc_filter_followers and is_btc_follower and event_strength < 7:
                            self.stats['btc_followers_filtered'] += 1
                            if self.debug_mode:
                                print(f"[FILTER] {symbol}: BTC follower (skipped)")
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
                            
                            # Registar para validação com TODOS os dados ML + BTC
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
                                movement_vs_btc = "INDEPENDENT" if abs(price_change_pct*100 - btc_change) > 2 else "FOLLOWER"
                                print(f"[ALERT] {symbol}: {event_type} {event_strength}/10 | BTC: {movement_vs_btc}")
                    
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
    print("🚀 ML Data Collection Bot v2.2.1 Starting... 🧪")
    print("📊 Goal: Clean dataset with BTC context")
    print("₿ BTC Tracking: ENABLED")
    print("⛔ Blacklist: ACTIVE")
    print("🧪 Test Alerts: Check Railway variables")
    print("🧠 Next: Machine Learning with rich features")
    
    bot = AdvancedPatternTradingBot()
    bot.run()

if __name__ == "__main__":
    main()
