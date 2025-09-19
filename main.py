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

# =========================
#   FILE-BASED DATABASE (Railway Compatible)
# =========================
class FileBasedPatternDB:
    """File-based database using JSON for Railway compatibility"""
    
    def __init__(self, data_dir="pattern_data"):
        self.data_dir = data_dir
        self.ensure_data_dir()
        
        # In-memory structures for fast access
        self.events_buffer = deque(maxlen=1000)  # Recent events
        self.correlations = {}  # symbol_pair -> correlation_data
        self.sessions = {}  # session_id -> session_data
        self.predictions = deque(maxlen=100)  # Recent predictions
        
        # Load existing data
        self._load_existing_data()
        
        print("File-based pattern database initialized (Railway compatible)")
    
    def ensure_data_dir(self):
        """Ensure data directory exists"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def _load_existing_data(self):
        """Load existing data from files"""
        try:
            # Load recent events
            events_file = os.path.join(self.data_dir, "recent_events.json")
            if os.path.exists(events_file):
                with open(events_file, 'r') as f:
                    events_data = json.load(f)
                    for event in events_data[-1000:]:  # Load last 1000 events
                        self.events_buffer.append(event)
            
            # Load correlations
            corr_file = os.path.join(self.data_dir, "correlations.json")
            if os.path.exists(corr_file):
                with open(corr_file, 'r') as f:
                    self.correlations = json.load(f)
            
            print(f"Loaded {len(self.events_buffer)} events and {len(self.correlations)} correlations")
            
        except Exception as e:
            print(f"Error loading existing data: {e}")
    
    def _save_data_periodically(self):
        """Save data to files periodically"""
        try:
            # Save recent events
            events_file = os.path.join(self.data_dir, "recent_events.json")
            with open(events_file, 'w') as f:
                json.dump(list(self.events_buffer), f, indent=2)
            
            # Save correlations
            corr_file = os.path.join(self.data_dir, "correlations.json")
            with open(corr_file, 'w') as f:
                json.dump(self.correlations, f, indent=2)
            
        except Exception as e:
            print(f"Error saving data: {e}")
    
    def log_market_event(self, event_data: Dict) -> str:
        """Log market event and return session ID"""
        # Generate session ID (30-minute windows)
        timestamp = event_data['timestamp']
        window_start = (timestamp // 1800) * 1800
        session_id = f"SESSION_{window_start}"
        
        event_data['session_id'] = session_id
        event_data['logged_at'] = int(time.time())
        
        # Add to buffer
        self.events_buffer.append(event_data)
        
        # Update session data
        if session_id not in self.sessions:
            self.sessions[session_id] = {
                'session_id': session_id,
                'start_time': window_start,
                'events': [],
                'total_events': 0
            }
        
        self.sessions[session_id]['events'].append(event_data)
        self.sessions[session_id]['total_events'] += 1
        
        # Periodic save (every 10 events)
        if len(self.events_buffer) % 10 == 0:
            self._save_data_periodically()
        
        return session_id
    
    def get_recent_events(self, hours: int = 2) -> List[Dict]:
        """Get recent events"""
        since = int((datetime.now() - timedelta(hours=hours)).timestamp())
        
        recent = []
        for event in self.events_buffer:
            if event.get('timestamp', 0) > since:
                recent.append(event)
        
        return recent
    
    def update_symbol_correlation(self, symbol1: str, symbol2: str, 
                                 correlation_type: str, strength: float, delay: int):
        """Update correlation between symbols"""
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
            # Update existing correlation (moving average)
            existing = self.correlations[key]
            existing['correlation_strength'] = (existing['correlation_strength'] + strength) / 2
            existing['time_delay_minutes'] = (existing['time_delay_minutes'] + delay) / 2
            existing['sample_size'] += 1
            existing['last_updated'] = int(time.time())
    
    def get_symbol_correlations(self, symbol: str, min_strength: float = 0.7) -> List[Dict]:
        """Get correlations for a symbol"""
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
    """Advanced engine for detecting market correlation patterns"""
    
    def __init__(self, db: FileBasedPatternDB):
        self.db = db
        self.active_events = deque(maxlen=500)  # Keep recent events in memory
        self.correlation_threshold = 0.7
        self.time_window_minutes = 30
        
        print("Pattern Correlation Engine initialized with file storage")
    
    def process_new_event(self, event: MarketEvent) -> Dict:
        """Process new market event and detect correlations"""
        
        # Add to active events
        self.active_events.append(event)
        
        # Log to database
        event_data = asdict(event)
        session_id = self.db.log_market_event(event_data)
        
        # Analyze correlations with recent events
        correlations_found = self._analyze_event_correlations(event)
        
        # Check for cascade patterns
        cascade_risk = self._detect_cascade_risk(event)
        
        # Update market regime
        regime = self._assess_market_regime()
        
        # Generate predictions
        predictions = self._generate_predictions(event, correlations_found, cascade_risk)
        
        return {
            'session_id': session_id,
            'correlations_found': correlations_found,
            'cascade_risk': cascade_risk,
            'market_regime': regime,
            'predictions': predictions
        }
    
    def _analyze_event_correlations(self, new_event: MarketEvent) -> List[CorrelationPattern]:
        """Find correlations between new event and recent events"""
        
        correlations = []
        time_threshold = new_event.timestamp - (self.time_window_minutes * 60)
        
        # Group recent events by symbol
        recent_by_symbol = defaultdict(list)
        for event in self.active_events:
            if event.timestamp > time_threshold and event.symbol != new_event.symbol:
                recent_by_symbol[event.symbol].append(event)
        
        # Analyze each symbol for correlations
        for symbol, events in recent_by_symbol.items():
            if len(events) == 0:
                continue
            
            # Check for pump-follow patterns
            pump_correlation = self._check_pump_follow_pattern(new_event, events)
            if pump_correlation:
                correlations.append(pump_correlation)
                self.db.update_symbol_correlation(
                    new_event.symbol, symbol, 
                    "PUMP_FOLLOW", pump_correlation.strength, pump_correlation.time_delay
                )
            
            # Check for dump-follow patterns  
            dump_correlation = self._check_dump_follow_pattern(new_event, events)
            if dump_correlation:
                correlations.append(dump_correlation)
                self.db.update_symbol_correlation(
                    new_event.symbol, symbol,
                    "DUMP_FOLLOW", dump_correlation.strength, dump_correlation.time_delay
                )
        
        return correlations
    
    def _check_pump_follow_pattern(self, new_event: MarketEvent, recent_events: List[MarketEvent]) -> Optional[CorrelationPattern]:
        """Check if new pump follows recent pumps in another symbol"""
        
        if new_event.event_type != "PUMP":
            return None
        
        recent_pumps = [e for e in recent_events if e.event_type == "PUMP"]
        if not recent_pumps:
            return None
        
        closest_pump = min(recent_pumps, key=lambda x: abs(x.timestamp - new_event.timestamp))
        time_delay = abs(new_event.timestamp - closest_pump.timestamp) // 60  # minutes
        
        # Calculate correlation strength
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
        """Check for dump-follow correlations"""
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
        """Detect risk of market cascade"""
        
        recent_threshold = new_event.timestamp - 3600  # Last hour
        recent_events = [e for e in self.active_events if e.timestamp > recent_threshold]
        
        pumps_count = len([e for e in recent_events if e.event_type == "PUMP"])
        dumps_count = len([e for e in recent_events if e.event_type == "DUMP"])
        
        # Calculate cascade risk factors
        cascade_risk_score = 0.0
        
        if pumps_count >= 5:  # Many recent pumps
            cascade_risk_score += 0.4
        
        if new_event.event_type == "DUMP" and dumps_count >= 2:  # Dump cascade starting
            cascade_risk_score += 0.4
        
        # Time-based risk
        hour = datetime.fromtimestamp(new_event.timestamp).hour
        if hour in [22, 23, 0, 1, 2]:  # Late night dumps
            cascade_risk_score += 0.2
        
        return {
            'cascade_risk_score': min(cascade_risk_score, 1.0),
            'recent_pumps': pumps_count,
            'recent_dumps': dumps_count,
            'risk_level': 'HIGH' if cascade_risk_score > 0.7 else 'MEDIUM' if cascade_risk_score > 0.4 else 'LOW',
            'estimated_cascade_time': new_event.timestamp + (45 * 60) if pumps_count >= 3 else None
        }
    
    def _assess_market_regime(self) -> Dict:
        """Assess current market regime"""
        
        recent_events = list(self.active_events)[-50:]  # Last 50 events
        
        if len(recent_events) < 10:
            return {'regime': 'INSUFFICIENT_DATA', 'confidence': 0.0}
        
        pumps = [e for e in recent_events if e.event_type == "PUMP"]
        dumps = [e for e in recent_events if e.event_type == "DUMP"]
        
        pump_ratio = len(pumps) / len(recent_events)
        avg_volume_multiple = statistics.mean(e.volume_multiple for e in recent_events)
        
        # Determine regime
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
        """Calculate coordination score"""
        
        if len(events) < 5:
            return 0.0
        
        # Group events by time windows (5-minute windows)
        time_windows = defaultdict(list)
        for event in events:
            window = (event.timestamp // 300) * 300
            time_windows[window].append(event)
        
        # Find coordinated windows
        coordinated_windows = [w for w, evs in time_windows.items() if len(evs) >= 3]
        coordination_score = len(coordinated_windows) / len(time_windows) if time_windows else 0
        
        return min(coordination_score, 1.0)
    
    def _generate_predictions(self, event: MarketEvent, correlations: List[CorrelationPattern], 
                            cascade_risk: Dict) -> List[Dict]:
        """Generate predictions based on patterns"""
        
        predictions = []
        
        # Correlation-based predictions
        for corr in correlations:
            if corr.correlation_type == "PUMP_FOLLOW":
                predictions.append({
                    'type': 'CORRELATION_PUMP',
                    'target_symbol': corr.symbol_pair[1],
                    'predicted_time': event.timestamp + (corr.time_delay * 60),
                    'confidence': corr.confidence,
                    'reason': f"Follows {corr.symbol_pair[0]} pump pattern"
                })
        
        # Cascade predictions
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
#   ENHANCED TRADING BOT
# =========================
class AdvancedPatternTradingBot:
    """Trading bot with pattern correlation analysis - Railway compatible"""
    
    def __init__(self):
        # Initialize components
        self.db = FileBasedPatternDB()
        self.correlation_engine = PatternCorrelationEngine(self.db)
        
        # Bot configuration
        self.exchanges = {}
        self.watchlist = {}
        self.last_alert_ts = defaultdict(lambda: 0.0)
        
        # Configuration from environment
        self.exchanges_list = os.getenv("EXCHANGES", "binance,bingx").split(",")
        self.quote_filter = os.getenv("QUOTE_FILTER", "USDT").split(",")
        self.top_n_by_volume = int(os.getenv("TOP_N_BY_VOLUME", "50"))
        self.timeframe = os.getenv("TIMEFRAME", "1m")
        self.threshold = float(os.getenv("THRESHOLD", "3.0"))
        self.min_price_change = float(os.getenv("MIN_PRICE_CHANGE", "0.04"))
        self.sleep_seconds = int(os.getenv("SLEEP_SECONDS", "20"))
        self.cooldown_minutes = int(os.getenv("COOLDOWN_MINUTES", "15"))
        self.debug_mode = os.getenv("DEBUG_MODE", "true").lower() == "true"
        
        # Telegram
        self.tg_token = os.getenv("TG_TOKEN", "")
        self.tg_chat_id = os.getenv("TG_CHAT_ID", "")
        
        # Stats
        self.stats = {
            'events_logged': 0,
            'correlations_found': 0,
            'predictions_made': 0,
            'cascade_warnings': 0,
            'start_time': time.time()
        }
        
        print("Advanced Pattern Trading Bot initialized (Railway compatible)")
    
    def send_telegram(self, msg: str):
        """Send message to Telegram"""
        if not self.tg_token or not self.tg_chat_id:
            if self.debug_mode:
                print("[Telegram] Not configured. Message:")
                print(msg)
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
        """Build exchange"""
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
        """Get top volume symbols"""
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
        """Calculate RSI"""
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
        """Safe OHLCV fetch"""
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
        """Check if can send alert"""
        if (now_ts - self.last_alert_ts[key]) >= self.cooldown_minutes * 60:
            self.last_alert_ts[key] = now_ts
            return True
        return False
    
    def generate_correlation_alert(self, event: MarketEvent, analysis: Dict) -> str:
        """Generate correlation alert"""
        
        msg = f"""🧬 <b>PATTERN CORRELATION DETECTED</b>

🎯 <b>Event:</b> {event.symbol} ({event.exchange.upper()})
📊 <b>Type:</b> {event.event_type}
⚡ <b>Strength:</b> {event.event_strength}/10
💹 <b>Volume:</b> {event.volume_multiple:.1f}x
📈 <b>Price:</b> {event.price_change_pct:+.1f}%
🕐 <b>Time:</b> {datetime.fromtimestamp(event.timestamp).strftime('%H:%M:%S UTC')}"""

        # Add correlations
        correlations = analysis.get('correlations_found', [])
        if correlations:
            msg += f"\n\n🔗 <b>CORRELATIONS:</b>"
            for corr in correlations[:3]:
                other_symbol = corr.symbol_pair[0] if corr.symbol_pair[1] == event.symbol else corr.symbol_pair[1]
                msg += f"\n• {other_symbol}: {corr.correlation_type} ({corr.strength:.2f}, {corr.time_delay}min)"

        # Add cascade risk
        cascade_risk = analysis.get('cascade_risk', {})
        if cascade_risk.get('cascade_risk_score', 0) > 0.5:
            msg += f"\n\n⚠️ <b>CASCADE RISK: {cascade_risk.get('risk_level', 'MEDIUM')}</b>"
            msg += f"\n• Recent pumps: {cascade_risk.get('recent_pumps', 0)}"
            msg += f"\n• Risk score: {cascade_risk['cascade_risk_score']:.2f}"

        # Add market regime
        regime = analysis.get('market_regime', {}).get('regime', 'UNKNOWN')
        confidence = analysis.get('market_regime', {}).get('confidence', 0)
        msg += f"\n\n🏛️ <b>Regime:</b> {regime} ({confidence:.2f})"

        # Add predictions
        predictions = analysis.get('predictions', [])
        if predictions:
            msg += f"\n\n🔮 <b>PREDICTIONS:</b>"
            for pred in predictions[:2]:
                pred_time = datetime.fromtimestamp(pred['predicted_time'])
                msg += f"\n• {pred['type']}: {pred_time.strftime('%H:%M')} ({pred['confidence']:.2f})"

        return msg
    
    def run(self):
        """Main execution method"""
        try:
            # Initialize exchanges
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
            
            # Send startup notification
            startup_msg = f"""🧬 <b>PATTERN CORRELATION BOT ONLINE</b>

🏦 <b>Exchanges:</b> {', '.join(self.exchanges.keys())}
📊 <b>Symbols:</b> {total_symbols} total
🔬 <b>Analysis:</b> Real-time correlation detection
🎯 <b>Features:</b> Cascade warnings, regime analysis

Ready for pattern correlation analysis! 🚀"""
            
            self.send_telegram(startup_msg)
            
            # Start main loop
            self.run_correlation_analysis_loop()
            
        except KeyboardInterrupt:
            print("\n👋 Bot stopped")
            
            uptime_hours = (time.time() - self.stats['start_time']) / 3600
            shutdown_msg = f"""👋 <b>CORRELATION BOT OFFLINE</b>

📊 <b>Stats:</b> {uptime_hours:.1f}h runtime
• Events: {self.stats['events_logged']}
• Correlations: {self.stats['correlations_found']}
• Predictions: {self.stats['predictions_made']}"""
            
            self.send_telegram(shutdown_msg)
            
        except Exception as e:
            error_msg = f"❌ Bot crashed: {e}"
            print(error_msg)
            self.send_telegram(error_msg)
            raise
    
    def run_correlation_analysis_loop(self):
        """Main analysis loop"""
        print("🔬 Starting correlation analysis...")
        
        loop_count = 0
        
        while True:
            loop_start = time.time()
            loop_count += 1
            
            if self.debug_mode and loop_count % 20 == 0:
                uptime = (time.time() - self.stats['start_time']) / 3600
                print(f"[STATS] Loop #{loop_count}, {uptime:.1f}h uptime")
                print(f"  Events: {self.stats['events_logged']}, Correlations: {self.stats['correlations_found']}")
            
            for exchange_name, ex in self.exchanges.items():
                symbols = self.watchlist.get(exchange_name, [])
                
                for symbol in symbols:
                    try:
                        ohlcv = self.fetch_ohlcv_safe(ex, symbol, self.timeframe, 20)
                        if not ohlcv or len(ohlcv) < 10:
                            continue
                        # Calculate metrics
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
                        
                        # Apply detection thresholds
                        if vol_multiple < self.threshold or abs(price_change_pct) < self.min_price_change:
                            continue
                        
                        # Calculate RSI
                        prices = [c[4] for c in ohlcv]
                        rsi = self.calculate_rsi(prices)
                        
                        # Determine event type and strength
                        event_type = "PUMP" if price_change_pct > 0 else "DUMP"
                        event_strength = min(int((vol_multiple / 2 + abs(price_change_pct) * 20)), 10)
                        
                        # Create market event
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
                        
                        # Process through correlation engine
                        analysis = self.correlation_engine.process_new_event(event)
                        
                        # Update stats
                        self.stats['events_logged'] += 1
                        
                        if analysis['correlations_found']:
                            self.stats['correlations_found'] += len(analysis['correlations_found'])
                        
                        if analysis['predictions']:
                            self.stats['predictions_made'] += len(analysis['predictions'])
                        
                        if analysis['cascade_risk']['cascade_risk_score'] > 0.7:
                            self.stats['cascade_warnings'] += 1
                        
                        # Determine alert conditions
                        should_alert = False
                        alert_key = f"CORRELATION:{exchange_name}:{symbol}"
                        
                        if len(analysis['correlations_found']) >= 2:  # Multiple correlations
                            should_alert = True
                            alert_key = f"MULTI_CORR:{exchange_name}:{symbol}"
                        elif analysis['cascade_risk']['cascade_risk_score'] > 0.6:  # High cascade risk
                            should_alert = True
                            alert_key = f"CASCADE:{analysis['cascade_risk']['risk_level']}"
                        elif analysis['market_regime']['regime'] in ['PUMP_MANIPULATION', 'HIGH_MANIPULATION']:  # Manipulation
                            should_alert = True
                            alert_key = f"MANIPULATION:{analysis['market_regime']['regime']}"
                        elif len(analysis['predictions']) >= 2:  # Multiple predictions
                            should_alert = True
                            alert_key = f"PREDICTIONS:{exchange_name}:{symbol}"
                        
                        if should_alert and self.can_alert(alert_key, time.time()):
                            alert_message = self.generate_correlation_alert(event, analysis)
                            self.send_telegram(alert_message)
                            
                            if self.debug_mode:
                                print(f"[ALERT] {exchange_name} {symbol}: {event_type} {vol_multiple:.1f}x")
                                print(f"  Correlations: {len(analysis['correlations_found'])}")
                                print(f"  Cascade risk: {analysis['cascade_risk']['cascade_risk_score']:.2f}")
                    
                    except Exception as e:
                        if self.debug_mode:
                            print(f"Error processing {exchange_name} {symbol}: {e}")
                        continue
            
            # Sleep with timing control
            elapsed = time.time() - loop_start
            sleep_time = max(0, self.sleep_seconds - elapsed)
            time.sleep(sleep_time)

# =========================
#   MAIN EXECUTION
# =========================
def main():
    """Main function"""
    print("🧬 Advanced Pattern Correlation Bot Starting (Railway Compatible)...")
    
    bot = AdvancedPatternTradingBot()
    bot.run()

if __name__ == "__main__":
    main()
