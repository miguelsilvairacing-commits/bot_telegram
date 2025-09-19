import os
import time
import json
import sqlite3
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
#   ADVANCED PATTERN DATABASE
# =========================
class AdvancedPatternDB:
    """Advanced SQLite database for pattern correlation analysis"""
    
    def __init__(self, db_path="market_patterns.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize comprehensive database schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Market events table - core event logging
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            event_type TEXT NOT NULL,  -- PUMP, DUMP, VOLUME_SPIKE
            volume_multiple REAL NOT NULL,
            price_change_pct REAL NOT NULL,
            rsi REAL,
            market_cap_est REAL,
            volume_24h REAL,
            event_strength INTEGER,  -- 1-10 scale
            session_id TEXT,  -- Groups related events
            created_at INTEGER DEFAULT (strftime('%s','now'))
        )
        ''')
        
        # Market sessions - periods of coordinated activity
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT UNIQUE NOT NULL,
            start_time INTEGER NOT NULL,
            end_time INTEGER,
            session_type TEXT,  -- PUMP_CYCLE, DUMP_CYCLE, MIXED
            total_events INTEGER DEFAULT 0,
            avg_event_strength REAL,
            dominant_exchanges TEXT,
            outcome TEXT,  -- SUCCESS, DUMP, SIDEWAYS
            correlation_strength REAL,
            manipulation_score REAL,
            created_at INTEGER DEFAULT (strftime('%s','now'))
        )
        ''')
        
        # Symbol patterns - individual coin behavior patterns
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS symbol_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            pattern_type TEXT NOT NULL,
            pattern_data TEXT,  -- JSON with pattern details
            frequency_per_week REAL,
            success_rate REAL,
            avg_duration_minutes INTEGER,
            avg_peak_gain_pct REAL,
            avg_dump_loss_pct REAL,
            last_occurrence INTEGER,
            pattern_reliability REAL,
            created_at INTEGER DEFAULT (strftime('%s','now'))
        )
        ''')
        
        # Correlation matrix - which symbols move together
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS symbol_correlations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol_1 TEXT NOT NULL,
            symbol_2 TEXT NOT NULL,
            correlation_type TEXT,  -- PUMP_FOLLOW, DUMP_FOLLOW, INVERSE
            correlation_strength REAL,
            time_delay_minutes INTEGER,  -- How long between movements
            sample_size INTEGER,
            last_updated INTEGER,
            created_at INTEGER DEFAULT (strftime('%s','now')),
            UNIQUE(symbol_1, symbol_2, correlation_type)
        )
        ''')
        
        # Market regime tracking
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_regimes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            regime_type TEXT NOT NULL,  -- MANIPULATION, ORGANIC, SIDEWAYS, DUMP_CASCADE
            confidence REAL NOT NULL,
            active_symbols INTEGER,
            total_volume_multiple REAL,
            coordination_score REAL,
            prediction_accuracy REAL,
            regime_duration_minutes INTEGER,
            created_at INTEGER DEFAULT (strftime('%s','now'))
        )
        ''')
        
        # Predictive outcomes - track prediction accuracy
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            prediction_type TEXT NOT NULL,
            symbols_involved TEXT,
            predicted_outcome TEXT,
            confidence REAL,
            actual_outcome TEXT,
            accuracy_score REAL,
            time_horizon_minutes INTEGER,
            model_version TEXT,
            created_at INTEGER DEFAULT (strftime('%s','now'))
        )
        ''')
        
        # Create indexes for performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_market_events_timestamp ON market_events(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_market_events_symbol ON market_events(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_sessions_start_time ON market_sessions(start_time)",
            "CREATE INDEX IF NOT EXISTS idx_correlations_symbols ON symbol_correlations(symbol_1, symbol_2)",
            "CREATE INDEX IF NOT EXISTS idx_regimes_timestamp ON market_regimes(timestamp)"
        ]
        
        for idx in indexes:
            cursor.execute(idx)
        
        conn.commit()
        conn.close()
        print("Advanced pattern database initialized with correlation tracking")
    
    def log_market_event(self, event_data: Dict) -> str:
        """Log a market event and return session ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Generate or get session ID for this time window
        session_id = self._get_or_create_session(event_data['timestamp'])
        event_data['session_id'] = session_id
        
        cursor.execute('''
        INSERT INTO market_events 
        (timestamp, exchange, symbol, event_type, volume_multiple, price_change_pct, 
         rsi, market_cap_est, volume_24h, event_strength, session_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            event_data['timestamp'],
            event_data['exchange'],
            event_data['symbol'],
            event_data['event_type'],
            event_data['volume_multiple'],
            event_data['price_change_pct'],
            event_data.get('rsi'),
            event_data.get('market_cap_est'),
            event_data.get('volume_24h'),
            event_data.get('event_strength', 5),
            session_id
        ))
        
        conn.commit()
        conn.close()
        return session_id
    
    def _get_or_create_session(self, timestamp: int) -> str:
        """Get or create session ID for time window (30 min windows)"""
        # Round to 30-minute windows
        window_start = (timestamp // 1800) * 1800
        session_id = f"SESSION_{window_start}"
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if session exists
        cursor.execute('SELECT id FROM market_sessions WHERE session_id = ?', (session_id,))
        if not cursor.fetchone():
            cursor.execute('''
            INSERT INTO market_sessions (session_id, start_time, total_events)
            VALUES (?, ?, 0)
            ''', (session_id, window_start))
        
        # Increment event count
        cursor.execute('''
        UPDATE market_sessions 
        SET total_events = total_events + 1 
        WHERE session_id = ?
        ''', (session_id,))
        
        conn.commit()
        conn.close()
        return session_id
    
    def get_recent_events(self, hours: int = 2) -> List[Dict]:
        """Get recent market events"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        since = int((datetime.now() - timedelta(hours=hours)).timestamp())
        
        cursor.execute('''
        SELECT * FROM market_events 
        WHERE timestamp > ? 
        ORDER BY timestamp DESC
        ''', (since,))
        
        events = []
        for row in cursor.fetchall():
            events.append({
                'id': row[0], 'timestamp': row[1], 'exchange': row[2], 
                'symbol': row[3], 'event_type': row[4], 'volume_multiple': row[5],
                'price_change_pct': row[6], 'rsi': row[7], 'session_id': row[10]
            })
        
        conn.close()
        return events
    
    def update_symbol_correlation(self, symbol1: str, symbol2: str, 
                                 correlation_type: str, strength: float, delay: int):
        """Update correlation between two symbols"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT OR REPLACE INTO symbol_correlations 
        (symbol_1, symbol_2, correlation_type, correlation_strength, time_delay_minutes, 
         sample_size, last_updated)
        VALUES (?, ?, ?, ?, ?, 
                COALESCE((SELECT sample_size FROM symbol_correlations 
                         WHERE symbol_1=? AND symbol_2=? AND correlation_type=?), 0) + 1,
                ?)
        ''', (symbol1, symbol2, correlation_type, strength, delay, 
              symbol1, symbol2, correlation_type, int(time.time())))
        
        conn.commit()
        conn.close()
    
    def get_symbol_correlations(self, symbol: str, min_strength: float = 0.7) -> List[Dict]:
        """Get correlations for a symbol"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT * FROM symbol_correlations 
        WHERE (symbol_1 = ? OR symbol_2 = ?) 
        AND correlation_strength >= ?
        ORDER BY correlation_strength DESC
        ''', (symbol, symbol, min_strength))
        
        correlations = []
        for row in cursor.fetchall():
            correlations.append({
                'other_symbol': row[2] if row[1] == symbol else row[1],
                'correlation_type': row[3],
                'strength': row[4],
                'delay_minutes': row[5],
                'sample_size': row[6]
            })
        
        conn.close()
        return correlations

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
    
    def __init__(self, db: AdvancedPatternDB):
        self.db = db
        self.active_events = deque(maxlen=1000)  # Keep recent events in memory
        self.correlation_threshold = 0.7
        self.time_window_minutes = 30
        
        # Load existing correlations
        self.known_correlations = self._load_correlations()
        
        print("Pattern Correlation Engine initialized")
    
    def _load_correlations(self) -> Dict:
        """Load existing correlations from database"""
        correlations = {}
        # Implementation would load from database
        return correlations
    
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
                
                # Update database
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
            
            # Check for inverse patterns
            inverse_correlation = self._check_inverse_pattern(new_event, events)
            if inverse_correlation:
                correlations.append(inverse_correlation)
                
                self.db.update_symbol_correlation(
                    new_event.symbol, symbol,
                    "INVERSE", inverse_correlation.strength, inverse_correlation.time_delay
                )
        
        return correlations
    
    def _check_pump_follow_pattern(self, new_event: MarketEvent, recent_events: List[MarketEvent]) -> Optional[CorrelationPattern]:
        """Check if new pump follows recent pumps in another symbol"""
        
        if new_event.event_type != "PUMP":
            return None
        
        # Find recent pump events in the other symbol
        recent_pumps = [e for e in recent_events if e.event_type == "PUMP"]
        
        if not recent_pumps:
            return None
        
        # Find the closest pump in time
        closest_pump = min(recent_pumps, key=lambda x: abs(x.timestamp - new_event.timestamp))
        time_delay = abs(new_event.timestamp - closest_pump.timestamp) // 60  # minutes
        
        # Calculate correlation strength based on:
        # - Time proximity
        # - Volume similarity  
        # - Strength similarity
        
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
        # Similar logic to pump_follow but for dumps
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
    
    def _check_inverse_pattern(self, new_event: MarketEvent, recent_events: List[MarketEvent]) -> Optional[CorrelationPattern]:
        """Check for inverse correlations (when one pumps, other dumps)"""
        
        # Look for opposite events
        opposite_type = "DUMP" if new_event.event_type == "PUMP" else "PUMP"
        opposite_events = [e for e in recent_events if e.event_type == opposite_type]
        
        if not opposite_events:
            return None
        
        closest_opposite = min(opposite_events, key=lambda x: abs(x.timestamp - new_event.timestamp))
        time_delay = abs(new_event.timestamp - closest_opposite.timestamp) // 60
        
        time_score = max(0, 1 - (time_delay / self.time_window_minutes))
        
        if time_score >= self.correlation_threshold:
            return CorrelationPattern(
                symbol_pair=(closest_opposite.symbol, new_event.symbol),
                correlation_type="INVERSE",
                strength=time_score,
                time_delay=int(time_delay),
                confidence=time_score,
                sample_size=1
            )
        
        return None
    
    def _detect_cascade_risk(self, new_event: MarketEvent) -> Dict:
        """Detect risk of market cascade (multiple dumps following pumps)"""
        
        # Count recent events by type in last 60 minutes
        recent_threshold = new_event.timestamp - 3600
        recent_events = [e for e in self.active_events if e.timestamp > recent_threshold]
        
        pumps_count = len([e for e in recent_events if e.event_type == "PUMP"])
        dumps_count = len([e for e in recent_events if e.event_type == "DUMP"])
        
        # Calculate cascade risk factors
        pump_concentration = pumps_count / max(len(set(e.symbol for e in recent_events)), 1)
        
        # If many symbols pumped recently, dump risk is higher
        cascade_risk_score = 0.0
        
        if pumps_count >= 5:  # Many recent pumps
            cascade_risk_score += 0.4
        
        if pump_concentration > 0.7:  # Concentrated in few symbols
            cascade_risk_score += 0.3
        
        if new_event.event_type == "DUMP" and dumps_count >= 2:  # Dump cascade starting
            cascade_risk_score += 0.3
        
        # Time-based risk (certain hours are riskier)
        hour = datetime.fromtimestamp(new_event.timestamp).hour
        if hour in [22, 23, 0, 1, 2]:  # Late night dumps
            cascade_risk_score += 0.2
        
        return {
            'cascade_risk_score': min(cascade_risk_score, 1.0),
            'recent_pumps': pumps_count,
            'recent_dumps': dumps_count,
            'risk_level': 'HIGH' if cascade_risk_score > 0.7 else 'MEDIUM' if cascade_risk_score > 0.4 else 'LOW',
            'estimated_cascade_time': self._estimate_cascade_timing(recent_events)
        }
    
    def _estimate_cascade_timing(self, recent_events: List[MarketEvent]) -> Optional[int]:
        """Estimate when cascade might occur based on historical patterns"""
        
        # Look for pump-to-dump timing patterns
        pump_events = [e for e in recent_events if e.event_type == "PUMP"]
        
        if len(pump_events) < 3:
            return None
        
        # Calculate average time from pump cluster to first dumps
        # This is simplified - real implementation would use historical data
        avg_pump_to_dump_minutes = 45  # Based on pattern analysis
        
        latest_pump = max(pump_events, key=lambda x: x.timestamp)
        estimated_cascade_time = latest_pump.timestamp + (avg_pump_to_dump_minutes * 60)
        
        return estimated_cascade_time
    
    def _assess_market_regime(self) -> Dict:
        """Assess current market regime"""
        
        recent_events = list(self.active_events)[-50:]  # Last 50 events
        
        if len(recent_events) < 10:
            return {'regime': 'INSUFFICIENT_DATA', 'confidence': 0.0}
        
        # Calculate metrics
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
        elif 0.3 <= pump_ratio <= 0.7:
            regime = "MIXED_SIGNALS"
            confidence = 0.6
        else:
            regime = "SIDEWAYS"
            confidence = 0.5
        
        return {
            'regime': regime,
            'confidence': confidence,
            'pump_ratio': pump_ratio,
            'avg_volume': avg_volume_multiple,
            'coordination_score': self._calculate_coordination_score(recent_events)
        }
    
    def _calculate_coordination_score(self, events: List[MarketEvent]) -> float:
        """Calculate how coordinated the market activity appears"""
        
        if len(events) < 5:
            return 0.0
        
        # Group events by time windows (5-minute windows)
        time_windows = defaultdict(list)
        for event in events:
            window = (event.timestamp // 300) * 300  # 5-minute windows
            time_windows[window].append(event)
        
        # Find windows with multiple events (coordination)
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
            
            elif corr.correlation_type == "DUMP_FOLLOW":
                predictions.append({
                    'type': 'CORRELATION_DUMP',
                    'target_symbol': corr.symbol_pair[1], 
                    'predicted_time': event.timestamp + (corr.time_delay * 60),
                    'confidence': corr.confidence,
                    'reason': f"Follows {corr.symbol_pair[0]} dump pattern"
                })
        
        # Cascade-based predictions
        if cascade_risk['cascade_risk_score'] > 0.6:
            predictions.append({
                'type': 'MARKET_CASCADE',
                'target_symbol': 'MULTIPLE',
                'predicted_time': cascade_risk.get('estimated_cascade_time', event.timestamp + 2700),  # 45 min default
                'confidence': cascade_risk['cascade_risk_score'],
                'reason': f"High cascade risk detected ({cascade_risk['recent_pumps']} recent pumps)"
            })
        
        return predictions

# =========================
#   ENHANCED TRADING BOT WITH PATTERN CORRELATION
# =========================
class AdvancedPatternTradingBot:
    """Trading bot with advanced pattern correlation analysis"""
    
    def __init__(self):
        # Initialize components
        self.db = AdvancedPatternDB()
        self.correlation_engine = PatternCorrelationEngine(self.db)
        
        # Bot configuration
        self.exchanges = {}
        self.watchlist = {}
        self.last_alert_ts = defaultdict(lambda: 0.0)
        
        # Configuration from environment
        self.exchanges_list = os.getenv("EXCHANGES", "binance,bingx").split(",")
        self.quote_filter = os.getenv("QUOTE_FILTER", "USDT").split(",")
        self.top_n_by_volume = int(os.getenv("TOP_N_BY_VOLUME", "50"))  # More symbols for correlation analysis
        self.timeframe = os.getenv("TIMEFRAME", "1m")
        self.threshold = float(os.getenv("THRESHOLD", "3.0"))
        self.min_price_change = float(os.getenv("MIN_PRICE_CHANGE", "0.04"))
        self.sleep_seconds = int(os.getenv("SLEEP_SECONDS", "20"))  # Faster scanning for patterns
        self.cooldown_minutes = int(os.getenv("COOLDOWN_MINUTES", "15"))  # Shorter cooldown for correlation alerts
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
        
        print("Advanced Pattern Trading Bot with Correlation Analysis initialized")
    
    def send_telegram(self, msg: str):
        """Send message to Telegram"""
        if not self.tg_token or not self.tg_chat_id:
            if self.debug_mode:
                print("[Telegram] Not configured. Message would be:")
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
        """Build exchange with configurations"""
        name = name.strip().lower()
        if not hasattr(ccxt, name):
            raise ValueError(f"Exchange '{name}' not found")
        
        config = {"enableRateLimit": True, "options": {"adjustForTimeDifference": True}}
        
        if name == "bingx":
            config.update({"timeout": 30000, "rateLimit": 1500})  # Faster for correlation analysis
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
                if volume and 100_000 <= volume <= 100_000_000:  # Wider range for correlation analysis
                    volume_pairs.append((symbol, volume))
            
            volume_pairs.sort(key=lambda x: x[1], reverse=True)
            return [symbol for symbol, _ in volume_pairs[:limit]]
            
        except Exception as e:
            print(f"Error getting symbols: {e}")
            return []
    
    def generate_correlation_alert(self, event: MarketEvent, analysis: Dict) -> str:
        """Generate advanced correlation-based alert"""
        
        msg = f"""🧬 <b>PATTERN CORRELATION DETECTED</b>

<b>🎯 Event:</b> {event.symbol} ({event.exchange.upper()})
<b>📊 Type:</b> {event.event_type}
<b>⚡ Strength:</b> {event.event_strength}/10
<b>💹 Volume:</b> {event.volume_multiple:.1f}x
<b>📈 Price:</b> {event.price_change_pct:+.1f}%
<b>🕐 Time:</b> {datetime.fromtimestamp(event.timestamp).strftime('%H:%M:%S UTC')}"""

        # Add correlation information
        correlations = analysis.get('correlations_found', [])
        if correlations:
            msg += f"\n\n<b>🔗 CORRELATIONS FOUND:</b>"
            for i, corr in enumerate(correlations[:3]):  # Show top 3
                other_symbol = corr.symbol_pair[0] if corr.symbol_pair[1] == event.symbol else corr.symbol_pair[1]
                msg += f"\n• {other_symbol}: {corr.correlation_type} ({corr.strength:.2f} strength, {corr.time_delay}min delay)"

        # Add cascade risk
        cascade_risk = analysis.get('cascade_risk', {})
        if cascade_risk.get('cascade_risk_score', 0) > 0.5:
            risk_level = cascade_risk.get('risk_level', 'MEDIUM')
            recent_pumps = cascade_risk.get('recent_pumps', 0)
            recent_dumps = cascade_risk.get('recent_dumps', 0)
            
            msg += f"\n\n<b>⚠️ CASCADE RISK: {risk_level}</b>"
            msg += f"\n• Recent pumps: {recent_pumps}"
            msg += f"\n• Recent dumps: {recent_dumps}"
            msg += f"\n• Risk score: {cascade_risk['cascade_risk_score']:.2f}/1.0"
            
            if cascade_risk.get('estimated_cascade_time'):
                est_time = datetime.fromtimestamp(cascade_risk['estimated_cascade_time'])
                msg += f"\n• Est. cascade time: {est_time.strftime('%H:%M UTC')}"

        # Add market regime
        market_regime = analysis.get('market_regime', {})
        if market_regime:
            regime = market_regime.get('regime', 'UNKNOWN')
            confidence = market_regime.get('confidence', 0)
            coordination = market_regime.get('coordination_score', 0)
            
            msg += f"\n\n<b>🏛️ Market Regime:</b> {regime}"
            msg += f"\n• Confidence: {confidence:.2f}"
            msg += f"\n• Coordination: {coordination:.2f}"

        # Add predictions
        predictions = analysis.get('predictions', [])
        if predictions:
            msg += f"\n\n<b>🔮 PREDICTIONS:</b>"
            for pred in predictions[:2]:  # Show top 2 predictions
                pred_time = datetime.fromtimestamp(pred['predicted_time'])
                msg += f"\n• {pred['type']}: {pred_time.strftime('%H:%M')} ({pred['confidence']:.2f} confidence)"
                msg += f"\n  Reason: {pred['reason']}"

        # Add trading recommendations
        msg += f"\n\n<b>💡 RECOMMENDATIONS:</b>"
        
        if event.event_type == "PUMP":
            if cascade_risk.get('cascade_risk_score', 0) > 0.7:
                msg += f"\n🔴 <b>AVOID</b> - High cascade risk detected"
            elif len(correlations) >= 2:
                msg += f"\n🟡 <b>CAUTIOUS</b> - Multiple correlations active"
            else:
                msg += f"\n🟢 <b>CONSIDER</b> - Single event pattern"
        else:  # DUMP
            if len(correlations) >= 2:
                msg += f"\n🔴 <b>DUMP CASCADE</b> - Multiple dumps correlating"
            else:
                msg += f"\n🟡 <b>MONITOR</b> - Isolated dump event"

        return msg
    
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
    
    def run(self):
        """Main execution method"""
        try:
            # Initialize exchanges
            print("🏦 Initializing exchanges for pattern correlation analysis...")
            for exchange_name in self.exchanges_list:
                exchange_name = exchange_name.strip()
                if not exchange_name:
                    continue
                
                try:
                    ex = self.build_exchange(exchange_name)
                    self.exchanges[exchange_name] = ex
                    
                    symbols = self.get_symbols_for_exchange(ex, self.top_n_by_volume)
                    self.watchlist[exchange_name] = symbols
                    
                    print(f"✅ {exchange_name}: {len(symbols)} symbols for correlation analysis")
                    
                except Exception as e:
                    print(f"❌ Failed to initialize {exchange_name}: {e}")
            
            if not self.exchanges:
                raise SystemExit("❌ No exchanges initialized")
            
            total_symbols = sum(len(symbols) for symbols in self.watchlist.values())
            
            # Send startup notification
            startup_msg = f"""🧬 <b>ADVANCED PATTERN CORRELATION BOT ONLINE</b>

🏦 <b>Exchanges:</b> {', '.join(self.exchanges.keys())}
📊 <b>Symbols:</b> {total_symbols} total (correlation matrix)
🔬 <b>Analysis Features:</b>
• Real-time correlation detection
• Cascade pattern recognition  
• Market regime analysis
• Predictive pattern matching
• Cross-symbol event tracking

🎯 <b>Unique Capabilities:</b>
• Pump-follow correlations
• Dump cascade warnings
• Market manipulation detection
• Coordinated activity alerts

Ready for advanced pattern correlation analysis! 🚀"""
            
            self.send_telegram(startup_msg)
            
            # Start main analysis loop
            self.run_correlation_analysis_loop()
            
        except KeyboardInterrupt:
            print("\n👋 Bot stopped by user")
            
            uptime_hours = (time.time() - self.stats['start_time']) / 3600
            shutdown_msg = f"""👋 <b>PATTERN CORRELATION BOT OFFLINE</b>

📊 <b>Session Analysis Stats:</b>
• Runtime: {uptime_hours:.1f} hours
• Events logged: {self.stats['events_logged']}
• Correlations found: {self.stats['correlations_found']}
• Predictions made: {self.stats['predictions_made']}
• Cascade warnings: {self.stats['cascade_warnings']}

Pattern database contains correlation intelligence for future sessions! 🧬"""
            
            self.send_telegram(shutdown_msg)
            
        except Exception as e:
            error_msg = f"❌ Pattern Correlation Bot crashed: {e}"
            print(error_msg)
            self.send_telegram(error_msg)
            raise
        
        finally:
            if hasattr(self, 'db'):
                self.db.close()
    
    def run_correlation_analysis_loop(self):
        """Main correlation analysis loop"""
        print("🔬 Starting advanced pattern correlation analysis...")
        
        loop_count = 0
        
        while True:
            loop_start = time.time()
            loop_count += 1
            
            if self.debug_mode and loop_count % 20 == 0:
                uptime = (time.time() - self.stats['start_time']) / 3600
                print(f"[CORRELATION STATS] Loop #{loop_count}, Uptime: {uptime:.1f}h")
                print(f"  Events: {self.stats['events_logged']}, Correlations: {self.stats['correlations_found']}")
                print(f"  Predictions: {self.stats['predictions_made']}, Cascades: {self.stats['cascade_warnings']}")
            
            events_this_loop = 0
            
            for exchange_name, ex in self.exchanges.items():
                symbols = self.watchlist.get(exchange_name, [])
                
                for symbol in symbols:
                    try:
                        # Fetch OHLCV data
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
                        
                        # Process event through correlation engine
                        analysis = self.correlation_engine.process_new_event(event)
                        
                        # Update stats
                        self.stats['events_logged'] += 1
                        events_this_loop += 1
                        
                        if analysis['correlations_found']:
                            self.stats['correlations_found'] += len(analysis['correlations_found'])
                        
                        if analysis['predictions']:
                            self.stats['predictions_made'] += len(analysis['predictions'])
                        
                        if analysis['cascade_risk']['cascade_risk_score'] > 0.7:
                            self.stats['cascade_warnings'] += 1
                        
                        # Determine if should send alert
                        should_alert = False
                        alert_key = f"CORRELATION:{exchange_name}:{symbol}"
                        
                        # Alert conditions for correlation system
                        if len(analysis['correlations_found']) >= 2:  # Multiple correlations
                            should_alert = True
                            alert_key = f"MULTI_CORR:{exchange_name}:{symbol}"
                        elif analysis['cascade_risk']['cascade_risk_score'] > 0.6:  # High cascade risk
                            should_alert = True
                            alert_key = f"CASCADE:{analysis['cascade_risk']['risk_level']}"
                        elif analysis['market_regime']['regime'] in ['PUMP_MANIPULATION', 'HIGH_MANIPULATION']:  # Market manipulation
                            should_alert = True
                            alert_key = f"MANIPULATION:{analysis['market_regime']['regime']}"
                        elif len(analysis['predictions']) >= 2:  # Multiple predictions
                            should_alert = True
                            alert_key = f"PREDICTIONS:{exchange_name}:{symbol}"
                        
                        if should_alert and self.can_alert(alert_key, time.time()):
                            # Generate and send correlation alert
                            alert_message = self.generate_correlation_alert(event, analysis)
                            self.send_telegram(alert_message)
                            
                            if self.debug_mode:
                                corr_count = len(analysis['correlations_found'])
                                pred_count = len(analysis['predictions'])
                                cascade_score = analysis['cascade_risk']['cascade_risk_score']
                                regime = analysis['market_regime']['regime']
                                
                                print(f"[CORRELATION ALERT] {exchange_name} {symbol}:")
                                print(f"  {event_type}: {vol_multiple:.1f}x vol, {price_change_pct*100:+.1f}%")
                                print(f"  Correlations: {corr_count}, Predictions: {pred_count}")
                                print(f"  Cascade risk: {cascade_score:.2f}, Regime: {regime}")
                    
                    except Exception as e:
                        if self.debug_mode:
                            print(f"Error processing {exchange_name} {symbol}: {e}")
                        continue
            
            # Performance info
            if self.debug_mode and events_this_loop > 0:
                print(f"[LOOP] Processed {events_this_loop} events this loop")
            
            # Sleep with timing control
            elapsed = time.time() - loop_start
            sleep_time = max(0, self.sleep_seconds - elapsed)
            
            if elapsed > self.sleep_seconds * 1.2:
                print(f"[PERFORMANCE] Slow loop: {elapsed:.1f}s (target: {self.sleep_seconds}s)")
            
            time.sleep(sleep_time)

# =========================
#   MAIN EXECUTION
# =========================
def main():
    """Main function for advanced pattern correlation bot"""
    print("🧬 Advanced Pattern Correlation Trading Bot Starting...")
    
    bot = AdvancedPatternTradingBot()
    bot.run()

if __name__ == "__main__":
    main()
