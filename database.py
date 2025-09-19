# database.py - Core Database System
import sqlite3
import json
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import threading

class PatternDatabase:
    """
    Core database system para pattern tracking e historical analysis
    """
    
    def __init__(self, db_path: str = None):
        # Use environment variable or default path
        if db_path is None:
            try:
                from config import Config
                db_path = Config.DATABASE_PATH
            except ImportError:
                db_path = os.getenv("DATABASE_PATH", "pattern_data.db")
        
        self.db_path = db_path
        self.connection = None
        self._lock = threading.Lock()
        
        # Ensure directory exists for database file
        db_dir = os.path.dirname(self.db_path) if os.path.dirname(self.db_path) else '.'
        if db_dir != '.' and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        self._init_database()
        print(f"✅ Database initialized: {self.db_path}")
    
    def _get_connection(self):
        """Get thread-safe database connection"""
        if not self.connection:
            try:
                self.connection = sqlite3.connect(
                    self.db_path, 
                    check_same_thread=False,
                    timeout=30.0  # 30 second timeout
                )
                self.connection.row_factory = sqlite3.Row
                
                # Enable WAL mode for better concurrency
                self.connection.execute("PRAGMA journal_mode=WAL")
                self.connection.execute("PRAGMA synchronous=NORMAL")
                self.connection.execute("PRAGMA temp_store=MEMORY")
                self.connection.execute("PRAGMA mmap_size=268435456")  # 256MB
                
            except Exception as e:
                print(f"❌ Database connection error: {e}")
                raise
                
        return self.connection
    
    def _init_database(self):
        """Initialize database with all required tables"""
        conn = self._get_connection()
        
        try:
            # Pump Events - Core table para todos os eventos
            conn.execute('''
                CREATE TABLE IF NOT EXISTS pump_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    price REAL NOT NULL,
                    volume_24h REAL,
                    volume_multiple REAL,
                    rsi REAL,
                    risk_score INTEGER,
                    pump_stage TEXT,
                    confidence TEXT,
                    price_change_pct REAL,
                    market_cap_est REAL,
                    
                    -- Pump fingerprint (JSON)
                    volume_profile TEXT,
                    price_signature TEXT,
                    rsi_journey TEXT,
                    
                    -- Outcomes (filled later)
                    max_gain_pct REAL,
                    max_loss_pct REAL,
                    time_to_peak_minutes INTEGER,
                    time_to_dump_minutes INTEGER,
                    final_outcome TEXT, -- 'success', 'dump', 'sideways'
                    
                    -- Pattern matching
                    pattern_id TEXT,
                    similarity_score REAL,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(timestamp, exchange, symbol, event_type)
                )
            ''')
            
            # Market Data - OHLCV histórico para análise
            conn.execute('''
                CREATE TABLE IF NOT EXISTS market_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    open_price REAL NOT NULL,
                    high_price REAL NOT NULL,
                    low_price REAL NOT NULL,
                    close_price REAL NOT NULL,
                    volume REAL NOT NULL,
                    quote_volume REAL,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(timestamp, exchange, symbol, timeframe)
                )
            ''')
            
            # Pattern Templates - Fingerprints de padrões conhecidos
            conn.execute('''
                CREATE TABLE IF NOT EXISTS pattern_templates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pattern_id TEXT UNIQUE NOT NULL,
                    pattern_name TEXT NOT NULL,
                    pattern_type TEXT NOT NULL, -- 'pump', 'dump', 'manipulation'
                    
                    -- Template features
                    volume_signature TEXT, -- JSON
                    price_signature TEXT,  -- JSON
                    rsi_signature TEXT,    -- JSON
                    timing_features TEXT,  -- JSON
                    
                    -- Success stats
                    total_occurrences INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    avg_duration_minutes REAL,
                    avg_max_gain REAL,
                    avg_max_loss REAL,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Exchange Performance Stats
            conn.execute('''
                CREATE TABLE IF NOT EXISTS exchange_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    exchange TEXT NOT NULL,
                    date TEXT NOT NULL, -- YYYY-MM-DD
                    
                    total_pumps INTEGER DEFAULT 0,
                    successful_pumps INTEGER DEFAULT 0,
                    avg_pump_duration REAL DEFAULT 0,
                    avg_success_rate REAL DEFAULT 0,
                    
                    total_volume_tracked REAL DEFAULT 0,
                    best_performing_symbol TEXT,
                    worst_performing_symbol TEXT,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(exchange, date)
                )
            ''')
            
            # Cross-Exchange Correlations
            conn.execute('''
                CREATE TABLE IF NOT EXISTS cross_correlations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    symbol1 TEXT NOT NULL,
                    symbol2 TEXT NOT NULL,
                    exchange1 TEXT NOT NULL,
                    exchange2 TEXT NOT NULL,
                    
                    correlation_score REAL NOT NULL,
                    price_discrepancy_pct REAL,
                    volume_ratio REAL,
                    
                    -- Context
                    market_regime TEXT, -- 'bull', 'bear', 'crab'
                    volatility_level TEXT, -- 'low', 'medium', 'high'
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(timestamp, symbol1, symbol2, exchange1, exchange2)
                )
            ''')
            
            # ML Predictions tracking
            conn.execute('''
                CREATE TABLE IF NOT EXISTS ml_predictions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    
                    model_name TEXT NOT NULL,
                    model_version TEXT,
                    
                    prediction_type TEXT NOT NULL, -- 'dump_time', 'success_prob', 'risk_score'
                    predicted_value REAL NOT NULL,
                    confidence_score REAL,
                    
                    -- Features used (JSON)
                    input_features TEXT,
                    
                    -- Actual outcomes (filled later)
                    actual_value REAL,
                    prediction_error REAL,
                    was_accurate BOOLEAN,
                    
                    pump_event_id INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (pump_event_id) REFERENCES pump_events (id)
                )
            ''')
            
            # Market Manipulation Events
            conn.execute('''
                CREATE TABLE IF NOT EXISTS manipulation_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    
                    manipulation_type TEXT NOT NULL, -- 'coordinated_pump', 'wash_trading', 'cross_exchange'
                    confidence_score REAL NOT NULL,
                    
                    -- Involved assets
                    symbols_involved TEXT, -- JSON array
                    exchanges_involved TEXT, -- JSON array
                    estimated_coordinated_volume REAL,
                    
                    -- Pattern characteristics
                    duration_minutes INTEGER,
                    max_price_impact_pct REAL,
                    total_volume_suspicious REAL,
                    
                    -- Detection method
                    detection_algorithm TEXT,
                    alert_sent BOOLEAN DEFAULT FALSE,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Performance Tracking
            conn.execute('''
                CREATE TABLE IF NOT EXISTS bot_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL, -- YYYY-MM-DD
                    
                    total_alerts_sent INTEGER DEFAULT 0,
                    true_positives INTEGER DEFAULT 0,
                    false_positives INTEGER DEFAULT 0,
                    missed_opportunities INTEGER DEFAULT 0,
                    
                    accuracy_rate REAL DEFAULT 0,
                    precision_rate REAL DEFAULT 0,
                    recall_rate REAL DEFAULT 0,
                    f1_score REAL DEFAULT 0,
                    
                    avg_prediction_error REAL DEFAULT 0,
                    best_performing_pattern TEXT,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date)
                )
            ''')
            
            # Create indexes for performance
            indexes = [
                'CREATE INDEX IF NOT EXISTS idx_pump_events_timestamp ON pump_events(timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_pump_events_symbol ON pump_events(symbol)',
                'CREATE INDEX IF NOT EXISTS idx_pump_events_exchange ON pump_events(exchange)',
                'CREATE INDEX IF NOT EXISTS idx_pump_events_outcome ON pump_events(final_outcome)',
                'CREATE INDEX IF NOT EXISTS idx_market_data_symbol_time ON market_data(symbol, timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_market_data_exchange ON market_data(exchange)',
                'CREATE INDEX IF NOT EXISTS idx_patterns_type ON pattern_templates(pattern_type)',
                'CREATE INDEX IF NOT EXISTS idx_ml_predictions_symbol ON ml_predictions(symbol)',
                'CREATE INDEX IF NOT EXISTS idx_ml_predictions_timestamp ON ml_predictions(timestamp)'
            ]
            
            for index_sql in indexes:
                conn.execute(index_sql)
            
            conn.commit()
            print("✅ Database tables and indexes created successfully")
            
        except Exception as e:
            print(f"❌ Database initialization error: {e}")
            raise
    
    def save_pump_event(self, event_data: Dict) -> Optional[int]:
        """Save pump event to database"""
        with self._lock:
            conn = self._get_connection()
            
            try:
                cursor = conn.execute('''
                    INSERT OR REPLACE INTO pump_events (
                        timestamp, exchange, symbol, event_type, price, volume_24h,
                        volume_multiple, rsi, risk_score, pump_stage, confidence,
                        price_change_pct, market_cap_est, volume_profile, price_signature,
                        rsi_journey, pattern_id, similarity_score
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    event_data.get('timestamp'),
                    event_data.get('exchange'),
                    event_data.get('symbol'),
                    event_data.get('event_type'),
                    event_data.get('price'),
                    event_data.get('volume_24h'),
                    event_data.get('volume_multiple'),
                    event_data.get('rsi'),
                    event_data.get('risk_score'),
                    event_data.get('pump_stage'),
                    event_data.get('confidence'),
                    event_data.get('price_change_pct'),
                    event_data.get('market_cap_est'),
                    json.dumps(event_data.get('volume_profile', {})),
                    json.dumps(event_data.get('price_signature', {})),
                    json.dumps(event_data.get('rsi_journey', [])),
                    event_data.get('pattern_id'),
                    event_data.get('similarity_score')
                ))
                
                conn.commit()
                return cursor.lastrowid
                
            except sqlite3.Error as e:
                print(f"❌ Database error saving pump event: {e}")
                return None
            except Exception as e:
                print(f"❌ Unexpected error saving pump event: {e}")
                return None
    
    def update_pump_outcome(self, event_id: int, outcome_data: Dict):
        """Update pump event with actual outcomes"""
        with self._lock:
            conn = self._get_connection()
            
            try:
                conn.execute('''
                    UPDATE pump_events 
                    SET max_gain_pct = ?, max_loss_pct = ?, time_to_peak_minutes = ?,
                        time_to_dump_minutes = ?, final_outcome = ?
                    WHERE id = ?
                ''', (
                    outcome_data.get('max_gain_pct'),
                    outcome_data.get('max_loss_pct'),
                    outcome_data.get('time_to_peak_minutes'),
                    outcome_data.get('time_to_dump_minutes'),
                    outcome_data.get('final_outcome'),
                    event_id
                ))
                
                conn.commit()
                
            except sqlite3.Error as e:
                print(f"❌ Database error updating outcome: {e}")
    
    def save_market_data(self, data: List[Dict]):
        """Batch save OHLCV data"""
        if not data:
            return
        
        with self._lock:
            conn = self._get_connection()
            
            try:
                # Prepare data for batch insert
                records = []
                for d in data:
                    records.append((
                        d.get('timestamp'),
                        d.get('exchange'),
                        d.get('symbol'),
                        d.get('timeframe'),
                        d.get('open'),
                        d.get('high'),
                        d.get('low'),
                        d.get('close'),
                        d.get('volume'),
                        d.get('quote_volume')
                    ))
                
                conn.executemany('''
                    INSERT OR REPLACE INTO market_data (
                        timestamp, exchange, symbol, timeframe, open_price, high_price,
                        low_price, close_price, volume, quote_volume
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', records)
                
                conn.commit()
                print(f"✅ Saved {len(data)} market data records")
                
            except sqlite3.Error as e:
                print(f"❌ Database error saving market data: {e}")
            except Exception as e:
                print(f"❌ Unexpected error saving market data: {e}")
    
    def get_similar_pumps(self, symbol: str, exchange: str, limit: int = 50) -> List[Dict]:
        """Get similar historical pumps for a symbol"""
        with self._lock:
            conn = self._get_connection()
            
            try:
                cursor = conn.execute('''
                    SELECT * FROM pump_events 
                    WHERE symbol = ? AND exchange = ?
                    ORDER BY timestamp DESC 
                    LIMIT ?
                ''', (symbol, exchange, limit))
                
                return [dict(row) for row in cursor.fetchall()]
                
            except sqlite3.Error as e:
                print(f"❌ Database error getting similar pumps: {e}")
                return []
    
    def get_pattern_success_rate(self, pattern_id: str) -> Dict:
        """Get success statistics for a pattern"""
        with self._lock:
            conn = self._get_connection()
            
            try:
                cursor = conn.execute('''
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN final_outcome = 'success' THEN 1 ELSE 0 END) as successes,
                        AVG(max_gain_pct) as avg_gain,
                        AVG(time_to_dump_minutes) as avg_dump_time
                    FROM pump_events 
                    WHERE pattern_id = ? AND final_outcome IS NOT NULL
                ''', (pattern_id,))
                
                row = cursor.fetchone()
                if row and row['total'] > 0:
                    return {
                        'total': row['total'],
                        'success_rate': row['successes'] / row['total'],
                        'avg_gain': row['avg_gain'] or 0,
                        'avg_dump_time': row['avg_dump_time'] or 0
                    }
                    
                return {'total': 0, 'success_rate': 0, 'avg_gain': 0, 'avg_dump_time': 0}
                
            except sqlite3.Error as e:
                print(f"❌ Database error getting pattern stats: {e}")
                return {'total': 0, 'success_rate': 0, 'avg_gain': 0, 'avg_dump_time': 0}
    
    def get_exchange_performance(self, days: int = 30) -> Dict:
        """Get performance stats by exchange"""
        with self._lock:
            conn = self._get_connection()
            
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            
            try:
                cursor = conn.execute('''
                    SELECT 
                        exchange,
                        COUNT(*) as total_events,
                        AVG(CASE WHEN final_outcome = 'success' THEN 1.0 ELSE 0.0 END) as success_rate,
                        AVG(max_gain_pct) as avg_gain,
                        AVG(time_to_dump_minutes) as avg_dump_time
                    FROM pump_events 
                    WHERE timestamp > ? AND final_outcome IS NOT NULL
                    GROUP BY exchange
                    ORDER BY success_rate DESC
                ''', (cutoff_date,))
                
                return {row['exchange']: dict(row) for row in cursor.fetchall()}
                
            except sqlite3.Error as e:
                print(f"❌ Database error getting exchange performance: {e}")
                return {}
    
    def get_market_manipulation_signals(self, hours: int = 24) -> List[Dict]:
        """Get recent market manipulation events"""
        with self._lock:
            conn = self._get_connection()
            
            cutoff_time = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
            
            try:
                cursor = conn.execute('''
                    SELECT * FROM manipulation_events 
                    WHERE timestamp > ?
                    ORDER BY confidence_score DESC
                ''', (cutoff_time,))
                
                events = []
                for row in cursor.fetchall():
                    event = dict(row)
                    try:
                        event['symbols_involved'] = json.loads(event['symbols_involved'])
                        event['exchanges_involved'] = json.loads(event['exchanges_involved'])
                    except:
                        event['symbols_involved'] = []
                        event['exchanges_involved'] = []
                    events.append(event)
                
                return events
                
            except sqlite3.Error as e:
                print(f"❌ Database error getting manipulation signals: {e}")
                return []
    
    def save_ml_prediction(self, prediction_data: Dict) -> Optional[int]:
        """Save ML model prediction"""
        with self._lock:
            conn = self._get_connection()
            
            try:
                cursor = conn.execute('''
                    INSERT INTO ml_predictions (
                        timestamp, exchange, symbol, model_name, model_version,
                        prediction_type, predicted_value, confidence_score,
                        input_features, pump_event_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    prediction_data.get('timestamp'),
                    prediction_data.get('exchange'),
                    prediction_data.get('symbol'),
                    prediction_data.get('model_name'),
                    prediction_data.get('model_version'),
                    prediction_data.get('prediction_type'),
                    prediction_data.get('predicted_value'),
                    prediction_data.get('confidence_score'),
                    json.dumps(prediction_data.get('input_features', {})),
                    prediction_data.get('pump_event_id')
                ))
                
                conn.commit()
                return cursor.lastrowid
                
            except sqlite3.Error as e:
                print(f"❌ Database error saving ML prediction: {e}")
                return None
    
    def get_daily_performance(self, days: int = 30) -> List[Dict]:
        """Get daily bot performance metrics"""
        with self._lock:
            conn = self._get_connection()
            
            try:
                cursor = conn.execute('''
                    SELECT * FROM bot_performance 
                    ORDER BY date DESC 
                    LIMIT ?
                ''', (days,))
                
                return [dict(row) for row in cursor.fetchall()]
                
            except sqlite3.Error as e:
                print(f"❌ Database error getting performance: {e}")
                return []
    
    def cleanup_old_data(self, days_to_keep: int = 90):
        """Clean up old data to manage database size"""
        with self._lock:
            conn = self._get_connection()
            
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days_to_keep)).isoformat()
            
            try:
                # Keep pump events but clean market data
                cursor = conn.execute('DELETE FROM market_data WHERE timestamp < ?', (cutoff_date,))
                deleted_market = cursor.rowcount
                
                # Clean very old ML predictions
                cursor = conn.execute('DELETE FROM ml_predictions WHERE timestamp < ?', (cutoff_date,))
                deleted_predictions = cursor.rowcount
                
                # Clean old manipulation events
                cursor = conn.execute('DELETE FROM manipulation_events WHERE timestamp < ?', (cutoff_date,))
                deleted_manipulation = cursor.rowcount
                
                conn.commit()
                print(f"✅ Cleaned up {deleted_market} market records, {deleted_predictions} ML predictions, {deleted_manipulation} manipulation events")
                
            except sqlite3.Error as e:
                print(f"❌ Database error during cleanup: {e}")
    
    def get_statistics(self) -> Dict:
        """Get overall database statistics"""
        with self._lock:
            conn = self._get_connection()
            
            stats = {}
            
            try:
                # Count records in each table
                tables = ['pump_events', 'market_data', 'pattern_templates', 'ml_predictions', 'manipulation_events']
                
                for table in tables:
                    cursor = conn.execute(f'SELECT COUNT(*) as count FROM {table}')
                    stats[f'{table}_count'] = cursor.fetchone()['count']
                
                # Get date range
                cursor = conn.execute('SELECT MIN(timestamp) as oldest, MAX(timestamp) as newest FROM pump_events')
                row = cursor.fetchone()
                if row:
                    stats['data_range'] = {
                        'oldest': row['oldest'],
                        'newest': row['newest']
                    }
                
                # Get unique symbols and exchanges
                cursor = conn.execute('SELECT COUNT(DISTINCT symbol) as symbols, COUNT(DISTINCT exchange) as exchanges FROM pump_events')
                row = cursor.fetchone()
                if row:
                    stats['unique_symbols'] = row['symbols']
                    stats['unique_exchanges'] = row['exchanges']
                
                # Database file size
                try:
                    file_size = os.path.getsize(self.db_path)
                    stats['database_size_mb'] = round(file_size / 1024 / 1024, 2)
                except:
                    stats['database_size_mb'] = 0
                
                return stats
                
            except sqlite3.Error as e:
                print(f"❌ Database error getting statistics: {e}")
                return {}
    
    def vacuum_database(self):
        """Optimize database performance"""
        with self._lock:
            conn = self._get_connection()
            
            try:
                print("🔧 Optimizing database...")
                conn.execute("VACUUM")
                conn.execute("ANALYZE")
                conn.commit()
                print("✅ Database optimization completed")
                
            except sqlite3.Error as e:
                print(f"❌ Database optimization error: {e}")
    
    def backup_database(self, backup_path: str = None) -> bool:
        """Create database backup"""
        if backup_path is None:
            backup_path = f"{self.db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            with self._lock:
                conn = self._get_connection()
                
                # Create backup connection
                backup_conn = sqlite3.connect(backup_path)
                
                # Perform backup
                conn.backup(backup_conn)
                backup_conn.close()
                
                print(f"✅ Database backup created: {backup_path}")
                return True
                
        except Exception as e:
            print(f"❌ Database backup error: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        with self._lock:
            if self.connection:
                try:
                    self.connection.close()
                    self.connection = None
                    print("✅ Database connection closed")
                except Exception as e:
                    print(f"❌ Error closing database: {e}")
    
    def __del__(self):
        """Destructor to ensure connection is closed"""
        try:
            self.close()
        except:
            pass

# Test function
if __name__ == "__main__":
    # Test database creation and basic operations
    db = PatternDatabase("test_pattern_data.db")
    
    # Test event save
    test_event = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'exchange': 'test',
        'symbol': 'TEST/USDT',
        'event_type': 'PUMP',
        'price': 1.23,
        'volume_multiple': 5.0,
        'risk_score': 7,
        'price_change_pct': 15.5
    }
    
    event_id = db.save_pump_event(test_event)
    print(f"Test event saved with ID: {event_id}")
    
    # Get statistics
    stats = db.get_statistics()
    print("Database stats:", stats)
    
    db.close()
    
    # Clean up test file
    try:
        os.remove("test_pattern_data.db")
        print("Test database cleaned up")
    except:
        pass
