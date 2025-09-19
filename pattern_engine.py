# pattern_engine.py - Advanced Pattern Recognition Engine
import numpy as np
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import hashlib
from database import PatternDatabase

@dataclass
class PatternMatch:
    """Pattern matching result"""
    pattern_id: str
    similarity_score: float
    historical_success_rate: float
    avg_duration_minutes: float
    confidence: str
    risk_level: str
    recommendation: str

@dataclass 
class MarketRegime:
    """Market regime classification"""
    regime_type: str  # 'bull', 'bear', 'crab'
    volatility_level: str  # 'low', 'medium', 'high'
    manipulation_probability: float
    coordinated_activity: bool

class PatternEngine:
    """
    Advanced pattern recognition system para pump/dump detection
    """
    
    def __init__(self, db: PatternDatabase):
        self.db = db
        self.scaler = StandardScaler()
        self.pattern_clusters = {}
        self._load_existing_patterns()
    
    def _load_existing_patterns(self):
        """Load existing patterns from database"""
        # Implementation will load and cache patterns
        print("📊 Loading existing patterns from database...")
        # This would query pattern_templates table
    
    def create_pattern_fingerprint(self, pump_data: Dict) -> str:
        """Create unique fingerprint for a pump pattern"""
        
        # Extract key features for fingerprinting
        features = {
            'volume_profile': pump_data.get('volume_profile', {}),
            'price_signature': pump_data.get('price_signature', {}),
            'rsi_range': self._discretize_rsi(pump_data.get('rsi', 50)),
            'pump_stage': pump_data.get('pump_stage', ''),
            'timeframe_context': self._get_timeframe_context(pump_data.get('timestamp', '')),
            'exchange': pump_data.get('exchange', ''),
        }
        
        # Create hash-based fingerprint
        features_str = json.dumps(features, sort_keys=True)
        fingerprint = hashlib.md5(features_str.encode()).hexdigest()[:16]
        
        return f"PATTERN_{fingerprint}"
    
    def _discretize_rsi(self, rsi: float) -> str:
        """Convert RSI to discrete ranges for pattern matching"""
        if rsi < 30:
            return "OVERSOLD"
        elif rsi < 50:
            return "BEARISH"
        elif rsi < 70:
            return "NEUTRAL"
        elif rsi < 85:
            return "BULLISH"
        else:
            return "OVERBOUGHT"
    
    def _get_timeframe_context(self, timestamp: str) -> Dict:
        """Extract temporal context for pattern matching"""
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return {
                'hour': dt.hour,
                'day_of_week': dt.weekday(),
                'is_weekend': dt.weekday() >= 5,
                'market_session': self._classify_market_session(dt.hour)
            }
        except:
            return {'hour': 12, 'day_of_week': 2, 'is_weekend': False, 'market_session': 'UNKNOWN'}
    
    def _classify_market_session(self, hour: int) -> str:
        """Classify market session based on hour"""
        if 0 <= hour < 6:
            return "ASIA_LATE"
        elif 6 <= hour < 12:
            return "EUROPE_EARLY"
        elif 12 <= hour < 18:
            return "EUROPE_LATE_US_EARLY"
        else:
            return "US_LATE"
    
    def calculate_pattern_similarity(self, current_pump: Dict, historical_pump: Dict) -> float:
        """Calculate similarity between two pump patterns"""
        
        # Volume profile similarity
        vol_sim = self._compare_volume_profiles(
            current_pump.get('volume_profile', {}),
            historical_pump.get('volume_profile', {})
        )
        
        # Price signature similarity
        price_sim = self._compare_price_signatures(
            current_pump.get('price_signature', {}),
            historical_pump.get('price_signature', {})
        )
        
        # RSI journey similarity
        rsi_sim = self._compare_rsi_journeys(
            current_pump.get('rsi_journey', []),
            historical_pump.get('rsi_journey', [])
        )
        
        # Timing context similarity
        time_sim = self._compare_timing_context(
            current_pump.get('timestamp', ''),
            historical_pump.get('timestamp', '')
        )
        
        # Exchange context
        exchange_sim = 1.0 if current_pump.get('exchange') == historical_pump.get('exchange') else 0.7
        
        # Weighted similarity score
        similarity = (
            vol_sim * 0.3 +
            price_sim * 0.25 +
            rsi_sim * 0.2 +
            time_sim * 0.15 +
            exchange_sim * 0.1
        )
        
        return similarity
    
    def _compare_volume_profiles(self, profile1: Dict, profile2: Dict) -> float:
        """Compare volume profile fingerprints"""
        if not profile1 or not profile2:
            return 0.0
        
        features = ['mean', 'std', 'skewness', 'trend', 'spikes_count']
        similarities = []
        
        for feature in features:
            val1 = profile1.get(feature, 0)
            val2 = profile2.get(feature
