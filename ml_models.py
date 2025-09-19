# ml_models.py - Machine Learning Prediction Models
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
import json
from datetime import datetime, timezone
from dataclasses import dataclass
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, mean_absolute_error
import joblib
import warnings
warnings.filterwarnings('ignore')

@dataclass
class PredictionResult:
    """ML Prediction result"""
    model_name: str
    prediction_type: str
    predicted_value: float
    confidence_score: float
    feature_importance: Dict[str, float]
    explanation: str

@dataclass
class ModelPerformance:
    """Model performance metrics"""
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    mae: Optional[float] = None

class PumpClassifier:
    """
    Classificador para distinguir pumps reais vs fake pumps vs dump traps
    """
    
    def __init__(self):
        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            class_weight='balanced'
        )
        self.scaler = StandardScaler()
        self.label_encoder = LabelEncoder()
        self.feature_names = []
        self.is_trained = False
    
    def extract_features(self, pump_data: Dict) -> np.array:
        """Extract features for pump classification"""
        
        features = []
        
        # Basic metrics
        features.extend([
            pump_data.get('volume_multiple', 0),
            pump_data.get('price_change_pct', 0),
            pump_data.get('rsi', 50),
            pump_data.get('risk_score', 5),
        ])
        
        # Volume profile features
        vol_profile = pump_data.get('volume_profile', {})
        if isinstance(vol_profile, str):
            vol_profile = json.loads(vol_profile)
        
        features.extend([
            vol_profile.get('mean', 0),
            vol_profile.get('std', 0),
            vol_profile.get('skewness', 0),
            vol_profile.get('trend', 0),
            vol_profile.get('spikes_count', 0),
        ])
        
        # Price signature features  
        price_sig = pump_data.get('price_signature', {})
        if isinstance(price_sig, str):
            price_sig = json.loads(price_sig)
        
        features.extend([
            price_sig.get('volatility', 0),
            price_sig.get('trend_strength', 0),
            price_sig.get('upper_shadows_avg', 0),
            price_sig.get('lower_shadows_avg', 0),
            price_sig.get('body_size_avg', 0),
            price_sig.get('green_candles_pct', 0),
        ])
        
        # Timing features
        timestamp = pump_data.get('timestamp', '')
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            hour = dt.hour
            day_of_week = dt.weekday()
            is_weekend = 1 if day_of_week >= 5 else 0
        except:
            hour, day_of_week, is_weekend = 12, 2, 0
        
        features.extend([
            hour,
            day_of_week, 
            is_weekend,
            1 if 8 <= hour <= 16 else 0,  # Market hours
            1 if hour in [0,1,2,14,15,16,22,23] else 0,  # High activity hours
        ])
        
        # Exchange encoding (simple binary for now)
        exchange = pump_data.get('exchange', '').lower()
        features.extend([
            1 if exchange == 'binance' else 0,
            1 if exchange == 'bingx' else 0,
        ])
        
        # Market cap estimate 
        market_cap = pump_data.get('market_cap_est', 1000000)
        features.append(np.log10(max(market_cap, 1)))  # Log scale
        
        return np.array(features)
    
    def prepare_training_data(
