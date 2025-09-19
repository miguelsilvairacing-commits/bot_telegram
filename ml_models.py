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
    
    def prepare_training_data(self, historical_pumps: List[Dict]) -> Tuple[np.array, np.array]:
        """Prepare training data from historical pumps"""
        
        X = []
        y = []
        
        for pump in historical_pumps:
            if not pump.get('final_outcome'):
                continue
            
            features = self.extract_features(pump)
            X.append(features)
            
            # Classify outcome
            outcome = pump['final_outcome']
            if outcome == 'success':
                y.append('REAL_PUMP')
            elif outcome == 'dump':
                y.append('DUMP_TRAP')
            else:
                y.append('FAKE_PUMP')
        
        if not X:
            return np.array([]), np.array([])
        
        X = np.array(X)
        y = np.array(y)
        
        # Store feature names
        self.feature_names = [
            'volume_multiple', 'price_change_pct', 'rsi', 'risk_score',
            'vol_mean', 'vol_std', 'vol_skewness', 'vol_trend', 'vol_spikes',
            'price_volatility', 'price_trend', 'upper_shadows', 'lower_shadows', 
            'body_size', 'green_candles_pct',
            'hour', 'day_of_week', 'is_weekend', 'market_hours', 'high_activity_hours',
            'is_binance', 'is_bingx', 'log_market_cap'
        ]
        
        return X, y
    
    def train(self, historical_pumps: List[Dict]) -> ModelPerformance:
        """Train the pump classifier"""
        
        print(f"🤖 Training pump classifier with {len(historical_pumps)} samples...")
        
        X, y = self.prepare_training_data(historical_pumps)
        
        if len(X) < 10:
            print("❌ Insufficient training data")
            return ModelPerformance(0, 0, 0, 0)
        
        # Encode labels
        y_encoded = self.label_encoder.fit_transform(y)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y_encoded, test_size=0.2, random_state=42, stratify=y_encoded
        )
        
        # Train model
        self.model.fit(X_train, y_train)
        
        # Evaluate
        y_pred = self.model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        precision, recall, f1, _ = precision_recall_fscore_support(y_test, y_pred, average='weighted')
        
        self.is_trained = True
        
        performance = ModelPerformance(accuracy, precision, recall, f1)
        
        print(f"✅ Pump classifier trained - Accuracy: {accuracy:.3f}, F1: {f1:.3f}")
        
        return performance
    
    def predict(self, pump_data: Dict) -> PredictionResult:
        """Predict pump classification"""
        
        if not self.is_trained:
            return PredictionResult("PumpClassifier", "classification", 0.33, 0.5, {}, "Model not trained")
        
        features = self.extract_features(pump_data).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        
        # Get prediction and probabilities
        prediction = self.model.predict(features_scaled)[0]
        probabilities = self.model.predict_proba(features_scaled)[0]
        
        # Get class name
        predicted_class = self.label_encoder.inverse_transform([prediction])[0]
        confidence = max(probabilities)
        
        # Get feature importance
        feature_importance = {}
        if hasattr(self.model, 'feature_importances_'):
            importance_scores = self.model.feature_importances_
            for i, feature in enumerate(self.feature_names):
                if i < len(importance_scores):
                    feature_importance[feature] = float(importance_scores[i])
        
        # Generate explanation
        top_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:3]
        explanation = f"Classified as {predicted_class}. Top factors: {', '.join([f[0] for f in top_features])}"
        
        return PredictionResult(
            model_name="PumpClassifier",
            prediction_type="classification",
            predicted_value=confidence,
            confidence_score=confidence,
            feature_importance=feature_importance,
            explanation=explanation
        )

class DumpPredictor:
    """
    Modelo para prever timing de dumps após pumps
    """
    
    def __init__(self):
        self.model = GradientBoostingRegressor(
            n_estimators=100,
            learning_rate=0.1,
            max_depth=6,
            random_state=42
        )
        self.scaler = StandardScaler()
        self.feature_names = []
        self.is_trained = False
    
    def extract_features(self, pump_data: Dict) -> np.array:
        """Extract features for dump timing prediction"""
        
        features = []
        
        # Current pump characteristics
        features.extend([
            pump_data.get('volume_multiple', 0),
            pump_data.get('price_change_pct', 0),
            pump_data.get('rsi', 50),
            pump_data.get('risk_score', 5),
        ])
        
        # Volume profile analysis
        vol_profile = pump_data.get('volume_profile', {})
        if isinstance(vol_profile, str):
            vol_profile = json.loads(vol_profile)
        
        features.extend([
            vol_profile.get('mean', 0),
            vol_profile.get('std', 0),
            vol_profile.get('trend', 0),
            vol_profile.get('spikes_count', 0),
        ])
        
        # Price action features
        price_sig = pump_data.get('price_signature', {})
        if isinstance(price_sig, str):
            price_sig = json.loads(price_sig)
        
        features.extend([
            price_sig.get('volatility', 0),
            price_sig.get('trend_strength', 0),
            price_sig.get('upper_shadows_avg', 0),
            price_sig.get('body_size_avg', 0),
        ])
        
        # RSI momentum (if available)
        rsi_journey = pump_data.get('rsi_journey', [])
        if isinstance(rsi_journey, str):
            rsi_journey = json.loads(rsi_journey)
        
        if len(rsi_journey) >= 2:
            rsi_momentum = rsi_journey[-1] - rsi_journey[-2]
            rsi_acceleration = (rsi_journey[-1] - rsi_journey[-2]) - (rsi_journey[-2] - rsi_journey[-3]) if len(rsi_journey) >= 3 else 0
        else:
            rsi_momentum = 0
            rsi_acceleration = 0
        
        features.extend([rsi_momentum, rsi_acceleration])
        
        # Market timing
        timestamp = pump_data.get('timestamp', '')
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            hour = dt.hour
            is_weekend = 1 if dt.weekday() >= 5 else 0
        except:
            hour, is_weekend = 12, 0
        
        features.extend([
            hour,
            is_weekend,
            1 if 20 <= hour <= 23 or 0 <= hour <= 2 else 0,  # Late hours (higher dump risk)
        ])
        
        return np.array(features)
    
    def prepare_training_data(self, historical_pumps: List[Dict]) -> Tuple[np.array, np.array]:
        """Prepare training data for dump timing prediction"""
        
        X = []
        y = []
        
        for pump in historical_pumps:
            if not pump.get('time_to_dump_minutes') or pump['time_to_dump_minutes'] <= 0:
                continue
            
            features = self.extract_features(pump)
            X.append(features)
            
            # Target: time to dump in hours (easier to predict)
            dump_time_hours = min(pump['time_to_dump_minutes'] / 60.0, 24.0)  # Cap at 24 hours
            y.append(dump_time_hours)
        
        if not X:
            return np.array([]), np.array([])
        
        X = np.array(X)
        y = np.array(y)
        
        # Store feature names
        self.feature_names = [
            'volume_multiple', 'price_change_pct', 'rsi', 'risk_score',
            'vol_mean', 'vol_std', 'vol_trend', 'vol_spikes',
            'price_volatility', 'price_trend', 'upper_shadows', 'body_size',
            'rsi_momentum', 'rsi_acceleration',
            'hour', 'is_weekend', 'late_hours'
        ]
        
        return X, y
    
    def train(self, historical_pumps: List[Dict]) -> ModelPerformance:
        """Train the dump predictor"""
        
        print(f"🤖 Training dump predictor with {len(historical_pumps)} samples...")
        
        X, y = self.prepare_training_data(historical_pumps)
        
        if len(X) < 10:
            print("❌ Insufficient training data for dump predictor")
            return ModelPerformance(0, 0, 0, 0, float('inf'))
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y, test_size=0.2, random_state=42
        )
        
        # Train model
        self.model.fit(X_train, y_train)
        
        # Evaluate
        y_pred = self.model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        
        # Calculate R² score as accuracy proxy
        score = self.model.score(X_test, y_test)
        accuracy = max(0, score)  # R² can be negative
        
        self.is_trained = True
        
        performance = ModelPerformance(accuracy, 0, 0, 0, mae)
        
        print(f"✅ Dump predictor trained - R²: {accuracy:.3f}, MAE: {mae:.2f} hours")
        
        return performance
    
    def predict(self, pump_data: Dict) -> PredictionResult:
        """Predict time to dump"""
        
        if not self.is_trained:
            return PredictionResult("DumpPredictor", "regression", 6.0, 0.5, {}, "Model not trained")
        
        features = self.extract_features(pump_data).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        
        # Predict time to dump (hours)
        predicted_hours = self.model.predict(features_scaled)[0]
        predicted_hours = max(0.1, min(predicted_hours, 24.0))  # Reasonable bounds
        
        # Estimate confidence based on feature values
        confidence = self._estimate_confidence(pump_data)
        
        # Get feature importance
        feature_importance = {}
        if hasattr(self.model, 'feature_importances_'):
            importance_scores = self.model.feature_importances_
            for i, feature in enumerate(self.feature_names):
                if i < len(importance_scores):
                    feature_importance[feature] = float(importance_scores[i])
        
        # Generate explanation
        explanation = f"Predicted dump in {predicted_hours:.1f} hours based on volume and RSI patterns"
        
        return PredictionResult(
            model_name="DumpPredictor",
            prediction_type="regression",
            predicted_value=predicted_hours,
            confidence_score=confidence,
            feature_importance=feature_importance,
            explanation=explanation
        )
    
    def _estimate_confidence(self, pump_data: Dict) -> float:
        """Estimate confidence based on input data quality"""
        
        confidence = 0.5  # Base confidence
        
        # Higher confidence for extreme values
        rsi = pump_data.get('rsi', 50)
        if rsi > 80:  # Very overbought
            confidence += 0.3
        elif rsi > 70:
            confidence += 0.1
        
        # Volume multiple confidence
        vol_mult = pump_data.get('volume_multiple', 0)
        if vol_mult > 15:
            confidence += 0.2
        elif vol_mult > 8:
            confidence += 0.1
        
        return min(confidence, 0.95)

class RiskCalculator:
    """
    Advanced risk scoring using ML
    """
    
    def __init__(self):
        self.model = RandomForestClassifier(
            n_estimators=50,
            max_depth=8,
            random_state=42
        )
        self.scaler = StandardScaler()
        self.is_trained = False
    
    def calculate_ml_risk_score(self, pump_data: Dict, pattern_matches: List = None) -> Dict:
        """Calculate advanced risk score using multiple factors"""
        
        risk_factors = {
            'volume_risk': self._assess_volume_risk(pump_data),
            'price_risk': self._assess_price_risk(pump_data),
            'rsi_risk': self._assess_rsi_risk(pump_data),
            'timing_risk': self._assess_timing_risk(pump_data),
            'pattern_risk': self._assess_pattern_risk(pattern_matches) if pattern_matches else 0.5,
            'market_risk': self._assess_market_risk(pump_data)
        }
        
        # Weighted risk calculation
        weights = {
            'volume_risk': 0.25,
            'price_risk': 0.20,
            'rsi_risk': 0.20,
            'timing_risk': 0.15,
            'pattern_risk': 0.15,
            'market_risk': 0.05
        }
        
        overall_risk = sum(risk_factors[factor] * weights[factor] for factor in weights)
        overall_risk = np.clip(overall_risk, 0, 1)
        
        # Convert to 1-10 scale
        risk_score = int(overall_risk * 9) + 1
        
        return {
            'risk_score': risk_score,
            'risk_factors': risk_factors,
            'risk_level': self._categorize_risk(risk_score),
            'explanation': self._generate_risk_explanation(risk_factors, risk_score)
        }
    
    def _assess_volume_risk(self, pump_data: Dict) -> float:
        """Assess risk based on volume patterns"""
        vol_mult = pump_data.get('volume_multiple', 0)
        
        # Very high volume can indicate manipulation
        if vol_mult > 50:
            return 0.9
        elif vol_mult > 20:
            return 0.7
        elif vol_mult > 10:
            return 0.4
        elif vol_mult > 5:
            return 0.2
        else:
            return 0.6  # Low volume also risky
    
    def _assess_price_risk(self, pump_data: Dict) -> float:
        """Assess risk based on price movement"""
        price_change = abs(pump_data.get('price_change_pct', 0))
        
        # Extreme price movements are riskier
        if price_change > 50:
            return 0.95
        elif price_change > 30:
            return 0.8
        elif price_change > 20:
            return 0.6
        elif price_change > 10:
            return 0.3
        else:
            return 0.7  # Too small movements also suspicious
    
    def _assess_rsi_risk(self, pump_data: Dict) -> float:
        """Assess risk based on RSI levels"""
        rsi = pump_data.get('rsi', 50)
        
        if rsi > 85:
            return 0.9  # Extreme overbought
        elif rsi > 75:
            return 0.7
        elif rsi > 65:
            return 0.4
        elif rsi < 25:
            return 0.8  # Extreme oversold during pump = weird
        else:
            return 0.2  # Normal RSI range
    
    def _assess_timing_risk(self, pump_data: Dict) -> float:
        """Assess risk based on timing"""
        timestamp = pump_data.get('timestamp', '')
        
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            hour = dt.hour
            is_weekend = dt.weekday() >= 5
            
            # Late night/early morning pumps are riskier
            if hour in [0, 1, 2, 3]:
                risk = 0.8
            elif hour in [22, 23]:
                risk = 0.6
            elif is_weekend:
                risk = 0.5
            else:
                risk = 0.2
                
            return risk
            
        except:
            return 0.5
    
    def _assess_pattern_risk(self, pattern_matches: List) -> float:
        """Assess risk based on historical patterns"""
        if not pattern_matches:
            return 0.5
        
        # Get average success rate from top matches
        success_rates = [m.historical_success_rate for m in pattern_matches[:5] if hasattr(m, 'historical_success_rate')]
        
        if success_rates:
            avg_success = np.mean(success_rates)
            return 1.0 - avg_success  # Lower success rate = higher risk
        
        return 0.5
    
    def _assess_market_risk(self, pump_data: Dict) -> float:
        """Assess broader market risk"""
        # Placeholder - would incorporate market-wide indicators
        return 0.3
    
    def _categorize_risk(self, risk_score: int) -> str:
        """Categorize risk score into levels"""
        if risk_score >= 9:
            return "EXTREME"
        elif risk_score >= 7:
            return "HIGH"
        elif risk_score >= 5:
            return "MEDIUM"
        elif risk_score >= 3:
            return "LOW"
        else:
            return "MINIMAL"
    
    def _generate_risk_explanation(self, risk_factors: Dict, risk_score: int) -> str:
        """Generate human-readable risk explanation"""
        
        # Find highest risk factors
        sorted_factors = sorted(risk_factors.items(), key=lambda x: x[1], reverse=True)
        top_risks = [factor for factor, value in sorted_factors[:2] if value > 0.5]
        
        explanations = {
            'volume_risk': 'Unusual volume patterns',
            'price_risk': 'Extreme price movement',
            'rsi_risk': 'Overbought conditions',
            'timing_risk': 'Suspicious timing',
            'pattern_risk': 'Poor historical patterns',
            'market_risk': 'Adverse market conditions'
        }
        
        if top_risks:
            factors_text = ', '.join([explanations.get(risk, risk) for risk in top_risks])
            return f"Risk score {risk_score}/10. Main concerns: {factors_text}"
        else:
            return f"Risk score {risk_score}/10. Overall assessment based on multiple factors"

class MLModelManager:
    """
    Manager for all ML models
    """
    
    def __init__(self, db):
        self.db = db
        self.pump_classifier = PumpClassifier()
        self.dump_predictor = DumpPredictor()
        self.risk_calculator = RiskCalculator()
        self.models_trained = False
    
    def train_all_models(self) -> Dict[str, ModelPerformance]:
        """Train all ML models with historical data"""
        
        print("🤖 Starting ML model training...")
        
        # Get historical pump data from database
        # This is a placeholder - would need to implement database query
        historical_pumps = self._get_historical_training_data()
        
        if len(historical_pumps) < 20:
            print("⚠️ Insufficient historical data for ML training")
            return {}
        
        performances = {}
        
        # Train pump classifier
        try:
            perf = self.pump_classifier.train(historical_pumps)
            performances['pump_classifier'] = perf
        except Exception as e:
            print(f"❌ Error training pump classifier: {e}")
        
        # Train dump predictor
        try:
            perf = self.dump_predictor.train(historical_pumps)
            performances['dump_predictor'] = perf
        except Exception as e:
            print(f"❌ Error training dump predictor: {e}")
        
        self.models_trained = True
        print("✅ ML model training complete")
        
        return performances
    
    def _get_historical_training_data(self) -> List[Dict]:
        """Get historical data for training (placeholder)"""
        # This would query the database for historical pump events
        # For now, return empty list
        return []
    
    def get_comprehensive_prediction(self, pump_data: Dict, pattern_matches: List = None) -> Dict:
        """Get predictions from all models"""
        
        predictions = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'symbol': pump_data.get('symbol', ''),
            'exchange': pump_data.get('exchange', ''),
        }
        
        # Pump classification
        if self.pump_classifier.is_trained:
            classification = self.pump_classifier.predict(pump_data)
            predictions['classification'] = {
                'predicted_type': classification.explanation.split()[2],  # Extract class name
                'confidence': classification.confidence_score,
                'explanation': classification.explanation
            }
        
        # Dump timing prediction
        if self.dump_predictor.is_trained:
            dump_timing = self.dump_predictor.predict(pump_data)
            predictions['dump_timing'] = {
                'predicted_hours': dump_timing.predicted_value,
                'confidence': dump_timing.confidence_score,
                'explanation': dump_timing.explanation
            }
        
        # Advanced risk assessment
        risk_analysis = self.risk_calculator.calculate_ml_risk_score(pump_data, pattern_matches)
        predictions['risk_analysis'] = risk_analysis
        
        # Overall recommendation
        predictions['recommendation'] = self._generate_overall_recommendation(predictions)
        
        return predictions
    
    def _generate_overall_recommendation(self, predictions: Dict) -> Dict:
        """Generate overall recommendation based on all ML predictions"""
        
        # Extract key metrics
        risk_score = predictions.get('risk_analysis', {}).get('risk_score', 5)
        dump_hours = predictions.get('dump_timing', {}).get('predicted_hours', 6)
        classification = predictions.get('classification', {})
        
        # Generate action recommendation
        if risk_score >= 8:
            action = "AVOID"
            reason = "Very high risk detected"
        elif risk_score >= 6 and dump_hours < 2:
            action = "AVOID"
            reason = "High risk with quick dump predicted"
        elif risk_score <= 3 and dump_hours > 4:
            action = "BUY"
            reason = "Low risk with sustained pump expected"
        elif risk_score <= 5 and dump_hours > 2:
            action = "CAUTIOUS_BUY"
            reason = "Moderate setup with reasonable timing"
        else:
            action = "MONITOR"
            reason = "Mixed signals - wait for better clarity"
        
        return {
            'action': action,
            'reason': reason,
            'confidence': self._calculate_recommendation_confidence(predictions),
            'position_size': self._suggest_position_size(risk_score),
            'time_horizon': f"{dump_hours:.1f} hours" if dump_hours else "Unknown"
        }
    
    def _calculate_recommendation_confidence(self, predictions: Dict) -> float:
        """Calculate confidence in overall recommendation"""
        
        confidences = []
        
        if 'classification' in predictions:
            confidences.append(predictions['classification'].get('confidence', 0.5))
        
        if 'dump_timing' in predictions:
            confidences.append(predictions['dump_timing'].get('confidence', 0.5))
        
        if confidences:
            return np.mean(confidences)
        else:
            return 0.5
    
    def _suggest_position_size(self, risk_score: int) -> str:
        """Suggest position size based on risk score"""
        
        if risk_score >= 8:
            return "0% (avoid)"
        elif risk_score >= 6:
            return "25% (small)"
        elif risk_score >= 4:
            return "50% (moderate)"
        elif risk_score >= 2:
            return "75% (large)"
        else:
            return "100% (full position)"
    
    def save_prediction_result(self, prediction_data: Dict):
        """Save ML prediction to database for tracking accuracy"""
        
        # Save to ml_predictions table
        try:
            prediction_record = {
                'timestamp': prediction_data['timestamp'],
                'exchange': prediction_data['exchange'],
                'symbol': prediction_data['symbol'],
                'model_name': 'MLModelManager',
                'model_version': '1.0',
                'prediction_type': 'comprehensive',
                'predicted_value': prediction_data.get('risk_analysis', {}).get('risk_score', 5),
                'confidence_score': prediction_data.get('recommendation', {}).get('confidence', 0.5),
                'input_features': prediction_data
            }
            
            prediction_id = self.db.save_ml_prediction(prediction_record)
            
            if prediction_id:
                print(f"✅ Saved ML prediction {prediction_id}")
            
        except Exception as e:
            print(f"❌ Error saving ML prediction: {e}")

# Example usage and testing
if __name__ == "__main__":
    # Test ML models with sample data
    sample_pump = {
        'symbol': 'TEST/USDT',
        'exchange': 'binance',
        'volume_multiple': 8.5,
        'price_change_pct': 15.2,
        'rsi': 72.5,
        'risk_score': 7,
        'volume_profile': {'mean': 1000, 'std': 500, 'trend': 0.8, 'spikes_count': 2},
        'price_signature': {'volatility': 0.15, 'trend_strength': 0.7, 'upper_shadows_avg': 0.2, 'body_size_avg': 0.6},
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'market_cap_est': 5000000
    }
    
    # Test risk calculator
    risk_calc = RiskCalculator()
    risk_result = risk_calc.calculate_ml_risk_score(sample_pump)
    print("Risk Analysis:", risk_result)
