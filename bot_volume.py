# enhanced_bot_main.py - Complete AI-Enhanced Trading Bot
import os
import time
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import json
import numpy as np
from collections import defaultdict

# Import configuration
try:
    from config import Config
except ImportError:
    print("⚠️ config.py not found, using environment variables directly")
    Config = None

# Import our custom modules
from database import PatternDatabase
from historical_collector import HistoricalCollector
from pattern_engine import PatternEngine
from ml_models import MLModelManager

# Original bot imports
import requests
import ccxt
import math

class EnhancedTradingBot:
    """
    Complete AI-enhanced trading bot with historical analysis and ML predictions
    """
    
    def __init__(self):
        # Configuration - use Config class if available, otherwise env vars
        if Config:
            self.exchanges_list = Config.EXCHANGES
            self.quote_filter = Config.QUOTE_FILTER
            self.top_n_by_volume = Config.TOP_N_BY_VOLUME
            self.timeframe = Config.TIMEFRAME
            self.lookback = Config.LOOKBACK
            self.threshold = Config.THRESHOLD
            self.min_risk_score = Config.MIN_RISK_SCORE
            self.min_price_change = Config.MIN_PRICE_CHANGE
            self.sleep_seconds = Config.SLEEP_SECONDS
            self.cooldown_minutes = Config.COOLDOWN_MINUTES
            self.debug_mode = Config.DEBUG_MODE
            self.ai_mode = Config.AI_MODE
            self.pattern_analysis = Config.PATTERN_ANALYSIS
            self.ml_predictions = Config.ML_PREDICTIONS
            self.market_manipulation_detection = Config.MANIPULATION_DETECTION
            self.tg_token = Config.TG_TOKEN
            self.tg_chat_id = Config.TG_CHAT_ID
            self.historical_days = Config.HISTORICAL_DAYS
            self.auto_collect_historical = Config.AUTO_COLLECT_HISTORICAL
        else:
            # Fallback to environment variables
            self.exchanges_list = os.getenv("EXCHANGES", "binance,bingx").split(",")
            self.quote_filter = os.getenv("QUOTE_FILTER", "USDT").split(",")
            self.top_n_by_volume = int(os.getenv("TOP_N_BY_VOLUME", "30"))
            self.timeframe = os.getenv("TIMEFRAME", "1m")
            self.lookback = int(os.getenv("LOOKBACK", "8"))
            self.threshold = float(os.getenv("THRESHOLD", "3.0"))
            self.min_risk_score = int(os.getenv("MIN_RISK_SCORE", "4"))
            self.min_price_change = float(os.getenv("MIN_PRICE_CHANGE", "0.04"))
            self.sleep_seconds = int(os.getenv("SLEEP_SECONDS", "30"))
            self.cooldown_minutes = int(os.getenv("COOLDOWN_MINUTES", "25"))
            self.debug_mode = os.getenv("DEBUG_MODE", "false").lower() == "true"
            self.ai_mode = os.getenv("AI_MODE", "true").lower() == "true"
            self.pattern_analysis = os.getenv("PATTERN_ANALYSIS", "true").lower() == "true"
            self.ml_predictions = os.getenv("ML_PREDICTIONS", "true").lower() == "true"
            self.market_manipulation_detection = os.getenv("MANIPULATION_DETECTION", "true").lower() == "true"
            self.tg_token = os.getenv("TG_TOKEN", "")
            self.tg_chat_id = os.getenv("TG_CHAT_ID", "")
            self.historical_days = int(os.getenv("HISTORICAL_DAYS", "30"))
            self.auto_collect_historical = os.getenv("AUTO_COLLECT_HISTORICAL", "true").lower() == "true"
        
        # Exchange configurations
        self.exchanges_config = {
            'binance': {
                'enableRateLimit': True,
                'timeout': 20000,
                'options': {'adjustForTimeDifference': True}
            },
            'bingx': {
                'enableRateLimit': True,
                'timeout': 30000,
                'rateLimit': 2000,
                'options': {
                    'adjustForTimeDifference': True,
                    'recvWindow': 60000,
                }
            }
        }
        
        self.exchanges = {}
        self.watchlist = {}
        
        # Core components
        self.db = PatternDatabase()
        self.pattern_engine = PatternEngine(self.db)
        self.ml_manager = MLModelManager(self.db)
        
        # Alert cooldowns
        self.last_alert_ts = defaultdict(lambda: 0.0)
        
        # Market events buffer for manipulation detection
        self.recent_events = []
        self.max_events_buffer = 100
        
        # Performance tracking
        self.stats = {
            'alerts_sent': 0,
            'pumps_detected': 0,
            'patterns_matched': 0,
            'ml_predictions_made': 0,
            'manipulation_detected': 0,
            'start_time': time.time()
        }
        
        print("🚀 Enhanced Trading Bot initialized with AI capabilities")
        if self.debug_mode:
            print(f"🔧 Debug mode enabled")
            print(f"🤖 AI mode: {self.ai_mode}")
            print(f"📊 Pattern analysis: {self.pattern_analysis}")
            print(f"🧠 ML predictions: {self.ml_predictions}")
    
    def send_telegram(self, msg: str):
        """Send message to Telegram"""
        if not self.tg_token or not self.tg_chat_id:
            if self.debug_mode:
                print("[Telegram] Not configured. Message would be:")
                print(msg[:500] + "..." if len(msg) > 500 else msg)
            return
        
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{self.tg_token}/sendMessage",
                json={
                    "chat_id": self.tg_chat_id, 
                    "text": msg, 
                    "parse_mode": "HTML", 
                    "disable_web_page_preview": True
                },
                timeout=20
            )
            
            if response.status_code == 200:
                if self.debug_mode:
                    print("[Telegram] ✅ Message sent successfully")
            else:
                print(f"[Telegram] ❌ Error {response.status_code}: {response.text}")
                
        except Exception as e:
            print(f"[Telegram] ❌ Exception: {e}")
    
    def build_exchange(self, name: str):
        """Build exchange with specific configurations"""
        name = name.strip().lower()
        if not hasattr(ccxt, name):
            raise ValueError(f"Exchange '{name}' not found in ccxt")
        
        config = self.exchanges_config.get(name, {
            'enableRateLimit': True,
            'options': {'adjustForTimeDifference': True}
        })
        
        ex = getattr(ccxt, name)(config)
        
        try:
            ex.load_markets()
            print(f"[{name.upper()}] ✅ {len(ex.markets)} markets loaded")
            return ex
        except Exception as e:
            print(f"[{name.upper()}] ❌ Failed to load markets: {e}")
            raise
    
    def calculate_rsi(self, prices: List[float], period: int = 14) -> Optional[float]:
        """Calculate RSI for price series"""
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
            return float(rsi)
            
        except Exception:
            return None
    
    def fetch_ohlcv_safe(self, ex, symbol: str, timeframe: str, limit: int):
        """Safe OHLCV fetch with retry logic"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                if ex.id.lower() == 'bingx':
                    actual_limit = min(limit, 100)
                    return ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=actual_limit)
                else:
                    return ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                    
            except ccxt.NetworkError as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 3
                    if self.debug_mode:
                        print(f"[{ex.id}] Network error for {symbol}, retry {attempt + 1} in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    if self.debug_mode:
                        print(f"[{ex.id}] Max retries exceeded for {symbol}")
                    return None
                    
            except ccxt.ExchangeError as e:
                error_str = str(e).lower()
                if "rate limit" in error_str or "too many requests" in error_str:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 5
                        if self.debug_mode:
                            print(f"[{ex.id}] Rate limit for {symbol}, wait {wait_time}s")
                        time.sleep(wait_time)
                        continue
                    else:
                        return None
                elif any(x in error_str for x in ["invalid symbol", "not found"]):
                    if self.debug_mode:
                        print(f"[{ex.id}] Symbol {symbol} not found")
                    return None
                else:
                    if self.debug_mode:
                        print(f"[{ex.id}] Exchange error for {symbol}: {e}")
                    return None
                    
            except Exception as e:
                if self.debug_mode:
                    print(f"[{ex.id}] Unexpected error for {symbol}: {e}")
                return None
        
        return None
    
    def get_symbols_for_exchange(self, exchange_name: str, limit: int = 30) -> List[str]:
        """Get top volume symbols for exchange"""
        ex = self.exchanges.get(exchange_name)
        if not ex:
            return []
        
        try:
            print(f"[{exchange_name.upper()}] Fetching tickers...")
            tickers = ex.fetch_tickers()
            print(f"[{exchange_name.upper()}] ✅ {len(tickers)} tickers received")
            
            volume_pairs = []
            processed = 0
            
            for symbol, ticker in tickers.items():
                processed += 1
                
                # Quote currency filter
                if not any(symbol.endswith("/" + q) for q in self.quote_filter):
                    continue
                
                # Get volume
                if exchange_name.lower() == 'bingx':
                    volume = ticker.get("quoteVolume") or ticker.get("info", {}).get("volume")
                else:
                    volume = ticker.get("quoteVolume")
                
                try:
                    volume = float(volume) if volume else None
                except:
                    continue
                
                # Volume filtering based on config
                min_vol = getattr(Config, 'QV24H_MIN_USD', 200000) if Config else float(os.getenv("QV24H_MIN_USD", "200000"))
                max_vol = getattr(Config, 'QV24H_MAX_USD', 50000000) if Config else float(os.getenv("QV24H_MAX_USD", "50000000"))
                
                if volume and min_vol <= volume <= max_vol:
                    volume_pairs.append((symbol, volume))
            
            # Sort by volume and take top N
            volume_pairs.sort(key=lambda x: x[1], reverse=True)
            selected = [symbol for symbol, _ in volume_pairs[:limit]]
            
            print(f"[{exchange_name.upper()}] ✅ Selected {len(selected)} symbols from {processed} processed")
            
            if self.debug_mode and selected:
                print(f"[{exchange_name.upper()}] Top 5: {selected[:5]}")
                volumes = [f"${vol:,.0f}" for _, vol in volume_pairs[:3]]
                print(f"[{exchange_name.upper()}] Top volumes: {volumes}")
            
            return selected
            
        except Exception as e:
            print(f"[{exchange_name.upper()}] ❌ Error getting symbols: {e}")
            return []
    
    def can_alert(self, key: str, now_ts: float) -> bool:
        """Check if we can send alert (cooldown logic)"""
        last = self.last_alert_ts[key]
        if (now_ts - last) >= self.cooldown_minutes * 60:
            self.last_alert_ts[key] = now_ts
            return True
        return False
    
    def create_pump_fingerprint(self, ohlcv_data: List, volumes: List[float], 
                               rsi_values: List[float], index: int) -> Dict:
        """Create comprehensive pump fingerprint"""
        
        lookback = min(self.lookback, index)
        if lookback < 5:
            return {}
        
        try:
            # Volume profile
            vol_data = volumes[max(0, index-lookback):index+1]
            if vol_data:
                volume_profile = {
                    'mean': float(np.mean(vol_data)),
                    'std': float(np.std(vol_data)),
                    'trend': float(np.corrcoef(range(len(vol_data)), vol_data)[0,1]) if len(vol_data) > 1 else 0,
                    'spikes_count': int(sum(1 for v in vol_data if v > np.mean(vol_data) + 2*np.std(vol_data)))
                }
            else:
                volume_profile = {}
            
            # Price signature
            price_data = ohlcv_data[max(0, index-10):index+1]
            if price_data and len(price_data) > 1:
                closes = [c[4] for c in price_data]
                highs = [c[2] for c in price_data]
                lows = [c[3] for c in price_data]
                opens = [c[1] for c in price_data]
                
                mean_close = np.mean(closes)
                price_signature = {
                    'volatility': float(np.std(closes) / mean_close) if mean_close > 0 else 0,
                    'trend_strength': float(np.corrcoef(range(len(closes)), closes)[0,1]) if len(closes) > 1 else 0,
                    'upper_shadows_avg': float(np.mean([
                        (h-max(o,c))/(h-l) for o,h,l,c in zip(opens,highs,lows,closes) 
                        if h-l > 0
                    ])) if any(h-l > 0 for h,l in zip(highs,lows)) else 0,
                    'green_candles_pct': float(sum(1 for o,c in zip(opens,closes) if c > o) / len(closes)) if closes else 0
                }
            else:
                price_signature = {}
            
            # RSI journey
            rsi_journey = rsi_values[max(0, index-10):index+1] if rsi_values else []
            
            return {
                'volume_profile': volume_profile,
                'price_signature': price_signature,
                'rsi_journey': rsi_journey
            }
            
        except Exception as e:
            if self.debug_mode:
                print(f"[FINGERPRINT] Error creating fingerprint: {e}")
            return {}
    
    def analyze_pump_with_ai(self, symbol: str, exchange_name: str, ohlcv_data: List,
                            volumes: List[float], current_price: float, volume_multiple: float,
                            price_change_pct: float, rsi: Optional[float]) -> Optional[Dict]:
        """Comprehensive AI analysis of potential pump"""
        
        if not self.ai_mode:
            return None
        
        try:
            # Create pump data structure
            pump_data = {
                'symbol': symbol,
                'exchange': exchange_name,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'price': current_price,
                'volume_multiple': volume_multiple,
                'price_change_pct': price_change_pct,
                'rsi': rsi,
                'market_cap_est': current_price * 1_000_000,  # Rough estimate
            }
            
            # Add fingerprint data
            fingerprint = self.create_pump_fingerprint(ohlcv_data, volumes, [], len(ohlcv_data)-1)
            pump_data.update(fingerprint)
            
            analysis_results = {}
            
            # Pattern matching analysis
            if self.pattern_analysis:
                try:
                    pattern_matches = self.pattern_engine.find_similar_patterns(pump_data, limit=10)
                    if pattern_matches:
                        analysis_results['pattern_matches'] = pattern_matches
                        self.stats['patterns_matched'] += 1
                        
                        if self.debug_mode:
                            best_match = pattern_matches[0]
                            print(f"[PATTERN] {symbol}: {len(pattern_matches)} matches, best: {best_match.similarity_score:.2f} similarity")
                    
                except Exception as e:
                    if self.debug_mode:
                        print(f"[PATTERN] Error for {symbol}: {e}")
            
            # ML predictions
            if self.ml_predictions and hasattr(self.ml_manager, 'models_trained') and self.ml_manager.models_trained:
                try:
                    ml_predictions = self.ml_manager.get_comprehensive_prediction(pump_data, analysis_results.get('pattern_matches'))
                    analysis_results['ml_predictions'] = ml_predictions
                    self.stats['ml_predictions_made'] += 1
                    
                    if self.debug_mode:
                        recommendation = ml_predictions.get('recommendation', {})
                        print(f"[ML] {symbol}: {recommendation.get('action', 'UNKNOWN')} - {recommendation.get('reason', 'No reason')}")
                    
                except Exception as e:
                    if self.debug_mode:
                        print(f"[ML] Error for {symbol}: {e}")
            
            # Market manipulation detection
            if self.market_manipulation_detection:
                try:
                    # Add current event to recent events buffer
                    self.recent_events.append({
                        'timestamp': pump_data['timestamp'],
                        'symbol': symbol,
                        'exchange': exchange_name,
                        'volume_multiple': volume_multiple,
                        'price_change_pct': price_change_pct,
                        'rsi': rsi
                    })
                    
                    # Keep buffer manageable
                    if len(self.recent_events) > self.max_events_buffer:
                        self.recent_events = self.recent_events[-self.max_events_buffer:]
                    
                    # Detect market regime (only if we have enough events)
                    if len(self.recent_events) >= 5:
                        market_regime = self.pattern_engine.detect_market_manipulation(self.recent_events)
                        analysis_results['market_regime'] = market_regime
                        
                        if market_regime.manipulation_probability > 0.6:
                            self.stats['manipulation_detected'] += 1
                            
                            if self.debug_mode:
                                print(f"[MANIPULATION] Market manipulation detected: {market_regime.manipulation_probability:.2f} probability")
                    
                except Exception as e:
                    if self.debug_mode:
                        print(f"[MANIPULATION] Error: {e}")
            
            return analysis_results if analysis_results else None
            
        except Exception as e:
            if self.debug_mode:
                print(f"[AI_ANALYSIS] Error for {symbol}: {e}")
            return None
    
    def generate_enhanced_alert(self, symbol: str, exchange_name: str, pump_data: Dict, 
                               ai_analysis: Optional[Dict] = None) -> str:
        """Generate enhanced alert with AI insights"""
        
        try:
            # Basic metrics
            volume_mult = pump_data.get('volume_multiple', 0)
            price_change = pump_data.get('price_change_pct', 0)
            rsi = pump_data.get('rsi')
            current_price = pump_data.get('price', 0)
            
            # Exchange emoji
            exchange_emoji = '🟡' if exchange_name.lower() == 'binance' else '🔵' if exchange_name.lower() == 'bingx' else '⚪'
            
            # Base alert
            msg = f"""🚀 <b>AI-ENHANCED SIGNAL</b> {exchange_emoji}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

<b>Par:</b> {symbol}
<b>Exchange:</b> {exchange_name.upper()}
<b>Time:</b> {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}

<b>📊 Current Metrics:</b>
• Price: ${current_price:.6f} ({price_change:+.1f}%)
• Volume: {volume_mult:.1f}x average
• RSI: {rsi:.1f if rsi else 'N/A'}"""
            
            if ai_analysis:
                # Pattern analysis section
                pattern_matches = ai_analysis.get('pattern_matches', [])
                if pattern_matches:
                    best_match = pattern_matches[0]
                    success_rate = getattr(best_match, 'historical_success_rate', 0) * 100
                    msg += f"""

<b>🧬 Pattern Analysis:</b>
• Similarity: {best_match.similarity_score*100:.0f}% match
• Historical Success: {success_rate:.0f}% ({len(pattern_matches)} patterns)
• Risk Level: {getattr(best_match, 'risk_level', 'UNKNOWN')}
• Confidence: {getattr(best_match, 'confidence', 'MEDIUM')}"""
                
                # ML predictions section
                ml_predictions = ai_analysis.get('ml_predictions', {})
                if ml_predictions:
                    recommendation = ml_predictions.get('recommendation', {})
                    risk_analysis = ml_predictions.get('risk_analysis', {})
                    dump_timing = ml_predictions.get('dump_timing', {})
                    
                    msg += f"""

<b>🤖 AI Predictions:</b>
• Risk Score: {risk_analysis.get('risk_score', 'N/A')}/10
• Risk Level: {risk_analysis.get('risk_level', 'UNKNOWN')}"""
                    
                    if dump_timing.get('predicted_hours'):
                        msg += f"\n• Dump Timing: {dump_timing['predicted_hours']:.1f}h"
                    
                    msg += f"""
• Action: <b>{recommendation.get('action', 'MONITOR')}</b>
• Position: {recommendation.get('position_size', 'Unknown')}"""
                
                # Market manipulation warning
                market_regime = ai_analysis.get('market_regime')
                if market_regime and market_regime.manipulation_probability > 0.6:
                    msg += f"""

<b>⚠️ Manipulation Alert:</b>
• Probability: {market_regime.manipulation_probability*100:.0f}%
• Type: {market_regime.regime_type}
• Coordinated: {'Yes' if market_regime.coordinated_activity else 'No'}"""
                
                # AI recommendation
                if ml_predictions:
                    recommendation = ml_predictions.get('recommendation', {})
                    action = recommendation.get('action', 'MONITOR')
                    reason = recommendation.get('reason', 'No specific reason')
                    
                    if action == 'BUY':
                        msg += f"\n\n<b>💡 AI Recommendation:</b> 🟢 <b>BUY SIGNAL</b>\n• {reason}"
                    elif action == 'AVOID':
                        msg += f"\n\n<b>💡 AI Recommendation:</b> 🔴 <b>AVOID</b>\n• {reason}"
                    elif action == 'CAUTIOUS_BUY':
                        msg += f"\n\n<b>💡 AI Recommendation:</b> 🟡 <b>CAUTIOUS</b>\n• {reason}"
                    else:
                        msg += f"\n\n<b>💡 AI Recommendation:</b> ⚪ <b>MONITOR</b>\n• {reason}"
                
                # Trading guidance
                if pattern_matches:
                    msg += f"\n\n<b>📈 Guidance:</b>\n• Entry: Current or -2-3%\n• Target: +15-25%\n• Stop: -6-8%"
            else:
                msg += f"\n\n<b>🤖 AI Analysis:</b> Basic detection (AI components loading...)"
            
            # Link to exchange
            if exchange_name.lower() == 'binance':
                binance_symbol = symbol.replace('/', '')
                msg += f"\n\n<b>🔗 Trade:</b> binance.com/trade/{binance_symbol}"
            elif exchange_name.lower() == 'bingx':
                bingx_symbol = symbol.replace('/', '-')
                msg += f"\n\n<b>🔗 Trade:</b> bingx.com/spot/{bingx_symbol}"
            
            return msg
            
        except Exception as e:
            if self.debug_mode:
                print(f"[ALERT] Error generating alert for {symbol}: {e}")
            
            # Fallback basic alert
            return f"""🚀 <b>PUMP DETECTED</b>

<b>Par:</b> {symbol}
<b>Exchange:</b> {exchange_name.upper()}
<b>Price Change:</b> {pump_data.get('price_change_pct', 0):+.1f}%
<b>Volume:</b> {pump_data.get('volume_multiple', 0):.1f}x

<b>🔗 Trade:</b> Check {exchange_name} for {symbol}"""
    
    async def initialize_ai_components(self):
        """Initialize AI components with historical data"""
        
        if not self.ai_mode:
            print("⚠️ AI mode disabled, skipping AI initialization")
            return
        
        print("🧠 Initializing AI components...")
        
        try:
            # Check if we have enough historical data
            stats = self.db.get_statistics()
            pump_events_count = stats.get('pump_events_count', 0)
            
            print(f"📊 Current database: {pump_events_count} pump events")
            
            if pump_events_count < 20 and self.auto_collect_historical:
                print(f"📊 Collecting {self.historical_days} days of historical data...")
                
                # Initialize historical collector
                collector = HistoricalCollector(self.db, self.exchanges_config)
                
                # Get symbols to analyze (reduced for faster collection)
                symbols_per_exchange = {}
                for exchange_name in self.exchanges_list:
                    exchange_name = exchange_name.strip()
                    if exchange_name and exchange_name in self.exchanges:
                        symbols = self.get_symbols_for_exchange(exchange_name, 15)  # Reduced for speed
                        if symbols:
                            symbols_per_exchange[exchange_name] = symbols
                            print(f"[{exchange_name.upper()}] Will collect data for {len(symbols)} symbols")
                
                # Collect historical data (this may take 10-30 minutes)
                if symbols_per_exchange:
                    await collector.collect_all_historical(symbols_per_exchange)
                    print("✅ Historical data collection completed")
                else:
                    print("⚠️ No symbols found for historical collection")
            else:
                print("✅ Sufficient historical data available")
            
            # Train ML models
            if self.ml_predictions:
                try:
                    print("🤖 Training ML models...")
                    performances = self.ml_manager.train_all_models()
                    if performances:
                        print("✅ ML models trained successfully")
                        for model, perf in performances.items():
                            print(f"  • {model}: Accuracy {perf.accuracy:.3f}")
                    else:
                        print("⚠️ ML model training skipped (insufficient data)")
                        self.ml_predictions = False
                except Exception as e:
                    print(f"❌ ML training failed: {e}")
                    self.ml_predictions = False
            
            print("🎯 AI components initialization completed")
            
        except Exception as e:
            print(f"❌ AI initialization error: {e}")
            print("⚠️ Continuing with basic detection mode...")
            self.ai_mode = False
    
    async def run_enhanced_detection_loop(self):
        """Main detection loop with AI enhancements"""
        
        print("🔄 Starting enhanced detection loop...")
        
        loop_count = 0
        last_stats_update = time.time()
        
        while True:
            loop_start = time.time()
            loop_count += 1
            
            # Progress info every 50 loops (about every 25 minutes)
            if loop_count % 50 == 0:
                uptime = (time.time() - self.stats['start_time']) / 3600
                print(f"[STATS] Loop #{loop_count}")
                print(f"  • Uptime: {uptime:.1f}h")
                print(f"  • Alerts sent: {self.stats['alerts_sent']}")
                print(f"  • Pumps detected: {self.stats['pumps_detected']}")
                print(f"  • Pattern matches: {self.stats['patterns_matched']}")
                print(f"  • ML predictions: {self.stats['ml_predictions_made']}")
