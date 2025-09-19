# enhanced_bot_main.py - Complete AI-Enhanced Trading Bot
import os
import time
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import json
import numpy as np
from collections import defaultdict

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
        # Configuration from environment variables
        self.exchanges_config = {
            'binance': {},
            'bingx': {'timeout': 30000, 'rateLimit': 2000}
        }
        
        self.exchanges = {}
        self.watchlist = {}
        
        # Core components
        self.db = PatternDatabase()
        self.pattern_engine = PatternEngine(self.db)
        self.ml_manager = MLModelManager(self.db)
        
        # Bot settings from env
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
        self.debug_mode = os.getenv("DEBUG_MODE", "true").lower() == "true"
        
        # AI Enhancement settings
        self.ai_mode = os.getenv("AI_MODE", "true").lower() == "true"
        self.pattern_analysis = os.getenv("PATTERN_ANALYSIS", "true").lower() == "true"
        self.ml_predictions = os.getenv("ML_PREDICTIONS", "true").lower() == "true"
        self.market_manipulation_detection = os.getenv("MANIPULATION_DETECTION", "true").lower() == "true"
        
        # Telegram
        self.tg_token = os.getenv("TG_TOKEN", "")
        self.tg_chat_id = os.getenv("TG_CHAT_ID", "")
        
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
                json={"chat_id": self.tg_chat_id, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=20
            )
        except Exception as e:
            print(f"[Telegram] Error: {e}")
    
    def build_exchange(self, name: str):
        """Build exchange with specific configurations"""
        name = name.strip().lower()
        if not hasattr(ccxt, name):
            raise ValueError(f"Exchange '{name}' not found in ccxt")
        
        config = {
            "enableRateLimit": True,
            "options": {"adjustForTimeDifference": True}
        }
        
        if name in self.exchanges_config:
            config.update(self.exchanges_config[name])
        
        ex = getattr(ccxt, name)(config)
        ex.load_markets()
        return ex
    
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
                    
            except ccxt.NetworkError:
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 3)
                    continue
                return None
            except ccxt.ExchangeError as e:
                if "rate limit" in str(e).lower():
                    time.sleep(5)
                    continue
                return None
            except Exception:
                return None
        
        return None
    
    def get_symbols_for_exchange(self, exchange_name: str, limit: int = 30) -> List[str]:
        """Get top volume symbols for exchange"""
        ex = self.exchanges.get(exchange_name)
        if not ex:
            return []
        
        try:
            tickers = ex.fetch_tickers()
            volume_pairs = []
            
            for symbol, ticker in tickers.items():
                if not any(symbol.endswith("/" + q) for q in self.quote_filter):
                    continue
                
                volume = ticker.get("quoteVolume")
                if volume and 200_000 <= volume <= 50_000_000:
                    volume_pairs.append((symbol, volume))
            
            volume_pairs.sort(key=lambda x: x[1], reverse=True)
            return [symbol for symbol, _ in volume_pairs[:limit]]
            
        except Exception as e:
            print(f"❌ Error getting symbols for {exchange_name}: {e}")
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
        
        # Volume profile
        vol_data = volumes[max(0, index-lookback):index+1]
        volume_profile = {
            'mean': float(np.mean(vol_data)),
            'std': float(np.std(vol_data)),
            'trend': float(np.corrcoef(range(len(vol_data)), vol_data)[0,1]) if len(vol_data) > 1 else 0,
            'spikes_count': int(sum(1 for v in vol_data if v > np.mean(vol_data) + 2*np.std(vol_data)))
        }
        
        # Price signature
        price_data = ohlcv_data[max(0, index-10):index+1]
        if price_data:
            closes = [c[4] for c in price_data]
            highs = [c[2] for c in price_data]
            lows = [c[3] for c in price_data]
            opens = [c[1] for c in price_data]
            
            price_signature = {
                'volatility': float(np.std(closes) / np.mean(closes)) if np.mean(closes) > 0 else 0,
                'trend_strength': float(np.corrcoef(range(len(closes)), closes)[0,1]) if len(closes) > 1 else 0,
                'upper_shadows_avg': float(np.mean([(h-max(o,c))/(h-l) for o,h,l,c in zip(opens,highs,lows,closes) if h-l > 0])) if any(h-l > 0 for h,l in zip(highs,lows)) else 0,
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
    
    def analyze_pump_with_ai(self, symbol: str, exchange_name: str, ohlcv_data: List,
                            volumes: List[float], current_price: float, volume_multiple: float,
                            price_change_pct: float, rsi: Optional[float]) -> Optional[Dict]:
        """Comprehensive AI analysis of potential pump"""
        
        if not self.ai_mode:
            return None
        
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
                        print(f"[PATTERN] {symbol}: {len(pattern_matches)} matches, best: {best_match.similarity_score:.2f} similarity, {best_match.historical_success_rate:.2f} success rate")
                
            except Exception as e:
                print(f"❌ Pattern analysis error for {symbol}: {e}")
        
        # ML predictions
        if self.ml_predictions and self.ml_manager.models_trained:
            try:
                ml_predictions = self.ml_manager.get_comprehensive_prediction(pump_data, analysis_results.get('pattern_matches'))
                analysis_results['ml_predictions'] = ml_predictions
                self.stats['ml_predictions_made'] += 1
                
                if self.debug_mode:
                    recommendation = ml_predictions.get('recommendation', {})
                    print(f"[ML] {symbol}: {recommendation.get('action', 'UNKNOWN')} - {recommendation.get('reason', 'No reason')}")
                
            except Exception as e:
                print(f"❌ ML prediction error for {symbol}: {e}")
        
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
                
                # Detect market regime
                market_regime = self.pattern_engine.detect_market_manipulation(self.recent_events)
                analysis_results['market_regime'] = market_regime
                
                if market_regime.manipulation_probability > 0.6:
                    self.stats['manipulation_detected'] += 1
                    
                    if self.debug_mode:
                        print(f"[MANIPULATION] Market manipulation detected: {market_regime.manipulation_probability:.2f} probability")
                
            except Exception as e:
                print(f"❌ Manipulation detection error: {e}")
        
        return analysis_results if analysis_results else None
    
    def generate_enhanced_alert(self, symbol: str, exchange_name: str, pump_data: Dict, 
                               ai_analysis: Optional[Dict] = None) -> str:
        """Generate enhanced alert with AI insights"""
        
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
                msg += f"""

<b>🧬 Pattern Analysis:</b>
• Similarity: {best_match.similarity_score*100:.0f}% match to historical
• Success Rate: {best_match.historical_success_rate*100:.0f}% ({len(pattern_matches)} patterns)
• Risk Level: {best_match.risk_level}
• Confidence: {best_match.confidence}"""
            
            # ML predictions section
            ml_predictions = ai_analysis.get('ml_predictions', {})
            if ml_predictions:
                recommendation = ml_predictions.get('recommendation', {})
                risk_analysis = ml_predictions.get('risk_analysis', {})
                dump_timing = ml_predictions.get('dump_timing', {})
                
                msg += f"""

<b>🤖 AI Predictions:</b>
• Risk Score: {risk_analysis.get('risk_score', 'N/A')}/10 ({risk_analysis.get('risk_level', 'UNKNOWN')})
• Dump Timing: {dump_timing.get('predicted_hours', 'N/A'):.1f}h
• Action: <b>{recommendation.get('action', 'MONITOR')}</b>
• Position Size: {recommendation.get('position_size', 'Unknown')}"""
            
            # Market manipulation warning
            market_regime = ai_analysis.get('market_regime')
            if market_regime and market_regime.manipulation_probability > 0.6:
                msg += f"""

<b>⚠️ Market Manipulation Alert:</b>
• Probability: {market_regime.manipulation_probability*100:.0f}%
• Regime: {market_regime.regime_type}
• Coordinated Activity: {'Yes' if market_regime.coordinated_activity else 'No'}"""
            
            # AI recommendation
            if ml_predictions:
                recommendation = ml_predictions.get('recommendation', {})
                action = recommendation.get('action', 'MONITOR')
                reason = recommendation.get('reason', 'No specific reason')
                
                if action == 'BUY':
                    msg += f"\n\n<b>💡 AI Recommendation:</b> 🟢 <b>BUY SIGNAL</b>\n• Reason: {reason}"
                elif action == 'AVOID':
                    msg += f"\n\n<b>💡 AI Recommendation:</b> 🔴 <b>AVOID</b>\n• Reason: {reason}"
                elif action == 'CAUTIOUS_BUY':
                    msg += f"\n\n<b>💡 AI Recommendation:</b> 🟡 <b>CAUTIOUS BUY</b>\n• Reason: {reason}"
                else:
                    msg += f"\n\n<b>💡 AI Recommendation:</b> ⚪ <b>MONITOR</b>\n• Reason: {reason}"
            
            # Trading guidance
            if pattern_matches:
                best_match = pattern_matches[0]
                msg += f"\n\n<b>📈 Trading Guidance:</b>\n• Entry: Current or -2-3%\n• Target: +15-25% (based on pattern)\n• Stop Loss: -6-8%\n• Time Horizon: {best_match.avg_duration_minutes/60:.1f}h typical"
        
        # Link to exchange
        if exchange_name.lower() == 'binance':
            binance_symbol = symbol.replace('/', '')
            msg += f"\n\n<b>🔗 Trade:</b> binance.com/trade/{binance_symbol}"
        elif exchange_name.lower() == 'bingx':
            bingx_symbol = symbol.replace('/', '-')
            msg += f"\n\n<b>🔗 Trade:</b> bingx.com/spot/{bingx_symbol}"
        
        return msg
    
    async def initialize_ai_components(self):
        """Initialize AI components with historical data"""
        
        if not self.ai_mode:
            print("⚠️ AI mode disabled, skipping AI initialization")
            return
        
        print("🧠 Initializing AI components...")
        
        # Check if we have enough historical data
        stats = self.db.get_statistics()
        pump_events_count = stats.get('pump_events_count', 0)
        
        if pump_events_count < 50:
            print("📊 Insufficient historical data, starting collection...")
            
            # Initialize historical collector
            collector = HistoricalCollector(self.db, self.exchanges_config)
            
            # Get symbols to analyze
            symbols_per_exchange = {}
            for exchange_name in self.exchanges_list:
                if exchange_name.strip() and exchange_name in self.exchanges:
                    symbols = self.get_symbols_for_exchange(exchange_name, 20)  # Reduced for faster collection
                    if symbols:
                        symbols_per_exchange[exchange_name] = symbols
            
            # Collect historical data (async)
            if symbols_per_exchange:
                await collector.collect_all_historical(symbols_per_exchange)
        
        # Train ML models
        if self.ml_predictions:
            try:
                performances = self.ml_manager.train_all_models()
                if performances:
                    print("✅ ML models trained successfully")
                    for model, perf in performances.items():
                        print(f"  {model}: Accuracy {perf.accuracy:.3f}")
            except Exception as e:
                print(f"❌ ML training failed: {e}")
                self.ml_predictions = False
        
        print("🎯 AI components initialized")
    
    async def run_enhanced_detection_loop(self):
        """Main detection loop with AI enhancements"""
        
        print("🔄 Starting enhanced detection loop...")
        
        loop_count = 0
        
        while True:
            loop_start = time.time()
            loop_count += 1
            
            # Progress info
            if self.debug_mode and loop_count % 10 == 0:
                uptime = (time.time() - self.stats['start_time']) / 3600
                print(f"[STATS] Loop #{loop_count}, Uptime: {uptime:.1f}h, Alerts: {self.stats['alerts_sent']}, ML Predictions: {self.stats['ml_predictions_made']}")
            
            for exchange_name, ex in self.exchanges.items():
                symbols = self.watchlist.get(exchange_name, [])
                
                for symbol in symbols:
                    try:
                        # Fetch OHLCV data
                        ohlcv = self.fetch_ohlcv_safe(ex, symbol, self.timeframe, self.lookback + 15)
                        if not ohlcv or len(ohlcv) < self.lookback + 1:
                            continue
                        
                        # Calculate basic metrics
                        *hist, last = ohlcv
                        volumes = [c[5] for c in hist[-self.lookback:]]
                        vol_avg = sum(volumes) / len(volumes) if volumes else 0
                        vol_last = last[5]
                        close_last = last[4]
                        
                        vol_multiple = vol_last / vol_avg if vol_avg > 0 else 0
                        
                        # Price change
                        price_change_pct = 0
                        if len(hist) > 0:
                            prev_close = hist[-1][4]
                            price_change_pct = (close_last - prev_close) / prev_close if prev_close > 0 else 0
                        
                        # Apply basic filters
                        if abs(price_change_pct) < self.min_price_change or vol_multiple < self.threshold:
                            continue
                        
                        # Calculate RSI
                        prices = [c[4] for c in ohlcv]
                        rsi = self.calculate_rsi(prices)
                        
                        # Basic risk score (will be enhanced by AI)
                        basic_risk_score = min(int((vol_multiple / 5 + abs(price_change_pct) * 10) * 2), 10)
                        
                        if basic_risk_score < self.min_risk_score:
                            continue
                        
                        # AI Analysis
                        ai_analysis = self.analyze_pump_with_ai(
                            symbol, exchange_name, ohlcv, volumes, close_last, 
                            vol_multiple, price_change_pct * 100, rsi
                        )
                        
                        # Determine if we should alert based on AI analysis
                        should_alert = True
                        
                        if ai_analysis and ai_analysis.get('ml_predictions'):
                            ml_rec = ai_analysis['ml_predictions'].get('recommendation', {})
                            if ml_rec.get('action') == 'AVOID' and ml_rec.get('confidence', 0) > 0.7:
                                should_alert = False
                                if self.debug_mode:
                                    print(f"[AI FILTER] {symbol}: Skipped alert - AI recommends AVOID")
                        
                        if should_alert:
                            key = f"ENHANCED:{exchange_name}:{symbol}"
                            if self.can_alert(key, time.time()):
                                
                                # Create pump data for alert
                                pump_data = {
                                    'volume_multiple': vol_multiple,
                                    'price_change_pct': price_change_pct * 100,
                                    'rsi': rsi,
                                    'price': close_last
                                }
                                
                                # Generate and send enhanced alert
                                alert_message = self.generate_enhanced_alert(symbol, exchange_name, pump_data, ai_analysis)
                                self.send_telegram(alert_message)
                                
                                self.stats['alerts_sent'] += 1
                                self.stats['pumps_detected'] += 1
                                
                                # Save event to database
                                event_data = {
                                    'timestamp': datetime.now(timezone.utc).isoformat(),
                                    'exchange': exchange_name,
                                    'symbol': symbol,
                                    'event_type': 'AI_ENHANCED_PUMP',
                                    'price': close_last,
                                    'volume_multiple': vol_multiple,
                                    'rsi': rsi,
                                    'risk_score': basic_risk_score,
                                    'price_change_pct': price_change_pct * 100,
                                    **pump_data
                                }
                                
                                self.db.save_pump_event(event_data)
                                
                                # Save ML prediction if available
                                if ai_analysis and ai_analysis.get('ml_predictions'):
                                    self.ml_manager.save_prediction_result(ai_analysis['ml_predictions'])
                    
                    except ccxt.NetworkError:
                        continue
                    except Exception as e:
                        if self.debug_mode:
                            print(f"❌ Error processing {exchange_name} {symbol}: {e}")
                        continue
            
            # Sleep with timing control
            elapsed = time.time() - loop_start
            sleep_time = max(0, self.sleep_seconds - elapsed)
            
            if self.debug_mode and elapsed > self.sleep_seconds * 0.8:
                print(f"[DEBUG] Slow loop: {elapsed:.1f}s (target: {self.sleep_seconds}s)")
            
            await asyncio.sleep(sleep_time)
    
    async def run(self):
        """Main run method"""
        
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
                    
                    # Get watchlist
                    symbols = self.get_symbols_for_exchange(exchange_name, self.top_n_by_volume)
                    self.watchlist[exchange_name] = symbols
                    
                    print(f"✅ {exchange_name}: {len(symbols)} symbols")
                    
                except Exception as e:
                    print(f"❌ Failed to initialize {exchange_name}: {e}")
            
            if not self.exchanges:
                raise SystemExit("❌ No exchanges initialized")
            
            total_symbols = sum(len(symbols) for symbols in self.watchlist.values())
            
            # Initialize AI components
            await self.initialize_ai_components()
            
            # Send startup notification
            startup_msg = f"""✅ <b>Enhanced Trading Bot ONLINE</b>

🏦 <b>Exchanges:</b> {', '.join(self.exchanges.keys())}
📊 <b>Symbols:</b> {total_symbols} total
🤖 <b>AI Features:</b>
• Pattern Analysis: {'✅' if self.pattern_analysis else '❌'}
• ML Predictions: {'✅' if self.ml_predictions else '❌'}
• Manipulation Detection: {'✅' if self.market_manipulation_detection else '❌'}

🎯 <b>Settings:</b>
• Volume Threshold: {self.threshold}x
• Min Price Change: {self.min_price_change*100:.1f}%
• Min Risk Score: {self.min_risk_score}/10
• Timeframe: {self.timeframe}

Ready for AI-enhanced pump detection! 🚀"""
            
            self.send_telegram(startup_msg)
            
            # Start main detection loop
            await self.run_enhanced_detection_loop()
            
        except KeyboardInterrupt:
            print("\n👋 Bot stopped by user")
            
            # Send shutdown notification with stats
            uptime_hours = (time.time() - self.stats['start_time']) / 3600
            shutdown_msg = f"""👋 <b>Enhanced Trading Bot OFFLINE</b>

📊 <b>Session Stats:</b>
• Runtime: {uptime_hours:.1f} hours
• Alerts Sent: {self.stats['alerts_sent']}
• Pumps Detected: {self.stats['pumps_detected']}
• Pattern Matches: {self.stats['patterns_matched']}
• ML Predictions: {self.stats['ml_predictions_made']}
• Manipulations Detected: {self.stats['manipulation_detected']}

See you next time! 🚀"""
            
            self.send_telegram(shutdown_msg)
            
        except Exception as e:
            error_msg = f"❌ Bot crashed: {e}"
            print(error_msg)
            self.send_telegram(error_msg)
            raise
        
        finally:
            # Cleanup
            if hasattr(self, 'db'):
                self.db.close()

# Main execution
async def main():
    """Main function"""
    bot = EnhancedTradingBot()
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
