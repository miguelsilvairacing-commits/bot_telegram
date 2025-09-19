# historical_collector.py - Historical Data Collection System
import asyncio
import ccxt
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import time
import json
from dataclasses import dataclass
from database import PatternDatabase

@dataclass
class PumpEvent:
    """Data class para pump events detectados no histórico"""
    timestamp: str
    exchange: str
    symbol: str
    event_type: str
    price: float
    volume_multiple: float
    rsi: Optional[float]
    risk_score: int
    pump_stage: str
    confidence: str
    price_change_pct: float
    volume_profile: Dict
    price_signature: Dict
    rsi_journey: List[float]

class HistoricalCollector:
    """
    Sistema para coletar dados históricos e detectar pumps/dumps passados
    """
    
    def __init__(self, db: PatternDatabase, exchanges_config: Dict):
        self.db = db
        self.exchanges = {}
        self.exchanges_config = exchanges_config
        
        # Configurações de coleta
        self.lookback_days = 90  # 3 meses de dados
        self.timeframes = ['1m', '5m', '15m', '1h', '4h']
        self.main_timeframe = '1m'  # Para detecção de pumps
        
        # Thresholds para detecção histórica
        self.volume_threshold = 3.0  # 3x volume média
        self.price_change_threshold = 0.05  # 5% mínimo
        self.rsi_period = 14
        
        self._initialize_exchanges()
    
    def _initialize_exchanges(self):
        """Initialize exchange connections"""
        for name, config in self.exchanges_config.items():
            try:
                if hasattr(ccxt, name):
                    ex_class = getattr(ccxt, name)
                    self.exchanges[name] = ex_class({
                        'enableRateLimit': True,
                        'timeout': 30000,
                        **config
                    })
                    self.exchanges[name].load_markets()
                    print(f"✅ {name}: {len(self.exchanges[name].markets)} markets loaded")
                else:
                    print(f"❌ Exchange {name} not found in ccxt")
            except Exception as e:
                print(f"❌ Failed to initialize {name}: {e}")
    
    def calculate_rsi(self, prices: List[float], period: int = 14) -> List[float]:
        """Calculate RSI for price series"""
        if len(prices) < period + 1:
            return [50.0] * len(prices)  # Default neutral RSI
        
        prices = np.array(prices)
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        rsi_values = []
        
        # Calculate initial RSI
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])
        
        if avg_loss == 0:
            rs = 100
        else:
            rs = avg_gain / avg_loss
        
        rsi = 100 - (100 / (1 + rs))
        rsi_values.append(rsi)
        
        # Calculate subsequent RSI values
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            
            if avg_loss == 0:
                rs = 100
            else:
                rs = avg_gain / avg_loss
            
            rsi = 100 - (100 / (1 + rs))
            rsi_values.append(rsi)
        
        # Pad beginning with neutral RSI
        return [50.0] * period + rsi_values
    
    def create_volume_profile(self, volumes: List[float], lookback: int = 20) -> Dict:
        """Create volume profile fingerprint"""
        if len(volumes) < lookback:
            return {}
        
        recent_volumes = volumes[-lookback:]
        
        return {
            'mean': float(np.mean(recent_volumes)),
            'std': float(np.std(recent_volumes)),
            'max': float(np.max(recent_volumes)),
            'min': float(np.min(recent_volumes)),
            'skewness': float(np.mean([(v - np.mean(recent_volumes))**3 for v in recent_volumes])),
            'trend': float(np.corrcoef(range(len(recent_volumes)), recent_volumes)[0,1]) if len(recent_volumes) > 1 else 0.0,
            'spikes_count': int(sum(1 for v in recent_volumes if v > np.mean(recent_volumes) + 2*np.std(recent_volumes)))
        }
    
    def create_price_signature(self, ohlc_data: List[List[float]]) -> Dict:
        """Create price action signature"""
        if len(ohlc_data) < 5:
            return {}
        
        # Extract OHLC components
        opens = [candle[1] for candle in ohlc_data]
        highs = [candle[2] for candle in ohlc_data]
        lows = [candle[3] for candle in ohlc_data]
        closes = [candle[4] for candle in ohlc_data]
        
        return {
            'volatility': float(np.std(closes) / np.mean(closes)) if np.mean(closes) > 0 else 0,
            'trend_strength': float(np.corrcoef(range(len(closes)), closes)[0,1]) if len(closes) > 1 else 0,
            'upper_shadows_avg': float(np.mean([(h-max(o,c))/(h-l) for o,h,l,c in zip(opens,highs,lows,closes) if h-l > 0])),
            'lower_shadows_avg': float(np.mean([(min(o,c)-l)/(h-l) for o,h,l,c in zip(opens,highs,lows,closes) if h-l > 0])),
            'body_size_avg': float(np.mean([abs(c-o)/(h-l) for o,h,l,c in zip(opens,highs,lows,closes) if h-l > 0])),
            'green_candles_pct': float(sum(1 for o,c in zip(opens,closes) if c > o) / len(closes)),
            'price_range_pct': float((max(highs) - min(lows)) / min(lows)) if min(lows) > 0 else 0
        }
    
    def detect_historical_pump(self, ohlc_data: List[List[float]], volumes: List[float], 
                              rsi_values: List[float], index: int) -> Optional[PumpEvent]:
        """Detect pump event in historical data"""
        
        lookback = 20
        if index < lookback or index >= len(ohlc_data) - 5:  # Need some data after for outcome
            return None
        
        # Current candle data
        current_volume = volumes[index]
        current_price = ohlc_data[index][4]  # Close price
        current_rsi = rsi_values[index] if index < len(rsi_values) else 50.0
        
        # Historical volume average
        hist_volumes = volumes[index-lookback:index]
        vol_avg = np.mean(hist_volumes) if hist_volumes else 0
        vol_multiple = current_volume / vol_avg if vol_avg > 0 else 0
        
        # Price change
        prev_price = ohlc_data[index-1][4] if index > 0 else current_price
        price_change_pct = (current_price - prev_price) / prev_price if prev_price > 0 else 0
        
        # Check if this qualifies as a pump
        if vol_multiple >= self.volume_threshold and abs(price_change_pct) >= self.price_change_threshold:
            
            # Create fingerprints
            volume_profile = self.create_volume_profile(volumes[max(0, index-lookback):index+1])
            price_signature = self.create_price_signature(ohlc_data[max(0, index-10):index+1])
            rsi_journey = rsi_values[max(0, index-10):index+1] if index < len(rsi_values) else []
            
            # Determine pump stage based on RSI and price action
            pump_stage = self.classify_pump_stage(current_rsi, price_change_pct, vol_multiple)
            
            # Calculate risk score
            risk_score = self.calculate_historical_risk_score(vol_multiple, price_change_pct, current_rsi)
            
            # Determine confidence
            confidence = "HIGH" if vol_multiple > 10 and abs(price_change_pct) > 0.15 else "MEDIUM"
            
            # Event type
            event_type = "PUMP" if price_change_pct > 0 else "DUMP"
            
            timestamp = datetime.fromtimestamp(ohlc_data[index][0]/1000, tz=timezone.utc).isoformat()
            
            return PumpEvent(
                timestamp=timestamp,
                exchange="",  # Will be set by caller
                symbol="",    # Will be set by caller  
                event_type=event_type,
                price=current_price,
                volume_multiple=vol_multiple,
                rsi=current_rsi,
                risk_score=risk_score,
                pump_stage=pump_stage,
                confidence=confidence,
                price_change_pct=price_change_pct * 100,
                volume_profile=volume_profile,
                price_signature=price_signature,
                rsi_journey=rsi_journey
            )
        
        return None
    
    def classify_pump_stage(self, rsi: float, price_change_pct: float, vol_multiple: float) -> str:
        """Classify pump stage based on indicators"""
        abs_change = abs(price_change_pct)
        
        if rsi < 60 and abs_change < 0.12 and vol_multiple > 4:
            return "EARLY_PUMP"
        elif 60 <= rsi <= 78 and abs_change < 0.20:
            return "MID_PUMP"
        elif rsi > 78 or abs_change > 0.25:
            return "LATE_PUMP"
        elif rsi < 35 and price_change_pct < -0.08:
            return "DUMP_PHASE"
        else:
            return "UNKNOWN"
    
    def calculate_historical_risk_score(self, volume_mult: float, price_change: float, rsi: float) -> int:
        """Calculate risk score for historical event"""
        score = 0
        
        # Volume component
        if volume_mult > 15: score += 4
        elif volume_mult > 10: score += 3
        elif volume_mult > 6: score += 2
        elif volume_mult > 3: score += 1
        
        # Price change component
        abs_change = abs(price_change)
        if abs_change > 0.20: score += 3
        elif abs_change > 0.12: score += 2
        elif abs_change > 0.06: score += 1
        
        # RSI component
        if 50 <= rsi <= 75: score += 2  # Sweet spot
        elif 40 <= rsi <= 80: score += 1
        
        return min(score, 10)
    
    def calculate_pump_outcome(self, ohlc_data: List[List[float]], start_index: int, 
                              lookforward_hours: int = 24) -> Dict:
        """Calculate what happened after a pump"""
        
        start_price = ohlc_data[start_index][4]
        max_gain = 0
        max_loss = 0
        time_to_peak = 0
        time_to_dump = None
        
        # Look forward up to lookforward_hours
        end_index = min(start_index + lookforward_hours * 60, len(ohlc_data))  # Assuming 1m data
        
        for i in range(start_index + 1, end_index):
            current_price = ohlc_data[i][4]
            price_change = (current_price - start_price) / start_price
            
            # Track maximum gain
            if price_change > max_gain:
                max_gain = price_change
                time_to_peak = i - start_index
            
            # Track maximum loss
            if price_change < max_loss:
                max_loss = price_change
            
            # Detect significant dump after pump
            if max_gain > 0.1 and price_change < max_gain * 0.7:  # Lost 30% of gains
                if time_to_dump is None:
                    time_to_dump = i - start_index
        
        # Classify final outcome
        if max_gain > 0.15:  # Good pump
            outcome = "success"
        elif max_loss < -0.1:  # Significant dump
            outcome = "dump"
        else:
            outcome = "sideways"
        
        return {
            'max_gain_pct': max_gain * 100,
            'max_loss_pct': max_loss * 100,
            'time_to_peak_minutes': time_to_peak,
            'time_to_dump_minutes': time_to_dump,
            'final_outcome': outcome
        }
    
    async def collect_historical_data(self, exchange_name: str, symbol: str, 
                                     timeframe: str = '1m', days_back: int = 30) -> List[Dict]:
        """Collect historical OHLCV data for a symbol"""
        
        if exchange_name not in self.exchanges:
            print(f"❌ Exchange {exchange_name} not initialized")
            return []
        
        exchange = self.exchanges[exchange_name]
        
        try:
            # Calculate how much data we need
            since = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp() * 1000)
            
            print(f"📊 Collecting {days_back} days of {timeframe} data for {symbol} on {exchange_name}")
            
            # Fetch historical data
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
            
            if not ohlcv:
                print(f"⚠️ No data received for {symbol}")
                return []
            
            # Convert to our format
            market_data = []
            for candle in ohlcv:
                market_data.append({
                    'timestamp': datetime.fromtimestamp(candle[0]/1000, tz=timezone.utc).isoformat(),
                    'exchange': exchange_name,
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'open': candle[1],
                    'high': candle[2],
                    'low': candle[3],
                    'close': candle[4],
                    'volume': candle[5],
                    'quote_volume': None  # Not always available
                })
            
            print(f"✅ Collected {len(market_data)} candles for {symbol}")
            return market_data
            
        except Exception as e:
            print(f"❌ Error collecting data for {symbol}: {e}")
            return []
        
        # Rate limiting
        await asyncio.sleep(exchange.rateLimit / 1000)
    
    async def analyze_historical_pumps(self, exchange_name: str, symbol: str, 
                                     ohlcv_data: List[Dict]) -> List[PumpEvent]:
        """Analyze historical data to find pump events"""
        
        if len(ohlcv_data) < 50:  # Need minimum data
            return []
        
        print(f"🔍 Analyzing {len(ohlcv_data)} candles for pumps in {symbol}")
        
        # Extract data arrays
        ohlc_candles = [[
            int(datetime.fromisoformat(d['timestamp']).timestamp() * 1000),
            d['open'], d['high'], d['low'], d['close']
        ] for d in ohlcv_data]
        
        volumes = [d['volume'] for d in ohlcv_data]
        prices = [d['close'] for d in ohlcv_data]
        
        # Calculate RSI
        rsi_values = self.calculate_rsi(prices, self.rsi_period)
        
        # Detect pump events
        pump_events = []
        
        for i in range(20, len(ohlc_candles) - 5):  # Leave space for lookback and outcome calculation
            pump_event = self.detect_historical_pump(ohlc_candles, volumes, rsi_values, i)
            
            if pump_event:
                # Set exchange and symbol
                pump_event.exchange = exchange_name
                pump_event.symbol = symbol
                
                # Calculate outcome
                outcome = self.calculate_pump_outcome(ohlc_candles, i, 24)
                
                # Save to database
                event_data = {
                    'timestamp': pump_event.timestamp,
                    'exchange': pump_event.exchange,
                    'symbol': pump_event.symbol,
                    'event_type': pump_event.event_type,
                    'price': pump_event.price,
                    'volume_multiple': pump_event.volume_multiple,
                    'rsi': pump_event.rsi,
                    'risk_score': pump_event.risk_score,
                    'pump_stage': pump_event.pump_stage,
                    'confidence': pump_event.confidence,
                    'price_change_pct': pump_event.price_change_pct,
                    'volume_profile': pump_event.volume_profile,
                    'price_signature': pump_event.price_signature,
                    'rsi_journey': pump_event.rsi_journey,
                }
                
                event_id = self.db.save_pump_event(event_data)
                
                if event_id:
                    # Update with outcome
                    self.db.update_pump_outcome(event_id, outcome)
                
                pump_events.append(pump_event)
        
        print(f"✅ Found {len(pump_events)} pump events in {symbol}")
        return pump_events
    
    async def collect_symbol_historical(self, exchange_name: str, symbol: str):
        """Complete historical collection and analysis for one symbol"""
        
        try:
            # Collect market data
            market_data = await self.collect_historical_data(
                exchange_name, symbol, self.main_timeframe, self.lookback_days
            )
            
            if not market_data:
                return
            
            # Save market data to database
            self.db.save_market_data(market_data)
            
            # Analyze for pump events
            pump_events = await self.analyze_historical_pumps(exchange_name, symbol, market_data)
            
            print(f"📊 {exchange_name} {symbol}: {len(market_data)} candles, {len(pump_events)} pumps")
            
        except Exception as e:
            print(f"❌ Error processing {exchange_name} {symbol}: {e}")
    
    async def collect_all_historical(self, symbols_per_exchange: Dict[str, List[str]]):
        """Collect historical data for all symbols across exchanges"""
        
        print(f"🚀 Starting historical collection for {sum(len(syms) for syms in symbols_per_exchange.values())} symbols")
        
        total_symbols = 0
        completed_symbols = 0
        
        for exchange_name, symbols in symbols_per_exchange.items():
            if exchange_name not in self.exchanges:
                print(f"⚠️ Skipping {exchange_name} - not initialized")
                continue
            
            total_symbols += len(symbols)
            
            print(f"\n📊 Processing {len(symbols)} symbols on {exchange_name}")
            
            for i, symbol in enumerate(symbols):
                print(f"[{i+1}/{len(symbols)}] Processing {symbol}...")
                
                await self.collect_symbol_historical(exchange_name, symbol)
                completed_symbols += 1
                
                # Progress update
                if (i + 1) % 10 == 0:
                    progress = ((completed_symbols / total_symbols) * 100)
                    print(f"📈 Progress: {completed_symbols}/{total_symbols} ({progress:.1f}%)")
                
                # Rate limiting between symbols
                await asyncio.sleep(2)
        
        print(f"\n✅ Historical collection complete!")
        print(f"📊 Processed {completed_symbols} symbols")
        
        # Print database statistics
        stats = self.db.get_statistics()
        print(f"📈 Database: {stats.get('pump_events_count', 0)} pump events, {stats.get('market_data_count', 0)} market records")
    
    def get_top_volume_symbols(self, exchange_name: str, limit: int = 50) -> List[str]:
        """Get top volume symbols for historical analysis"""
        
        if exchange_name not in self.exchanges:
            return []
        
        try:
            exchange = self.exchanges[exchange_name]
            tickers = exchange.fetch_tickers()
            
            # Filter and sort by volume
            volume_pairs = []
            for symbol, ticker in tickers.items():
                if '/USDT' in symbol and ticker.get('quoteVolume'):
                    try:
                        volume = float(ticker['quoteVolume'])
                        if 200_000 <= volume <= 50_000_000:  # Target range for micro caps
                            volume_pairs.append((symbol, volume))
                    except:
                        continue
            
            # Sort by volume and take top N
            volume_pairs.sort(key=lambda x: x[1], reverse=True)
            return [symbol for symbol, _ in volume_pairs[:limit]]
            
        except Exception as e:
            print(f"❌ Error getting top symbols for {exchange_name}: {e}")
            return []

# Usage example and main execution function
async def main():
    """Main execution function for historical collection"""
    
    # Initialize database
    db = PatternDatabase()
    
    # Exchange configurations
    exchanges_config = {
        'binance': {},
        'bingx': {
            'sandbox': False,
            'timeout': 30000,
        }
    }
    
    # Initialize collector
    collector = HistoricalCollector(db, exchanges_config)
    
    # Get symbols to analyze
    symbols_per_exchange = {}
    for exchange_name in exchanges_config.keys():
        symbols = collector.get_top_volume_symbols(exchange_name, 30)  # Top 30 per exchange
        if symbols:
            symbols_per_exchange[exchange_name] = symbols
            print(f"📊 {exchange_name}: Will analyze {len(symbols)} symbols")
    
    # Start historical collection
    await collector.collect_all_historical(symbols_per_exchange)
    
    # Close database
    db.close()

if __name__ == "__main__":
    asyncio.run(main())
