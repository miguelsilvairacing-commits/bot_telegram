# config.py - Centralized Configuration
import os
from typing import Dict, List

class Config:
    """Centralized configuration management"""
    
    # Exchanges
    EXCHANGES: List[str] = os.getenv("EXCHANGES", "binance,bingx").split(",")
    QUOTE_FILTER: List[str] = os.getenv("QUOTE_FILTER", "USDT").split(",")
    
    # Market filtering
    QV24H_MIN_USD: float = float(os.getenv("QV24H_MIN_USD", "200000"))
    QV24H_MAX_USD: float = float(os.getenv("QV24H_MAX_USD", "20000000"))
    TOP_N_BY_VOLUME: int = int(os.getenv("TOP_N_BY_VOLUME", "30"))
    
    # Detection settings
    TIMEFRAME: str = os.getenv("TIMEFRAME", "1m")
    LOOKBACK: int = int(os.getenv("LOOKBACK", "8"))
    THRESHOLD: float = float(os.getenv("THRESHOLD", "3.0"))
    MIN_RISK_SCORE: int = int(os.getenv("MIN_RISK_SCORE", "4"))
    MIN_PRICE_CHANGE: float = float(os.getenv("MIN_PRICE_CHANGE", "0.04"))
    
    # AI settings
    AI_MODE: bool = os.getenv("AI_MODE", "true").lower() == "true"
    PATTERN_ANALYSIS: bool = os.getenv("PATTERN_ANALYSIS", "true").lower() == "true"
    ML_PREDICTIONS: bool = os.getenv("ML_PREDICTIONS", "true").lower() == "true"
    MANIPULATION_DETECTION: bool = os.getenv("MANIPULATION_DETECTION", "true").lower() == "true"
    
    # RSI
    RSI_OVERBOUGHT: float = float(os.getenv("RSI_OVERBOUGHT", "80"))
    RSI_OVERSOLD: float = float(os.getenv("RSI_OVERSOLD", "20"))
    RSI_PERIOD: int = int(os.getenv("RSI_PERIOD", "14"))
    
    # Timing
    SLEEP_SECONDS: int = int(os.getenv("SLEEP_SECONDS", "30"))
    COOLDOWN_MINUTES: int = int(os.getenv("COOLDOWN_MINUTES", "25"))
    
    # Telegram
    TG_TOKEN: str = os.getenv("TG_TOKEN", "")
    TG_CHAT_ID: str = os.getenv("TG_CHAT_ID", "")
    
    # Debug
    DEBUG_MODE: bool = os.getenv("DEBUG_MODE", "false").lower() == "true"
    
    # Database
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "pattern_data.db")
    
    # Historical collection
    HISTORICAL_DAYS: int = int(os.getenv("HISTORICAL_DAYS", "30"))
    AUTO_COLLECT_HISTORICAL: bool = os.getenv("AUTO_COLLECT_HISTORICAL", "true").lower() == "true"
    
    @classmethod
    def validate(cls) -> bool:
        """Validate critical configuration"""
        if not cls.TG_TOKEN or not cls.TG_CHAT_ID:
            print("⚠️ Warning: Telegram not configured")
            return False
        
        if not cls.EXCHANGES:
            print("❌ Error: No exchanges configured")
            return False
            
        return True
    
    @classmethod
    def print_config(cls):
        """Print current configuration"""
        print("🔧 Current Configuration:")
        print(f"  Exchanges: {cls.EXCHANGES}")
        print(f"  AI Mode: {cls.AI_MODE}")
        print(f"  Pattern Analysis: {cls.PATTERN_ANALYSIS}")
        print(f"  ML Predictions: {cls.ML_PREDICTIONS}")
        print(f"  Volume Threshold: {cls.THRESHOLD}x")
        print(f"  Min Price Change: {cls.MIN_PRICE_CHANGE*100:.1f}%")
        print(f"  Timeframe: {cls.TIMEFRAME}")
        print(f"  Debug Mode: {cls.DEBUG_MODE}")
