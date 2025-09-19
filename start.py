#!/usr/bin/env python3
# start.py - Startup script with error handling
import sys
import asyncio
import traceback
from datetime import datetime

def print_banner():
    """Print startup banner"""
    print("""
🚀 AI-Enhanced Crypto Trading Bot
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🧬 Pattern DNA Recognition
🤖 Machine Learning Predictions  
⚠️ Market Manipulation Detection
📊 90-Day Historical Analysis
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    """)

async def main():
    """Main startup function with error handling"""
    
    print_banner()
    print(f"🕐 Starting at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    try:
        # Import after banner for better UX
        from config import Config
        from enhanced_bot_main import EnhancedTradingBot
        
        # Validate configuration
        if not Config.validate():
            print("❌ Configuration validation failed")
            sys.exit(1)
        
        # Print configuration
        if Config.DEBUG_MODE:
            Config.print_config()
        
        # Initialize and run bot
        print("🤖 Initializing AI-Enhanced Trading Bot...")
        bot = EnhancedTradingBot()
        await bot.run()
        
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
        sys.exit(0)
        
    except ImportError as e:
        print(f"❌ Import Error: {e}")
        print("📦 Check if all dependencies are installed:")
        print("   pip install -r requirements.txt")
        sys.exit(1)
        
    except Exception as e:
        print(f"❌ Fatal Error: {e}")
        print("\n🔍 Full traceback:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
