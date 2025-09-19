# database.py - Versão sem SQLite para Railway

import os
import json
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime

class SimpleFileDatabase:
    """Database simples usando arquivos JSON para Railway"""
    
    def __init__(self):
        self.data_dir = "data"
        self.ensure_data_dir()
        
    def ensure_data_dir(self):
        """Garante que o diretório de dados existe"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def _get_file_path(self, table_name: str) -> str:
        """Caminho do arquivo para a tabela"""
        return os.path.join(self.data_dir, f"{table_name}.json")
    
    def _load_table(self, table_name: str) -> List[Dict]:
        """Carrega dados de uma tabela"""
        file_path = self._get_file_path(table_name)
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    return json.load(f)
            return []
        except Exception as e:
            print(f"❌ Erro ao carregar {table_name}: {e}")
            return []
    
    def _save_table(self, table_name: str, data: List[Dict]):
        """Salva dados em uma tabela"""
        file_path = self._get_file_path(table_name)
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"❌ Erro ao salvar {table_name}: {e}")
    
    async def save_kline_data(self, symbol: str, timeframe: str, data: List[Dict]):
        """Salva dados de candlestick"""
        try:
            table_name = f"klines_{symbol}_{timeframe}"
            existing_data = self._load_table(table_name)
            
            # Adiciona timestamp se não existir
            for item in data:
                if 'saved_at' not in item:
                    item['saved_at'] = datetime.now().isoformat()
            
            # Combina com dados existentes (evita duplicatas por timestamp)
            existing_timestamps = {item.get('timestamp') for item in existing_data}
            new_data = [item for item in data if item.get('timestamp') not in existing_timestamps]
            
            combined_data = existing_data + new_data
            
            # Mantém apenas os últimos 10000 registros
            if len(combined_data) > 10000:
                combined_data = combined_data[-10000:]
            
            self._save_table(table_name, combined_data)
            print(f"✅ Salvos {len(new_data)} novos registros para {symbol} {timeframe}")
            
        except Exception as e:
            print(f"❌ Erro ao salvar klines: {e}")
    
    async def get_kline_data(self, symbol: str, timeframe: str, limit: Optional[int] = None) -> List[Dict]:
        """Recupera dados de candlestick"""
        try:
            table_name = f"klines_{symbol}_{timeframe}"
            data = self._load_table(table_name)
            
            # Ordena por timestamp
            data.sort(key=lambda x: x.get('timestamp', 0))
            
            if limit:
                data = data[-limit:]
            
            return data
            
        except Exception as e:
            print(f"❌ Erro ao recuperar klines: {e}")
            return []
    
    async def save_analysis_result(self, symbol: str, timeframe: str, analysis: Dict):
        """Salva resultado de análise"""
        try:
            table_name = "analysis_results"
            data = self._load_table(table_name)
            
            analysis_record = {
                'symbol': symbol,
                'timeframe': timeframe,
                'timestamp': datetime.now().isoformat(),
                'analysis': analysis
            }
            
            data.append(analysis_record)
            
            # Mantém apenas os últimos 1000 resultados
            if len(data) > 1000:
                data = data[-1000:]
            
            self._save_table(table_name, data)
            print(f"✅ Análise salva para {symbol} {timeframe}")
            
        except Exception as e:
            print(f"❌ Erro ao salvar análise: {e}")
    
    async def get_latest_analysis(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """Recupera última análise"""
        try:
            table_name = "analysis_results"
            data = self._load_table(table_name)
            
            # Filtra por symbol e timeframe
            filtered_data = [
                item for item in data 
                if item.get('symbol') == symbol and item.get('timeframe') == timeframe
            ]
            
            if filtered_data:
                # Ordena por timestamp e retorna o mais recente
                filtered_data.sort(key=lambda x: x.get('timestamp', ''))
                return filtered_data[-1]
            
            return None
            
        except Exception as e:
            print(f"❌ Erro ao recuperar análise: {e}")
            return None
    
    async def get_all_symbols(self) -> List[str]:
        """Lista todos os símbolos salvos"""
        try:
            symbols = set()
            files = os.listdir(self.data_dir)
            
            for file in files:
                if file.startswith('klines_') and file.endswith('.json'):
                    # Extrai symbol do nome do arquivo: klines_BTCUSDT_1h.json
                    parts = file.replace('klines_', '').replace('.json', '').split('_')
                    if len(parts) >= 2:
                        symbol = '_'.join(parts[:-1])  # Tudo exceto o último (timeframe)
                        symbols.add(symbol)
            
            return list(symbols)
            
        except Exception as e:
            print(f"❌ Erro ao listar símbolos: {e}")
            return []
    
    async def cleanup_old_data(self, days_old: int = 30):
        """Remove dados antigos"""
        try:
            cutoff_date = datetime.now().timestamp() - (days_old * 24 * 60 * 60)
            cleaned_count = 0
            
            files = os.listdir(self.data_dir)
            for file in files:
                if file.endswith('.json'):
                    file_path = self._get_file_path(file.replace('.json', ''))
                    data = self._load_table(file.replace('.json', ''))
                    
                    original_count = len(data)
                    data = [
                        item for item in data 
                        if item.get('timestamp', 0) > cutoff_date * 1000  # Convert to milliseconds
                    ]
                    
                    if len(data) < original_count:
                        self._save_table(file.replace('.json', ''), data)
                        cleaned_count += original_count - len(data)
            
            print(f"✅ Removidos {cleaned_count} registros antigos")
            
        except Exception as e:
            print(f"❌ Erro na limpeza: {e}")

# Instância global
database = SimpleFileDatabase()
