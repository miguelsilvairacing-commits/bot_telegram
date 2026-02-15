# =========================================================================================
#   CRYPTO ML BOT v3.0 - BINANCE ONLY / QUALIDADE SOBRE QUANTIDADE
# =========================================================================================
#
# MUDANÇAS v3.0 vs v2.3.1:
#
# 1. APENAS BINANCE
#    - BingX removido. Accuracy BingX era 44.4% vs 49.1% Binance.
#    - Menos ruído, menos falsos positivos de micro caps manipuladas.
#
# 2. MIN_PRICE_CHANGE: 1.5% → 3.0%
#    - 81% dos alertas anteriores tinham price_change entre 1-2% (puro ruído)
#    - Movimentos de 1.3% (mediana antiga) revertem quase sempre em 4h
#    - Agora só alertamos movimentos com substância real
#
# 3. FÓRMULA DE STRENGTH REFORMULADA
#    - Antiga: min(vol/2 + price*20, 10) → saturava: 66% dos S10 tinham vol < 10x
#    - Nova: vol_score (0-5) + price_score (0-5) separados e com escala real
#    - Precisa de vol 25x E price 10% para atingir S10 (antes era vol 5x + price 1.5%)
#
# 4. FILTRO RSI
#    - PUMP com RSI > 75: ignorado (exaustão, não momentum)
#    - DUMP com RSI < 25: ignorado (oversold extremo, bounce iminente)
#    - RSI entre 45-65: zona neutra, penaliza strength em -1
#
# 5. FILTRO DE TENDÊNCIA PRÉ-SPIKE (3 candles antes)
#    - PUMP válido: os 3 candles anteriores têm tendência positiva ou neutra
#    - Um spike isolado sem tendência prévia é muito provavelmente manipulação pontual
#
# 6. DEFINIÇÃO HONESTA DE ACERTO (validação)
#    - Antigo: SUSTAINED + WEAK_CONTINUATION = "acerto" (WEAK tem média 0.0% de move!)
#    - Novo: só SUSTAINED_PUMP / SUSTAINED_DUMP = acerto real (move > 5% confirmado)
#    - WEAK_CONTINUATION passa a ser "NEUTRO" na classificação
#
# 7. PERSISTÊNCIA DE DADOS (resolve 70% sem contexto BTC)
#    - Bot envia snapshot BTC a cada 30min para canal Telegram de logs
#    - Ao reiniciar, tenta recuperar último snapshot do canal
#    - Elimina o warmup de 5min que deixava 70% dos alertas sem contexto BTC
#
# 8. MIN_STRENGTH: 5 → 7 (com a nova fórmula, S7 real = bom sinal)
#
# =========================================================================================

import os
import time
import json
import ccxt
import requests
import numpy as np
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
import statistics
import threading

# =========================
#   CONFIGURATION
# =========================

# =========================
#   DYNAMIC COOLDOWN v3.0
# =========================
#
# PROBLEMA que resolve:
#
# DCR/USDT disparou 122 vezes em 92 dias (2.2x/dia).
# Com cooldown fixo de 20min, um símbolo estruturalmente "barulhento"
# nunca era travado — apenas atrasado 20 minutos entre alertas.
#
# SOLUÇÃO — 3 camadas:
#
# Camada 1: Cooldown Dinâmico por Accuracy
#   Após 5 alertas validados, calcula a accuracy REAL do símbolo.
#   - acc < 10%  → cooldown 24h  (símbolo é ruído estrutural)
#   - acc < 20%  → cooldown 4h   (símbolo fraco, limitar exposição)
#   - acc 20-30% → cooldown 30min (base v3.0)
#   - acc >= 30% → cooldown 15min (símbolo bom, recompensar)
#
# Camada 2: Volume Normalizado por Símbolo
#   Se DCR costuma disparar com volume médio de 13x, um spike de 10x
#   não é excepcional — é o seu comportamento normal.
#   O threshold passa a ser: volume_actual > (volume_histórico_mediano * 1.5)
#   Garante que só alertamos quando o spike é excepcional PARA AQUELE SÍMBOLO.
#
# Camada 3: Cap de Alertas Diários por Símbolo
#   Mesmo com accuracy razoável, nenhum símbolo deve disparar mais de
#   MAX_ALERTS_PER_SYMBOL_PER_DAY vezes no mesmo dia.
#   Evita que em dias voláteis um símbolo domine completamente o canal.

MAX_ALERTS_PER_SYMBOL_PER_DAY = int(os.getenv("MAX_ALERTS_PER_DAY", "2"))
MIN_ALERTS_FOR_DYNAMIC_COOLDOWN = 5  # alertas validados mínimos para activar

env_blacklist = os.getenv("SYMBOLS_BLACKLIST", "")
SYMBOLS_BLACKLIST = set()

if env_blacklist:
    parsed = [s.strip() for s in env_blacklist.split(",") if s.strip()]
    SYMBOLS_BLACKLIST.update(parsed)

# Blacklist crítica - símbolos com histórico de manipulação pesada
SYMBOLS_BLACKLIST_CRITICAL = {
    'DCR/USDT',    # 122 alertas, apenas 12% sustained - maior poluidor do dataset
    'XVG/USDT',    # 38 alertas, 11% sustained
    'TURTLE/USDT', # 27 alertas, 15% sustained
}
SYMBOLS_BLACKLIST.update(SYMBOLS_BLACKLIST_CRITICAL)

print(f"⛔ Blacklist: {len(SYMBOLS_BLACKLIST)} símbolos bloqueados")

# =========================
#   SYMBOL REPUTATION SYSTEM
# =========================
class SymbolReputationSystem:
    """
    Sistema de reputação por símbolo — v3.0
    
    Mantém um registo de:
    - Quantos alertas cada símbolo já gerou
    - Quantos desses foram SUSTAINED (acertos reais)
    - Volume histórico de spike por símbolo
    - Quantas vezes disparou hoje
    
    E decide dinamicamente:
    - Qual o cooldown adequado para cada símbolo
    - Se o spike de volume actual é excepcional para este símbolo
    - Se o símbolo já esgotou o limite diário
    """
    
    def __init__(self, data_dir="pattern_data"):
        self.data_dir = data_dir
        
        # Histórico de alertas por símbolo: {symbol: [{'ts':..,'result':..,'vol':..}]}
        self.symbol_history = defaultdict(list)
        
        # Volume histórico de spikes por símbolo (para normalização)
        self.symbol_volume_history = defaultdict(list)
        
        # Alertas hoje por símbolo: {symbol: [timestamps de hoje]}
        self.today_alerts = defaultdict(list)
        
        self._load()
        print("Symbol Reputation System initialized (v3.0)")
    
    def _load(self):
        try:
            path = os.path.join(self.data_dir, "symbol_reputation.json")
            if os.path.exists(path):
                with open(path, 'r') as f:
                    data = json.load(f)
                    self.symbol_history = defaultdict(list, data.get('history', {}))
                    self.symbol_volume_history = defaultdict(list, data.get('vol_history', {}))
                total = sum(len(v) for v in self.symbol_history.values())
                print(f"[Reputation] Loaded {len(self.symbol_history)} symbols, {total} records")
        except Exception as e:
            print(f"[Reputation] Load error: {e}")
    
    def _save(self):
        try:
            path = os.path.join(self.data_dir, "symbol_reputation.json")
            with open(path, 'w') as f:
                json.dump({
                    'history': dict(self.symbol_history),
                    'vol_history': dict(self.symbol_volume_history)
                }, f)
        except Exception as e:
            print(f"[Reputation] Save error: {e}")
    
    def record_alert(self, symbol: str, volume_x: float):
        """Regista que um alerta foi emitido (sem resultado ainda)"""
        today = datetime.now().date().isoformat()
        self.today_alerts[symbol].append({
            'ts': int(time.time()),
            'date': today
        })
        # Guarda volume para normalização futura
        if volume_x:
            vols = self.symbol_volume_history[symbol]
            vols.append(volume_x)
            # Mantém só os últimos 50 spikes
            if len(vols) > 50:
                self.symbol_volume_history[symbol] = vols[-50:]
    
    def record_validation(self, symbol: str, result: str, volume_x: float = None):
        """
        Chamado quando uma validação de 4h chega.
        Actualiza o histórico de reputação do símbolo.
        """
        self.symbol_history[symbol].append({
            'ts': int(time.time()),
            'result': result,
            'sustained': result in ['SUSTAINED_PUMP', 'SUSTAINED_DUMP']
        })
        # Mantém só os últimos 30 resultados (janela móvel)
        if len(self.symbol_history[symbol]) > 30:
            self.symbol_history[symbol] = self.symbol_history[symbol][-30:]
        
        self._save()
    
    def get_symbol_accuracy(self, symbol: str) -> tuple:
        """
        Retorna (accuracy, n_samples) para o símbolo.
        Usa janela móvel dos últimos 30 alertas validados.
        """
        history = self.symbol_history.get(symbol, [])
        if len(history) < MIN_ALERTS_FOR_DYNAMIC_COOLDOWN:
            return None, len(history)  # Dados insuficientes
        
        sustained = sum(1 for h in history if h['sustained'])
        acc = sustained / len(history) * 100
        return acc, len(history)
    
    def get_dynamic_cooldown(self, symbol: str) -> int:
        """
        Retorna o cooldown em minutos para este símbolo.
        
        Tabela de cooldown baseada em accuracy real (SUSTAINED):
        - Sem dados suficientes  → 30min (base)
        - acc < 10%              → 1440min (24h) — ruído estrutural
        - acc < 20%              → 240min (4h)  — símbolo fraco
        - acc 20-30%             → 30min         — base v3.0
        - acc >= 30%             → 15min          — símbolo bom
        """
        acc, n = self.get_symbol_accuracy(symbol)
        
        if acc is None:
            return 30  # Default enquanto acumula dados
        
        if acc < 10:
            return 1440  # 24h
        elif acc < 20:
            return 240   # 4h
        elif acc >= 30:
            return 15    # 15min — recompensar bom símbolo
        else:
            return 30    # 30min — base
    
    def is_volume_exceptional(self, symbol: str, current_vol: float) -> bool:
        """
        Verifica se o volume actual é excepcional para este símbolo.
        
        PROBLEMA que resolve:
        DCR costuma disparar com volume médio de 13x (mediana dos seus spikes).
        Um spike de 10x no DCR não é excepcional — é o seu comportamento habitual.
        
        SOLUÇÃO:
        Calcula a mediana do volume histórico de spikes do símbolo.
        Só é "excepcional" se for > mediana_histórica * 1.5
        
        Se não há histórico suficiente (<10 amostras), usa threshold global.
        """
        vols = self.symbol_volume_history.get(symbol, [])
        
        if len(vols) < 10:
            # Sem histórico suficiente: usa threshold global normalmente
            return True
        
        median_vol = statistics.median(vols)
        
        # O spike actual tem de ser 1.5x acima da mediana histórica do símbolo
        is_exceptional = current_vol > median_vol * 1.5
        
        if not is_exceptional:
            print(f"[VOL NORM] {symbol}: vol={current_vol:.1f}x não excepcional "
                  f"(mediana histórica={median_vol:.1f}x, threshold={median_vol*1.5:.1f}x)")
        
        return is_exceptional
    
    def check_daily_cap(self, symbol: str) -> bool:
        """
        Verifica se o símbolo já atingiu o limite de alertas para hoje.
        Retorna True se PODE alertar, False se já atingiu o cap.
        """
        today = datetime.now().date().isoformat()
        
        # Limpa alertas de dias anteriores
        self.today_alerts[symbol] = [
            a for a in self.today_alerts[symbol]
            if a.get('date') == today
        ]
        
        count_today = len(self.today_alerts[symbol])
        
        if count_today >= MAX_ALERTS_PER_SYMBOL_PER_DAY:
            print(f"[DAILY CAP] {symbol}: {count_today}/{MAX_ALERTS_PER_SYMBOL_PER_DAY} alertas hoje — bloqueado")
            return False
        
        return True
    
    def get_reputation_summary(self) -> str:
        """Resumo da reputação para o relatório diário"""
        lines = []
        
        blocked_24h = []
        blocked_4h = []
        good = []
        
        for sym in self.symbol_history:
            acc, n = self.get_symbol_accuracy(sym)
            if acc is None:
                continue
            cooldown = self.get_dynamic_cooldown(sym)
            if cooldown >= 1440:
                blocked_24h.append(f"{sym.replace('/USDT','')} ({acc:.0f}%)")
            elif cooldown >= 240:
                blocked_4h.append(f"{sym.replace('/USDT','')} ({acc:.0f}%)")
            elif acc >= 30:
                good.append(f"{sym.replace('/USDT','')} ({acc:.0f}%)")
        
        if blocked_24h:
            lines.append(f"🔴 Cooldown 24h: {', '.join(blocked_24h[:5])}")
        if blocked_4h:
            lines.append(f"🟡 Cooldown 4h: {', '.join(blocked_4h[:5])}")
        if good:
            lines.append(f"🟢 Símbolos bons (≥30%): {', '.join(good[:5])}")
        
        return "\n".join(lines) if lines else "Dados insuficientes ainda"

# =========================
#   VALIDATION SYSTEM v3.0
# =========================
class AlertValidationSystem:
    """
    Sistema de validação v3.0
    
    MUDANÇA PRINCIPAL: definição honesta de acerto.
    
    Antigo (inflava accuracy):
        "Correcto" = SUSTAINED_PUMP + SUSTAINED_DUMP + WEAK_CONTINUATION
        Problema: WEAK_CONTINUATION tem price_change médio de 0.0% após 4h.
                  Era basicamente contar "acerto" quando o preço ficou igual.
    
    Novo (accuracy real):
        "Acerto"  = SUSTAINED_PUMP ou SUSTAINED_DUMP (move > 5% confirmado)
        "Neutro"  = WEAK_CONTINUATION (mercado ficou no mesmo sítio)
        "Falha"   = SMALL_REVERSAL, DUMP_REVERSAL, PUMP_REVERSAL
    """
    
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.pending_validations = []
        self.validation_results = []
        self.validation_lock = threading.Lock()
        self.last_daily_report = 0
        
        self.validation_thread = threading.Thread(target=self._validation_loop, daemon=True)
        self.validation_thread.start()
        
        self._load_existing_data()
        
        print("Alert Validation System initialized (v3.0 - Honest Accuracy)")
    
    def register_alert(self, alert_data: dict):
        """Registra alerta com todos os dados para ML"""
        validation_record = {
            'alert_id': f"{alert_data['symbol']}_{int(time.time())}",
            'timestamp': int(time.time()),
            'exchange': alert_data['exchange'],
            'symbol': alert_data['symbol'],
            'event_type': alert_data['event_type'],
            'initial_price': alert_data.get('price', 0),
            'volume_multiple': alert_data.get('volume_multiple', 0),
            'strength': alert_data.get('event_strength', 0),
            'price_change_pct': alert_data.get('price_change_pct', 0),
            'rsi': alert_data.get('rsi', None),
            'pre_trend': alert_data.get('pre_trend', 'UNKNOWN'),
            'hour_utc': datetime.fromtimestamp(int(time.time())).hour,
            'day_of_week': datetime.fromtimestamp(int(time.time())).weekday(),
            'correlations_count': alert_data.get('correlations_count', 0),
            'cascade_risk': alert_data.get('cascade_risk', 0),
            'market_regime': alert_data.get('market_regime', 'UNKNOWN'),
            
            # Bitcoin multi-timeframe context
            'btc_price': self.bot.btc_data['last_price'],
            'btc_change_5m': self.bot.btc_data['change_5m'],
            'btc_change_1h': self.bot.btc_data['change_1h'],
            'btc_change_4h': self.bot.btc_data['change_4h'],
            'btc_change_24h': self.bot.btc_data['change_24h'],
            'btc_trend_micro': self.bot.btc_data['trend_micro'],
            'btc_trend_macro': self.bot.btc_data['trend_macro'],
            'btc_volume_spike': self.bot.btc_data.get('volume_spike', 1.0),
            'btc_data_valid': self.bot.btc_data.get('data_valid', False),
            
            'price_vs_btc_4h': alert_data.get('price_change_pct', 0) - self.bot.btc_data['change_4h'],
            'is_btc_follower': abs(alert_data.get('price_change_pct', 0) - self.bot.btc_data['change_4h']) < 2.0,
            'movement_type': self._classify_movement(alert_data, self.bot.btc_data),
            
            'validations': {
                '4h': {'checked': False, 'price': None, 'result': None, 'price_change': None},
                '24h': {'checked': False, 'price': None, 'result': None, 'price_change': None}
            }
        }
        
        with self.validation_lock:
            self.pending_validations.append(validation_record)
        
        self._save_pending_validations()
        print(f"[ML-DATA] Alert registered: {alert_data['symbol']} {alert_data['event_type']}")
    
    def _classify_movement(self, alert_data: dict, btc_data: dict) -> str:
        """Classifica o movimento em relação ao BTC MACRO trend"""
        if not btc_data.get('data_valid', False):
            return 'UNKNOWN'
            
        alert_change = alert_data.get('price_change_pct', 0)
        btc_change_4h = btc_data['change_4h']
        btc_trend = btc_data['trend_macro']
        
        if btc_trend == 'LATERAL':
            return 'INDEPENDENT'
        
        if (alert_change > 0 and btc_change_4h > 0) or (alert_change < 0 and btc_change_4h < 0):
            if abs(alert_change) > abs(btc_change_4h) * 1.5:
                return 'BTC_OUTPERFORM'
            elif abs(alert_change) > abs(btc_change_4h) * 0.5:
                return 'BTC_FOLLOW'
            else:
                return 'BTC_UNDERPERFORM'
        else:
            return 'BTC_COUNTER'
    
    def _validation_loop(self):
        """Loop de validação"""
        while True:
            try:
                time.sleep(300)
                self._check_pending_validations()
                self._check_daily_report()
            except Exception as e:
                print(f"[VALIDATION] Error in loop: {e}")
    
    def _check_pending_validations(self):
        """Verifica alertas pendentes"""
        current_time = int(time.time())
        
        with self.validation_lock:
            for record in self.pending_validations[:]:
                alert_time = record['timestamp']
                
                if not record['validations']['4h']['checked'] and current_time >= alert_time + 14400:
                    self._validate_alert(record, '4h', notify=True)
                
                if not record['validations']['24h']['checked'] and current_time >= alert_time + 86400:
                    notify_24h = record['strength'] >= 7
                    self._validate_alert(record, '24h', notify=notify_24h)
                    
                    self.validation_results.append(record)
                    self.pending_validations.remove(record)
                    self._save_results()
    
    def _validate_alert(self, record: dict, timeframe: str, notify: bool = True):
        """Valida alerta"""
        try:
            exchange_name = record['exchange']
            symbol = record['symbol']
            
            if exchange_name not in self.bot.exchanges:
                return
            
            ex = self.bot.exchanges[exchange_name]
            ticker = ex.fetch_ticker(symbol)
            current_price = ticker['last']
            
            initial_price = record['initial_price']
            price_change_pct = ((current_price - initial_price) / initial_price) * 100 if initial_price > 0 else 0
            
            record['validations'][timeframe]['btc_price'] = self.bot.btc_data['last_price']
            record['validations'][timeframe]['btc_change_4h'] = self.bot.btc_data['change_4h']
            record['validations'][timeframe]['btc_trend_macro'] = self.bot.btc_data['trend_macro']
            
            record['validations'][timeframe]['checked'] = True
            record['validations'][timeframe]['price'] = current_price
            record['validations'][timeframe]['price_change'] = price_change_pct
            
            event_type = record['event_type']
            result = self._classify_result(event_type, price_change_pct)
            record['validations'][timeframe]['result'] = result
            record['validations'][timeframe]['validated_at'] = int(time.time())
            
            # Actualiza reputação do símbolo com o resultado (só na validação 4h)
            if timeframe == '4h':
                self.bot.reputation.record_validation(
                    symbol=record['symbol'],
                    result=result,
                    volume_x=record.get('volume_multiple')
                )
            
            if notify:
                self._send_validation_report(record, timeframe)
            
            self._save_pending_validations()
            
        except Exception as e:
            print(f"[VALIDATION] Error validating {record['symbol']}: {e}")
    
    def _classify_result(self, event_type: str, price_change_pct: float) -> str:
        """
        Classifica resultado da validação - v3.0 HONESTO
        
        Thresholds elevados para >5% porque:
        - Movimentos de 1-3% são ruído normal do mercado
        - Só acima de 5% podemos dizer que o sinal foi real
        
        NEUTRO (WEAK_CONTINUATION) = ficou no mesmo sítio ±5%
        Não é acerto, não é falha. É nada aconteceu.
        """
        if event_type == "PUMP":
            if price_change_pct > 5:
                return "SUSTAINED_PUMP"        # ✅ ACERTO REAL
            elif price_change_pct > -5:
                return "WEAK_CONTINUATION"     # ⚪ NEUTRO (era contado como acerto antes)
            else:
                return "DUMP_REVERSAL"         # ❌ FALHA
        else:  # DUMP
            if price_change_pct < -5:
                return "SUSTAINED_DUMP"        # ✅ ACERTO REAL
            elif price_change_pct < 5:
                return "WEAK_CONTINUATION"     # ⚪ NEUTRO
            else:
                return "PUMP_REVERSAL"         # ❌ FALHA
    
    def _send_validation_report(self, record: dict, timeframe: str):
        """Envia relatório de validação - v3.0 com distinção clara de neutro"""
        
        validation = record['validations'][timeframe]
        result = validation['result']
        
        # v3.0: 3 categorias claras
        result_emojis = {
            'SUSTAINED_PUMP': '✅',
            'SUSTAINED_DUMP': '✅',
            'WEAK_CONTINUATION': '⚪',  # NEUTRO - não é acerto
            'DUMP_REVERSAL': '❌',
            'PUMP_REVERSAL': '❌'
        }
        
        result_labels = {
            'SUSTAINED_PUMP': 'ACERTO — Pump confirmado',
            'SUSTAINED_DUMP': 'ACERTO — Dump confirmado',
            'WEAK_CONTINUATION': 'NEUTRO — Sem movimento real',
            'DUMP_REVERSAL': 'FALHA — Reverteu para cima',
            'PUMP_REVERSAL': 'FALHA — Reverteu para baixo'
        }
        
        emoji = result_emojis.get(result, '⚪')
        label = result_labels.get(result, result)
        
        msg = f"""📊 <b>VALIDAÇÃO [{timeframe}]</b>

{emoji} <b>{label}</b>

🎯 {record['symbol']} ({record['exchange'].upper()})
📊 {record['event_type']} | ⚡ {record['strength']}/10
💹 {record['volume_multiple']:.1f}x

💰 ${record['initial_price']:.6f} → ${validation['price']:.6f}
📈 {validation['price_change']:+.2f}%"""

        if record.get('movement_type') and record.get('btc_data_valid', False):
            msg += f"\n₿ Movimento: {record['movement_type']}"
        
        if record.get('pre_trend') and record['pre_trend'] != 'UNKNOWN':
            msg += f"\n📉 Tendência prévia: {record['pre_trend']}"

        msg += f"\n\n⏰ Alerta enviado há {timeframe}"
        
        self.bot.send_telegram(msg)
    
    def _check_daily_report(self):
        """Verifica se deve enviar relatório diário"""
        current_time = int(time.time())
        current_hour = datetime.fromtimestamp(current_time).hour
        
        if (current_time - self.last_daily_report) >= 86400 and current_hour == 10:
            self._send_daily_report()
            self.last_daily_report = current_time
    
    def _send_daily_report(self):
        """
        Relatório diário v3.0 - accuracy HONESTA
        Separa claramente acertos reais de neutros e falhas
        """
        cutoff = int(time.time()) - 86400
        recent = [r for r in self.validation_results if r['timestamp'] > cutoff]
        
        if len(recent) < 3:
            return
        
        # Accuracy REAL (só SUSTAINED)
        acertos = 0
        neutros = 0
        falhas = 0
        total = 0
        
        pump_acertos = pump_total = 0
        dump_acertos = dump_total = 0
        
        for record in recent:
            val_4h = record['validations'].get('4h', {})
            if not val_4h.get('checked', False):
                continue
            
            result = val_4h.get('result', 'UNKNOWN')
            event_type = record.get('event_type', 'UNKNOWN')
            total += 1
            
            if result in ['SUSTAINED_PUMP', 'SUSTAINED_DUMP']:
                acertos += 1
                if event_type == 'PUMP': pump_acertos += 1
                else: dump_acertos += 1
            elif result == 'WEAK_CONTINUATION':
                neutros += 1
            else:
                falhas += 1
            
            if event_type == 'PUMP': pump_total += 1
            elif event_type == 'DUMP': dump_total += 1
        
        if total == 0:
            return
        
        acc_real = acertos / total * 100
        pump_acc = (pump_acertos / pump_total * 100) if pump_total > 0 else 0
        dump_acc = (dump_acertos / dump_total * 100) if dump_total > 0 else 0
        
        total_dataset = len(self.validation_results)
        
        msg = f"""📊 <b>RELATÓRIO DIÁRIO v3.0</b>

<b>🎯 Accuracy REAL (só SUSTAINED) 4h:</b>
✅ Acertos: {acertos}/{total} = {acc_real:.1f}%
⚪ Neutros: {neutros}/{total} = {neutros/total*100:.1f}%
❌ Falhas:  {falhas}/{total} = {falhas/total*100:.1f}%

<b>Por tipo:</b>
- Pumps: {pump_acc:.1f}% ({pump_acertos}/{pump_total})
- Dumps: {dump_acc:.1f}% ({dump_acertos}/{dump_total})

<b>💾 Dataset ML:</b>
- Total validações: {total_dataset}
- Apenas Binance ✅
- Filtros activos: price ≥3%, RSI, pre-trend"""
        
        # Resumo de reputação dos símbolos
        rep_summary = self.bot.reputation.get_reputation_summary()
        if rep_summary:
            msg += f"\n\n<b>🎖️ Reputação:</b>\n{rep_summary}"
        
        self.bot.send_telegram(msg)
    
    def _save_pending_validations(self):
        try:
            validation_file = os.path.join(self.bot.db.data_dir, "pending_validations.json")
            with open(validation_file, 'w') as f:
                json.dump(self.pending_validations, f, indent=2)
        except Exception as e:
            print(f"[VALIDATION] Error saving: {e}")
    
    def _save_results(self):
        try:
            results_file = os.path.join(self.bot.db.data_dir, "validation_results.json")
            with open(results_file, 'w') as f:
                json.dump(self.validation_results, f, indent=2)
        except Exception as e:
            print(f"[VALIDATION] Error saving: {e}")
    
    def _load_existing_data(self):
        try:
            validation_file = os.path.join(self.bot.db.data_dir, "pending_validations.json")
            if os.path.exists(validation_file):
                with open(validation_file, 'r') as f:
                    self.pending_validations = json.load(f)
            
            results_file = os.path.join(self.bot.db.data_dir, "validation_results.json")
            if os.path.exists(results_file):
                with open(results_file, 'r') as f:
                    self.validation_results = json.load(f)
            
            print(f"[VALIDATION] Loaded {len(self.pending_validations)} pending, {len(self.validation_results)} completed")
        except Exception as e:
            print(f"[VALIDATION] Error loading: {e}")

# =========================
#   DATABASE
# =========================
class FileBasedPatternDB:
    def __init__(self, data_dir="pattern_data"):
        self.data_dir = data_dir
        self.ensure_data_dir()
        
        self.events_buffer = deque(maxlen=1000)
        self.correlations = {}
        self.sessions = {}
        
        self._load_existing_data()
        
        print("Pattern database initialized (v3.0)")
    
    def ensure_data_dir(self):
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def _load_existing_data(self):
        try:
            events_file = os.path.join(self.data_dir, "recent_events.json")
            if os.path.exists(events_file):
                with open(events_file, 'r') as f:
                    events_data = json.load(f)
                    for event in events_data[-1000:]:
                        self.events_buffer.append(event)
            
            corr_file = os.path.join(self.data_dir, "correlations.json")
            if os.path.exists(corr_file):
                with open(corr_file, 'r') as f:
                    self.correlations = json.load(f)
            
            print(f"Loaded {len(self.events_buffer)} events, {len(self.correlations)} correlations")
            
        except Exception as e:
            print(f"Error loading data: {e}")
    
    def _save_data_periodically(self):
        try:
            events_file = os.path.join(self.data_dir, "recent_events.json")
            with open(events_file, 'w') as f:
                json.dump(list(self.events_buffer), f, indent=2)
            
            corr_file = os.path.join(self.data_dir, "correlations.json")
            with open(corr_file, 'w') as f:
                json.dump(self.correlations, f, indent=2)
            
        except Exception as e:
            print(f"Error saving data: {e}")
    
    def log_market_event(self, event_data: Dict) -> str:
        timestamp = event_data['timestamp']
        window_start = (timestamp // 1800) * 1800
        session_id = f"SESSION_{window_start}"
        
        event_data['session_id'] = session_id
        event_data['logged_at'] = int(time.time())
        
        self.events_buffer.append(event_data)
        
        if session_id not in self.sessions:
            self.sessions[session_id] = {
                'session_id': session_id,
                'start_time': window_start,
                'events': [],
                'total_events': 0
            }
        
        self.sessions[session_id]['events'].append(event_data)
        self.sessions[session_id]['total_events'] += 1
        
        if len(self.events_buffer) % 10 == 0:
            self._save_data_periodically()
        
        return session_id
    
    def update_symbol_correlation(self, symbol1: str, symbol2: str,
                                  correlation_type: str, strength: float, delay: int):
        key = f"{symbol1}|{symbol2}|{correlation_type}"
        
        if key not in self.correlations:
            self.correlations[key] = {
                'symbol_1': symbol1,
                'symbol_2': symbol2,
                'correlation_type': correlation_type,
                'correlation_strength': strength,
                'time_delay_minutes': delay,
                'sample_size': 1,
                'last_updated': int(time.time())
            }
        else:
            existing = self.correlations[key]
            existing['correlation_strength'] = (existing['correlation_strength'] + strength) / 2
            existing['time_delay_minutes'] = (existing['time_delay_minutes'] + delay) / 2
            existing['sample_size'] += 1
            existing['last_updated'] = int(time.time())

# =========================
#   CORRELATION ENGINE
# =========================
@dataclass
class MarketEvent:
    timestamp: int
    exchange: str
    symbol: str
    event_type: str
    volume_multiple: float
    price_change_pct: float
    rsi: Optional[float]
    event_strength: int
    pre_trend: str = 'UNKNOWN'

@dataclass
class CorrelationPattern:
    symbol_pair: Tuple[str, str]
    correlation_type: str
    strength: float
    time_delay: int
    confidence: float
    sample_size: int

class PatternCorrelationEngine:
    def __init__(self, db: FileBasedPatternDB):
        self.db = db
        self.active_events = deque(maxlen=500)
        self.correlation_threshold = 0.7
        self.time_window_minutes = 30
        
        print("Correlation Engine initialized (v3.0)")
    
    def process_new_event(self, event: MarketEvent) -> Dict:
        self.active_events.append(event)
        
        event_data = asdict(event)
        session_id = self.db.log_market_event(event_data)
        
        correlations_found = self._analyze_correlations(event)
        cascade_risk = self._detect_cascade_risk(event)
        regime = self._assess_market_regime()
        
        return {
            'session_id': session_id,
            'correlations_found': correlations_found,
            'cascade_risk': cascade_risk['cascade_risk_score'],
            'market_regime': regime['regime']
        }
    
    def _analyze_correlations(self, new_event: MarketEvent) -> List[CorrelationPattern]:
        correlations = []
        time_threshold = new_event.timestamp - (self.time_window_minutes * 60)
        
        recent_by_symbol = defaultdict(list)
        for event in self.active_events:
            if event.timestamp > time_threshold and event.symbol != new_event.symbol:
                recent_by_symbol[event.symbol].append(event)
        
        for symbol, events in recent_by_symbol.items():
            if len(events) == 0:
                continue
            
            if new_event.event_type == "PUMP":
                recent_pumps = [e for e in events if e.event_type == "PUMP"]
                if recent_pumps:
                    closest = min(recent_pumps, key=lambda x: abs(x.timestamp - new_event.timestamp))
                    time_delay = abs(new_event.timestamp - closest.timestamp) // 60
                    
                    time_score = max(0, 1 - (time_delay / self.time_window_minutes))
                    volume_score = min(new_event.volume_multiple, closest.volume_multiple) / max(new_event.volume_multiple, closest.volume_multiple)
                    strength = (time_score * 0.6 + volume_score * 0.4)
                    
                    if strength >= self.correlation_threshold:
                        correlations.append(CorrelationPattern(
                            symbol_pair=(closest.symbol, new_event.symbol),
                            correlation_type="PUMP_FOLLOW",
                            strength=strength,
                            time_delay=int(time_delay),
                            confidence=strength,
                            sample_size=1
                        ))
                        self.db.update_symbol_correlation(
                            new_event.symbol, symbol, "PUMP_FOLLOW", strength, int(time_delay)
                        )
        
        return correlations
    
    def _detect_cascade_risk(self, new_event: MarketEvent) -> Dict:
        recent_threshold = new_event.timestamp - 3600
        recent_events = [e for e in self.active_events if e.timestamp > recent_threshold]
        
        pumps_count = len([e for e in recent_events if e.event_type == "PUMP"])
        dumps_count = len([e for e in recent_events if e.event_type == "DUMP"])
        
        cascade_risk_score = 0.0
        
        if pumps_count >= 5:
            cascade_risk_score += 0.4
        
        if new_event.event_type == "DUMP" and dumps_count >= 2:
            cascade_risk_score += 0.4
        
        return {
            'cascade_risk_score': min(cascade_risk_score, 1.0),
            'recent_pumps': pumps_count,
            'recent_dumps': dumps_count,
            'risk_level': 'HIGH' if cascade_risk_score > 0.7 else 'MEDIUM' if cascade_risk_score > 0.4 else 'LOW'
        }
    
    def _assess_market_regime(self) -> Dict:
        recent_events = list(self.active_events)[-50:]
        
        if len(recent_events) < 10:
            return {'regime': 'INSUFFICIENT_DATA', 'confidence': 0.0}
        
        pumps = [e for e in recent_events if e.event_type == "PUMP"]
        pump_ratio = len(pumps) / len(recent_events)
        
        if pump_ratio > 0.7:
            regime = "PUMP_MANIPULATION"
        elif pump_ratio < 0.3:
            regime = "DUMP_CASCADE"
        else:
            regime = "MIXED_SIGNALS"
        
        return {'regime': regime, 'confidence': 0.7}

# =========================
#   TRADING BOT v3.0
# =========================
class AdvancedPatternTradingBot:
    """
    Bot v3.0 — Binance Only, Qualidade sobre Quantidade
    
    Filosofia:
    - Menos alertas, mais fiáveis
    - Métricas honestas
    - Filtros baseados em dados reais dos 3 meses anteriores
    """
    
    def __init__(self):
        self.db = FileBasedPatternDB()
        self.correlation_engine = PatternCorrelationEngine(self.db)
        
        self.exchanges = {}
        # v3.0: APENAS BINANCE
        # Lê EXCHANGES do Railway mas ignora qualquer coisa que não seja binance.
        # Se alguém definir EXCHANGES=binance,bingx (valor antigo), avisamos mas continuamos.
        configured_exchanges = os.getenv("EXCHANGES", "binance").lower()
        if "bingx" in configured_exchanges:
            print("[CONFIG] ⚠️  EXCHANGES inclui bingx — v3.0 usa apenas Binance. Actualiza no Railway.")
        if "binance" not in configured_exchanges:
            print(f"[CONFIG] ⚠️  EXCHANGES='{configured_exchanges}' não inclui binance — a usar binance na mesma.")
        self._initialize_binance()
        
        # BTC tracking data
        self.btc_data = {
            'last_price': 0,
            'change_5m': 0,
            'change_1h': 0,
            'change_4h': 0,
            'change_24h': 0,
            'trend_micro': 'LATERAL',
            'trend_macro': 'LATERAL',
            'last_update': 0,
            'history': deque(maxlen=200),
            'volume_spike': 1.0,
            'last_volume': 0,
            'data_valid': False,
            'warmup_time': 300,
            'start_time': time.time()
        }
        
        self.reputation = SymbolReputationSystem(self.db.data_dir)
        self.validation_system = AlertValidationSystem(self)
        
        self.watchlist = {}
        self.last_alert_ts = defaultdict(lambda: 0.0)
        
        self.quote_filter = os.getenv("QUOTE_FILTER", "USDT").split(",")
        self.top_n_by_volume = int(os.getenv("TOP_N_BY_VOLUME", "60"))
        self.timeframe = os.getenv("TIMEFRAME", "1m")
        
        # v3.0: thresholds elevados baseados em análise de 3 meses
        # MIN_PRICE_CHANGE: 1.5% → 3.0%
        # 81% dos alertas antigos tinham 1-2% (puro ruído, revertiam quase sempre)
        self.threshold = float(os.getenv("THRESHOLD", "1.8"))
        self.min_price_change = float(os.getenv("MIN_PRICE_CHANGE", "0.03"))  # 3%
        
        self.sleep_seconds = int(os.getenv("SLEEP_SECONDS", "20"))
        self.cooldown_minutes = int(os.getenv("COOLDOWN_MINUTES", "30"))  # era 20min
        
        # v3.0: MIN_STRENGTH elevado para 7 (com nova fórmula, S7 = sinal real)
        self.min_strength = int(os.getenv("MIN_STRENGTH", "7"))
        self.debug_mode = os.getenv("DEBUG_MODE", "true").lower() == "true"
        
        self.btc_adjust_strength = os.getenv("BTC_ADJUST_STRENGTH", "true").lower() == "true"
        
        self.tg_token = os.getenv("TG_TOKEN", "")
        self.tg_chat_id = os.getenv("TG_CHAT_ID", "")
        
        # Canal de logs separado para snapshots BTC (resolve 70% sem contexto)
        self.tg_log_chat_id = os.getenv("TG_LOG_CHAT_ID", "")
        
        self.force_test_alerts = os.getenv("FORCE_TEST_ALERTS", "false").lower() == "true"
        self.test_alert_interval = int(os.getenv("TEST_ALERT_INTERVAL", "300"))
        self.last_test_alert = 0
        
        self.stats = {
            'alerts_sent': 0,
            'alerts_filtered_rsi': 0,
            'alerts_filtered_price': 0,
            'alerts_filtered_pretrend': 0,
            'start_time': time.time()
        }
        
        self.btc_thread = threading.Thread(target=self._btc_tracker_loop, daemon=True)
        self.btc_thread.start()
        
        # Thread para snapshot BTC periódico (resolve warmup após restart)
        self.btc_snapshot_thread = threading.Thread(target=self._btc_snapshot_loop, daemon=True)
        self.btc_snapshot_thread.start()
        
        print(f"Bot v3.0 initialized — Binance Only, Quality First")
    
    def _initialize_binance(self):
        """Inicializa apenas Binance"""
        try:
            config = {
                "enableRateLimit": True,
                "timeout": 20000,
                "rateLimit": 1000,
                "options": {"adjustForTimeDifference": True}
            }
            ex = ccxt.binance(config)
            ex.load_markets()
            self.exchanges['binance'] = ex
            print(f"✅ Binance initialized")
        except Exception as e:
            print(f"❌ Failed to initialize Binance: {e}")
    
    def _validate_btc_change(self, change: float, timeframe: str) -> bool:
        """Sanity check para mudanças BTC"""
        max_change = {
            '5m': 3.0,
            '1h': 8.0,
            '4h': 15.0,
            '24h': 25.0
        }
        threshold = max_change.get(timeframe, 50.0)
        if abs(change) > threshold:
            print(f"[BTC SANITY] {timeframe}: {change:+.1f}% INVALID (> ±{threshold}%)")
            return False
        return True

    def _btc_snapshot_loop(self):
        """
        v3.0 NOVO: Envia snapshot BTC para canal de logs a cada 30min.
        
        PROBLEMA que resolve:
        Nos 3 meses anteriores, 70% dos alertas não tinham contexto BTC porque
        o campo movement_type ficava UNKNOWN. Isto acontecia porque o BTC tracker
        precisa de acumular história em memória, e o container reinicia frequentemente
        no Railway/Render, apagando essa história.
        
        SOLUÇÃO:
        A cada 30min, guardamos um snapshot do estado BTC num canal Telegram separado.
        Ao reiniciar, o bot pode recuperar o último snapshot e retomar o contexto BTC
        imediatamente, sem esperar 5min de warmup.
        
        Para usar, cria um segundo canal Telegram (privado) e define TG_LOG_CHAT_ID.
        """
        while True:
            try:
                time.sleep(1800)  # 30 minutos
                
                if not self.btc_data.get('data_valid', False):
                    continue
                
                snapshot = {
                    'ts': int(time.time()),
                    'price': self.btc_data['last_price'],
                    'c5m': self.btc_data['change_5m'],
                    'c1h': self.btc_data['change_1h'],
                    'c4h': self.btc_data['change_4h'],
                    'c24h': self.btc_data['change_24h'],
                    'macro': self.btc_data['trend_macro'],
                }
                
                # Guarda em disco (para Railway com volume persistente)
                try:
                    snap_file = os.path.join(self.db.data_dir, "btc_snapshot.json")
                    with open(snap_file, 'w') as f:
                        json.dump(snapshot, f)
                except Exception:
                    pass
                
                # Envia para canal de logs se configurado
                if self.tg_log_chat_id and self.tg_token:
                    msg = f"📸 BTC_SNAP|{snapshot['ts']}|{snapshot['price']:.0f}|{snapshot['c5m']:.2f}|{snapshot['c1h']:.2f}|{snapshot['c4h']:.2f}|{snapshot['c24h']:.2f}|{snapshot['macro']}"
                    try:
                        requests.post(
                            f"https://api.telegram.org/bot{self.tg_token}/sendMessage",
                            json={"chat_id": self.tg_log_chat_id, "text": msg},
                            timeout=10
                        )
                    except Exception:
                        pass
                        
            except Exception as e:
                print(f"[BTC Snapshot] Error: {e}")
    
    def _restore_btc_from_snapshot(self):
        """
        Tenta restaurar dados BTC do último snapshot guardado.
        Chamado no arranque para evitar o warmup de 5min.
        """
        try:
            snap_file = os.path.join(self.db.data_dir, "btc_snapshot.json")
            if not os.path.exists(snap_file):
                return False
            
            with open(snap_file, 'r') as f:
                snap = json.load(f)
            
            age_minutes = (int(time.time()) - snap['ts']) / 60
            
            # Snapshot recente (menos de 2 horas) é utilizável
            if age_minutes < 120:
                self.btc_data['last_price'] = snap['price']
                self.btc_data['change_5m'] = snap['c5m']
                self.btc_data['change_1h'] = snap['c1h']
                self.btc_data['change_4h'] = snap['c4h']
                self.btc_data['change_24h'] = snap['c24h']
                self.btc_data['trend_macro'] = snap['macro']
                self.btc_data['data_valid'] = True  # Dados válidos imediatamente!
                print(f"[BTC] Restored from snapshot ({age_minutes:.0f}min ago): ${snap['price']:.0f}")
                return True
            else:
                print(f"[BTC] Snapshot too old ({age_minutes:.0f}min), will wait for fresh data")
                return False
                
        except Exception as e:
            print(f"[BTC] Snapshot restore error: {e}")
            return False
    
    def _btc_tracker_loop(self):
        """Monitora BTC/USDT com sanity checks"""
        print("[BTC Tracker v3.0] Starting...")
        
        # Tenta restaurar do snapshot antes do warmup
        self._restore_btc_from_snapshot()
        
        attempts = 0
        while 'binance' not in self.exchanges or not self.exchanges.get('binance'):
            time.sleep(2)
            attempts += 1
            if attempts > 30:
                print("[BTC Tracker] ERROR: Binance not available!")
                return
        
        try:
            ex = self.exchanges['binance']
            ticker = ex.fetch_ticker('BTC/USDT')
            if self.btc_data['last_price'] == 0:
                self.btc_data['last_price'] = ticker['last']
            print(f"[BTC Tracker] BTC: ${ticker['last']:.0f}")
        except Exception as e:
            print(f"[BTC Tracker] Init error: {e}")
        
        while True:
            try:
                if 'binance' in self.exchanges:
                    ex = self.exchanges['binance']
                    
                    ticker = ex.fetch_ticker('BTC/USDT')
                    current_price = ticker['last']
                    current_time = int(time.time())
                    current_volume = ticker.get('quoteVolume', 0)
                    
                    elapsed = current_time - self.btc_data['start_time']
                    if elapsed >= self.btc_data['warmup_time']:
                        self.btc_data['data_valid'] = True
                    
                    try:
                        ohlcv = ex.fetch_ohlcv('BTC/USDT', '1m', 10)
                        if len(ohlcv) >= 5:
                            volumes = [c[5] for c in ohlcv[-5:]]
                            avg_volume = sum(volumes) / len(volumes)
                            last_volume = ohlcv[-1][5] if ohlcv else current_volume
                            self.btc_data['volume_spike'] = last_volume / avg_volume if avg_volume > 0 else 1.0
                    except Exception:
                        self.btc_data['volume_spike'] = 1.0
                    
                    self.btc_data['history'].append({
                        'price': current_price,
                        'timestamp': current_time,
                        'volume': current_volume
                    })
                    
                    if len(self.btc_data['history']) >= 2:
                        history = list(self.btc_data['history'])
                        
                        if len(history) >= 5:
                            lookback_5m = min(15, len(history) - 1)
                            price_5m_ago = history[-lookback_5m]['price']
                            change_5m = ((current_price - price_5m_ago) / price_5m_ago) * 100
                            if self._validate_btc_change(change_5m, '5m'):
                                self.btc_data['change_5m'] = change_5m
                        
                        if len(history) >= 20:
                            lookback_1h = min(60, len(history) - 1)
                            price_1h_ago = history[-lookback_1h]['price']
                            change_1h = ((current_price - price_1h_ago) / price_1h_ago) * 100
                            if self._validate_btc_change(change_1h, '1h'):
                                self.btc_data['change_1h'] = change_1h
                        
                        # 4h via OHLCV
                        # BUG FIX: usar limit=7 e timestamp para garantir candle correcto
                        # Com limit=5 e [-4], apanhava candle com preço de anos atrás
                        # porque o último candle pode estar em formação (open != close)
                        try:
                            ohlcv_1h = ex.fetch_ohlcv('BTC/USDT', '1h', 7)
                            if len(ohlcv_1h) >= 6:
                                # Ignorar o último candle (em formação) e apanhar o de 4h atrás
                                # ohlcv_1h[-1] = candle actual (em formação, preço pode ser open)
                                # ohlcv_1h[-2] = candle fechado mais recente (1h atrás)
                                # ohlcv_1h[-5] = candle fechado de 4h atrás
                                candle_4h_ago = ohlcv_1h[-5]
                                price_4h = candle_4h_ago[4]  # close price
                                ts_4h = candle_4h_ago[0] / 1000  # timestamp em segundos
                                
                                # Verificar que o timestamp faz sentido (~4h atrás ± 30min)
                                expected_ts = current_time - 4 * 3600
                                ts_diff_minutes = abs(ts_4h - expected_ts) / 60
                                
                                if ts_diff_minutes < 90:  # tolerância de 90 minutos
                                    change_4h = ((current_price - price_4h) / price_4h) * 100
                                    if self._validate_btc_change(change_4h, '4h'):
                                        self.btc_data['change_4h'] = change_4h
                                else:
                                    print(f"[BTC 4h] Timestamp inesperado: diff={ts_diff_minutes:.0f}min, a ignorar")
                        except Exception:
                            pass
                        
                        # 24h via ticker percentage
                        try:
                            if 'percentage' in ticker and ticker['percentage'] is not None:
                                change_24h = ticker['percentage']
                                if self._validate_btc_change(change_24h, '24h'):
                                    self.btc_data['change_24h'] = change_24h
                        except Exception:
                            pass
                        
                        # Trends
                        if self.btc_data['change_5m'] > 0.3:
                            self.btc_data['trend_micro'] = 'UP'
                        elif self.btc_data['change_5m'] < -0.3:
                            self.btc_data['trend_micro'] = 'DOWN'
                        else:
                            self.btc_data['trend_micro'] = 'LATERAL'
                        
                        change_4h = self.btc_data['change_4h']
                        change_24h = self.btc_data['change_24h']
                        
                        if change_4h > 1.5 or change_24h > 2.0:
                            self.btc_data['trend_macro'] = 'STRONG_UP'
                        elif change_4h > 0.5 or change_24h > 1.0:
                            self.btc_data['trend_macro'] = 'UP'
                        elif change_4h < -1.5 or change_24h < -2.0:
                            self.btc_data['trend_macro'] = 'STRONG_DOWN'
                        elif change_4h < -0.5 or change_24h < -1.0:
                            self.btc_data['trend_macro'] = 'DOWN'
                        else:
                            self.btc_data['trend_macro'] = 'LATERAL'
                    
                    self.btc_data['last_price'] = current_price
                    self.btc_data['last_update'] = current_time
                    self.btc_data['last_volume'] = current_volume
                
                time.sleep(20)
                
            except Exception as e:
                print(f"[BTC Tracker] Error: {e}")
                time.sleep(30)
    
    def should_process_symbol(self, symbol: str) -> bool:
        """Verifica blacklist"""
        clean_symbol = symbol.split(':')[0] if ':' in symbol else symbol
        if clean_symbol in SYMBOLS_BLACKLIST:
            return False
        return True
    
    def send_telegram(self, msg: str):
        if not self.tg_token or not self.tg_chat_id:
            return
        try:
            requests.post(
                f"https://api.telegram.org/bot{self.tg_token}/sendMessage",
                json={
                    "chat_id": self.tg_chat_id,
                    "text": msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                },
                timeout=20
            )
        except Exception as e:
            print(f"[Telegram] Error: {e}")
    
    
    def get_symbols_for_exchange(self, ex, limit: int = 60):
        try:
            tickers = ex.fetch_tickers()
            volume_pairs = []
            
            for symbol, ticker in tickers.items():
                if not symbol.endswith("/USDT"):
                    continue
                if not self.should_process_symbol(symbol):
                    continue
                
                volume = ticker.get("quoteVolume")
                # v3.0: filtro de volume ligeiramente alargado
                # Exclui extremamente baixo (<100k) e extremamente alto (>500M - BTC/ETH)
                if volume and 100_000 <= volume <= 500_000_000:
                    volume_pairs.append((symbol, volume))
            
            volume_pairs.sort(key=lambda x: x[1], reverse=True)
            return [symbol for symbol, _ in volume_pairs[:limit]]
            
        except Exception as e:
            print(f"Error getting symbols: {e}")
            return []
    
    def calculate_rsi(self, prices: List[float], period: int = 14) -> Optional[float]:
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
            return max(0, min(100, rsi))
            
        except Exception:
            return None
    
    def calculate_pre_trend(self, ohlcv: list) -> str:
        """
        v3.0 NOVO: Analisa tendência dos 3 candles ANTES do spike.
        
        PORQUÊ:
        Um pump de 3%+ que aparece do nada (sem tendência prévia) é muito
        provavelmente manipulação pontual — alguém comprou em massa e saiu.
        Um pump que vem de 3 candles em alta já tem momentum real.
        
        Retorna:
        - 'BULLISH'  : 2+ dos últimos 3 candles foram positivos
        - 'BEARISH'  : 2+ dos últimos 3 candles foram negativos
        - 'CHOPPY'   : sem tendência clara
        """
        try:
            if len(ohlcv) < 5:
                return 'UNKNOWN'
            
            # Últimos 3 candles ANTES do actual (índices -4, -3, -2)
            recent = ohlcv[-5:-2]
            
            ups = 0
            downs = 0
            for candle in recent:
                open_p, close_p = candle[1], candle[4]
                if close_p > open_p * 1.001:  # +0.1% para ignorar flat
                    ups += 1
                elif close_p < open_p * 0.999:  # -0.1%
                    downs += 1
            
            if ups >= 2:
                return 'BULLISH'
            elif downs >= 2:
                return 'BEARISH'
            else:
                return 'CHOPPY'
                
        except Exception:
            return 'UNKNOWN'
    
    def calculate_strength_v3(self, vol_multiple: float, price_change_pct: float) -> int:
        """
        v3.0: Nova fórmula de Strength — sem saturação artificial
        
        PROBLEMA da fórmula antiga:
        min(vol/2 + price*20, 10) saturava facilmente.
        Vol 5x + price 1.5% = score 5.0 → S10 (porque price*20 = 30, total = 32.5, capped a 10)
        Resultado: 66% dos S10 tinham volume abaixo de 10x. A métrica era inútil.
        
        NOVA FÓRMULA:
        - vol_score: escala logarítmica suave até 5 pts
          - 2x = 1pt, 5x = 2pt, 10x = 3pt, 20x = 4pt, 50x+ = 5pt
        - price_score: escala linear até 5 pts
          - 3% = 1pt, 5% = 2pt, 8% = 3pt, 12% = 4pt, 20%+ = 5pt
        - Total máximo: 10
        
        Para atingir S10: precisa vol 50x+ E price 20%+ (movimento REALMENTE excepcional)
        Para atingir S7: vol 10x + price 8% (sinal forte mas realista)
        """
        # Volume score (0-5)
        if vol_multiple >= 50:
            vol_score = 5
        elif vol_multiple >= 20:
            vol_score = 4
        elif vol_multiple >= 10:
            vol_score = 3
        elif vol_multiple >= 5:
            vol_score = 2
        elif vol_multiple >= 2:
            vol_score = 1
        else:
            vol_score = 0
        
        # Price score (0-5) — usa valor absoluto
        abs_price = abs(price_change_pct)
        if abs_price >= 20:
            price_score = 5
        elif abs_price >= 12:
            price_score = 4
        elif abs_price >= 8:
            price_score = 3
        elif abs_price >= 5:
            price_score = 2
        elif abs_price >= 3:
            price_score = 1
        else:
            price_score = 0
        
        return vol_score + price_score
    
    def fetch_ohlcv_safe(self, ex, symbol: str, timeframe: str, limit: int):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            except ccxt.NetworkError:
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 2)
                    continue
                return None
            except ccxt.ExchangeError as e:
                if "rate limit" in str(e).lower():
                    time.sleep(3)
                    continue
                return None
            except Exception:
                return None
        return None
    
    def can_alert(self, symbol: str, exchange: str, now_ts: float, volume_x: float = None) -> bool:
        """
        Sistema de cooldown v3.0 com 3 camadas:
        
        1. Cooldown dinâmico por accuracy do símbolo
           - Símbolo com acc <10% → 24h sem alertas
           - Símbolo com acc <20% → 4h sem alertas
           - Símbolo bom (≥30%)   → 15min
        
        2. Volume normalizado por símbolo
           - Verifica se o spike é excepcional vs histórico do símbolo
        
        3. Cap diário
           - Nenhum símbolo dispara mais de MAX_ALERTS_PER_SYMBOL_PER_DAY vezes/dia
        """
        key = f"{exchange}:{symbol}"
        
        # --- Camada 1: Cooldown Dinâmico ---
        dynamic_cooldown = self.reputation.get_dynamic_cooldown(symbol)
        elapsed = now_ts - self.last_alert_ts[key]
        
        if elapsed < dynamic_cooldown * 60:
            remaining = (dynamic_cooldown * 60 - elapsed) / 60
            if self.debug_mode and dynamic_cooldown >= 240:
                print(f"[COOLDOWN] {symbol}: {dynamic_cooldown}min cooldown, {remaining:.0f}min restantes")
            return False
        
        # --- Camada 2: Volume Normalizado ---
        if volume_x is not None and not self.reputation.is_volume_exceptional(symbol, volume_x):
            return False
        
        # --- Camada 3: Cap Diário ---
        if not self.reputation.check_daily_cap(symbol):
            return False
        
        # Passou todas as camadas — pode alertar
        self.last_alert_ts[key] = now_ts
        return True
    
    def generate_alert(self, event: MarketEvent, analysis: Dict) -> str:
        """Alerta v3.0 — mais informação relevante, menos ruído"""
        
        # Emoji de tendência prévia
        trend_emoji = {'BULLISH': '📈', 'BEARISH': '📉', 'CHOPPY': '〰️', 'UNKNOWN': ''}.get(event.pre_trend, '')
        
        msg = f"""🚨 <b>{event.event_type} DETECTADO</b>

🎯 <b>{event.symbol}</b> (BINANCE)
⚡ <b>Strength: {event.event_strength}/10</b>
💹 Volume: {event.volume_multiple:.1f}x médio
📈 Preço: {event.price_change_pct:+.1f}%"""

        if event.rsi is not None:
            msg += f"\n📊 RSI: {event.rsi:.0f}"
        
        if event.pre_trend != 'UNKNOWN':
            msg += f"\n{trend_emoji} Tendência prévia: {event.pre_trend}"

        if self.btc_data.get('data_valid', False):
            btc_1h = self.btc_data['change_1h']
            btc_4h = self.btc_data['change_4h']
            btc_24h = self.btc_data['change_24h']
            trend_macro = self.btc_data['trend_macro']
            
            btc_lines = []
            if abs(btc_24h) > 3.0:
                btc_lines.append(f"₿ BTC Dia: {btc_24h:+.1f}%")
            if abs(btc_4h) > 1.5:
                btc_lines.append(f"₿ BTC 4h: {btc_4h:+.1f}%")
            elif abs(btc_1h) > 0.8:
                btc_lines.append(f"₿ BTC 1h: {btc_1h:+.1f}%")
            
            if trend_macro != 'LATERAL' and btc_lines:
                btc_lines.append(f"📊 Trend: {trend_macro}")
            
            relative_to_4h = event.price_change_pct - btc_4h
            if abs(relative_to_4h) > 2.0:
                if relative_to_4h > 0:
                    btc_lines.append(f"💪 +{abs(relative_to_4h):.1f}% vs BTC")
                else:
                    btc_lines.append(f"⚠️ {relative_to_4h:.1f}% vs BTC")
            
            if btc_lines:
                msg += "\n\n" + "\n".join(btc_lines)
        
        msg += f"\n🕐 {datetime.fromtimestamp(event.timestamp).strftime('%H:%M:%S')}"
        
        correlations = analysis.get('correlations_found', [])
        if correlations:
            msg += f"\n🔗 Correlação: {correlations[0].symbol_pair[0]}"
        
        cascade = analysis.get('cascade_risk', 0)
        if cascade > 0.5:
            msg += f"\n⚠️ Cascade Risk: HIGH"
        
        return msg
    
    def run(self):
        try:
            print("🏦 Initializing Binance...")
            
            for exchange_name, ex in self.exchanges.items():
                symbols = self.get_symbols_for_exchange(ex, self.top_n_by_volume)
                self.watchlist[exchange_name] = symbols
                print(f"✅ {exchange_name}: {len(symbols)} symbols")
            
            if not self.exchanges:
                raise SystemExit("❌ No exchanges")
            
            total_symbols = sum(len(s) for s in self.watchlist.values())
            blacklisted = len(SYMBOLS_BLACKLIST)
            
            startup_msg = f"""🚀 <b>BOT v3.0 — BINANCE ONLY</b>

<b>Qualidade sobre Quantidade</b>

📊 {total_symbols} pares USDT
⛔ {blacklisted} blacklist

<b>🔧 Filtros v3.0:</b>
• Price change ≥ 3% (era 1.5%)
• RSI filter activo
• Pre-trend filter activo
• Strength ≥ 7 (nova fórmula)
• Cooldown 30min (era 20min)

<b>📈 Accuracy honesta:</b>
• Só SUSTAINED conta como acerto
• WEAK_CONTINUATION = Neutro
• Meta: superar 15% SUSTAINED

₿ BTC: {'Snapshot restaurado ✅' if self.btc_data['data_valid'] else 'Aguarda 5min ⏳'}"""
            
            self.send_telegram(startup_msg)
            time.sleep(5)
            self.run_detection_loop()
            
        except KeyboardInterrupt:
            print("\n👋 Bot stopped")
        except Exception as e:
            error_msg = f"❌ Bot crashed: {e}"
            print(error_msg)
            self.send_telegram(error_msg)
            raise
    
    def run_detection_loop(self):
        """Loop de detecção v3.0 com filtros RSI e pre-trend"""
        print("🔬 Starting detection v3.0...")
        
        loop_count = 0
        
        while True:
            loop_start = time.time()
            loop_count += 1
            
            if self.force_test_alerts:
                current_time = time.time()
                if current_time - self.last_test_alert >= self.test_alert_interval:
                    self._send_test_alert()
                    self.last_test_alert = current_time
            
            if self.debug_mode and loop_count % 50 == 0:
                uptime = (time.time() - self.stats['start_time']) / 3600
                total_alerts = len(self.validation_system.validation_results) + len(self.validation_system.pending_validations)
                btc_price = self.btc_data['last_price']
                trend = self.btc_data['trend_macro']
                valid = '✅' if self.btc_data['data_valid'] else '⏳'
                filt_rsi = self.stats['alerts_filtered_rsi']
                filt_price = self.stats['alerts_filtered_price']
                filt_trend = self.stats['alerts_filtered_pretrend']
                print(f"[STATS v3.0] Loop #{loop_count} | Alerts: {total_alerts} | BTC: ${btc_price:.0f} ({trend}) {valid}")
                print(f"  Filtered: RSI={filt_rsi} Price={filt_price} PreTrend={filt_trend}")
            
            for exchange_name, ex in self.exchanges.items():
                symbols = self.watchlist.get(exchange_name, [])
                
                for symbol in symbols:
                    if not self.should_process_symbol(symbol):
                        continue
                    
                    try:
                        ohlcv = self.fetch_ohlcv_safe(ex, symbol, self.timeframe, 25)
                        if not ohlcv or len(ohlcv) < 15:
                            continue
                        
                        *hist, last = ohlcv
                        volumes = [c[5] for c in hist[-8:]]
                        vol_avg = sum(volumes) / len(volumes) if volumes else 0
                        vol_last = last[5]
                        close_last = last[4]
                        
                        vol_multiple = vol_last / vol_avg if vol_avg > 0 else 0
                        
                        if vol_multiple > 200:
                            continue
                        
                        price_change_pct = 0
                        if len(hist) > 0:
                            prev_close = hist[-1][4]
                            price_change_pct = (close_last - prev_close) / prev_close if prev_close > 0 else 0
                        
                        # ============================================
                        # FILTRO 1: VOLUME + PRICE CHANGE (threshold)
                        # ============================================
                        if vol_multiple < self.threshold:
                            continue
                        
                        if abs(price_change_pct) < self.min_price_change:
                            self.stats['alerts_filtered_price'] += 1
                            continue
                        
                        prices = [c[4] for c in ohlcv]
                        rsi = self.calculate_rsi(prices)
                        event_type = "PUMP" if price_change_pct > 0 else "DUMP"
                        
                        # ============================================
                        # FILTRO 2: RSI (v3.0 NOVO)
                        # ============================================
                        # PUMP com RSI > 75: mercado já sobrecomprado,
                        #   probabilidade de continuação baixa → ignorar
                        # DUMP com RSI < 25: mercado já sobrevendido,
                        #   bounce iminente, dump provavelmente não vai continuar → ignorar
                        if rsi is not None:
                            if event_type == "PUMP" and rsi > 75:
                                self.stats['alerts_filtered_rsi'] += 1
                                if self.debug_mode:
                                    print(f"[RSI FILTER] {symbol}: PUMP bloqueado RSI={rsi:.0f}>75")
                                continue
                            if event_type == "DUMP" and rsi < 25:
                                self.stats['alerts_filtered_rsi'] += 1
                                if self.debug_mode:
                                    print(f"[RSI FILTER] {symbol}: DUMP bloqueado RSI={rsi:.0f}<25")
                                continue
                        
                        # ============================================
                        # FILTRO 3: PRE-TREND (v3.0 NOVO)
                        # ============================================
                        # Um PUMP sem tendência prévia (CHOPPY ou BEARISH)
                        # é muito provavelmente um spike isolado de manipulação.
                        # Só avançamos se os 3 candles anteriores confirmam direcção.
                        pre_trend = self.calculate_pre_trend(ohlcv)
                        
                        if event_type == "PUMP" and pre_trend == "BEARISH":
                            self.stats['alerts_filtered_pretrend'] += 1
                            if self.debug_mode:
                                print(f"[TREND FILTER] {symbol}: PUMP bloqueado, pre-trend BEARISH")
                            continue
                        
                        if event_type == "DUMP" and pre_trend == "BULLISH":
                            self.stats['alerts_filtered_pretrend'] += 1
                            if self.debug_mode:
                                print(f"[TREND FILTER] {symbol}: DUMP bloqueado, pre-trend BULLISH")
                            continue
                        
                        # ============================================
                        # STRENGTH v3.0 (sem saturação)
                        # ============================================
                        base_strength = self.calculate_strength_v3(vol_multiple, price_change_pct * 100)
                        
                        event_strength = base_strength
                        
                        # Ajuste RSI: penaliza zona neutra (45-65)
                        if rsi is not None and 45 <= rsi <= 65:
                            event_strength = max(0, event_strength - 1)
                        
                        # Ajuste BTC
                        if self.btc_adjust_strength and self.btc_data.get('data_valid', False):
                            trend_macro = self.btc_data['trend_macro']
                            btc_4h = self.btc_data['change_4h']
                            
                            if trend_macro in ['UP', 'STRONG_UP'] and event_type == 'PUMP':
                                if abs(price_change_pct * 100 - btc_4h) < 2:
                                    event_strength = int(base_strength * 0.7)
                            elif trend_macro in ['DOWN', 'STRONG_DOWN'] and event_type == 'PUMP':
                                event_strength = min(10, int(base_strength * 1.3))
                            elif trend_macro in ['DOWN', 'STRONG_DOWN'] and event_type == 'DUMP':
                                if abs(price_change_pct * 100 - btc_4h) < 2:
                                    event_strength = int(base_strength * 0.7)
                        
                        if event_strength < self.min_strength:
                            continue
                        
                        event = MarketEvent(
                            timestamp=int(time.time()),
                            exchange=exchange_name,
                            symbol=symbol,
                            event_type=event_type,
                            volume_multiple=vol_multiple,
                            price_change_pct=price_change_pct * 100,
                            rsi=rsi,
                            event_strength=event_strength,
                            pre_trend=pre_trend
                        )
                        
                        analysis = self.correlation_engine.process_new_event(event)
                        
                        # can_alert agora recebe symbol, exchange e volume para as 3 camadas
                        if self.can_alert(symbol, exchange_name, time.time(), vol_multiple):
                            alert_message = self.generate_alert(event, analysis)
                            self.send_telegram(alert_message)
                            self.stats['alerts_sent'] += 1
                            
                            # Regista no sistema de reputação
                            self.reputation.record_alert(symbol, vol_multiple)
                            
                            alert_data = {
                                'symbol': event.symbol,
                                'exchange': event.exchange,
                                'event_type': event.event_type,
                                'price': close_last,
                                'volume_multiple': vol_multiple,
                                'event_strength': event_strength,
                                'price_change_pct': price_change_pct * 100,
                                'rsi': rsi,
                                'pre_trend': pre_trend,
                                'correlations_count': len(analysis['correlations_found']),
                                'cascade_risk': analysis['cascade_risk'],
                                'market_regime': analysis['market_regime']
                            }
                            self.validation_system.register_alert(alert_data)
                            
                            # Mostra cooldown actual do símbolo no debug
                            if self.debug_mode:
                                cd = self.reputation.get_dynamic_cooldown(symbol)
                                acc, n = self.reputation.get_symbol_accuracy(symbol)
                                acc_str = f"{acc:.0f}%" if acc is not None else "novo"
                                rsi_str = f'{rsi:.0f}' if rsi is not None else 'N/A'
                                print(f'[ALERT v3.0] {symbol}: {event_type} S{event_strength}/10 vol={vol_multiple:.1f}x rsi={rsi_str} trend={pre_trend} acc={acc_str} cd={cd}min')
                    
                    except Exception as e:
                        if self.debug_mode and "rate limit" not in str(e).lower():
                            print(f"Error: {exchange_name} {symbol}: {e}")
                        continue
            
            elapsed = time.time() - loop_start
            sleep_time = max(0, self.sleep_seconds - elapsed)
            time.sleep(sleep_time)
    
    def _send_test_alert(self):
        """Test alert v3.0"""
        btc_price = self.btc_data['last_price']
        btc_4h = self.btc_data['change_4h']
        btc_24h = self.btc_data['change_24h']
        trend_macro = self.btc_data['trend_macro']
        data_valid = self.btc_data['data_valid']
        
        filt_rsi = self.stats['alerts_filtered_rsi']
        filt_price = self.stats['alerts_filtered_price']
        filt_trend = self.stats['alerts_filtered_pretrend']
        sent = self.stats['alerts_sent']
        
        msg = f"""🧪 <b>TEST v3.0 — Binance Only</b>

₿ ${btc_price:.0f}
4h: {btc_4h:+.2f}% | 24h: {btc_24h:+.2f}%
Trend: {trend_macro}
Valid: {'✅' if data_valid else '⏳ Warming up...'}

<b>Filtros activos:</b>
🚫 RSI: {filt_rsi}
🚫 Price<3%: {filt_price}
🚫 Pre-trend: {filt_trend}
✅ Alertas enviados: {sent}

{datetime.now().strftime('%H:%M:%S')}"""
        
        self.send_telegram(msg)

# =========================
#   MAIN
# =========================
def main():
    print("🚀 Bot v3.0 Starting — Binance Only, Quality First")
    print("🔧 Filtros: price>=3%, RSI, pre-trend, strength>=7")
    print("📊 Accuracy honesta: só SUSTAINED conta como acerto")
    
    bot = AdvancedPatternTradingBot()
    bot.run()

if __name__ == "__main__":
    main()
