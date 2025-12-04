"""
SolAnalyzer - Wallet Analysis Module
====================================
Celery tasks for analyzing Solana wallet transactions and calculating P&L.
"""

import json
import logging
import math
import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from random import uniform
from time import sleep

import base58
import redis
import requests
from celery import Celery, current_task
from celery.utils.log import get_task_logger
from dotenv import load_dotenv
from redis import Redis
from redis.lock import Lock
from solana.rpc.api import Client
from solders.pubkey import Pubkey
from solders.signature import Signature

from celery_config import app, redis_client

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = get_task_logger(__name__)

# =============================================================================
# Constants
# =============================================================================

RAYDIUM_POOL_ADDRESS = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"
SOL_MINT_ADDRESS = "So11111111111111111111111111111111111111112"
USDC_MINT_ADDRESS = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT_ADDRESS = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"

# Caches
mint_address_cache = {}
token_name_cache = {}
sol_price_cache = {"timestamp": None, "price": None}

# =============================================================================
# Lock Utilities
# =============================================================================

def acquire_lock_with_timeout(conn, lockname, acquire_timeout=10, lock_timeout=10):
    """
    Acquire a distributed lock using Redis.
    
    Args:
        conn: Redis connection
        lockname: Name of the lock
        acquire_timeout: Maximum time to wait for lock acquisition
        lock_timeout: How long the lock should be held
    
    Returns:
        Lock identifier if acquired, False otherwise
    """
    identifier = str(uuid.uuid4())
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout))
    end = time.time() + acquire_timeout
    
    while time.time() < end:
        if conn.setnx(lockname, identifier):
            conn.expire(lockname, lock_timeout)
            return identifier
        elif not conn.ttl(lockname):
            conn.expire(lockname, lock_timeout)
        time.sleep(0.001)
    return False


def release_lock(conn, lockname, identifier):
    """
    Release a distributed lock.
    
    Args:
        conn: Redis connection
        lockname: Name of the lock
        identifier: Lock identifier returned by acquire_lock_with_timeout
    
    Returns:
        True if lock was released, False otherwise
    """
    pipe = conn.pipeline(True)
    lockname = 'lock:' + lockname
    
    while True:
        try:
            pipe.watch(lockname)
            lock_value = pipe.get(lockname)
            if lock_value and lock_value.decode('utf-8') == identifier:
                pipe.multi()
                pipe.delete(lockname)
                pipe.execute()
                return True
            pipe.unwatch()
            break
        except redis.exceptions.WatchError:
            pass
    return False


# =============================================================================
# Solana Endpoint Manager
# =============================================================================

class SolanaEndpointManager:
    """
    Manages multiple Solana RPC endpoints with load balancing and availability tracking.
    """
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.endpoints = self._load_endpoints()
        self.initialize_endpoints()
    
    def _load_endpoints(self):
        """Load endpoints from environment variables."""
        endpoints = []
        for i in range(1, 11):
            endpoint = os.getenv(f'ENDPOINT_{i}')
            if endpoint:
                endpoints.append(endpoint)
        return endpoints
    
    def initialize_endpoints(self):
        """Mark all endpoints as available in Redis."""
        for endpoint in self.endpoints:
            if endpoint:
                self.redis_client.setnx(f"endpoint_available:{endpoint}", 1)
                logger.info(f"Endpoint initialized: {endpoint[:50]}...")
    
    def acquire_endpoint_with_retry(self, retries=20, delay=6):
        """
        Acquire an available endpoint with retry logic.
        
        Args:
            retries: Number of retry attempts
            delay: Delay between retries in seconds
        
        Returns:
            Available endpoint URL
        
        Raises:
            Exception: If no endpoint could be acquired
        """
        for attempt in range(retries):
            identifier = acquire_lock_with_timeout(
                self.redis_client, "endpoint_lock", lock_timeout=10
            )
            if not identifier:
                logger.warning(f"Could not acquire lock. Retrying in {delay}s...")
                time.sleep(delay)
                continue
            
            available_endpoints = [
                endpoint for endpoint in self.endpoints
                if endpoint and self._is_endpoint_available(endpoint)
            ]
            
            if available_endpoints:
                endpoint = random.choice(available_endpoints)
                self.redis_client.set(f"endpoint_available:{endpoint}", 0)
                release_lock(self.redis_client, "endpoint_lock", identifier)
                logger.info(f"Endpoint acquired: {endpoint[:50]}...")
                return endpoint
            
            release_lock(self.redis_client, "endpoint_lock", identifier)
            logger.warning(f"No endpoints available. Retrying in {delay}s...")
            time.sleep(delay)
        
        raise Exception("Could not acquire endpoint after multiple attempts")
    
    def _is_endpoint_available(self, endpoint):
        """Check if an endpoint is available."""
        value = self.redis_client.get(f"endpoint_available:{endpoint}")
        return value and value.decode() == '1'
    
    def release_endpoint(self, url):
        """Release an endpoint back to the pool."""
        try:
            self.redis_client.set(f"endpoint_available:{url}", 1)
            logger.info(f"Endpoint released: {url[:50]}...")
        except redis.ConnectionError as e:
            logger.error(f"Error releasing endpoint: {e}. Retrying...")
            time.sleep(5)
            self.release_endpoint(url)


# Initialize endpoint manager
endpoint_manager = SolanaEndpointManager(redis_client)


# =============================================================================
# Price APIs
# =============================================================================

def get_sol_price_at_time(block_time_unix):
    """Get historical SOL price at a specific time."""
    date = datetime.fromtimestamp(block_time_unix, timezone.utc).strftime('%d-%m-%Y')
    url = f"https://api.coingecko.com/api/v3/coins/solana/history?date={date}&localization=false"
    
    try:
        response = requests.get(url, timeout=10)
        price = response.json()['market_data']['current_price']['usd']
        return price
    except Exception as e:
        logger.error(f"Error getting historical SOL price: {e}")
        return None


def get_current_sol_price_usd():
    """
    Get current SOL price in USD with caching.
    Cache is valid for 5 minutes.
    """
    if (sol_price_cache["timestamp"] is not None and 
        (time.time() - sol_price_cache["timestamp"]) < 300):
        return sol_price_cache["price"]
    
    url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
    
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        sol_price_usd = data['solana']['usd']
        
        sol_price_cache["price"] = sol_price_usd
        sol_price_cache["timestamp"] = time.time()
        
        return sol_price_usd
    except Exception as e:
        logger.error(f"Error getting current SOL price: {e}")
        return None


# =============================================================================
# Token Metadata APIs
# =============================================================================

def _get_moralis_apis():
    """Get Moralis API configurations from environment variables."""
    apis = []
    
    api_key_1 = os.getenv('MORALIS_API_KEY_1')
    api_key_2 = os.getenv('MORALIS_API_KEY_2')
    
    if api_key_1:
        apis.append({
            "url_template": "https://solana-gateway.moralis.io/token/mainnet/{mint_address}/metadata",
            "headers": {
                "Accept": "application/json",
                "X-API-Key": api_key_1
            }
        })
    
    if api_key_2:
        apis.append({
            "url_template": "https://solana-gateway.moralis.io/token/mainnet/{mint_address}/metadata",
            "headers": {
                "Accept": "application/json",
                "X-API-Key": api_key_2
            }
        })
    
    # Fallback to hardcoded keys if env vars not set (for backwards compatibility)
    if not apis:
        apis = [
            {
                "url_template": "https://solana-gateway.moralis.io/token/mainnet/{mint_address}/metadata",
                "headers": {
                    "Accept": "application/json",
                    "X-API-Key": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6ImNhMzcyMTU1LTU1ZGEtNGNmNi05MzIwLWZiMDBhMmI5NGQ3NyIsIm9yZ0lkIjoiMzgxMDY1IiwidXNlcklkIjoiMzkxNTU5IiwidHlwZUlkIjoiYzRjM2I1NmMtZWU4OS00MTE2LTg5Y2EtYzFjNWNjMjQyNTVhIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3MDk0ODI3NTYsImV4cCI6NDg2NTI0Mjc1Nn0.92giuYeXQO52raWnOOOqgiq4V_EU_1-CyLiIzGy2jnk"
                }
            }
        ]
    
    return apis


def get_token_name(mint_address, max_retries=2):
    """
    Get token name from mint address using Moralis API.
    Results are cached to reduce API calls.
    """
    if mint_address in token_name_cache:
        return token_name_cache[mint_address]
    
    apis = _get_moralis_apis()
    
    for attempt in range(max_retries):
        api = random.choice(apis)
        url = api["url_template"].format(mint_address=mint_address)
        headers = api["headers"]
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            token_name = data.get('name', 'Unknown Token')
            
            token_name_cache[mint_address] = token_name
            return token_name
        except Exception as e:
            if attempt < max_retries - 1:
                sleep_time = 2 ** attempt
                time.sleep(sleep_time)
            else:
                logger.warning(f"Could not get token name after {max_retries} attempts: {e}")
                return "Unknown Token"
    
    return "Unknown Token"


# =============================================================================
# Transaction Processing
# =============================================================================

def obtener_transacciones(cadena, limite=3000, endpoint_url=None):
    """
    Get transaction signatures for a wallet address.
    
    Args:
        cadena: Wallet address (base58 encoded)
        limite: Maximum number of transactions to fetch
        endpoint_url: Solana RPC endpoint URL
    
    Returns:
        List of transaction signatures
    """
    if not endpoint_url:
        logger.error("No endpoint provided for fetching transactions")
        return []
    
    pubkey_cadena = Pubkey(base58.b58decode(cadena))
    transacciones_totales = []
    ultima_firma = None
    
    solana_client = Client(endpoint_url)
    
    while len(transacciones_totales) < limite:
        respuesta = solana_client.get_signatures_for_address(
            pubkey_cadena, before=ultima_firma, limit=1000
        )
        transacciones = respuesta.value
        
        if not transacciones:
            break
        
        transacciones_totales.extend(transacciones)
        
        if len(transacciones) < 1000:
            break
        
        ultima_firma = transacciones[-1].signature
    
    if len(transacciones_totales) > limite:
        transacciones_totales = transacciones_totales[:limite]
    
    return transacciones_totales


def get_solana_balance(user_wallet, endpoint_url):
    """Get SOL balance for a wallet."""
    if not endpoint_url:
        logger.error("No endpoint provided for balance check")
        return None
    
    solana_client = Client(endpoint_url)
    pubkey = Pubkey.from_string(user_wallet)
    response = solana_client.get_balance(pubkey)
    balance = response.value / 1_000_000_000
    
    return "{:.4f}".format(balance)


def find_wallet_and_mint_in_transaction(transaction_data):
    """Extract wallet and mint addresses from transaction instructions."""
    instructions = (transaction_data.get('result', {})
                   .get('transaction', {})
                   .get('message', {})
                   .get('instructions', []))
    
    for instruction in instructions:
        if 'parsed' in instruction:
            info = instruction['parsed'].get('info', {})
            if 'wallet' in info and 'mint' in info:
                return info['wallet'], info['mint']
    
    return None, None


def extract_received_tokens(transaction_meta, user_wallet, transaction_data):
    """
    Extract received tokens from a transaction.
    
    Returns:
        Tuple of (received_tokens, token_mint_address)
    """
    _, relevant_mint_address = find_wallet_and_mint_in_transaction(transaction_data)
    
    post_balances = transaction_meta.get('postTokenBalances', [])
    pre_balances = transaction_meta.get('preTokenBalances', [])
    received_tokens = None
    token_mint_address = None
    
    excluded_mints = [USDC_MINT_ADDRESS, SOL_MINT_ADDRESS, USDT_MINT_ADDRESS, relevant_mint_address]
    
    # First pass: look for tokens owned by user
    for post_entry in post_balances:
        if (post_entry.get('owner') == user_wallet and 
            post_entry.get('mint') not in excluded_mints):
            
            account_index = post_entry.get('accountIndex')
            pre_entry = next(
                (item for item in pre_balances if item.get('accountIndex') == account_index),
                None
            )
            post_amount = float(post_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
            
            if pre_entry:
                pre_amount = float(pre_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
                change = post_amount - pre_amount
            else:
                change = post_amount
            
            if change != 0:
                received_tokens = change
                token_mint_address = post_entry.get('mint')
                break
    
    # Second pass: check Raydium pool
    if received_tokens is None or received_tokens == 0:
        excluded_mints_no_relevant = [USDC_MINT_ADDRESS, SOL_MINT_ADDRESS, USDT_MINT_ADDRESS]
        
        for post_entry in reversed(post_balances):
            if (post_entry.get('owner') == RAYDIUM_POOL_ADDRESS and 
                post_entry.get('mint') not in excluded_mints_no_relevant):
                
                account_index = post_entry.get('accountIndex')
                pre_entry = next(
                    (item for item in pre_balances if item.get('accountIndex') == account_index),
                    None
                )
                post_amount = float(post_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
                
                if pre_entry:
                    pre_amount = float(pre_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
                    change = max(post_amount, pre_amount) - min(post_amount, pre_amount)
                    received_tokens = change
                    token_mint_address = post_entry.get('mint')
                    break
    
    # Discard if it's a sale matching relevant mint
    if received_tokens and received_tokens < 0 and token_mint_address == relevant_mint_address:
        received_tokens = None
    
    return received_tokens, token_mint_address


def extract_transaction_details(transaction_meta, user_wallet, block_time_unix, 
                                transaction_instructions, transaction_data):
    """
    Extract detailed transaction information.
    
    Returns:
        Tuple containing transaction details or None values if should be omitted
    """
    fee_lamports = transaction_meta.get('fee', 0)
    fee_sol = fee_lamports / 1_000_000_000
    swap_sol_used_list = []
    transaction_type = 'compra'
    block_time_formatted = datetime.fromtimestamp(
        block_time_unix, timezone.utc
    ).strftime('%Y-%m-%d %H:%M:%S') + ' UTC'
    
    received_tokens, token_mint_address = extract_received_tokens(
        transaction_meta, user_wallet, transaction_data
    )
    
    if received_tokens is None:
        return (fee_sol, 0, 0, None, transaction_type, block_time_formatted, 
                'No aplicable', 'No aplicable', True)
    
    # Find SOL used in Raydium pool
    for post_entry in transaction_meta.get('postTokenBalances', []):
        if (post_entry.get('owner') == RAYDIUM_POOL_ADDRESS and 
            post_entry.get('mint') == SOL_MINT_ADDRESS):
            
            account_index = post_entry.get('accountIndex')
            pre_entry = next(
                (item for item in transaction_meta.get('preTokenBalances', []) 
                 if item.get('accountIndex') == account_index),
                None
            )
            
            post_balance = float(post_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
            if pre_entry:
                pre_balance = float(pre_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
                swap_sol_used_list.append(post_balance - pre_balance)
            else:
                swap_sol_used_list.append(post_balance)
            break
    
    # Search in inner instructions if not found
    if not swap_sol_used_list:
        _, received_token_mint = extract_received_tokens(
            transaction_meta, user_wallet, transaction_data
        )
        
        for inner_instruction in transaction_meta.get('innerInstructions', []):
            for instruction in inner_instruction['instructions']:
                if 'parsed' in instruction:
                    info = instruction['parsed']['info']
                    if (info.get('authority') == user_wallet and 
                        info.get('mint') != received_token_mint):
                        destination = info.get('destination')
                        for post_instruction in transaction_instructions:
                            if 'parsed' in post_instruction:
                                post_info = post_instruction['parsed']['info']
                                if post_info.get('source') == destination:
                                    if ('mint' in post_info and 
                                        post_info['mint'] == SOL_MINT_ADDRESS):
                                        swap_sol_used_list.append(
                                            float(post_info['tokenAmount']['uiAmountString'])
                                        )
                                    elif 'amount' in post_info:
                                        swap_sol_used_list.append(
                                            float(post_info['amount']) / 1_000_000_000
                                        )
    
    # Use the last SOL value if multiple found
    if swap_sol_used_list:
        swap_sol_used = swap_sol_used_list[-1]
    else:
        swap_sol_used = _find_sol_in_inner_instructions(
            transaction_meta, user_wallet, token_mint_address, received_tokens
        )
    
    if swap_sol_used is None or swap_sol_used == 0:
        return (fee_sol, None, received_tokens, get_token_name(token_mint_address), 
                transaction_type, block_time_formatted, 'No aplicable', 'No aplicable', True)
    
    # Handle alternative SOL calculation
    swap_sol_used = _handle_alternative_sol_calculation(
        swap_sol_used, received_tokens, transaction_meta, token_mint_address
    )
    
    # Correct signs based on transaction type
    if received_tokens > 0 and swap_sol_used < 0:
        swap_sol_used = abs(swap_sol_used)
    
    if received_tokens < 0 and swap_sol_used > 0:
        swap_sol_used = -swap_sol_used
    
    # Determine labels
    if received_tokens < 0:
        sol_label = 'Sol received'
        tokens_label = 'Tokens sold'
    else:
        sol_label = 'Sol used'
        tokens_label = 'Tokens received'
    
    token_name = get_token_name(token_mint_address)
    
    return (fee_sol, swap_sol_used, received_tokens, token_name, transaction_type, 
            block_time_formatted, token_mint_address, sol_label, tokens_label, False)


def _find_sol_in_inner_instructions(transaction_meta, user_wallet, token_mint_address, received_tokens):
    """Helper to find SOL amount in inner instructions."""
    swap_sol_used = None
    received_tokens_str = ''.join(filter(str.isdigit, str(received_tokens)))
    
    for inner_instruction in transaction_meta.get('innerInstructions', []):
        for instruction in inner_instruction['instructions']:
            if 'parsed' in instruction:
                parsed_info = instruction['parsed']
                info = parsed_info['info']
                
                if (parsed_info['type'] in ['transfer', 'transferChecked'] and 
                    info.get('authority') == user_wallet):
                    if not token_mint_address or info.get('mint') != token_mint_address:
                        amount = float(info.get('amount') or 
                                      info.get('tokenAmount', {}).get('amount', 0))
                        swap_sol_used = amount / 1_000_000_000
                        
                        swap_sol_used_str = ''.join(filter(str.isdigit, str(swap_sol_used)))
                        if swap_sol_used_str[:-2] == received_tokens_str[:-2]:
                            return (0, 0, received_tokens, get_token_name(token_mint_address),
                                   'compra', '', 'No aplicable', 'No aplicable', True)
                        else:
                            swap_sol_used = None
        
        if swap_sol_used is not None:
            break
    
    # Try finding any SOL transfer
    if swap_sol_used is None:
        for inner_instruction in transaction_meta.get('innerInstructions', []):
            for instruction in inner_instruction['instructions']:
                if 'parsed' in instruction:
                    parsed_info = instruction['parsed']
                    info = parsed_info['info']
                    if (parsed_info['type'] in ['transfer', 'transferChecked'] and 
                        info.get('mint') == SOL_MINT_ADDRESS):
                        amount = info.get('amount') or info.get('tokenAmount', {}).get('amount', 0)
                        swap_sol_used = float(amount) / 1_000_000_000
                        break
            if swap_sol_used is not None:
                break
    
    # Check post token balances
    if swap_sol_used is None:
        for post_entry in reversed(transaction_meta.get('postTokenBalances', [])):
            if post_entry.get('mint') == SOL_MINT_ADDRESS:
                account_index = post_entry.get('accountIndex')
                pre_entry = next(
                    (item for item in transaction_meta.get('preTokenBalances', []) 
                     if item.get('accountIndex') == account_index),
                    None
                )
                
                post_amount = float(post_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
                if pre_entry:
                    pre_amount = float(pre_entry.get('uiTokenAmount', {}).get('uiAmountString', '0'))
                    swap_sol_used = post_amount - pre_amount
                    break
    
    # Check system program transfers
    if swap_sol_used is None:
        for inner_instruction in transaction_meta.get('innerInstructions', []):
            for instruction in inner_instruction['instructions']:
                if ('parsed' in instruction and
                    instruction['parsed'].get('type') == 'transfer' and
                    instruction['parsed']['info'].get('source') == user_wallet and
                    instruction['programId'] == '11111111111111111111111111111111'):
                    swap_sol_used = instruction['parsed']['info']['lamports'] / 1_000_000_000
                    break
                if swap_sol_used is not None:
                    break
            if swap_sol_used is not None:
                break
    
    return swap_sol_used


def _handle_alternative_sol_calculation(swap_sol_used, received_tokens, transaction_meta, token_mint_address):
    """Handle alternative SOL calculation when values match."""
    if swap_sol_used is None or received_tokens is None:
        return swap_sol_used
    
    swap_sol_used_str = ''.join(filter(str.isdigit, str(swap_sol_used)))
    received_tokens_str = ''.join(filter(str.isdigit, str(received_tokens)))
    
    if swap_sol_used_str == received_tokens_str:
        swap_sol_used_alternative = None
        
        for inner_instruction in transaction_meta.get('innerInstructions', []):
            for instruction in inner_instruction['instructions']:
                if 'parsed' in instruction and instruction['parsed']['type'] == 'transfer':
                    info = instruction['parsed']['info']
                    info_amount_str = ''.join(filter(str.isdigit, str(info.get('amount', '0'))))
                    
                    if (info_amount_str != received_tokens_str and 
                        info.get('mint', '') != token_mint_address):
                        amount = info.get('amount', 0)
                        swap_sol_used_alternative = float(amount) / 1_000_000_000
                        
                        if info_amount_str[:-2] == received_tokens_str[:-2]:
                            break
                        else:
                            swap_sol_used = None
            
            if swap_sol_used_alternative is not None:
                return swap_sol_used_alternative
    
    return swap_sol_used


def process_transaction(signature, user_wallet, fecha_inicio, endpoint_url, 
                       attempt=1, max_attempts=3):
    """
    Process a single transaction and extract relevant information.
    
    Args:
        signature: Transaction signature
        user_wallet: User's wallet address
        fecha_inicio: Start date for analysis
        endpoint_url: Solana RPC endpoint
        attempt: Current attempt number
        max_attempts: Maximum retry attempts
    
    Returns:
        Dictionary with transaction details or None
    """
    global mint_address_cache
    
    if not endpoint_url:
        logger.error("No valid endpoint provided")
        return None
    
    solana_client = Client(endpoint_url)
    
    try:
        response = solana_client.get_transaction(
            signature, "jsonParsed", max_supported_transaction_version=0
        )
        response_json = json.loads(response.to_json())
        
        if 'result' not in response_json or not response_json['result']:
            return None
        
        transaction_meta = response_json['result']['meta']
        block_time_unix = response_json['result']['blockTime']
        fecha_transaccion = datetime.fromtimestamp(block_time_unix, timezone.utc)
        
        if fecha_transaccion < fecha_inicio:
            return "Transacción omitida por fecha."
        
        if (transaction_meta.get('err') is not None or 
            any("failed" in log for log in transaction_meta.get('logMessages', []))):
            return None
        
        inner_instructions = transaction_meta.get('innerInstructions', [])
        unique_indexes = len(set(i['index'] for i in inner_instructions))
        total_parsed_instructions = sum(len(i['instructions']) for i in inner_instructions)
        
        if unique_indexes <= 1 and total_parsed_instructions < 2:
            return None
        
        inner_instructions_list = []
        for inner_instruction in inner_instructions:
            for instruction in inner_instruction['instructions']:
                inner_instructions_list.append(instruction)
        
        result = extract_transaction_details(
            transaction_meta, user_wallet, block_time_unix, 
            inner_instructions_list, response_json['result']
        )
        
        (fee_sol, swap_sol_used, received_tokens, token_name, transaction_type, 
         block_time_formatted, token_mint_address, sol_label, tokens_label, 
         omit_transaction) = result
        
        if token_mint_address:
            if token_mint_address in mint_address_cache:
                token_name = mint_address_cache[token_mint_address]
            else:
                token_name = get_token_name(token_mint_address)
                mint_address_cache[token_mint_address] = token_name
        
        if not omit_transaction:
            return {
                'token_name': token_name,
                'amount_tokens': received_tokens,
                'sol_amount': swap_sol_used,
                'fee': fee_sol,
                'date': block_time_formatted,
                'type': 'buy' if received_tokens > 0 else 'sell',
                'timestamp': block_time_unix,
                'mint_address': token_mint_address
            }
        
    except Exception as e:
        if attempt < max_attempts:
            sleep_time = min(2 ** attempt, 60) + (uniform(0, 1) * 0.1)
            sleep(sleep_time)
            return process_transaction(
                signature, user_wallet, fecha_inicio, endpoint_url, 
                attempt + 1, max_attempts
            )
        logger.error(f"Failed to process transaction after {max_attempts} attempts: {e}")
    
    return None


# =============================================================================
# Celery Task
# =============================================================================

@app.task(bind=True)
def analizar_wallet(self, user_wallet, dias_a_analizar):
    """
    Celery task to analyze a Solana wallet's trading history.
    
    Args:
        user_wallet: Wallet address to analyze
        dias_a_analizar: Number of days to look back
    
    Returns:
        Dictionary containing analysis results
    """
    try:
        endpoint_url = endpoint_manager.acquire_endpoint_with_retry()
    except Exception as e:
        logger.error(f"Failed to acquire endpoint: {str(e)}")
        raise self.retry(countdown=10)
    
    self.update_state(state='PROGRESS', meta={'endpoint_url': endpoint_url})
    logger.info(f"Task {self.request.id} started with endpoint: {endpoint_url[:50]}...")
    
    try:
        fecha_inicio = datetime.now(timezone.utc) - timedelta(days=dias_a_analizar)
        balance = get_solana_balance(user_wallet, endpoint_url)
        firmas = obtener_transacciones(user_wallet, limite=3000, endpoint_url=endpoint_url)
        transactions_summary = {}
        
        contador_omisiones = 0
        max_omisiones = 6
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_signature = {
                executor.submit(
                    process_transaction, firma.signature, user_wallet, 
                    fecha_inicio, endpoint_url
                ): firma
                for firma in firmas
            }
            
            for future in as_completed(future_to_signature):
                # Check for task revocation
                if redis_client.get(f"task_revoked:{self.request.id}"):
                    logger.info(f"Task {self.request.id} revoked, stopping execution")
                    for f in future_to_signature:
                        if not f.done():
                            f.cancel()
                    return "Tarea revocada."
                
                try:
                    transaction = future.result()
                    
                    if transaction == "Transacción omitida por fecha.":
                        contador_omisiones += 1
                        if contador_omisiones > max_omisiones:
                            for f in future_to_signature:
                                if not f.done():
                                    f.cancel()
                            break
                        continue
                    
                    contador_omisiones = 0
                    
                    if transaction:
                        _update_transactions_summary(transactions_summary, transaction)
                
                except Exception as e:
                    logger.error(f"Error processing transaction: {e}")
                    continue
        
        # Calculate profits
        _calculate_profits(transactions_summary)
        
        # Get current SOL price
        sol_price_usd = get_current_sol_price_usd()
        
        if sol_price_usd is not None:
            _calculate_usd_profits(transactions_summary, sol_price_usd)
        
        _calculate_profit_percentages(transactions_summary)
        
        # Remove tokens with only sells
        transactions_summary = {
            k: v for k, v in transactions_summary.items() 
            if v['buy_count'] > 0
        }
        
        # Calculate totals
        total_profit_sol = sum(
            data.get('profit_sol', 0) for data in transactions_summary.values()
        )
        total_profit_usd = sum(
            data.get('profit_usd', 0) for data in transactions_summary.values()
        )
        
        # Calculate average buy
        total_sol_in_buys = sum(
            data['buy']['sol_amount'] for data in transactions_summary.values() 
            if data['buy_count'] > 0
        )
        total_tokens_with_buys = sum(
            1 for data in transactions_summary.values() if data['buy_count'] > 0
        )
        average_sol_per_buy = (
            total_sol_in_buys / total_tokens_with_buys 
            if total_tokens_with_buys > 0 else 0
        )
        
        # Build results
        resultados = _build_results(
            balance, total_profit_sol, total_profit_usd, 
            average_sol_per_buy, transactions_summary, user_wallet
        )
        
        return resultados
        
    finally:
        try:
            if endpoint_url:
                endpoint_manager.release_endpoint(endpoint_url)
                logger.info(f"Endpoint released for task {self.request.id}")
                redis_client.delete(f"task_revoked:{self.request.id}")
        except Exception as e:
            logger.error(f"Error in cleanup: {e}")


def _update_transactions_summary(transactions_summary, transaction):
    """Update transactions summary with a new transaction."""
    token_name = transaction['token_name']
    token_mint_address = transaction['mint_address']
    tran_type = transaction['type']
    
    token_key = f"{token_name}_{token_mint_address}"
    
    try:
        token_name, token_mint_address = token_key.split('_', 1)
    except ValueError as e:
        logger.error(f"Error splitting token_key: {token_key}, Error: {e}")
        return
    
    if token_key not in transactions_summary:
        transactions_summary[token_key] = {
            'buy_count': 0,
            'sell_count': 0,
            'buy': {'sol_amount': 0, 'tokens': 0, 'fee': 0, 'first_date': None, 'last_date': None},
            'sell': {'sol_amount': 0, 'tokens': 0, 'fee': 0, 'first_date': None, 'last_date': None},
            'mint_address': token_mint_address
        }
    
    summary = transactions_summary[token_key][tran_type]
    
    summary['sol_amount'] += transaction['sol_amount']
    summary['tokens'] += transaction['amount_tokens']
    summary['fee'] += transaction['fee']
    
    if tran_type == 'buy':
        if summary['first_date'] is None or transaction['timestamp'] < summary['first_date']:
            summary['first_date'] = transaction['timestamp']
        transactions_summary[token_key]['buy_count'] += 1
    elif tran_type == 'sell':
        if summary['last_date'] is None or transaction['timestamp'] > summary['last_date']:
            summary['last_date'] = transaction['timestamp']
        transactions_summary[token_key]['sell_count'] += 1


def _calculate_profits(transactions_summary):
    """Calculate SOL profits for each token."""
    for token, data in transactions_summary.items():
        total_sold_sol = abs(data['sell']['sol_amount'])
        total_bought_sol = data['buy']['sol_amount']
        data['profit_sol'] = total_sold_sol - total_bought_sol


def _calculate_usd_profits(transactions_summary, sol_price_usd):
    """Calculate USD profits for each token."""
    for token, data in transactions_summary.items():
        if 'profit_sol' in data:
            data['profit_usd'] = data['profit_sol'] * sol_price_usd


def _calculate_profit_percentages(transactions_summary):
    """Calculate profit percentages for each token."""
    for token, data in transactions_summary.items():
        if 'profit_sol' in data:
            total_bought_sol = data['buy']['sol_amount']
            if total_bought_sol != 0:
                data['profit_percentage'] = (data['profit_sol'] / total_bought_sol) * 100
            else:
                data['profit_percentage'] = 0


def _build_results(balance, total_profit_sol, total_profit_usd, 
                   average_sol_per_buy, transactions_summary, user_wallet):
    """Build the final results dictionary."""
    resultados = {
        'balance_SOL': "{:.2f}".format(float(balance)),
        'total_profit_SOL': total_profit_sol,
        'total_profit_USD': total_profit_usd,
        'average_final_SOL_per_buy': f"{average_sol_per_buy:.2f} SOL",
        'tokens_summary': [],
        'formatted_total_profit_SOL': f"{total_profit_sol:.2f}",
        'formatted_total_profit_USD': f"${total_profit_usd:.2f}",
    }
    
    for token_key, data in transactions_summary.items():
        token_name, token_mint_address = token_key.split('_', 1)
        
        token_data = {
            'name': token_name,
            'buy_sell': f"{data['buy_count']}/{data['sell_count']}",
            'details': [],
            'profit_SOL': data.get('profit_sol'),
            'profit_USD': data.get('profit_usd'),
            'profit_percentage': data.get('profit_percentage'),
            'formatted_profit_SOL': f"{data['profit_sol']:.2f}" if 'profit_sol' in data else "N/A",
            'formatted_profit_USD': f"${data['profit_usd']:.2f}" if 'profit_usd' in data else "N/A",
            'formatted_profit_percentage': f"{data['profit_percentage']:.2f}%" if 'profit_percentage' in data else "N/A",
            'chart_url': f"https://dexscreener.com/solana/{data['mint_address']}?maker={user_wallet}" if 'mint_address' in data else "#"
        }
        
        for tran_type in ['buy', 'sell']:
            if tran_type in data and data[tran_type]['tokens'] != 0:
                tran_details = {
                    'type': tran_type,
                    'first_date': (
                        datetime.fromtimestamp(data[tran_type]['first_date'], timezone.utc)
                        .strftime('%Y-%m-%d %H:%M:%S') + ' UTC' 
                        if data[tran_type]['first_date'] else "N/A"
                    ),
                    'last_date': (
                        datetime.fromtimestamp(data[tran_type]['last_date'], timezone.utc)
                        .strftime('%Y-%m-%d %H:%M:%S') + ' UTC' 
                        if data[tran_type]['last_date'] else "N/A"
                    ),
                    'SOL': f"{abs(data[tran_type]['sol_amount']):.2f}",
                    'tokens': f"{abs(data[tran_type]['tokens']):,.2f}",
                    'fee': f"{data[tran_type]['fee']:.6f}"
                }
                token_data['details'].append(tran_details)
        
        resultados['tokens_summary'].append(token_data)
    
    return resultados
