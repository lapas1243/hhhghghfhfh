import logging
import json
import time
import asyncio
import requests
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from solana.rpc.api import Client
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import Transaction
from utils import get_db_connection, send_message_with_retry, format_currency, send_purchase_log

# --- CONFIGURATION ---
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
ADMIN_WALLET = os.getenv("SOLANA_ADMIN_WALLET")  # Must be set in environment
RECOVERY_WALLET = os.getenv("SOLANA_RECOVERY_WALLET")  # Optional: For recovering stuck funds
ENABLE_AUTO_SWEEP = True  # Automatically send funds to admin wallet after payment

logger = logging.getLogger(__name__)
client = Client(SOLANA_RPC_URL)


def get_incoming_tx_signature(wallet_address: str) -> str | None:
    """
    Get the most recent incoming transaction signature for a wallet.
    Used for generating Solscan links in payment logs.
    
    Args:
        wallet_address: The public key of the wallet to check
        
    Returns:
        Transaction signature string or None if not found
    """
    try:
        pubkey = Pubkey.from_string(wallet_address)
        # Get recent signatures for this address (limit to 5 for efficiency)
        signatures_response = client.get_signatures_for_address(pubkey, limit=5)
        
        if signatures_response.value and len(signatures_response.value) > 0:
            # Return the most recent transaction signature
            tx_sig = str(signatures_response.value[0].signature)
            logger.info(f"📋 Found incoming tx signature for {wallet_address[:16]}...: {tx_sig[:20]}...")
            return tx_sig
        else:
            logger.debug(f"No transactions found for wallet {wallet_address[:16]}...")
            return None
            
    except Exception as e:
        logger.warning(f"Could not fetch tx signature for {wallet_address[:16]}...: {e}")
        return None


# ===== PRODUCTION-GRADE PRICE CACHING SYSTEM =====
_price_cache = {'price': None, 'timestamp': 0, 'last_api_used': None}
PRICE_CACHE_TTL = 300  # 5 minutes cache
STALE_CACHE_MAX_AGE = 3600  # Accept stale cache up to 1 hour if all APIs fail

def get_sol_price_from_db():
    """Get cached price from database (survives restarts)"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT setting_value, updated_at 
            FROM bot_settings 
            WHERE setting_key = 'sol_price_eur_cache'
        """)
        result = c.fetchone()
        conn.close()
        
        if result:
            price = Decimal(str(result['setting_value']))
            cache_age = time.time() - result['updated_at'].timestamp()
            if cache_age < 600:  # 10 minutes
                logger.info(f"📊 DB cached SOL price: {price} EUR (age: {int(cache_age)}s)")
                return price
    except Exception as e:
        logger.debug(f"Could not fetch DB price cache: {e}")
    return None

def save_sol_price_to_db(price):
    """Save price to database for persistence"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        # Use proper timestamp format for SQLite
        c.execute("""
            INSERT OR REPLACE INTO bot_settings (setting_key, setting_value, updated_at)
            VALUES ('sol_price_eur_cache', ?, datetime('now'))
        """, (str(price),))
        conn.commit()
        conn.close()
        logger.debug(f"💾 Saved SOL price to DB: {price} EUR")
    except Exception as e:
        logger.debug(f"Could not save price to DB: {e}")

def fetch_price_from_api(api_name, url, parser_func):
    """Generic API fetcher with timeout and error handling"""
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            price = parser_func(response.json())
            if price:
                logger.info(f"✅ {api_name} SOL price: {price} EUR")
                return price
        elif response.status_code == 429:
            logger.warning(f"⚠️ {api_name} rate limited (429)")
        else:
            logger.warning(f"⚠️ {api_name} returned status {response.status_code}")
    except requests.Timeout:
        logger.warning(f"⏱️ {api_name} timeout")
    except Exception as e:
        logger.debug(f"{api_name} error: {e}")
    return None


def _get_jupiter_sol_price():
    """
    Get SOL price from Jupiter - Solana's native DEX aggregator.
    This is the most reliable source for SOL prices - no rate limits, always available.
    Returns price in USD.
    """
    # SOL mint address (wrapped SOL)
    SOL_MINT = "So11111111111111111111111111111111111111112"
    
    try:
        # Jupiter Price API v2 - very reliable, no API key needed
        url = f"https://api.jup.ag/price/v2?ids={SOL_MINT}"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and SOL_MINT in data['data']:
                price = Decimal(str(data['data'][SOL_MINT]['price']))
                logger.info(f"✅ Jupiter SOL price: ${price}")
                return price
        else:
            logger.warning(f"⚠️ Jupiter returned status {response.status_code}")
    except Exception as e:
        logger.warning(f"⚠️ Jupiter error: {e}")
    
    return None


def _get_sol_usd_price():
    """Get SOL price in USD from multiple sources - Jupiter first (most reliable)"""
    
    # Try Jupiter first - it's Solana native and most reliable
    jupiter_price = _get_jupiter_sol_price()
    if jupiter_price:
        return jupiter_price
    
    # Fallback to other USD APIs
    usd_apis = [
        ('Kraken-USD', 'https://api.kraken.com/0/public/Ticker?pair=SOLUSD',
         lambda d: Decimal(str(d['result']['SOLUSD']['c'][0])) if 'result' in d and 'SOLUSD' in d.get('result', {}) else None),
        
        ('KuCoin-USD', 'https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=SOL-USDT',
         lambda d: Decimal(str(d['data']['price'])) if d.get('data', {}).get('price') else None),
        
        ('Binance-USD', 'https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT',
         lambda d: Decimal(str(d['price'])) if 'price' in d else None),
        
        ('CoinGecko-USD', 'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd',
         lambda d: Decimal(str(d['solana']['usd'])) if 'solana' in d and 'usd' in d['solana'] else None),
    ]
    
    for api_name, url, parser in usd_apis:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                price = parser(response.json())
                if price:
                    logger.info(f"✅ {api_name}: ${price}")
                    return price
            else:
                logger.debug(f"{api_name} returned status {response.status_code}")
        except Exception as e:
            logger.debug(f"{api_name} error: {e}")
    
    logger.warning("⚠️ All USD price sources failed")
    return None


def _get_eur_usd_rate():
    """Get EUR/USD exchange rate from reliable sources"""
    eur_apis = [
        ('ECB-Frankfurter', 'https://api.frankfurter.app/latest?from=USD&to=EUR',
         lambda d: Decimal(str(d['rates']['EUR'])) if 'rates' in d and 'EUR' in d['rates'] else None),
        
        ('ExchangeRate-API', 'https://open.er-api.com/v6/latest/USD',
         lambda d: Decimal(str(d['rates']['EUR'])) if 'rates' in d and 'EUR' in d['rates'] else None),
         
        ('Fixer-Free', 'https://api.exchangerate.host/latest?base=USD&symbols=EUR',
         lambda d: Decimal(str(d['rates']['EUR'])) if 'rates' in d and 'EUR' in d['rates'] else None),
    ]
    
    for api_name, url, parser in eur_apis:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                rate = parser(response.json())
                if rate and rate > Decimal("0.5") and rate < Decimal("1.5"):  # Sanity check
                    logger.info(f"✅ {api_name} EUR/USD rate: {rate}")
                    return rate
        except Exception as e:
            logger.debug(f"{api_name} error: {e}")
    
    # Fallback to approximate rate if all APIs fail
    logger.warning("⚠️ EUR/USD rate APIs failed, using fallback rate 0.92")
    return Decimal("0.92")  # Approximate EUR/USD rate as fallback

def get_sol_price_eur():
    """
    PRODUCTION-GRADE: Multi-layer caching + smart API rotation
    
    Strategy:
    1. Check memory cache (instant, 5 min TTL)
    2. Check DB cache (fast, 10 min TTL)
    3. Try APIs in rotation (avoid hammering one)
    4. Use stale cache up to 1 hour (last resort)
    """
    now = time.time()
    
    # Layer 1: Memory cache
    if _price_cache['price'] and (now - _price_cache['timestamp']) < PRICE_CACHE_TTL:
        cache_age = int(now - _price_cache['timestamp'])
        logger.info(f"💰 Memory cached SOL price: {_price_cache['price']} EUR (age: {cache_age}s)")
        return _price_cache['price']
    
    # Layer 2: Database cache
    db_price = get_sol_price_from_db()
    if db_price:
        _price_cache['price'] = db_price
        _price_cache['timestamp'] = now
        return db_price
    
    # Layer 3: PRIMARY METHOD - Jupiter (Solana native) + USD conversion
    # This is the most reliable method - Jupiter never fails and USD APIs are more available
    logger.debug("Trying Jupiter + USD conversion (primary method)...")
    usd_price = _get_sol_usd_price()  # Jupiter is tried first inside this function
    if usd_price:
        eur_usd_rate = _get_eur_usd_rate()
        if eur_usd_rate:
            price = (usd_price * eur_usd_rate).quantize(Decimal("0.01"))
            logger.info(f"✅ SOL price: {price} EUR (${usd_price} × {eur_usd_rate})")
            _price_cache['price'] = price
            _price_cache['timestamp'] = now
            save_sol_price_to_db(price)
            return price
    
    # Layer 3.5: Fallback to direct EUR pairs (less reliable due to geo-blocking)
    logger.debug("Jupiter failed, trying direct EUR APIs as fallback...")
    eur_apis = [
        ('Kraken-EUR', 'https://api.kraken.com/0/public/Ticker?pair=SOLEUR',
         lambda data: Decimal(str(data['result']['SOLEUR']['c'][0])) if 'result' in data and 'SOLEUR' in data.get('result', {}) else None),
        
        ('CryptoCompare-EUR', 'https://min-api.cryptocompare.com/data/price?fsym=SOL&tsyms=EUR',
         lambda data: Decimal(str(data['EUR'])) if 'EUR' in data else None),
    ]
    
    for api_name, url, parser in eur_apis:
        price = fetch_price_from_api(api_name, url, parser)
        if price:
            _price_cache['price'] = price
            _price_cache['timestamp'] = now
            save_sol_price_to_db(price)
            return price
    
    # Layer 4: Stale cache (up to 1 hour old)
    if _price_cache['price']:
        age = int(now - _price_cache['timestamp'])
        if age < STALE_CACHE_MAX_AGE:
            logger.warning(f"⚠️ All APIs failed, using stale cache ({age}s old): {_price_cache['price']} EUR")
            return _price_cache['price']
        else:
            logger.error(f"❌ Stale cache too old ({age}s), cannot use")
    
    logger.error(f"❌ CRITICAL: All price sources failed!")
    return None

async def refresh_price_cache(context=None):
    """
    Background job: Proactively refresh price cache every 4 minutes
    """
    logger.info("🔄 Background price refresh triggered")
    
    old_timestamp = _price_cache['timestamp']
    _price_cache['timestamp'] = 0
    
    price = get_sol_price_eur()
    
    if price:
        logger.info(f"✅ Background refresh successful: {price} EUR")
    else:
        logger.warning(f"⚠️ Background refresh failed, restoring old cache")
        _price_cache['timestamp'] = old_timestamp

async def create_solana_payment(user_id, order_id, eur_amount):
    """
    Generates a unique SOL wallet for this transaction.
    Returns: dict with address, amount, and payment_id
    """
    price = get_sol_price_eur()
    if not price:
        logger.error("Could not fetch SOL price")
        return {'error': 'estimate_failed'}

    # Calculate SOL amount
    sol_amount = (Decimal(eur_amount) / price).quantize(Decimal("0.00001"))
    
    # Generate new Keypair
    kp = Keypair()
    pubkey = str(kp.pubkey())
    private_key_json = json.dumps(list(bytes(kp)))

    conn = get_db_connection()
    c = conn.cursor()
    try:
        # Check if order_id already exists
        c.execute("SELECT public_key, expected_amount FROM solana_wallets WHERE order_id = ?", (order_id,))
        existing = c.fetchone()
        
        if existing:
            logger.info(f"Found existing Solana wallet for order {order_id}")
            return {
                'pay_address': existing['public_key'],
                'pay_amount': str(existing['expected_amount']),
                'pay_currency': 'SOL',
                'exchange_rate': float(price),
                'payment_id': order_id
            }

        c.execute("""
            INSERT INTO solana_wallets (user_id, order_id, public_key, private_key, expected_amount, status)
            VALUES (?, ?, ?, ?, ?, 'pending')
        """, (user_id, order_id, pubkey, private_key_json, float(sol_amount)))
        conn.commit()
    except Exception as e:
        logger.error(f"DB Error creating solana payment: {e}")
        return {'error': 'internal_server_error'}
    finally:
        conn.close()

    return {
        'pay_address': pubkey,
        'pay_amount': str(sol_amount),
        'pay_currency': 'SOL',
        'exchange_rate': float(price),
        'payment_id': order_id
    }

async def check_solana_deposits(context):
    """
    Background task to check all pending wallets for deposits.
    Call this periodically (e.g., every 30-60 seconds).
    """
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute("SELECT * FROM solana_wallets WHERE status = 'pending'")
        pending = c.fetchall()
        
        if not pending:
            return

        for wallet in pending:
            try:
                pubkey_str = wallet['public_key']
                expected = Decimal(str(wallet['expected_amount']))
                wallet_id = wallet['id']
                order_id = wallet['order_id']
                user_id = wallet['user_id']
                created_at_str = wallet['created_at']
                
                # Parse created_at string to datetime
                try:
                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=timezone.utc)
                except (ValueError, AttributeError):
                    # Fallback: assume it's recent if parsing fails
                    created_at = datetime.now(timezone.utc)
                
                # Rate limit RPC calls
                await asyncio.sleep(0.2)
                
                # Check Balance
                try:
                    balance_resp = client.get_balance(Pubkey.from_string(pubkey_str))
                    lamports = balance_resp.value
                    sol_balance = Decimal(lamports) / Decimal(10**9)
                except Exception as rpc_e:
                    logger.warning(f"RPC Error checking wallet {pubkey_str}: {rpc_e}")
                    continue
                
                # 1. Check if Paid (allowing 99.5% tolerance)
                if sol_balance > 0 and sol_balance >= (expected * Decimal("0.995")):
                    logger.info(f"✅ Payment detected for Order {order_id}: {sol_balance} SOL")
                    
                    # Mark as Paid in DB
                    c.execute("UPDATE solana_wallets SET status = 'paid', amount_received = ?, updated_at = datetime('now') WHERE id = ?", (float(sol_balance), wallet_id))
                    conn.commit()
                    
                    # Get incoming transaction signature for Solscan link
                    tx_signature = get_incoming_tx_signature(pubkey_str)
                    
                    # Get user info for logs
                    c.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
                    user_info = c.fetchone()
                    username = user_info['username'] if user_info and user_info['username'] else str(user_id)
                    
                    # Get EUR amount
                    price = get_sol_price_eur()
                    eur_amount = float(sol_balance * price) if price else None
                    
                    # Handle Overpayment
                    surplus = sol_balance - expected
                    if surplus > Decimal("0.0005"):
                        try:
                            if price:
                                surplus_eur = (surplus * price).quantize(Decimal("0.01"))
                                if surplus_eur > 0:
                                    logger.info(f"💰 Overpayment of {surplus} SOL ({surplus_eur} EUR) detected for {order_id}")
                                    from payment import credit_user_balance
                                    await credit_user_balance(user_id, surplus_eur, f"Overpayment bonus for order {order_id}", context)
                        except Exception as over_e:
                            logger.error(f"Error processing overpayment: {over_e}")
                    
                    # 2. Trigger Payment Success Logic
                    from payment import process_successful_crypto_purchase, process_successful_refill
                    
                    c.execute("SELECT is_purchase, basket_snapshot_json as basket_snapshot, discount_code_used as discount_code, target_eur_amount FROM pending_deposits WHERE payment_id = ?", (order_id,))
                    deposit_info = c.fetchone()
                    
                    if deposit_info:
                        is_purchase = deposit_info['is_purchase']
                        payment_type = "purchase" if is_purchase else "refill"
                        target_eur = float(deposit_info['target_eur_amount']) if deposit_info['target_eur_amount'] else eur_amount
                        
                        if is_purchase:
                            basket_snapshot = deposit_info['basket_snapshot'] if 'basket_snapshot' in deposit_info.keys() else None
                            if isinstance(basket_snapshot, str):
                                try:
                                    basket_snapshot = json.loads(basket_snapshot)
                                except:
                                    pass
                                
                            discount_code = deposit_info['discount_code'] if 'discount_code' in deposit_info.keys() else None
                            
                            await process_successful_crypto_purchase(user_id, basket_snapshot, discount_code, order_id, context)
                        else:
                            # Refill
                            amount_eur = Decimal(str(deposit_info['target_eur_amount'])) if deposit_info['target_eur_amount'] else Decimal("0.0")
                            await process_successful_refill(user_id, amount_eur, order_id, context)
                        
                        # 📋 Send purchase log to logs channel
                        try:
                            await send_purchase_log(
                                bot=context.bot,
                                user_id=user_id,
                                username=username,
                                amount_paid=float(sol_balance),
                                currency="SOL",
                                payment_id=order_id,
                                is_success=True,
                                payment_type=payment_type,
                                tx_signature=tx_signature,
                                eur_amount=target_eur,
                                basket_snapshot=basket_snapshot if is_purchase else None
                            )
                        except Exception as log_e:
                            logger.warning(f"Failed to send purchase log for {order_id}: {log_e}")
                    else:
                        logger.error(f"Could not find pending_deposit record for solana order {order_id}")
                    
                    # 3. Sweep Funds
                    if ENABLE_AUTO_SWEEP and ADMIN_WALLET:
                        asyncio.create_task(sweep_wallet(wallet, lamports))
                
                # 2. Check for Underpayment - IMMEDIATE
                elif sol_balance > 0:
                    logger.info(f"📉 Underpayment detected for {order_id} ({sol_balance} SOL). Refunding immediately.")
                    try:
                        price = get_sol_price_eur()
                        if price:
                            refund_eur = (sol_balance * price).quantize(Decimal("0.01"))
                            if refund_eur > 0:
                                from payment import credit_user_balance
                                msg = f"⚠️ Underpayment detected ({sol_balance} SOL). Refunded {refund_eur} EUR to balance. Please use Top Up."
                                await send_message_with_retry(context.bot, user_id, msg, parse_mode=None)
                                await credit_user_balance(user_id, refund_eur, f"Underpayment refund {order_id}", context)
                                
                                # Mark as refunded
                                c.execute("UPDATE solana_wallets SET status = 'refunded', amount_received = ?, updated_at = datetime('now') WHERE id = ?", (float(sol_balance), wallet_id))
                                conn.commit()
                                
                                # Get user info and tx signature for logs
                                c.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
                                user_info = c.fetchone()
                                username = user_info['username'] if user_info and user_info['username'] else str(user_id)
                                tx_signature = get_incoming_tx_signature(pubkey_str)
                                
                                # 📋 Send failed payment log to logs channel
                                try:
                                    await send_purchase_log(
                                        bot=context.bot,
                                        user_id=user_id,
                                        username=username,
                                        amount_paid=float(sol_balance),
                                        currency="SOL",
                                        payment_id=order_id,
                                        is_success=False,
                                        payment_type="underpayment",
                                        tx_signature=tx_signature,
                                        eur_amount=float(refund_eur)
                                    )
                                except Exception as log_e:
                                    logger.warning(f"Failed to send underpayment log for {order_id}: {log_e}")
                                
                                # Sweep the partial funds
                                if ENABLE_AUTO_SWEEP and ADMIN_WALLET:
                                    asyncio.create_task(sweep_wallet(wallet, lamports))
                    except Exception as refund_e:
                        logger.error(f"Error refunding underpayment {order_id}: {refund_e}")

                # 3. Check for Expiration (Empty) - 20 minutes
                elif datetime.now(timezone.utc) - created_at > timedelta(minutes=20):
                    c.execute("UPDATE solana_wallets SET status = 'expired', updated_at = datetime('now') WHERE id = ?", (wallet_id,))
                    conn.commit()
                        
            except Exception as e:
                logger.error(f"Error checking wallet {wallet['public_key'] if 'public_key' in wallet.keys() else 'unknown'}: {e}", exc_info=True)
                
    except Exception as e:
        logger.error(f"Error in check_solana_deposits loop: {e}", exc_info=True)
    finally:
        conn.close()
        
    # RECOVERY: Check for 'paid' wallets that haven't been swept
    if ENABLE_AUTO_SWEEP and ADMIN_WALLET:
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT * FROM solana_wallets WHERE status = 'paid'")
            paid_wallets = c.fetchall()
            conn.close()
            
            for wallet in paid_wallets:
                asyncio.create_task(sweep_wallet(wallet))
        except Exception as e:
            logger.error(f"Error in sweep recovery loop: {e}")

async def sweep_wallet(wallet_data, current_lamports=0):
    """Moves funds from temp wallet to ADMIN_WALLET"""
    try:
        # Fetch balance if not provided
        if current_lamports == 0:
            try:
                balance_resp = client.get_balance(Pubkey.from_string(wallet_data['public_key']))
                current_lamports = balance_resp.value
            except Exception as e:
                logger.error(f"Error fetching balance for sweep {wallet_data['public_key']}: {e}")
                return

        if current_lamports < 5000:  # Ignore dust
            if wallet_data['status'] == 'paid' and current_lamports < 5000:
                conn = get_db_connection()
                conn.cursor().execute("UPDATE solana_wallets SET status = 'swept' WHERE id = ?", (wallet_data['id'],))
                conn.commit()
                conn.close()
            return

        # Load Keypair
        priv_key_list = json.loads(wallet_data['private_key'])
        kp = Keypair.from_bytes(bytes(priv_key_list))
        
        # Calculate fee
        fee = 5000
        amount_to_send = current_lamports - fee
        
        if amount_to_send <= 0:
            return

        logger.info(f"🧹 Sweeping {amount_to_send} lamports from {wallet_data['public_key']} to {ADMIN_WALLET}...")

        # Create Transaction
        ix = transfer(
            TransferParams(
                from_pubkey=kp.pubkey(),
                to_pubkey=Pubkey.from_string(ADMIN_WALLET),
                lamports=int(amount_to_send)
            )
        )
        
        # Get blockhash
        latest_blockhash = client.get_latest_blockhash().value.blockhash
        
        # Construct and sign transaction
        transaction = Transaction.new_signed_with_payer(
            [ix],
            kp.pubkey(),
            [kp],
            latest_blockhash
        )
        
        # Send
        txn_sig = client.send_transaction(transaction)
        
        logger.info(f"✅ Swept funds. Sig: {txn_sig.value}")
        
        # Update DB
        conn = get_db_connection()
        conn.cursor().execute("UPDATE solana_wallets SET status = 'swept' WHERE id = ?", (wallet_data['id'],))
        conn.commit()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to sweep wallet {wallet_data['public_key']}: {e}", exc_info=True)


# =========================================================================
# STUCK FUNDS RECOVERY SYSTEM
# =========================================================================

def _check_balance_with_retry(pubkey_str: str, max_retries: int = 3) -> int:
    """
    Check wallet balance with retry logic and exponential backoff.
    Returns lamports or -1 if failed.
    """
    for attempt in range(max_retries):
        try:
            balance_resp = client.get_balance(Pubkey.from_string(pubkey_str))
            return balance_resp.value
        except Exception as e:
            error_str = str(e).lower()
            # Check for rate limiting
            if '429' in error_str or 'rate' in error_str or 'too many' in error_str:
                wait_time = (2 ** attempt) * 0.5  # 0.5s, 1s, 2s
                logger.debug(f"Rate limited, waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                time.sleep(wait_time)
            elif attempt < max_retries - 1:
                time.sleep(0.2 * (attempt + 1))  # Small delay before retry
            else:
                raise e
    return -1


async def find_stuck_wallets():
    """
    Finds all wallets in the database that have SOL balance but were never swept.
    Checks actual on-chain balance for each wallet with rate limiting.
    
    Returns: List of dicts with wallet info and current balance
    """
    stuck_wallets = []
    conn = None
    
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Find ALL wallets - check on-chain balance regardless of DB status
        # (a wallet might be marked 'swept' but sweep tx could have failed)
        c.execute("""
            SELECT id, user_id, order_id, public_key, private_key, expected_amount, 
                   status, amount_received, created_at
            FROM solana_wallets 
            ORDER BY created_at DESC
        """)
        
        wallets = c.fetchall()
        conn.close()
        conn = None
        
        if not wallets:
            logger.info("🔍 No potentially stuck wallets found in database.")
            return []
        
        total_wallets = len(wallets)
        
        # Generate detailed status report
        status_counts = {}
        for w in wallets:
            status = w['status'] or 'unknown'
            status_counts[status] = status_counts.get(status, 0) + 1
        
        logger.info(f"🔍 Checking {total_wallets} wallets for stuck funds (with rate limiting)...")
        logger.info(f"📊 Database Status Breakdown: {status_counts}")
        
        # Get price once for all calculations
        price = get_sol_price_eur()
        
        # Process in batches with delays to avoid rate limiting
        BATCH_SIZE = 10  # Check 10 wallets at a time
        BATCH_DELAY = 1.0  # 1 second between batches
        RPC_DELAY = 0.15  # 150ms between individual RPC calls
        
        checked = 0
        failed = 0
        
        for i in range(0, total_wallets, BATCH_SIZE):
            batch = wallets[i:i + BATCH_SIZE]
            
            for wallet in batch:
                wallet_dict = dict(wallet)
                pubkey_str = wallet_dict['public_key']
                
                try:
                    # VALIDATION: Verify private key can derive correct public key
                    try:
                        priv_key_list = json.loads(wallet_dict['private_key'])
                        kp = Keypair.from_bytes(bytes(priv_key_list))
                        derived_pubkey = str(kp.pubkey())
                        if derived_pubkey != pubkey_str:
                            logger.warning(f"⚠️ CORRUPTED KEY: {pubkey_str[:16]}... private key derives {derived_pubkey[:16]}... - SKIPPING")
                            failed += 1
                            continue
                    except Exception as key_e:
                        logger.warning(f"⚠️ INVALID KEY: {pubkey_str[:16]}... - Error: {str(key_e)[:50]} - SKIPPING")
                        failed += 1
                        continue
                    
                    # Check actual on-chain balance with retry logic
                    lamports = _check_balance_with_retry(pubkey_str)
                    
                    if lamports < 0:
                        failed += 1
                        continue
                    
                    sol_balance = Decimal(lamports) / Decimal(10**9)
                    
                    # If balance > dust threshold (0.0001 SOL), it's stuck
                    if sol_balance > Decimal("0.0001"):
                        eur_value = float(sol_balance * price) if price else 0
                        
                        stuck_wallets.append({
                            'id': wallet_dict['id'],
                            'user_id': wallet_dict['user_id'],
                            'order_id': wallet_dict['order_id'],
                            'public_key': pubkey_str,
                            'private_key': wallet_dict['private_key'],
                            'expected_amount': wallet_dict['expected_amount'],
                            'status': wallet_dict['status'],
                            'sol_balance': float(sol_balance),
                            'lamports': lamports,
                            'eur_value': eur_value,
                            'created_at': wallet_dict['created_at']
                        })
                        
                        logger.info(f"💰 Found stuck funds: {pubkey_str[:16]}... = {sol_balance:.6f} SOL (~{eur_value:.2f} EUR) [Status: {wallet_dict['status']}]")
                    
                    checked += 1
                    
                    # Small delay between RPC calls to avoid rate limiting
                    await asyncio.sleep(RPC_DELAY)
                        
                except Exception as rpc_e:
                    failed += 1
                    error_msg = str(rpc_e) if str(rpc_e) else "Empty RPC response (rate limited?)"
                    logger.warning(f"⚠️ Could not check balance for {pubkey_str[:16]}...: {error_msg}")
                    # Add extra delay on errors (likely rate limited)
                    await asyncio.sleep(0.5)
                    continue
            
            # Progress update every batch
            progress_pct = min(100, ((i + len(batch)) / total_wallets) * 100)
            logger.info(f"📊 Progress: {progress_pct:.0f}% ({i + len(batch)}/{total_wallets}) - Found {len(stuck_wallets)} stuck so far, {failed} failed checks")
            
            # Delay between batches (skip if last batch)
            if i + BATCH_SIZE < total_wallets:
                await asyncio.sleep(BATCH_DELAY)
        
        logger.info(f"✅ Scan complete: Found {len(stuck_wallets)} wallets with stuck funds. (Checked: {checked}, Failed: {failed})")
        return stuck_wallets
        
    except Exception as e:
        logger.error(f"❌ Error finding stuck wallets: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


async def check_single_wallet(wallet_address: str):
    """
    Check a single wallet's balance - useful for quick checks.
    Returns dict with balance info or None if failed.
    """
    try:
        lamports = _check_balance_with_retry(wallet_address)
        if lamports < 0:
            return None
        
        sol_balance = Decimal(lamports) / Decimal(10**9)
        price = get_sol_price_eur()
        eur_value = float(sol_balance * price) if price else 0
        
        return {
            'public_key': wallet_address,
            'sol_balance': float(sol_balance),
            'lamports': lamports,
            'eur_value': eur_value
        }
    except Exception as e:
        logger.error(f"❌ Failed to check wallet {wallet_address}: {e}")
        return None


async def recover_stuck_funds(target_wallet: str = None):
    """
    Recovers all stuck funds by sweeping them to the recovery wallet.
    
    Args:
        target_wallet: Optional. If not provided, uses RECOVERY_WALLET env var,
                      falling back to ADMIN_WALLET if RECOVERY_WALLET is not set.
    
    Returns: Dict with recovery results
    """
    # Determine target wallet
    if target_wallet is None:
        target_wallet = RECOVERY_WALLET or ADMIN_WALLET
    
    if not target_wallet:
        logger.error("❌ No recovery wallet configured! Set SOLANA_RECOVERY_WALLET or SOLANA_ADMIN_WALLET environment variable.")
        return {'success': False, 'error': 'No recovery wallet configured', 'recovered': []}
    
    logger.info(f"🔄 Starting stuck funds recovery to wallet: {target_wallet}")
    
    # Find all stuck wallets
    stuck_wallets = await find_stuck_wallets()
    
    if not stuck_wallets:
        logger.info("✅ No stuck funds to recover.")
        return {'success': True, 'message': 'No stuck funds found', 'recovered': []}
    
    total_sol_recovered = Decimal('0')
    total_eur_recovered = Decimal('0')
    recovered = []
    failed = []
    
    for wallet_info in stuck_wallets:
        try:
            pubkey_str = wallet_info['public_key']
            lamports = wallet_info['lamports']
            sol_balance = Decimal(str(wallet_info['sol_balance']))
            
            logger.info(f"🧹 Recovering {sol_balance:.6f} SOL from {pubkey_str[:16]}...")
            
            # Load Keypair from private key
            priv_key_list = json.loads(wallet_info['private_key'])
            kp = Keypair.from_bytes(bytes(priv_key_list))
            
            # Calculate fee and amount to send
            fee = 5000  # lamports
            amount_to_send = lamports - fee
            
            if amount_to_send <= 0:
                logger.warning(f"⚠️ Balance too low to recover from {pubkey_str[:16]}... (balance: {lamports} lamports)")
                continue
            
            # Create transfer instruction
            ix = transfer(
                TransferParams(
                    from_pubkey=kp.pubkey(),
                    to_pubkey=Pubkey.from_string(target_wallet),
                    lamports=int(amount_to_send)
                )
            )
            
            # Get latest blockhash
            latest_blockhash = client.get_latest_blockhash().value.blockhash
            
            # Construct and sign transaction
            transaction = Transaction.new_signed_with_payer(
                [ix],
                kp.pubkey(),
                [kp],
                latest_blockhash
            )
            
            # Send transaction
            txn_sig = client.send_transaction(transaction)
            
            logger.info(f"✅ Recovered {sol_balance:.6f} SOL from {pubkey_str[:16]}... Sig: {txn_sig.value}")
            
            # Update database
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("""
                UPDATE solana_wallets 
                SET status = 'swept', 
                    amount_received = ?,
                    updated_at = datetime('now')
                WHERE id = ?
            """, (float(sol_balance), wallet_info['id']))
            conn.commit()
            conn.close()
            
            total_sol_recovered += sol_balance
            total_eur_recovered += Decimal(str(wallet_info['eur_value']))
            
            recovered.append({
                'public_key': pubkey_str,
                'sol_amount': float(sol_balance),
                'eur_value': wallet_info['eur_value'],
                'user_id': wallet_info['user_id'],
                'order_id': wallet_info['order_id'],
                'tx_signature': str(txn_sig.value)
            })
            
            # Small delay between transactions to avoid rate limiting
            await asyncio.sleep(0.5)
            
        except Exception as e:
            logger.error(f"❌ Failed to recover from {wallet_info['public_key'][:16]}...: {e}")
            failed.append({
                'public_key': wallet_info['public_key'],
                'sol_amount': wallet_info['sol_balance'],
                'error': str(e)
            })
    
    result = {
        'success': True,
        'target_wallet': target_wallet,
        'total_sol_recovered': float(total_sol_recovered),
        'total_eur_recovered': float(total_eur_recovered),
        'wallets_recovered': len(recovered),
        'wallets_failed': len(failed),
        'recovered': recovered,
        'failed': failed
    }
    
    logger.info(f"🎉 Recovery complete! Recovered {total_sol_recovered:.6f} SOL (~{total_eur_recovered:.2f} EUR) from {len(recovered)} wallets.")
    if failed:
        logger.warning(f"⚠️ Failed to recover from {len(failed)} wallets.")
    
    return result


async def recover_single_wallet(wallet_address: str, target_wallet: str = None):
    """
    Recovers funds from a specific wallet address.
    
    Args:
        wallet_address: The public key of the wallet to recover from
        target_wallet: Optional. If not provided, uses RECOVERY_WALLET or ADMIN_WALLET
    
    Returns: Dict with recovery result
    """
    # Determine target wallet
    if target_wallet is None:
        target_wallet = RECOVERY_WALLET or ADMIN_WALLET
    
    if not target_wallet:
        return {'success': False, 'error': 'No recovery wallet configured'}
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Find the wallet in database
        c.execute("""
            SELECT id, user_id, order_id, public_key, private_key, expected_amount, status
            FROM solana_wallets 
            WHERE public_key = ?
        """, (wallet_address,))
        
        wallet = c.fetchone()
        conn.close()
        conn = None
        
        if not wallet:
            return {'success': False, 'error': f'Wallet {wallet_address} not found in database'}
        
        wallet_dict = dict(wallet)
        
        # Check on-chain balance
        balance_resp = client.get_balance(Pubkey.from_string(wallet_address))
        lamports = balance_resp.value
        sol_balance = Decimal(lamports) / Decimal(10**9)
        
        if sol_balance <= Decimal("0.0001"):
            return {'success': False, 'error': f'Wallet has no funds to recover (balance: {sol_balance} SOL)'}
        
        price = get_sol_price_eur()
        eur_value = float(sol_balance * price) if price else 0
        
        logger.info(f"🧹 Recovering {sol_balance:.6f} SOL (~{eur_value:.2f} EUR) from {wallet_address[:16]}...")
        
        # Load Keypair
        priv_key_list = json.loads(wallet_dict['private_key'])
        kp = Keypair.from_bytes(bytes(priv_key_list))
        
        # Calculate fee and amount
        fee = 5000
        amount_to_send = lamports - fee
        
        if amount_to_send <= 0:
            return {'success': False, 'error': 'Balance too low to cover transaction fee'}
        
        # Create and send transaction
        ix = transfer(
            TransferParams(
                from_pubkey=kp.pubkey(),
                to_pubkey=Pubkey.from_string(target_wallet),
                lamports=int(amount_to_send)
            )
        )
        
        latest_blockhash = client.get_latest_blockhash().value.blockhash
        
        transaction = Transaction.new_signed_with_payer(
            [ix],
            kp.pubkey(),
            [kp],
            latest_blockhash
        )
        
        txn_sig = client.send_transaction(transaction)
        
        logger.info(f"✅ Recovered {sol_balance:.6f} SOL! Sig: {txn_sig.value}")
        
        # Update database
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            UPDATE solana_wallets 
            SET status = 'swept', 
                amount_received = ?,
                updated_at = datetime('now')
            WHERE id = ?
        """, (float(sol_balance), wallet_dict['id']))
        conn.commit()
        conn.close()
        
        return {
            'success': True,
            'wallet_address': wallet_address,
            'target_wallet': target_wallet,
            'sol_recovered': float(sol_balance),
            'eur_value': eur_value,
            'user_id': wallet_dict['user_id'],
            'order_id': wallet_dict['order_id'],
            'tx_signature': str(txn_sig.value)
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to recover from {wallet_address}: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()


def get_recovery_status():
    """
    Returns current recovery configuration and stuck funds summary.
    Useful for admin dashboard or status checks.
    """
    return {
        'admin_wallet': ADMIN_WALLET,
        'recovery_wallet': RECOVERY_WALLET,
        'recovery_wallet_configured': bool(RECOVERY_WALLET),
        'auto_sweep_enabled': ENABLE_AUTO_SWEEP
    }

