# --- START OF FILE payment.py ---

import logging
import sqlite3
import time
import os
import shutil
import asyncio
import uuid
import json
from decimal import Decimal, ROUND_UP, ROUND_DOWN
from datetime import datetime, timezone, timedelta
from collections import Counter, defaultdict

# --- Telegram Imports ---
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import ContextTypes
from telegram import helpers
import telegram.error as telegram_error
from telegram import InputMediaPhoto, InputMediaVideo, InputMediaAnimation

# Import necessary items from utils and user
from utils import (
    send_message_with_retry, format_currency, ADMIN_ID,
    LANGUAGES, load_all_data, BASKET_TIMEOUT, MIN_DEPOSIT_EUR,
    WEBHOOK_URL, format_expiration_time,
    add_pending_deposit, remove_pending_deposit,
    get_db_connection, MEDIA_DIR, PRODUCT_TYPES, DEFAULT_PRODUCT_EMOJI,
    clear_expired_basket,
    _get_lang_data,
    log_admin_action,
    get_first_primary_admin_id,
    send_media_with_retry, send_media_group_with_retry
)
import user

# --- Import Reseller Helper ---
try:
    from reseller_management import get_reseller_discount, get_reseller_discount_with_connection
except ImportError:
    logger_dummy_reseller_payment = logging.getLogger(__name__ + "_dummy_reseller_payment")
    logger_dummy_reseller_payment.error("Could not import get_reseller_discount from reseller_management.py. Reseller discounts will not work in payment processing.")
    def get_reseller_discount(user_id: int, product_type: str) -> Decimal:
        return Decimal('0.0')
    
    async def get_reseller_discount_with_connection(cursor, user_id: int, product_type: str) -> Decimal:
        return Decimal('0.0')

# --- Import Unreserve Helper ---
try:
    from user import _unreserve_basket_items
except ImportError:
    try:
        from utils import _unreserve_basket_items
    except ImportError:
        logger_unreserve_import_error = logging.getLogger(__name__)
        logger_unreserve_import_error.error("Could not import _unreserve_basket_items helper function from user.py or utils.py! Un-reserving on failure might not work.")
        def _unreserve_basket_items(basket_snapshot: list | None):
            logger_unreserve_import_error.critical("CRITICAL: _unreserve_basket_items function is missing! Cannot un-reserve items on payment failure.")

logger = logging.getLogger(__name__)

# --- Solana Payment Integration ---
from payment_solana import (
    create_solana_payment,
    get_sol_price_eur,
    check_solana_deposits,
    refresh_price_cache
)


# --- Helper to check payment status (Solana) ---
async def check_payment_status(payment_id: str) -> dict:
    """Checks the current status of a Solana payment."""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT * FROM solana_wallets WHERE order_id = ?", (payment_id,))
        wallet = c.fetchone()
        conn.close()
        
        if not wallet:
            return {'error': 'payment_not_found'}
        
        status = wallet['status']
        
        if status in ['paid', 'swept']:
            return {
                'payment_status': 'confirmed',
                'actually_paid': wallet.get('amount_received', 0),
                'pay_currency': 'SOL'
            }
        elif status == 'pending':
            return {
                'payment_status': 'waiting',
                'actually_paid': 0,
                'pay_currency': 'SOL'
            }
        else:
            return {
                'payment_status': status,
                'actually_paid': 0,
                'pay_currency': 'SOL'
            }
            
    except Exception as e:
        logger.error(f"Exception checking Solana payment status {payment_id}: {e}", exc_info=True)
        return {'error': str(e)}


# --- Payment Status Verification Function ---
async def verify_payment_with_nowpayments(payment_id: str) -> dict:
    """Verify payment status (now using Solana)"""
    return await check_payment_status(payment_id)


# --- Solana Payment Creation ---
async def create_nowpayments_payment(
    user_id: int,
    target_eur_amount: Decimal,
    pay_currency_code: str,
    is_purchase: bool = False,
    basket_snapshot: list | None = None,
    discount_code: str | None = None
) -> dict:
    """
    Creates a payment invoice using Solana.
    The pay_currency_code parameter is ignored as we only support SOL.
    """
    log_type = "direct purchase" if is_purchase else "refill"
    logger.info(f"Attempting to create Solana {log_type} invoice for user {user_id}, {target_eur_amount} EUR")

    # Re-validate discount code right before payment creation to prevent race conditions
    if is_purchase and discount_code:
        from user import validate_and_apply_discount_atomic
        basket_total_before_discount = Decimal('0.0')
        if basket_snapshot:
            for item in basket_snapshot:
                item_price = Decimal(str(item.get('price', 0)))
                item_type = item.get('product_type', '')
                try:
                    logger.info(f"🔄 BULLETPROOF: Calculating reseller discount for user {user_id}, product {item_type}")
                    reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, item_type)
                    reseller_discount = (item_price * reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                    basket_total_before_discount += (item_price - reseller_discount)
                    logger.info(f"✅ BULLETPROOF: Reseller discount calculated: {reseller_discount_percent}% = {reseller_discount} EUR")
                except Exception as reseller_error:
                    logger.warning(f"⚠️ BULLETPROOF: Error calculating reseller discount for user {user_id}, product {item_type} during payment creation: {reseller_error}. Using full price.")
                    basket_total_before_discount += item_price
        
        code_valid, validation_message, discount_details = validate_and_apply_discount_atomic(discount_code, float(basket_total_before_discount), user_id)
        if not code_valid:
            logger.warning(f"Discount code '{discount_code}' became invalid during payment creation for user {user_id}: {validation_message}")
            return {'error': 'discount_code_invalid', 'reason': validation_message, 'code': discount_code}
        
        expected_final_total = Decimal(str(discount_details['final_total']))
        if abs(expected_final_total - target_eur_amount) > Decimal('0.01'):
            logger.warning(f"Discount code '{discount_code}' total mismatch for user {user_id}. Expected: {expected_final_total}, Got: {target_eur_amount}")
            return {'error': 'discount_amount_mismatch', 'expected': float(expected_final_total), 'received': float(target_eur_amount)}
        
        logger.info(f"Discount code '{discount_code}' re-validated successfully for user {user_id} payment creation")

    # Create unique order ID
    order_id_prefix = "PURCHASE" if is_purchase else "REFILL"
    order_id = f"USER{user_id}_{order_id_prefix}_{int(time.time())}_{uuid.uuid4().hex[:6]}"

    # Create Solana payment
    try:
        payment_result = await create_solana_payment(user_id, order_id, float(target_eur_amount))
        
        if 'error' in payment_result:
            logger.error(f"Failed to create Solana payment: {payment_result}")
            return payment_result
        
        # Add to pending deposits
        add_success = await asyncio.to_thread(
            add_pending_deposit,
            order_id,  # payment_id is order_id for Solana
            user_id,
            'sol',  # currency
            float(target_eur_amount),
            float(Decimal(str(payment_result['pay_amount']))),
            is_purchase=is_purchase,
            basket_snapshot=basket_snapshot,
            discount_code=discount_code
        )
        
        if not add_success:
            logger.error(f"Failed to add pending deposit to DB for payment_id {order_id} (user {user_id}).")
            return {'error': 'pending_db_error'}

        # Format response to match expected structure
        result = {
            'payment_id': payment_result['payment_id'],
            'pay_address': payment_result['pay_address'],
            'pay_amount': payment_result['pay_amount'],
            'pay_currency': 'SOL',
            'target_eur_amount_orig': float(target_eur_amount),
            'is_purchase': is_purchase,
            'expiration_estimate_date': (datetime.now(timezone.utc) + timedelta(minutes=20)).isoformat()
        }
        
        logger.info(f"Successfully created Solana {log_type} invoice {order_id} for user {user_id}.")
        return result

    except Exception as e:
        logger.error(f"Unexpected error in create_nowpayments_payment for user {user_id}: {e}", exc_info=True)
        return {'error': 'internal_server_error', 'details': str(e)}


# --- Callback Handler for Crypto Selection during Refill ---
async def handle_select_refill_crypto(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the user selecting SOL for refill, creates Solana invoice."""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    # For Solana-only, we don't need the params (crypto selection)
    # params can be ignored or checked if needed
    logger.info(f"User {user_id} selected SOL for refill.")

    refill_eur_amount_float = context.user_data.get('refill_eur_amount')
    if not refill_eur_amount_float or refill_eur_amount_float <= 0:
        logger.error(f"Refill amount context lost before asset selection for user {user_id}.")
        await query.edit_message_text("❌ Error: Refill amount context lost. Please start the top up again.", parse_mode=None)
        context.user_data.pop('state', None)
        return

    refill_eur_amount_decimal = Decimal(str(refill_eur_amount_float))

    preparing_invoice_msg = lang_data.get("preparing_invoice", "⏳ Preparing your payment invoice...")
    failed_invoice_creation_msg = lang_data.get("failed_invoice_creation", "❌ Failed to create payment invoice. Please try again later or contact support.")
    error_estimate_failed_msg = lang_data.get("error_estimate_failed", "❌ Error: Could not estimate crypto amount. Please try again.")
    back_to_profile_button = lang_data.get("back_profile_button", "Back to Profile")
    back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {back_to_profile_button}", callback_data="profile")]])

    try:
        await query.edit_message_text(preparing_invoice_msg, reply_markup=None, parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.warning(f"Couldn't edit message in handle_select_refill_crypto: {e}")
        await query.answer("Preparing...")

    # Call payment creation with SOL
    payment_result = await create_nowpayments_payment(
        user_id, refill_eur_amount_decimal, 'sol',
        is_purchase=False
    )

    if 'error' in payment_result:
        error_code = payment_result['error']
        logger.error(f"Failed to create Solana refill invoice for user {user_id}: {error_code} - Details: {payment_result}")

        error_message_to_user = failed_invoice_creation_msg
        if error_code == 'estimate_failed':
            error_message_to_user = error_estimate_failed_msg

        try:
            await query.edit_message_text(error_message_to_user, reply_markup=back_button_markup, parse_mode=None)
        except Exception as edit_e:
            logger.error(f"Failed to edit message with invoice creation error: {edit_e}")
            await send_message_with_retry(context.bot, chat_id, error_message_to_user, reply_markup=back_button_markup, parse_mode=None)
        context.user_data.pop('refill_eur_amount', None)
        context.user_data.pop('state', None)
    else:
        logger.info(f"Solana refill invoice created successfully for user {user_id}. Payment ID: {payment_result.get('payment_id')}")
        context.user_data.pop('refill_eur_amount', None)
        context.user_data.pop('state', None)
        await display_nowpayments_invoice(update, context, payment_result)


# --- Callback Handler for Crypto Selection during Basket Payment ---
async def handle_select_basket_crypto(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the user selecting SOL for direct basket payment."""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    # For Solana-only, params not needed
    logger.info(f"User {user_id} selected SOL for basket payment.")

    # Retrieve stored basket context
    basket_snapshot = context.user_data.get('basket_pay_snapshot')
    final_total_eur_float = context.user_data.get('basket_pay_total_eur')
    discount_code_used = context.user_data.get('basket_pay_discount_code')

    if basket_snapshot is None or final_total_eur_float is None:
        logger.error(f"Basket payment context lost before crypto selection for user {user_id}.")
        await query.edit_message_text("❌ Error: Payment context lost. Please go back to your basket.",
                                       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ View Basket", callback_data="view_basket")]]),
                                       parse_mode=None)
        context.user_data.pop('state', None)
        context.user_data.pop('basket_pay_snapshot', None)
        context.user_data.pop('basket_pay_total_eur', None)
        context.user_data.pop('basket_pay_discount_code', None)
        return

    final_total_eur_decimal = Decimal(str(final_total_eur_float))

    preparing_invoice_msg = lang_data.get("preparing_invoice", "⏳ Preparing your payment invoice...")
    failed_invoice_creation_msg = lang_data.get("failed_invoice_creation", "❌ Failed to create payment invoice. Please try again later or contact support.")
    error_estimate_failed_msg = lang_data.get("error_estimate_failed", "❌ Error: Could not estimate crypto amount. Please try again.")
    error_discount_invalid_msg = lang_data.get("error_discount_invalid_payment", "❌ Your discount code is no longer valid: {reason}. Please return to your basket to continue without the discount.")
    error_discount_mismatch_msg = lang_data.get("error_discount_mismatch_payment", "❌ Payment amount mismatch detected. Please return to your basket and try again.")
    back_to_basket_button = lang_data.get("back_basket_button", "Back to Basket")
    back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {back_to_basket_button}", callback_data="view_basket")]])

    try:
        await query.edit_message_text(preparing_invoice_msg, reply_markup=None, parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.warning(f"Couldn't edit message in handle_select_basket_crypto: {e}")
        await query.answer("Preparing...")

    # Call payment creation
    payment_result = await create_nowpayments_payment(
        user_id, final_total_eur_decimal, 'sol',
        is_purchase=True,
        basket_snapshot=basket_snapshot,
        discount_code=discount_code_used
    )

    # Store snapshot temporarily before clearing context
    snapshot_before_clear = context.user_data.get('basket_pay_snapshot')

    # Clear reservation tracking
    from utils import clear_reservation_tracking
    clear_reservation_tracking(user_id)

    # Clear context
    context.user_data.pop('basket_pay_snapshot', None)
    context.user_data.pop('basket_pay_total_eur', None)
    context.user_data.pop('basket_pay_discount_code', None)
    context.user_data.pop('state', None)

    if 'error' in payment_result:
        error_code = payment_result['error']
        logger.error(f"Failed to create Solana basket payment invoice for user {user_id}: {error_code} - Details: {payment_result}")

        # Unreserve items if invoice creation failed
        if error_code in ['estimate_failed', 'payment_api_misconfigured']:
            logger.info(f"Invoice creation failed ({error_code}) before pending record. Un-reserving items from snapshot.")
            try:
                await asyncio.to_thread(_unreserve_basket_items, snapshot_before_clear)
            except NameError:
                 logger.critical("CRITICAL: _unreserve_basket_items function call failed due to NameError!")
            except Exception as unreserve_e:
                 logger.error(f"Error occurred during item un-reservation: {unreserve_e}")

        error_message_to_user = failed_invoice_creation_msg
        if error_code == 'estimate_failed':
            error_message_to_user = error_estimate_failed_msg
        elif error_code == 'discount_code_invalid': 
            error_message_to_user = error_discount_invalid_msg.format(reason=payment_result.get('reason', 'Unknown reason'))
        elif error_code == 'discount_amount_mismatch': 
            error_message_to_user = error_discount_mismatch_msg

        try:
            await query.edit_message_text(error_message_to_user, reply_markup=back_button_markup, parse_mode=None)
        except Exception as edit_e:
            logger.error(f"Failed to edit message with basket payment creation error: {edit_e}")
            await send_message_with_retry(context.bot, chat_id, error_message_to_user, reply_markup=back_button_markup, parse_mode=None)
    else:
        logger.info(f"Solana basket payment invoice created successfully for user {user_id}. Payment ID: {payment_result.get('payment_id')}")
        await display_nowpayments_invoice(update, context, payment_result)


# --- Display Solana Invoice ---
async def display_nowpayments_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE, payment_data: dict):
    """Displays the Solana invoice details with improved formatting."""
    query = update.callback_query
    chat_id = query.message.chat_id
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    final_msg = "Error displaying invoice."
    is_purchase_invoice = payment_data.get('is_purchase', False)

    try:
        pay_address = payment_data.get('pay_address')
        pay_amount_str = payment_data.get('pay_amount')
        pay_currency = 'SOL'
        payment_id = payment_data.get('payment_id')
        target_eur_orig = payment_data.get('target_eur_amount_orig')
        expiration_date_str = payment_data.get('expiration_estimate_date')

        if not pay_address or not pay_amount_str or not payment_id:
            logger.error(f"Missing critical data in Solana response for display: {payment_data}")
            raise ValueError("Missing payment address, amount, or ID")

        # Store payment_id in user_data for cancellation
        context.user_data['pending_payment_id'] = payment_id
        logger.debug(f"Stored pending_payment_id {payment_id} in user_data.")

        pay_amount_decimal = Decimal(pay_amount_str)
        pay_amount_display = '{:f}'.format(pay_amount_decimal.normalize())
        target_eur_display = format_currency(Decimal(str(target_eur_orig))) if target_eur_orig else "N/A"
        expiry_time_display = format_expiration_time(expiration_date_str)

        invoice_title_template = lang_data.get("invoice_title_purchase", "*Payment Invoice Created*") if is_purchase_invoice else lang_data.get("invoice_title_refill", "*Top\\-Up Invoice Created*")
        amount_label = lang_data.get("amount_label", "*Amount:*")
        payment_address_label = lang_data.get("payment_address_label", "*Payment Address:*")
        expires_at_label = lang_data.get("expires_at_label", "*Expires At:*")
        payment_id_label = lang_data.get("payment_id_label", "*Payment ID:*")
        send_warning_template = lang_data.get("send_warning_template", "⚠️ *Important:* Send *exactly* this amount of {asset} to this address\\.")
        confirmation_note = lang_data.get("confirmation_note", "✅ Confirmation is automatic after network confirmation\\.")
        overpayment_note = lang_data.get("overpayment_note", "ℹ️ _Sending more than this amount is okay\\! Your balance will be credited based on the amount received after network confirmation\\._")
        cancel_payment_button_text = lang_data.get("cancel_payment_button", "Cancel Payment")

        invoice_send_following_amount = lang_data.get("invoice_send_following_amount", "Please send the following amount:")
        invoice_payment_deadline = lang_data.get("invoice_payment_deadline", "Payment must be completed within 20 minutes of invoice creation.")
        
        escaped_target_eur = helpers.escape_markdown(target_eur_display, version=2)
        escaped_pay_amount = helpers.escape_markdown(pay_amount_display, version=2)
        escaped_currency = helpers.escape_markdown(pay_currency, version=2)
        escaped_address = helpers.escape_markdown(pay_address, version=2)
        escaped_expiry = helpers.escape_markdown(expiry_time_display, version=2)

        msg = f"""{invoice_title_template}

_{helpers.escape_markdown(f"({lang_data.get('invoice_amount_label_text', 'Amount')}: {target_eur_display} EUR)", version=2)}_

{invoice_send_following_amount}
{amount_label} `{escaped_pay_amount}` {escaped_currency}

{payment_address_label}
`{escaped_address}`

{payment_id_label} `{helpers.escape_markdown(payment_id, version=2)}`

{expires_at_label} {escaped_expiry}
⚠️ _{helpers.escape_markdown(invoice_payment_deadline, version=2)}_

"""
        if is_purchase_invoice:
            msg += f"{send_warning_template.format(asset=escaped_currency)}\n"
        else:
            msg += f"{overpayment_note}\n"
        msg += f"\n{confirmation_note}"

        final_msg = msg.strip()

        # Cancel button only
        keyboard = [[InlineKeyboardButton(f"❌ {cancel_payment_button_text}", callback_data="cancel_crypto_payment")]]

        await query.edit_message_text(
            final_msg, reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True
        )
    except (ValueError, KeyError, TypeError) as e:
        logger.error(f"Error formatting or displaying Solana invoice: {e}. Data: {payment_data}", exc_info=True)
        error_display_msg = lang_data.get("error_preparing_payment", "❌ An error occurred while preparing the payment details. Please try again later.")
        back_button_text = lang_data.get("back_basket_button", "Back to Basket") if is_purchase_invoice else lang_data.get("back_profile_button", "Back to Profile")
        back_callback = "view_basket" if is_purchase_invoice else "profile"
        back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {back_button_text}", callback_data=back_callback)]])
        try:
            await query.edit_message_text(error_display_msg, reply_markup=back_button_markup, parse_mode=None)
        except Exception:
            pass
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.error(f"Error editing Solana invoice message: {e}")
        else:
            await query.answer()
    except Exception as e:
         logger.error(f"Unexpected error in display_nowpayments_invoice: {e}", exc_info=True)
         error_display_msg = lang_data.get("error_preparing_payment", "❌ An unexpected error occurred while preparing the payment details.")
         back_button_text = lang_data.get("back_basket_button", "Back to Basket") if is_purchase_invoice else lang_data.get("back_profile_button", "Back to Profile")
         back_callback = "view_basket" if is_purchase_invoice else "profile"
         back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {back_button_text}", callback_data=back_callback)]])
         try:
            await query.edit_message_text(error_display_msg, reply_markup=back_button_markup, parse_mode=None)
         except Exception:
            pass


# --- Process Successful Refill ---
async def process_successful_refill(user_id: int, amount_to_add_eur: Decimal, payment_id: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    # Get bot instance, handle None context from background jobs
    bot = None
    if context is not None and hasattr(context, 'bot'):
        bot = context.bot
    else:
        # Import Application to get bot instance
        try:
            from main import application
            if application:
                bot = application.bot
        except Exception as e:
            logger.error(f"Could not get bot instance for refill notification: {e}")
    
    # Create a minimal context-like object if context is None
    if context is None:
        from types import SimpleNamespace
        context = SimpleNamespace(bot=bot)
    
    user_lang = 'en'
    conn_lang = None
    try:
        conn_lang = get_db_connection()
        c_lang = conn_lang.cursor()
        c_lang.execute("SELECT language FROM users WHERE user_id = ?", (user_id,))
        lang_res = c_lang.fetchone()
        if lang_res and lang_res['language'] in LANGUAGES:
            user_lang = lang_res['language']
    except sqlite3.Error as e:
        logger.error(f"DB error fetching language for user {user_id} during refill confirmation: {e}")
    finally:
        if conn_lang:
            conn_lang.close()

    lang_data = LANGUAGES.get(user_lang, LANGUAGES['en'])

    if not isinstance(amount_to_add_eur, Decimal) or amount_to_add_eur <= Decimal('0.0'):
        logger.error(f"Invalid amount_to_add_eur in process_successful_refill: {amount_to_add_eur}")
        return False

    # Use the separate crediting function
    return await credit_user_balance(user_id, amount_to_add_eur, f"Refill payment {payment_id}", context)


# --- HELPER: Finalize Purchase ---
async def _finalize_purchase(user_id: int, basket_snapshot: list, discount_code_used: str | None, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Shared logic to finalize a purchase after payment confirmation (balance or crypto).
    Decrements stock, adds purchase record, sends media first, then text separately,
    cleans up product records.
    """
    # Handle None context from background jobs
    chat_id = user_id
    bot_instance = None
    
    if context is not None:
        chat_id = context._chat_id or context._user_id or user_id
        if hasattr(context, 'bot'):
            bot_instance = context.bot
    
    # Get bot instance for background jobs
    if bot_instance is None:
        try:
            from main import application
            if application:
                bot_instance = application.bot
        except Exception as e:
            logger.error(f"Could not get bot instance for purchase finalization: {e}")
    
    if not chat_id:
         logger.error(f"Cannot determine chat_id for user {user_id} in _finalize_purchase")

    lang, lang_data = _get_lang_data(context)
    if not basket_snapshot:
        logger.error(f"Empty basket_snapshot for user {user_id} purchase finalization.")
        return False

    conn = None
    processed_product_ids = []
    purchases_to_insert = []
    final_pickup_details = defaultdict(list)
    db_update_successful = False
    total_price_paid_decimal = Decimal('0.0')

    # --- Database Operations ---
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        logger.info(f"🔄 Starting purchase finalization for user {user_id} with {len(basket_snapshot)} items")
        c.execute("BEGIN IMMEDIATE")
        purchase_time_iso = datetime.now(timezone.utc).isoformat()

        # Pre-validate all products
        product_ids = [item['product_id'] for item in basket_snapshot]
        placeholders = ','.join('?' * len(product_ids))
        c.execute(f"""
            SELECT id, available, reserved FROM products 
            WHERE id IN ({placeholders})
        """, product_ids)
        available_products = {row['id']: {'available': row['available'], 'reserved': row['reserved']} for row in c.fetchall()}
        
        for item_snapshot in basket_snapshot:
            product_id = item_snapshot['product_id']
            if product_id not in available_products:
                logger.error(f"Product {product_id} no longer exists for user {user_id}")
                conn.rollback()
                return False
            
            available = available_products[product_id]['available']
            if available <= 0:
                logger.error(f"Product {product_id} no longer available for user {user_id}")
                conn.rollback()
                return False

        for item_snapshot in basket_snapshot:
            product_id = item_snapshot['product_id']
            
            avail_update = c.execute("UPDATE products SET available = available - 1 WHERE id = ? AND available > 0", (product_id,))
            
            if avail_update.rowcount == 0:
                logger.error(f"Failed to decrement stock for product {product_id} for user {user_id}")
                conn.rollback()
                return False

            item_original_price_decimal = Decimal(str(item_snapshot['price']))
            item_product_type = item_snapshot['product_type']
            item_name = item_snapshot['name']
            item_size = item_snapshot['size']
            item_city = item_snapshot['city'] 
            item_district = item_snapshot['district'] 
            item_original_text_pickup = item_snapshot.get('original_text')

            # Calculate reseller discount
            item_reseller_discount_percent = Decimal('0')
            item_reseller_discount_amount = Decimal('0')
            item_price_paid_decimal = item_original_price_decimal
            
            try:
                logger.info(f"🔄 BULLETPROOF: Calculating reseller discount for user {user_id}, product {item_product_type}")
                item_reseller_discount_percent = await get_reseller_discount_with_connection(c, user_id, item_product_type)
                item_reseller_discount_amount = (item_original_price_decimal * item_reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                item_price_paid_decimal = item_original_price_decimal - item_reseller_discount_amount
                logger.info(f"✅ BULLETPROOF: Reseller discount calculated: {item_reseller_discount_percent}% = {item_reseller_discount_amount} EUR")
            except Exception as reseller_error:
                logger.warning(f"⚠️ BULLETPROOF: Error calculating reseller discount for user {user_id}, product {item_product_type}: {reseller_error}. Using full price.")
                item_reseller_discount_percent = Decimal('0')
                item_reseller_discount_amount = Decimal('0')
                item_price_paid_decimal = item_original_price_decimal
                
            total_price_paid_decimal += item_price_paid_decimal
            item_price_paid_float = float(item_price_paid_decimal)

            purchases_to_insert.append((
                user_id, product_id, item_name, item_product_type, item_size,
                item_price_paid_float, item_city, item_district, purchase_time_iso
            ))
            processed_product_ids.append(product_id)
            final_pickup_details[product_id].append({'name': item_name, 'size': item_size, 'text': item_original_text_pickup, 'type': item_product_type})

        if not purchases_to_insert:
            logger.warning(f"No items processed during finalization for user {user_id}. Rolling back.")
            conn.rollback()
            if chat_id:
                await send_message_with_retry(context.bot, chat_id, lang_data.get("error_processing_purchase_contact_support", "❌ Error processing purchase."), parse_mode=None)
            return False

        c.executemany("INSERT INTO purchases (user_id, product_id, product_name, product_type, product_size, price_paid, city, district, purchase_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", purchases_to_insert)
        c.execute("UPDATE users SET total_purchases = total_purchases + ? WHERE user_id = ?", (len(purchases_to_insert), user_id))
        
        if discount_code_used:
            update_result = c.execute("""
                UPDATE discount_codes 
                SET uses_count = uses_count + 1 
                WHERE code = ? AND (max_uses IS NULL OR uses_count < max_uses)
            """, (discount_code_used,))
            
            if update_result.rowcount == 0:
                c.execute("SELECT uses_count, max_uses FROM discount_codes WHERE code = ?", (discount_code_used,))
                code_check = c.fetchone()
                if code_check:
                    if code_check['max_uses'] is not None and code_check['uses_count'] >= code_check['max_uses']:
                        logger.warning(f"Discount code '{discount_code_used}' usage limit exceeded during payment finalization for user {user_id}.")
                    else:
                        logger.error(f"Unexpected: Failed to increment discount code '{discount_code_used}' for user {user_id}")
                else:
                    logger.warning(f"Discount code '{discount_code_used}' not found in database during payment finalization for user {user_id}")
            else:
                logger.info(f"Successfully incremented usage count for discount code '{discount_code_used}' for user {user_id}")
                
        c.execute("UPDATE users SET basket = '' WHERE user_id = ?", (user_id,))
        conn.commit()
        db_update_successful = True
        logger.info(f"Finalized purchase DB update user {user_id}. Processed {len(purchases_to_insert)} items. Total Paid: {total_price_paid_decimal:.2f} EUR")

    except sqlite3.Error as e:
        logger.error(f"DB error during purchase finalization user {user_id}: {e}", exc_info=True)
        db_update_successful = False
        if conn and conn.in_transaction:
            conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error during purchase finalization user {user_id}: {e}", exc_info=True)
        db_update_successful = False
        if conn and conn.in_transaction:
            conn.rollback()
    finally:
        if conn:
            conn.close()

    # --- Post-Transaction Cleanup & Message Sending ---
    if db_update_successful:
        # Clear basket in context (only if context exists and has user_data that is not None)
        if context is not None and hasattr(context, 'user_data') and context.user_data is not None:
            context.user_data['basket'] = []
            context.user_data.pop('applied_discount', None)

        # Fetch Media
        media_details = defaultdict(list)
        if processed_product_ids:
            conn_media = None
            try:
                conn_media = get_db_connection()
                c_media = conn_media.cursor()
                media_placeholders = ','.join('?' * len(processed_product_ids))
                c_media.execute(f"SELECT product_id, media_type, telegram_file_id, file_path FROM product_media WHERE product_id IN ({media_placeholders})", processed_product_ids)
                media_rows = c_media.fetchall()
                logger.info(f"Fetched {len(media_rows)} media records for products {processed_product_ids} for user {user_id}")
                for row in media_rows: 
                    media_details[row['product_id']].append(dict(row))
            except sqlite3.Error as e: 
                logger.error(f"DB error fetching media post-purchase: {e}")
            finally:
                if conn_media:
                    conn_media.close()

        # Send media and messages
        media_delivery_successful = True
        if chat_id:
            try:
                success_title = lang_data.get("purchase_success", "🎉 Purchase Complete! Pickup details below:")
                await send_message_with_retry(bot_instance, chat_id, success_title, parse_mode=None)

                for prod_id in processed_product_ids:
                    item_details_list = final_pickup_details.get(prod_id)
                    if not item_details_list:
                        continue
                        
                    item_details = item_details_list[0]
                    item_name, item_size = item_details['name'], item_details['size']
                    item_original_text = item_details['text'] or "(No specific pickup details provided)"
                    product_type = item_details['type']
                    product_emoji = PRODUCT_TYPES.get(product_type, DEFAULT_PRODUCT_EMOJI)
                    item_header = f"--- Item: {product_emoji} {item_name} {item_size} ---"

                    combined_caption = f"{item_header}\n\n{item_original_text}"
                    if len(combined_caption) > 4090:
                        combined_caption = combined_caption[:4090] + "..."

                    media_items_for_product = media_details.get(prod_id, [])
                    photo_video_group_details = []
                    animations_to_send_details = []
                    opened_files = []

                    logger.info(f"Processing media for P{prod_id} user {user_id}: Found {len(media_items_for_product)} media items")

                    # Separate Media
                    for media_item in media_items_for_product:
                        media_type = media_item.get('media_type')
                        file_id = media_item.get('telegram_file_id')
                        file_path = media_item.get('file_path')
                        
                        if media_type in ['photo', 'video']:
                            photo_video_group_details.append({'type': media_type, 'id': file_id, 'path': file_path})
                        elif media_type == 'gif':
                            animations_to_send_details.append({'type': media_type, 'id': file_id, 'path': file_path})
                        else:
                            logger.warning(f"Unsupported media type '{media_type}' found for P{prod_id}")

                    # Send Photos/Videos Group
                    if photo_video_group_details:
                        media_group_input = []
                        files_for_this_group = []
                        
                        if len(photo_video_group_details) > 10:
                            logger.warning(f"Media group for P{prod_id} has {len(photo_video_group_details)} items, exceeding 10-item limit. Taking first 10.")
                            photo_video_group_details = photo_video_group_details[:10]
                        
                        try:
                            for item in photo_video_group_details:
                                input_media = None
                                file_handle = None
                                
                                if item['path'] and await asyncio.to_thread(os.path.exists, item['path']):
                                    file_handle = await asyncio.to_thread(open, item['path'], 'rb')
                                    opened_files.append(file_handle)
                                    files_for_this_group.append(file_handle)
                                    if item['type'] == 'photo':
                                        input_media = InputMediaPhoto(media=file_handle)
                                    elif item['type'] == 'video':
                                        input_media = InputMediaVideo(media=file_handle)
                                    
                                if input_media: 
                                    media_group_input.append(input_media)

                            if media_group_input:
                                    result = await send_media_group_with_retry(bot_instance, chat_id, media=media_group_input)
                                    if result:
                                        logger.info(f"✅ Successfully sent photo/video group for P{prod_id}")
                                    else:
                                        logger.error(f"❌ Failed to send media group for P{prod_id}")
                                        raise Exception(f"Media group delivery failed for P{prod_id}")
                        except Exception as group_e:
                            logger.error(f"❌ Error sending photo/video group P{prod_id}: {group_e}", exc_info=True)
                        finally:
                            for f in files_for_this_group:
                                try:
                                    if not f.closed:
                                        await asyncio.to_thread(f.close)
                                        opened_files.remove(f)
                                except Exception:
                                    pass

                    # Send Animations
                    if animations_to_send_details:
                        for item in animations_to_send_details:
                            anim_file_handle = None
                            try:
                                if item['path'] and await asyncio.to_thread(os.path.exists, item['path']):
                                    anim_file_handle = await asyncio.to_thread(open, item['path'], 'rb')
                                    opened_files.append(anim_file_handle)
                                    anim_result = await send_media_with_retry(context.bot, chat_id, media=anim_file_handle, media_type='animation')
                                    if anim_result:
                                        logger.info(f"✅ Successfully sent animation for P{prod_id}")
                                    else:
                                        logger.error(f"❌ Failed to send animation for P{prod_id}")
                                        raise Exception(f"Animation delivery failed for P{prod_id}")
                            except Exception as anim_e:
                                logger.error(f"❌ Error sending animation P{prod_id}: {anim_e}", exc_info=True)
                            finally:
                                if anim_file_handle and anim_file_handle in opened_files:
                                    try:
                                        await asyncio.to_thread(anim_file_handle.close)
                                        opened_files.remove(anim_file_handle)
                                    except Exception:
                                        pass

                    # Send Text Details
                    if combined_caption:
                        await send_message_with_retry(bot_instance, chat_id, combined_caption, parse_mode=None)
                    else:
                        fallback_text = f"(No details provided for Product ID {prod_id})"
                        await send_message_with_retry(bot_instance, chat_id, fallback_text, parse_mode=None)

                    # Close remaining file handles
                    for f in opened_files:
                        try:
                            if not f.closed:
                                await asyncio.to_thread(f.close)
                        except Exception as close_e:
                            logger.warning(f"Error closing file handle: {close_e}")

                # Final Message
                leave_review_button = lang_data.get("leave_review_button", "Leave a Review")
                keyboard = [[InlineKeyboardButton(f"✍️ {leave_review_button}", callback_data="leave_review_now")]]
                await send_message_with_retry(bot_instance, chat_id, "Thank you for your purchase!", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
                
            except Exception as media_error:
                logger.critical(f"🚨 CRITICAL: Media delivery failed for user {user_id}: {media_error}")
                media_delivery_successful = False
                
                if get_first_primary_admin_id():
                    admin_msg = f"🚨 URGENT: Media delivery FAILED for user {user_id}\nPayment successful but products not delivered!\nProducts: {', '.join([str(pid) for pid in processed_product_ids])}\nError: {str(media_error)[:200]}"
                    try:
                        await send_message_with_retry(bot_instance, get_first_primary_admin_id(), admin_msg, parse_mode=None)
                    except Exception as admin_notify_error:
                        logger.error(f"Failed to notify admin about media delivery failure: {admin_notify_error}")
                
                user_msg = f"⚠️ PAYMENT SUCCESSFUL - DELIVERY ISSUE\n\nYour payment was processed successfully, but we encountered a technical issue delivering your products.\n\n✅ Payment confirmed\n📦 Products purchased: {len(processed_product_ids)}\n⚠️ Delivery status: PENDING\n\nOur support team has been automatically notified."
                await send_message_with_retry(bot_instance, chat_id, user_msg, parse_mode=None)

        # Delete Product Records (only if media delivery successful)
        if processed_product_ids and media_delivery_successful:
            conn_del = None
            try:
                conn_del = get_db_connection()
                c_del = conn_del.cursor()
                ids_tuple_list = [(pid,) for pid in processed_product_ids]
                
                media_delete_placeholders = ','.join('?' * len(processed_product_ids))
                c_del.execute(f"DELETE FROM product_media WHERE product_id IN ({media_delete_placeholders})", processed_product_ids)
                delete_result = c_del.executemany("DELETE FROM products WHERE id = ?", ids_tuple_list)
                conn_del.commit()
                deleted_count = delete_result.rowcount
                logger.info(f"Deleted {deleted_count} purchased product records for user {user_id}")
                
                # Schedule media directory deletion
                for prod_id in processed_product_ids:
                    media_dir_to_delete = os.path.join(MEDIA_DIR, str(prod_id))
                    if await asyncio.to_thread(os.path.exists, media_dir_to_delete):
                        asyncio.create_task(asyncio.to_thread(shutil.rmtree, media_dir_to_delete, ignore_errors=True))
                        
            except sqlite3.Error as e: 
                logger.error(f"DB error deleting purchased products: {e}", exc_info=True)
                if conn_del and conn_del.in_transaction: 
                    conn_del.rollback()
            except Exception as e: 
                logger.error(f"Unexpected error deleting purchased products: {e}", exc_info=True)
            finally:
                if conn_del:
                    conn_del.close()
        elif processed_product_ids and not media_delivery_successful:
            logger.warning(f"⚠️ SKIPPING product deletion for user {user_id} due to media delivery failure")

        if media_delivery_successful:
            return True
        else:
            logger.critical(f"🚨 CRITICAL: Purchase {user_id} - Database updated but media delivery failed!")
            return False
    else:
        # Clear basket in context (only if context exists and has user_data that is not None)
        if context is not None and hasattr(context, 'user_data') and context.user_data is not None:
            context.user_data['basket'] = []
            context.user_data.pop('applied_discount', None)
        
        if chat_id:
            # Get bot instance
            bot_instance = None
            if context is not None and hasattr(context, 'bot'):
                bot_instance = context.bot
            else:
                try:
                    from main import application
                    if application:
                        bot_instance = application.bot
                except Exception as e:
                    logger.error(f"Could not get bot instance: {e}")
            
            if bot_instance:
                # Get language
                lang = "en"
                if context is not None and hasattr(context, 'user_data') and context.user_data is not None:
                    lang = context.user_data.get("lang", "en")
                lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
                await send_message_with_retry(bot_instance, chat_id, lang_data.get("error_processing_purchase_contact_support", "❌ Error processing purchase."), parse_mode=None)
        return False


# --- Process Purchase with Balance ---
async def process_purchase_with_balance(user_id: int, amount_to_deduct: Decimal, basket_snapshot: list, discount_code_used: str | None, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handles DB updates when paying with internal balance."""
    chat_id = context._chat_id or context._user_id or user_id
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    if not basket_snapshot:
        logger.error(f"Empty basket_snapshot for user {user_id} balance purchase.")
        return False
    if not isinstance(amount_to_deduct, Decimal) or amount_to_deduct < Decimal('0.0'):
        logger.error(f"Invalid amount_to_deduct {amount_to_deduct}.")
        return False

    conn = None
    db_balance_deducted = False
    balance_changed_error = lang_data.get("balance_changed_error", "❌ Transaction failed: Balance changed.")
    error_processing_purchase_contact_support = lang_data.get("error_processing_purchase_contact_support", "❌ Error processing purchase. Contact support.")

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN IMMEDIATE")
        
        c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        current_balance_result = c.fetchone()
        if not current_balance_result or Decimal(str(current_balance_result['balance'])) < amount_to_deduct:
             logger.warning(f"Insufficient balance user {user_id}. Needed: {amount_to_deduct:.2f}")
             conn.rollback()
             await asyncio.to_thread(_unreserve_basket_items, basket_snapshot)
             if chat_id:
                await send_message_with_retry(context.bot, chat_id, balance_changed_error, parse_mode=None)
             return False
            
        amount_float_to_deduct = float(amount_to_deduct)
        update_res = c.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount_float_to_deduct, user_id))
        if update_res.rowcount == 0:
            logger.error(f"Failed to deduct balance user {user_id}.")
            conn.rollback()
            return False

        conn.commit()
        db_balance_deducted = True
        logger.info(f"Deducted {amount_to_deduct:.2f} EUR from balance for user {user_id}.")

    except sqlite3.Error as e:
        logger.error(f"DB error deducting balance user {user_id}: {e}", exc_info=True)
        db_balance_deducted = False
        if conn and conn.in_transaction:
            conn.rollback()
    finally:
        if conn:
            conn.close()

    if db_balance_deducted:
        logger.info(f"Calling _finalize_purchase for user {user_id} after balance deduction.")
        finalize_success = await _finalize_purchase(user_id, basket_snapshot, discount_code_used, context)
        
        if not finalize_success:
            logger.critical(f"CRITICAL: Balance deducted for user {user_id} but _finalize_purchase FAILED! Attempting refund.")
            refund_conn = None
            try:
                refund_conn = get_db_connection()
                refund_c = refund_conn.cursor()
                refund_c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount_float_to_deduct, user_id))
                refund_conn.commit()
                logger.info(f"Successfully refunded {amount_float_to_deduct} EUR to user {user_id}")
                if chat_id:
                    await send_message_with_retry(context.bot, chat_id, error_processing_purchase_contact_support + " Balance refunded.", parse_mode=None)
            except Exception as refund_e:
                logger.critical(f"CRITICAL REFUND FAILED for user {user_id}: {refund_e}")
                if get_first_primary_admin_id() and chat_id:
                    await send_message_with_retry(context.bot, get_first_primary_admin_id(), f"⚠️ CRITICAL REFUND FAILED for user {user_id}. Amount: {amount_to_deduct}.", parse_mode=None)
                if chat_id:
                    await send_message_with_retry(context.bot, chat_id, error_processing_purchase_contact_support, parse_mode=None)
            finally:
                if refund_conn:
                    refund_conn.close()
        return finalize_success
    else:
        logger.error(f"Skipping purchase finalization for user {user_id} due to balance deduction failure.")
        await asyncio.to_thread(_unreserve_basket_items, basket_snapshot)
        if chat_id:
            await send_message_with_retry(context.bot, chat_id, error_processing_purchase_contact_support, parse_mode=None)
        return False


# --- Process Successful Crypto Purchase ---
async def process_successful_crypto_purchase(user_id: int, basket_snapshot: list, discount_code_used: str | None, payment_id: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handles finalizing a purchase paid via crypto webhook."""
    # Handle None context from background jobs
    chat_id = user_id
    lang = "en"
    
    if context is not None:
        chat_id = context._chat_id or context._user_id or user_id
        if hasattr(context, 'user_data') and context.user_data:
            lang = context.user_data.get("lang", "en")
    else:
        # Fetch language from DB for background jobs
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT lang FROM users WHERE user_id = ?", (user_id,))
            result = c.fetchone()
            conn.close()
            if result:
                lang = result['lang'] or "en"
        except Exception as e:
            logger.debug(f"Could not fetch user language from DB: {e}")
    
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    logger.info(f"Processing successful crypto purchase for user {user_id}, payment {payment_id}. Basket items: {len(basket_snapshot) if basket_snapshot else 0}")

    if not basket_snapshot:
        logger.error(f"CRITICAL: Successful crypto payment {payment_id} for user {user_id} received, but basket snapshot was empty!")
        if get_first_primary_admin_id() and chat_id:
            # Get bot instance
            bot_instance = None
            if context is not None and hasattr(context, 'bot'):
                bot_instance = context.bot
            else:
                try:
                    from main import application
                    if application:
                        bot_instance = application.bot
                except Exception as e:
                    logger.error(f"Could not get bot instance: {e}")
            
            if bot_instance:
                try:
                    await send_message_with_retry(bot_instance, get_first_primary_admin_id(), f"⚠️ Critical Issue: Crypto payment {payment_id} success for user {user_id}, but basket data missing!", parse_mode=None)
                except Exception as admin_notify_e:
                    logger.error(f"Failed to notify admin about critical missing basket data: {admin_notify_e}")
        return False

    finalize_success = await _finalize_purchase(user_id, basket_snapshot, discount_code_used, context)

    if finalize_success:
        logger.info(f"Crypto purchase finalized for {user_id}, payment {payment_id}.")
    else:
        logger.error(f"CRITICAL: Crypto payment {payment_id} success for user {user_id}, but _finalize_purchase failed!")
        if get_first_primary_admin_id() and chat_id:
            # Get bot instance
            bot_instance = None
            if context is not None and hasattr(context, 'bot'):
                bot_instance = context.bot
            else:
                try:
                    from main import application
                    if application:
                        bot_instance = application.bot
                except Exception as e:
                    logger.error(f"Could not get bot instance: {e}")
            
            if bot_instance:
                try:
                    await send_message_with_retry(bot_instance, get_first_primary_admin_id(), f"⚠️ Critical Issue: Crypto payment {payment_id} success for user {user_id}, but finalization FAILED!", parse_mode=None)
                except Exception as admin_notify_e:
                     logger.error(f"Failed to notify admin about critical finalization failure: {admin_notify_e}")
        
        if chat_id:
            # Get bot instance
            bot_instance = None
            if context is not None and hasattr(context, 'bot'):
                bot_instance = context.bot
            else:
                try:
                    from main import application
                    if application:
                        bot_instance = application.bot
                except Exception as e:
                    logger.error(f"Could not get bot instance: {e}")
            
            if bot_instance:
                await send_message_with_retry(bot_instance, chat_id, lang_data.get("error_processing_purchase_contact_support", "❌ Error processing purchase. Contact support."), parse_mode=None)

    return finalize_success


# --- Helper Function to Credit User Balance ---
async def credit_user_balance(user_id: int, amount_eur: Decimal, reason: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Adds funds to a user's balance and notifies them."""
    if not isinstance(amount_eur, Decimal) or amount_eur <= Decimal('0.0'):
        logger.error(f"Invalid amount provided to credit_user_balance for user {user_id}: {amount_eur}")
        return False

    conn = None
    db_update_successful = False
    amount_float = float(amount_eur)
    new_balance_decimal = Decimal('0.0')

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN")
        logger.info(f"Attempting to credit balance for user {user_id} by {amount_float:.2f} EUR. Reason: {reason}")

        c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        old_balance_res = c.fetchone()
        old_balance_float = old_balance_res['balance'] if old_balance_res else 0.0

        update_result = c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount_float, user_id))
        if update_result.rowcount == 0:
            logger.error(f"User {user_id} not found during balance credit update.")
            conn.rollback()
            return False

        c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        new_balance_result = c.fetchone()
        if new_balance_result:
             new_balance_decimal = Decimal(str(new_balance_result['balance']))
        else:
            logger.error(f"Could not fetch new balance for {user_id} after credit update.")
            conn.rollback()
            return False

        conn.commit()
        db_update_successful = True
        logger.info(f"Successfully credited balance for user {user_id}. Added: {amount_eur:.2f} EUR. New Balance: {new_balance_decimal:.2f} EUR")

        log_admin_action(
            admin_id=0,
             action="BALANCE_CREDIT_AUTO",
             target_user_id=user_id,
             reason=reason,
             amount_change=amount_float,
             old_value=old_balance_float,
             new_value=float(new_balance_decimal)
        )

        # Notify User
        bot_instance = None
        if context is not None and hasattr(context, 'bot'):
            bot_instance = context.bot
        else:
            # Get bot from application for background jobs
            try:
                from main import application
                if application:
                    bot_instance = application.bot
            except Exception as e:
                logger.warning(f"Could not get bot instance for user notification: {e}")
        
        if bot_instance:
            lang = "en"
            if context is not None and hasattr(context, 'user_data') and context.user_data:
                lang = context.user_data.get("lang", "en")
            
            if not lang or lang == "en":
                conn_lang = None
                try:
                    conn_lang = get_db_connection()
                    c_lang = conn_lang.cursor()
                    c_lang.execute("SELECT language FROM users WHERE user_id = ?", (user_id,))
                    lang_res = c_lang.fetchone()
                    if lang_res and lang_res['language'] in LANGUAGES:
                        lang = lang_res['language']
                except Exception as lang_e:
                    logger.warning(f"Could not fetch user lang for credit msg: {lang_e}")
                finally:
                    if conn_lang:
                        conn_lang.close()
            lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

            if "Overpayment" in reason:
                notify_msg_template = lang_data.get("credit_overpayment_purchase", "✅ Your purchase was successful! Additionally, an overpayment of {amount} EUR has been credited to your balance. Your new balance is {new_balance} EUR.")
            elif "Underpayment" in reason:
                 notify_msg_template = lang_data.get("credit_underpayment_purchase", "ℹ️ Your purchase failed due to underpayment, but the received amount ({amount} EUR) has been credited to your balance. Your new balance is {new_balance} EUR.")
            else:
                notify_msg_template = lang_data.get("credit_refill", "✅ Your balance has been credited by {amount} EUR. Reason: {reason}. New balance: {new_balance} EUR.")

            notify_msg = notify_msg_template.format(
                amount=format_currency(amount_eur),
                new_balance=format_currency(new_balance_decimal),
                reason=reason
            )

            await send_message_with_retry(bot_instance, user_id, notify_msg, parse_mode=None)
        else:
             logger.error(f"Could not get bot instance to notify user {user_id} about balance credit.")

        return True

    except sqlite3.Error as e:
        logger.error(f"DB error during credit_user_balance user {user_id}: {e}", exc_info=True)
        if conn and conn.in_transaction:
            conn.rollback()
        return False
    except Exception as e:
         logger.error(f"Unexpected error during credit_user_balance user {user_id}: {e}", exc_info=True)
         if conn and conn.in_transaction:
            conn.rollback()
         return False
    finally:
        if conn:
            conn.close()


# --- Callback Handler Wrapper ---
async def handle_confirm_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Wrapper function for confirm_pay."""
    logger.debug("Payment.handle_confirm_pay called, forwarding to user.handle_confirm_pay")
    await user.handle_confirm_pay(update, context, params)


# --- Callback Handler for Crypto Payment Cancellation ---
async def handle_cancel_crypto_payment(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles user clicking Cancel Payment button."""
    query = update.callback_query
    user_id = query.from_user.id
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    
    pending_payment_id = context.user_data.get('pending_payment_id')
    
    if not pending_payment_id:
        logger.warning(f"User {user_id} tried to cancel crypto payment but no pending_payment_id found.")
        await query.answer("No pending payment found. Session may have expired.", show_alert=True)
        await user.handle_shop(update, context)
        return
    
    logger.info(f"User {user_id} requested to cancel crypto payment {pending_payment_id}.")
    
    removal_success = await asyncio.to_thread(remove_pending_deposit, pending_payment_id, trigger="user_cancellation")
    
    context.user_data.pop('pending_payment_id', None)
    
    if removal_success:
        cancellation_success_msg = lang_data.get("payment_cancelled_success", "✅ Payment cancelled successfully. Reserved items have been released.")
        logger.info(f"Successfully cancelled payment {pending_payment_id} for user {user_id}")
    else:
        cancellation_success_msg = lang_data.get("payment_cancel_error", "⚠️ Payment cancellation processed, but there may have been an issue.")
        logger.warning(f"Issue occurred during payment cancellation {pending_payment_id} for user {user_id}")
    
    back_button_text = lang_data.get("back_basket_button", "Back to Basket")
    back_callback = "view_basket"
    
    keyboard = [[InlineKeyboardButton(f"⬅️ {back_button_text}", callback_data=back_callback)]]
    
    try:
        await query.edit_message_text(
            cancellation_success_msg, 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode=None
        )
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.warning(f"Could not edit message during payment cancellation for user {user_id}: {e}")
        await query.answer("Payment cancelled!")
    
    await query.answer()


# --- END OF FILE payment.py ---
