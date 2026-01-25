# --- START OF FILE main.py ---

import logging
import asyncio
import os
import signal
import sqlite3 # Keep for error handling if needed directly
from functools import wraps
from datetime import timedelta
import threading # Added for Flask thread
import json # Added for webhook processing
from decimal import Decimal, ROUND_DOWN, ROUND_UP, ROUND_HALF_UP
import hmac # For webhook signature verification
import hashlib # For webhook signature verification

# --- Telegram Imports ---
from telegram import Update, BotCommand, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardMarkup
from telegram.ext import (
    Application, ApplicationBuilder, Defaults, ContextTypes,
    CommandHandler, CallbackQueryHandler, MessageHandler, filters,
    PicklePersistence, JobQueue
)
from telegram.constants import ParseMode
from telegram.error import Forbidden, BadRequest, NetworkError, RetryAfter, TelegramError

# --- Flask Imports ---
from flask import Flask, request, Response # Added for webhook server
import nest_asyncio # Added to allow nested asyncio loops

# --- Local Imports ---
from utils import (
    TOKEN, ADMIN_ID, init_db, load_all_data, LANGUAGES, THEMES,
    SUPPORT_USERNAME, BASKET_TIMEOUT, clear_all_expired_baskets,
    SECONDARY_ADMIN_IDS, WEBHOOK_URL,
    get_db_connection,
    DATABASE_PATH,
    get_pending_deposit, remove_pending_deposit, FEE_ADJUSTMENT,
    send_message_with_retry,
    log_admin_action,
    format_currency,
    clean_expired_pending_payments,
    get_expired_payments_for_notification,
    clean_abandoned_reservations,
    get_crypto_price_eur,
    get_first_primary_admin_id, # Admin helper for notifications
    is_user_banned  # Import ban check helper
)
import user # Import user module
from user import (
    start, handle_shop, handle_city_selection, handle_district_selection,
    handle_type_selection, handle_product_selection, handle_add_to_basket,
    handle_view_basket, handle_clear_basket, handle_remove_from_basket,
    handle_profile, handle_language_selection, handle_price_list,
    handle_price_list_city, handle_reviews_menu, handle_leave_review,
    handle_view_reviews, handle_leave_review_message, handle_back_start,
    handle_user_discount_code_message, apply_discount_start, remove_discount,
    handle_leave_review_now, handle_refill, handle_view_history,
    handle_refill_amount_message, validate_discount_code,
    handle_apply_discount_basket_pay,
    handle_skip_discount_basket_pay,
    handle_basket_discount_code_message,
    _show_crypto_choices_for_basket,
    handle_pay_single_item,
    handle_confirm_pay, # Direct import of the function
    # <<< ADDED Single Item Discount Flow Handlers from user.py >>>
    handle_apply_discount_single_pay,
    handle_skip_discount_single_pay,
    handle_single_item_discount_code_message
)
import admin # Import admin module
from admin import (
    handle_admin_menu, handle_sales_analytics_menu, handle_sales_dashboard,
    handle_sales_select_period, handle_sales_run, handle_adm_city, handle_adm_dist,
    handle_adm_type, handle_adm_add, handle_adm_size, handle_adm_custom_size,
    handle_confirm_add_drop, cancel_add, handle_adm_manage_cities, handle_adm_add_city,
    handle_adm_edit_city, handle_adm_delete_city, handle_adm_manage_districts,
    handle_adm_manage_districts_city, handle_adm_add_district, handle_adm_edit_district,
    handle_adm_remove_district, handle_adm_manage_products, handle_adm_manage_products_city,
    handle_adm_manage_products_dist, handle_adm_manage_products_type, handle_adm_delete_prod,
    handle_adm_manage_types, handle_adm_add_type, handle_adm_delete_type,
    handle_adm_edit_type_menu, handle_adm_change_type_emoji, handle_adm_change_type_name, handle_adm_confirm_type_name_change,
    handle_adm_reassign_type_start, handle_adm_reassign_select_old, handle_adm_reassign_confirm,
    handle_adm_manage_discounts, handle_adm_toggle_discount, handle_adm_delete_discount,
    handle_adm_add_discount_start, handle_adm_use_generated_code, handle_adm_set_discount_type,
    handle_adm_discount_code_message, handle_adm_discount_value_message,
    handle_adm_set_media,
    handle_adm_broadcast_start, handle_cancel_broadcast,
    handle_confirm_broadcast,
    handle_adm_broadcast_target_type, handle_adm_broadcast_target_city, handle_adm_broadcast_target_status,
    handle_adm_clear_reservations_confirm,
    handle_confirm_yes,
    handle_adm_bot_media_message,  # Import bot media handler
    # Bulk product handlers
    handle_adm_bulk_city, handle_adm_bulk_dist, handle_adm_bulk_type, handle_adm_bulk_add,
    handle_adm_bulk_size, handle_adm_bulk_custom_size, handle_adm_bulk_custom_size_message,
    handle_adm_bulk_price_message, handle_adm_bulk_drop_details_message,
    handle_adm_bulk_remove_last_message, handle_adm_bulk_back_to_messages, handle_adm_bulk_execute_messages,
    cancel_bulk_add,
    # Message handlers that actually exist
    handle_adm_add_city_message, handle_adm_edit_city_message, handle_adm_add_district_message,
    handle_adm_edit_district_message, handle_adm_custom_size_message,
    handle_adm_drop_details_message, handle_adm_price_message,
    # Product type message handlers
    handle_adm_new_type_name_message, handle_adm_new_type_emoji_message, handle_adm_edit_type_name_message,
    handle_adm_new_type_description_message, handle_adm_edit_type_emoji_message,
    # User search handlers
    handle_adm_search_user_start, handle_adm_search_username_message,
    # User detail handlers
    handle_adm_user_deposits, handle_adm_user_purchases, handle_adm_user_actions,
    handle_adm_user_discounts, handle_adm_user_overview,
)
from viewer_admin import (
    handle_viewer_admin_menu,
    handle_viewer_added_products,
    handle_viewer_view_product_media,
    handle_manage_users_start,
    handle_view_user_profile,
    handle_adjust_balance_start,
    handle_toggle_ban_user,
    handle_adjust_balance_amount_message,
    handle_adjust_balance_reason_message
)
try:
    from reseller_management import (
        handle_manage_resellers_menu,
        handle_reseller_manage_id_message,
        handle_reseller_toggle_status,
        handle_manage_reseller_discounts_select_reseller,
        handle_manage_specific_reseller_discounts,
        handle_reseller_add_discount_select_type,
        handle_reseller_add_discount_enter_percent,
        handle_reseller_edit_discount,
        handle_reseller_percent_message,
        handle_reseller_delete_discount_confirm,
    )
except ImportError:
    logger_dummy_reseller = logging.getLogger(__name__ + "_dummy_reseller")
    logger_dummy_reseller.error("Could not import handlers from reseller_management.py.")
    async def handle_manage_resellers_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
        query = update.callback_query; msg = "Reseller Status Mgmt handler not found."
        if query: await query.edit_message_text(msg)
        else: await send_message_with_retry(context.bot, update.effective_chat.id, msg)
    async def handle_manage_reseller_discounts_select_reseller(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
        query = update.callback_query; msg = "Reseller Discount Mgmt handler not found."
        if query: await query.edit_message_text(msg)
        else: await send_message_with_retry(context.bot, update.effective_chat.id, msg)
    async def handle_reseller_manage_id_message(update: Update, context: ContextTypes.DEFAULT_TYPE): pass
    async def handle_reseller_toggle_status(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_manage_specific_reseller_discounts(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_reseller_add_discount_select_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_reseller_add_discount_enter_percent(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_reseller_edit_discount(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_reseller_percent_message(update: Update, context: ContextTypes.DEFAULT_TYPE): pass
    async def handle_reseller_delete_discount_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass

import payment
from payment import credit_user_balance
from payment_solana import check_solana_deposits, refresh_price_cache
from stock import handle_view_stock

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger('apscheduler.scheduler').setLevel(logging.WARNING)
logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)
logging.getLogger('werkzeug').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

nest_asyncio.apply()

flask_app = Flask(__name__)
telegram_app: Application | None = None
main_loop = None

# --- Callback Data Parsing Decorator ---
def callback_query_router(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Check if user is banned before processing any callback query
        if update.effective_user:
            user_id = update.effective_user.id
            
            if await is_user_banned(user_id):
                logger.info(f"Ignoring callback query from banned user {user_id}.")
                if update.callback_query:
                    try:
                        await update.callback_query.answer("❌ Your access has been restricted.", show_alert=True)
                    except Exception as e:
                        logger.error(f"Error answering callback from banned user {user_id}: {e}")
                return
        
        query = update.callback_query
        if query and query.data:
            parts = query.data.split('|')
            command = parts[0]
            params = parts[1:]
            target_func_name = f"handle_{command}"

            KNOWN_HANDLERS = {
                # User Handlers (from user.py)
                "start": user.start, "back_start": user.handle_back_start, "shop": user.handle_shop,
                "city": user.handle_city_selection, "dist": user.handle_district_selection,
                "type": user.handle_type_selection, "product": user.handle_product_selection,
                "add": user.handle_add_to_basket,
                "pay_single_item": user.handle_pay_single_item,
                "view_basket": user.handle_view_basket,
                "clear_basket": user.handle_clear_basket, "remove": user.handle_remove_from_basket,
                "profile": user.handle_profile, "language": user.handle_language_selection,
                "price_list": user.handle_price_list, "price_list_city": user.handle_price_list_city,
                "reviews": user.handle_reviews_menu, "leave_review": user.handle_leave_review,
                "view_reviews": user.handle_view_reviews, "leave_review_now": user.handle_leave_review_now,
                "refill": user.handle_refill,
                "view_history": user.handle_view_history,
                "apply_discount_start": user.apply_discount_start, "remove_discount": user.remove_discount,
                "confirm_pay": user.handle_confirm_pay, # <<< CORRECTED
                "apply_discount_basket_pay": user.handle_apply_discount_basket_pay,
                "skip_discount_basket_pay": user.handle_skip_discount_basket_pay,
                # <<< ADDED Single Item Discount Flow Callbacks (from user.py) >>>
                "apply_discount_single_pay": user.handle_apply_discount_single_pay,
                "skip_discount_single_pay": user.handle_skip_discount_single_pay,

                # Payment Handlers (from payment.py)
                "select_basket_crypto": payment.handle_select_basket_crypto,
                "cancel_crypto_payment": payment.handle_cancel_crypto_payment,
                "select_refill_crypto": payment.handle_select_refill_crypto,

                # Primary Admin Handlers (from admin.py)
                "admin_menu": admin.handle_admin_menu,
                "sales_analytics_menu": admin.handle_sales_analytics_menu, "sales_dashboard": admin.handle_sales_dashboard,
                "sales_select_period": admin.handle_sales_select_period, "sales_run": admin.handle_sales_run,
                "adm_city": admin.handle_adm_city, "adm_dist": admin.handle_adm_dist, "adm_type": admin.handle_adm_type,
                "adm_add": admin.handle_adm_add, "adm_size": admin.handle_adm_size, "adm_custom_size": admin.handle_adm_custom_size,
                "confirm_add_drop": admin.handle_confirm_add_drop, "cancel_add": admin.cancel_add,
                "adm_manage_cities": admin.handle_adm_manage_cities, "adm_add_city": admin.handle_adm_add_city,
                "adm_edit_city": admin.handle_adm_edit_city, "adm_delete_city": admin.handle_adm_delete_city,
                "adm_manage_districts": admin.handle_adm_manage_districts, "adm_manage_districts_city": admin.handle_adm_manage_districts_city,
                "adm_add_district": admin.handle_adm_add_district, "adm_edit_district": admin.handle_adm_edit_district,
                "adm_remove_district": admin.handle_adm_remove_district,
                "adm_manage_products": admin.handle_adm_manage_products, "adm_manage_products_city": admin.handle_adm_manage_products_city,
                "adm_manage_products_dist": admin.handle_adm_manage_products_dist, "adm_manage_products_type": admin.handle_adm_manage_products_type,
                "adm_delete_prod": admin.handle_adm_delete_prod,
                "adm_manage_types": admin.handle_adm_manage_types,
                "adm_edit_type_menu": admin.handle_adm_edit_type_menu,
                "adm_change_type_emoji": admin.handle_adm_change_type_emoji,
                "adm_change_type_name": admin.handle_adm_change_type_name,
                "adm_confirm_type_name_change": admin.handle_adm_confirm_type_name_change,
                "adm_add_type": admin.handle_adm_add_type,
                "adm_delete_type": admin.handle_adm_delete_type,
                "adm_reassign_type_start": admin.handle_adm_reassign_type_start,
                "adm_reassign_select_old": admin.handle_adm_reassign_select_old,
                "adm_reassign_confirm": admin.handle_adm_reassign_confirm,
                "confirm_force_delete_prompt": admin.handle_confirm_force_delete_prompt, # Changed from confirm_force_delete_type
                "adm_manage_discounts": admin.handle_adm_manage_discounts, "adm_toggle_discount": admin.handle_adm_toggle_discount,
                "adm_delete_discount": admin.handle_adm_delete_discount, "adm_add_discount_start": admin.handle_adm_add_discount_start,
                "adm_use_generated_code": admin.handle_adm_use_generated_code, "adm_set_discount_type": admin.handle_adm_set_discount_type,
                "adm_discount_code_message": admin.handle_adm_discount_code_message,
                "adm_discount_value_message": admin.handle_adm_discount_value_message,
                "adm_set_media": admin.handle_adm_set_media,
                "adm_clear_reservations_confirm": admin.handle_adm_clear_reservations_confirm,
                "confirm_yes": admin.handle_confirm_yes,
                "adm_broadcast_start": admin.handle_adm_broadcast_start,
                "adm_broadcast_target_type": admin.handle_adm_broadcast_target_type,
                "adm_broadcast_target_city": admin.handle_adm_broadcast_target_city,
                "adm_broadcast_target_status": admin.handle_adm_broadcast_target_status,
                "cancel_broadcast": admin.handle_cancel_broadcast,
                "confirm_broadcast": admin.handle_confirm_broadcast,
                "adm_manage_reviews": admin.handle_adm_manage_reviews,
                "adm_delete_review_confirm": admin.handle_adm_delete_review_confirm,
                "adm_manage_welcome": admin.handle_adm_manage_welcome,
                "adm_activate_welcome": admin.handle_adm_activate_welcome,
                "adm_add_welcome_start": admin.handle_adm_add_welcome_start,
                "adm_edit_welcome": admin.handle_adm_edit_welcome,
                "adm_delete_welcome_confirm": admin.handle_adm_delete_welcome_confirm,
                "adm_edit_welcome_text": admin.handle_adm_edit_welcome_text,
                "adm_edit_welcome_desc": admin.handle_adm_edit_welcome_desc,
                "adm_reset_default_confirm": admin.handle_reset_default_welcome,
                "confirm_save_welcome": admin.handle_confirm_save_welcome,
                # Bulk product handlers
                "adm_bulk_city": admin.handle_adm_bulk_city,
                "adm_bulk_dist": admin.handle_adm_bulk_dist,
                "adm_bulk_type": admin.handle_adm_bulk_type,
                "adm_bulk_add": admin.handle_adm_bulk_add,
                "adm_bulk_size": admin.handle_adm_bulk_size,
                "adm_bulk_custom_size": admin.handle_adm_bulk_custom_size,
                "cancel_bulk_add": admin.cancel_bulk_add,
                # New bulk message handlers
                "adm_bulk_remove_last_message": admin.handle_adm_bulk_remove_last_message,
                "adm_bulk_back_to_messages": admin.handle_adm_bulk_back_to_messages,
                "adm_bulk_execute_messages": admin.handle_adm_bulk_execute_messages,
                "adm_bulk_create_all": admin.handle_adm_bulk_confirm_all,

                # Viewer Admin Handlers (from viewer_admin.py)
                "viewer_admin_menu": handle_viewer_admin_menu,
                "viewer_added_products": handle_viewer_added_products,
                "viewer_view_product_media": handle_viewer_view_product_media,
                "adm_manage_users": handle_manage_users_start,
                "adm_view_user": handle_view_user_profile,
                "adm_adjust_balance_start": handle_adjust_balance_start,
                "adm_toggle_ban": handle_toggle_ban_user,

                # Reseller Management Handlers (from reseller_management.py)
                "manage_resellers_menu": handle_manage_resellers_menu,
                "reseller_toggle_status": handle_reseller_toggle_status,
                "manage_reseller_discounts_select_reseller": handle_manage_reseller_discounts_select_reseller,
                "reseller_manage_specific": handle_manage_specific_reseller_discounts,
                "reseller_add_discount_select_type": handle_reseller_add_discount_select_type,
                "reseller_add_discount_enter_percent": handle_reseller_add_discount_enter_percent,
                "reseller_edit_discount": handle_reseller_edit_discount,
                "reseller_delete_discount_confirm": handle_reseller_delete_discount_confirm,

                # Stock Handler (from stock.py)
                "view_stock": handle_view_stock,
                
                # User Search Handlers (from admin.py)
                "adm_search_user_start": admin.handle_adm_search_user_start,
                "adm_user_deposits": admin.handle_adm_user_deposits,
                "adm_user_purchases": admin.handle_adm_user_purchases,
                "adm_user_actions": admin.handle_adm_user_actions,
                "adm_user_discounts": admin.handle_adm_user_discounts,
    "adm_debug_reseller_discount": admin.handle_adm_debug_reseller_discount,
    "adm_recent_purchases": admin.handle_adm_recent_purchases,
                "adm_user_overview": admin.handle_adm_user_overview,
                "manual_payment_recovery": admin.handle_manual_payment_recovery,
                "adm_analyze_logs_start": admin.handle_adm_analyze_logs_start,
                # Bulk Price Editor handlers
                "adm_bulk_edit_prices_start": admin.handle_adm_bulk_edit_prices_start,
                "adm_bulk_price_type": admin.handle_adm_bulk_price_type,
                "adm_bulk_price_scope": admin.handle_adm_bulk_price_scope,
                "adm_bulk_price_city": admin.handle_adm_bulk_price_city,
                "adm_bulk_price_city_for_district": admin.handle_adm_bulk_price_city_for_district,
                "adm_bulk_price_district": admin.handle_adm_bulk_price_district,
                "adm_bulk_price_confirm": admin.handle_adm_bulk_price_confirm,
                "adm_edit_single_price": admin.handle_adm_edit_single_price,
                
                # Stuck SOL Funds Recovery Handlers (from admin.py)
                "adm_recover_stuck_funds": admin.handle_recover_stuck_funds,
                "adm_recover_confirm": admin.handle_recover_confirm,
                "adm_recover_single": admin.handle_recover_single_prompt,
                "adm_recover_single_confirm": admin.handle_recover_single_confirm,
            }

            target_func = KNOWN_HANDLERS.get(command)

            if target_func and asyncio.iscoroutinefunction(target_func):
                await target_func(update, context, params)
            else:
                logger.warning(f"No async handler function found or mapped for callback command: {command}")
                try: await query.answer("Unknown action.", show_alert=True)
                except Exception as e: logger.error(f"Error answering unknown callback query {command}: {e}")
        elif query:
            logger.warning("Callback query handler received update without data.")
            try: await query.answer()
            except Exception as e: logger.error(f"Error answering callback query without data: {e}")
        else:
            logger.warning("Callback query handler received update without query object.")
    return wrapper

@callback_query_router
async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback query handler (ban check is now handled in the decorator)."""
    # Ban check is handled in @callback_query_router decorator
    pass

# --- Start Command Wrapper with Ban Check ---
async def start_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Wrapper for /start command that includes ban check"""
    user_id = update.effective_user.id
    
    # Check if user is banned before processing /start command
    if await is_user_banned(user_id):
        logger.info(f"Banned user {user_id} attempted to use /start command.")
        ban_message = "❌ Your access to this bot has been restricted. If you believe this is an error, please contact support."
        await send_message_with_retry(context.bot, update.effective_chat.id, ban_message, parse_mode=None)
        return
    
    # If not banned, proceed with normal start command
    await user.start(update, context)

# --- Admin Command Wrapper with Ban Check ---
async def admin_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Wrapper for /admin command that includes ban check"""
    user_id = update.effective_user.id
    
    # Check if user is banned before processing /admin command
    if await is_user_banned(user_id):
        logger.info(f"Banned user {user_id} attempted to use /admin command.")
        ban_message = "❌ Your access to this bot has been restricted. If you believe this is an error, please contact support."
        await send_message_with_retry(context.bot, update.effective_chat.id, ban_message, parse_mode=None)
        return
    
    # If not banned, proceed with normal admin command
    await admin.handle_admin_menu(update, context)

# --- Central Message Handler (for states) ---
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user: return

    user_id = update.effective_user.id
    state = context.user_data.get('state')
    logger.debug(f"Message received from user {user_id}, state: {state}")

    STATE_HANDLERS = {
        # User Handlers (from user.py)
        'awaiting_review': user.handle_leave_review_message,
        'awaiting_user_discount_code': user.handle_user_discount_code_message,
        'awaiting_basket_discount_code': user.handle_basket_discount_code_message,
        'awaiting_refill_amount': user.handle_refill_amount_message,
        'awaiting_single_item_discount_code': user.handle_single_item_discount_code_message, # <<< ADDED
        'awaiting_refill_crypto_choice': None,
        'awaiting_basket_crypto_choice': None,

        # Admin Message Handlers (from admin.py)
        'awaiting_new_city_name': admin.handle_adm_add_city_message,
        'awaiting_edit_city_name': admin.handle_adm_edit_city_message,
        'awaiting_new_district_name': admin.handle_adm_add_district_message,
        'awaiting_edit_district_name': admin.handle_adm_edit_district_message,
        'awaiting_custom_size': admin.handle_adm_custom_size_message,
        'awaiting_drop_details': admin.handle_adm_drop_details_message,
        'awaiting_price': admin.handle_adm_price_message,
        # Discount code message handlers
        'awaiting_discount_code': admin.handle_adm_discount_code_message,
        'awaiting_discount_value': admin.handle_adm_discount_value_message,
        # Product type message handlers
        'awaiting_new_type_name': admin.handle_adm_new_type_name_message,
        'awaiting_edit_type_name': admin.handle_adm_edit_type_name_message,
        'awaiting_new_type_emoji': admin.handle_adm_new_type_emoji_message,
        'awaiting_new_type_description': admin.handle_adm_new_type_description_message,
        'awaiting_edit_type_emoji': admin.handle_adm_edit_type_emoji_message,
        # Bulk product message handlers
        'awaiting_bulk_custom_size': admin.handle_adm_bulk_custom_size_message,
        'awaiting_bulk_price': admin.handle_adm_bulk_price_message,
        'awaiting_bulk_drop_details': admin.handle_adm_bulk_drop_details_message,
        'awaiting_bulk_messages': admin.handle_adm_bulk_drop_details_message,

        # User Management States (from viewer_admin.py)
        'awaiting_balance_adjustment_amount': handle_adjust_balance_amount_message,
        'awaiting_balance_adjustment_reason': handle_adjust_balance_reason_message,

        # Reseller Management States (from reseller_management.py)
        'awaiting_reseller_manage_id': handle_reseller_manage_id_message,
        'awaiting_reseller_discount_percent': handle_reseller_percent_message,
        
        # User Search States (from admin.py)
        'awaiting_search_username': admin.handle_adm_search_username_message,
        
        # Broadcast States (from admin.py)
        'awaiting_broadcast_message': admin.handle_adm_broadcast_message,
        'awaiting_broadcast_inactive_days': admin.handle_adm_broadcast_inactive_days_message,
        
        # Bot Media States (from admin.py)
        'awaiting_bot_media': admin.handle_adm_bot_media_message,
        
        # Welcome Message States (from admin.py)
        'awaiting_welcome_template_name': admin.handle_adm_welcome_template_name_message,
        'awaiting_welcome_template_text': admin.handle_adm_welcome_template_text_message,
        'awaiting_welcome_template_edit': admin.handle_adm_welcome_template_text_message,
        'awaiting_welcome_description': admin.handle_adm_welcome_description_message,
        'awaiting_welcome_description_edit': admin.handle_adm_welcome_description_message,
        
        # Manual Payment Recovery States (from admin.py)
        'awaiting_payment_recovery_id': admin.handle_payment_recovery_id_message,
        'awaiting_recovery_decision': admin.handle_recovery_decision_message,
        
        # Log Analysis States (from admin.py)
        'awaiting_render_logs': admin.handle_adm_render_logs_message,
        
        # Bulk Price Editor States (from admin.py)
        'awaiting_bulk_price_value': admin.handle_adm_bulk_price_value_message,
        'awaiting_single_price_edit': admin.handle_adm_single_price_edit_message,
        
        # Stuck SOL Funds Recovery States (from admin.py)
        'awaiting_recovery_wallet_address': admin.handle_recovery_wallet_address_message,
        
    }

    # Check if user is banned before processing ANY message (including state handlers)
    if await is_user_banned(user_id):
        logger.info(f"Ignoring message from banned user {user_id} (state: {state}).")
        # Send ban notification message
        try:
            ban_message = "❌ Your access to this bot has been restricted. If you believe this is an error, please contact support."
            await send_message_with_retry(context.bot, update.effective_chat.id, ban_message, parse_mode=None)
        except Exception as e:
            logger.error(f"Error sending ban message to user {user_id}: {e}")
        return
    
    handler_func = STATE_HANDLERS.get(state)
    if handler_func:
        await handler_func(update, context)
    else:
        logger.debug(f"No handler found for user {user_id} in state: {state}")

# --- Error Handler ---
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Extract context info first
    chat_id = None
    user_id = None
    if isinstance(update, Update):
        if update.effective_chat: chat_id = update.effective_chat.id
        if update.effective_user: user_id = update.effective_user.id
    
    # Check for common benign errors FIRST (before logging full traceback)
    if isinstance(context.error, BadRequest):
        error_str_lower = str(context.error).lower()
        if "message is not modified" in error_str_lower:
            logger.debug(f"Ignoring 'message is not modified' error for chat {chat_id}.")
            return
        if "query is too old" in error_str_lower:
            logger.debug(f"Ignoring 'query is too old' error for chat {chat_id}.")
            return
    
    # For actual errors, log the full traceback
    logger.error(msg="Exception while handling an update:", exc_info=context.error)
    logger.error(f"Caught error type: {type(context.error)}")
    logger.debug(f"Error context: user_data={context.user_data}, chat_data={context.chat_data}")

    if chat_id:
        error_message = "An internal error occurred. Please try again later or contact support."
        if isinstance(context.error, BadRequest):
            error_str_lower = str(context.error).lower()
            logger.warning(f"Telegram API BadRequest for chat {chat_id} (User: {user_id}): {context.error}")
            if "can't parse entities" in error_str_lower:
                error_message = "An error occurred displaying the message due to formatting. Please try again."
            else:
                 error_message = "An error occurred communicating with Telegram. Please try again."
        elif isinstance(context.error, NetworkError):
            logger.warning(f"Telegram API NetworkError for chat {chat_id} (User: {user_id}): {context.error}")
            error_message = "A network error occurred. Please check your connection and try again."
        elif isinstance(context.error, Forbidden):
             logger.warning(f"Forbidden error for chat {chat_id} (User: {user_id}): Bot possibly blocked or kicked.")
             return
        elif isinstance(context.error, RetryAfter):
             retry_seconds = context.error.retry_after + 1
             logger.warning(f"Rate limit hit during update processing for chat {chat_id}. Error: {context.error}")
             return
        elif isinstance(context.error, sqlite3.Error):
            logger.error(f"Database error during update handling for chat {chat_id} (User: {user_id}): {context.error}", exc_info=True)
        elif isinstance(context.error, NameError):
             logger.error(f"NameError encountered for chat {chat_id} (User: {user_id}): {context.error}", exc_info=True)
             if 'clear_expired_basket' in str(context.error): error_message = "An internal processing error occurred (payment). Please try again."
             elif 'handle_adm_welcome_' in str(context.error): error_message = "An internal processing error occurred (welcome msg). Please try again."
             else: error_message = "An internal processing error occurred. Please try again or contact support if it persists."
        elif isinstance(context.error, AttributeError):
             logger.error(f"AttributeError encountered for chat {chat_id} (User: {user_id}): {context.error}", exc_info=True)
             if "'NoneType' object has no attribute 'get'" in str(context.error) and "_process_collected_media" in str(context.error.__traceback__): error_message = "An internal processing error occurred (media group). Please try again."
             elif "'module' object has no attribute" in str(context.error) and "handle_confirm_pay" in str(context.error): error_message = "A critical configuration error occurred. Please contact support immediately."
             else: error_message = "An unexpected internal error occurred. Please contact support."
        else:
             logger.exception(f"An unexpected error occurred during update handling for chat {chat_id} (User: {user_id}).")
             error_message = "An unexpected error occurred. Please contact support."
        try:
            bot_instance = context.bot if hasattr(context, 'bot') else (telegram_app.bot if telegram_app else None)
            if bot_instance: await send_message_with_retry(bot_instance, chat_id, error_message, parse_mode=None)
            else: logger.error("Could not get bot instance to send error message.")
        except Exception as e:
            logger.error(f"Failed to send error message to user {chat_id}: {e}")

# --- Bot Setup Functions ---
async def post_init(application: Application) -> None:
    logger.info("Running post_init setup...")
    logger.info("Setting bot commands...")
    await application.bot.set_my_commands([
        BotCommand("start", "Start the bot / Main menu"),
        BotCommand("admin", "Access admin panel (Admin only)"),
    ])
    logger.info("Post_init finished.")

async def post_shutdown(application: Application) -> None:
    logger.info("Running post_shutdown cleanup...")
    logger.info("Post_shutdown finished.")

async def clear_expired_baskets_job_wrapper(context: ContextTypes.DEFAULT_TYPE):
    logger.debug("Running background job: clear_expired_baskets_job")
    try:
        await asyncio.to_thread(clear_all_expired_baskets)
    except Exception as e:
        logger.error(f"Error in background job clear_expired_baskets_job: {e}", exc_info=True)

async def clean_expired_payments_job_wrapper(context: ContextTypes.DEFAULT_TYPE):
    logger.debug("Running background job: clean_expired_payments_job")
    try:
        # Get the list of expired payments before cleaning them up
        expired_user_notifications = await asyncio.to_thread(get_expired_payments_for_notification)
        
        # Clean up the expired payments
        await asyncio.to_thread(clean_expired_pending_payments)
        
        # Send notifications to users
        if expired_user_notifications:
            await send_timeout_notifications(context, expired_user_notifications)
            
    except Exception as e:
        logger.error(f"Error in background job clean_expired_payments_job: {e}", exc_info=True)

async def clean_abandoned_reservations_job_wrapper(context: ContextTypes.DEFAULT_TYPE):
    logger.debug("Running background job: clean_abandoned_reservations_job")
    try:
        await asyncio.to_thread(clean_abandoned_reservations)
    except Exception as e:
        logger.error(f"Error in background job clean_abandoned_reservations_job: {e}", exc_info=True)


async def payment_recovery_job_wrapper(context: ContextTypes.DEFAULT_TYPE):
    """BULLETPROOF: Wrapper for payment recovery job"""
    logger.debug("Running background job: payment_recovery_job")
    try:
        from utils import run_payment_recovery_job
        await asyncio.to_thread(run_payment_recovery_job)
    except Exception as e:
        logger.error(f"❌ BULLETPROOF: Error in payment recovery job: {e}", exc_info=True)

async def solana_deposits_check_job_wrapper(context: ContextTypes.DEFAULT_TYPE):
    """Check Solana deposits periodically"""
    logger.debug("Running background job: solana_deposits_check")
    try:
        await check_solana_deposits(context)
    except Exception as e:
        logger.error(f"Error in Solana deposits check job: {e}", exc_info=True)

async def price_refresh_job_wrapper(context: ContextTypes.DEFAULT_TYPE):
    """Refresh SOL price cache proactively"""
    logger.debug("Running background job: price_refresh")
    try:
        await refresh_price_cache(context)
    except Exception as e:
        logger.error(f"Error in price refresh job: {e}", exc_info=True)


async def send_timeout_notifications(context: ContextTypes.DEFAULT_TYPE, user_notifications: list):
    """Send timeout notifications to users whose payments have expired."""
    for user_notification in user_notifications:
        user_id = user_notification['user_id']
        user_lang = user_notification['language']
        
        try:
            lang_data = LANGUAGES.get(user_lang, LANGUAGES['en'])
            notification_msg = lang_data.get("payment_timeout_notification", 
                "⏰ Payment Timeout: Your payment for basket items has expired after 2 hours. Reserved items have been released.")
            
            await send_message_with_retry(context.bot, user_id, notification_msg, parse_mode=None)
            logger.info(f"Sent payment timeout notification to user {user_id}")
            
        except Exception as e:
            logger.error(f"Failed to send timeout notification to user {user_id}: {e}")


async def retry_purchase_finalization(user_id: int, basket_snapshot: list, discount_code_used: str | None, payment_id: str, context: ContextTypes.DEFAULT_TYPE, max_retries: int = 3):
    """Retry purchase finalization with exponential backoff in case of failures."""
    import payment
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Retrying purchase finalization for payment {payment_id}, attempt {attempt + 1}/{max_retries}")
            
            # Wait with exponential backoff: 5s, 15s, 45s
            if attempt > 0:
                wait_time = 5 * (3 ** attempt)
                logger.info(f"Waiting {wait_time} seconds before retry attempt {attempt + 1}")
                await asyncio.sleep(wait_time)
            
            # Retry the finalization
            purchase_finalized = await payment.process_successful_crypto_purchase(
                user_id, basket_snapshot, discount_code_used, payment_id, context
            )
            
            if purchase_finalized:
                logger.info(f"✅ SUCCESS: Purchase finalization retry succeeded for payment {payment_id} on attempt {attempt + 1}")
                # Remove the pending deposit on success
                await asyncio.to_thread(remove_pending_deposit, payment_id, trigger="retry_success")
                return True
            else:
                logger.warning(f"Purchase finalization retry failed for payment {payment_id} on attempt {attempt + 1}")
                
        except Exception as e:
            logger.error(f"Exception during purchase finalization retry for payment {payment_id}, attempt {attempt + 1}: {e}", exc_info=True)
    
    # All retries failed
    logger.critical(f"🚨 CRITICAL: All {max_retries} retry attempts failed for purchase finalization payment {payment_id} user {user_id}")
    
    # Send critical alert to admin
    if get_first_primary_admin_id() and telegram_app:
        try:
            await send_message_with_retry(
                telegram_app.bot, 
                ADMIN_ID, 
                f"🚨 CRITICAL FAILURE: Purchase {payment_id} for user {user_id} FAILED after {max_retries} retries. "
                f"Payment was successful but finalization completely failed. URGENT MANUAL INTERVENTION REQUIRED!",
                parse_mode=None
            )
        except Exception as notify_error:
            logger.error(f"Failed to notify admin about critical purchase failure: {notify_error}")
    
    return False


# --- Flask Webhook Routes ---
# Note: Solana payments are checked via background job, not webhook
# Keeping this endpoint for compatibility but it's not actively used

@flask_app.route("/webhook", methods=['POST'])
def solana_webhook():
    """
    Solana webhook placeholder - payments are processed via background polling.
    This endpoint is kept for compatibility but not actively used.
    """
    global telegram_app, main_loop
    
    logger.info("🔍 WEBHOOK: Solana webhook accessed (payments use background polling)")
    
    # Solana payments don't use webhooks - they use background polling
    return Response("Solana payments use background polling", status=200)

@flask_app.route(f"/telegram/{TOKEN}", methods=['POST'])
async def telegram_webhook():
    global telegram_app, main_loop
    if not telegram_app or not main_loop:
        logger.error("Telegram webhook received but app/loop not ready.")
        return Response(status=503)
    try:
        update_data = request.get_json(force=True)
        update = Update.de_json(update_data, telegram_app.bot)
        asyncio.run_coroutine_threadsafe(telegram_app.process_update(update), main_loop)
        return Response(status=200)
    except json.JSONDecodeError:
        logger.error("Telegram webhook received invalid JSON.")
        return Response("Invalid JSON", status=400)
    except Exception as e:
        logger.error(f"Error processing Telegram webhook: {e}", exc_info=True)
        return Response("Internal Server Error", status=500)

@flask_app.route("/health", methods=['GET'])
def health_check():
    """Health check endpoint to verify Flask server is running"""
    logger.info("🔍 HEALTH CHECK: Health check endpoint accessed")
    return Response("OK - Flask server is running", status=200)

@flask_app.route("/webhook-test", methods=['POST'])
def webhook_test():
    """Test endpoint to verify webhook reception"""
    logger.info("🔍 WEBHOOK TEST: Test webhook received!")
    logger.info(f"🔍 WEBHOOK TEST: Headers: {dict(request.headers)}")
    logger.info(f"🔍 WEBHOOK TEST: Raw body: {request.get_data()}")
    return Response("Test webhook received successfully", status=200)

@flask_app.route("/", methods=['GET'])
def root():
    """Root endpoint to verify server is running"""
    logger.info("🔍 ROOT: Root endpoint accessed")
    return Response("Payment Bot Server is Running! Webhook: /webhook", status=200)



def main() -> None:
    global telegram_app, main_loop
    logger.info("Starting bot...")
    init_db()
    load_all_data()
    defaults = Defaults(parse_mode=None, block=False)
    
    # Configure persistence to maintain user states across restarts
    persistence = PicklePersistence(filepath="bot_persistence.pickle")
    
    app_builder = ApplicationBuilder().token(TOKEN).defaults(defaults).job_queue(JobQueue()).persistence(persistence)
    app_builder.post_init(post_init)
    app_builder.post_shutdown(post_shutdown)
    application = app_builder.build()
    application.add_handler(CommandHandler("start", start_command_wrapper)) # Use wrapped start with ban check
    application.add_handler(CommandHandler("admin", admin_command_wrapper)) # Use wrapped admin with ban check
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(MessageHandler(
        (filters.TEXT & ~filters.COMMAND) | filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.Document.ALL,
        handle_message
    ))
    application.add_error_handler(error_handler)
    telegram_app = application
    main_loop = asyncio.get_event_loop()
    if BASKET_TIMEOUT > 0:
        job_queue = application.job_queue
        if job_queue:
            logger.info(f"Setting up background jobs...")
            # Basket cleanup job (reduced frequency to 5 minutes for better performance)
            job_queue.run_repeating(clear_expired_baskets_job_wrapper, interval=timedelta(minutes=5), first=timedelta(seconds=10), name="clear_baskets")
            # Payment timeout cleanup job (runs every 10 minutes for better stability)
            job_queue.run_repeating(clean_expired_payments_job_wrapper, interval=timedelta(minutes=10), first=timedelta(minutes=1), name="clean_payments")
            # Abandoned reservation cleanup job (runs every 3 minutes for faster response)
            job_queue.run_repeating(clean_abandoned_reservations_job_wrapper, interval=timedelta(minutes=3), first=timedelta(minutes=2), name="clean_abandoned")
            
            # BULLETPROOF: Payment recovery job (runs every 5 minutes to recover failed payments)
            job_queue.run_repeating(payment_recovery_job_wrapper, interval=timedelta(minutes=5), first=timedelta(minutes=3), name="payment_recovery")
            
            # Solana deposit checking job (runs every minute to check for payments)
            job_queue.run_repeating(solana_deposits_check_job_wrapper, interval=timedelta(minutes=1), first=timedelta(seconds=30), name="solana_deposits")
            
            # SOL price refresh job (runs every 4 minutes to keep price cache fresh)
            job_queue.run_repeating(price_refresh_job_wrapper, interval=timedelta(minutes=4), first=timedelta(minutes=1), name="price_refresh")
            
            logger.info("Background jobs setup complete (basket + payment + reservations + Solana deposits + price refresh).")
        else: logger.warning("Job Queue is not available. Background jobs skipped.")
    else: logger.warning("BASKET_TIMEOUT is not positive. Skipping background job setup.")

    async def setup_webhooks_and_run():
        nonlocal application
        logger.info("Initializing application...")
        await application.initialize()
        logger.info(f"Setting Telegram webhook to: {WEBHOOK_URL}/telegram/{TOKEN}")
        if await application.bot.set_webhook(url=f"{WEBHOOK_URL}/telegram/{TOKEN}", allowed_updates=Update.ALL_TYPES):
            logger.info("Telegram webhook set successfully.")
        else:
            logger.error("Failed to set Telegram webhook.")
            return
        await application.start()
        logger.info("Telegram application started (webhook mode).")
        port = int(os.environ.get("PORT", 10000))
        flask_thread = threading.Thread(target=lambda: flask_app.run(host='0.0.0.0', port=port, debug=False), daemon=True)
        flask_thread.start()
        logger.info(f"Flask server started in a background thread on port {port}.")
        logger.info("Main thread entering keep-alive loop...")
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for s in signals: main_loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s, main_loop, application)))
        try:
            while True: await asyncio.sleep(3600)
        except asyncio.CancelledError: logger.info("Keep-alive loop cancelled.")
        finally: logger.info("Exiting keep-alive loop.")

    async def shutdown(signal, loop, application):
        logger.info(f"Received exit signal {signal.name}...")
        logger.info("Shutting down application...")
        if application:
            await application.stop()
            await application.shutdown()
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]
        logger.info(f"Cancelling {len(tasks)} outstanding tasks")
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Flushing metrics")
        loop.stop()

    try:
        main_loop.run_until_complete(setup_webhooks_and_run())
    except (KeyboardInterrupt, SystemExit) as e:
        logger.info(f"Shutdown initiated by {type(e).__name__}.")
    except Exception as e:
        logger.critical(f"Critical error in main execution loop: {e}", exc_info=True)
    finally:
        logger.info("Main loop finished or interrupted.")
        if main_loop.is_running():
            logger.info("Stopping event loop.") 
            main_loop.stop()
        logger.info("Bot shutdown complete.")

if __name__ == '__main__':
    main()

# --- END OF FILE main.py ---
