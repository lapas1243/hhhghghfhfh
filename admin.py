import sqlite3
import os
import logging
import json
import tempfile
import shutil
import time
import secrets # For generating random codes
import asyncio
from datetime import datetime, timedelta, timezone # <<< Added timezone import
from collections import defaultdict
import math # Add math for pagination calculation
from decimal import Decimal # Ensure Decimal is imported

# Need emoji library for validation (or implement a simpler check)
# Let's try a simpler check first to avoid adding a dependency
# import emoji # Optional, for more robust emoji validation

# --- Telegram Imports ---
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaPhoto, InputMediaVideo, InputMediaAnimation
)
from telegram.constants import ParseMode # Keep for reference
from telegram.ext import ContextTypes, JobQueue # Import JobQueue
from telegram import helpers
import telegram.error as telegram_error

# --- Local Imports ---
from utils import (
    CITIES, DISTRICTS, PRODUCT_TYPES, ADMIN_ID, PRIMARY_ADMIN_IDS, LANGUAGES, THEMES,
    BOT_MEDIA, SIZES, fetch_reviews, format_currency, send_message_with_retry,
    get_date_range, TOKEN, load_all_data, format_discount_value,
    SECONDARY_ADMIN_IDS,
    get_db_connection, MEDIA_DIR, BOT_MEDIA_JSON_PATH, # Import helpers/paths
    DEFAULT_PRODUCT_EMOJI, # Import default emoji
    fetch_user_ids_for_broadcast, # <-- Import broadcast user fetch function
    update_user_broadcast_status, # <-- Import broadcast status tracking function
    save_bot_media_config, # Import bot media save function
    # <<< Welcome Message Helpers >>>
    get_welcome_message_templates, get_welcome_message_template_count, # <-- Added count helper
    add_welcome_message_template,
    update_welcome_message_template,
    delete_welcome_message_template,
    set_active_welcome_message,
    DEFAULT_WELCOME_MESSAGE, # Fallback if needed
    # User status helpers
    get_user_status, get_progress_bar,
    _get_lang_data,  # <<<===== IMPORT THE HELPER =====>>>
    # <<< Admin Logging >>>
    log_admin_action, ACTION_RESELLER_DISCOUNT_DELETE, # Import logging helper and action constant
    ACTION_PRODUCT_TYPE_REASSIGN, # <<< ADDED for reassign type log
    # Admin authorization helpers
    is_primary_admin, is_secondary_admin, is_any_admin, get_first_primary_admin_id
)
# --- Import viewer admin handlers ---
# These now include the user management handlers
try:
    from viewer_admin import (
        handle_viewer_admin_menu,
        handle_manage_users_start, # <-- Needed for the new button
        # Import other viewer handlers if needed elsewhere in admin.py
        handle_viewer_added_products, # <<< NEED THIS
        handle_viewer_view_product_media # <<< NEED THIS
    )
except ImportError:
    logger_dummy_viewer = logging.getLogger(__name__ + "_dummy_viewer")
    logger_dummy_viewer.error("Could not import handlers from viewer_admin.py.")
    # Define dummy handlers for viewer admin menu and user management if import fails
    async def handle_viewer_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
        query = update.callback_query
        msg = "Secondary admin menu handler not found."
        if query: await query.edit_message_text(msg, parse_mode=None)
        else: await send_message_with_retry(context.bot, update.effective_chat.id, msg, parse_mode=None)
    async def handle_manage_users_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
        query = update.callback_query
        msg = "Manage Users handler not found."
        if query: await query.edit_message_text(msg, parse_mode=None)
        else: await send_message_with_retry(context.bot, update.effective_chat.id, msg, parse_mode=None)
    # Add dummies for other viewer handlers if they were used directly in admin.py
    async def handle_viewer_added_products(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
        query = update.callback_query
        msg = "Added Products Log handler not found."
        if query: await query.edit_message_text(msg, parse_mode=None)
        else: await send_message_with_retry(context.bot, update.effective_chat.id, msg, parse_mode=None)
    async def handle_viewer_view_product_media(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
        query = update.callback_query
        msg = "View Product Media handler not found."
        if query: await query.edit_message_text(msg, parse_mode=None)
        else: await send_message_with_retry(context.bot, update.effective_chat.id, msg, parse_mode=None)
# ------------------------------------

# --- Import Reseller Management Handlers ---
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
    # Add dummies for other reseller handlers if needed (less critical for basic menu)
    async def handle_reseller_manage_id_message(update: Update, context: ContextTypes.DEFAULT_TYPE): pass
    async def handle_reseller_toggle_status(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_manage_specific_reseller_discounts(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_reseller_add_discount_select_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_reseller_add_discount_enter_percent(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_reseller_edit_discount(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_reseller_percent_message(update: Update, context: ContextTypes.DEFAULT_TYPE): pass
    async def handle_reseller_delete_discount_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
# ------------------------------------------


# Import stock handler
try: from stock import handle_view_stock
except ImportError:
    logger_dummy_stock = logging.getLogger(__name__ + "_dummy_stock")
    logger_dummy_stock.error("Could not import handle_view_stock from stock.py.")
    async def handle_view_stock(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
        query = update.callback_query # Corrected variable name
        msg = "Stock viewing handler not found."
        if query: await query.edit_message_text(msg, parse_mode=None)
        else: await send_message_with_retry(context.bot, update.effective_chat.id, msg, parse_mode=None)

# Logging setup
logger = logging.getLogger(__name__)

# --- Constants for Media Group Handling ---
MEDIA_GROUP_COLLECTION_DELAY = 3.5 # Increased from 2.0 to 3.5 seconds to ensure all media is collected
TEMPLATES_PER_PAGE = 5 # Pagination for welcome templates

# --- Helper Function to Remove Existing Job ---
def remove_job_if_exists(name: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Removes a job by name if it exists."""
    if not hasattr(context, 'job_queue') or not context.job_queue:
        logger.warning("Job queue not available in context for remove_job_if_exists.")
        return False
    current_jobs = context.job_queue.get_jobs_by_name(name)
    if not current_jobs:
        return False
    for job in current_jobs:
        job.schedule_removal()
        logger.debug(f"Removed existing job: {name}")
    return True

# --- Helper to Prepare and Confirm Drop (Handles Download) ---
async def _prepare_and_confirm_drop(
    context: ContextTypes.DEFAULT_TYPE,
    user_data: dict,
    chat_id: int,
    user_id: int,
    text: str,
    collected_media_info: list
    ):
    """Downloads media (if any) and presents the confirmation message."""
    required_context = ["admin_city", "admin_district", "admin_product_type", "pending_drop_size", "pending_drop_price"]
    if not all(k in user_data for k in required_context):
        logger.error(f"_prepare_and_confirm_drop: Context lost for user {user_id}.")
        await send_message_with_retry(context.bot, chat_id, "❌ Klaida: Kontekstas prarastas. Pradėkite iš naujo.", parse_mode=None)
        keys_to_clear = ["state", "pending_drop", "pending_drop_size", "pending_drop_price", "collecting_media_group_id", "collected_media"]
        for key in keys_to_clear: user_data.pop(key, None)
        return

    temp_dir = None
    media_list_for_db = []
    download_errors = 0

    if collected_media_info:
        try:
            temp_dir = await asyncio.to_thread(tempfile.mkdtemp)
            logger.info(f"Created temp dir for media download: {temp_dir} (User: {user_id})")
            for i, media_info in enumerate(collected_media_info):
                media_type = media_info['type']
                file_id = media_info['file_id']
                file_extension = ".jpg" if media_type == "photo" else ".mp4" if media_type in ["video", "gif"] else ".dat"
                temp_file_path = os.path.join(temp_dir, f"{file_id}{file_extension}")
                try:
                    logger.info(f"Downloading media {i+1}/{len(collected_media_info)} ({file_id}) to {temp_file_path}")
                    file_obj = await context.bot.get_file(file_id)
                    await file_obj.download_to_drive(custom_path=temp_file_path)
                    if not await asyncio.to_thread(os.path.exists, temp_file_path) or await asyncio.to_thread(os.path.getsize, temp_file_path) == 0:
                        raise IOError(f"Downloaded file {temp_file_path} is missing or empty.")
                    media_list_for_db.append({"type": media_type, "path": temp_file_path, "file_id": file_id})
                    logger.info(f"Media download {i+1} successful.")
                except (telegram_error.TelegramError, IOError, OSError) as e:
                    logger.error(f"Error downloading/verifying media {i+1} ({file_id}): {e}")
                    download_errors += 1
                except Exception as e:
                    logger.error(f"Unexpected error downloading media {i+1} ({file_id}): {e}", exc_info=True)
                    download_errors += 1
            if download_errors > 0:
                await send_message_with_retry(context.bot, chat_id, f"⚠️ Warning: {download_errors} media file(s) failed to download. Adding drop with successfully downloaded media only.", parse_mode=None)
        except Exception as e:
             logger.error(f"Error setting up/during media download loop user {user_id}: {e}", exc_info=True)
             await send_message_with_retry(context.bot, chat_id, "⚠️ Warning: Error during media processing. Drop will be added without media.", parse_mode=None)
             media_list_for_db = []
             if temp_dir and await asyncio.to_thread(os.path.exists, temp_dir): await asyncio.to_thread(shutil.rmtree, temp_dir, ignore_errors=True); temp_dir = None

    user_data["pending_drop"] = {
        "city": user_data["admin_city"], "district": user_data["admin_district"],
        "product_type": user_data["admin_product_type"], "size": user_data["pending_drop_size"],
        "price": user_data["pending_drop_price"], "original_text": text,
        "media": media_list_for_db,
        "temp_dir": temp_dir
    }
    user_data.pop("state", None)

    city_name = user_data['admin_city']
    dist_name = user_data['admin_district']
    type_name = user_data['admin_product_type']
    type_emoji = PRODUCT_TYPES.get(type_name, DEFAULT_PRODUCT_EMOJI)
    size_name = user_data['pending_drop_size']
    price_str = format_currency(user_data['pending_drop_price'])
    text_preview = text[:200] + ("..." if len(text) > 200 else "")
    text_display = text_preview if text_preview else "No details text provided"
    media_count = len(user_data["pending_drop"]["media"])
    total_submitted_media = len(collected_media_info)
    media_status = f"{media_count}/{total_submitted_media} Downloaded" if total_submitted_media > 0 else "No"
    if download_errors > 0: media_status += " (Errors)"

    msg = (f"📦 Patvirtinti naują produktą\n\n🏙️ Miestas: {city_name}\n🏘️ Rajonas: {dist_name}\n{type_emoji} Tipas: {type_name}\n"
           f"📏 Dydis: {size_name}\n💰 Kaina: {price_str} EUR\n📝 Aprašymas: {text_display}\n"
           f"📸 Medija: {media_status}\n\nPridėti šį produktą?")
    keyboard = [[InlineKeyboardButton("✅ Taip, pridėti", callback_data="confirm_add_drop"),
                InlineKeyboardButton("❌ Ne, atšaukti", callback_data="cancel_add")]]
    await send_message_with_retry(context.bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

# --- Job Function to Process Collected Media Group ---
async def _process_collected_media(context: ContextTypes.DEFAULT_TYPE):
    """Job callback to process a collected media group."""
    job_data = context.job.data
    user_id = job_data.get("user_id")
    chat_id = job_data.get("chat_id")
    media_group_id = job_data.get("media_group_id")

    if not user_id or not chat_id or not media_group_id:
        logger.error(f"Job _process_collected_media missing user_id, chat_id, or media_group_id in data: {job_data}")
        return

    logger.info(f"Job executing: Process media group {media_group_id} for user {user_id}")
    user_data = context.application.user_data.get(user_id, {})
    if not user_data:
         logger.error(f"Job {media_group_id}: Could not find user_data for user {user_id}.")
         return

    collected_info = user_data.get('collected_media', {}).get(media_group_id)
    if not collected_info or 'media' not in collected_info:
        logger.warning(f"Job {media_group_id}: No collected media info found in user_data for user {user_id}. Might be already processed or cancelled.")
        user_data.pop('collecting_media_group_id', None)
        if 'collected_media' in user_data:
            user_data['collected_media'].pop(media_group_id, None)
            if not user_data['collected_media']:
                user_data.pop('collected_media', None)
        return

    collected_media = collected_info.get('media', [])
    caption = collected_info.get('caption', '')

    user_data.pop('collecting_media_group_id', None)
    if 'collected_media' in user_data and media_group_id in user_data['collected_media']:
        del user_data['collected_media'][media_group_id]
        if not user_data['collected_media']:
            user_data.pop('collected_media', None)

    await _prepare_and_confirm_drop(context, user_data, chat_id, user_id, caption, collected_media)

# --- Job Function to Process Bulk Collected Media Group ---
async def _process_bulk_collected_media(context: ContextTypes.DEFAULT_TYPE):
    """Job callback to process a bulk collected media group."""
    job_data = context.job.data
    user_id = job_data.get("user_id")
    chat_id = job_data.get("chat_id")
    media_group_id = job_data.get("media_group_id")

    if not user_id or not chat_id or not media_group_id:
        logger.error(f"Job _process_bulk_collected_media missing user_id, chat_id, or media_group_id in data: {job_data}")
        return

    logger.info(f"BULK DEBUG: Job executing: Process bulk media group {media_group_id} for user {user_id}")
    user_data = context.application.user_data.get(user_id, {})
    if not user_data:
        logger.error(f"BULK DEBUG: Job {media_group_id}: Could not find user_data for user {user_id}.")
        return

    collected_info = user_data.get('bulk_collected_media', {}).get(media_group_id)
    if not collected_info or 'media' not in collected_info:
        logger.warning(f"BULK DEBUG: Job {media_group_id}: No bulk collected media info found in user_data for user {user_id}. Might be already processed or cancelled.")
        user_data.pop('bulk_collecting_media_group_id', None)
        if 'bulk_collected_media' in user_data:
            user_data['bulk_collected_media'].pop(media_group_id, None)
            if not user_data['bulk_collected_media']:
                user_data.pop('bulk_collected_media', None)
        return

    collected_media = collected_info.get('media', [])
    caption = collected_info.get('caption', '')

    # Clean up the media group data
    user_data.pop('bulk_collecting_media_group_id', None)
    if 'bulk_collected_media' in user_data and media_group_id in user_data['bulk_collected_media']:
        del user_data['bulk_collected_media'][media_group_id]
        if not user_data['bulk_collected_media']:
            user_data.pop('bulk_collected_media', None)

    # Create message data for the bulk collection
    bulk_messages = user_data.get("bulk_messages", [])
    message_data = {
        "text": caption,
        "media": collected_media,
        "timestamp": int(time.time())
    }

    # Add the collected media group as a single message
    bulk_messages.append(message_data)
    user_data["bulk_messages"] = bulk_messages
    
    logger.info(f"BULK DEBUG: Added media group {media_group_id} to bulk_messages as single message. New count: {len(bulk_messages)}")
    
    # Send a simple status update message instead of trying to recreate the full status
    try:
        from utils import send_message_with_retry
        await send_message_with_retry(context.bot, chat_id, 
            f"✅ Media group added to bulk collection! Total messages: {len(bulk_messages)}/10", 
            parse_mode=None)
        logger.info(f"BULK DEBUG: Sent status update for media group {media_group_id}")
    except Exception as e:
        logger.error(f"BULK DEBUG: Error sending bulk status update: {e}")


# --- Modified Handler for Drop Details Message ---
async def handle_adm_drop_details_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the message containing drop text and optional media (single or group)."""
    if not update.message or not update.effective_user:
        logger.warning("handle_adm_drop_details_message received invalid update.")
        return

    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    user_specific_data = context.user_data

    if not is_primary_admin(user_id): return

    if user_specific_data.get("state") != "awaiting_drop_details":
        logger.debug(f"Ignoring drop details message from user {user_id}, state is not 'awaiting_drop_details' (state: {user_specific_data.get('state')})")
        return

    required_context = ["admin_city", "admin_district", "admin_product_type", "pending_drop_size", "pending_drop_price"]
    if not all(k in user_specific_data for k in required_context):
        logger.warning(f"Context lost for user {user_id} before processing drop details.")
        await send_message_with_retry(context.bot, chat_id, "❌ Klaida: Kontekstas prarastas. Pradėkite iš naujo.", parse_mode=None)
        keys_to_clear = ["state", "pending_drop", "pending_drop_size", "pending_drop_price", "collecting_media_group_id", "collected_media"]
        for key in keys_to_clear: user_specific_data.pop(key, None)
        return

    media_group_id = update.message.media_group_id
    job_name = f"process_media_group_{user_id}_{media_group_id}" if media_group_id else None

    media_type, file_id = None, None
    if update.message.photo: media_type, file_id = "photo", update.message.photo[-1].file_id
    elif update.message.video: media_type, file_id = "video", update.message.video.file_id
    elif update.message.animation: media_type, file_id = "gif", update.message.animation.file_id

    text = (update.message.caption or update.message.text or "").strip()

    if media_group_id:
        logger.debug(f"Received message part of media group {media_group_id} from user {user_id}")
        if 'collected_media' not in user_specific_data:
            user_specific_data['collected_media'] = {}

        if media_group_id not in user_specific_data['collected_media']:
            user_specific_data['collected_media'][media_group_id] = {'media': [], 'caption': None}
            logger.info(f"Started collecting media for group {media_group_id} user {user_id}")
            user_specific_data['collecting_media_group_id'] = media_group_id

        if media_type and file_id:
            if not any(m['file_id'] == file_id for m in user_specific_data['collected_media'][media_group_id]['media']):
                user_specific_data['collected_media'][media_group_id]['media'].append(
                    {'type': media_type, 'file_id': file_id}
                )
                logger.debug(f"Added media {file_id} ({media_type}) to group {media_group_id}")

        if text:
             user_specific_data['collected_media'][media_group_id]['caption'] = text
             logger.debug(f"Stored/updated caption for group {media_group_id}")

        remove_job_if_exists(job_name, context)
        if hasattr(context, 'job_queue') and context.job_queue:
            try:
                context.job_queue.run_once(
                    _process_collected_media,
                    when=timedelta(seconds=MEDIA_GROUP_COLLECTION_DELAY),
                    data={'media_group_id': media_group_id, 'chat_id': chat_id, 'user_id': user_id},
                    name=job_name,
                    job_kwargs={'misfire_grace_time': 30}  # Increased grace time from 15 to 30 seconds
                )
                logger.debug(f"Scheduled/Rescheduled job {job_name} for media group {media_group_id}")
            except Exception as job_error:
                logger.error(f"Failed to schedule media group job {job_name}: {job_error}")
                # Fallback: Process immediately if job scheduling fails
                await _prepare_and_confirm_drop(context, user_specific_data, chat_id, user_id, text, user_specific_data['collected_media'][media_group_id]['media'])
        else:
            logger.error("JobQueue not found in context. Cannot schedule media group processing.")
            # Fallback: Process immediately if no job queue
            if media_group_id in user_specific_data.get('collected_media', {}):
                await _prepare_and_confirm_drop(context, user_specific_data, chat_id, user_id, text, user_specific_data['collected_media'][media_group_id]['media'])
            else:
                await send_message_with_retry(context.bot, chat_id, "❌ Error: Internal components missing. Cannot process media group.", parse_mode=None)

    else:
        if user_specific_data.get('collecting_media_group_id'):
            logger.warning(f"Received single message from user {user_id} while potentially collecting media group {user_specific_data['collecting_media_group_id']}. Ignoring for drop.")
            return

        logger.debug(f"Received single message (or text only) for drop details from user {user_id}")
        user_specific_data.pop('collecting_media_group_id', None)
        user_specific_data.pop('collected_media', None)

        single_media_info = []
        if media_type and file_id:
            single_media_info.append({'type': media_type, 'file_id': file_id})

        await _prepare_and_confirm_drop(context, user_specific_data, chat_id, user_id, text, single_media_info)


# --- Admin Callback Handlers ---
async def handle_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Displays the main admin dashboard, handling both command and callback."""
    user = update.effective_user
    query = update.callback_query
    if not user:
        logger.warning("handle_admin_menu triggered without effective_user.")
        if query: await query.answer("Error: Could not identify user.", show_alert=True)
        return

    user_id = user.id
    chat_id = update.effective_chat.id
    primary_admin = is_primary_admin(user_id)
    secondary_admin = is_secondary_admin(user_id)

    if not primary_admin and not secondary_admin:
        logger.warning(f"Non-admin user {user_id} attempted to access admin menu via {'command' if not query else 'callback'}.")
        msg = "Prieiga uždrausta."
        if query: await query.answer(msg, show_alert=True)
        else: await send_message_with_retry(context.bot, chat_id, msg, parse_mode=None)
        return

    if secondary_admin and not primary_admin:
        logger.info(f"Redirecting secondary admin {user_id} to viewer admin menu.")
        try:
            return await handle_viewer_admin_menu(update, context)
        except NameError:
            logger.error("handle_viewer_admin_menu not found, check imports.")
            fallback_msg = "Viewer admin menu handler is missing."
            if query: await query.edit_message_text(fallback_msg)
            else: await send_message_with_retry(context.bot, chat_id, fallback_msg)
            return

    total_users, total_user_balance, active_products, total_sales_value = 0, Decimal('0.0'), 0, Decimal('0.0')
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM users")
        res_users = c.fetchone(); total_users = res_users['count'] if res_users else 0
        c.execute("SELECT COALESCE(SUM(balance), 0.0) as total_bal FROM users")
        res_balance = c.fetchone(); total_user_balance = Decimal(str(res_balance['total_bal'])) if res_balance else Decimal('0.0')
        c.execute("SELECT COUNT(*) as count FROM products WHERE available > reserved")
        res_products = c.fetchone(); active_products = res_products['count'] if res_products else 0
        c.execute("SELECT COALESCE(SUM(price_paid), 0.0) as total_sales FROM purchases")
        res_sales = c.fetchone(); total_sales_value = Decimal(str(res_sales['total_sales'])) if res_sales else Decimal('0.0')
    except sqlite3.Error as e:
        logger.error(f"DB error fetching admin dashboard data: {e}", exc_info=True)
        error_message = "❌ Klaida įkeliant admin duomenis."
        if query:
            try: await query.edit_message_text(error_message, parse_mode=None)
            except Exception: pass
        else: await send_message_with_retry(context.bot, chat_id, error_message, parse_mode=None)
        return
    finally:
        if conn: conn.close()

    total_user_balance_str = format_currency(total_user_balance)
    total_sales_value_str = format_currency(total_sales_value)
    msg = (
       f"🔧 Administratoriaus Valdymas\n\n"
       f"👥 Viso vartotojų: {total_users}\n"
       f"💰 Vartotojų balansų suma: {total_user_balance_str} EUR\n"
       f"📈 Bendra pardavimų vertė: {total_sales_value_str} EUR\n"
       f"📦 Aktyvūs produktai: {active_products}\n\n"
       "Pasirinkite veiksmą:"
    )

    keyboard = [
        [InlineKeyboardButton("📊 Pardavimų analizė", callback_data="sales_analytics_menu")],
        [InlineKeyboardButton("🔍 Paskutiniai pirkimai", callback_data="adm_recent_purchases|0")],
        [InlineKeyboardButton("➕ Pridėti produktus", callback_data="adm_city")],
        [InlineKeyboardButton("📦 Masinis produktų pridėjimas", callback_data="adm_bulk_city")],
        [InlineKeyboardButton("🗑️ Valdyti produktus", callback_data="adm_manage_products")],
        [InlineKeyboardButton("🔍 Ieškoti vartotojo", callback_data="adm_search_user_start")],
        [InlineKeyboardButton("👑 Valdyti perpardavėjus", callback_data="manage_resellers_menu")],
        [InlineKeyboardButton("🏷️ Perpardavėjų nuolaidos", callback_data="manage_reseller_discounts_select_reseller|0")],
        [InlineKeyboardButton("🏷️ Valdyti nuolaidų kodus", callback_data="adm_manage_discounts")],
        [InlineKeyboardButton("👋 Valdyti pasveikinimą", callback_data="adm_manage_welcome|0")],
        [InlineKeyboardButton("📦 Peržiūrėti atsargas", callback_data="view_stock")],
        [InlineKeyboardButton("📜 Pridėtų produktų žurnalas", callback_data="viewer_added_products|0")],
        [InlineKeyboardButton("🗺️ Valdyti rajonus", callback_data="adm_manage_districts")],
        [InlineKeyboardButton("🏙️ Valdyti miestus", callback_data="adm_manage_cities")],
        [InlineKeyboardButton("🧩 Valdyti produktų tipus", callback_data="adm_manage_types")],
        [InlineKeyboardButton("🔄 Priskirti produkto tipą", callback_data="adm_reassign_type_start")],
        [InlineKeyboardButton("🚫 Valdyti atsiliepimus", callback_data="adm_manage_reviews|0")],
        [InlineKeyboardButton("🧹 Išvalyti VISAS rezervacijas", callback_data="adm_clear_reservations_confirm")],
        [InlineKeyboardButton("📢 Transliuoti žinutę", callback_data="adm_broadcast_start")],
        [InlineKeyboardButton("🔧 Rankinis mokėjimo atkūrimas", callback_data="manual_payment_recovery")],
        [InlineKeyboardButton("💰 Masinis kainų redagavimas", callback_data="adm_bulk_edit_prices_start")],
        [InlineKeyboardButton("➕ Pridėti naują miestą", callback_data="adm_add_city")],
        [InlineKeyboardButton("📸 Nustatyti boto mediją", callback_data="adm_set_media")],
        [InlineKeyboardButton("🏠 Pagrindinis meniu", callback_data="back_start")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if query:
        try:
            await query.edit_message_text(msg, reply_markup=reply_markup, parse_mode=None)
        except telegram_error.BadRequest as e:
            if "message is not modified" not in str(e).lower():
                logger.error(f"Error editing admin menu message: {e}")
                await send_message_with_retry(context.bot, chat_id, msg, reply_markup=reply_markup, parse_mode=None)
            else:
                await query.answer()
        except Exception as e:
            logger.error(f"Unexpected error editing admin menu: {e}", exc_info=True)
            await send_message_with_retry(context.bot, chat_id, msg, reply_markup=reply_markup, parse_mode=None)
    else:
        await send_message_with_retry(context.bot, chat_id, msg, reply_markup=reply_markup, parse_mode=None)


# --- Sales Analytics Handlers ---
async def handle_sales_analytics_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Displays the sales analytics submenu."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    msg = "📊 Pardavimų analizė\n\nPasirinkite ataskaitą:"
    keyboard = [
        [InlineKeyboardButton("📈 Peržiūrėti suvestinę", callback_data="sales_dashboard")],
        [InlineKeyboardButton("📅 Generuoti ataskaitą", callback_data="sales_select_period|main")],
        [InlineKeyboardButton("🏙️ Pardavimai pagal miestą", callback_data="sales_select_period|by_city")],
        [InlineKeyboardButton("💎 Pardavimai pagal tipą", callback_data="sales_select_period|by_type")],
        [InlineKeyboardButton("🏆 Populiariausi produktai", callback_data="sales_select_period|top_prod")],
        [InlineKeyboardButton("⬅️ Atgal", callback_data="admin_menu")]
    ]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_sales_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Displays a quick sales dashboard for today, this week, this month."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    periods = {
        "today": ("☀️ Šiandien ({})", datetime.now(timezone.utc).strftime("%Y-%m-%d")), # Use UTC
        "week": ("🗓️ Ši savaitė (Pr-Sk)", None),
        "month": ("📆 Šis mėnuo", None)
    }
    msg = "📊 Pardavimų suvestinė\n\n"
    conn = None # Initialize conn
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        for period_key, (label_template, date_str) in periods.items():
            start, end = get_date_range(period_key)
            if not start or not end:
                msg += f"Could not calculate range for {period_key}.\n\n"
                continue
            # Use column names
            c.execute("SELECT COALESCE(SUM(price_paid), 0.0) as total_revenue, COUNT(*) as total_units FROM purchases WHERE purchase_date BETWEEN ? AND ?", (start, end))
            result = c.fetchone()
            revenue = result['total_revenue'] if result else 0.0
            units = result['total_units'] if result else 0
            aov = revenue / units if units > 0 else 0.0
            revenue_str = format_currency(revenue)
            aov_str = format_currency(aov)
            label_formatted = label_template.format(date_str) if date_str else label_template
            msg += f"{label_formatted}\n"
            msg += f"    Pajamos: {revenue_str} EUR\n"
            msg += f"    Parduota vienetų: {units}\n"
            msg += f"    Vid. užsakymo vertė: {aov_str} EUR\n\n"
    except sqlite3.Error as e:
        logger.error(f"DB error generating sales dashboard: {e}", exc_info=True)
        msg += "\n❌ Error fetching dashboard data."
    except Exception as e:
        logger.error(f"Unexpected error in sales dashboard: {e}", exc_info=True)
        msg += "\n❌ An unexpected error occurred."
    finally:
         if conn: conn.close() # Close connection if opened
    keyboard = [[InlineKeyboardButton("⬅️ Atgal", callback_data="sales_analytics_menu")]]
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Error editing sales dashboard: {e}")
        else: await query.answer()

async def handle_sales_select_period(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows options for selecting a reporting period."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params:
        logger.warning("handle_sales_select_period called without report_type.")
        return await query.answer("Error: Report type missing.", show_alert=True)
    report_type = params[0]
    context.user_data['sales_report_type'] = report_type
    keyboard = [
        [InlineKeyboardButton("Šiandien", callback_data=f"sales_run|{report_type}|today"),
         InlineKeyboardButton("Vakar", callback_data=f"sales_run|{report_type}|yesterday")],
        [InlineKeyboardButton("Ši savaitė", callback_data=f"sales_run|{report_type}|week"),
         InlineKeyboardButton("Praėjusi savaitė", callback_data=f"sales_run|{report_type}|last_week")],
        [InlineKeyboardButton("Šis mėnuo", callback_data=f"sales_run|{report_type}|month"),
         InlineKeyboardButton("Praėjęs mėnuo", callback_data=f"sales_run|{report_type}|last_month")],
        [InlineKeyboardButton("Nuo metų pradžios", callback_data=f"sales_run|{report_type}|year")],
        [InlineKeyboardButton("⬅️ Atgal", callback_data="sales_analytics_menu")]
    ]
    await query.edit_message_text("📅 Pasirinkite laikotarpį", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_sales_run(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Generates and displays the selected sales report."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params or len(params) < 2:
        logger.warning("handle_sales_run called with insufficient parameters.")
        return await query.answer("Error: Report type or period missing.", show_alert=True)
    report_type, period_key = params[0], params[1]
    start_time, end_time = get_date_range(period_key)
    if not start_time or not end_time:
        return await query.edit_message_text("❌ Error: Invalid period selected.", parse_mode=None)
    period_title = period_key.replace('_', ' ').title()
    msg = ""
    conn = None # Initialize conn
    try:
        conn = get_db_connection() # Use helper
        # row_factory is set in helper
        c = conn.cursor()
        base_query = "FROM purchases WHERE purchase_date BETWEEN ? AND ?"
        base_params = (start_time, end_time)
        if report_type == "main":
            c.execute(f"SELECT COALESCE(SUM(price_paid), 0.0) as total_revenue, COUNT(*) as total_units {base_query}", base_params)
            result = c.fetchone()
            revenue = result['total_revenue'] if result else 0.0
            units = result['total_units'] if result else 0
            aov = revenue / units if units > 0 else 0.0
            revenue_str = format_currency(revenue)
            aov_str = format_currency(aov)
            msg = (f"📊 Pardavimų ataskaita: {period_title}\n\nPajamos: {revenue_str} EUR\n"
                   f"Parduota: {units}\nVid. užsakymo vertė: {aov_str} EUR")
        elif report_type == "by_city":
            c.execute(f"SELECT city, COALESCE(SUM(price_paid), 0.0) as city_revenue, COUNT(*) as city_units {base_query} GROUP BY city ORDER BY city_revenue DESC", base_params)
            results = c.fetchall()
            msg = f"🏙️ Pardavimai pagal miestą: {period_title}\n\n"
            if results:
                for row in results:
                    msg += f"{row['city'] or 'N/A'}: {format_currency(row['city_revenue'])} EUR ({row['city_units'] or 0} vnt.)\n"
            else: msg += "Nėra pardavimų duomenų šiam laikotarpiui."
        elif report_type == "by_type":
            c.execute(f"SELECT product_type, COALESCE(SUM(price_paid), 0.0) as type_revenue, COUNT(*) as type_units {base_query} GROUP by product_type ORDER BY type_revenue DESC", base_params)
            results = c.fetchall()
            msg = f"📊 Pardavimai pagal tipą: {period_title}\n\n"
            if results:
                for row in results:
                    type_name = row['product_type'] or 'N/A'
                    emoji = PRODUCT_TYPES.get(type_name, DEFAULT_PRODUCT_EMOJI)
                    msg += f"{emoji} {type_name}: {format_currency(row['type_revenue'])} EUR ({row['type_units'] or 0} vnt.)\n"
            else: msg += "Nėra pardavimų duomenų šiam laikotarpiui."
        elif report_type == "top_prod":
            c.execute(f"""
                SELECT pu.product_name, pu.product_size, pu.product_type,
                       COALESCE(SUM(pu.price_paid), 0.0) as prod_revenue,
                       COUNT(pu.id) as prod_units
                FROM purchases pu
                WHERE pu.purchase_date BETWEEN ? AND ?
                GROUP BY pu.product_name, pu.product_size, pu.product_type
                ORDER BY prod_revenue DESC LIMIT 10
            """, base_params) # Simplified query relying on purchase record details
            results = c.fetchall()
            msg = f"🏆 Populiariausi produktai: {period_title}\n\n"
            if results:
                for i, row in enumerate(results):
                    type_name = row['product_type'] or 'N/A'
                    emoji = PRODUCT_TYPES.get(type_name, DEFAULT_PRODUCT_EMOJI)
                    msg += f"{i+1}. {emoji} {row['product_name'] or 'N/A'} ({row['product_size'] or 'N/A'}): {format_currency(row['prod_revenue'])} EUR ({row['prod_units'] or 0} vnt.)\n"
            else: msg += "Nėra pardavimų duomenų šiam laikotarpiui."
        else: msg = "❌ Unknown report type requested."
    except sqlite3.Error as e:
        logger.error(f"DB error generating sales report '{report_type}' for '{period_key}': {e}", exc_info=True)
        msg = "❌ Error generating report due to database issue."
    except Exception as e:
        logger.error(f"Unexpected error generating sales report: {e}", exc_info=True)
        msg = "❌ An unexpected error occurred."
    finally:
         if conn: conn.close()
    keyboard = [[InlineKeyboardButton("⬅️ Atgal į laikotarpį", callback_data=f"sales_select_period|{report_type}"),
                 InlineKeyboardButton("📊 Analizės meniu", callback_data="sales_analytics_menu")]]
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Error editing sales report: {e}")
        else: await query.answer()

# --- Add Product Flow Handlers ---
async def handle_adm_city(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin selects city to add product to."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    lang, lang_data = _get_lang_data(context) # Use helper
    if not CITIES:
        return await query.edit_message_text("Nėra sukonfigūruotų miestų. Pirmiausia pridėkite miestą per 'Valdyti miestus'.", parse_mode=None)
    sorted_city_ids = sorted(CITIES.keys(), key=lambda city_id: CITIES.get(city_id, ''))
    keyboard = [[InlineKeyboardButton(f"🏙️ {CITIES.get(c,'N/A')}", callback_data=f"adm_dist|{c}")] for c in sorted_city_ids]
    keyboard.append([InlineKeyboardButton("⬅️ Atgal", callback_data="admin_menu")])
    select_city_text = lang_data.get("admin_select_city", "Select City to Add Product:")
    await query.edit_message_text(select_city_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_dist(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin selects district within the chosen city."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Klaida: Trūksta miesto ID.", show_alert=True)
    city_id = params[0]
    city_name = CITIES.get(city_id)
    if not city_name:
        return await query.edit_message_text("Klaida: Miestas nerastas. Pasirinkite dar kartą.", parse_mode=None)
    districts_in_city = DISTRICTS.get(city_id, {})
    lang, lang_data = _get_lang_data(context) # Use helper
    select_district_template = lang_data.get("admin_select_district", "Select District in {city}:")
    if not districts_in_city:
        keyboard = [[InlineKeyboardButton("⬅️ Atgal į miestus", callback_data="adm_city")]]
        return await query.edit_message_text(f"Rajonų nerasta mieste {city_name}. Pridėkite rajonus per 'Valdyti rajonus'.",
                                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    sorted_district_ids = sorted(districts_in_city.keys(), key=lambda dist_id: districts_in_city.get(dist_id,''))
    keyboard = []
    for d in sorted_district_ids:
        dist_name = districts_in_city.get(d)
        if dist_name:
            keyboard.append([InlineKeyboardButton(f"🏘️ {dist_name}", callback_data=f"adm_type|{city_id}|{d}")])
        else: logger.warning(f"District name missing for ID {d} in city {city_id}")
    keyboard.append([InlineKeyboardButton("⬅️ Atgal į miestus", callback_data="adm_city")])
    select_district_text = select_district_template.format(city=city_name)
    await query.edit_message_text(select_district_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin selects product type."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params or len(params) < 2: return await query.answer("Klaida: Trūksta miesto arba rajono ID.", show_alert=True)
    city_id, dist_id = params[0], params[1]
    city_name = CITIES.get(city_id)
    district_name = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city_name or not district_name:
        return await query.edit_message_text("Klaida: Miestas/Rajonas nerastas. Pasirinkite dar kartą.", parse_mode=None)
    lang, lang_data = _get_lang_data(context) # Use helper
    select_type_text = lang_data.get("admin_select_type", "Select Product Type:")
    if not PRODUCT_TYPES:
        return await query.edit_message_text("No product types configured. Add types via 'Manage Product Types'.", parse_mode=None)

    keyboard = []
    for type_name, emoji in sorted(PRODUCT_TYPES.items()):
        keyboard.append([InlineKeyboardButton(f"{emoji} {type_name}", callback_data=f"adm_add|{city_id}|{dist_id}|{type_name}")])

    keyboard.append([InlineKeyboardButton("⬅️ Back to Districts", callback_data=f"adm_dist|{city_id}")])
    await query.edit_message_text(select_type_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def handle_adm_add(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin selects size for the new product."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params or len(params) < 3: return await query.answer("Klaida: Trūksta vietos/tipo informacijos.", show_alert=True)
    city_id, dist_id, p_type = params
    city_name = CITIES.get(city_id)
    district_name = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city_name or not district_name:
        return await query.edit_message_text("Klaida: Miestas/Rajonas nerastas. Pasirinkite dar kartą.", parse_mode=None)
    type_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    context.user_data["admin_city_id"] = city_id
    context.user_data["admin_district_id"] = dist_id
    context.user_data["admin_product_type"] = p_type
    context.user_data["admin_city"] = city_name
    context.user_data["admin_district"] = district_name
    keyboard = [[InlineKeyboardButton(f"📏 {s}", callback_data=f"adm_size|{s}")] for s in SIZES]
    keyboard.append([InlineKeyboardButton("📏 Kitas dydis", callback_data="adm_custom_size")])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Types", callback_data=f"adm_type|{city_id}|{dist_id}")])
    await query.edit_message_text(f"📦 Adding {type_emoji} {p_type} in {city_name} / {district_name}\n\nPasirinkite dydį:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_size(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles selection of a predefined size."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Klaida: Trūksta dydžio.", show_alert=True)
    size = params[0]
    if not all(k in context.user_data for k in ["admin_city", "admin_district", "admin_product_type"]):
        return await query.edit_message_text("❌ Klaida: Kontekstas prarastas. Pradėkite iš naujo.", parse_mode=None)
    context.user_data["pending_drop_size"] = size
    context.user_data["state"] = "awaiting_price"
    keyboard = [[InlineKeyboardButton("❌ Cancel Add", callback_data="cancel_add")]]
    await query.edit_message_text(f"Dydis nustatytas {size}. Atsakykite su kaina (pvz., 12.50 arba 12.5):",
                            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Įveskite kainą pokalbyje.")

async def handle_adm_custom_size(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Custom Size' button press."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not all(k in context.user_data for k in ["admin_city", "admin_district", "admin_product_type"]):
        return await query.edit_message_text("❌ Klaida: Kontekstas prarastas. Pradėkite iš naujo.", parse_mode=None)
    context.user_data["state"] = "awaiting_custom_size"
    keyboard = [[InlineKeyboardButton("❌ Cancel Add", callback_data="cancel_add")]]
    await query.edit_message_text("Atsakykite su dydžiu (pvz., 10g, 1/4 oz):",
                            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Įveskite dydį pokalbyje.")

async def handle_confirm_add_drop(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles confirmation (Yes/No) for adding the drop."""
    query = update.callback_query
    user_id = query.from_user.id
    if not is_primary_admin(user_id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    chat_id = query.message.chat_id
    user_specific_data = context.user_data # Use context.user_data for the admin's data
    pending_drop = user_specific_data.get("pending_drop")

    if not pending_drop:
        logger.error(f"Confirmation 'yes' received for add drop, but no pending_drop data found for user {user_id}.")
        user_specific_data.pop("state", None)
        return await query.edit_message_text("❌ Klaida: Nerasta laukiamų duomenų. Pradėkite iš naujo.", parse_mode=None)

    city = pending_drop.get("city"); district = pending_drop.get("district"); p_type = pending_drop.get("product_type")
    size = pending_drop.get("size"); price = pending_drop.get("price"); original_text = pending_drop.get("original_text", "")
    media_list = pending_drop.get("media", []); temp_dir = pending_drop.get("temp_dir")

    if not all([city, district, p_type, size, price is not None]):
        logger.error(f"Missing data in pending_drop for user {user_id}: {pending_drop}")
        if temp_dir and await asyncio.to_thread(os.path.exists, temp_dir): await asyncio.to_thread(shutil.rmtree, temp_dir, ignore_errors=True)
        keys_to_clear = ["state", "pending_drop", "pending_drop_size", "pending_drop_price", "admin_city_id", "admin_district_id", "admin_product_type", "admin_city", "admin_district"]
        for key in keys_to_clear: user_specific_data.pop(key, None)
        return await query.edit_message_text("❌ Klaida: Nepilni duomenys. Pradėkite iš naujo.", parse_mode=None)

    product_name = f"{p_type} {size} {int(time.time())}"; conn = None; product_id = None
    try:
        conn = get_db_connection(); c = conn.cursor(); c.execute("BEGIN")
        insert_params = (
            city, district, p_type, size, product_name, price, original_text, ADMIN_ID, datetime.now(timezone.utc).isoformat()
        )
        logger.debug(f"Inserting product with params count: {len(insert_params)}") # Add debug log
        c.execute("""INSERT INTO products
                        (city, district, product_type, size, name, price, available, reserved, original_text, added_by, added_date)
                     VALUES (?, ?, ?, ?, ?, ?, 1, 0, ?, ?, ?)""", insert_params)
        product_id = c.lastrowid

        if product_id and media_list and temp_dir:
            final_media_dir = os.path.join(MEDIA_DIR, str(product_id))
            await asyncio.to_thread(os.makedirs, final_media_dir, exist_ok=True)
            media_inserts = []
            
            for media_item in media_list:
                if "path" in media_item and "type" in media_item and "file_id" in media_item:
                    temp_file_path = media_item["path"]
                    if await asyncio.to_thread(os.path.exists, temp_file_path):
                        # Generate unique filename to prevent conflicts
                        base_filename = os.path.basename(temp_file_path)
                        name, ext = os.path.splitext(base_filename)
                        counter = 1
                        final_persistent_path = os.path.join(final_media_dir, f"{name}_{counter}{ext}")
                        
                        # Ensure unique filename
                        while await asyncio.to_thread(os.path.exists, final_persistent_path):
                            counter += 1
                            final_persistent_path = os.path.join(final_media_dir, f"{name}_{counter}{ext}")
                        
                        try:
                            await asyncio.to_thread(shutil.copy2, temp_file_path, final_persistent_path)
                            media_inserts.append((product_id, media_item["type"], final_persistent_path, media_item["file_id"]))
                        except OSError as move_err:
                            logger.error(f"Error copying media {temp_file_path}: {move_err}")
                    else:
                        logger.warning(f"Temp media not found: {temp_file_path}")
                else:
                    logger.warning(f"Incomplete media item: {media_item}")
            
            # Insert all media records at once (outside the loop)
            if media_inserts:
                c.executemany("INSERT INTO product_media (product_id, media_type, file_path, telegram_file_id) VALUES (?, ?, ?, ?)", media_inserts)
                logger.info(f"Successfully inserted {len(media_inserts)} media records for bulk product {product_id}")
            else:
                logger.warning(f"No media was inserted for product {product_id}. Media list: {media_list}, Temp dir: {temp_dir}")

        conn.commit(); logger.info(f"Added product {product_id} ({product_name}).")
        if temp_dir and await asyncio.to_thread(os.path.exists, temp_dir): await asyncio.to_thread(shutil.rmtree, temp_dir, ignore_errors=True); logger.info(f"Cleaned temp dir: {temp_dir}")
        await query.edit_message_text("✅ Produktas sėkmingai pridėtas!", parse_mode=None)
        ctx_city_id = user_specific_data.get('admin_city_id'); ctx_dist_id = user_specific_data.get('admin_district_id'); ctx_p_type = user_specific_data.get('admin_product_type')
        add_another_callback = f"adm_add|{ctx_city_id}|{ctx_dist_id}|{ctx_p_type}" if all([ctx_city_id, ctx_dist_id, ctx_p_type]) else "admin_menu"
        keyboard = [ [InlineKeyboardButton("➕ Add Another Same Type", callback_data=add_another_callback)],
                     [InlineKeyboardButton("🔧 Admin meniu", callback_data="admin_menu"), InlineKeyboardButton("🏠 Pagrindinis", callback_data="back_start")] ]
        await send_message_with_retry(context.bot, chat_id, "What next?", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except (sqlite3.Error, OSError, Exception) as e:
        try: conn.rollback() if conn and conn.in_transaction else None
        except Exception as rb_err: logger.error(f"Rollback failed: {rb_err}")
        logger.error(f"Error saving confirmed drop for user {user_id}: {e}", exc_info=True)
        await query.edit_message_text("❌ Klaida: Nepavyko išsaugoti produkto. Patikrinkite žurnalus ir bandykite dar kartą.", parse_mode=None)
        if temp_dir and await asyncio.to_thread(os.path.exists, temp_dir): await asyncio.to_thread(shutil.rmtree, temp_dir, ignore_errors=True); logger.info(f"Cleaned temp dir after error: {temp_dir}")
    finally:
        if conn: conn.close()
        keys_to_clear = ["state", "pending_drop", "pending_drop_size", "pending_drop_price"]
        for key in keys_to_clear: user_specific_data.pop(key, None)


async def cancel_add(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Cancels the add product flow and cleans up."""
    query = update.callback_query
    user_id = update.effective_user.id
    user_specific_data = context.user_data # Use context.user_data
    pending_drop = user_specific_data.get("pending_drop")
    if pending_drop and "temp_dir" in pending_drop and pending_drop["temp_dir"]:
        temp_dir_path = pending_drop["temp_dir"]
        if await asyncio.to_thread(os.path.exists, temp_dir_path):
            try: await asyncio.to_thread(shutil.rmtree, temp_dir_path, ignore_errors=True); logger.info(f"Cleaned temp dir on cancel: {temp_dir_path}")
            except Exception as e: logger.error(f"Error cleaning temp dir {temp_dir_path}: {e}")
    keys_to_clear = ["state", "pending_drop", "pending_drop_size", "pending_drop_price", "admin_city_id", "admin_district_id", "admin_product_type", "admin_city", "admin_district", "collecting_media_group_id", "collected_media"]
    for key in keys_to_clear: user_specific_data.pop(key, None)
    if 'collecting_media_group_id' in user_specific_data:
        media_group_id = user_specific_data.pop('collecting_media_group_id', None)
        if media_group_id: job_name = f"process_media_group_{user_id}_{media_group_id}"; remove_job_if_exists(job_name, context)
    if query:
         try:
             await query.edit_message_text("❌ Add Product Cancelled", parse_mode=None)
         except telegram_error.BadRequest as e:
             if "message is not modified" in str(e).lower():
                 pass # It's okay if the message wasn't modified
             else:
                 logger.error(f"Error editing cancel message: {e}")
         keyboard = [[InlineKeyboardButton("🔧 Admin meniu", callback_data="admin_menu"), InlineKeyboardButton("🏠 Pagrindinis", callback_data="back_start")]]; await send_message_with_retry(context.bot, query.message.chat_id, "Grįžtama į Admin meniu.", reply_markup=InlineKeyboardMarkup(keyboard))
    elif update.message: await send_message_with_retry(context.bot, update.message.chat_id, "Add product cancelled.")
    else: logger.info("Add product flow cancelled internally (no query/message object).")


# --- Bulk Add Products Handlers ---
async def handle_adm_bulk_city(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin selects city to add bulk products to."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    lang, lang_data = _get_lang_data(context) # Use helper
    if not CITIES:
        return await query.edit_message_text("Nėra sukonfigūruotų miestų. Pirmiausia pridėkite miestą per 'Valdyti miestus'.", parse_mode=None)
    sorted_city_ids = sorted(CITIES.keys(), key=lambda city_id: CITIES.get(city_id, ''))
    keyboard = [[InlineKeyboardButton(f"🏙️ {CITIES.get(c,'N/A')}", callback_data=f"adm_bulk_dist|{c}")] for c in sorted_city_ids]
    keyboard.append([InlineKeyboardButton("⬅️ Atgal", callback_data="admin_menu")])
    select_city_text = lang_data.get("admin_select_city", "Select City to Add Bulk Products:")
    await query.edit_message_text(f"📦 Masinis produktų pridėjimas\n\n{select_city_text}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_bulk_dist(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin selects district for bulk products."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Klaida: Trūksta miesto ID.", show_alert=True)
    city_id = params[0]
    city_name = CITIES.get(city_id)
    if not city_name:
        return await query.edit_message_text("Klaida: Miestas nerastas. Pasirinkite dar kartą.", parse_mode=None)
    districts_in_city = DISTRICTS.get(city_id, {})
    lang, lang_data = _get_lang_data(context) # Use helper
    select_district_template = lang_data.get("admin_select_district", "Select District in {city}:")
    if not districts_in_city:
        keyboard = [[InlineKeyboardButton("⬅️ Atgal į miestus", callback_data="adm_bulk_city")]]
        return await query.edit_message_text(f"Rajonų nerasta mieste {city_name}. Pridėkite rajonus per 'Valdyti rajonus'.",
                                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    sorted_district_ids = sorted(districts_in_city.keys(), key=lambda d_id: districts_in_city.get(d_id,''))
    keyboard = []
    for d in sorted_district_ids:
        dist_name = districts_in_city.get(d)
        if dist_name:
            keyboard.append([InlineKeyboardButton(f"🏘️ {dist_name}", callback_data=f"adm_bulk_type|{city_id}|{d}")])
        else: logger.warning(f"District name missing for ID {d} in city {city_id}")
    keyboard.append([InlineKeyboardButton("⬅️ Atgal į miestus", callback_data="adm_bulk_city")])
    select_district_text = select_district_template.format(city=city_name)
    await query.edit_message_text(select_district_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_bulk_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin selects product type for bulk products."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params or len(params) < 2: return await query.answer("Klaida: Trūksta miesto arba rajono ID.", show_alert=True)
    city_id, dist_id = params[0], params[1]
    city_name = CITIES.get(city_id)
    district_name = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city_name or not district_name:
        return await query.edit_message_text("Klaida: Miestas/Rajonas nerastas. Pasirinkite dar kartą.", parse_mode=None)
    lang, lang_data = _get_lang_data(context) # Use helper
    select_type_text = lang_data.get("admin_select_type", "Select Product Type:")
    if not PRODUCT_TYPES:
        return await query.edit_message_text("No product types configured. Add types via 'Manage Product Types'.", parse_mode=None)

    keyboard = []
    for type_name, emoji in sorted(PRODUCT_TYPES.items()):
        keyboard.append([InlineKeyboardButton(f"{emoji} {type_name}", callback_data=f"adm_bulk_add|{city_id}|{dist_id}|{type_name}")])

    keyboard.append([InlineKeyboardButton("⬅️ Back to Districts", callback_data=f"adm_bulk_dist|{city_id}")])
    await query.edit_message_text(f"📦 Masinis produktų pridėjimas - {city_name} / {district_name}\n\n{select_type_text}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_bulk_add(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin selects size for the bulk products."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params or len(params) < 3: return await query.answer("Klaida: Trūksta vietos/tipo informacijos.", show_alert=True)
    city_id, dist_id, p_type = params
    city_name = CITIES.get(city_id)
    district_name = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city_name or not district_name:
        return await query.edit_message_text("Klaida: Miestas/Rajonas nerastas. Pasirinkite dar kartą.", parse_mode=None)
    type_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    
    # Store initial bulk product details
    context.user_data["bulk_admin_city_id"] = city_id
    context.user_data["bulk_admin_district_id"] = dist_id
    context.user_data["bulk_admin_product_type"] = p_type
    context.user_data["bulk_admin_city"] = city_name
    context.user_data["bulk_admin_district"] = district_name
    
    keyboard = [[InlineKeyboardButton(f"📏 {s}", callback_data=f"adm_bulk_size|{s}")] for s in SIZES]
    keyboard.append([InlineKeyboardButton("📏 Kitas dydis", callback_data="adm_bulk_custom_size")])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Types", callback_data=f"adm_bulk_type|{city_id}|{dist_id}")])
    await query.edit_message_text(f"📦 Masinis pridėjimas {type_emoji} {p_type} in {city_name} / {district_name}\n\nPasirinkite dydį:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_bulk_size(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles selection of a predefined size for bulk products."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Klaida: Trūksta dydžio.", show_alert=True)
    size = params[0]
    if not all(k in context.user_data for k in ["bulk_admin_city", "bulk_admin_district", "bulk_admin_product_type"]):
        return await query.edit_message_text("❌ Klaida: Kontekstas prarastas. Pradėkite masinį pridėjimą iš naujo.", parse_mode=None)
    context.user_data["bulk_pending_drop_size"] = size
    context.user_data["state"] = "awaiting_bulk_price"
    keyboard = [[InlineKeyboardButton("❌ Atšaukti masinį pridėjimą", callback_data="cancel_bulk_add")]]
    await query.edit_message_text(f"📦 Masiniai produktai - Dydis nustatytas {size}. Atsakykite su kaina (pvz., 12.50 arba 12.5):",
                            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Įveskite kainą pokalbyje.")

async def handle_adm_bulk_custom_size(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Custom Size' button press for bulk products."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not all(k in context.user_data for k in ["bulk_admin_city", "bulk_admin_district", "bulk_admin_product_type"]):
        return await query.edit_message_text("❌ Klaida: Kontekstas prarastas. Pradėkite masinį pridėjimą iš naujo.", parse_mode=None)
    context.user_data["state"] = "awaiting_bulk_custom_size"
    keyboard = [[InlineKeyboardButton("❌ Atšaukti masinį pridėjimą", callback_data="cancel_bulk_add")]]
    await query.edit_message_text("📦 Masiniai produktai - Atsakykite su dydžiu (pvz., 10g, 1/4 oz):",
                            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Įveskite dydį pokalbyje.")

async def handle_adm_bulk_custom_size_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the custom size reply for bulk products."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(user_id): return
    if not update.message or not update.message.text: return
    if context.user_data.get("state") != "awaiting_bulk_custom_size": return

    size = update.message.text.strip()
    if not size: return await send_message_with_retry(context.bot, chat_id, "Size cannot be empty.", parse_mode=None)
    if len(size) > 50: return await send_message_with_retry(context.bot, chat_id, "Size too long (max 50 chars).", parse_mode=None)

    context.user_data["bulk_pending_drop_size"] = size
    context.user_data["state"] = "awaiting_bulk_price"
    keyboard = [[InlineKeyboardButton("❌ Atšaukti masinį pridėjimą", callback_data="cancel_bulk_add")]]
    await send_message_with_retry(context.bot, chat_id, f"📦 Masiniai produktai - Dydis nustatytas: {size}\n\nAtsakykite su kaina (pvz., 12.50 arba 12.5):",
                            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_bulk_price_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the price reply for bulk products."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(user_id): return
    if not update.message or not update.message.text: return
    if context.user_data.get("state") != "awaiting_bulk_price": return

    price_text = update.message.text.strip()
    try: price = float(price_text)
    except ValueError: return await send_message_with_retry(context.bot, chat_id, "❌ Neteisingas kainos formatas. Įveskite skaičių (pvz., 12.50).", parse_mode=None)
    if price <= 0: return await send_message_with_retry(context.bot, chat_id, "❌ Price must be greater than 0.", parse_mode=None)
    if price > 999999: return await send_message_with_retry(context.bot, chat_id, "❌ Price too high (max 999999).", parse_mode=None)

    context.user_data["bulk_pending_drop_price"] = price
    context.user_data["state"] = "awaiting_bulk_messages"
    
    # Initialize bulk messages collection
    context.user_data["bulk_messages"] = []
    
    price_str = format_currency(price)
    size = context.user_data.get("bulk_pending_drop_size", "")
    p_type = context.user_data.get("bulk_admin_product_type", "")
    city = context.user_data.get("bulk_admin_city", "")
    district = context.user_data.get("bulk_admin_district", "")
    type_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    
    msg = (f"📦 Masinių produktų nustatymas baigtas\n\n"
           f"📍 Location: {city} / {district}\n"
           f"{type_emoji} Type: {p_type}\n"
           f"📏 Size: {size}\n"
           f"💰 Price: {price_str}€\n\n"
           f"Now forward or send up to 10 different messages. Each message can contain:\n"
           f"• Photos, videos, GIFs\n"
           f"• Text descriptions\n"
           f"• Any combination of media and text\n\n"
           f"Each message will become a separate product drop in this category.\n\n"
           f"Surinkta žinučių: 0/10")
    
    keyboard = [
        [InlineKeyboardButton("✅ Finish & Create Products", callback_data="adm_bulk_create_all")],
        [InlineKeyboardButton("❌ Atšaukti masinę operaciją", callback_data="cancel_bulk_add")]
    ]
    
    await send_message_with_retry(context.bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_bulk_drop_details_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles collecting multiple different messages for bulk products."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(user_id): return
    if not update.message: return
    if context.user_data.get("state") != "awaiting_bulk_messages": return

    bulk_messages = context.user_data.get("bulk_messages", [])
    
    # Check if we've reached the limit
    if len(bulk_messages) >= 10:
        await send_message_with_retry(context.bot, chat_id, 
            "❌ Jau surinkote 10 žinučių (maksimumas). Užbaikite produktų kūrimą arba atšaukite operaciją.", 
            parse_mode=None)
        return

    media_group_id = update.message.media_group_id
    job_name = f"process_bulk_media_group_{user_id}_{media_group_id}" if media_group_id else None

    media_type, file_id = None, None
    if update.message.photo: media_type, file_id = "photo", update.message.photo[-1].file_id
    elif update.message.video: media_type, file_id = "video", update.message.video.file_id
    elif update.message.animation: media_type, file_id = "gif", update.message.animation.file_id

    text = (update.message.caption or update.message.text or "").strip()

    # Debug logging
    logger.info(f"BULK DEBUG: User {user_id} sent message. Media Group ID: {media_group_id}, Media Type: {media_type}, Text: '{text[:50]}...', Current bulk messages count: {len(bulk_messages)}")

    if media_group_id:
        logger.info(f"BULK DEBUG: Processing media group {media_group_id} from user {user_id}")
        if 'bulk_collected_media' not in context.user_data:
            context.user_data['bulk_collected_media'] = {}

        if media_group_id not in context.user_data['bulk_collected_media']:
            context.user_data['bulk_collected_media'][media_group_id] = {'media': [], 'caption': None}
            logger.info(f"BULK DEBUG: Started collecting bulk media for group {media_group_id} user {user_id}")
            context.user_data['bulk_collecting_media_group_id'] = media_group_id

        if media_type and file_id:
            if not any(m['file_id'] == file_id for m in context.user_data['bulk_collected_media'][media_group_id]['media']):
                context.user_data['bulk_collected_media'][media_group_id]['media'].append(
                    {'type': media_type, 'file_id': file_id}
                )
                logger.info(f"BULK DEBUG: Added bulk media {file_id} ({media_type}) to group {media_group_id}. Group now has {len(context.user_data['bulk_collected_media'][media_group_id]['media'])} media items")

        if text:
            context.user_data['bulk_collected_media'][media_group_id]['caption'] = text
            logger.info(f"BULK DEBUG: Stored/updated bulk caption for group {media_group_id}: '{text[:50]}...'")

        remove_job_if_exists(job_name, context)
        if hasattr(context, 'job_queue') and context.job_queue:
            try:
                context.job_queue.run_once(
                    _process_bulk_collected_media,
                    when=timedelta(seconds=MEDIA_GROUP_COLLECTION_DELAY),
                    data={'media_group_id': media_group_id, 'chat_id': chat_id, 'user_id': user_id},
                    name=job_name,
                    job_kwargs={'misfire_grace_time': 30}  # Increased grace time from 15 to 30 seconds
                )
                logger.info(f"BULK DEBUG: Scheduled bulk job {job_name} for media group {media_group_id} to run in {MEDIA_GROUP_COLLECTION_DELAY} seconds")
            except Exception as job_error:
                logger.error(f"BULK DEBUG: Failed to schedule bulk media group job {job_name}: {job_error}")
                # Fallback: Process immediately if job scheduling fails
                collected_media = context.user_data['bulk_collected_media'][media_group_id]['media']
                message_data = {
                    "text": text,
                    "media": collected_media,
                    "timestamp": int(time.time())
                }
                bulk_messages = context.user_data.get("bulk_messages", [])
                bulk_messages.append(message_data)
                context.user_data["bulk_messages"] = bulk_messages
                await send_message_with_retry(context.bot, chat_id, 
                    f"✅ Media group added to bulk collection! Total messages: {len(bulk_messages)}/10", 
                    parse_mode=None)
        else:
            logger.error("JobQueue not found in context. Cannot schedule bulk media group processing.")
            # Fallback: Process immediately if no job queue
            if media_group_id in context.user_data.get('bulk_collected_media', {}):
                collected_media = context.user_data['bulk_collected_media'][media_group_id]['media']
                message_data = {
                    "text": text,
                    "media": collected_media,
                    "timestamp": int(time.time())
                }
                bulk_messages = context.user_data.get("bulk_messages", [])
                bulk_messages.append(message_data)
                context.user_data["bulk_messages"] = bulk_messages
                await send_message_with_retry(context.bot, chat_id, 
                    f"✅ Media group added to bulk collection! Total messages: {len(bulk_messages)}/10", 
                    parse_mode=None)
            else:
                await send_message_with_retry(context.bot, chat_id, "❌ Error: Internal components missing. Cannot process media group.", parse_mode=None)

    else:
        if context.user_data.get('bulk_collecting_media_group_id'):
            logger.warning(f"BULK DEBUG: Received single bulk message from user {user_id} while potentially collecting media group {context.user_data['bulk_collecting_media_group_id']}. Ignoring for bulk.")
            return

        logger.info(f"BULK DEBUG: Received single bulk message (or text only) from user {user_id}. Adding as individual message.")
        context.user_data.pop('bulk_collecting_media_group_id', None)
        context.user_data.pop('bulk_collected_media', None)

        # Extract message content
        message_data = {
            "text": text,
            "media": [],
            "timestamp": int(time.time())
        }

        # Get media content for single message
        if media_type and file_id:
            message_data["media"].append({"type": media_type, "file_id": file_id})

        # Store the message
        bulk_messages.append(message_data)
        context.user_data["bulk_messages"] = bulk_messages
        
        logger.info(f"BULK DEBUG: Added single message to bulk_messages. New count: {len(bulk_messages)}")
        
        # Show updated status
        await show_bulk_messages_status(update, context)

async def show_bulk_messages_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows the current status of collected bulk messages."""
    chat_id = update.effective_chat.id if update.effective_chat else update.message.chat_id
    
    bulk_messages = context.user_data.get("bulk_messages", [])
    price = context.user_data.get("bulk_pending_drop_price", 0)
    size = context.user_data.get("bulk_pending_drop_size", "")
    p_type = context.user_data.get("bulk_admin_product_type", "")
    city = context.user_data.get("bulk_admin_city", "")
    district = context.user_data.get("bulk_admin_district", "")
    type_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    price_str = format_currency(price)
    
    msg = (f"📦 Masinių produktų rinkimas\n\n"
           f"📍 Location: {city} / {district}\n"
           f"{type_emoji} Type: {p_type}\n"
           f"📏 Size: {size}\n"
           f"💰 Price: {price_str}€\n\n"
           f"Surinkta žinučių: {len(bulk_messages)}/10\n\n")
    
    if not bulk_messages:
        msg += "Dar nėra surinktų žinučių. Siųskite arba persiųskite pirmą žinutę su produkto duomenimis ir medija."
    else:
        msg += "Collected messages:\n"
        for i, msg_data in enumerate(bulk_messages, 1):
            text_preview = msg_data.get("text", "")[:50]
            if len(text_preview) > 50:
                text_preview += "..."
            if not text_preview:
                text_preview = "(No text)"
            
            media_count = len(msg_data.get("media", []))
            media_info = f" + {media_count} media" if media_count > 0 else ""
            
            msg += f"{i}. {text_preview}{media_info}\n"
    
    msg += f"\n{10 - len(bulk_messages)} more messages can be added."
    
    keyboard = []
    
    if bulk_messages:
        keyboard.append([InlineKeyboardButton("🗑️ Pašalinti paskutinę žinutę", callback_data="adm_bulk_remove_last_message")])
        keyboard.append([InlineKeyboardButton("✅ Create All Products", callback_data="adm_bulk_create_all")])
    
    if len(bulk_messages) < 10:
        msg += "\n\nSend or forward your next message..."
    
    keyboard.append([InlineKeyboardButton("❌ Atšaukti masinę operaciją", callback_data="cancel_bulk_add")])
    
    await send_message_with_retry(context.bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_bulk_remove_last_message(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Removes the last collected message from bulk operation."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    bulk_messages = context.user_data.get("bulk_messages", [])
    if not bulk_messages:
        return await query.answer("No messages to remove!", show_alert=True)
    
    removed_message = bulk_messages.pop()
    context.user_data["bulk_messages"] = bulk_messages
    
    # Get some info about the removed message for feedback
    text_preview = removed_message.get("text", "")[:30]
    if len(text_preview) > 30:
        text_preview += "..."
    if not text_preview:
        text_preview = "(media only)"
    
    await query.answer(f"Removed: {text_preview}")
    await show_bulk_messages_status(update, context)

async def handle_adm_bulk_back_to_management(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Returns to bulk management interface."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    # This function is no longer needed since we switched to message-based bulk instead of location-based
    # Redirect to the message collection status
    context.user_data["state"] = "awaiting_bulk_messages"
    await show_bulk_messages_status(update, context)

async def handle_adm_bulk_confirm_all(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirms and creates all products from the collected messages."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    bulk_messages = context.user_data.get("bulk_messages", [])
    if not bulk_messages:
        return await query.answer("Nėra surinktų žinučių! Pirmiausia pridėkite žinučių.", show_alert=True)
    
    # Get all the setup data
    city = context.user_data.get("bulk_admin_city", "")
    district = context.user_data.get("bulk_admin_district", "")
    p_type = context.user_data.get("bulk_admin_product_type", "")
    size = context.user_data.get("bulk_pending_drop_size", "")
    price = context.user_data.get("bulk_pending_drop_price", 0)
    
    if not all([city, district, p_type, size, price]):
        return await query.edit_message_text("❌ Klaida: Trūksta nustatymų duomenų. Pradėkite iš naujo.", parse_mode=None)
    
    # Show confirmation
    type_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    price_str = format_currency(price)
    
    msg = f"⚠️ Confirm Bulk Creation\n\n"
    msg += f"You are about to create {len(bulk_messages)} products:\n\n"
    msg += f"📍 Location: {city} / {district}\n"
    msg += f"{type_emoji} Type: {p_type}\n"
    msg += f"📏 Size: {size}\n"
    msg += f"💰 Price: {price_str}€\n\n"
    msg += f"Products to create:\n"
    for i, msg_data in enumerate(bulk_messages, 1):
        text_preview = msg_data.get("text", "")[:40]
        if len(text_preview) > 40:
            text_preview += "..."
        if not text_preview:
            text_preview = "(media only)"
        
        media_count = len(msg_data.get("media", []))
        media_info = f" + {media_count} media" if media_count > 0 else ""
        
        msg += f"{i}. {text_preview}{media_info}\n"
    
    msg += f"\nProceed with creation?"
    
    keyboard = [
        [InlineKeyboardButton("✅ Yes, Create All Products", callback_data="adm_bulk_execute_messages")],
        [InlineKeyboardButton("❌ No, Go Back", callback_data="adm_bulk_back_to_messages")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_bulk_execute(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Executes the bulk product creation."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    chat_id = query.message.chat_id
    bulk_template = context.user_data.get("bulk_template", {})
    bulk_drops = context.user_data.get("bulk_drops", [])
    
    if not bulk_drops or not bulk_template:
        return await query.edit_message_text("❌ Klaida: Trūksta masinių duomenų. Pradėkite iš naujo.", parse_mode=None)
    
    await query.edit_message_text("⏳ Creating bulk products...", parse_mode=None)
    
    p_type = bulk_template.get("product_type", "")
    size = bulk_template.get("size", "")
    price = bulk_template.get("price", 0)
    original_text = bulk_template.get("original_text", "")
    media_list = bulk_template.get("media", [])
    
    created_count = 0
    failed_count = 0
    
    # Create a temporary directory for media if needed
    temp_dir = None
    if media_list:
        import tempfile
        temp_dir = await asyncio.to_thread(tempfile.mkdtemp, prefix="bulk_media_")
        
        # Download media to temp directory
        for i, media_item in enumerate(media_list):
            try:
                file_obj = await context.bot.get_file(media_item["file_id"])
                file_extension = os.path.splitext(file_obj.file_path)[1] if file_obj.file_path else ""
                if not file_extension:
                    if media_item["type"] == "photo": file_extension = ".jpg"
                    elif media_item["type"] == "video": file_extension = ".mp4"
                    elif media_item["type"] == "animation": file_extension = ".gif"
                    else: file_extension = ".bin"
                
                temp_file_path = os.path.join(temp_dir, f"media_{i}_{int(time.time())}{file_extension}")
                await file_obj.download_to_drive(temp_file_path)
                media_item["path"] = temp_file_path
            except Exception as e:
                logger.error(f"Error downloading media for bulk operation: {e}")
                failed_count += 1
    
    # Create products for each location
    for drop in bulk_drops:
        city = drop["city"]
        district = drop["district"]
        product_name = f"{p_type} {size} {int(time.time())}"
        
        conn = None
        product_id = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("BEGIN")
            
            insert_params = (
                city, district, p_type, size, product_name, price, original_text, ADMIN_ID, datetime.now(timezone.utc).isoformat()
            )
            
            c.execute("""INSERT INTO products
                            (city, district, product_type, size, name, price, available, reserved, original_text, added_by, added_date)
                         VALUES (?, ?, ?, ?, ?, ?, 1, 0, ?, ?, ?)""", insert_params)
            product_id = c.lastrowid
            
            # Handle media for this product
            if product_id and media_list and temp_dir:
                final_media_dir = os.path.join(MEDIA_DIR, str(product_id))
                await asyncio.to_thread(os.makedirs, final_media_dir, exist_ok=True)
                media_inserts = []
                
                for media_item in media_list:
                    if "path" in media_item and "type" in media_item and "file_id" in media_item:
                        temp_file_path = media_item["path"]
                        if await asyncio.to_thread(os.path.exists, temp_file_path):
                            # Generate unique filename to prevent conflicts
                            base_filename = os.path.basename(temp_file_path)
                            name, ext = os.path.splitext(base_filename)
                            counter = 1
                            final_persistent_path = os.path.join(final_media_dir, f"{name}_{counter}{ext}")
                            
                            # Ensure unique filename
                            while await asyncio.to_thread(os.path.exists, final_persistent_path):
                                counter += 1
                                final_persistent_path = os.path.join(final_media_dir, f"{name}_{counter}{ext}")
                            
                            try:
                                await asyncio.to_thread(shutil.copy2, temp_file_path, final_persistent_path)
                                media_inserts.append((product_id, media_item["type"], final_persistent_path, media_item["file_id"]))
                            except OSError as move_err:
                                logger.error(f"Error copying media {temp_file_path}: {move_err}")
                        else:
                            logger.warning(f"Temp media not found: {temp_file_path}")
                    else:
                        logger.warning(f"Incomplete media item: {media_item}")
                
                # Insert all media records at once (outside the loop)
                if media_inserts:
                    c.executemany("INSERT INTO product_media (product_id, media_type, file_path, telegram_file_id) VALUES (?, ?, ?, ?)", media_inserts)
                    logger.info(f"Successfully inserted {len(media_inserts)} media records for bulk product {product_id}")
                else:
                    logger.warning(f"No media was inserted for product {product_id}. Media list: {media_list}, Temp dir: {temp_dir}")
            
            conn.commit()
            created_count += 1
            logger.info(f"Bulk created product {product_id} ({product_name}) in {city}/{district}")
            
        except Exception as e:
            failed_count += 1
            logger.error(f"Error creating bulk product in {city}/{district}: {e}", exc_info=True)
            if conn:
                try:
                    conn.rollback()
                except Exception as rb_err:
                    logger.error(f"Rollback failed: {rb_err}")
        finally:
            if conn:
                conn.close()
    
    # Clean up temp directory
    if temp_dir and await asyncio.to_thread(os.path.exists, temp_dir):
        await asyncio.to_thread(shutil.rmtree, temp_dir, ignore_errors=True)
        logger.info(f"Cleaned bulk temp dir: {temp_dir}")
    
    # Clear bulk data from context
    keys_to_clear = ["bulk_template", "bulk_drops", "bulk_admin_city_id", "bulk_admin_district_id", 
                     "bulk_admin_product_type", "bulk_admin_city", "bulk_admin_district", 
                     "bulk_pending_drop_size", "bulk_pending_drop_price", "state"]
    for key in keys_to_clear:
        context.user_data.pop(key, None)
    
    # Show results
    type_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    result_msg = f"✅ Bulk Operation Complete!\n\n"
    result_msg += f"{type_emoji} Product: {p_type} {size}\n"
    result_msg += f"💰 Price: {format_currency(price)}€\n\n"
    result_msg += f"📊 Results:\n"
    result_msg += f"✅ Created: {created_count}\n"
    if failed_count > 0:
        result_msg += f"❌ Failed: {failed_count}\n"
    
    keyboard = [
        [InlineKeyboardButton("📦 Add More Bulk Products", callback_data="adm_bulk_city")],
        [InlineKeyboardButton("🔧 Admin meniu", callback_data="admin_menu"), 
         InlineKeyboardButton("🏠 Pagrindinis", callback_data="back_start")]
    ]
    
    await send_message_with_retry(context.bot, chat_id, result_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def cancel_bulk_add(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Cancels the bulk add product flow and cleans up."""
    query = update.callback_query
    user_id = update.effective_user.id
    user_specific_data = context.user_data
    
    # Clean up any temp directory if it exists
    bulk_template = user_specific_data.get("bulk_template", {})
    if bulk_template and "media" in bulk_template:
        for media_item in bulk_template["media"]:
            if "path" in media_item:
                temp_file_path = media_item["path"]
                temp_dir = os.path.dirname(temp_file_path)
                if await asyncio.to_thread(os.path.exists, temp_dir):
                    try:
                        await asyncio.to_thread(shutil.rmtree, temp_dir, ignore_errors=True)
                        logger.info(f"Cleaned bulk temp dir on cancel: {temp_dir}")
                        break  # Only need to remove the directory once
                    except Exception as e:
                        logger.error(f"Error cleaning bulk temp dir {temp_dir}: {e}")
    
    # Cancel any scheduled bulk media group jobs
    if 'bulk_collecting_media_group_id' in user_specific_data:
        media_group_id = user_specific_data.get('bulk_collecting_media_group_id')
        if media_group_id:
            job_name = f"process_bulk_media_group_{user_id}_{media_group_id}"
            remove_job_if_exists(job_name, context)
            logger.info(f"Cancelled bulk media group job: {job_name}")
    
    # Clear all bulk-related data
    keys_to_clear = ["state", "bulk_template", "bulk_drops", "bulk_admin_city_id", "bulk_admin_district_id", 
                     "bulk_admin_product_type", "bulk_admin_city", "bulk_admin_district", 
                     "bulk_pending_drop_size", "bulk_pending_drop_price", "bulk_messages", "bulk_processing_groups",
                     "bulk_collected_media", "bulk_collecting_media_group_id"]
    for key in keys_to_clear:
        user_specific_data.pop(key, None)
    
    if query:
        try:
            await query.edit_message_text("❌ Bulk Add Products Cancelled", parse_mode=None)
        except telegram_error.BadRequest as e:
            if "message is not modified" in str(e).lower():
                pass  # It's okay if the message wasn't modified
            else:
                logger.error(f"Error editing cancel bulk message: {e}")
        
        keyboard = [[InlineKeyboardButton("🔧 Admin meniu", callback_data="admin_menu"), 
                     InlineKeyboardButton("🏠 Pagrindinis", callback_data="back_start")]]
        await send_message_with_retry(context.bot, query.message.chat_id, "Grįžtama į Admin meniu.", 
                                      reply_markup=InlineKeyboardMarkup(keyboard))
    elif update.message:
        await send_message_with_retry(context.bot, update.message.chat_id, "Bulk add products cancelled.")
    else:
        logger.info("Bulk add product flow cancelled internally (no query/message object).")


# --- Manage Geography Handlers ---
async def handle_adm_manage_cities(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows options to manage existing cities."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not CITIES:
         return await query.edit_message_text("No cities configured. Use 'Add New City'.", parse_mode=None,
                                 reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ Add New City", callback_data="adm_add_city")],
                                                                      [InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")]]))
    sorted_city_ids = sorted(CITIES.keys(), key=lambda city_id: CITIES.get(city_id, ''))
    keyboard = []
    for c in sorted_city_ids:
        city_name = CITIES.get(c,'N/A')
        keyboard.append([
             InlineKeyboardButton(f"🏙️ {city_name}", callback_data=f"adm_edit_city|{c}"),
             InlineKeyboardButton(f"🗑️ Delete", callback_data=f"adm_delete_city|{c}")
        ])
    keyboard.append([InlineKeyboardButton("➕ Add New City", callback_data="adm_add_city")])
    keyboard.append([InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")])
    await query.edit_message_text("🏙️ Manage Cities\n\nSelect a city or action:",
                            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_add_city(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Add New City' button press."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    context.user_data["state"] = "awaiting_new_city_name"
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="adm_manage_cities")]]
    await query.edit_message_text("🏙️ Atsakykite su naujo miesto pavadinimu:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter city name in chat.")

async def handle_adm_edit_city(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Edit City' button press."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Klaida: Trūksta miesto ID.", show_alert=True)
    city_id = params[0]
    city_name = CITIES.get(city_id)
    if not city_name:
        return await query.edit_message_text("Klaida: Miestas nerastas.", parse_mode=None)
    context.user_data["state"] = "awaiting_edit_city_name"
    context.user_data["edit_city_id"] = city_id
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="adm_manage_cities")]]
    await query.edit_message_text(f"✏️ Editing city: {city_name}\n\nPlease reply with the new name for this city:",
                            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter new city name in chat.")

async def handle_adm_delete_city(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Delete City' button press, shows confirmation."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Klaida: Trūksta miesto ID.", show_alert=True)
    city_id = params[0]
    city_name = CITIES.get(city_id)
    if not city_name:
        return await query.edit_message_text("Klaida: Miestas nerastas.", parse_mode=None)
    context.user_data["confirm_action"] = f"delete_city|{city_id}"
    msg = (f"⚠️ Confirm Deletion\n\n"
           f"Are you sure you want to delete city: {city_name}?\n\n"
           f"🚨 This will permanently delete this city, all its districts, and all products listed within those districts!")
    keyboard = [[InlineKeyboardButton("✅ Yes, Delete City", callback_data="confirm_yes"),
                 InlineKeyboardButton("❌ No, Cancel", callback_data="adm_manage_cities")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_manage_districts(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows list of cities to choose from for managing districts."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not CITIES:
         return await query.edit_message_text("No cities configured. Add a city first.", parse_mode=None,
                                 reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")]]))
    sorted_city_ids = sorted(CITIES.keys(), key=lambda city_id: CITIES.get(city_id,''))
    keyboard = [[InlineKeyboardButton(f"🏙️ {CITIES.get(c, 'N/A')}", callback_data=f"adm_manage_districts_city|{c}")] for c in sorted_city_ids]
    keyboard.append([InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")])
    await query.edit_message_text("🗺️ Manage Districts\n\nSelect the city whose districts you want to manage:",
                            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_manage_districts_city(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows districts for the selected city and management options."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Klaida: Trūksta miesto ID.", show_alert=True)
    city_id = params[0]
    city_name = CITIES.get(city_id)
    if not city_name:
        return await query.edit_message_text("Klaida: Miestas nerastas.", parse_mode=None)
    districts_in_city = {}
    conn = None
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        # Use column names
        c.execute("SELECT id, name FROM districts WHERE city_id = ? ORDER BY name", (int(city_id),))
        districts_in_city = {str(row['id']): row['name'] for row in c.fetchall()}
    except (sqlite3.Error, ValueError) as e:
        logger.error(f"Failed to reload districts for city {city_id}: {e}")
        districts_in_city = DISTRICTS.get(city_id, {}) # Fallback to potentially outdated global
    finally:
        if conn: conn.close()

    msg = f"🗺️ Districts in {city_name}\n\n"
    keyboard = []
    if not districts_in_city: msg += "No districts found for this city."
    else:
        sorted_district_ids = sorted(districts_in_city.keys(), key=lambda dist_id: districts_in_city.get(dist_id,''))
        for d_id in sorted_district_ids:
            dist_name = districts_in_city.get(d_id)
            if dist_name:
                 keyboard.append([
                     InlineKeyboardButton(f"✏️ Edit {dist_name}", callback_data=f"adm_edit_district|{city_id}|{d_id}"),
                     InlineKeyboardButton(f"🗑️ Delete {dist_name}", callback_data=f"adm_remove_district|{city_id}|{d_id}")
                 ])
            else: logger.warning(f"District name missing for ID {d_id} in city {city_id} (manage view)")
    keyboard.extend([
        [InlineKeyboardButton("➕ Add New District", callback_data=f"adm_add_district|{city_id}")],
        [InlineKeyboardButton("⬅️ Atgal į miestus", callback_data="adm_manage_districts")]
    ])
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Error editing manage districts city message: {e}")
        else: await query.answer()

async def handle_adm_add_district(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Add New District' button press."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Klaida: Trūksta miesto ID.", show_alert=True)
    city_id = params[0]
    city_name = CITIES.get(city_id)
    if not city_name:
        return await query.edit_message_text("Klaida: Miestas nerastas.", parse_mode=None)
    context.user_data["state"] = "awaiting_new_district_name"
    context.user_data["admin_add_district_city_id"] = city_id
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data=f"adm_manage_districts_city|{city_id}")]]
    await query.edit_message_text(f"➕ Adding district to {city_name}\n\nPlease reply with the name for the new district:",
                            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter district name in chat.")

async def handle_adm_edit_district(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Edit District' button press."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params or len(params) < 2: return await query.answer("Error: City/District ID missing.", show_alert=True)
    city_id, dist_id = params
    city_name = CITIES.get(city_id)
    district_name = None
    conn = None
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        # Use column name
        c.execute("SELECT name FROM districts WHERE id = ? AND city_id = ?", (int(dist_id), int(city_id)))
        res = c.fetchone(); district_name = res['name'] if res else None
    except (sqlite3.Error, ValueError) as e: logger.error(f"Failed to fetch district name for edit: {e}")
    finally:
         if conn: conn.close()
    if not city_name or district_name is None:
        return await query.edit_message_text("Error: City/District not found.", parse_mode=None)
    context.user_data["state"] = "awaiting_edit_district_name"
    context.user_data["edit_city_id"] = city_id
    context.user_data["edit_district_id"] = dist_id
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data=f"adm_manage_districts_city|{city_id}")]]
    await query.edit_message_text(f"✏️ Editing district: {district_name} in {city_name}\n\nPlease reply with the new name for this district:",
                           reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter new district name in chat.")

async def handle_adm_remove_district(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Delete District' button press, shows confirmation."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params or len(params) < 2: return await query.answer("Error: City/District ID missing.", show_alert=True)
    city_id, dist_id = params
    city_name = CITIES.get(city_id)
    district_name = None
    conn = None
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        # Use column name
        c.execute("SELECT name FROM districts WHERE id = ? AND city_id = ?", (int(dist_id), int(city_id)))
        res = c.fetchone(); district_name = res['name'] if res else None
    except (sqlite3.Error, ValueError) as e: logger.error(f"Failed to fetch district name for delete confirmation: {e}")
    finally:
        if conn: conn.close()
    if not city_name or district_name is None:
        return await query.edit_message_text("Error: City/District not found.", parse_mode=None)
    context.user_data["confirm_action"] = f"remove_district|{city_id}|{dist_id}"
    msg = (f"⚠️ Confirm Deletion\n\n"
           f"Are you sure you want to delete district: {district_name} from {city_name}?\n\n"
           f"🚨 This will permanently delete this district and all products listed within it!")
    keyboard = [[InlineKeyboardButton("✅ Yes, Delete District", callback_data="confirm_yes"),
                 InlineKeyboardButton("❌ No, Cancel", callback_data=f"adm_manage_districts_city|{city_id}")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


# --- Manage Products Handlers ---
async def handle_adm_manage_products(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin selects city to manage products in."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not CITIES:
         return await query.edit_message_text("No cities configured. Add a city first.", parse_mode=None,
                                 reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")]]))
    sorted_city_ids = sorted(CITIES.keys(), key=lambda city_id: CITIES.get(city_id,''))
    keyboard = [[InlineKeyboardButton(f"🏙️ {CITIES.get(c,'N/A')}", callback_data=f"adm_manage_products_city|{c}")] for c in sorted_city_ids]
    keyboard.append([InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")])
    await query.edit_message_text("🗑️ Manage Products\n\nSelect the city where the products are located:",
                            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def handle_adm_manage_products_city(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin selects district to manage products in."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Klaida: Trūksta miesto ID.", show_alert=True)
    city_id = params[0]
    city_name = CITIES.get(city_id)
    if not city_name:
        return await query.edit_message_text("Klaida: Miestas nerastas.", parse_mode=None)
    districts_in_city = DISTRICTS.get(city_id, {})
    if not districts_in_city:
         keyboard = [[InlineKeyboardButton("⬅️ Atgal į miestus", callback_data="adm_manage_products")]]
         return await query.edit_message_text(f"No districts found for {city_name}. Cannot manage products.",
                                 reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    sorted_district_ids = sorted(districts_in_city.keys(), key=lambda d_id: districts_in_city.get(d_id,''))
    keyboard = []
    for d in sorted_district_ids:
         dist_name = districts_in_city.get(d)
         if dist_name:
             keyboard.append([InlineKeyboardButton(f"🏘️ {dist_name}", callback_data=f"adm_manage_products_dist|{city_id}|{d}")])
         else: logger.warning(f"District name missing for ID {d} in city {city_id} (manage products)")
    keyboard.append([InlineKeyboardButton("⬅️ Atgal į miestus", callback_data="adm_manage_products")])
    await query.edit_message_text(f"🗑️ Manage Products in {city_name}\n\nSelect district:",
                            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def handle_adm_manage_products_dist(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin selects product type to manage within the district."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params or len(params) < 2: return await query.answer("Error: City/District ID missing.", show_alert=True)
    city_id, dist_id = params
    city_name = CITIES.get(city_id)
    district_name = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city_name or not district_name:
        return await query.edit_message_text("Error: City/District not found.", parse_mode=None)
    conn = None # Initialize conn
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        # Use column name
        c.execute("SELECT DISTINCT product_type FROM products WHERE city = ? AND district = ? ORDER BY product_type", (city_name, district_name))
        product_types_in_dist = sorted([row['product_type'] for row in c.fetchall()])
        if not product_types_in_dist:
             keyboard = [[InlineKeyboardButton("⬅️ Back to Districts", callback_data=f"adm_manage_products_city|{city_id}")]]
             return await query.edit_message_text(f"No product types found in {city_name} / {district_name}.",
                                     reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        keyboard = []
        for pt in product_types_in_dist:
             emoji = PRODUCT_TYPES.get(pt, DEFAULT_PRODUCT_EMOJI)
             keyboard.append([InlineKeyboardButton(f"{emoji} {pt}", callback_data=f"adm_manage_products_type|{city_id}|{dist_id}|{pt}")])

        keyboard.append([InlineKeyboardButton("⬅️ Back to Districts", callback_data=f"adm_manage_products_city|{city_id}")])
        await query.edit_message_text(f"🗑️ Manage Products in {city_name} / {district_name}\n\nSelect product type:",
                                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except sqlite3.Error as e:
        logger.error(f"DB error fetching product types for managing in {city_name}/{district_name}: {e}", exc_info=True)
        await query.edit_message_text("❌ Error fetching product types.", parse_mode=None)
    finally:
        if conn: conn.close() # Close connection if opened


async def handle_adm_manage_products_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows specific products of a type and allows deletion."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params or len(params) < 3: return await query.answer("Klaida: Trūksta vietos/tipo informacijos.", show_alert=True)
    city_id, dist_id, p_type = params
    city_name = CITIES.get(city_id)
    district_name = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city_name or not district_name:
        return await query.edit_message_text("Error: City/District not found.", parse_mode=None)

    type_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)

    conn = None # Initialize conn
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        # Use column names
        c.execute("""
            SELECT id, size, price, available, reserved, name
            FROM products WHERE city = ? AND district = ? AND product_type = ?
            ORDER BY size, price, id
        """, (city_name, district_name, p_type))
        products = c.fetchall()
        msg = f"🗑️ Products: {type_emoji} {p_type} in {city_name} / {district_name}\n\n"
        keyboard = []
        full_msg = msg # Initialize full message

        if not products:
            full_msg += "No products of this type found here."
        else:
             header = "ID | Size | Price | Status (Avail/Reserved)\n" + "----------------------------------------\n"
             full_msg += header
             items_text_list = []
             for prod in products:
                prod_id, size_str, price_str = prod['id'], prod['size'], format_currency(prod['price'])
                status_str = f"{prod['available']}/{prod['reserved']}"
                items_text_list.append(f"{prod_id} | {size_str} | {price_str}€ | {status_str}")
                keyboard.append([InlineKeyboardButton(f"🗑️ Delete ID {prod_id}", callback_data=f"adm_delete_prod|{prod_id}")])
             full_msg += "\n".join(items_text_list)

        keyboard.append([InlineKeyboardButton("⬅️ Back to Types", callback_data=f"adm_manage_products_dist|{city_id}|{dist_id}")])
        try:
            await query.edit_message_text(full_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except telegram_error.BadRequest as e:
             if "message is not modified" not in str(e).lower(): logger.error(f"Error editing manage products type: {e}.")
             else: await query.answer() # Acknowledge if not modified
    except sqlite3.Error as e:
        logger.error(f"DB error fetching products for deletion: {e}", exc_info=True)
        await query.edit_message_text("❌ Error fetching products.", parse_mode=None)
    finally:
        if conn: conn.close() # Close connection if opened


async def handle_adm_delete_prod(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Delete Product' button press, shows confirmation."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Error: Product ID missing.", show_alert=True)
    try: product_id = int(params[0])
    except ValueError: return await query.answer("Error: Invalid Product ID.", show_alert=True)
    product_name = f"Product ID {product_id}"
    product_details = ""
    back_callback = "adm_manage_products" # Default back location
    conn = None # Initialize conn
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        # Use column names
        c.execute("""
            SELECT p.name, p.city, p.district, p.product_type, p.size, p.price, ci.id as city_id, di.id as dist_id
            FROM products p LEFT JOIN cities ci ON p.city = ci.name
            LEFT JOIN districts di ON p.district = di.name AND ci.id = di.city_id
            WHERE p.id = ?
        """, (product_id,))
        result = c.fetchone()
        if result:
            type_name = result['product_type']
            emoji = PRODUCT_TYPES.get(type_name, DEFAULT_PRODUCT_EMOJI)
            product_name = result['name'] or product_name
            product_details = f"{emoji} {type_name} {result['size']} ({format_currency(result['price'])}€) in {result['city']}/{result['district']}"
            if result['city_id'] and result['dist_id'] and result['product_type']:
                back_callback = f"adm_manage_products_type|{result['city_id']}|{result['dist_id']}|{result['product_type']}"
            else: logger.warning(f"Could not retrieve full details for product {product_id} during delete confirmation.")
        else:
            return await query.edit_message_text("Error: Product not found.", parse_mode=None)
    except sqlite3.Error as e:
         logger.warning(f"Could not fetch full details for product {product_id} for delete confirmation: {e}")
    finally:
        if conn: conn.close() # Close connection if opened

    context.user_data["confirm_action"] = f"confirm_remove_product|{product_id}"
    msg = (f"⚠️ Confirm Deletion\n\nAre you sure you want to permanently delete this specific product instance?\n"
           f"Product ID: {product_id}\nDetails: {product_details}\n\n🚨 This action is irreversible!")
    keyboard = [[InlineKeyboardButton("✅ Yes, Delete Product", callback_data="confirm_yes"),
                 InlineKeyboardButton("❌ No, Cancel", callback_data=back_callback)]] # Use dynamic back callback
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


# --- Product Type Reassignment Handler ---
async def handle_adm_reassign_type_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows interface for reassigning products from one type to another."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): 
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    load_all_data()
    if len(PRODUCT_TYPES) < 2:
        return await query.edit_message_text(
            "🔄 Reassign Product Type\n\n❌ You need at least 2 product types to perform reassignment.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Atgal", callback_data="admin_menu")]]),
            parse_mode=None
        )
    
    msg = "🔄 Reassign Product Type\n\n"
    msg += "Select the OLD product type (the one you want to change FROM):\n\n"
    msg += "⚠️ This will:\n"
    msg += "• Move all products from OLD type to NEW type\n"
    msg += "• Update all reseller discounts to use NEW type\n"
    msg += "• Delete the OLD product type\n"
    
    keyboard = []
    for type_name, emoji in sorted(PRODUCT_TYPES.items()):
        # Get product count for this type
        conn = None
        product_count = 0
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT COUNT(*) as count FROM products WHERE product_type = ?", (type_name,))
            result = c.fetchone()
            product_count = result['count'] if result else 0
        except sqlite3.Error as e:
            logger.error(f"Error counting products for type {type_name}: {e}")
        finally:
            if conn: conn.close()
        
        button_text = f"{emoji} {type_name}"
        if product_count > 0:
            button_text += f" ({product_count} products)"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"adm_reassign_select_old|{type_name}")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")])
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_reassign_select_old(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles selection of the old product type to reassign from."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): 
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params:
        return await query.answer("Error: Type name missing.", show_alert=True)
    
    old_type_name = params[0]
    load_all_data()
    
    if old_type_name not in PRODUCT_TYPES:
        return await query.edit_message_text(
            f"❌ Error: Product type '{old_type_name}' not found.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Atgal", callback_data="adm_reassign_type_start")]]),
            parse_mode=None
        )
    
    # Store the old type selection
    context.user_data['reassign_old_type_name'] = old_type_name
    
    msg = f"🔄 Reassign Product Type\n\n"
    msg += f"OLD Type: {PRODUCT_TYPES[old_type_name]} {old_type_name}\n\n"
    msg += "Select the NEW product type (where products will be moved TO):\n"
    
    keyboard = []
    for type_name, emoji in sorted(PRODUCT_TYPES.items()):
        if type_name == old_type_name:
            continue  # Don't show the same type as an option
        
        keyboard.append([InlineKeyboardButton(f"{emoji} {type_name}", callback_data=f"adm_reassign_confirm|{old_type_name}|{type_name}")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back to Select Old Type", callback_data="adm_reassign_type_start")])
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_reassign_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows confirmation for the product type reassignment."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): 
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params or len(params) < 2:
        return await query.answer("Error: Type names missing.", show_alert=True)
    
    old_type_name = params[0]
    new_type_name = params[1]
    
    load_all_data()
    
    if old_type_name not in PRODUCT_TYPES or new_type_name not in PRODUCT_TYPES:
        return await query.edit_message_text(
            "❌ Error: One or both product types not found.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Atgal", callback_data="adm_reassign_type_start")]]),
            parse_mode=None
        )
    
    # Count affected items
    conn = None
    product_count = 0
    reseller_discount_count = 0
    
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Count products that will be reassigned
        c.execute("SELECT COUNT(*) as count FROM products WHERE product_type = ?", (old_type_name,))
        result = c.fetchone()
        product_count = result['count'] if result else 0
        
        # Count reseller discounts that will be affected
        c.execute("SELECT COUNT(*) as count FROM reseller_discounts WHERE product_type = ?", (old_type_name,))
        result = c.fetchone()
        reseller_discount_count = result['count'] if result else 0
        
    except sqlite3.Error as e:
        logger.error(f"Error counting items for reassignment: {e}")
        return await query.edit_message_text(
            "❌ Database error checking reassignment impact.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Atgal", callback_data="adm_reassign_type_start")]]),
            parse_mode=None
        )
    finally:
        if conn: conn.close()
    
    old_emoji = PRODUCT_TYPES.get(old_type_name, '📦')
    new_emoji = PRODUCT_TYPES.get(new_type_name, '📦')
    
    msg = f"🔄 Confirm Product Type Reassignment\n\n"
    msg += f"FROM: {old_emoji} {old_type_name}\n"
    msg += f"TO: {new_emoji} {new_type_name}\n\n"
    msg += f"📊 Impact Summary:\n"
    msg += f"• Products to reassign: {product_count}\n"
    msg += f"• Reseller discount rules to update: {reseller_discount_count}\n\n"
    msg += f"⚠️ This action will:\n"
    msg += f"1. Move all {product_count} products from '{old_type_name}' to '{new_type_name}'\n"
    msg += f"2. Update {reseller_discount_count} reseller discount rules\n"
    msg += f"3. Delete the '{old_type_name}' product type completely\n\n"
    msg += f"🚨 THIS ACTION CANNOT BE UNDONE!"
    
    # Store data for confirmation
    context.user_data['reassign_old_type_name'] = old_type_name
    context.user_data['reassign_new_type_name'] = new_type_name
    
    keyboard = [
        [InlineKeyboardButton(f"✅ YES, Reassign {product_count} Products", callback_data=f"confirm_yes|confirm_reassign_type|{old_type_name}|{new_type_name}")],
        [InlineKeyboardButton("❌ Atšaukti", callback_data="adm_reassign_type_start")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

# --- Manage Product Types Handlers ---
async def handle_adm_manage_types(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows options to manage product types (edit emoji, delete)."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    load_all_data() # Ensure PRODUCT_TYPES is up-to-date
    if not PRODUCT_TYPES: msg = "🧩 Manage Product Types\n\nNo product types configured."
    else: msg = "🧩 Manage Product Types\n\nSelect a type to edit or delete:"
    keyboard = []
    for type_name, emoji in sorted(PRODUCT_TYPES.items()):
         keyboard.append([
             InlineKeyboardButton(f"{emoji} {type_name}", callback_data=f"adm_edit_type_menu|{type_name}"),
             InlineKeyboardButton(f"🗑️ Delete", callback_data=f"adm_delete_type|{type_name}")
         ])
    keyboard.extend([
        [InlineKeyboardButton("➕ Add New Type", callback_data="adm_add_type")],
        [InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")]
    ])
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

# --- Edit Type Menu ---
async def handle_adm_edit_type_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows options for a specific product type: change emoji, edit description, or delete."""
    query = update.callback_query
    lang, lang_data = _get_lang_data(context) # Use helper
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params: return await query.answer("Error: Type name missing.", show_alert=True)

    type_name = params[0]
    current_emoji = PRODUCT_TYPES.get(type_name, DEFAULT_PRODUCT_EMOJI)

    # Fetch current description
    current_description = ""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT description FROM product_types WHERE name = ?", (type_name,))
        res = c.fetchone()
        if res: current_description = res['description'] or "(Description not set)"
        else: current_description = "(Type not found in DB)"
    except sqlite3.Error as e:
        logger.error(f"Error fetching description for type {type_name}: {e}")
        current_description = "(DB Error fetching description)"
    finally:
        if conn: conn.close()


    safe_name = type_name # No Markdown V2 here
    safe_desc = current_description # No Markdown V2 here

    msg_template = lang_data.get("admin_edit_type_menu", "🧩 Editing Type: {type_name}\n\nCurrent Emoji: {emoji}\nDescription: {description}\n\nWhat would you like to do?")
    msg = msg_template.format(type_name=safe_name, emoji=current_emoji, description=safe_desc)

    change_emoji_button_text = lang_data.get("admin_edit_type_emoji_button", "✏️ Change Emoji")
    change_name_button_text = lang_data.get("admin_edit_type_name_button", "📝 Change Name")
    change_desc_button_text = lang_data.get("admin_edit_type_desc_button", "📝 Edit Description") # Keep commented out

    keyboard = [
        [InlineKeyboardButton(change_emoji_button_text, callback_data=f"adm_change_type_emoji|{type_name}")],
        [InlineKeyboardButton(change_name_button_text, callback_data=f"adm_change_type_name|{type_name}")],
        # [InlineKeyboardButton(change_desc_button_text, callback_data=f"adm_edit_type_desc|{type_name}")], # Description editing for types not implemented
        [InlineKeyboardButton(f"🗑️ Delete {type_name}", callback_data=f"adm_delete_type|{type_name}")],
        [InlineKeyboardButton("⬅️ Back to Types", callback_data="adm_manage_types")]
    ]

    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" in str(e).lower(): await query.answer()
        else:
            logger.error(f"Error editing type menu: {e}. Message: {msg}")
            await query.answer("Error displaying menu.", show_alert=True)

# --- Change Type Emoji Prompt ---
async def handle_adm_change_type_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Change Emoji' button press."""
    query = update.callback_query
    lang, lang_data = _get_lang_data(context) # Use helper
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Error: Type name missing.", show_alert=True)
    type_name = params[0]

    context.user_data["state"] = "awaiting_edit_type_emoji"
    context.user_data["edit_type_name"] = type_name
    current_emoji = PRODUCT_TYPES.get(type_name, DEFAULT_PRODUCT_EMOJI)

    prompt_text = lang_data.get("admin_enter_type_emoji", "✍️ Please reply with a single emoji for the product type:")
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data=f"adm_edit_type_menu|{type_name}")]]
    await query.edit_message_text(f"Current Emoji: {current_emoji}\n\n{prompt_text}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter new emoji in chat.")


async def handle_adm_change_type_name(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Change Name' button press."""
    query = update.callback_query
    lang, lang_data = _get_lang_data(context) # Use helper
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Error: Type name missing.", show_alert=True)
    
    old_type_name = params[0]
    context.user_data["state"] = "awaiting_edit_type_name"
    context.user_data["edit_old_type_name"] = old_type_name
    
    prompt_text = lang_data.get("admin_enter_type_name", "✍️ Please reply with the new name for this product type:")
    warning_text = "⚠️ WARNING: This will update ALL products and reseller discounts using this type!"
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data=f"adm_edit_type_menu|{old_type_name}")]]
    
    await query.edit_message_text(
        f"Current Name: {old_type_name}\n\n{warning_text}\n\n{prompt_text}", 
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None
    )
    await query.answer("Enter new name in chat.")

# --- Add Type asks for name first ---
async def handle_adm_add_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Add New Type' button press - asks for name first."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    context.user_data["state"] = "awaiting_new_type_name"
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="adm_manage_types")]]
    await query.edit_message_text("🧩 Please reply with the name for the new product type:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter type name in chat.")

async def handle_adm_delete_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Delete Type' button, checks usage, shows confirmation or force delete option."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Error: Type name missing.", show_alert=True)
    type_name_to_delete = params[0] # Use a distinct variable name
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM products WHERE product_type = ?", (type_name_to_delete,))
        product_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM reseller_discounts WHERE product_type = ?", (type_name_to_delete,))
        reseller_discount_count = c.fetchone()[0]

        if product_count > 0 or reseller_discount_count > 0:
            error_msg_parts = []
            if product_count > 0: error_msg_parts.append(f"{product_count} product(s)")
            if reseller_discount_count > 0: error_msg_parts.append(f"{reseller_discount_count} reseller discount rule(s)")
            usage_details = " and ".join(error_msg_parts)

            # Store the type name in user_data for the next step
            context.user_data['force_delete_type_name'] = type_name_to_delete

            force_delete_msg = (
                f"⚠️ Type '{type_name_to_delete}' is currently used by {usage_details}.\n\n"
                f"You can 'Force Delete' to remove this type AND all associated products/discount rules.\n\n"
                f"🚨 THIS IS IRREVERSIBLE AND WILL DELETE THE LISTED ITEMS."
            )
            # Use a very short callback_data, type_name is now in user_data
            keyboard = [
                [InlineKeyboardButton(f"💣 Force Delete Type & {usage_details}", callback_data="confirm_force_delete_prompt")],
                [InlineKeyboardButton("⬅️ Back to Manage Types", callback_data="adm_manage_types")]
            ]
            await query.edit_message_text(force_delete_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        else:
            # No usage, proceed with normal delete confirmation
            context.user_data["confirm_action"] = f"delete_type|{type_name_to_delete}" # Normal delete
            msg = (f"⚠️ Confirm Deletion\n\nAre you sure you want to delete product type: {type_name_to_delete}?\n\n"
                   f"🚨 This action is irreversible!")
            keyboard = [[InlineKeyboardButton("✅ Yes, Delete Type", callback_data="confirm_yes"),
                         InlineKeyboardButton("❌ No, Cancel", callback_data="adm_manage_types")]]
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except sqlite3.Error as e:
        logger.error(f"DB error checking product type usage for '{type_name_to_delete}': {e}", exc_info=True)
        await query.edit_message_text("❌ Error checking type usage.", parse_mode=None)
    finally:
        if conn: conn.close()

# <<< RENAMED AND MODIFIED CALLBACK HANDLER FOR FORCE DELETE CONFIRMATION >>>
async def handle_confirm_force_delete_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows a final, more severe confirmation for force deleting a product type and its associated items."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)

    # Retrieve the type name from user_data
    type_name = context.user_data.get('force_delete_type_name')
    if not type_name:
        logger.error("handle_confirm_force_delete_prompt: force_delete_type_name not found in user_data.")
        await query.edit_message_text("Error: Could not retrieve type name for force delete. Please try again.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Atgal", callback_data="adm_manage_types")]]))
        return

    context.user_data["confirm_action"] = f"force_delete_type_CASCADE|{type_name}" # Set up for handle_confirm_yes

    # Fetch counts again for the confirmation message
    product_count = 0
    reseller_discount_count = 0
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM products WHERE product_type = ?", (type_name,))
        product_count_res = c.fetchone()
        if product_count_res: product_count = product_count_res[0]

        c.execute("SELECT COUNT(*) FROM reseller_discounts WHERE product_type = ?", (type_name,))
        reseller_discount_count_res = c.fetchone()
        if reseller_discount_count_res: reseller_discount_count = reseller_discount_count_res[0]
    except sqlite3.Error as e:
        logger.error(f"DB error fetching counts for force delete confirmation of '{type_name}': {e}")
        await query.edit_message_text("Error fetching item counts for confirmation. Cannot proceed.", parse_mode=None)
        return
    finally:
        if conn: conn.close()

    usage_details_parts = []
    if product_count > 0: usage_details_parts.append(f"{product_count} product(s)")
    if reseller_discount_count > 0: usage_details_parts.append(f"{reseller_discount_count} reseller discount rule(s)")
    usage_details = " and ".join(usage_details_parts) if usage_details_parts else "associated items"


    msg = (f"🚨🚨🚨 FINAL CONFIRMATION 🚨🚨🚨\n\n"
           f"Are you ABSOLUTELY SURE you want to delete product type '{type_name}'?\n\n"
           f"This will also PERMANENTLY DELETE:\n"
           f"  • All {usage_details} linked to this type.\n"
           f"  • All media associated with those products.\n\n"
           f"THIS ACTION CANNOT BE UNDONE AND WILL RESULT IN DATA LOSS.")
    keyboard = [[InlineKeyboardButton("✅ YES, I understand, DELETE ALL", callback_data="confirm_yes")],
                 [InlineKeyboardButton("❌ NO, Cancel Force Delete", callback_data="adm_manage_types")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


# --- Discount Handlers ---
async def handle_adm_manage_discounts(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Displays existing discount codes and management options."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    conn = None # Initialize conn
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        c.execute("""
            SELECT id, code, discount_type, value, is_active, max_uses, uses_count, expiry_date
            FROM discount_codes ORDER BY created_date DESC
        """)
        codes = c.fetchall()
        msg = "🏷️ Manage General Discount Codes\n\n" # Clarified title
        keyboard = []
        if not codes: msg += "No general discount codes found."
        else:
            for code in codes: # Access by column name
                status = "✅ Active" if code['is_active'] else "❌ Inactive"
                value_str = format_discount_value(code['discount_type'], code['value'])
                usage_limit = f"/{code['max_uses']}" if code['max_uses'] is not None else "/∞"
                usage = f"{code['uses_count']}{usage_limit}"
                expiry_info = ""
                if code['expiry_date']:
                     try:
                         # Ensure stored date is treated as UTC before comparison
                         expiry_dt = datetime.fromisoformat(code['expiry_date']).replace(tzinfo=timezone.utc)
                         expiry_info = f" | Expires: {expiry_dt.strftime('%Y-%m-%d')}"
                         # Compare with current UTC time
                         if datetime.now(timezone.utc) > expiry_dt and code['is_active']: status = "⏳ Expired"
                     except ValueError: expiry_info = " | Invalid Date"
                toggle_text = "Deactivate" if code['is_active'] else "Activate"
                delete_text = "🗑️ Delete"
                code_text = code['code']
                msg += f"`{code_text}` ({value_str} {code['discount_type']}) | {status} | Used: {usage}{expiry_info}\n" # Use markdown for code
                keyboard.append([
                    InlineKeyboardButton(f"{'❌' if code['is_active'] else '✅'} {toggle_text}", callback_data=f"adm_toggle_discount|{code['id']}"),
                    InlineKeyboardButton(f"{delete_text}", callback_data=f"adm_delete_discount|{code['id']}")
                ])
        keyboard.extend([
            [InlineKeyboardButton("➕ Add New General Discount", callback_data="adm_add_discount_start")],
            [InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")]
        ])
        try:
             # Use MarkdownV2 for code formatting
            await query.edit_message_text(helpers.escape_markdown(msg, version=2), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
        except telegram_error.BadRequest as e:
             if "message is not modified" not in str(e).lower():
                 logger.error(f"Error editing discount list (MarkdownV2): {e}. Falling back to plain.")
                 try:
                     # Fallback to plain text
                     plain_msg = msg.replace('`', '') # Simple removal
                     await query.edit_message_text(plain_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
                 except Exception as fallback_e:
                     logger.error(f"Error editing discount list (Fallback): {fallback_e}")
                     await query.answer("Error updating list.", show_alert=True)
             else: await query.answer() # Ignore not modified
    except sqlite3.Error as e:
        logger.error(f"DB error loading discount codes: {e}", exc_info=True)
        await query.edit_message_text("❌ Error loading discount codes.", parse_mode=None)
    except Exception as e:
         logger.error(f"Unexpected error managing discounts: {e}", exc_info=True)
         await query.edit_message_text("❌ An unexpected error occurred.", parse_mode=None)
    finally:
        if conn: conn.close() # Close connection if opened


async def handle_adm_toggle_discount(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Activates or deactivates a specific discount code."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params: return await query.answer("Error: Code ID missing.", show_alert=True)
    conn = None # Initialize conn
    try:
        code_id = int(params[0])
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        c.execute("SELECT is_active FROM discount_codes WHERE id = ?", (code_id,))
        result = c.fetchone()
        if not result: return await query.answer("Code not found.", show_alert=True)
        current_status = result['is_active']
        new_status = 0 if current_status == 1 else 1
        c.execute("UPDATE discount_codes SET is_active = ? WHERE id = ?", (new_status, code_id))
        conn.commit()
        action = 'deactivated' if new_status == 0 else 'activated'
        logger.info(f"Admin {query.from_user.id} {action} discount code ID {code_id}.")
        await query.answer(f"Code {action} successfully.")
        await handle_adm_manage_discounts(update, context) # Refresh list
    except (sqlite3.Error, ValueError) as e:
        logger.error(f"Error toggling discount code {params[0]}: {e}", exc_info=True)
        await query.answer("Error updating code status.", show_alert=True)
    finally:
        if conn: conn.close() # Close connection if opened


async def handle_adm_delete_discount(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles delete button press for discount code, shows confirmation."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params: return await query.answer("Error: Code ID missing.", show_alert=True)
    conn = None # Initialize conn
    try:
        code_id = int(params[0])
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        c.execute("SELECT code FROM discount_codes WHERE id = ?", (code_id,))
        result = c.fetchone()
        if not result: return await query.answer("Code not found.", show_alert=True)
        code_text = result['code']
        context.user_data["confirm_action"] = f"delete_discount|{code_id}"
        msg = (f"⚠️ Confirm Deletion\n\nAre you sure you want to permanently delete discount code: `{helpers.escape_markdown(code_text, version=2)}`?\n\n"
               f"🚨 This action is irreversible!")
        keyboard = [[InlineKeyboardButton("✅ Yes, Delete Code", callback_data="confirm_yes"),
                     InlineKeyboardButton("❌ No, Cancel", callback_data="adm_manage_discounts")]]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
    except (sqlite3.Error, ValueError) as e:
        logger.error(f"Error preparing delete confirmation for discount code {params[0]}: {e}", exc_info=True)
        await query.answer("Error fetching code details.", show_alert=True)
    except telegram_error.BadRequest as e_tg:
         # Fallback if Markdown fails
         logger.warning(f"Markdown error displaying delete confirm: {e_tg}. Falling back.")
         msg_plain = msg.replace('`', '') # Simple removal
         await query.edit_message_text(msg_plain, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    finally:
        if conn: conn.close() # Close connection if opened


async def handle_adm_add_discount_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Starts the process of adding a new discount code."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    context.user_data['state'] = 'awaiting_discount_code'
    context.user_data['new_discount_info'] = {} # Initialize dict
    random_code = secrets.token_urlsafe(8).upper().replace('-', '').replace('_', '')[:8]
    keyboard = [
        [InlineKeyboardButton(f"Use Generated: {random_code}", callback_data=f"adm_use_generated_code|{random_code}")],
        [InlineKeyboardButton("❌ Atšaukti", callback_data="adm_manage_discounts")]
    ]
    await query.edit_message_text(
        "🏷️ Add New General Discount Code\n\nPlease reply with the code text you want to use (e.g., SUMMER20), or use the generated one below.\n"
        "Codes are case-sensitive.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=None
    )
    await query.answer("Enter code text or use generated.")


async def handle_adm_use_generated_code(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles using the suggested random code."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params: return await query.answer("Error: Generated code missing.", show_alert=True)
    code_text = params[0]
    await process_discount_code_input(update, context, code_text) # This function will handle message editing


async def process_discount_code_input(update, context, code_text):
    """Processes discount code input and moves to type selection."""
    query = update.callback_query if hasattr(update, 'callback_query') and update.callback_query else None
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    if not is_primary_admin(user_id):
        if query:
            await query.answer("Access Denied.", show_alert=True)
        return
    
    # Validate code
    if not code_text or not code_text.strip():
        error_msg = "❌ Code cannot be empty."
        if query:
            await query.edit_message_text(error_msg, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Atgal", callback_data="adm_manage_discounts")]]), parse_mode=None)
        else:
            await send_message_with_retry(context.bot, chat_id, error_msg, parse_mode=None)
        return
    
    code_text = code_text.strip()
    
    # Check if code already exists
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT code FROM discount_codes WHERE UPPER(code) = ?", (code_text.upper(),))
        existing = c.fetchone()
        if existing:
            error_msg = f"❌ Code '{code_text}' already exists. Please choose a different one."
            if query:
                keyboard = [[InlineKeyboardButton("⬅️ Atgal", callback_data="adm_add_discount_start")]]
                await query.edit_message_text(error_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
            else:
                await send_message_with_retry(context.bot, chat_id, error_msg, parse_mode=None)
            return
    except sqlite3.Error as e:
        logger.error(f"DB error checking existing discount codes: {e}")
        error_msg = "❌ Database error. Please try again."
        if query:
            await query.edit_message_text(error_msg, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Atgal", callback_data="adm_manage_discounts")]]), parse_mode=None)
        else:
            await send_message_with_retry(context.bot, chat_id, error_msg, parse_mode=None)
        return
    finally:
        if conn:
            conn.close()
    
    # Store code and move to type selection
    context.user_data['new_discount_info'] = {'code': code_text}
    context.user_data['state'] = 'awaiting_discount_type'
    
    msg = f"Code: {code_text}\n\nSelect discount type:"
    keyboard = [
        [InlineKeyboardButton("📊 Percentage", callback_data="adm_set_discount_type|percentage")],
        [InlineKeyboardButton("💰 Fixed Amount", callback_data="adm_set_discount_type|fixed")],
        [InlineKeyboardButton("❌ Atšaukti", callback_data="adm_manage_discounts")]
    ]
    
    if query:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        await query.answer()
    else:
        await send_message_with_retry(context.bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def handle_adm_discount_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles admin entering a discount code via message."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if not is_primary_admin(user_id):
        return
        
    if context.user_data.get("state") != 'awaiting_discount_code':
        return
        
    if not update.message or not update.message.text:
        await send_message_with_retry(context.bot, chat_id, "Please send the code as text.", parse_mode=None)
        return
    
    code_text = update.message.text.strip()
    context.user_data.pop('state', None)  # Clear state
    
    await process_discount_code_input(update, context, code_text)


async def handle_adm_discount_value_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles admin entering discount value via message."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if not is_primary_admin(user_id):
        return
        
    if context.user_data.get("state") != 'awaiting_discount_value':
        return
        
    if not update.message or not update.message.text:
        await send_message_with_retry(context.bot, chat_id, "Please send the value as text.", parse_mode=None)
        return
    
    value_text = update.message.text.strip()
    discount_info = context.user_data.get('new_discount_info', {})
    
    if not discount_info.get('code') or not discount_info.get('type'):
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Context lost. Please start again.", parse_mode=None)
        context.user_data.pop('state', None)
        context.user_data.pop('new_discount_info', None)
        keyboard = [[InlineKeyboardButton("⬅️ Back to Discounts", callback_data="adm_manage_discounts")]]
        await send_message_with_retry(context.bot, chat_id, "Returning to discount management.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        return
    
    # Validate value
    try:
        value = float(value_text)
        if value <= 0:
            await send_message_with_retry(context.bot, chat_id, "❌ Value must be greater than 0.", parse_mode=None)
            return
            
        if discount_info['type'] == 'percentage' and value > 100:
            await send_message_with_retry(context.bot, chat_id, "❌ Percentage cannot exceed 100%.", parse_mode=None)
            return
            
        if discount_info['type'] == 'fixed' and value > 10000:
            await send_message_with_retry(context.bot, chat_id, "❌ Fixed amount too high (max 10000).", parse_mode=None)
            return
            
    except ValueError:
        await send_message_with_retry(context.bot, chat_id, "❌ Invalid number format. Please enter a valid number.", parse_mode=None)
        return
    
    # Save the discount code
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Insert new discount code
        c.execute("""
            INSERT INTO discount_codes (code, discount_type, value, is_active, max_uses, uses_count, created_date)
            VALUES (?, ?, ?, 1, NULL, 0, ?)
        """, (discount_info['code'], discount_info['type'], value, datetime.now(timezone.utc).isoformat()))
        
        conn.commit()
        
        # Success message
        value_str = format_discount_value(discount_info['type'], value)
        success_msg = f"✅ Discount code created successfully!\n\nCode: {discount_info['code']}\nType: {discount_info['type'].capitalize()}\nValue: {value_str}"
        
        keyboard = [[InlineKeyboardButton("⬅️ Back to Discounts", callback_data="adm_manage_discounts")]]
        await send_message_with_retry(context.bot, chat_id, success_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
        logger.info(f"Admin {user_id} created discount code '{discount_info['code']}' ({discount_info['type']}: {value})")
        
    except sqlite3.Error as e:
        logger.error(f"DB error creating discount code: {e}", exc_info=True)
        await send_message_with_retry(context.bot, chat_id, "❌ Database error creating discount code.", parse_mode=None)
        
    except Exception as e:
        logger.error(f"Unexpected error creating discount code: {e}", exc_info=True)
        await send_message_with_retry(context.bot, chat_id, "❌ An unexpected error occurred.", parse_mode=None)
        
    finally:
        if conn:
            conn.close()
        
        # Clean up state
        context.user_data.pop('state', None)
        context.user_data.pop('new_discount_info', None)


async def handle_adm_set_discount_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Sets the discount type and asks for the value."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params: return await query.answer("Error: Discount type missing.", show_alert=True)
    current_state = context.user_data.get("state")
    if current_state not in ['awaiting_discount_type', 'awaiting_discount_code']: # Check if state is valid
         logger.warning(f"handle_adm_set_discount_type called in wrong state: {current_state}")
         if context.user_data and 'new_discount_info' in context.user_data and 'code' in context.user_data['new_discount_info']:
             context.user_data['state'] = 'awaiting_discount_type'
             logger.info("Forcing state back to awaiting_discount_type")
         else:
             return await handle_adm_manage_discounts(update, context)

    discount_type = params[0]
    if discount_type not in ['percentage', 'fixed']:
        return await query.answer("Invalid discount type.", show_alert=True)
    if 'new_discount_info' not in context.user_data: context.user_data['new_discount_info'] = {}
    context.user_data['new_discount_info']['type'] = discount_type
    context.user_data['state'] = 'awaiting_discount_value'
    value_prompt = ("Enter the percentage value (e.g., 10 for 10%):" if discount_type == 'percentage' else
                    "Enter the fixed discount amount in EUR (e.g., 5.50):")
    code_text = context.user_data.get('new_discount_info', {}).get('code', 'N/A')
    msg = f"Code: {code_text} | Type: {discount_type.capitalize()}\n\n{value_prompt}"
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="adm_manage_discounts")]]
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        await query.answer("Enter the discount value.")
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower():
             logger.error(f"Error editing message in handle_adm_set_discount_type: {e}. Message: {msg}")
             await query.answer("Error updating prompt. Please try again.", show_alert=True)
        else: await query.answer()

# --- Set Bot Media Handlers ---
async def handle_adm_set_media(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Set Bot Media' button press."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    lang, lang_data = _get_lang_data(context) # Use helper
    set_media_prompt_text = lang_data.get("set_media_prompt_plain", "Send a photo, video, or GIF to display above all messages:")
    context.user_data["state"] = "awaiting_bot_media"
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="admin_menu")]]
    await query.edit_message_text(set_media_prompt_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Send photo, video, or GIF.")


async def handle_adm_bot_media_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the admin sending bot media content."""
    if not is_primary_admin(update.effective_user.id):
        await update.message.reply_text("Access Denied.", parse_mode=None)
        return
    
    if context.user_data.get("state") != "awaiting_bot_media":
        return
    
    chat_id = update.effective_chat.id
    
    # Extract media information
    media_file_id = None
    media_type = None
    
    if update.message.photo:
        media_file_id = update.message.photo[-1].file_id
        media_type = "photo"
    elif update.message.video:
        media_file_id = update.message.video.file_id
        media_type = "video"
    elif update.message.animation:
        media_file_id = update.message.animation.file_id
        media_type = "gif"
    else:
        await send_message_with_retry(context.bot, chat_id, 
            "❌ Please send a photo, video, or GIF only.", parse_mode=None)
        return
    
    try:
        # Download the media file
        file_obj = await context.bot.get_file(media_file_id)
        
        # Determine file extension
        file_extension = os.path.splitext(file_obj.file_path)[1] if file_obj.file_path else ""
        if not file_extension:
            if media_type == "photo":
                file_extension = ".jpg"
            elif media_type == "video":
                file_extension = ".mp4"
            elif media_type == "gif":
                file_extension = ".gif"
            else:
                file_extension = ".bin"
        
        # Create media filename
        media_filename = f"bot_media{file_extension}"
        media_path = os.path.join(MEDIA_DIR, media_filename)
        
        # Ensure media directory exists
        await asyncio.to_thread(os.makedirs, MEDIA_DIR, exist_ok=True)
        
        # Download the file
        await file_obj.download_to_drive(media_path)
        
        # Save bot media configuration
        await save_bot_media_config(media_type, media_path)
        
        # Clear state
        context.user_data.pop('state', None)
        
        # Confirmation message
        success_msg = f"✅ Bot media updated successfully!\n\n📎 Type: {media_type.upper()}\n📁 Saved as: {media_filename}\n\nThis media will now be displayed when users start the bot."
        
        keyboard = [[InlineKeyboardButton("🔙 Atgal į Admin meniu", callback_data="admin_menu")]]
        await send_message_with_retry(context.bot, chat_id, success_msg, 
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
        # Log admin action
        log_admin_action(admin_id=update.effective_user.id, action="BOT_MEDIA_UPDATE", 
                        reason=f"Updated bot media: {media_type} - {media_filename}")
        
    except Exception as e:
        logger.error(f"Error processing bot media upload: {e}", exc_info=True)
        await send_message_with_retry(context.bot, chat_id, 
            "❌ Error processing media upload. Please try again.", parse_mode=None)


# --- Review Management Handlers ---
async def handle_adm_manage_reviews(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Displays reviews paginated for the admin with delete options."""
    query = update.callback_query
    user_id = query.from_user.id
    primary_admin = is_primary_admin(user_id)
    secondary_admin = is_secondary_admin(user_id)
    if not primary_admin and not secondary_admin: return await query.answer("Access Denied.", show_alert=True)
    offset = 0
    if params and len(params) > 0 and params[0].isdigit(): offset = int(params[0])
    reviews_per_page = 5
    reviews_data = fetch_reviews(offset=offset, limit=reviews_per_page + 1) # Sync function uses helper
    msg = "🚫 Manage Reviews\n\n"
    keyboard = []
    item_buttons = []
    if not reviews_data:
        if offset == 0: msg += "No reviews have been left yet."
        else: msg += "No more reviews to display."
    else:
        has_more = len(reviews_data) > reviews_per_page
        reviews_to_show = reviews_data[:reviews_per_page]
        for review in reviews_to_show:
            review_id = review.get('review_id', 'N/A')
            try:
                date_str = review.get('review_date', '')
                formatted_date = "???"
                if date_str:
                    try: formatted_date = datetime.fromisoformat(date_str.replace('Z','+00:00')).strftime("%Y-%m-%d") # Handle Z for UTC
                    except ValueError: pass
                username = review.get('username', 'anonymous')
                username_display = f"@{username}" if username and username != 'anonymous' else username
                review_text = review.get('review_text', '')
                review_text_preview = review_text[:100] + ('...' if len(review_text) > 100 else '')
                msg += f"ID {review_id} | {username_display} ({formatted_date}):\n{review_text_preview}\n\n"
                if primary_admin: # Only primary admin can delete
                     item_buttons.append([InlineKeyboardButton(f"🗑️ Delete Review #{review_id}", callback_data=f"adm_delete_review_confirm|{review_id}")])
            except Exception as e:
                 logger.error(f"Error formatting review item #{review_id} for admin view: {review}, Error: {e}")
                 msg += f"ID {review_id} | (Error displaying review)\n\n"
                 if primary_admin: item_buttons.append([InlineKeyboardButton(f"🗑️ Delete Review #{review_id}", callback_data=f"adm_delete_review_confirm|{review_id}")])
        keyboard.extend(item_buttons)
        nav_buttons = []
        if offset > 0: nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"adm_manage_reviews|{max(0, offset - reviews_per_page)}"))
        if has_more: nav_buttons.append(InlineKeyboardButton("➡️ Next", callback_data=f"adm_manage_reviews|{offset + reviews_per_page}"))
        if nav_buttons: keyboard.append(nav_buttons)
    back_callback = "admin_menu" if primary_admin else "viewer_admin_menu"
    keyboard.append([InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data=back_callback)])
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.warning(f"Failed to edit message for adm_manage_reviews: {e}"); await query.answer("Error updating review list.", show_alert=True)
        else:
            await query.answer() # Acknowledge if not modified
    except Exception as e:
        logger.error(f"Unexpected error in adm_manage_reviews: {e}", exc_info=True)
        await query.edit_message_text("❌ An unexpected error occurred while loading reviews.", parse_mode=None)


async def handle_adm_delete_review_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Delete Review' button press, shows confirmation."""
    query = update.callback_query
    user_id = query.from_user.id
    if not is_primary_admin(user_id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    if not params: return await query.answer("Error: Review ID missing.", show_alert=True)
    try: review_id = int(params[0])
    except ValueError: return await query.answer("Error: Invalid Review ID.", show_alert=True)
    review_text_snippet = "N/A"
    conn = None # Initialize conn
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        # Use column name
        c.execute("SELECT review_text FROM reviews WHERE review_id = ?", (review_id,))
        result = c.fetchone()
        if result: review_text_snippet = result['review_text'][:100]
        else:
            await query.answer("Review not found.", show_alert=True)
            try: await query.edit_message_text("Error: Review not found.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Reviews", callback_data="adm_manage_reviews|0")]]), parse_mode=None)
            except telegram_error.BadRequest: pass
            return
    except sqlite3.Error as e: logger.warning(f"Could not fetch review text for confirmation (ID {review_id}): {e}")
    finally:
        if conn: conn.close() # Close connection if opened
    context.user_data["confirm_action"] = f"delete_review|{review_id}"
    msg = (f"⚠️ Confirm Deletion\n\nAre you sure you want to permanently delete review ID {review_id}?\n\n"
           f"Preview: {review_text_snippet}{'...' if len(review_text_snippet) >= 100 else ''}\n\n"
           f"🚨 This action is irreversible!")
    keyboard = [[InlineKeyboardButton("✅ Yes, Delete Review", callback_data="confirm_yes"),
                 InlineKeyboardButton("❌ No, Cancel", callback_data="adm_manage_reviews|0")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


# --- Broadcast Handlers ---

async def handle_adm_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Starts the broadcast message process by asking for the target audience."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)

    lang, lang_data = _get_lang_data(context) # Use helper

    # Clear previous broadcast data
    context.user_data.pop('broadcast_content', None)
    context.user_data.pop('broadcast_target_type', None)
    context.user_data.pop('broadcast_target_value', None)

    prompt_msg = lang_data.get("broadcast_select_target", "📢 Broadcast Message\n\nSelect the target audience:")
    keyboard = [
        [InlineKeyboardButton(lang_data.get("broadcast_target_all", "👥 All Users"), callback_data="adm_broadcast_target_type|all")],
        [InlineKeyboardButton(lang_data.get("broadcast_target_city", "🏙️ By Last Purchased City"), callback_data="adm_broadcast_target_type|city")],
        [InlineKeyboardButton(lang_data.get("broadcast_target_status", "👑 By User Status"), callback_data="adm_broadcast_target_type|status")],
        [InlineKeyboardButton(lang_data.get("broadcast_target_inactive", "⏳ By Inactivity (Days)"), callback_data="adm_broadcast_target_type|inactive")],
        [InlineKeyboardButton("❌ Atšaukti", callback_data="admin_menu")]
    ]
    await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer()


async def handle_adm_broadcast_target_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the selection of the broadcast target type."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params: return await query.answer("Error: Target type missing.", show_alert=True)

    target_type = params[0]
    context.user_data['broadcast_target_type'] = target_type
    lang, lang_data = _get_lang_data(context) # Use helper

    if target_type == 'all':
        context.user_data['state'] = 'awaiting_broadcast_message'
        ask_msg_text = lang_data.get("broadcast_ask_message", "📝 Now send the message content (text, photo, video, or GIF with caption):")
        keyboard = [[InlineKeyboardButton("❌ Cancel Broadcast", callback_data="cancel_broadcast")]]
        await query.edit_message_text(ask_msg_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        await query.answer("Send the message content.")

    elif target_type == 'city':
        load_all_data()
        if not CITIES:
             await query.edit_message_text("No cities configured. Cannot target by city.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Atgal", callback_data="adm_broadcast_start")]]), parse_mode=None)
             return
        sorted_city_ids = sorted(CITIES.keys(), key=lambda city_id: CITIES.get(city_id, ''))
        keyboard = [[InlineKeyboardButton(f"🏙️ {CITIES.get(c,'N/A')}", callback_data=f"adm_broadcast_target_city|{CITIES.get(c,'N/A')}")] for c in sorted_city_ids if CITIES.get(c)]
        keyboard.append([InlineKeyboardButton("❌ Cancel Broadcast", callback_data="cancel_broadcast")])
        select_city_text = lang_data.get("broadcast_select_city_target", "🏙️ Select City to Target\n\nUsers whose last purchase was in:")
        await query.edit_message_text(select_city_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        await query.answer()

    elif target_type == 'status':
        select_status_text = lang_data.get("broadcast_select_status_target", "👑 Select Status to Target:")
        vip_label = lang_data.get("broadcast_status_vip", "VIP 👑")
        regular_label = lang_data.get("broadcast_status_regular", "Regular ⭐")
        new_label = lang_data.get("broadcast_status_new", "New 🌱")
        keyboard = [
            [InlineKeyboardButton(vip_label, callback_data=f"adm_broadcast_target_status|{vip_label}")],
            [InlineKeyboardButton(regular_label, callback_data=f"adm_broadcast_target_status|{regular_label}")],
            [InlineKeyboardButton(new_label, callback_data=f"adm_broadcast_target_status|{new_label}")],
            [InlineKeyboardButton("❌ Cancel Broadcast", callback_data="cancel_broadcast")]
        ]
        await query.edit_message_text(select_status_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        await query.answer()

    elif target_type == 'inactive':
        context.user_data['state'] = 'awaiting_broadcast_inactive_days'
        inactive_prompt = lang_data.get("broadcast_enter_inactive_days", "⏳ Enter Inactivity Period\n\nPlease reply with the number of days since the user's last purchase (or since registration if no purchases). Users inactive for this many days or more will receive the message.")
        keyboard = [[InlineKeyboardButton("❌ Cancel Broadcast", callback_data="cancel_broadcast")]]
        await query.edit_message_text(inactive_prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        await query.answer("Enter number of days.")

    else:
        await query.answer("Unknown target type selected.", show_alert=True)
        await handle_adm_broadcast_start(update, context)


async def handle_adm_broadcast_target_city(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles selecting the city for targeted broadcast."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params: return await query.answer("Error: City name missing.", show_alert=True)

    city_name = params[0]
    context.user_data['broadcast_target_value'] = city_name
    lang, lang_data = _get_lang_data(context) # Use helper

    context.user_data['state'] = 'awaiting_broadcast_message'
    ask_msg_text = lang_data.get("broadcast_ask_message", "📝 Now send the message content (text, photo, video, or GIF with caption):")
    keyboard = [[InlineKeyboardButton("❌ Cancel Broadcast", callback_data="cancel_broadcast")]]
    await query.edit_message_text(f"Targeting users last purchased in: {city_name}\n\n{ask_msg_text}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Send the message content.")

async def handle_adm_broadcast_target_status(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles selecting the status for targeted broadcast."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params: return await query.answer("Error: Status value missing.", show_alert=True)

    status_value = params[0]
    context.user_data['broadcast_target_value'] = status_value
    lang, lang_data = _get_lang_data(context) # Use helper

    context.user_data['state'] = 'awaiting_broadcast_message'
    ask_msg_text = lang_data.get("broadcast_ask_message", "📝 Now send the message content (text, photo, video, or GIF with caption):")
    keyboard = [[InlineKeyboardButton("❌ Cancel Broadcast", callback_data="cancel_broadcast")]]
    await query.edit_message_text(f"Targeting users with status: {status_value}\n\n{ask_msg_text}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Send the message content.")


async def handle_confirm_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the 'Yes' confirmation for the broadcast."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)

    broadcast_content = context.user_data.get('broadcast_content')
    if not broadcast_content:
        logger.error("Broadcast content not found during confirmation.")
        return await query.edit_message_text("❌ Error: Broadcast content not found. Please start again.", parse_mode=None)

    text = broadcast_content.get('text')
    media_file_id = broadcast_content.get('media_file_id')
    media_type = broadcast_content.get('media_type')
    target_type = broadcast_content.get('target_type', 'all')
    target_value = broadcast_content.get('target_value')
    admin_chat_id = query.message.chat_id

    try:
        await query.edit_message_text("⏳ Broadcast initiated. Fetching users and sending messages...", parse_mode=None)
    except telegram_error.BadRequest: await query.answer()

    context.user_data.pop('broadcast_target_type', None)
    context.user_data.pop('broadcast_target_value', None)
    context.user_data.pop('broadcast_content', None)

    asyncio.create_task(send_broadcast(context, text, media_file_id, media_type, target_type, target_value, admin_chat_id))


async def handle_cancel_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Cancels the broadcast process."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)

    context.user_data.pop('state', None)
    context.user_data.pop('broadcast_content', None)
    context.user_data.pop('broadcast_target_type', None)
    context.user_data.pop('broadcast_target_value', None)

    try:
        await query.edit_message_text("❌ Broadcast cancelled.", parse_mode=None)
    except telegram_error.BadRequest: await query.answer()

    keyboard = [[InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")]]
    await send_message_with_retry(context.bot, query.message.chat_id, "Grįžtama į Admin meniu.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

# --- Handler for Broadcast Message Content ---
async def handle_adm_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the admin sending broadcast message content."""
    if not is_primary_admin(update.effective_user.id):
        await update.message.reply_text("Access Denied.", parse_mode=None)
        return

    lang, lang_data = _get_lang_data(context)
    target_type = context.user_data.get('broadcast_target_type', 'all')
    target_value = context.user_data.get('broadcast_target_value')
    
    # Extract message content
    text = update.message.text or update.message.caption or ""
    media_file_id = None
    media_type = None
    
    # Check for media
    if update.message.photo:
        media_file_id = update.message.photo[-1].file_id
        media_type = "photo"
    elif update.message.video:
        media_file_id = update.message.video.file_id
        media_type = "video"
    elif update.message.animation:
        media_file_id = update.message.animation.file_id
        media_type = "gif"
    
    # Store broadcast content
    context.user_data['broadcast_content'] = {
        'text': text,
        'media_file_id': media_file_id,
        'media_type': media_type,
        'target_type': target_type,
        'target_value': target_value
    }
    
    # Clear state
    context.user_data.pop('state', None)
    
    # Show confirmation with preview
    preview_msg = "📢 Broadcast Preview\n\n"
    preview_msg += f"🎯 Target: {target_type}"
    if target_value:
        preview_msg += f" = {target_value}"
    preview_msg += "\n\n"
    
    if media_type:
        preview_msg += f"📎 Media: {media_type.upper()}\n"
    if text:
        preview_msg += f"📝 Text: {text[:100]}"
        if len(text) > 100:
            preview_msg += "..."
    else:
        preview_msg += "📝 Text: (media only)"
    
    preview_msg += "\n\n⚠️ Are you sure you want to send this broadcast?"
    
    keyboard = [
        [InlineKeyboardButton("✅ Yes, Send Broadcast", callback_data="confirm_broadcast")],
        [InlineKeyboardButton("❌ Atšaukti", callback_data="cancel_broadcast")]
    ]
    
    await update.message.reply_text(
        preview_msg, 
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=None
    )

# --- Handler for Inactive Days Input ---
async def handle_adm_broadcast_inactive_days_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the admin entering inactive days for broadcast targeting."""
    if not is_primary_admin(update.effective_user.id):
        await update.message.reply_text("Access Denied.", parse_mode=None)
        return
    
    lang, lang_data = _get_lang_data(context)
    
    try:
        days = int(update.message.text.strip())
        if days <= 0:
            raise ValueError("Days must be positive")
        
        context.user_data['broadcast_target_value'] = days
        context.user_data['state'] = 'awaiting_broadcast_message'
        
        ask_msg_text = lang_data.get("broadcast_ask_message", "📝 Now send the message content (text, photo, video, or GIF with caption):")
        keyboard = [[InlineKeyboardButton("❌ Cancel Broadcast", callback_data="cancel_broadcast")]]
        
        await update.message.reply_text(
            f"Targeting users inactive for {days}+ days\n\n{ask_msg_text}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=None
        )
        
    except ValueError:
        error_msg = lang_data.get("broadcast_invalid_days", "❌ Please enter a valid number of days (positive integer).")
        keyboard = [[InlineKeyboardButton("❌ Cancel Broadcast", callback_data="cancel_broadcast")]]
        await update.message.reply_text(
            error_msg,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=None
        )

async def send_broadcast(context: ContextTypes.DEFAULT_TYPE, text: str, media_file_id: str | None, media_type: str | None, target_type: str, target_value: str | int | None, admin_chat_id: int):
    """Sends the broadcast message to the target audience with improved reliability."""
    bot = context.bot
    lang_data = LANGUAGES.get('en', {}) # Use English for internal messages
    
    # Temporary flag to disable status tracking if it causes issues
    ENABLE_STATUS_TRACKING = False  # TEMPORARILY DISABLED for testing

    user_ids = await asyncio.to_thread(fetch_user_ids_for_broadcast, target_type, target_value)

    if not user_ids:
        logger.warning(f"No users found for broadcast target: type={target_type}, value={target_value}")
        no_users_msg = lang_data.get("broadcast_no_users_found_target", "⚠️ Broadcast Warning: No users found matching the target criteria.")
        await send_message_with_retry(bot, admin_chat_id, no_users_msg, parse_mode=None)
        return

    success_count, fail_count, block_count, total_users = 0, 0, 0, len(user_ids)
    logger.info(f"Starting broadcast to {total_users} users (Target: {target_type}={target_value})...")

    status_message = None
    status_update_interval = max(10, total_users // 20)
    
    # Add heartbeat tracking
    import time
    last_heartbeat = time.time()

    # Initialize status message
    status_message = None
    try:
        status_message = await send_message_with_retry(bot, admin_chat_id, f"⏳ Broadcasting... (0/{total_users})", parse_mode=None)
    except Exception as status_init_e:
        logger.error(f"Failed to initialize status message: {status_init_e}")
        # Continue without status message

    # Process each user with maximum resilience - NO OUTER TRY-CATCH TO PREVENT EARLY TERMINATION
    for i, user_id in enumerate(user_ids):
        # Log progress every 10 users to track where broadcast stops
        if (i + 1) % 10 == 0:
            current_time = time.time()
            elapsed = current_time - last_heartbeat
            logger.info(f"Broadcast progress: {i+1}/{total_users} users processed (Success: {success_count}, Failed: {fail_count}) - Heartbeat: {elapsed:.1f}s since last update")
            last_heartbeat = current_time
        
        # Process each user - isolated error handling
        try:
            message_sent = False
            
            # Send media or text message
            if media_file_id and media_type:
                # Try to send media first
                try:
                    send_kwargs = {'chat_id': user_id, 'caption': text, 'parse_mode': None}
                    if media_type == "photo": 
                        result = await bot.send_photo(photo=media_file_id, **send_kwargs)
                    elif media_type == "video": 
                        result = await bot.send_video(video=media_file_id, **send_kwargs)
                    elif media_type == "gif": 
                        result = await bot.send_animation(animation=media_file_id, **send_kwargs)
                    
                    if result:
                        success_count += 1
                        message_sent = True
                        logger.debug(f"Broadcast media sent successfully to user {user_id}")
                except telegram_error.BadRequest as media_e:
                    error_str = str(media_e).lower()
                    if "wrong file identifier" in error_str or "file_id" in error_str or "file not found" in error_str:
                        logger.warning(f"Media file ID invalid for user {user_id}, falling back to text-only: {media_e}")
                        # Fall through to text-only sending
                    else:
                        raise media_e
                except Exception as media_e:
                    logger.warning(f"Media send failed for user {user_id}: {media_e}")
                    # Fall through to text-only sending
            
            # Send text-only message if no media was sent successfully
            if not message_sent:
                try:
                    result = await send_message_with_retry(bot, user_id, text, parse_mode=None, disable_web_page_preview=True)
                    if result:
                        success_count += 1
                        logger.debug(f"Broadcast text sent successfully to user {user_id}")
                    else:
                        fail_count += 1
                        logger.warning(f"Broadcast text failed for user {user_id} after retries")
                except Exception as text_e:
                    logger.warning(f"Text send failed for user {user_id}: {text_e}")
                    fail_count += 1

        except telegram_error.BadRequest as e:
            error_str = str(e).lower()
            if "chat not found" in error_str or "user is deactivated" in error_str or "bot was blocked" in error_str:
                logger.warning(f"Broadcast fail/block for user {user_id}: {e}")
                fail_count += 1
                block_count += 1
            else:
                logger.error(f"Broadcast BadRequest for {user_id}: {e}")
                fail_count += 1

        except telegram_error.Forbidden as e:
            logger.warning(f"Broadcast fail/block for user {user_id}: {e}")
            fail_count += 1
            block_count += 1

        except telegram_error.RetryAfter as e:
            retry_seconds = e.retry_after + 1
            logger.warning(f"Rate limit hit during broadcast. Sleeping {retry_seconds}s.")
            if retry_seconds > 300:
                logger.error(f"RetryAfter > 5 min. Skipping user {user_id}.")
                fail_count += 1
            else:
                await asyncio.sleep(retry_seconds)
                # Try to send again after rate limit
                try:
                    result = await send_message_with_retry(bot, user_id, text, parse_mode=None, disable_web_page_preview=True)
                    if result:
                        success_count += 1
                        logger.info(f"Broadcast retry successful for user {user_id}")
                    else:
                        fail_count += 1
                except Exception as retry_e:
                    logger.error(f"Broadcast fail after retry for {user_id}: {retry_e}")
                    fail_count += 1

        except Exception as e:
            # CRITICAL: Catch ANY exception that might terminate the broadcast
            logger.error(f"CRITICAL: Unexpected exception processing user {user_id} at position {i+1}/{total_users}: {e}", exc_info=True)
            fail_count += 1
            # Continue processing the next user instead of terminating

        # Rate limiting
        try:
            await asyncio.sleep(0.1)  # 10 messages per second
        except Exception as sleep_e:
            logger.warning(f"Error during sleep: {sleep_e}")

        # Status updates
        try:
            if status_message and (i + 1) % max(5, status_update_interval // 2) == 0:
                try:
                    await context.bot.edit_message_text(
                        chat_id=admin_chat_id,
                        message_id=status_message.message_id,
                        text=f"⏳ Broadcasting... ({i+1}/{total_users} | ✅{success_count} | ❌{fail_count})",
                        parse_mode=None
                    )
                except telegram_error.BadRequest:
                    pass  # Ignore if message is not modified
                except Exception as edit_e:
                    logger.warning(f"Could not edit broadcast status message: {edit_e}")
        except Exception as status_update_e:
            logger.warning(f"Error during status update processing: {status_update_e}")
    
    # Log completion of the broadcast loop
    logger.info(f"Broadcast loop completed. Processed {i+1}/{total_users} users. Success: {success_count}, Failed: {fail_count}")

    # Final summary
    success_rate = (success_count / total_users * 100) if total_users > 0 else 0
    summary_msg = (f"✅ Broadcast Complete\n\n"
                  f"🎯 Target: {target_type} = {target_value or 'N/A'}\n"
                  f"📊 Results: {success_count}/{total_users} ({success_rate:.1f}%)\n"
                  f"❌ Failed: {fail_count}\n"
                  f"🚫 Blocked/Deactivated: {block_count}")
    
    try:
        if status_message:
            try:
                await context.bot.edit_message_text(
                    chat_id=admin_chat_id, 
                    message_id=status_message.message_id, 
                    text=summary_msg, 
                    parse_mode=None
                )
            except Exception:
                await send_message_with_retry(bot, admin_chat_id, summary_msg, parse_mode=None)
        else:
            await send_message_with_retry(bot, admin_chat_id, summary_msg, parse_mode=None)
    except Exception as summary_e:
        logger.error(f"Failed to send final summary: {summary_e}")
    
    logger.info(f"Broadcast finished. Target: {target_type}={target_value}. "
               f"Success: {success_count}/{total_users} ({success_rate:.1f}%), "
               f"Failed: {fail_count}, Blocked: {block_count}")


# <<< ADDED: Handler for Clear Reservations Confirmation Button >>>
async def handle_adm_clear_reservations_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows confirmation prompt for clearing all reservations."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)

    context.user_data["confirm_action"] = "clear_all_reservations"
    msg = (f"⚠️ Confirm Action: Clear All Reservations\n\n"
           f"Are you sure you want to clear ALL product reservations and empty ALL user baskets?\n\n"
           f"🚨 This action cannot be undone and will affect all users!")
    keyboard = [[InlineKeyboardButton("✅ Yes, Clear Reservations", callback_data="confirm_yes"),
                 InlineKeyboardButton("❌ No, Cancel", callback_data="admin_menu")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

# --- Confirmation Handler ---
async def handle_confirm_yes(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles generic 'Yes' confirmation based on stored action in user_data."""
    query = update.callback_query
    user_id = query.from_user.id
    primary_admin = is_primary_admin(user_id)
    if not primary_admin:
        logger.warning(f"Non-primary admin {user_id} tried to confirm a destructive action.")
        await query.answer("Permission denied for this action.", show_alert=True)
        return

    user_specific_data = context.user_data
    action = user_specific_data.pop("confirm_action", None)

    if not action:
        try: await query.edit_message_text("❌ Error: No action pending confirmation.", parse_mode=None)
        except telegram_error.BadRequest: pass # Ignore if not modified
        return
    chat_id = query.message.chat_id
    action_parts = action.split("|")
    action_type = action_parts[0]
    action_params = action_parts[1:]
    logger.info(f"Admin {user_id} confirmed action: {action_type} with params: {action_params}")
    success_msg, next_callback = "✅ Veiksmas sėkmingai įvykdytas!", "admin_menu"
    conn = None # Initialize conn
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        c.execute("BEGIN")
        # --- Delete City Logic ---
        if action_type == "delete_city":
             if not action_params: raise ValueError("Missing city_id")
             city_id_str = action_params[0]; city_id_int = int(city_id_str)
             city_name = CITIES.get(city_id_str)
             if city_name:
                 c.execute("SELECT id FROM products WHERE city = ?", (city_name,))
                 product_ids_to_delete = [row['id'] for row in c.fetchall()] # Use column name
                 logger.info(f"Admin Action (delete_city): Deleting city '{city_name}'. Associated product IDs to be deleted: {product_ids_to_delete}")
                 if product_ids_to_delete:
                     placeholders = ','.join('?' * len(product_ids_to_delete))
                     c.execute(f"DELETE FROM product_media WHERE product_id IN ({placeholders})", product_ids_to_delete)
                     for pid in product_ids_to_delete:
                          media_dir_to_del = os.path.join(MEDIA_DIR, str(pid))
                          if await asyncio.to_thread(os.path.exists, media_dir_to_del):
                              asyncio.create_task(asyncio.to_thread(shutil.rmtree, media_dir_to_del, ignore_errors=True))
                              logger.info(f"Scheduled deletion of media dir: {media_dir_to_del}")
                 c.execute("DELETE FROM products WHERE city = ?", (city_name,)) # Actual product deletion
                 c.execute("DELETE FROM districts WHERE city_id = ?", (city_id_int,))
                 delete_city_result = c.execute("DELETE FROM cities WHERE id = ?", (city_id_int,))
                 if delete_city_result.rowcount > 0:
                     conn.commit(); load_all_data()
                     success_msg = f"✅ City '{city_name}' and contents deleted!"
                     next_callback = "adm_manage_cities"
                 else: conn.rollback(); success_msg = f"❌ Error: City '{city_name}' not found."
             else: conn.rollback(); success_msg = "❌ Error: City not found (already deleted?)."
        # --- Delete District Logic ---
        elif action_type == "remove_district":
             if len(action_params) < 2: raise ValueError("Missing city/dist_id")
             city_id_str, dist_id_str = action_params[0], action_params[1]
             city_id_int, dist_id_int = int(city_id_str), int(dist_id_str)
             city_name = CITIES.get(city_id_str)
             c.execute("SELECT name FROM districts WHERE id = ? AND city_id = ?", (dist_id_int, city_id_int))
             dist_res = c.fetchone(); district_name = dist_res['name'] if dist_res else None # Use column name
             if city_name and district_name:
                 c.execute("SELECT id FROM products WHERE city = ? AND district = ?", (city_name, district_name))
                 product_ids_to_delete = [row['id'] for row in c.fetchall()] # Use column name
                 logger.info(f"Admin Action (remove_district): Deleting district '{district_name}' in '{city_name}'. Associated product IDs to be deleted: {product_ids_to_delete}")
                 if product_ids_to_delete:
                     placeholders = ','.join('?' * len(product_ids_to_delete))
                     c.execute(f"DELETE FROM product_media WHERE product_id IN ({placeholders})", product_ids_to_delete)
                     for pid in product_ids_to_delete:
                          media_dir_to_del = os.path.join(MEDIA_DIR, str(pid))
                          if await asyncio.to_thread(os.path.exists, media_dir_to_del):
                              asyncio.create_task(asyncio.to_thread(shutil.rmtree, media_dir_to_del, ignore_errors=True))
                              logger.info(f"Scheduled deletion of media dir: {media_dir_to_del}")
                 c.execute("DELETE FROM products WHERE city = ? AND district = ?", (city_name, district_name)) # Actual product deletion
                 delete_dist_result = c.execute("DELETE FROM districts WHERE id = ? AND city_id = ?", (dist_id_int, city_id_int))
                 if delete_dist_result.rowcount > 0:
                     conn.commit(); load_all_data()
                     success_msg = f"✅ District '{district_name}' removed from {city_name}!"
                     next_callback = f"adm_manage_districts_city|{city_id_str}"
                 else: conn.rollback(); success_msg = f"❌ Error: District '{district_name}' not found."
             else: conn.rollback(); success_msg = "❌ Error: City or District not found."
        # --- Delete Product Logic ---
        elif action_type == "confirm_remove_product":
             if not action_params: raise ValueError("Missing product_id")
             product_id = int(action_params[0])
             c.execute("SELECT ci.id as city_id, di.id as dist_id, p.product_type FROM products p LEFT JOIN cities ci ON p.city = ci.name LEFT JOIN districts di ON p.district = di.name AND ci.id = di.city_id WHERE p.id = ?", (product_id,))
             back_details_tuple = c.fetchone() # Result is already a Row object
             logger.info(f"Admin Action (confirm_remove_product): Deleting product ID {product_id}")
             c.execute("DELETE FROM product_media WHERE product_id = ?", (product_id,))
             delete_prod_result = c.execute("DELETE FROM products WHERE id = ?", (product_id,)) # Actual product deletion
             if delete_prod_result.rowcount > 0:
                  conn.commit()
                  success_msg = f"✅ Product ID {product_id} removed!"
                  media_dir_to_delete = os.path.join(MEDIA_DIR, str(product_id))
                  if await asyncio.to_thread(os.path.exists, media_dir_to_delete):
                       asyncio.create_task(asyncio.to_thread(shutil.rmtree, media_dir_to_delete, ignore_errors=True))
                       logger.info(f"Scheduled deletion of media dir: {media_dir_to_delete}")
                  if back_details_tuple and all([back_details_tuple['city_id'], back_details_tuple['dist_id'], back_details_tuple['product_type']]):
                      next_callback = f"adm_manage_products_type|{back_details_tuple['city_id']}|{back_details_tuple['dist_id']}|{back_details_tuple['product_type']}" # Use column names
                  else: next_callback = "adm_manage_products"
             else: conn.rollback(); success_msg = f"❌ Error: Product ID {product_id} not found."
        # --- Safe Delete Product Type Logic ---
        elif action_type == "delete_type":
              if not action_params: raise ValueError("Missing type_name")
              type_name = action_params[0]
              c.execute("SELECT COUNT(*) FROM products WHERE product_type = ?", (type_name,))
              product_count = c.fetchone()[0]
              c.execute("SELECT COUNT(*) FROM reseller_discounts WHERE product_type = ?", (type_name,))
              reseller_discount_count = c.fetchone()[0]
              if product_count == 0 and reseller_discount_count == 0:
                  delete_type_result = c.execute("DELETE FROM product_types WHERE name = ?", (type_name,))
                  if delete_type_result.rowcount > 0:
                       conn.commit(); load_all_data()
                       success_msg = f"✅ Type '{type_name}' deleted!"
                       next_callback = "adm_manage_types"
                  else: conn.rollback(); success_msg = f"❌ Error: Type '{type_name}' not found."
              else:
                  conn.rollback();
                  error_msg_parts = []
                  if product_count > 0: error_msg_parts.append(f"{product_count} product(s)")
                  if reseller_discount_count > 0: error_msg_parts.append(f"{reseller_discount_count} reseller discount rule(s)")
                  usage_details = " and ".join(error_msg_parts)
                  success_msg = f"❌ Error: Cannot delete type '{type_name}' as it is used by {usage_details}."
                  next_callback = "adm_manage_types"
        # --- Force Delete Product Type Logic (CASCADE) ---
        elif action_type == "force_delete_type_CASCADE":
            if not action_params: raise ValueError("Missing type_name for force delete")
            type_name = action_params[0]
            # Clean up the user_data entry now that we are processing it
            user_specific_data.pop('force_delete_type_name', None)
            logger.warning(f"Admin {user_id} initiated FORCE DELETE for type '{type_name}' and all associated data.")

            c.execute("SELECT id FROM products WHERE product_type = ?", (type_name,))
            product_ids_to_delete_media_for = [row['id'] for row in c.fetchall()]

            if product_ids_to_delete_media_for:
                placeholders = ','.join('?' * len(product_ids_to_delete_media_for))
                c.execute(f"DELETE FROM product_media WHERE product_id IN ({placeholders})", product_ids_to_delete_media_for)
                logger.info(f"Force delete: Deleted media entries for {len(product_ids_to_delete_media_for)} products of type '{type_name}'.")
                for pid in product_ids_to_delete_media_for:
                    media_dir_to_del = os.path.join(MEDIA_DIR, str(pid))
                    if await asyncio.to_thread(os.path.exists, media_dir_to_del):
                        asyncio.create_task(asyncio.to_thread(shutil.rmtree, media_dir_to_del, ignore_errors=True))
                        logger.info(f"Force delete: Scheduled deletion of media dir: {media_dir_to_del}")

            delete_products_res = c.execute("DELETE FROM products WHERE product_type = ?", (type_name,))
            products_deleted_count = delete_products_res.rowcount if delete_products_res else 0
            delete_discounts_res = c.execute("DELETE FROM reseller_discounts WHERE product_type = ?", (type_name,))
            discounts_deleted_count = delete_discounts_res.rowcount if delete_discounts_res else 0
            delete_type_res = c.execute("DELETE FROM product_types WHERE name = ?", (type_name,))

            if delete_type_res.rowcount > 0:
                conn.commit(); load_all_data()
                log_admin_action(admin_id=user_id, action="PRODUCT_TYPE_FORCE_DELETE",
                                 reason=f"Type: '{type_name}'. Deleted {products_deleted_count} products, {discounts_deleted_count} discount rules.",
                                 old_value=type_name)
                success_msg = (f"💣 Type '{type_name}' and all associated data FORCE DELETED.\n"
                               f"Deleted: {products_deleted_count} products, {discounts_deleted_count} discount rules.")
            else:
                conn.rollback()
                success_msg = f"❌ Error: Type '{type_name}' not found during final delete step. It might have been deleted already or partial changes occurred."
            next_callback = "adm_manage_types"
        # --- Product Type Reassignment Logic ---
        elif action_type == "confirm_reassign_type":
            if len(action_params) < 2: raise ValueError("Missing old_type_name or new_type_name for reassign")
            old_type_name, new_type_name = action_params[0], action_params[1]
            load_all_data()

            if old_type_name == new_type_name:
                success_msg = "❌ Error: Old and new type names cannot be the same."
                next_callback = "adm_reassign_type_start"
            elif not (old_type_name in PRODUCT_TYPES and new_type_name in PRODUCT_TYPES):
                success_msg = "❌ Error: One or both product types not found. Ensure they exist."
                next_callback = "adm_reassign_type_start"
            else:
                logger.info(f"Admin {user_id} confirmed reassignment from '{old_type_name}' to '{new_type_name}'.")
                update_products_res = c.execute("UPDATE products SET product_type = ? WHERE product_type = ?", (new_type_name, old_type_name))
                products_reassigned = update_products_res.rowcount if update_products_res else 0
                reseller_reassigned = 0
                try:
                    update_reseller_res = c.execute("UPDATE reseller_discounts SET product_type = ? WHERE product_type = ?", (new_type_name, old_type_name))
                    reseller_reassigned = update_reseller_res.rowcount if update_reseller_res else 0
                except sqlite3.IntegrityError as ie:
                    logger.warning(f"IntegrityError reassigning reseller_discounts from '{old_type_name}' to '{new_type_name}': {ie}. Deleting old conflicting rules.")
                    delete_conflicting_reseller_rules = c.execute("DELETE FROM reseller_discounts WHERE product_type = ?", (old_type_name,))
                    reseller_reassigned = delete_conflicting_reseller_rules.rowcount if delete_conflicting_reseller_rules else 0
                    logger.info(f"Deleted {reseller_reassigned} discount rules for old type '{old_type_name}' due to conflict on reassign.")

                delete_type_res = c.execute("DELETE FROM product_types WHERE name = ?", (old_type_name,))
                type_deleted = delete_type_res.rowcount > 0

                if type_deleted:
                    conn.commit(); load_all_data()
                    log_admin_action(admin_id=user_id, action=ACTION_PRODUCT_TYPE_REASSIGN,
                                     reason=f"From '{old_type_name}' to '{new_type_name}'. Reassigned {products_reassigned} products, affected {reseller_reassigned} discount entries.",
                                     old_value=old_type_name, new_value=new_type_name)
                    success_msg = (f"✅ Type '{old_type_name}' reassigned to '{new_type_name}' and deleted.\n"
                                   f"Reassigned: {products_reassigned} products. Affected discount entries: {reseller_reassigned}.")
                else:
                    conn.rollback()
                    success_msg = f"❌ Error: Could not delete old type '{old_type_name}'. No changes made."
                next_callback = "adm_manage_types"
        # --- Delete General Discount Code Logic ---
        elif action_type == "delete_discount":
             if not action_params: raise ValueError("Missing discount_id")
             code_id = int(action_params[0])
             c.execute("SELECT code FROM discount_codes WHERE id = ?", (code_id,))
             code_res = c.fetchone(); code_text = code_res['code'] if code_res else f"ID {code_id}"
             delete_disc_result = c.execute("DELETE FROM discount_codes WHERE id = ?", (code_id,))
             if delete_disc_result.rowcount > 0:
                 conn.commit(); success_msg = f"✅ Discount code {code_text} deleted!"
                 next_callback = "adm_manage_discounts"
             else: conn.rollback(); success_msg = f"❌ Error: Discount code {code_text} not found."
        # --- Delete Review Logic ---
        elif action_type == "delete_review":
            if not action_params: raise ValueError("Missing review_id")
            review_id = int(action_params[0])
            delete_rev_result = c.execute("DELETE FROM reviews WHERE review_id = ?", (review_id,))
            if delete_rev_result.rowcount > 0:
                conn.commit(); success_msg = f"✅ Review ID {review_id} deleted!"
                next_callback = "adm_manage_reviews|0"
            else: conn.rollback(); success_msg = f"❌ Error: Review ID {review_id} not found."
        # <<< Welcome Message Delete Logic >>>
        elif action_type == "delete_welcome_template":
            if not action_params: raise ValueError("Missing template_name")
            name_to_delete = action_params[0]
            delete_wm_result = c.execute("DELETE FROM welcome_messages WHERE name = ?", (name_to_delete,))
            if delete_wm_result.rowcount > 0:
                 conn.commit(); success_msg = f"✅ Welcome template '{name_to_delete}' deleted!"
                 next_callback = "adm_manage_welcome|0"
            else: conn.rollback(); success_msg = f"❌ Error: Welcome template '{name_to_delete}' not found."
        # <<< Reset Welcome Message Logic >>>
        elif action_type == "reset_default_welcome":
            try:
                built_in_text = LANGUAGES['en']['welcome']
                c.execute("UPDATE welcome_messages SET template_text = ? WHERE name = ?", (built_in_text, "default"))
                c.execute("INSERT OR REPLACE INTO bot_settings (setting_key, setting_value) VALUES (?, ?)",
                          ("active_welcome_message_name", "default"))
                conn.commit(); success_msg = "✅ 'default' welcome template reset and activated."
            except Exception as reset_e:
                 conn.rollback(); logger.error(f"Error resetting default welcome message: {reset_e}", exc_info=True)
                 success_msg = "❌ Error resetting default template."
            next_callback = "adm_manage_welcome|0"
        # <<< Delete Reseller Discount Rule Logic >>>
        elif action_type == "confirm_delete_reseller_discount":
            if len(action_params) < 2: raise ValueError("Missing reseller_id or product_type")
            try:
                reseller_id = int(action_params[0]); product_type = action_params[1]
                c.execute("SELECT discount_percentage FROM reseller_discounts WHERE reseller_user_id = ? AND product_type = ?", (reseller_id, product_type))
                old_res = c.fetchone(); old_value = old_res['discount_percentage'] if old_res else None
                delete_res_result = c.execute("DELETE FROM reseller_discounts WHERE reseller_user_id = ? AND product_type = ?", (reseller_id, product_type))
                if delete_res_result.rowcount > 0:
                    conn.commit(); log_admin_action(user_id, ACTION_RESELLER_DISCOUNT_DELETE, reseller_id, reason=f"Type: {product_type}", old_value=old_value)
                    success_msg = f"✅ Reseller discount rule deleted for {product_type}."
                else: conn.rollback(); success_msg = f"❌ Error: Reseller discount rule for {product_type} not found."
                next_callback = f"reseller_manage_specific|{reseller_id}"
            except (ValueError, IndexError) as param_err:
                conn.rollback(); logger.error(f"Invalid params for delete reseller discount: {action_params} - {param_err}")
                success_msg = "❌ Klaida apdorojant užklausą."; next_callback = "admin_menu"
        # <<< Clear All Reservations Logic >>>
        elif action_type == "clear_all_reservations":
            logger.warning(f"ADMIN ACTION: Admin {user_id} is clearing ALL reservations and baskets.")
            update_products_res = c.execute("UPDATE products SET reserved = 0 WHERE reserved > 0")
            products_cleared = update_products_res.rowcount if update_products_res else 0
            update_users_res = c.execute("UPDATE users SET basket = '' WHERE basket IS NOT NULL AND basket != ''")
            baskets_cleared = update_users_res.rowcount if update_users_res else 0
            conn.commit()
            log_admin_action(admin_id=user_id, action="CLEAR_ALL_RESERVATIONS", reason=f"Cleared {products_cleared} reservations and {baskets_cleared} user baskets.")
            success_msg = f"✅ Cleared {products_cleared} product reservations and emptied {baskets_cleared} user baskets."
            next_callback = "admin_menu"
        else:
            logger.error(f"Unknown confirmation action type: {action_type}")
            conn.rollback(); success_msg = "❌ Unknown action confirmed."
            next_callback = "admin_menu"

        try: await query.edit_message_text(success_msg, parse_mode=None)
        except telegram_error.BadRequest: pass

        keyboard = [[InlineKeyboardButton("⬅️ Atgal", callback_data=next_callback)]]
        await send_message_with_retry(context.bot, chat_id, "Action complete. What next?", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except (sqlite3.Error, ValueError, OSError, Exception) as e:
        logger.error(f"Error executing confirmed action '{action}': {e}", exc_info=True)
        if conn and conn.in_transaction: conn.rollback()
        error_text = str(e)
        try: await query.edit_message_text(f"❌ An error occurred: {error_text}", parse_mode=None)
        except Exception as edit_err: logger.error(f"Failed to edit message with error: {edit_err}")
    finally:
        if conn: conn.close()
        # Clean up specific user_data keys used by certain flows after confirmation
        if action_type.startswith("force_delete_type_CASCADE"):
            user_specific_data.pop('force_delete_type_name', None)
        elif action_type.startswith("confirm_reassign_type"):
            user_specific_data.pop('reassign_old_type_name', None)
            user_specific_data.pop('reassign_new_type_name', None)

async def handle_adm_edit_welcome_text(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Initiates editing the template text."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params: return await query.answer("Error: Template name missing.", show_alert=True)

    template_name = params[0]
    offset = context.user_data.get('editing_welcome_offset', 0) # Get offset from context
    lang, lang_data = _get_lang_data(context) # Use helper

    # Fetch current text to show in prompt
    current_text = ""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT template_text FROM welcome_messages WHERE name = ?", (template_name,))
        row = c.fetchone()
        if row: current_text = row['template_text']
    except sqlite3.Error as e: logger.error(f"DB error fetching text for edit: {e}")
    finally:
         if conn: conn.close()

    context.user_data['state'] = 'awaiting_welcome_template_edit' # Reusing state, but specifically for text
    context.user_data['editing_welcome_template_name'] = template_name # Ensure it's set
    context.user_data['editing_welcome_field'] = 'text' # Indicate we are editing text

    placeholders = "{username}, {status}, {progress_bar}, {balance_str}, {purchases}, {basket_count}" # Plain text placeholders
    prompt_template = lang_data.get("welcome_edit_text_prompt", "Editing Text for '{name}'. Current text:\n\n{current_text}\n\nPlease reply with the new text. Available placeholders:\n{placeholders}")
    # Display plain text
    prompt = prompt_template.format(
        name=template_name,
        current_text=current_text,
        placeholders=placeholders
    )
    if len(prompt) > 4000: prompt = prompt[:4000] + "\n[... Current text truncated ...]"

    # Go back to the specific template's edit menu
    keyboard = [[InlineKeyboardButton("❌ Cancel Edit", callback_data=f"adm_edit_welcome|{template_name}|{offset}")]]
    try:
        await query.edit_message_text(prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Error editing edit text prompt: {e}")
        else: await query.answer()
    await query.answer("Enter new template text.")

async def handle_adm_edit_welcome_desc(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Initiates editing the template description."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params: return await query.answer("Error: Template name missing.", show_alert=True)

    template_name = params[0]
    offset = context.user_data.get('editing_welcome_offset', 0)
    lang, lang_data = _get_lang_data(context) # Use helper

    # Fetch current description
    current_desc = ""
    conn = None
    try:
        conn = get_db_connection(); c = conn.cursor()
        c.execute("SELECT description FROM welcome_messages WHERE name = ?", (template_name,))
        row = c.fetchone(); current_desc = row['description'] or ""
    except sqlite3.Error as e: logger.error(f"DB error fetching desc for edit: {e}")
    finally:
        if conn: conn.close()

    context.user_data['state'] = 'awaiting_welcome_description_edit' # New state for description edit
    context.user_data['editing_welcome_template_name'] = template_name # Ensure it's set
    context.user_data['editing_welcome_field'] = 'description' # Indicate we are editing description

    prompt_template = lang_data.get("welcome_edit_description_prompt", "Editing description for '{name}'. Current: '{current_desc}'.\n\nEnter new description or send '-' to skip.")
    prompt = prompt_template.format(name=template_name, current_desc=current_desc or "Not set")

    keyboard = [[InlineKeyboardButton("❌ Cancel Edit", callback_data=f"adm_edit_welcome|{template_name}|{offset}")]]
    await query.edit_message_text(prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter new description.")

async def handle_adm_delete_welcome_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirms deletion of a welcome message template."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params or len(params) < 2 or not params[1].isdigit():
         return await query.answer("Error: Template name or offset missing.", show_alert=True)

    template_name = params[0]
    offset = int(params[1])
    lang, lang_data = _get_lang_data(context) # Use helper

    # Fetch current active template
    conn = None
    active_template_name = "default"
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT setting_value FROM bot_settings WHERE setting_key = ?", ("active_welcome_message_name",))
        row = c.fetchone(); active_template_name = row['setting_value'] if row else "default" # Use column name
    except sqlite3.Error as e: logger.error(f"DB error checking template status for delete: {e}")
    finally:
         if conn: conn.close()

    if template_name == "default":
        await query.answer("Cannot delete the 'default' template.", show_alert=True)
        return await handle_adm_manage_welcome(update, context, params=[str(offset)])

    # <<< Improvement: Prevent deleting the active template >>>
    if template_name == active_template_name:
        cannot_delete_msg = lang_data.get("welcome_cannot_delete_active", "❌ Cannot delete the active template. Activate another first.")
        await query.answer(cannot_delete_msg, show_alert=True)
        return await handle_adm_manage_welcome(update, context, params=[str(offset)]) # Refresh list

    context.user_data["confirm_action"] = f"delete_welcome_template|{template_name}"
    title = lang_data.get("welcome_delete_confirm_title", "⚠️ Confirm Deletion")
    text_template = lang_data.get("welcome_delete_confirm_text", "Are you sure you want to delete the welcome message template named '{name}'?")
    msg = f"{title}\n\n{text_template.format(name=template_name)}"

    keyboard = [
        [InlineKeyboardButton(lang_data.get("welcome_delete_button_yes", "✅ Yes, Delete Template"), callback_data="confirm_yes")],
        [InlineKeyboardButton("❌ No, Cancel", callback_data=f"adm_manage_welcome|{offset}")]
    ]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

# <<< Reset Default Welcome Handler >>>
async def handle_reset_default_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirms resetting the 'default' template to the built-in text and activating it."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    lang, lang_data = _get_lang_data(context)

    context.user_data["confirm_action"] = "reset_default_welcome"
    title = lang_data.get("welcome_reset_confirm_title", "⚠️ Confirm Reset")
    text = lang_data.get("welcome_reset_confirm_text", "Are you sure you want to reset the text of the 'default' template to the built-in version and activate it?")
    msg = f"{title}\n\n{text}"

    keyboard = [
        [InlineKeyboardButton(lang_data.get("welcome_reset_button_yes", "✅ Yes, Reset & Activate"), callback_data="confirm_yes")],
        [InlineKeyboardButton("❌ No, Cancel", callback_data="adm_manage_welcome|0")]
    ]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


# --- Welcome Message Management Handlers --- END


# --- Welcome Message Message Handlers ---

async def handle_adm_welcome_template_name_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text reply when state is 'awaiting_welcome_template_name'."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(user_id): return
    if not update.message or not update.message.text: return
    if context.user_data.get("state") != "awaiting_welcome_template_name": return
    
    template_name = update.message.text.strip()
    if not template_name:
        return await send_message_with_retry(context.bot, chat_id, "Template name cannot be empty.", parse_mode=None)
    
    if len(template_name) > 50:
        return await send_message_with_retry(context.bot, chat_id, "Template name too long (max 50 characters).", parse_mode=None)

    # Check if template name already exists
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT 1 FROM welcome_messages WHERE name = ?", (template_name,))
        if c.fetchone():
            lang, lang_data = _get_lang_data(context)
            error_msg = lang_data.get("welcome_add_name_exists", "❌ Error: A template with the name '{name}' already exists.")
            await send_message_with_retry(context.bot, chat_id, error_msg.format(name=template_name), parse_mode=None)
            return
    except sqlite3.Error as e:
        logger.error(f"DB error checking template name '{template_name}': {e}")
        await send_message_with_retry(context.bot, chat_id, "❌ Database error checking template name.", parse_mode=None)
        return
    finally:
        if conn: conn.close()

    # Set up for text input
    context.user_data['state'] = 'awaiting_welcome_template_text'
    context.user_data['pending_welcome_template'] = {
        'name': template_name,
        'is_editing': False,
        'offset': 0
    }

    lang, lang_data = _get_lang_data(context)
    placeholders = "{username}, {status}, {progress_bar}, {balance_str}, {purchases}, {basket_count}"
    prompt_template = lang_data.get("welcome_add_text_prompt", "Template Name: {name}\n\nPlease reply with the full welcome message text. Available placeholders:\n`{placeholders}`")
    prompt = prompt_template.format(name=template_name, placeholders=placeholders)
    
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="adm_manage_welcome|0")]]
    await send_message_with_retry(context.bot, chat_id, prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_welcome_template_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text reply when state is 'awaiting_welcome_template_text' or 'awaiting_welcome_template_edit'."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(user_id): return
    if not update.message or not update.message.text: return
    
    state = context.user_data.get("state")
    if state not in ["awaiting_welcome_template_text", "awaiting_welcome_template_edit"]: 
        return
    
    template_text = update.message.text.strip()
    if not template_text:
        return await send_message_with_retry(context.bot, chat_id, "Template text cannot be empty.", parse_mode=None)

    if state == "awaiting_welcome_template_text":
        # Adding new template - get data from pending template
        pending_template = context.user_data.get("pending_welcome_template")
        if not pending_template or not pending_template.get("name"):
            await send_message_with_retry(context.bot, chat_id, "❌ Error: Template data lost. Please start again.", parse_mode=None)
            context.user_data.pop("state", None)
            return

        # Update pending template with text and move to description input
        pending_template['text'] = template_text
        context.user_data['state'] = 'awaiting_welcome_description'
        
        lang, lang_data = _get_lang_data(context)
        prompt = lang_data.get("welcome_add_description_prompt", "Optional: Enter a short description for this template (admin view only). Send '-' to skip.")
        keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="adm_manage_welcome|0")]]
        await send_message_with_retry(context.bot, chat_id, prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
    elif state == "awaiting_welcome_template_edit":
        # Editing existing template text
        template_name = context.user_data.get('editing_welcome_template_name')
        offset = context.user_data.get('editing_welcome_offset', 0)
        
        if not template_name:
            await send_message_with_retry(context.bot, chat_id, "❌ Error: Template name lost. Please start again.", parse_mode=None)
            context.user_data.pop("state", None)
            return

        # Get current description to preserve it
        current_description = None
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT description FROM welcome_messages WHERE name = ?", (template_name,))
            row = c.fetchone()
            if row:
                current_description = row['description']
        except sqlite3.Error as e:
            logger.error(f"DB error fetching description for '{template_name}': {e}")
        finally:
            if conn: conn.close()

        # Set up for preview
        context.user_data['pending_welcome_template'] = {
            'name': template_name,
            'text': template_text,
            'description': current_description,
            'is_editing': True,
            'offset': offset
        }
        
        # Show preview
        await _show_welcome_preview(update, context)

async def handle_adm_welcome_description_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text reply when state is 'awaiting_welcome_description' or 'awaiting_welcome_description_edit'."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(user_id): return
    if not update.message or not update.message.text: return
    
    state = context.user_data.get("state")
    if state not in ["awaiting_welcome_description", "awaiting_welcome_description_edit"]: 
        return
    
    description_text = update.message.text.strip()
    description = None if description_text == "-" else description_text
    
    if state == "awaiting_welcome_description":
        # Adding new template - finalize and show preview
        pending_template = context.user_data.get("pending_welcome_template")
        if not pending_template or not pending_template.get("name") or not pending_template.get("text"):
            await send_message_with_retry(context.bot, chat_id, "❌ Error: Template data lost. Please start again.", parse_mode=None)
            context.user_data.pop("state", None)
            return

        pending_template['description'] = description
        await _show_welcome_preview(update, context)
        
    elif state == "awaiting_welcome_description_edit":
        # Editing existing template description
        template_name = context.user_data.get('editing_welcome_template_name')
        offset = context.user_data.get('editing_welcome_offset', 0)
        
        if not template_name:
            await send_message_with_retry(context.bot, chat_id, "❌ Error: Template name lost. Please start again.", parse_mode=None)
            context.user_data.pop("state", None)
            return

        # Get current text to preserve it
        current_text = None
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT template_text FROM welcome_messages WHERE name = ?", (template_name,))
            row = c.fetchone()
            if row:
                current_text = row['template_text']
        except sqlite3.Error as e:
            logger.error(f"DB error fetching text for '{template_name}': {e}")
        finally:
            if conn: conn.close()

        if not current_text:
            await send_message_with_retry(context.bot, chat_id, "❌ Error: Could not load current template text.", parse_mode=None)
            context.user_data.pop("state", None)
            return

        # Set up for preview
        context.user_data['pending_welcome_template'] = {
            'name': template_name,
            'text': current_text,
            'description': description,
            'is_editing': True,
            'offset': offset
        }
        
        # Show preview
        await _show_welcome_preview(update, context)

# --- Welcome Message Message Handlers --- END


# --- Welcome Message Preview & Save Handlers --- START

async def _show_welcome_preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows a preview of the welcome message with dummy data."""
    query = update.callback_query # Could be None if called from message handler
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    lang, lang_data = _get_lang_data(context)

    pending_template = context.user_data.get("pending_welcome_template")
    if not pending_template or not pending_template.get("name"): # Need at least name
        logger.error("Attempted to show welcome preview, but pending data missing.")
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Preview data lost.", parse_mode=None)
        context.user_data.pop("state", None)
        context.user_data.pop("pending_welcome_template", None)
        # Attempt to go back to the management menu
        if query:
             await handle_adm_manage_welcome(update, context, params=["0"])
        return
    
    template_name = pending_template['name']
    template_text = pending_template.get('text', '') # Use get with fallback
    template_description = pending_template.get('description', 'Not set')
    is_editing = pending_template.get('is_editing', False)
    offset = pending_template.get('offset', 0)

    # Dummy data for formatting
    dummy_username = update.effective_user.first_name or "Admin"
    dummy_status = "VIP 👑"
    dummy_progress = get_progress_bar(10)
    dummy_balance = format_currency(123.45)
    dummy_purchases = 15
    dummy_basket = 2
    preview_text_raw = "_(Formatting Error)_" # Fallback preview

    try:
        # Format using the raw username and placeholders
        preview_text_raw = template_text.format(
            username=dummy_username,
            status=dummy_status,
            progress_bar=dummy_progress,
            balance_str=dummy_balance,
            purchases=dummy_purchases,
            basket_count=dummy_basket
        ) # Keep internal markdown

    except KeyError as e:
        logger.warning(f"KeyError formatting welcome preview for '{template_name}': {e}")
        err_msg_template = lang_data.get("welcome_invalid_placeholder", "⚠️ Formatting Error! Missing placeholder: `{key}`\n\nRaw Text:\n{text}")
        preview_text_raw = err_msg_template.format(key=e, text=template_text[:500]) # Show raw text in case of error
    except Exception as format_e:
        logger.error(f"Unexpected error formatting preview: {format_e}")
        err_msg_template = lang_data.get("welcome_formatting_error", "⚠️ Unexpected Formatting Error!\n\nRaw Text:\n{text}")
        preview_text_raw = err_msg_template.format(text=template_text[:500])

    # Prepare display message (plain text)
    title = lang_data.get("welcome_preview_title", "--- Welcome Message Preview ---")
    name_label = lang_data.get("welcome_preview_name", "Name")
    desc_label = lang_data.get("welcome_preview_desc", "Desc")
    confirm_prompt = lang_data.get("welcome_preview_confirm", "Save this template?")

    msg = f"{title}\n\n"
    msg += f"{name_label}: {template_name}\n"
    msg += f"{desc_label}: {template_description or 'Not set'}\n"
    msg += f"---\n"
    msg += f"{preview_text_raw}\n" # Display the formatted (and potentially error) message raw
    msg += f"---\n"
    msg += f"\n{confirm_prompt}"

    # Set state for confirmation callback
    context.user_data['state'] = 'awaiting_welcome_confirmation'

    # Go back to the specific template edit menu if editing, or manage menu if adding
    cancel_callback = f"adm_edit_welcome|{template_name}|{offset}" if is_editing else f"adm_manage_welcome|{offset}"

    keyboard = [
        [InlineKeyboardButton(lang_data.get("welcome_button_save", "💾 Save Template"), callback_data=f"confirm_save_welcome")],
        [InlineKeyboardButton("❌ Atšaukti", callback_data=cancel_callback)]
    ]

    # Send or edit the message (using plain text)
    message_to_edit = query.message if query else None
    if message_to_edit:
        try:
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except telegram_error.BadRequest as e:
             if "message is not modified" not in str(e).lower():
                 logger.error(f"Error editing preview message: {e}")
                 # Send as new message if edit fails
                 await send_message_with_retry(context.bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
             else: await query.answer() # Ignore modification error
    else:
        # Send as new message if no original message to edit
        await send_message_with_retry(context.bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    if query:
        await query.answer()

# <<< NEW >>>
async def handle_confirm_save_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the 'Save Template' button after preview."""
    query = update.callback_query
    user_id = query.from_user.id
    if not is_primary_admin(user_id): return await query.answer("Access Denied.", show_alert=True)
    if context.user_data.get("state") != 'awaiting_welcome_confirmation':
        logger.warning("handle_confirm_save_welcome called in wrong state.")
        return await query.answer("Invalid state.", show_alert=True)

    pending_template = context.user_data.get("pending_welcome_template")
    if not pending_template or not pending_template.get("name") or pending_template.get("text") is None: # Text can be empty, but key must exist
        logger.error("Attempted to save welcome template, but pending data missing.")
        await query.edit_message_text("❌ Error: Save data lost. Please start again.", parse_mode=None)
        context.user_data.pop("state", None)
        context.user_data.pop("pending_welcome_template", None)
        return

    template_name = pending_template['name']
    template_text = pending_template['text']
    template_description = pending_template.get('description') # Can be None
    is_editing = pending_template.get('is_editing', False)
    offset = pending_template.get('offset', 0)
    lang, lang_data = _get_lang_data(context) # Use helper

    # Perform the actual save operation
    success = False
    if is_editing:
        success = update_welcome_message_template(template_name, template_text, template_description)
        msg_template = lang_data.get("welcome_edit_success", "✅ Template '{name}' updated.") if success else lang_data.get("welcome_edit_fail", "❌ Failed to update template '{name}'.")
    else:
        success = add_welcome_message_template(template_name, template_text, template_description)
        msg_template = lang_data.get("welcome_add_success", "✅ Welcome message template '{name}' added.") if success else lang_data.get("welcome_add_fail", "❌ Failed to add welcome message template.")

    # Clean up context
    context.user_data.pop("state", None)
    context.user_data.pop("pending_welcome_template", None)

    await query.edit_message_text(msg_template.format(name=template_name), parse_mode=None)

    # Go back to the management list
    await handle_adm_manage_welcome(update, context, params=[str(offset)])


# --- Welcome Message Management Handlers --- END


# --- Welcome Message Preview & Save Handlers --- END


# --- Admin Message Handlers (Used when state is set) ---
# --- These handlers are primarily for the core admin flow ---
# --- Reseller state message handlers are defined in reseller_management.py ---

async def handle_adm_add_city_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text reply when state is 'awaiting_new_city_name'."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(user_id): return
    if not update.message or not update.message.text: return
    if context.user_data.get("state") != "awaiting_new_city_name": return
    text = update.message.text.strip()
    if not text: return await send_message_with_retry(context.bot, chat_id, "City name cannot be empty.", parse_mode=None)
    conn = None # Initialize conn
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        c.execute("INSERT INTO cities (name) VALUES (?)", (text,))
        new_city_id = c.lastrowid
        conn.commit()
        load_all_data() # Reload global data
        context.user_data.pop("state", None)
        success_text = f"✅ City '{text}' added successfully!"
        keyboard = [[InlineKeyboardButton("⬅️ Manage Cities", callback_data="adm_manage_cities")]]
        await send_message_with_retry(context.bot, chat_id, success_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except sqlite3.IntegrityError:
        await send_message_with_retry(context.bot, chat_id, f"❌ Error: City '{text}' already exists.", parse_mode=None)
    except sqlite3.Error as e:
        logger.error(f"DB error adding city '{text}': {e}", exc_info=True)
        if conn and conn.in_transaction: conn.rollback()
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Failed to add city.", parse_mode=None)
        context.user_data.pop("state", None)
    finally:
        if conn: conn.close() # Close connection if opened

async def handle_adm_add_district_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text reply when state is 'awaiting_new_district_name'."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(user_id): return
    if not update.message or not update.message.text: return
    if context.user_data.get("state") != "awaiting_new_district_name": return
    text = update.message.text.strip()
    city_id_str = context.user_data.get("admin_add_district_city_id")
    city_name = CITIES.get(city_id_str)
    if not city_id_str or not city_name:
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Could not determine city.", parse_mode=None)
        context.user_data.pop("state", None); context.user_data.pop("admin_add_district_city_id", None)
        return
    if not text: return await send_message_with_retry(context.bot, chat_id, "District name cannot be empty.", parse_mode=None)
    conn = None # Initialize conn
    try:
        city_id_int = int(city_id_str)
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        c.execute("INSERT INTO districts (city_id, name) VALUES (?, ?)", (city_id_int, text))
        conn.commit()
        load_all_data() # Reload global data
        context.user_data.pop("state", None); context.user_data.pop("admin_add_district_city_id", None)
        success_text = f"✅ District '{text}' added to {city_name}!"
        keyboard = [[InlineKeyboardButton("⬅️ Manage Districts", callback_data=f"adm_manage_districts_city|{city_id_str}")]]
        await send_message_with_retry(context.bot, chat_id, success_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except sqlite3.IntegrityError:
        await send_message_with_retry(context.bot, chat_id, f"❌ Error: District '{text}' already exists in {city_name}.", parse_mode=None)
    except (sqlite3.Error, ValueError) as e:
        logger.error(f"DB/Value error adding district '{text}' to city {city_id_str}: {e}", exc_info=True)
        if conn and conn.in_transaction: conn.rollback()
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Failed to add district.", parse_mode=None)
        context.user_data.pop("state", None); context.user_data.pop("admin_add_district_city_id", None)
    finally:
        if conn: conn.close() # Close connection if opened

async def handle_adm_edit_district_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text reply when state is 'awaiting_edit_district_name'."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(user_id): return
    if not update.message or not update.message.text: return
    if context.user_data.get("state") != "awaiting_edit_district_name": return
    new_name = update.message.text.strip()
    city_id_str = context.user_data.get("edit_city_id")
    dist_id_str = context.user_data.get("edit_district_id")
    city_name = CITIES.get(city_id_str)
    old_district_name = None
    conn = None # Initialize conn
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        # Use column name
        c.execute("SELECT name FROM districts WHERE id = ? AND city_id = ?", (int(dist_id_str), int(city_id_str)))
        res = c.fetchone(); old_district_name = res['name'] if res else None
    except (sqlite3.Error, ValueError) as e: logger.error(f"Failed to fetch old district name for edit: {e}")
    finally:
        if conn: conn.close() # Close connection if opened
    if not city_id_str or not dist_id_str or not city_name or old_district_name is None:
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Could not find district/city.", parse_mode=None)
        context.user_data.pop("state", None); context.user_data.pop("edit_city_id", None); context.user_data.pop("edit_district_id", None)
        return
    if not new_name: return await send_message_with_retry(context.bot, chat_id, "New district name cannot be empty.", parse_mode=None)
    if new_name == old_district_name:
        await send_message_with_retry(context.bot, chat_id, "New name is the same. No changes.", parse_mode=None)
        context.user_data.pop("state", None); context.user_data.pop("edit_city_id", None); context.user_data.pop("edit_district_id", None)
        keyboard = [[InlineKeyboardButton("⬅️ Manage Districts", callback_data=f"adm_manage_districts_city|{city_id_str}")]]
        return await send_message_with_retry(context.bot, chat_id, "No changes detected.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    conn = None # Re-initialize for update transaction
    try:
        city_id_int, dist_id_int = int(city_id_str), int(dist_id_str)
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        c.execute("BEGIN")
        c.execute("UPDATE districts SET name = ? WHERE id = ? AND city_id = ?", (new_name, dist_id_int, city_id_int))
        # Update products table as well
        c.execute("UPDATE products SET district = ? WHERE district = ? AND city = ?", (new_name, old_district_name, city_name))
        conn.commit()
        load_all_data() # Reload global data
        context.user_data.pop("state", None); context.user_data.pop("edit_city_id", None); context.user_data.pop("edit_district_id", None)
        success_text = f"✅ District updated to '{new_name}' successfully!"
        keyboard = [[InlineKeyboardButton("⬅️ Manage Districts", callback_data=f"adm_manage_districts_city|{city_id_str}")]]
        await send_message_with_retry(context.bot, chat_id, success_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except sqlite3.IntegrityError:
        await send_message_with_retry(context.bot, chat_id, f"❌ Error: District '{new_name}' already exists.", parse_mode=None)
    except (sqlite3.Error, ValueError) as e:
        logger.error(f"DB/Value error updating district {dist_id_str}: {e}", exc_info=True)
        if conn and conn.in_transaction: conn.rollback()
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Failed to update district.", parse_mode=None)
        context.user_data.pop("state", None); context.user_data.pop("edit_city_id", None); context.user_data.pop("edit_district_id", None)
    finally:
         if conn: conn.close() # Close connection if opened


async def handle_adm_edit_city_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text reply when state is 'awaiting_edit_city_name'."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(user_id): return
    if not update.message or not update.message.text: return
    if context.user_data.get("state") != "awaiting_edit_city_name": return
    new_name = update.message.text.strip()
    city_id_str = context.user_data.get("edit_city_id")
    old_name = None
    conn = None # Initialize conn
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        # Use column name
        c.execute("SELECT name FROM cities WHERE id = ?", (int(city_id_str),))
        res = c.fetchone(); old_name = res['name'] if res else None
    except (sqlite3.Error, ValueError) as e: logger.error(f"Failed to fetch old city name for edit: {e}")
    finally:
        if conn: conn.close() # Close connection if opened
    if not city_id_str or old_name is None:
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Could not find city.", parse_mode=None)
        context.user_data.pop("state", None); context.user_data.pop("edit_city_id", None)
        return
    if not new_name: return await send_message_with_retry(context.bot, chat_id, "New city name cannot be empty.", parse_mode=None)
    if new_name == old_name:
        await send_message_with_retry(context.bot, chat_id, "New name is the same. No changes.", parse_mode=None)
        context.user_data.pop("state", None); context.user_data.pop("edit_city_id", None)
        keyboard = [[InlineKeyboardButton("⬅️ Manage Cities", callback_data="adm_manage_cities")]]
        return await send_message_with_retry(context.bot, chat_id, "No changes detected.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    conn = None # Re-initialize for update transaction
    try:
        city_id_int = int(city_id_str)
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        c.execute("BEGIN")
        c.execute("UPDATE cities SET name = ? WHERE id = ?", (new_name, city_id_int))
        # Update products table as well
        c.execute("UPDATE products SET city = ? WHERE city = ?", (new_name, old_name))
        conn.commit()
        load_all_data() # Reload global data
        context.user_data.pop("state", None); context.user_data.pop("edit_city_id", None)
        success_text = f"✅ City updated to '{new_name}' successfully!"
        keyboard = [[InlineKeyboardButton("⬅️ Manage Cities", callback_data="adm_manage_cities")]]
        await send_message_with_retry(context.bot, chat_id, success_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except sqlite3.IntegrityError:
        await send_message_with_retry(context.bot, chat_id, f"❌ Error: City '{new_name}' already exists.", parse_mode=None)
    except (sqlite3.Error, ValueError) as e:
        logger.error(f"DB/Value error updating city {city_id_str}: {e}", exc_info=True)
        if conn and conn.in_transaction: conn.rollback()
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Failed to update city.", parse_mode=None)
        context.user_data.pop("state", None); context.user_data.pop("edit_city_id", None)
    finally:
         if conn: conn.close() # Close connection if opened


async def handle_adm_custom_size_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text reply when state is 'awaiting_custom_size'."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(user_id): return
    if not update.message or not update.message.text: return
    if context.user_data.get("state") != "awaiting_custom_size": return
    custom_size = update.message.text.strip()
    if not custom_size: return await send_message_with_retry(context.bot, chat_id, "Custom size cannot be empty.", parse_mode=None)
    if len(custom_size) > 50: return await send_message_with_retry(context.bot, chat_id, "Custom size too long (max 50 chars).", parse_mode=None)
    if not all(k in context.user_data for k in ["admin_city", "admin_district", "admin_product_type"]):
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Context lost.", parse_mode=None)
        context.user_data.pop("state", None)
        return
    context.user_data["pending_drop_size"] = custom_size
    context.user_data["state"] = "awaiting_price"
    keyboard = [[InlineKeyboardButton("❌ Cancel Add", callback_data="cancel_add")]]
    await send_message_with_retry(context.bot, chat_id, f"Custom size set to '{custom_size}'. Reply with the price (e.g., 12.50):",
                                  reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_price_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles price input for regular product adding."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(user_id): return
    if not update.message or not update.message.text: return
    if context.user_data.get("state") != "awaiting_price": return
    
    price_text = update.message.text.strip()
    if not price_text:
        return await send_message_with_retry(context.bot, chat_id, "Price cannot be empty.", parse_mode=None)
    
    try:
        price = float(price_text)
        if price <= 0:
            return await send_message_with_retry(context.bot, chat_id, "Price must be greater than 0.", parse_mode=None)
        if price > 10000:
            return await send_message_with_retry(context.bot, chat_id, "Price too high (max 10000).", parse_mode=None)
    except ValueError:
        return await send_message_with_retry(context.bot, chat_id, "Invalid price format. Use numbers like 12.50", parse_mode=None)
    
    # Check required context
    if not all(k in context.user_data for k in ["admin_city", "admin_district", "admin_product_type", "pending_drop_size"]):
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Context lost.", parse_mode=None)
        context.user_data.pop("state", None)
        return
    
    context.user_data["pending_drop_price"] = price
    context.user_data["state"] = "awaiting_drop_details"
    
    await send_message_with_retry(context.bot, chat_id, 
        f"💰 Price set to: {price:.2f}€\n\n"
        "📝 Now please send the product details (description/name) and any media (photos/videos/GIFs).\n\n"
        "You can send text, images, videos, GIFs, or a combination.\n"
        "When finished, send any message with the text 'done' to confirm.", 
        parse_mode=None)

async def display_user_search_results(bot, chat_id: int, user_info: dict):
    """Displays user overview with buttons to view detailed sections."""
    user_id = user_info['user_id']
    username = user_info['username'] or f"ID_{user_id}"
    balance = Decimal(str(user_info['balance']))
    total_purchases = user_info['total_purchases']
    is_banned = user_info['is_banned'] == 1
    is_reseller = user_info['is_reseller'] == 1
    
    # Get user status and progress
    status = get_user_status(total_purchases)
    progress_bar = get_progress_bar(total_purchases)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get counts for different sections
        c.execute("SELECT COUNT(*) as count FROM purchases WHERE user_id = ?", (user_id,))
        total_purchases_count = c.fetchone()['count']
        
        c.execute("SELECT COUNT(*) as count FROM pending_deposits WHERE user_id = ?", (user_id,))
        pending_deposits_count = c.fetchone()['count']
        
        c.execute("SELECT COUNT(*) as count FROM admin_log WHERE target_user_id = ?", (user_id,))
        admin_actions_count = c.fetchone()['count']
        
        # Calculate total spent
        c.execute("SELECT COALESCE(SUM(price_paid), 0.0) as total_spent FROM purchases WHERE user_id = ?", (user_id,))
        total_spent_result = c.fetchone()
        total_spent = Decimal(str(total_spent_result['total_spent'])) if total_spent_result else Decimal('0.0')
        
    except sqlite3.Error as e:
        logger.error(f"DB error fetching user overview for {user_id}: {e}", exc_info=True)
        await send_message_with_retry(bot, chat_id, "❌ Error fetching user details.", parse_mode=None)
        return
    finally:
        if conn: 
            conn.close()
    
    # Build overview message
    banned_str = "Yes 🚫" if is_banned else "No ✅"
    reseller_str = "Yes 👑" if is_reseller else "No"
    balance_str = format_currency(balance)
    total_spent_str = format_currency(total_spent)
    
    msg = f"🔍 User Overview\n\n"
    msg += f"👤 User: @{username} (ID: {user_id})\n"
    msg += f"📊 Status: {status} {progress_bar}\n"
    msg += f"💰 Balance: {balance_str} EUR\n"
    msg += f"💸 Total Spent: {total_spent_str} EUR\n"
    msg += f"📦 Total Purchases: {total_purchases_count}\n"
    msg += f"🚫 Banned: {banned_str}\n"
    msg += f"👑 Reseller: {reseller_str}\n\n"
    
    msg += f"📋 Available Details:\n"
    if pending_deposits_count > 0:
        msg += f"⏳ Pending Deposits: {pending_deposits_count}\n"
    if total_purchases_count > 0:
        msg += f"📜 Purchase History: {total_purchases_count}\n"
    if admin_actions_count > 0:
        msg += f"🔧 Admin Actions: {admin_actions_count}\n"
    if is_reseller:
        msg += f"🏷️ Reseller Discounts\n"
    
    msg += f"\nSelect a section to view detailed information:"
    
    # Create section buttons
    keyboard = []
    
    # First row - Quick actions
    keyboard.append([
        InlineKeyboardButton("💰 Adjust Balance", callback_data=f"adm_adjust_balance_start|{user_id}|0"),
        InlineKeyboardButton("🚫 Ban/Unban", callback_data=f"adm_toggle_ban|{user_id}|0")
    ])
    
    # Detail sections
    detail_buttons = []
    if pending_deposits_count > 0:
        detail_buttons.append(InlineKeyboardButton(f"⏳ Deposits ({pending_deposits_count})", callback_data=f"adm_user_deposits|{user_id}"))
    if total_purchases_count > 0:
        detail_buttons.append(InlineKeyboardButton(f"📜 Purchases ({total_purchases_count})", callback_data=f"adm_user_purchases|{user_id}|0"))
    
    # Split detail buttons into rows of 2
    for i in range(0, len(detail_buttons), 2):
        keyboard.append(detail_buttons[i:i+2])
    
    if admin_actions_count > 0:
        keyboard.append([InlineKeyboardButton(f"🔧 Admin Actions ({admin_actions_count})", callback_data=f"adm_user_actions|{user_id}|0")])
    
    if is_reseller:
        keyboard.append([InlineKeyboardButton("🏷️ Reseller Discounts", callback_data=f"adm_user_discounts|{user_id}"),
                        InlineKeyboardButton("🔍 Debug Reseller", callback_data=f"adm_debug_reseller_discount|{user_id}")])
    
    # Navigation buttons
    keyboard.append([
        InlineKeyboardButton("🔍 Search Another", callback_data="adm_search_user_start"),
        InlineKeyboardButton("👥 Browse All", callback_data="adm_manage_users|0")
    ])
    keyboard.append([InlineKeyboardButton("⬅️ Admin meniu", callback_data="admin_menu")])
    
    await send_message_with_retry(bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


# --- Missing Functions That Were Accidentally Removed ---

async def handle_adm_bulk_back_to_messages(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Returns to the message collection interface."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    context.user_data["state"] = "awaiting_bulk_messages"
    await show_bulk_messages_status(update, context)

async def handle_adm_bulk_execute_messages(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Executes the bulk product creation from collected messages."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    chat_id = query.message.chat_id
    bulk_messages = context.user_data.get("bulk_messages", [])
    
    # Get setup data
    city = context.user_data.get("bulk_admin_city", "")
    district = context.user_data.get("bulk_admin_district", "")
    p_type = context.user_data.get("bulk_admin_product_type", "")
    size = context.user_data.get("bulk_pending_drop_size", "")
    price = context.user_data.get("bulk_pending_drop_price", 0)
    
    if not bulk_messages or not all([city, district, p_type, size, price]):
        return await query.edit_message_text("❌ Error: Missing data. Please start again.", parse_mode=None)
    
    await query.edit_message_text("⏳ Creating bulk products...", parse_mode=None)
    
    created_count = 0
    failed_messages = []  # Track failed messages with details
    successful_products = []  # Track successfully created products
    
    # Process each message as a separate product
    for i, message_data in enumerate(bulk_messages):
        message_number = i + 1
        text_content = message_data.get("text", "")
        media_list = message_data.get("media", [])
        
        # Create unique product name
        product_name = f"{p_type} {size} {int(time.time())}_{message_number}"
        
        conn = None
        product_id = None
        temp_dir = None
        
        try:
            # Download media if present
            if media_list:
                import tempfile
                temp_dir = await asyncio.to_thread(tempfile.mkdtemp, prefix="bulk_msg_media_")
                
                for j, media_item in enumerate(media_list):
                    try:
                        file_obj = await context.bot.get_file(media_item["file_id"])
                        file_extension = os.path.splitext(file_obj.file_path)[1] if file_obj.file_path else ""
                        if not file_extension:
                            if media_item["type"] == "photo": file_extension = ".jpg"
                            elif media_item["type"] == "video": file_extension = ".mp4"
                            elif media_item["type"] == "gif": file_extension = ".gif"
                            else: file_extension = ".bin"
                        
                        temp_file_path = os.path.join(temp_dir, f"media_{j}_{int(time.time())}{file_extension}")
                        await file_obj.download_to_drive(temp_file_path)
                        media_item["path"] = temp_file_path
                    except Exception as e:
                        logger.error(f"Error downloading media for bulk message {message_number}: {e}")
                        raise Exception(f"Media download failed: {str(e)}")
            
            # Create product in database
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("BEGIN")
            
            insert_params = (
                city, district, p_type, size, product_name, price, text_content, ADMIN_ID, datetime.now(timezone.utc).isoformat()
            )
            
            c.execute("""INSERT INTO products
                            (city, district, product_type, size, name, price, available, reserved, original_text, added_by, added_date)
                         VALUES (?, ?, ?, ?, ?, ?, 1, 0, ?, ?, ?)""", insert_params)
            product_id = c.lastrowid
            
            # Handle media for this product
            if product_id and media_list and temp_dir:
                final_media_dir = os.path.join(MEDIA_DIR, str(product_id))
                await asyncio.to_thread(os.makedirs, final_media_dir, exist_ok=True)
                media_inserts = []
                
                for media_item in media_list:
                    if "path" in media_item and "type" in media_item and "file_id" in media_item:
                        temp_file_path = media_item["path"]
                        if await asyncio.to_thread(os.path.exists, temp_file_path):
                            # Generate unique filename to prevent conflicts
                            base_filename = os.path.basename(temp_file_path)
                            name, ext = os.path.splitext(base_filename)
                            counter = 1
                            final_persistent_path = os.path.join(final_media_dir, f"{name}_{counter}{ext}")
                            
                            # Ensure unique filename
                            while await asyncio.to_thread(os.path.exists, final_persistent_path):
                                counter += 1
                                final_persistent_path = os.path.join(final_media_dir, f"{name}_{counter}{ext}")
                            
                            try:
                                await asyncio.to_thread(shutil.copy2, temp_file_path, final_persistent_path)
                                media_inserts.append((product_id, media_item["type"], final_persistent_path, media_item["file_id"]))
                            except OSError as move_err:
                                logger.error(f"Error copying media {temp_file_path}: {move_err}")
                        else:
                            logger.warning(f"Temp media not found: {temp_file_path}")
                    else:
                        logger.warning(f"Incomplete media item: {media_item}")
                
                # Insert all media records at once (outside the loop)
                if media_inserts:
                    c.executemany("INSERT INTO product_media (product_id, media_type, file_path, telegram_file_id) VALUES (?, ?, ?, ?)", media_inserts)
                    logger.info(f"Successfully inserted {len(media_inserts)} media records for bulk product {product_id}")
                else:
                    logger.warning(f"No media was inserted for product {product_id}. Media list: {media_list}, Temp dir: {temp_dir}")
            
            conn.commit()
            created_count += 1
            successful_products.append({
                'message_number': message_number,
                'product_id': product_id,
                'product_name': product_name
            })
            logger.info(f"Bulk created product {product_id} ({product_name}) from message {message_number}")
            
        except Exception as e:
            # Track detailed failure information
            text_preview = text_content[:30] + "..." if len(text_content) > 30 else text_content
            if not text_preview:
                text_preview = "(media only)"
            
            error_reason = str(e)
            if "Media download failed" in error_reason:
                error_type = "Media Download Error"
            elif "Media file" in error_reason:
                error_type = "Media Processing Error"
            elif "database" in error_reason.lower():
                error_type = "Database Error"
            else:
                error_type = "Unknown Error"
            
            failed_messages.append({
                'message_number': message_number,
                'text_preview': text_preview,
                'error_type': error_type,
                'error_reason': error_reason,
                'media_count': len(media_list)
            })
            
            logger.error(f"Error creating bulk product from message {message_number}: {e}", exc_info=True)
            if conn:
                try:
                    conn.rollback()
                except Exception as rb_err:
                    logger.error(f"Rollback failed: {rb_err}")
        finally:
            if conn:
                conn.close()
            
            # Clean up temp directory for this message
            if temp_dir and await asyncio.to_thread(os.path.exists, temp_dir):
                await asyncio.to_thread(shutil.rmtree, temp_dir, ignore_errors=True)
    
    # Clear bulk data from context
    keys_to_clear = ["bulk_messages", "bulk_admin_city_id", "bulk_admin_district_id", 
                     "bulk_admin_product_type", "bulk_admin_city", "bulk_admin_district", 
                     "bulk_pending_drop_size", "bulk_pending_drop_price", "state"]
    for key in keys_to_clear:
        context.user_data.pop(key, None)
    
    # Show detailed results
    type_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    total_messages = len(bulk_messages)
    failed_count = len(failed_messages)
    
    # Main result message
    result_msg = f"📦 Masinė operacija baigta!\n\n"
    result_msg += f"📍 Location: {city} / {district}\n"
    result_msg += f"{type_emoji} Product: {p_type} {size}\n"
    result_msg += f"💰 Price: {format_currency(price)}€\n\n"
    result_msg += f"📊 Summary:\n"
    result_msg += f"📝 Total Messages: {total_messages}\n"
    result_msg += f"✅ Successfully Created: {created_count} products\n"
    
    if failed_count > 0:
        result_msg += f"❌ Failed: {failed_count}\n\n"
        result_msg += f"🔍 Failed Messages Details:\n"
        
        for failure in failed_messages:
            result_msg += f"• Message #{failure['message_number']}: {failure['text_preview']}\n"
            result_msg += f"  Error: {failure['error_type']}\n"
            if failure['media_count'] > 0:
                result_msg += f"  Media: {failure['media_count']} files\n"
            result_msg += f"  Reason: {failure['error_reason'][:50]}...\n\n"
        
        result_msg += f"💡 You can retry the failed messages by:\n"
        result_msg += f"1. Starting a new bulk operation\n"
        result_msg += f"2. Re-forwarding only the failed messages\n"
        result_msg += f"3. Using the same settings ({city}/{district}, {p_type}, {size})\n\n"
    else:
        result_msg += f"\n🎉 All messages processed successfully!\n\n"
    
    if successful_products:
        result_msg += f"✅ Created Product IDs: "
        product_ids = [str(p['product_id']) for p in successful_products[:5]]  # Show first 5
        result_msg += ", ".join(product_ids)
        if len(successful_products) > 5:
            result_msg += f" (+{len(successful_products) - 5} more)"
        result_msg += "\n"
    
    keyboard = [
        [InlineKeyboardButton("📦 Add More Bulk Products", callback_data="adm_bulk_city")],
        [InlineKeyboardButton("🔧 Admin meniu", callback_data="admin_menu"), 
         InlineKeyboardButton("🏠 Pagrindinis", callback_data="back_start")]
    ]
    
    # Send the main result message
    await send_message_with_retry(context.bot, chat_id, result_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    
    # If there are failures, send a separate detailed failure message for better readability
    if failed_count > 0:
        failure_detail_msg = f"🚨 Detailed Failure Report:\n\n"
        for failure in failed_messages:
            failure_detail_msg += f"📝 Message #{failure['message_number']}:\n"
            failure_detail_msg += f"   Text: {failure['text_preview']}\n"
            failure_detail_msg += f"   Media Files: {failure['media_count']}\n"
            failure_detail_msg += f"   Error Type: {failure['error_type']}\n"
            failure_detail_msg += f"   Full Error: {failure['error_reason']}\n"
            failure_detail_msg += f"   ─────────────────\n"
        
        failure_detail_msg += f"\n📋 To retry failed messages:\n"
        failure_detail_msg += f"1. Copy the message numbers that failed\n"
        failure_detail_msg += f"2. Start new bulk operation with same settings\n"
        failure_detail_msg += f"3. Forward only those specific messages\n"
        
        await send_message_with_retry(context.bot, chat_id, failure_detail_msg, parse_mode=None)

# Product type message handlers
async def handle_adm_new_type_name_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles new product type name input."""
    if not is_primary_admin(update.effective_user.id): return
    if not update.message or not update.message.text: return
    
    type_name = update.message.text.strip()
    if not type_name:
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            "❌ Please enter a valid type name.", parse_mode=None)
        return
    
    # Check if type already exists
    load_all_data()
    if type_name in PRODUCT_TYPES:
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            f"❌ Product type '{type_name}' already exists. Please choose a different name.", parse_mode=None)
        return
    
    # Store the type name and ask for emoji
    context.user_data["new_type_name"] = type_name
    context.user_data["state"] = "awaiting_new_type_emoji"
    
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="adm_manage_types")]]
    await send_message_with_retry(context.bot, update.effective_chat.id, 
        f"🧩 Product Type: {type_name}\n\n"
        "✍️ Please reply with a single emoji for this product type:", 
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_new_type_emoji_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles new product type emoji input."""
    if not is_primary_admin(update.effective_user.id): return
    if not update.message or not update.message.text: return
    
    emoji = update.message.text.strip()
    if not emoji:
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            "❌ Please enter a valid emoji.", parse_mode=None)
        return
    
    # Basic emoji validation (check if it's a single character or emoji)
    if len(emoji) > 4:  # Allow for multi-byte emojis
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            "❌ Please enter only a single emoji.", parse_mode=None)
        return
    
    # Store the emoji and ask for description
    context.user_data["new_type_emoji"] = emoji
    context.user_data["state"] = "awaiting_new_type_description"
    
    type_name = context.user_data.get("new_type_name", "Unknown")
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="adm_manage_types")]]
    await send_message_with_retry(context.bot, update.effective_chat.id, 
        f"🧩 Product Type: {emoji} {type_name}\n\n"
        "📝 Please reply with a description for this product type (or send 'skip' to leave empty):", 
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_new_type_description_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles new product type description input and creates the type."""
    if not is_primary_admin(update.effective_user.id): return
    if not update.message or not update.message.text: return
    
    description = update.message.text.strip()
    if description.lower() == 'skip':
        description = None
    elif not description:
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            "❌ Please enter a description or send 'skip' to leave empty.", parse_mode=None)
        return
    
    type_name = context.user_data.get("new_type_name")
    emoji = context.user_data.get("new_type_emoji")
    
    if not type_name or not emoji:
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            "❌ Error: Missing type name or emoji. Please start over.", parse_mode=None)
        context.user_data.pop("state", None)
        return
    
    # Save to database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("INSERT INTO product_types (name, emoji, description) VALUES (?, ?, ?)", 
                  (type_name, emoji, description))
        conn.commit()
        load_all_data()  # Reload data
        
        context.user_data.pop("state", None)
        context.user_data.pop("new_type_name", None)
        context.user_data.pop("new_type_emoji", None)
        
        log_admin_action(admin_id=update.effective_user.id, action="PRODUCT_TYPE_ADD", 
                        reason=f"Added type '{type_name}' with emoji '{emoji}'", 
                        new_value=type_name)
        
        # Create the manage types keyboard to show the updated list
        keyboard = []
        for existing_type_name, existing_emoji in sorted(PRODUCT_TYPES.items()):
            keyboard.append([
                InlineKeyboardButton(f"{existing_emoji} {existing_type_name}", callback_data=f"adm_edit_type_menu|{existing_type_name}"),
                InlineKeyboardButton(f"🗑️ Delete", callback_data=f"adm_delete_type|{existing_type_name}")
            ])
        keyboard.extend([
            [InlineKeyboardButton("➕ Add New Type", callback_data="adm_add_type")],
            [InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")]
        ])
        
        success_msg = f"✅ Product type '{emoji} {type_name}' created successfully!"
        if description:
            success_msg += f"\nDescription: {description}"
        success_msg += "\n\n🧩 Manage Product Types\n\nSelect a type to edit or delete:"
        
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            success_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
    except sqlite3.Error as e:
        logger.error(f"DB error creating product type '{type_name}': {e}", exc_info=True)
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            "❌ Database error creating product type. Please try again.", parse_mode=None)
        context.user_data.pop("state", None)
    finally:
        if conn: conn.close()

async def handle_adm_edit_type_emoji_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles editing product type emoji input."""
    if not is_primary_admin(update.effective_user.id): return
    if not update.message or not update.message.text: return
    
    emoji = update.message.text.strip()
    if not emoji:
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            "❌ Please enter a valid emoji.", parse_mode=None)
        return
    
    # Basic emoji validation
    if len(emoji) > 4:
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            "❌ Please enter only a single emoji.", parse_mode=None)
        return
    
    type_name = context.user_data.get("edit_type_name")
    if not type_name:
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            "❌ Error: Type name not found. Please start over.", parse_mode=None)
        context.user_data.pop("state", None)
        return
    
    # Update emoji in database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("UPDATE product_types SET emoji = ? WHERE name = ?", (emoji, type_name))
        
        if c.rowcount > 0:
            conn.commit()
            load_all_data()  # Reload data
            
            context.user_data.pop("state", None)
            context.user_data.pop("edit_type_name", None)
            
            log_admin_action(admin_id=update.effective_user.id, action="PRODUCT_TYPE_EDIT", 
                            reason=f"Changed emoji for type '{type_name}' to '{emoji}'", 
                            old_value=type_name, new_value=f"{emoji} {type_name}")
            
            # Show updated type info
            current_description = ""
            c.execute("SELECT description FROM product_types WHERE name = ?", (type_name,))
            res = c.fetchone()
            if res: current_description = res['description'] or "(Description not set)"
            
            keyboard = [
                [InlineKeyboardButton("✏️ Change Emoji", callback_data=f"adm_change_type_emoji|{type_name}")],
                [InlineKeyboardButton("📝 Change Name", callback_data=f"adm_change_type_name|{type_name}")],
                [InlineKeyboardButton("🗑️ Delete Type", callback_data=f"adm_delete_type|{type_name}")],
                [InlineKeyboardButton("⬅️ Back to Manage Types", callback_data="adm_manage_types")]
            ]
            
            await send_message_with_retry(context.bot, update.effective_chat.id, 
                f"✅ Emoji updated successfully!\n\n"
                f"🧩 Editing Type: {type_name}\n\n"
                f"Current Emoji: {emoji}\n"
                f"Description: {current_description}\n\n"
                f"What would you like to do?", 
                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        else:
            await send_message_with_retry(context.bot, update.effective_chat.id, 
                f"❌ Error: Product type '{type_name}' not found.", parse_mode=None)
            context.user_data.pop("state", None)
    except sqlite3.Error as e:
        logger.error(f"DB error updating emoji for type '{type_name}': {e}", exc_info=True)
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            "❌ Database error updating emoji. Please try again.", parse_mode=None)
        context.user_data.pop("state", None)
    finally:
        if conn: conn.close()

# User search handlers
async def handle_adm_search_user_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Starts the user search process by prompting for username."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): 
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    context.user_data['state'] = 'awaiting_search_username'
    
    prompt_msg = (
        "🔍 Search User by Username or ID\n\n"
        "Please reply with the Telegram username (with or without @) or User ID of the person you want to search for.\n\n"
        "Examples:\n"
        "• @username123 or username123\n"
        "• 123456789 (User ID)"
    )
    
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="admin_menu")]]
    
    await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter username or User ID in chat.")

async def handle_adm_search_username_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the admin entering a username or User ID for search."""
    admin_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if not is_primary_admin(admin_id): 
        return
    if context.user_data.get("state") != 'awaiting_search_username': 
        return
    if not update.message or not update.message.text:
        return
    
    search_term = update.message.text.strip()
    
    # Remove @ symbol if present
    if search_term.startswith('@'):
        search_term = search_term[1:]
    
    # Clear state
    context.user_data.pop('state', None)
    
    # Try to find user by username or user ID
    conn = None
    user_info = None
    search_by_id = False
    
    try:
        # Check if search term is a number (User ID)
        try:
            user_id_search = int(search_term)
            search_by_id = True
        except ValueError:
            search_by_id = False
        
        conn = get_db_connection()
        c = conn.cursor()
        
        if search_by_id:
            # Search by User ID
            c.execute("SELECT user_id, username, balance, total_purchases, is_banned, is_reseller FROM users WHERE user_id = ?", (user_id_search,))
        else:
            # Search by username (case insensitive)
            c.execute("SELECT user_id, username, balance, total_purchases, is_banned, is_reseller FROM users WHERE LOWER(username) = LOWER(?)", (search_term,))
        
        user_info = c.fetchone()
        
    except sqlite3.Error as e:
        logger.error(f"DB error searching for user '{search_term}': {e}")
        await send_message_with_retry(context.bot, chat_id, "❌ Database error during search.", parse_mode=None)
        return
    finally:
        if conn: 
            conn.close()
    
    if not user_info:
        search_type = "User ID" if search_by_id else "username"
        await send_message_with_retry(
            context.bot, chat_id, 
            f"❌ No user found with {search_type}: {search_term}\n\nPlease check the spelling or try a different search term.",
            parse_mode=None
        )
        
        # Offer to search again
        keyboard = [
            [InlineKeyboardButton("🔍 Search Again", callback_data="adm_search_user_start")],
            [InlineKeyboardButton("👥 Browse All Users", callback_data="adm_manage_users|0")],
            [InlineKeyboardButton("⬅️ Admin meniu", callback_data="admin_menu")]
        ]
        await send_message_with_retry(
            context.bot, chat_id, 
            "What would you like to do?", 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode=None
        )
        return
            
    # User found - display comprehensive information
    await display_user_search_results(context.bot, chat_id, user_info)

# Detailed User Information Handlers
async def handle_adm_user_deposits(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows detailed pending deposits for a user."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): 
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params or not params[0].isdigit():
        return await query.answer("Error: Invalid user ID.", show_alert=True)
    
    user_id = int(params[0])
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get user info
        c.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
        user_result = c.fetchone()
        if not user_result:
            return await query.answer("User not found.", show_alert=True)
        
        username = user_result['username'] or f"ID_{user_id}"
        
        # Get all pending deposits
        c.execute("""
            SELECT payment_id, currency, target_eur_amount, expected_crypto_amount, created_at, is_purchase
            FROM pending_deposits 
            WHERE user_id = ? 
            ORDER BY created_at DESC
        """, (user_id,))
        deposits = c.fetchall()
        
    except sqlite3.Error as e:
        logger.error(f"DB error fetching deposits for user {user_id}: {e}", exc_info=True)
        await query.answer("Database error.", show_alert=True)
        return
    finally:
        if conn: 
            conn.close()
    
    msg = f"⏳ Pending Deposits - @{username}\n\n"
    
    if not deposits:
        msg += "No pending deposits found."
    else:
        for i, deposit in enumerate(deposits, 1):
            payment_id = deposit['payment_id'][:12] + "..."
            currency = deposit['currency'].upper()
            amount = format_currency(deposit['target_eur_amount'])
            expected_crypto = deposit['expected_crypto_amount']
            deposit_type = "Purchase" if deposit['is_purchase'] else "Refill"
            
            try:
                created_dt = datetime.fromisoformat(deposit['created_at'].replace('Z', '+00:00'))
                if created_dt.tzinfo is None: 
                    created_dt = created_dt.replace(tzinfo=timezone.utc)
                date_str = created_dt.strftime('%Y-%m-%d %H:%M')
            except (ValueError, TypeError): 
                date_str = "Unknown date"
            
            msg += f"{i}. {deposit_type} - {amount}€\n"
            msg += f"   💰 Expected: {expected_crypto} {currency}\n"
            msg += f"   📅 Created: {date_str}\n"
            msg += f"   🆔 Payment: {payment_id}\n\n"
    
    keyboard = [
        [InlineKeyboardButton("⬅️ Back to User", callback_data=f"adm_user_overview|{user_id}")],
        [InlineKeyboardButton("🔍 Search Another", callback_data="adm_search_user_start")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_user_purchases(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows paginated purchase history for a user."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): 
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params or len(params) < 2 or not params[0].isdigit() or not params[1].isdigit():
        return await query.answer("Error: Invalid parameters.", show_alert=True)
    
    user_id = int(params[0])
    offset = int(params[1])
    limit = 10
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get user info
        c.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
        user_result = c.fetchone()
        if not user_result:
            return await query.answer("User not found.", show_alert=True)
        
        username = user_result['username'] or f"ID_{user_id}"
        
        # Get total count
        c.execute("SELECT COUNT(*) as count FROM purchases WHERE user_id = ?", (user_id,))
        total_count = c.fetchone()['count']
        
        # Get purchases for this page
        c.execute("""
            SELECT purchase_date, product_name, product_type, product_size, price_paid, city, district
            FROM purchases 
            WHERE user_id = ? 
            ORDER BY purchase_date DESC 
            LIMIT ? OFFSET ?
        """, (user_id, limit, offset))
        purchases = c.fetchall()
        
    except sqlite3.Error as e:
        logger.error(f"DB error fetching purchases for user {user_id}: {e}", exc_info=True)
        await query.answer("Database error.", show_alert=True)
        return
    finally:
        if conn: 
            conn.close()
    
    current_page = (offset // limit) + 1
    total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
    
    msg = f"📜 Purchase History - @{username}\n"
    msg += f"Page {current_page}/{total_pages} ({total_count} total)\n\n"
    
    if not purchases:
        msg += "No purchases found."
    else:
        for i, purchase in enumerate(purchases, offset + 1):
            try:
                dt_obj = datetime.fromisoformat(purchase['purchase_date'].replace('Z', '+00:00'))
                if dt_obj.tzinfo is None: 
                    dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                date_str = dt_obj.strftime('%Y-%m-%d %H:%M')
            except (ValueError, TypeError): 
                date_str = "Unknown date"
            
            p_type = purchase['product_type']
            p_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
            p_size = purchase['product_size'] or 'N/A'
            p_price = format_currency(purchase['price_paid'])
            p_city = purchase['city'] or 'N/A'
            p_district = purchase['district'] or 'N/A'
            
            msg += f"{i}. {p_emoji} {p_type} {p_size} - {p_price}€\n"
            msg += f"   📍 {p_city}/{p_district}\n"
            msg += f"   📅 {date_str}\n\n"
    
    # Pagination buttons
    keyboard = []
    nav_buttons = []
    
    if current_page > 1:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"adm_user_purchases|{user_id}|{max(0, offset - limit)}"))
    if current_page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"adm_user_purchases|{user_id}|{offset + limit}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    keyboard.append([InlineKeyboardButton("⬅️ Back to User", callback_data=f"adm_user_overview|{user_id}")])
    keyboard.append([InlineKeyboardButton("🔍 Search Another", callback_data="adm_search_user_start")])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_user_actions(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows paginated admin actions for a user."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): 
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params or len(params) < 2 or not params[0].isdigit() or not params[1].isdigit():
        return await query.answer("Error: Invalid parameters.", show_alert=True)
    
    user_id = int(params[0])
    offset = int(params[1])
    limit = 10
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get user info
        c.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
        user_result = c.fetchone()
        if not user_result:
            return await query.answer("User not found.", show_alert=True)
        
        username = user_result['username'] or f"ID_{user_id}"
        
        # Get total count
        c.execute("SELECT COUNT(*) as count FROM admin_log WHERE target_user_id = ?", (user_id,))
        total_count = c.fetchone()['count']
        
        # Get actions for this page
        c.execute("""
            SELECT timestamp, action, reason, amount_change, old_value, new_value
            FROM admin_log 
            WHERE target_user_id = ? 
            ORDER BY timestamp DESC 
            LIMIT ? OFFSET ?
        """, (user_id, limit, offset))
        actions = c.fetchall()
        
    except sqlite3.Error as e:
        logger.error(f"DB error fetching admin actions for user {user_id}: {e}", exc_info=True)
        await query.answer("Database error.", show_alert=True)
        return
    finally:
        if conn: 
            conn.close()
    
    current_page = (offset // limit) + 1
    total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
    
    msg = f"🔧 Admin Actions - @{username}\n"
    msg += f"Page {current_page}/{total_pages} ({total_count} total)\n\n"
    
    if not actions:
        msg += "No admin actions found."
    else:
        for i, action in enumerate(actions, offset + 1):
            try:
                action_dt = datetime.fromisoformat(action['timestamp'].replace('Z', '+00:00'))
                if action_dt.tzinfo is None: 
                    action_dt = action_dt.replace(tzinfo=timezone.utc)
                date_str = action_dt.strftime('%Y-%m-%d %H:%M')
            except (ValueError, TypeError): 
                date_str = "Unknown date"
            
            action_name = action['action']
            reason = action['reason'] or 'No reason'
            amount_change = action['amount_change']
            
            msg += f"{i}. {action_name}\n"
            msg += f"   📅 {date_str}\n"
            if amount_change:
                msg += f"   💰 Amount: {format_currency(amount_change)}€\n"
            msg += f"   📝 Reason: {reason}\n\n"
    
    # Pagination buttons
    keyboard = []
    nav_buttons = []
    
    if current_page > 1:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"adm_user_actions|{user_id}|{max(0, offset - limit)}"))
    if current_page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"adm_user_actions|{user_id}|{offset + limit}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    keyboard.append([InlineKeyboardButton("⬅️ Back to User", callback_data=f"adm_user_overview|{user_id}")])
    keyboard.append([InlineKeyboardButton("🔍 Search Another", callback_data="adm_search_user_start")])
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_user_discounts(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows reseller discounts for a user."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): 
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params or not params[0].isdigit():
        return await query.answer("Error: Invalid user ID.", show_alert=True)
    
    user_id = int(params[0])
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get user info
        c.execute("SELECT username, is_reseller FROM users WHERE user_id = ?", (user_id,))
        user_result = c.fetchone()
        if not user_result:
            return await query.answer("User not found.", show_alert=True)
        
        username = user_result['username'] or f"ID_{user_id}"
        is_reseller = user_result['is_reseller'] == 1
        
        if not is_reseller:
            return await query.answer("User is not a reseller.", show_alert=True)
        
        # Get reseller discounts
        c.execute("""
            SELECT product_type, discount_percentage 
            FROM reseller_discounts 
            WHERE reseller_user_id = ? 
            ORDER BY product_type
        """, (user_id,))
        discounts = c.fetchall()
        
    except sqlite3.Error as e:
        logger.error(f"DB error fetching discounts for user {user_id}: {e}", exc_info=True)
        await query.answer("Database error.", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
        
    msg = f"🏷️ Reseller Discounts - @{username}\n\n"
    
    if not discounts:
        msg += "No reseller discounts configured."
    else:
        for discount in discounts:
            product_type = discount['product_type']
            percentage = discount['discount_percentage']
            emoji = PRODUCT_TYPES.get(product_type, DEFAULT_PRODUCT_EMOJI)
            msg += f"{emoji} {product_type}: {percentage}%\n"
    
    keyboard = [
        [InlineKeyboardButton("⬅️ Back to User", callback_data=f"adm_user_overview|{user_id}")],
        [InlineKeyboardButton("🔍 Search Another", callback_data="adm_search_user_start")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_adm_user_overview(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Returns to user overview from detailed sections."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): 
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params or not params[0].isdigit():
        return await query.answer("Error: Invalid user ID.", show_alert=True)
    
    user_id = int(params[0])
    
    # Get user info and redisplay overview
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT user_id, username, balance, total_purchases, is_banned, is_reseller FROM users WHERE user_id = ?", (user_id,))
        user_info = c.fetchone()
        
        if not user_info:
            return await query.answer("User not found.", show_alert=True)
        
    except sqlite3.Error as e:
        logger.error(f"DB error fetching user info for overview {user_id}: {e}", exc_info=True)
        await query.answer("Database error.", show_alert=True)
        return
    finally:
        if conn: 
            conn.close()
    
    # Redisplay the overview
    await display_user_search_results(context.bot, query.message.chat_id, dict(user_info))


# --- Welcome Message Management Handlers ---

async def handle_adm_manage_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Displays the paginated menu for managing welcome message templates."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access Denied.", show_alert=True)

    lang, lang_data = _get_lang_data(context) # Use helper
    offset = 0
    if params and len(params) > 0 and params[0].isdigit():
        offset = int(params[0])

    # Fetch templates and active template name
    templates = get_welcome_message_templates(limit=TEMPLATES_PER_PAGE, offset=offset)
    total_templates = get_welcome_message_template_count()
    conn = None
    active_template_name = "default" # Default fallback
    try:
        conn = get_db_connection()
        c = conn.cursor()
        # Use column name
        c.execute("SELECT setting_value FROM bot_settings WHERE setting_key = ?", ("active_welcome_message_name",))
        setting_row = c.fetchone()
        if setting_row and setting_row['setting_value']: # Check if value is not None/empty
            active_template_name = setting_row['setting_value'] # Use column name
    except sqlite3.Error as e:
        logger.error(f"DB error fetching active welcome template name: {e}")
    finally:
        if conn: conn.close()

    # Build message and keyboard
    title = lang_data.get("manage_welcome_title", "⚙️ Manage Welcome Messages")
    prompt = lang_data.get("manage_welcome_prompt", "Select a template to manage or activate:")
    msg_parts = [f"{title}\n\n{prompt}\n"] # Use list to build message
    keyboard = []

    if not templates and offset == 0:
        msg_parts.append("\nNo custom templates found. Add one?")
    else:
        for template in templates:
            name = template['name']
            desc = template['description'] or "No description"

            is_active = (name == active_template_name)
            active_indicator = " (Active ✅)" if is_active else ""

            # Display Name, Description, and Active Status
            msg_parts.append(f"\n📄 {name}{active_indicator}\n{desc}\n")

            # Buttons: Edit | Activate (if not active) | Delete (if not default and not active)
            row = [InlineKeyboardButton("✏️ Edit", callback_data=f"adm_edit_welcome|{name}|{offset}")]
            if not is_active:
                 row.append(InlineKeyboardButton("✅ Activate", callback_data=f"adm_activate_welcome|{name}|{offset}"))

            can_delete = not (name == "default") and not is_active # Cannot delete default or active
            if can_delete:
                 row.append(InlineKeyboardButton("🗑️ Delete", callback_data=f"adm_delete_welcome_confirm|{name}|{offset}"))
            keyboard.append(row)

        # Pagination
        total_pages = math.ceil(total_templates / TEMPLATES_PER_PAGE)
        current_page = (offset // TEMPLATES_PER_PAGE) + 1
        nav_buttons = []
        if current_page > 1: nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"adm_manage_welcome|{max(0, offset - TEMPLATES_PER_PAGE)}"))
        if current_page < total_pages: nav_buttons.append(InlineKeyboardButton("➡️ Next", callback_data=f"adm_manage_welcome|{offset + TEMPLATES_PER_PAGE}"))
        if nav_buttons: keyboard.append(nav_buttons)
        if total_pages > 1:
            page_indicator = f"Page {current_page}/{total_pages}"
            msg_parts.append(f"\n{page_indicator}")

    # Add "Add New" and "Reset Default" buttons
    keyboard.append([InlineKeyboardButton("➕ Add New Template", callback_data="adm_add_welcome_start")])
    keyboard.append([InlineKeyboardButton("🔄 Reset to Built-in Default", callback_data="adm_reset_default_confirm")])
    keyboard.append([InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")])

    final_msg = "".join(msg_parts)

    # Send/Edit message
    try:
        await query.edit_message_text(final_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.error(f"Error editing welcome management menu: {e}")
            await query.answer("Error displaying menu.", show_alert=True)
        else:
             await query.answer() # Acknowledge if not modified
    except Exception as e:
        logger.error(f"Unexpected error in handle_adm_manage_welcome: {e}", exc_info=True)
        await query.answer("An error occurred displaying the menu.", show_alert=True)

async def handle_adm_activate_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Activates the selected welcome message template."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params or len(params) < 2 or not params[1].isdigit():
        return await query.answer("Error: Template name or offset missing.", show_alert=True)

    template_name = params[0]
    offset = int(params[1])
    lang, lang_data = _get_lang_data(context) # Use helper

    success = set_active_welcome_message(template_name) # Use helper from utils
    if success:
        msg_template = lang_data.get("welcome_activate_success", "✅ Template '{name}' activated.")
        await query.answer(msg_template.format(name=template_name))
        await handle_adm_manage_welcome(update, context, params=[str(offset)]) # Refresh menu at same page
    else:
        msg_template = lang_data.get("welcome_activate_fail", "❌ Failed to activate template '{name}'.")
        await query.answer(msg_template.format(name=template_name), show_alert=True)

async def handle_adm_add_welcome_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Starts the process of adding a new welcome template (gets name)."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    lang, lang_data = _get_lang_data(context) # Use helper

    context.user_data['state'] = 'awaiting_welcome_template_name'
    prompt = lang_data.get("welcome_add_name_prompt", "Enter a unique short name for the new template (e.g., 'default', 'promo_weekend'):")
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="adm_manage_welcome|0")]] # Go back to first page
    await query.edit_message_text(prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter template name in chat.")

async def handle_adm_edit_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows options for editing an existing welcome template (text or description)."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params or len(params) < 2 or not params[1].isdigit():
        return await query.answer("Error: Template name or offset missing.", show_alert=True)

    template_name = params[0]
    offset = int(params[1])
    lang, lang_data = _get_lang_data(context) # Use helper

    # Fetch current text and description
    current_text = ""
    current_description = ""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT template_text, description FROM welcome_messages WHERE name = ?", (template_name,))
        row = c.fetchone()
        if not row:
             await query.answer("Template not found.", show_alert=True)
             return await handle_adm_manage_welcome(update, context, params=[str(offset)])
        current_text = row['template_text']
        current_description = row['description'] or ""
    except sqlite3.Error as e:
        logger.error(f"DB error fetching template '{template_name}' for edit options: {e}")
        await query.answer("Error fetching template details.", show_alert=True)
        return await handle_adm_manage_welcome(update, context, params=[str(offset)])
    finally:
        if conn: conn.close()

    # Store info needed for potential edits
    context.user_data['editing_welcome_template_name'] = template_name
    context.user_data['editing_welcome_offset'] = offset

    # Display using plain text
    safe_name = template_name
    safe_desc = current_description or 'Not set'

    msg = f"✏️ Editing Template: {safe_name}\n\n"
    msg += f"📝 Description: {safe_desc}\n\n"
    msg += "Choose what to edit:"

    keyboard = [
        [InlineKeyboardButton("Edit Text", callback_data=f"adm_edit_welcome_text|{template_name}")],
        [InlineKeyboardButton("Edit Description", callback_data=f"adm_edit_welcome_desc|{template_name}")],
        [InlineKeyboardButton("⬅️ Atgal", callback_data=f"adm_manage_welcome|{offset}")]
    ]
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Error editing edit welcome menu: {e}")
        else: await query.answer() # Acknowledge if not modified
    except Exception as e:
        logger.error(f"Unexpected error in handle_adm_edit_welcome: {e}")
        await query.answer("Error displaying edit menu.", show_alert=True)

async def handle_adm_edit_welcome_text(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Initiates editing the template text."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params: return await query.answer("Error: Template name missing.", show_alert=True)

    template_name = params[0]
    offset = context.user_data.get('editing_welcome_offset', 0) # Get offset from context
    lang, lang_data = _get_lang_data(context) # Use helper

    # Fetch current text to show in prompt
    current_text = ""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT template_text FROM welcome_messages WHERE name = ?", (template_name,))
        row = c.fetchone()
        if row: current_text = row['template_text']
    except sqlite3.Error as e: logger.error(f"DB error fetching text for edit: {e}")
    finally:
         if conn: conn.close()

    context.user_data['state'] = 'awaiting_welcome_template_edit' # Reusing state, but specifically for text
    context.user_data['editing_welcome_template_name'] = template_name # Ensure it's set
    context.user_data['editing_welcome_field'] = 'text' # Indicate we are editing text

    placeholders = "{username}, {status}, {progress_bar}, {balance_str}, {purchases}, {basket_count}" # Plain text placeholders
    prompt_template = lang_data.get("welcome_edit_text_prompt", "Editing Text for '{name}'. Current text:\n\n{current_text}\n\nPlease reply with the new text. Available placeholders:\n{placeholders}")
    # Display plain text
    prompt = prompt_template.format(
        name=template_name,
        current_text=current_text,
        placeholders=placeholders
    )
    if len(prompt) > 4000: prompt = prompt[:4000] + "\n[... Current text truncated ...]"

    # Go back to the specific template's edit menu
    keyboard = [[InlineKeyboardButton("❌ Cancel Edit", callback_data=f"adm_edit_welcome|{template_name}|{offset}")]]
    try:
        await query.edit_message_text(prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Error editing edit text prompt: {e}")
        else: await query.answer()
    await query.answer("Enter new template text.")

async def handle_adm_edit_welcome_desc(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Initiates editing the template description."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params: return await query.answer("Error: Template name missing.", show_alert=True)

    template_name = params[0]
    offset = context.user_data.get('editing_welcome_offset', 0)
    lang, lang_data = _get_lang_data(context) # Use helper

    # Fetch current description
    current_desc = ""
    conn = None
    try:
        conn = get_db_connection(); c = conn.cursor()
        c.execute("SELECT description FROM welcome_messages WHERE name = ?", (template_name,))
        row = c.fetchone(); current_desc = row['description'] or ""
    except sqlite3.Error as e: logger.error(f"DB error fetching desc for edit: {e}")
    finally:
        if conn: conn.close()

    context.user_data['state'] = 'awaiting_welcome_description_edit' # New state for description edit
    context.user_data['editing_welcome_template_name'] = template_name # Ensure it's set
    context.user_data['editing_welcome_field'] = 'description' # Indicate we are editing description

    prompt_template = lang_data.get("welcome_edit_description_prompt", "Editing description for '{name}'. Current: '{current_desc}'.\n\nEnter new description or send '-' to skip.")
    prompt = prompt_template.format(name=template_name, current_desc=current_desc or "Not set")

    keyboard = [[InlineKeyboardButton("❌ Cancel Edit", callback_data=f"adm_edit_welcome|{template_name}|{offset}")]]
    await query.edit_message_text(prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter new description.")

async def handle_adm_delete_welcome_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirms deletion of a welcome message template."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    if not params or len(params) < 2 or not params[1].isdigit():
         return await query.answer("Error: Template name or offset missing.", show_alert=True)

    template_name = params[0]
    offset = int(params[1])
    lang, lang_data = _get_lang_data(context) # Use helper

    # Fetch current active template
    conn = None
    active_template_name = "default"
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT setting_value FROM bot_settings WHERE setting_key = ?", ("active_welcome_message_name",))
        row = c.fetchone(); active_template_name = row['setting_value'] if row else "default" # Use column name
    except sqlite3.Error as e: logger.error(f"DB error checking template status for delete: {e}")
    finally:
         if conn: conn.close()

    if template_name == "default":
        await query.answer("Cannot delete the 'default' template.", show_alert=True)
        return await handle_adm_manage_welcome(update, context, params=[str(offset)])

    # Prevent deleting the active template
    if template_name == active_template_name:
        cannot_delete_msg = lang_data.get("welcome_cannot_delete_active", "❌ Cannot delete the active template. Activate another first.")
        await query.answer(cannot_delete_msg, show_alert=True)
        return await handle_adm_manage_welcome(update, context, params=[str(offset)]) # Refresh list

    context.user_data["confirm_action"] = f"delete_welcome_template|{template_name}"
    title = lang_data.get("welcome_delete_confirm_title", "⚠️ Confirm Deletion")
    text_template = lang_data.get("welcome_delete_confirm_text", "Are you sure you want to delete the welcome message template named '{name}'?")
    msg = f"{title}\n\n{text_template.format(name=template_name)}"

    keyboard = [
        [InlineKeyboardButton("✅ Yes, Delete Template", callback_data="confirm_yes")],
        [InlineKeyboardButton("❌ No, Cancel", callback_data=f"adm_manage_welcome|{offset}")]
    ]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_reset_default_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirms resetting the 'default' template to the built-in text and activating it."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    lang, lang_data = _get_lang_data(context)

    context.user_data["confirm_action"] = "reset_default_welcome"
    title = lang_data.get("welcome_reset_confirm_title", "⚠️ Confirm Reset")
    text = lang_data.get("welcome_reset_confirm_text", "Are you sure you want to reset the text of the 'default' template to the built-in version and activate it?")
    msg = f"{title}\n\n{text}"

    keyboard = [
        [InlineKeyboardButton("✅ Yes, Reset & Activate", callback_data="confirm_yes")],
        [InlineKeyboardButton("❌ No, Cancel", callback_data="adm_manage_welcome|0")]
    ]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

async def handle_confirm_save_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the 'Save Template' button after preview."""
    query = update.callback_query
    user_id = query.from_user.id
    if not is_primary_admin(user_id): return await query.answer("Access Denied.", show_alert=True)
    if context.user_data.get("state") != 'awaiting_welcome_confirmation':
        logger.warning("handle_confirm_save_welcome called in wrong state.")
        return await query.answer("Invalid state.", show_alert=True)

    pending_template = context.user_data.get("pending_welcome_template")
    if not pending_template or not pending_template.get("name") or pending_template.get("text") is None: # Text can be empty, but key must exist
        logger.error("Attempted to save welcome template, but pending data missing.")
        await query.edit_message_text("❌ Error: Save data lost. Please start again.", parse_mode=None)
        context.user_data.pop("state", None)
        context.user_data.pop("pending_welcome_template", None)
        return

    template_name = pending_template['name']
    template_text = pending_template['text']
    template_description = pending_template.get('description') # Can be None
    is_editing = pending_template.get('is_editing', False)
    offset = pending_template.get('offset', 0)
    lang, lang_data = _get_lang_data(context) # Use helper

    # Perform the actual save operation
    success = False
    if is_editing:
        success = update_welcome_message_template(template_name, template_text, template_description)
        msg_template = lang_data.get("welcome_edit_success", "✅ Template '{name}' updated.") if success else lang_data.get("welcome_edit_fail", "❌ Failed to update template '{name}'.")
    else:
        success = add_welcome_message_template(template_name, template_text, template_description)
        msg_template = lang_data.get("welcome_add_success", "✅ Welcome message template '{name}' added.") if success else lang_data.get("welcome_add_fail", "❌ Failed to add welcome message template.")

    # Clean up context
    context.user_data.pop("state", None)
    context.user_data.pop("pending_welcome_template", None)

    await query.edit_message_text(msg_template.format(name=template_name), parse_mode=None)

    # Go back to the management list
    await handle_adm_manage_welcome(update, context, params=[str(offset)])


# --- Missing helper functions that are referenced ---

def _get_lang_data(context):
    """Helper function to get language data."""
    return 'en', LANGUAGES.get('en', {})

def get_welcome_message_templates(limit=10, offset=0):
    """Helper function to get welcome message templates."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT name, description FROM welcome_messages ORDER BY name LIMIT ? OFFSET ?", (limit, offset))
        return c.fetchall()
    except sqlite3.Error as e:
        logger.error(f"DB error fetching welcome templates: {e}")
        return []
    finally:
        if conn: conn.close()

def get_welcome_message_template_count():
    """Helper function to get total count of welcome message templates."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM welcome_messages")
        result = c.fetchone()
        return result['count'] if result else 0
    except sqlite3.Error as e:
        logger.error(f"DB error counting welcome templates: {e}")
        return 0
    finally:
        if conn: conn.close()

def set_active_welcome_message(template_name):
    """Helper function to set active welcome message template."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO bot_settings (setting_key, setting_value) VALUES (?, ?)",
                  ("active_welcome_message_name", template_name))
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"DB error setting active welcome template: {e}")
        return False
    finally:
        if conn: conn.close()

def add_welcome_message_template(name, text, description=None):
    """Helper function to add welcome message template."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("INSERT INTO welcome_messages (name, template_text, description) VALUES (?, ?, ?)",
                  (name, text, description))
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"DB error adding welcome template: {e}")
        return False
    finally:
        if conn: conn.close()

def update_welcome_message_template(name, text, description=None):
    """Helper function to update welcome message template."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("UPDATE welcome_messages SET template_text = ?, description = ? WHERE name = ?",
                  (text, description, name))
        conn.commit()
        return c.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"DB error updating welcome template: {e}")
        return False
    finally:
        if conn: conn.close()

# Constants for pagination
TEMPLATES_PER_PAGE = 5


async def handle_adm_debug_reseller_discount(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Debug reseller discount system for a specific user."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): 
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params or not params[0].isdigit():
        return await query.answer("Error: Invalid user ID.", show_alert=True)
    
    user_id = int(params[0])
    
    # Import the reseller discount function
    try:
        from reseller_management import get_reseller_discount
    except ImportError:
        return await query.answer("Reseller system not available.", show_alert=True)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get user info
        c.execute("SELECT username, is_reseller FROM users WHERE user_id = ?", (user_id,))
        user_result = c.fetchone()
        if not user_result:
            return await query.answer("User not found.", show_alert=True)
        
        username = user_result['username'] or f"ID_{user_id}"
        is_reseller = user_result['is_reseller']
        
        # Get all product types for testing
        from utils import PRODUCT_TYPES
        
        msg = f"🔍 Reseller Discount Debug - @{username}\n\n"
        msg += f"Reseller Status: {'✅ Yes' if is_reseller == 1 else '❌ No'} (DB value: {is_reseller})\n\n"
        
        if is_reseller == 1:
            # Get all discount records
            c.execute("SELECT product_type, discount_percentage FROM reseller_discounts WHERE reseller_user_id = ? ORDER BY product_type", (user_id,))
            discount_records = c.fetchall()
            
            msg += f"Discount Records ({len(discount_records)}):\n"
            if discount_records:
                for record in discount_records:
                    emoji = PRODUCT_TYPES.get(record['product_type'], '📦')
                    msg += f"• {emoji} {record['product_type']}: {record['discount_percentage']}%\n"
            else:
                msg += "• No discount records found\n"
            
            msg += "\nLive Discount Check:\n"
            # Test discount lookup for each product type
            for product_type in PRODUCT_TYPES.keys():
                discount = get_reseller_discount(user_id, product_type)
                emoji = PRODUCT_TYPES.get(product_type, '📦')
                msg += f"• {emoji} {product_type}: {discount}%\n"
        else:
            msg += "User is not marked as reseller in database.\n"
            msg += "To enable: Admin Menu → Manage Resellers → Enter User ID → Enable Reseller Status"
        
    except Exception as e:
        logger.error(f"Error in reseller debug for user {user_id}: {e}", exc_info=True)
        await query.answer("Error occurred during debug.", show_alert=True)
        return
    finally:
        if conn:
            conn.close()
    
    keyboard = [
        [InlineKeyboardButton("⬅️ Back to User", callback_data=f"adm_user_overview|{user_id}")],
        [InlineKeyboardButton("🔍 Search Another", callback_data="adm_search_user_start")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)




async def handle_adm_recent_purchases(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows real-time monitoring of recent purchases with detailed information."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    # Get pagination offset if provided
    offset = 0
    if params and len(params) > 0 and params[0].isdigit():
        offset = int(params[0])
    
    purchases_per_page = 25
    conn = None
    
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get total count of purchases
        c.execute("SELECT COUNT(*) as count FROM purchases")
        total_purchases = c.fetchone()['count']
        
        # Get recent purchases with user and product details
        c.execute("""
            SELECT 
                p.id,
                p.user_id,
                p.product_type,
                p.product_size,
                p.city,
                p.district,
                p.price_paid,
                p.purchase_date,
                u.username
            FROM purchases p
            LEFT JOIN users u ON p.user_id = u.user_id
            ORDER BY p.purchase_date DESC
            LIMIT ? OFFSET ?
        """, (purchases_per_page, offset))
        
        recent_purchases = c.fetchall()
        
    except sqlite3.Error as e:
        logger.error(f"DB error fetching recent purchases: {e}", exc_info=True)
        await query.edit_message_text("❌ Database error fetching purchases.", parse_mode=None)
        return
    finally:
        if conn:
            conn.close()
    
    # Build the message
    msg = f"📊 Real-Time Purchase Monitor\n\n"
    msg += f"📈 Total Purchases: {total_purchases:,}\n"
    msg += f"📋 Showing {len(recent_purchases)} recent purchases:\n\n"
    
    if not recent_purchases:
        msg += "No purchases found."
    else:
        from utils import PRODUCT_TYPES
        
        for purchase in recent_purchases:
            # Format purchase time
            try:
                # Parse ISO format datetime
                purchase_dt = datetime.fromisoformat(purchase['purchase_date'].replace('Z', '+00:00'))
                # Convert to local time for display
                local_dt = purchase_dt.replace(tzinfo=timezone.utc).astimezone()
                time_str = local_dt.strftime('%m-%d %H:%M')
            except:
                time_str = purchase['purchase_date'][:16] if purchase['purchase_date'] else "Unknown"
            
            # Get product emoji
            product_type = purchase['product_type'] or "Unknown"
            product_emoji = PRODUCT_TYPES.get(product_type, '📦')
            
            # Format buyer info
            username = purchase['username'] or f"ID_{purchase['user_id']}"
            
            # Format location
            city = purchase['city'] or "Unknown"
            district = purchase['district'] or "Unknown"
            
            # Format price
            price = purchase['price_paid'] or 0
            price_str = format_currency(price)
            
            # Format size
            size = purchase['product_size'] or "N/A"
            
            msg += f"🕐 {time_str} | {product_emoji} {product_type} {size}\n"
            msg += f"📍 {city} / {district} | 💰 {price_str}€\n"
            msg += f"👤 @{username}\n"
            msg += f"────────────────\n"
    
    # Add pagination
    keyboard = []
    
    # Pagination controls
    total_pages = math.ceil(total_purchases / purchases_per_page) if total_purchases > 0 else 1
    current_page = (offset // purchases_per_page) + 1
    
    nav_buttons = []
    if current_page > 1:
        prev_offset = max(0, offset - purchases_per_page)
        nav_buttons.append(InlineKeyboardButton("⬅️ Newer", callback_data=f"adm_recent_purchases|{prev_offset}"))
    
    if current_page < total_pages:
        next_offset = offset + purchases_per_page
        nav_buttons.append(InlineKeyboardButton("Older ➡️", callback_data=f"adm_recent_purchases|{next_offset}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Add page info and refresh button
    if total_pages > 1:
        msg += f"\nPage {current_page}/{total_pages}"
    
    keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data="adm_recent_purchases|0")])
    keyboard.append([InlineKeyboardButton("⬅️ Admin meniu", callback_data="admin_menu")])
    
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.error(f"Error editing recent purchases display: {e}")
            await query.answer("Error updating display.", show_alert=True)
        else:
            await query.answer("Refreshed!")
    except Exception as e:
        logger.error(f"Error in recent purchases display: {e}", exc_info=True)
        await query.edit_message_text("❌ Error displaying purchases.", parse_mode=None)

# --- Manual Payment Recovery System ---
async def handle_manual_payment_recovery(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Manual payment recovery for failed payments"""
    query = update.callback_query
    admin_id = query.from_user.id
    
    if not is_primary_admin(admin_id):
        await query.answer("Access Denied.", show_alert=True)
        return
    
    # Set state to expect payment ID
    context.user_data['state'] = 'awaiting_payment_recovery_id'
    
    msg = ("🔧 Manual Payment Recovery\n\n"
           "Enter the payment ID that failed to process:")
    
    keyboard = [[InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter Payment ID in chat.")

async def handle_payment_recovery_id_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle payment ID input for recovery"""
    admin_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if not is_primary_admin(admin_id):
        return
    
    if context.user_data.get("state") != 'awaiting_payment_recovery_id':
        return
    
    if not update.message or not update.message.text:
        return
    
    payment_id = update.message.text.strip()
    context.user_data.pop('state', None)
    
    try:
        # Import required modules
        from utils import get_pending_deposit, remove_pending_deposit
        import payment
        
        # Get pending deposit info
        pending_info = await asyncio.to_thread(get_pending_deposit, payment_id)
        
        if not pending_info:
            await send_message_with_retry(context.bot, chat_id, f"❌ No pending deposit found for payment ID: {payment_id}", parse_mode=None)
            return
        
        user_id = pending_info['user_id']
        basket_snapshot = pending_info.get('basket_snapshot')
        discount_code_used = pending_info.get('discount_code_used')
        
        if not basket_snapshot:
            await send_message_with_retry(context.bot, chat_id, f"❌ No basket snapshot found for payment {payment_id}", parse_mode=None)
            return
        
        # CRITICAL: Verify payment was actually received by NOWPayments
        logger.info(f"Verifying payment {payment_id} with NOWPayments before recovery...")
        payment_verification = await payment.verify_payment_with_nowpayments(payment_id)
        
        if not payment_verification.get('verified'):
            error_reason = payment_verification.get('reason', 'unknown')
            await send_message_with_retry(context.bot, chat_id, 
                f"❌ Payment {payment_id} was NOT received by NOWPayments. Reason: {error_reason}\n\n"
                f"⚠️ Cannot recover payment that was never received. Check NOWPayments dashboard.", 
                parse_mode=None)
            return
        
        # ADDITIONAL SECURITY: Verify payment amount matches expected amount
        actually_paid = payment_verification.get('actually_paid')
        if not actually_paid or actually_paid <= 0:
            await send_message_with_retry(context.bot, chat_id, 
                f"❌ Payment {payment_id} verification failed: No valid payment amount received.\n\n"
                f"⚠️ Cannot recover payment with zero or invalid amount.", 
                parse_mode=None)
            return
        
        logger.info(f"Payment {payment_id} verified with NOWPayments. Status: {payment_verification.get('status')}, Amount: {actually_paid}")
        
        # Check if products are still available before attempting recovery
        logger.info(f"Checking product availability for payment {payment_id}...")
        conn = None
        unavailable_products = []
        try:
            conn = get_db_connection()
            c = conn.cursor()
            
            # Check each product in the basket snapshot
            for item in basket_snapshot:
                product_id = item['product_id']
                c.execute("SELECT available, reserved FROM products WHERE id = ?", (product_id,))
                product_data = c.fetchone()
                
                if not product_data:
                    unavailable_products.append(f"Product ID {product_id} (no longer exists)")
                elif product_data['available'] <= 0:
                    unavailable_products.append(f"Product ID {product_id} (out of stock)")
            
        except Exception as e:
            logger.error(f"Error checking product availability: {e}")
            await send_message_with_retry(context.bot, chat_id, f"❌ Error checking product availability: {str(e)}", parse_mode=None)
            return
        finally:
            if conn:
                conn.close()
        
        # If some products are unavailable, ask admin what to do
        if unavailable_products:
            unavailable_list = "\n".join([f"• {prod}" for prod in unavailable_products])
            await send_message_with_retry(context.bot, chat_id, 
                f"⚠️ Some products are no longer available for payment {payment_id}:\n\n"
                f"{unavailable_list}\n\n"
                f"❓ What would you like to do?\n"
                f"1. Proceed anyway (user gets available products only)\n"
                f"2. Cancel recovery (user keeps their money)\n"
                f"3. Refund user and cancel recovery\n\n"
                f"Reply with '1', '2', or '3'", 
                parse_mode=None)
            
            # Set state to await admin decision
            context.user_data['state'] = 'awaiting_recovery_decision'
            context.user_data['recovery_payment_id'] = payment_id
            context.user_data['recovery_user_id'] = user_id
            context.user_data['recovery_basket_snapshot'] = basket_snapshot
            context.user_data['recovery_discount_code'] = discount_code_used
            context.user_data['recovery_unavailable_products'] = unavailable_products
            return
        
        # All products available, proceed with recovery
        await send_message_with_retry(context.bot, chat_id, f"✅ All products available. Proceeding with recovery for payment {payment_id}...", parse_mode=None)
        
        # Create dummy context for processing
        dummy_context = ContextTypes.DEFAULT_TYPE(application=None, chat_id=user_id, user_id=user_id)
        
        # SECURITY LOG: Admin payment recovery attempt (all products available)
        logger.warning(f"🔐 ADMIN RECOVERY: Admin {admin_id} attempting to recover payment {payment_id} for user {user_id} with {len(basket_snapshot)} products (all available)")
        
        # Attempt to recover the payment
        success = await payment.process_successful_crypto_purchase(
            user_id, basket_snapshot, discount_code_used, payment_id, dummy_context
        )
        
        if success:
            await send_message_with_retry(context.bot, chat_id, f"✅ Successfully recovered payment {payment_id} for user {user_id}", parse_mode=None)
            # Remove pending deposit
            await asyncio.to_thread(remove_pending_deposit, payment_id, trigger="manual_recovery")
        else:
            await send_message_with_retry(context.bot, chat_id, f"❌ Failed to recover payment {payment_id}. Check logs for details.", parse_mode=None)
            
    except Exception as e:
        logger.error(f"Error in manual payment recovery: {e}", exc_info=True)
        await send_message_with_retry(context.bot, chat_id, f"❌ Error during recovery: {str(e)}", parse_mode=None)

async def handle_recovery_decision_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle admin's decision for payment recovery when products are unavailable"""
    admin_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if not is_primary_admin(admin_id):
        return
    
    if context.user_data.get("state") != 'awaiting_recovery_decision':
        return
    
    if not update.message or not update.message.text:
        return
    
    decision = update.message.text.strip()
    context.user_data.pop('state', None)
    
    # Get recovery data from context
    payment_id = context.user_data.get('recovery_payment_id')
    user_id = context.user_data.get('recovery_user_id')
    basket_snapshot = context.user_data.get('recovery_basket_snapshot')
    discount_code_used = context.user_data.get('recovery_discount_code')
    unavailable_products = context.user_data.get('recovery_unavailable_products', [])
    
    # Clean up context
    for key in ['recovery_payment_id', 'recovery_user_id', 'recovery_basket_snapshot', 
                'recovery_discount_code', 'recovery_unavailable_products']:
        context.user_data.pop(key, None)
    
    try:
        if decision == '1':  # Proceed anyway
            await send_message_with_retry(context.bot, chat_id, 
                f"✅ Proceeding with recovery for payment {payment_id} (available products only)...", 
                parse_mode=None)
            
            # Filter out unavailable products from basket snapshot
            available_basket = []
            for item in basket_snapshot:
                product_id = item['product_id']
                # Check if this product is unavailable
                is_unavailable = any(f"Product ID {product_id}" in unavail for unavail in unavailable_products)
                if not is_unavailable:
                    available_basket.append(item)
            
            if not available_basket:
                await send_message_with_retry(context.bot, chat_id, 
                    f"❌ No products available for payment {payment_id}. Cannot proceed.", 
                    parse_mode=None)
                return
            
            # Create dummy context for processing
            dummy_context = ContextTypes.DEFAULT_TYPE(application=None, chat_id=user_id, user_id=user_id)
            
            # SECURITY LOG: Admin payment recovery attempt
            logger.warning(f"🔐 ADMIN RECOVERY: Admin {admin_id} attempting to recover payment {payment_id} for user {user_id} with {len(available_basket)} products")
            
            # Process with available products only
            success = await payment.process_successful_crypto_purchase(
                user_id, available_basket, discount_code_used, payment_id, dummy_context
            )
            
            if success:
                await send_message_with_retry(context.bot, chat_id, 
                    f"✅ Successfully recovered payment {payment_id} for user {user_id} (available products only)", 
                    parse_mode=None)
                # Remove pending deposit
                await asyncio.to_thread(remove_pending_deposit, payment_id, trigger="manual_recovery_partial")
            else:
                await send_message_with_retry(context.bot, chat_id, 
                    f"❌ Failed to recover payment {payment_id}. Check logs for details.", 
                    parse_mode=None)
        
        elif decision == '2':  # Cancel recovery (user keeps money)
            await send_message_with_retry(context.bot, chat_id, 
                f"❌ Recovery cancelled for payment {payment_id}. User keeps their money.\n\n"
                f"⚠️ Products remain available for other customers.", 
                parse_mode=None)
            
            # Remove pending deposit without processing
            await asyncio.to_thread(remove_pending_deposit, payment_id, trigger="manual_recovery_cancelled")
        
        elif decision == '3':  # Refund user and cancel recovery
            await send_message_with_retry(context.bot, chat_id, 
                f"💰 Refund initiated for payment {payment_id}. User will receive their money back.\n\n"
                f"⚠️ Manual refund required through NOWPayments dashboard or crypto wallet.", 
                parse_mode=None)
            
            # Remove pending deposit
            await asyncio.to_thread(remove_pending_deposit, payment_id, trigger="manual_recovery_refund")
            
            # TODO: Implement automatic refund if possible with NOWPayments API
        
        else:
            await send_message_with_retry(context.bot, chat_id, 
                f"❌ Invalid choice '{decision}'. Please use '1', '2', or '3'.", 
                parse_mode=None)
            
            # Restore state to await decision again
            context.user_data['state'] = 'awaiting_recovery_decision'
            context.user_data['recovery_payment_id'] = payment_id
            context.user_data['recovery_user_id'] = user_id
            context.user_data['recovery_basket_snapshot'] = basket_snapshot
            context.user_data['recovery_discount_code'] = discount_code_used
            context.user_data['recovery_unavailable_products'] = unavailable_products
            
    except Exception as e:
        logger.error(f"Error handling recovery decision: {e}", exc_info=True)
        await send_message_with_retry(context.bot, chat_id, f"❌ Error processing decision: {str(e)}", parse_mode=None)


async def handle_adm_edit_type_name_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles new product type name input for editing existing type."""
    if not is_primary_admin(update.effective_user.id): return
    if not update.message or not update.message.text: return
    
    new_type_name = update.message.text.strip()
    if not new_type_name:
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            "❌ Please enter a valid type name.", parse_mode=None)
        return
    
    old_type_name = context.user_data.get("edit_old_type_name")
    if not old_type_name:
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            "❌ Error: No type being edited. Please start over.", parse_mode=None)
        return
    
    # Check if new name already exists
    load_all_data()
    if new_type_name in PRODUCT_TYPES and new_type_name != old_type_name:
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            f"❌ Product type '{new_type_name}' already exists. Please choose a different name.", parse_mode=None)
        return
    
    # If name is the same, just cancel
    if new_type_name == old_type_name:
        await send_message_with_retry(context.bot, update.effective_chat.id, 
            f"ℹ️ Name is the same as current name. No changes made.", parse_mode=None)
        # Return to edit menu
        await handle_adm_edit_type_menu(update, context, [old_type_name])
        return
    
    # Show confirmation before making changes
    context.user_data["edit_new_type_name"] = new_type_name
    context.user_data["state"] = "awaiting_edit_type_name_confirm"
    
    keyboard = [
        [InlineKeyboardButton("✅ Confirm Change", callback_data="adm_confirm_type_name_change")],
        [InlineKeyboardButton("❌ Atšaukti", callback_data=f"adm_edit_type_menu|{old_type_name}")]
    ]
    
    await send_message_with_retry(context.bot, update.effective_chat.id, 
        f"⚠️ CONFIRM TYPE NAME CHANGE\n\n"
        f"Old Name: {old_type_name}\n"
        f"New Name: {new_type_name}\n\n"
        f"This will update ALL products and reseller discounts using this type!\n\n"
        f"Are you sure you want to proceed?",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def handle_adm_confirm_type_name_change(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles confirmation of type name change."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    old_type_name = context.user_data.get("edit_old_type_name")
    new_type_name = context.user_data.get("edit_new_type_name")
    
    if not old_type_name or not new_type_name:
        await query.answer("Error: Missing type names.", show_alert=True)
        return
    
    # Update all database references
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Start transaction
        c.execute("BEGIN TRANSACTION")
        
        # Update products table
        products_updated = c.execute("UPDATE products SET product_type = ? WHERE product_type = ?", 
                                   (new_type_name, old_type_name)).rowcount
        
        # Update reseller_discounts table
        reseller_updated = c.execute("UPDATE reseller_discounts SET product_type = ? WHERE product_type = ?", 
                                   (new_type_name, old_type_name)).rowcount
        
        # Update product_types table
        c.execute("UPDATE product_types SET name = ? WHERE name = ?", (new_type_name, old_type_name))
        
        # Commit transaction
        c.execute("COMMIT")
        conn.commit()
        
        # Reload data
        load_all_data()
        
        # Clear user data
        context.user_data.pop("state", None)
        context.user_data.pop("edit_old_type_name", None)
        context.user_data.pop("edit_new_type_name", None)
        
        # Log admin action
        log_admin_action(admin_id=query.from_user.id, action="PRODUCT_TYPE_RENAME", 
                        reason=f"Renamed type from '{old_type_name}' to '{new_type_name}'", 
                        old_value=old_type_name, new_value=new_type_name)
        
        # Show success message
        success_msg = (f"✅ Type name changed successfully!\n\n"
                      f"Old Name: {old_type_name}\n"
                      f"New Name: {new_type_name}\n\n"
                      f"Updated: {products_updated} products, {reseller_updated} reseller discounts")
        
        keyboard = [
            [InlineKeyboardButton("✏️ Change Emoji", callback_data=f"adm_change_type_emoji|{new_type_name}")],
            [InlineKeyboardButton("📝 Change Name", callback_data=f"adm_change_type_name|{new_type_name}")],
            [InlineKeyboardButton("🗑️ Delete Type", callback_data=f"adm_delete_type|{new_type_name}")],
            [InlineKeyboardButton("⬅️ Back to Manage Types", callback_data="adm_manage_types")]
        ]
        
        await query.edit_message_text(
            f"{success_msg}\n\n🧩 Editing Type: {new_type_name}\n\nWhat would you like to do?",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None
        )
        
    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Error changing type name from '{old_type_name}' to '{new_type_name}': {e}")
        await query.edit_message_text(
            f"❌ Error changing type name: {str(e)}\n\nPlease try again.",
            parse_mode=None
        )
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Unexpected error changing type name: {e}")
        await query.edit_message_text(
            f"❌ Unexpected error: {str(e)}\n\nPlease try again.",
            parse_mode=None
        )
    finally:
        if conn:
            conn.close()


# === LOG ANALYSIS FUNCTIONS FOR SECONDARY ADMINS ===

async def handle_adm_analyze_logs_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start the log analysis process - SECONDARY ADMIN ONLY"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # SECURITY: Only secondary admins can access this feature
    if not is_secondary_admin(user_id):
        return await query.answer("Prieiga uždrausta. This feature is for secondary admins only.", show_alert=True)
    
    await query.answer()
    
    # Set state for file upload
    context.user_data['state'] = 'awaiting_render_logs'
    
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="admin_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    message = (
        "📋 **RENDER LOG ANALYSIS** (Secondary Admin)\n\n"
        "🔍 This tool analyzes render logs to find customers who bought multiple items "
        "but only received 1 item due to the delivery bug.\n\n"
        "📁 **Upload your render log file** (.txt or .log, max 20MB)\n\n"
        "⚠️ **What this tool does:**\n"
        "• Searches for payment completion patterns in logs\n"
        "• If no patterns found, analyzes recent database purchases\n"
        "• Identifies multi-item purchases with missing products\n" 
        "• Shows missing products with full details\n"
        "• Displays product photos/videos/text\n"
        "• Lists affected customers\n\n"
        "💡 **Tip**: Upload logs from when the delivery bug was active, or the tool will analyze recent database records.\n\n"
        "🛡️ **SAFE**: Analysis only - no automatic sending!"
    )
    
    await query.edit_message_text(message, reply_markup=reply_markup, parse_mode=None)


async def handle_adm_render_logs_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle uploaded render log file - SECONDARY ADMIN ONLY"""
    user_id = update.effective_user.id
    
    # SECURITY: Only secondary admins can access this feature
    if not is_secondary_admin(user_id):
        await send_message_with_retry(context.bot, update.effective_chat.id, "Prieiga uždrausta. This feature is for secondary admins only.", parse_mode=None)
        return
    
    # Check if user is in the right state
    if context.user_data.get('state') != 'awaiting_render_logs':
        return
    
    # Clear state
    context.user_data.pop('state', None)
    
    # Check if message has a document
    if not update.message.document:
        keyboard = [[InlineKeyboardButton("🔙 Atgal į Admin meniu", callback_data="admin_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await send_message_with_retry(
            context.bot, 
            update.effective_chat.id,
            "❌ Please upload a text file (.txt or .log) containing your render logs.\n\n"
            "Try again by clicking 'Analyze Render Logs' in the admin menu.",
            reply_markup=reply_markup,
            parse_mode=None
        )
        return
    
    document = update.message.document
    
    # Validate file type
    if not (document.file_name.endswith('.txt') or document.file_name.endswith('.log')):
        keyboard = [[InlineKeyboardButton("🔙 Atgal į Admin meniu", callback_data="admin_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await send_message_with_retry(
            context.bot,
            update.effective_chat.id, 
            f"❌ Invalid file type: {document.file_name}\n\n"
            "Please upload a .txt or .log file containing render logs.",
            reply_markup=reply_markup,
            parse_mode=None
        )
        return
    
    # Validate file size (max 20MB)
    if document.file_size > 20 * 1024 * 1024:
        keyboard = [[InlineKeyboardButton("🔙 Atgal į Admin meniu", callback_data="admin_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await send_message_with_retry(
            context.bot,
            update.effective_chat.id,
            f"❌ File too large: {document.file_size / (1024*1024):.1f}MB\n\n"
            "Please upload a file smaller than 20MB.",
            reply_markup=reply_markup,
            parse_mode=None
        )
        return
    
    try:
        # Send processing message
        processing_msg = await send_message_with_retry(
            context.bot,
            update.effective_chat.id,
            "🔄 Processing render logs...\n\n"
            "📊 Analyzing purchase data\n"
            "🔍 Identifying missing products\n"
            "📋 Generating report...",
            parse_mode=None
        )
        
        # Download and process the file
        file = await context.bot.get_file(document.file_id)
        
        # Download file content
        import io
        file_content = io.BytesIO()
        await file.download_to_memory(file_content)
        file_content.seek(0)
        
        # Decode content
        log_content = file_content.read().decode('utf-8', errors='ignore')
        
        # Analyze logs for missing products
        try:
            analysis_result = await analyze_render_logs(log_content)
        except Exception as analysis_error:
            logger.error(f"Error in log analysis: {analysis_error}", exc_info=True)
            analysis_result = {
                "error": f"Analysis failed: {str(analysis_error)}",
                "affected_users": {},
                "total_missing": 0,
                "total_value": 0.0
            }
        
        # Delete processing message
        try:
            await context.bot.delete_message(update.effective_chat.id, processing_msg.message_id)
        except:
            pass
        
        # Send results
        await send_log_analysis_results(context.bot, update.effective_chat.id, analysis_result)
            
    except Exception as e:
        logger.error(f"Error processing render logs: {e}", exc_info=True)
        
        keyboard = [[InlineKeyboardButton("🔙 Atgal į Admin meniu", callback_data="admin_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await send_message_with_retry(
            context.bot,
            update.effective_chat.id,
            f"❌ Error processing log file: {str(e)}\n\n"
            "Please try again with a valid render log file.",
            reply_markup=reply_markup,
            parse_mode=None
        )


async def analyze_render_logs(log_content: str) -> dict:
    """Analyze render logs to find missing products from multi-item purchases"""
    import re
    from datetime import datetime, timedelta
    
    # Find successful payment processing entries - try multiple patterns
    patterns = [
        r'Successfully processed and removed pending deposit (\d+)',
        r'Successfully processed.*payment.*(\d+)',
        r'Payment.*(\d+).*confirmed',
        r'BULLETPROOF.*Processing.*confirmed.*payment.*(\d+)',
        r'REFILL.*(\d+).*User.*paid',
        r'Successfully.*payment.*(\d+)'
    ]
    
    successful_payments = []
    for pattern in patterns:
        matches = re.findall(pattern, log_content, re.IGNORECASE)
        successful_payments.extend(matches)
    
    # Remove duplicates
    successful_payments = list(set(successful_payments))
    
    if not successful_payments:
        # Try alternative analysis - look for any purchase-related patterns
        return await analyze_logs_alternative(log_content)
    
    # Get database connection
    conn = get_db_connection()
    if not conn:
        return {"error": "Database connection failed"}
    
    try:
        c = conn.cursor()
        affected_users = {}
        total_missing = 0
        total_value = 0.0
        
        for payment_id in successful_payments:
            # Find purchases for this payment within a reasonable time window
            # Look for purchases made around the time of the log entry
            # Since purchases table doesn't have payment_id, we'll need to find purchases by user and time
            # This is a simplified approach - in reality we'd need more sophisticated matching
            c.execute("""
                SELECT user_id, product_id, purchase_date, price_paid
                FROM purchases 
                WHERE purchase_date >= datetime('now', '-1 hour')
                ORDER BY purchase_date DESC
                LIMIT 50
            """)
            
            purchases = c.fetchall()
            
            if len(purchases) > 1:  # Multi-item purchase
                user_id = purchases[0][0]
                purchase_date = purchases[0][2]
                total_paid = purchases[0][3]
                
                # Get user details
                c.execute("SELECT username, first_name FROM users WHERE user_id = ?", (user_id,))
                user_info = c.fetchone()
                username = user_info[0] if user_info else "Unknown"
                first_name = user_info[1] if user_info else "Unknown"
                
                # Get detailed product information for each missing product
                missing_products = []
                for purchase in purchases[1:]:  # Skip first item (they received it)
                    product_id = purchase[1]
                    
                    # Get product details
                    c.execute("""
                        SELECT name, type, price, location, original_text
                        FROM products 
                        WHERE id = ?
                    """, (product_id,))
                    product_info = c.fetchone()
                    
                    logger.debug(f"Product {product_id} info: {product_info}")
                    
                    if product_info:
                        # Get media files
                        c.execute("""
                            SELECT file_path, media_type
                            FROM product_media 
                            WHERE product_id = ?
                        """, (product_id,))
                        media_files = c.fetchall()
                        
                        missing_products.append({
                            "product_id": product_id,
                            "name": product_info[0],
                            "type": product_info[1],
                            "price": product_info[2],
                            "location": product_info[3],
                            "original_text": product_info[4],
                            "media_files": [{"path": m[0], "type": m[1]} for m in media_files],
                            "purchase_date": purchase_date
                        })
                
                if missing_products:
                    affected_users[user_id] = {
                        "username": username,
                        "first_name": first_name,
                        "missing_products": missing_products,
                        "total_paid": total_paid,
                        "purchase_date": purchase_date
                    }
                    total_missing += len(missing_products)
                    total_value += sum(p["price"] for p in missing_products)
        
        return {
            "affected_users": affected_users,
            "total_missing": total_missing,
            "total_value": total_value
        }
        
    finally:
        conn.close()


async def send_log_analysis_results(bot, chat_id: int, analysis_result: dict):
    """Send log analysis results to admin - SIMPLE VERSION"""
    if "error" in analysis_result:
        keyboard = [[InlineKeyboardButton("🔙 Atgal į Admin meniu", callback_data="admin_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await send_message_with_retry(
            bot, chat_id,
            f"❌ Analysis failed: {analysis_result['error']}",
            reply_markup=reply_markup,
            parse_mode=None
        )
        return
    
    affected_users = analysis_result["affected_users"]
    total_missing = analysis_result["total_missing"]
    total_value = analysis_result["total_value"]
    note = analysis_result.get("note", "")
    
    if not affected_users:
        keyboard = [[InlineKeyboardButton("🔙 Atgal į Admin meniu", callback_data="admin_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if note:
            message = f"✅ **LOG ANALYSIS COMPLETE** 🔍\n\n{note}\n\nNo multi-item purchases with missing products found. All customers appear to have received their complete orders."
        else:
            message = "✅ **LOG ANALYSIS COMPLETE** 🔍\n\nNo multi-item purchases with missing products found. All customers appear to have received their complete orders."
        
        await send_message_with_retry(
            bot, chat_id,
            message,
            reply_markup=reply_markup,
            parse_mode=None
        )
        return
    
    # Send summary
    summary_msg = (
        f"✅ **LOG ANALYSIS COMPLETE** 🔍\n\n"
        f"**📊 SUMMARY:**\n"
        f"• **Affected Users:** {len(affected_users)}\n"
        f"• **Missing Products:** {total_missing}\n"
        f"• **Total Value:** €{total_value:.2f}\n\n"
        f"{note}\n\n"
        f"**📋 MISSING PRODUCTS DETAILS:**"
    )
    
    await send_message_with_retry(
        bot, chat_id,
        summary_msg,
        parse_mode=None
    )
    
    # Send ALL missing products in simple format
    all_products_text = ""
    product_count = 0
    
    for user_id, user_data in affected_users.items():
        username = user_data.get("username", "Unknown")
        missing_products = user_data.get("missing_products", [])
        total_paid = user_data.get("total_paid", 0.0)
        purchase_date = user_data.get("purchase_date", "Unknown")
        
        all_products_text += f"\n👤 **USER:** @{username}\n"
        all_products_text += f"🆔 ID: {user_id}\n"
        all_products_text += f"💰 Total Paid: €{total_paid:.2f}\n"
        all_products_text += f"📅 Date: {purchase_date}\n"
        all_products_text += f"📦 Missing Products: {len(missing_products)}\n\n"
        
        for i, product in enumerate(missing_products, 1):
            product_count += 1
            all_products_text += f"**{product_count}. {product['name']}**\n"
            all_products_text += f"   Type: {product['type']}\n"
            all_products_text += f"   Price: €{product['price']}\n"
            all_products_text += f"   Location: {product['location']}\n"
            all_products_text += f"   Details: {product['original_text']}\n\n"
        
        all_products_text += "─" * 40 + "\n"
    
    # Send all products (split if too long)
    if len(all_products_text) > 4000:
        chunks = [all_products_text[i:i+4000] for i in range(0, len(all_products_text), 4000)]
        for chunk in chunks:
            await send_message_with_retry(bot, chat_id, chunk, parse_mode=None)
    else:
        await send_message_with_retry(bot, chat_id, all_products_text, parse_mode=None)
    
    # Add back button
    keyboard = [[InlineKeyboardButton("🔙 Atgal į Admin meniu", callback_data="admin_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await send_message_with_retry(bot, chat_id, "Analysis complete!", reply_markup=reply_markup, parse_mode=None)


async def send_user_missing_products(bot, chat_id: int, user_id: int, user_data: dict):
    """Send detailed missing product information for a specific user"""
    username = user_data["username"]
    first_name = user_data["first_name"]
    missing_products = user_data["missing_products"]
    total_paid = user_data["total_paid"]
    purchase_date = user_data["purchase_date"]
    
    # User header
    user_header = (
        f"👤 **AFFECTED CUSTOMER**\n"
        f"• ID: `{user_id}`\n"
        f"• Username: @{username}\n"
        f"• Name: {first_name}\n"
        f"• Missing Products: {len(missing_products)}\n"
        f"• Total Paid: €{total_paid:.2f}\n"
        f"• Purchase Date: {purchase_date}\n"
        f"{'─' * 30}\n"
    )
    
    await send_message_with_retry(bot, chat_id, user_header, parse_mode=None)
    
    # Send each missing product
    for i, product in enumerate(missing_products, 1):
        # Product header
        product_header = (
            f"📦 **MISSING PRODUCT #{i}**\n"
            f"• ID: {product['product_id']}\n"
            f"• Name: {product['name']}\n"
            f"• Type: {product['type']}\n"
            f"• Price: €{product['price']:.2f}\n"
            f"• Location: {product['location']}\n"
            f"• Purchase Date: {product['purchase_date']}\n"
            f"{'─' * 20}\n"
        )
        
        await send_message_with_retry(bot, chat_id, product_header, parse_mode=None)
        
        # Send media files if available
        for media in product["media_files"]:
            try:
                if media["type"] == "photo" and os.path.exists(media["path"]):
                    with open(media["path"], "rb") as f:
                        await bot.send_photo(chat_id, photo=f)
                elif media["type"] == "video" and os.path.exists(media["path"]):
                    with open(media["path"], "rb") as f:
                        await bot.send_video(chat_id, video=f)
                elif media["type"] == "animation" and os.path.exists(media["path"]):
                    with open(media["path"], "rb") as f:
                        await bot.send_animation(chat_id, animation=f)
            except Exception as e:
                logger.error(f"Error sending media for product {product['product_id']}: {e}")
        
        # Send original text
        if product["original_text"]:
            await send_message_with_retry(
                bot, chat_id,
                f"📝 **Original Product Text:**\n{product['original_text']}",
                parse_mode=None
            )
        
        # Separator between products
        if i < len(missing_products):
            await send_message_with_retry(bot, chat_id, "─" * 30, parse_mode=None)


async def analyze_logs_alternative(log_content: str) -> dict:
    """Simple log analysis - just find all recent multi-item purchases and show details"""
    
    # Get database connection
    conn = get_db_connection()
    if not conn:
        return {"error": "Database connection failed"}
    
    try:
        c = conn.cursor()
        
        # Simple query: get all recent multi-item purchases with full details
        c.execute("""
            SELECT user_id, COUNT(*) as item_count, 
                   GROUP_CONCAT(product_name || '|' || product_type || '|' || CAST(price_paid AS TEXT) || '|' || city || '|' || district) as product_details,
                   MAX(purchase_date) as last_purchase, SUM(price_paid) as total_paid
            FROM purchases 
            WHERE purchase_date >= datetime('now', '-7 days')
            GROUP BY user_id, purchase_date
            HAVING COUNT(*) > 1
            ORDER BY last_purchase DESC
            LIMIT 20
        """)
        
        multi_item_purchases = c.fetchall()
        logger.info(f"Found {len(multi_item_purchases)} multi-item purchases")
        
        if not multi_item_purchases:
            return {
                "affected_users": {}, 
                "total_missing": 0, 
                "total_value": 0.0, 
                "note": "No recent multi-item purchases found in database"
            }
        
        # Process each multi-item purchase
        affected_users = {}
        total_missing = 0
        total_value = 0.0
        
        for purchase in multi_item_purchases:
            user_id = purchase[0]
            item_count = purchase[1]
            product_details_str = purchase[2]
            last_purchase = purchase[3]
            total_paid = purchase[4]
            
            # Get user details
            c.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
            user_info = c.fetchone()
            username = user_info[0] if user_info else "Unknown"
            
            # Parse product details (skip first item - assume it was delivered)
            product_details = product_details_str.split(',')
            missing_products = []
            
            for i, detail_str in enumerate(product_details[1:], 1):  # Skip first item
                if '|' in detail_str:
                    parts = detail_str.split('|')
                    if len(parts) >= 5:
                        product_name = parts[0]
                        product_type = parts[1]
                        product_price = float(parts[2])
                        city = parts[3]
                        district = parts[4]
                        
                        missing_products.append({
                            "product_id": f"purchase_{i}",
                            "name": product_name,
                            "type": product_type,
                            "price": product_price,
                            "location": f"{city}, {district}",
                            "original_text": f"Product: {product_name}\nType: {product_type}\nPrice: €{product_price}\nLocation: {city}, {district}",
                            "media_files": [],  # No media available from purchase records
                            "purchase_date": last_purchase
                        })
            
            if missing_products:
                affected_users[user_id] = {
                    "username": username,
                    "missing_products": missing_products,
                    "total_paid": total_paid,
                    "purchase_date": last_purchase
                }
                total_missing += len(missing_products)
                total_value += sum(p["price"] for p in missing_products)
        
        return {
            "affected_users": affected_users,
            "total_missing": total_missing,
            "total_value": total_value,
            "note": f"Found {len(multi_item_purchases)} recent multi-item purchases. Analysis based on database records."
        }
        
    finally:
        conn.close()


# ============================================================================
# BULK PRICE EDITOR - Admin can set new prices for products by type
# ============================================================================

async def handle_adm_bulk_edit_prices_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Display list of product types for bulk price editing."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    load_all_data()  # Ensure PRODUCT_TYPES is up-to-date
    
    if not PRODUCT_TYPES:
        msg = "💰 Bulk Edit Prices\n\nNo product types available."
        keyboard = [[InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")]]
        return await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    
    msg = "💰 Bulk Edit Prices\n\nSelect the product type you want to update:"
    keyboard = []
    
    for type_name, emoji in sorted(PRODUCT_TYPES.items()):
        keyboard.append([InlineKeyboardButton(f"{emoji} {type_name}", callback_data=f"adm_bulk_price_type|{type_name}")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")])
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def handle_adm_bulk_price_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Store product type and show scope selection."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params or len(params) < 1:
        return await query.answer("Error: Product type not specified.", show_alert=True)
    
    product_type = params[0]
    context.user_data['bulk_price_type'] = product_type
    
    emoji = PRODUCT_TYPES.get(product_type, "📦")
    msg = f"💰 Bulk Edit Prices\n\nProduct Type: {emoji} {product_type}\n\nSelect the scope for price update:"
    
    keyboard = [
        [InlineKeyboardButton("🌍 All Cities and Districts", callback_data="adm_bulk_price_scope|all")],
        [InlineKeyboardButton("🏙️ Specific City (all districts)", callback_data="adm_bulk_price_scope|city")],
        [InlineKeyboardButton("📍 Specific City + District", callback_data="adm_bulk_price_scope|district")],
        [InlineKeyboardButton("⬅️ Atgal", callback_data="adm_bulk_edit_prices_start")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def handle_adm_bulk_price_scope(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle scope selection and proceed accordingly."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params or len(params) < 1:
        return await query.answer("Error: Scope not specified.", show_alert=True)
    
    scope = params[0]
    context.user_data['bulk_price_scope'] = scope
    product_type = context.user_data.get('bulk_price_type', 'Unknown')
    emoji = PRODUCT_TYPES.get(product_type, "📦")
    
    if scope == "all":
        # Skip to price input for all locations
        msg = f"💰 Bulk Edit Prices\n\nProduct Type: {emoji} {product_type}\nScope: All Cities and Districts\n\nEnter the new price (in EUR):"
        context.user_data['state'] = 'awaiting_bulk_price_value'
        keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="admin_menu")]]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    
    elif scope == "city" or scope == "district":
        # Show city selection
        if not CITIES:
            msg = "No cities configured. Please add cities first."
            keyboard = [[InlineKeyboardButton("⬅️ Atgal", callback_data="adm_bulk_edit_prices_start")]]
            return await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
        scope_text = "City (all districts)" if scope == "city" else "City + District"
        msg = f"💰 Bulk Edit Prices\n\nProduct Type: {emoji} {product_type}\nScope: {scope_text}\n\nSelect a city:"
        
        sorted_city_ids = sorted(CITIES.keys(), key=lambda city_id: CITIES.get(city_id, ''))
        keyboard = []
        
        for city_id in sorted_city_ids:
            city_name = CITIES.get(city_id, 'Unknown')
            if scope == "city":
                keyboard.append([InlineKeyboardButton(f"🏙️ {city_name}", callback_data=f"adm_bulk_price_city|{city_id}")])
            else:  # district scope
                keyboard.append([InlineKeyboardButton(f"🏙️ {city_name}", callback_data=f"adm_bulk_price_city_for_district|{city_id}")])
        
        keyboard.append([InlineKeyboardButton("⬅️ Atgal", callback_data=f"adm_bulk_price_type|{product_type}")])
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def handle_adm_bulk_price_city(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle city selection for city scope (all districts in city)."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params or len(params) < 1:
        return await query.answer("Error: City not specified.", show_alert=True)
    
    city_id = params[0]
    context.user_data['bulk_price_city'] = city_id
    
    product_type = context.user_data.get('bulk_price_type', 'Unknown')
    city_name = CITIES.get(city_id, 'Unknown')
    emoji = PRODUCT_TYPES.get(product_type, "📦")
    
    msg = f"💰 Bulk Edit Prices\n\nProduct Type: {emoji} {product_type}\nScope: {city_name} (all districts)\n\nEnter the new price (in EUR):"
    context.user_data['state'] = 'awaiting_bulk_price_value'
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="admin_menu")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def handle_adm_bulk_price_city_for_district(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle city selection when district scope is needed."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params or len(params) < 1:
        return await query.answer("Error: City not specified.", show_alert=True)
    
    city_id = params[0]
    context.user_data['bulk_price_city'] = city_id
    
    product_type = context.user_data.get('bulk_price_type', 'Unknown')
    city_name = CITIES.get(city_id, 'Unknown')
    emoji = PRODUCT_TYPES.get(product_type, "📦")
    
    # Show district selection
    city_districts = DISTRICTS.get(city_id, {})
    if not city_districts:
        msg = f"No districts found in {city_name}."
        keyboard = [[InlineKeyboardButton("⬅️ Atgal", callback_data=f"adm_bulk_price_type|{product_type}")]]
        return await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    
    msg = f"💰 Bulk Edit Prices\n\nProduct Type: {emoji} {product_type}\nScope: City + District\nCity: {city_name}\n\nSelect a district:"
    
    sorted_district_ids = sorted(city_districts.keys(), key=lambda dist_id: city_districts.get(dist_id, ''))
    keyboard = []
    
    for district_id in sorted_district_ids:
        district_name = city_districts.get(district_id, 'Unknown')
        keyboard.append([InlineKeyboardButton(f"📍 {district_name}", callback_data=f"adm_bulk_price_district|{district_id}")])
    
    keyboard.append([InlineKeyboardButton("⬅️ Atgal", callback_data=f"adm_bulk_price_type|{product_type}")])
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def handle_adm_bulk_price_district(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle district selection for district scope - show individual products."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params or len(params) < 1:
        return await query.answer("Error: District not specified.", show_alert=True)
    
    district_id = params[0]
    context.user_data['bulk_price_district'] = district_id
    
    product_type = context.user_data.get('bulk_price_type', 'Unknown')
    city_id = context.user_data.get('bulk_price_city', '')
    city_name = CITIES.get(city_id, 'Unknown')
    district_name = DISTRICTS.get(city_id, {}).get(district_id, 'Unknown')
    emoji = PRODUCT_TYPES.get(product_type, "📦")
    
    # Database stores city/district NAMES, not IDs!
    # Convert IDs to names for database query
    
    # Fetch individual products from database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Debug logging
        logger.info(f"Price editor searching for products: type='{product_type}', city='{city_name}' (id: {city_id}), district='{district_name}' (id: {district_id})")
        
        c.execute("""
            SELECT id, size, price, available, reserved 
            FROM products 
            WHERE product_type = ? AND city = ? AND district = ?
            ORDER BY price, size
        """, (product_type, city_name, district_name))
        products = c.fetchall()
        
        logger.info(f"Price editor found {len(products)} products for type='{product_type}', city='{city_id}', district='{district_id}'")
        
        if not products:
            # Try to debug why no products found
            c.execute("SELECT DISTINCT product_type, city, district FROM products LIMIT 20")
            sample_products = c.fetchall()
            sample_data = [(row['product_type'], row['city'], row['district']) for row in sample_products]
            logger.warning(f"No products found for price editor. Searched: type='{product_type}', city='{city_id}', district='{district_id}'")
            logger.warning(f"Sample of existing products in DB: {sample_data}")
            
            msg = f"❌ No products found:\n\nType: {emoji} {product_type}\nLocation: {city_name} - {district_name}"
            keyboard = [[InlineKeyboardButton("⬅️ Atgal", callback_data=f"adm_bulk_price_city_for_district|{city_id}")]]
            return await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
        # Show list of products to select from
        msg = f"💰 Edit Individual Product Price\n\n"
        msg += f"Type: {emoji} {product_type}\n"
        msg += f"Location: {city_name} - {district_name}\n\n"
        msg += f"Select the product to edit:\n"
        
        keyboard = []
        for product in products:
            product_label = f"{product['size']} - €{product['price']:.2f} (Stock: {product['available']})"
            keyboard.append([InlineKeyboardButton(product_label, callback_data=f"adm_edit_single_price|{product['id']}")])
        
        keyboard.append([InlineKeyboardButton("⬅️ Atgal", callback_data=f"adm_bulk_price_city_for_district|{city_id}")])
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
    except Exception as e:
        logger.error(f"Error fetching products for price edit: {e}", exc_info=True)
        msg = "❌ Error loading products. Please try again."
        keyboard = [[InlineKeyboardButton("⬅️ Atgal į Admin", callback_data="admin_menu")]]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    finally:
        if conn:
            conn.close()


async def handle_adm_edit_single_price(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle selection of specific product size to edit price (updates all products of that size in location)."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    if not params or len(params) < 1:
        return await query.answer("Error: Product not specified.", show_alert=True)
    
    product_id = int(params[0])
    
    # Fetch product details to get the size, type, city, district
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT id, size, price, product_type, city, district, available, reserved
            FROM products WHERE id = ?
        """, (product_id,))
        product = c.fetchone()
        
        if not product:
            msg = "❌ Product not found. It may have been deleted."
            keyboard = [[InlineKeyboardButton("⬅️ Atgal į Admin", callback_data="admin_menu")]]
            return await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
        # Store the criteria (not just product_id) so we can update ALL matching products
        # Database uses city/district NAMES, not IDs - store names directly from product
        context.user_data['edit_price_type'] = product['product_type']
        context.user_data['edit_price_size'] = product['size']
        context.user_data['edit_price_city'] = product['city']  # This is already the city NAME from DB
        context.user_data['edit_price_district'] = product['district']  # This is already the district NAME from DB
        
        # Count how many products match this criteria
        c.execute("""
            SELECT COUNT(*) as count, SUM(available) as total_stock
            FROM products 
            WHERE product_type = ? AND size = ? AND city = ? AND district = ?
        """, (product['product_type'], product['size'], product['city'], product['district']))
        count_result = c.fetchone()
        product_count = count_result['count'] if count_result else 0
        total_stock = count_result['total_stock'] if count_result else 0
        
        # City and district in DB are stored as names, not IDs
        # So we need to find them in CITIES/DISTRICTS by value, not key
        city_name = product['city']  # Already the name
        district_name = product['district']  # Already the name
        
        # Get emoji from PRODUCT_TYPES
        emoji = PRODUCT_TYPES.get(product['product_type'], "📦")
        
        msg = f"💰 Edit Product Price\n\n"
        msg += f"Type: {emoji} {product['product_type']}\n"
        msg += f"Size: {product['size']}\n"
        msg += f"Location: {city_name} - {district_name}\n"
        msg += f"Current Price: €{product['price']:.2f}\n"
        msg += f"Total Stock: {total_stock} items ({product_count} products)\n\n"
        msg += f"⚠️ This will update ALL {product_count} products of this size in this location.\n\n"
        msg += f"Enter the new price (in EUR):"
        
        context.user_data['state'] = 'awaiting_single_price_edit'
        keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="admin_menu")]]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
    except Exception as e:
        logger.error(f"Error fetching product {product_id} for price edit: {e}", exc_info=True)
        msg = "❌ Error loading product. Please try again."
        keyboard = [[InlineKeyboardButton("⬅️ Atgal į Admin", callback_data="admin_menu")]]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    finally:
        if conn:
            conn.close()


async def handle_adm_single_price_edit_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle price input - updates ALL products matching the size/type/location."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if not is_primary_admin(user_id):
        return
    
    if context.user_data.get('state') != 'awaiting_single_price_edit':
        return
    
    # Validate price input
    try:
        new_price = float(update.message.text.strip())
        if new_price <= 0:
            await send_message_with_retry(context.bot, chat_id, "❌ Error: Price must be greater than 0. Please try again:", parse_mode=None)
            return
    except ValueError:
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Invalid price format. Please enter a valid number (e.g., 7.50):", parse_mode=None)
        return
    
    # Get the stored criteria
    product_type = context.user_data.get('edit_price_type')
    size = context.user_data.get('edit_price_size')
    city = context.user_data.get('edit_price_city')
    district = context.user_data.get('edit_price_district')
    
    if not all([product_type, size, city, district]):
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Session expired. Please start again.", parse_mode=None)
        context.user_data.pop('state', None)
        return
    
    # Update ALL products matching the criteria
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get old price (any one of them, they should all be the same)
        c.execute("""
            SELECT price FROM products 
            WHERE product_type = ? AND size = ? AND city = ? AND district = ?
            LIMIT 1
        """, (product_type, size, city, district))
        old_price_result = c.fetchone()
        
        if not old_price_result:
            await send_message_with_retry(context.bot, chat_id, "❌ Error: Products not found.", parse_mode=None)
            context.user_data.pop('state', None)
            return
        
        old_price = old_price_result['price']
        
        # Update ALL matching products
        result = c.execute("""
            UPDATE products 
            SET price = ? 
            WHERE product_type = ? AND size = ? AND city = ? AND district = ?
        """, (new_price, product_type, size, city, district))
        
        updated_count = result.rowcount
        conn.commit()
        
        # Log the action
        # city and district variables here are already names from DB
        city_name = city
        district_name = district
        
        # Get emoji from PRODUCT_TYPES
        emoji = PRODUCT_TYPES.get(product_type, "📦")
        
        log_admin_action(
            admin_id=user_id,
            action="BULK_SIZE_PRICE_UPDATE",
            target_user_id=None,
            reason=f"{emoji} {product_type} - {size} in {city_name} - {district_name} ({updated_count} products)",
            amount_change=None,
            old_value=float(old_price),
            new_value=float(new_price)
        )
        
        success_msg = f"✅ Price Updated Successfully!\n\n"
        success_msg += f"Product: {emoji} {product_type} - {size}\n"
        success_msg += f"Location: {city_name} - {district_name}\n"
        success_msg += f"Updated: {updated_count} products\n\n"
        success_msg += f"Old Price: €{old_price:.2f}\n"
        success_msg += f"New Price: €{new_price:.2f}"
        
        keyboard = [[InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")]]
        await send_message_with_retry(context.bot, chat_id, success_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
        logger.info(f"Admin {user_id} updated price for {updated_count} products ({product_type} - {size} in {city}/{district}) from €{old_price:.2f} to €{new_price:.2f}")
        
    except Exception as e:
        logger.error(f"Error updating prices: {e}", exc_info=True)
        await send_message_with_retry(context.bot, chat_id, "❌ Error updating price. Please try again.", parse_mode=None)
    finally:
        if conn:
            conn.close()
        context.user_data.pop('state', None)
        context.user_data.pop('edit_price_type', None)
        context.user_data.pop('edit_price_size', None)
        context.user_data.pop('edit_price_city', None)
        context.user_data.pop('edit_price_district', None)


async def handle_adm_bulk_price_value_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle price input and show preview."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if not is_primary_admin(user_id):
        return
    
    # Validate price input
    try:
        new_price = float(update.message.text.strip())
        if new_price <= 0:
            await send_message_with_retry(context.bot, chat_id, "❌ Error: Price must be greater than 0. Please try again:", parse_mode=None)
            return
    except ValueError:
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Invalid price format. Please enter a valid number (e.g., 7.50):", parse_mode=None)
        return
    
    # Store price
    context.user_data['bulk_price_value'] = new_price
    
    # Get context data
    product_type = context.user_data.get('bulk_price_type', 'Unknown')
    scope = context.user_data.get('bulk_price_scope', 'all')
    city_id = context.user_data.get('bulk_price_city', '')
    district_id = context.user_data.get('bulk_price_district', '')
    
    emoji = PRODUCT_TYPES.get(product_type, "📦")
    
    # Build query to count affected products
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Base query
        query = "SELECT city, district, COUNT(*) as count, MIN(price) as min_price, MAX(price) as max_price FROM products WHERE product_type = ?"
        params = [product_type]
        
        # Add scope filters
        if scope == "city":
            query += " AND city = ?"
            params.append(city_id)
        elif scope == "district":
            query += " AND city = ? AND district = ?"
            params.extend([city_id, district_id])
        
        query += " GROUP BY city, district ORDER BY city, district"
        
        c.execute(query, params)
        results = c.fetchall()
        
        if not results:
            msg = f"❌ No products found matching criteria:\n\nProduct Type: {emoji} {product_type}\n"
            if scope == "city":
                msg += f"City: {CITIES.get(city_id, 'Unknown')}\n"
            elif scope == "district":
                msg += f"Location: {CITIES.get(city_id, 'Unknown')} - {DISTRICTS.get(city_id, {}).get(district_id, 'Unknown')}\n"
            else:
                msg += "Scope: All Cities\n"
            
            keyboard = [[InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")]]
            await send_message_with_retry(context.bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
            context.user_data.pop('state', None)
            return
        
        # Build preview message
        total_count = sum(row['count'] for row in results)
        all_min_prices = [row['min_price'] for row in results if row['min_price'] is not None]
        all_max_prices = [row['max_price'] for row in results if row['max_price'] is not None]
        price_range_min = min(all_min_prices) if all_min_prices else 0
        price_range_max = max(all_max_prices) if all_max_prices else 0
        
        # Determine scope description
        if scope == "all":
            scope_desc = "All Cities and Districts"
        elif scope == "city":
            scope_desc = f"{CITIES.get(city_id, 'Unknown')} (all districts)"
        else:  # district
            scope_desc = f"{CITIES.get(city_id, 'Unknown')} - {DISTRICTS.get(city_id, {}).get(district_id, 'Unknown')}"
        
        msg = f"📋 Preview: Bulk Price Update\n\n"
        msg += f"Product Type: {emoji} {product_type}\n"
        msg += f"Scope: {scope_desc}\n"
        msg += f"New Price: €{new_price:.2f}\n\n"
        msg += f"✅ This will update {total_count} product(s)\n\n"
        
        if len(results) <= 5:
            msg += "Breakdown by location:\n"
            for row in results:
                city_name = CITIES.get(row['city'], row['city'])
                district_name = DISTRICTS.get(row['city'], {}).get(row['district'], row['district'])
                msg += f"• {city_name} - {district_name}: {row['count']} product(s)\n"
        else:
            msg += f"Across {len(results)} location(s)\n"
        
        msg += f"\nCurrent price range: €{price_range_min:.2f} - €{price_range_max:.2f}\n\n"
        msg += "⚠️ Confirm to apply changes"
        
        keyboard = [
            [InlineKeyboardButton("✅ Confirm Update", callback_data="adm_bulk_price_confirm")],
            [InlineKeyboardButton("❌ Atšaukti", callback_data="admin_menu")]
        ]
        
        await send_message_with_retry(context.bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        context.user_data.pop('state', None)
        
    except Exception as e:
        logger.error(f"Error in bulk price preview for admin {user_id}: {e}", exc_info=True)
        await send_message_with_retry(context.bot, chat_id, "❌ Database error occurred. Please try again.", parse_mode=None)
        context.user_data.pop('state', None)
    finally:
        if conn:
            conn.close()


async def handle_adm_bulk_price_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Execute the bulk price update."""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_primary_admin(user_id):
        return await query.answer("Prieiga uždrausta.", show_alert=True)
    
    # Get stored data
    product_type = context.user_data.get('bulk_price_type')
    scope = context.user_data.get('bulk_price_scope')
    city_id = context.user_data.get('bulk_price_city', '')
    district_id = context.user_data.get('bulk_price_district', '')
    new_price = context.user_data.get('bulk_price_value')
    
    if not product_type or not new_price:
        await query.answer("Error: Missing required data. Please start over.", show_alert=True)
        return await query.edit_message_text("❌ Error: Session data lost. Please try again.", parse_mode=None)
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Build UPDATE query
        update_query = "UPDATE products SET price = ? WHERE product_type = ?"
        params = [new_price, product_type]
        
        # Add scope filters
        if scope == "city":
            update_query += " AND city = ?"
            params.append(city_id)
        elif scope == "district":
            update_query += " AND city = ? AND district = ?"
            params.extend([city_id, district_id])
        
        # Execute update
        result = c.execute(update_query, params)
        row_count = result.rowcount
        conn.commit()
        
        # Determine scope description for logging
        if scope == "all":
            scope_desc = "All locations"
        elif scope == "city":
            scope_desc = f"City: {CITIES.get(city_id, city_id)}"
        else:  # district
            scope_desc = f"Location: {CITIES.get(city_id, city_id)} - {DISTRICTS.get(city_id, {}).get(district_id, district_id)}"
        
        # Log the action
        log_admin_action(
            admin_id=user_id,
            action=ACTION_BULK_PRICE_UPDATE,
            target_user_id=None,
            reason=f"Type: {product_type}, Scope: {scope_desc}, New Price: €{new_price:.2f}",
            amount_change=None,
            old_value=None,
            new_value=float(new_price)
        )
        
        emoji = PRODUCT_TYPES.get(product_type, "📦")
        success_msg = f"✅ Bulk Price Update Complete!\n\n"
        success_msg += f"Product Type: {emoji} {product_type}\n"
        success_msg += f"New Price: €{new_price:.2f}\n"
        success_msg += f"Scope: {scope_desc}\n\n"
        success_msg += f"✅ Updated {row_count} product(s)"
        
        keyboard = [[InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")]]
        await query.edit_message_text(success_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        
        logger.info(f"Admin {user_id} bulk updated prices: {row_count} products of type '{product_type}' to €{new_price:.2f} (scope: {scope_desc})")
        
    except Exception as e:
        logger.error(f"Error executing bulk price update for admin {user_id}: {e}", exc_info=True)
        if conn and conn.in_transaction:
            conn.rollback()
        error_msg = "❌ Error: Failed to update prices. Please try again or contact support."
        keyboard = [[InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")]]
        await query.edit_message_text(error_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    finally:
        if conn:
            conn.close()
        
        # Clear context data
        context.user_data.pop('bulk_price_type', None)
        context.user_data.pop('bulk_price_scope', None)
        context.user_data.pop('bulk_price_city', None)
        context.user_data.pop('bulk_price_district', None)
        context.user_data.pop('bulk_price_value', None)
        context.user_data.pop('state', None)


# =========================================================================
# STUCK SOL FUNDS RECOVERY HANDLERS
# =========================================================================

async def handle_recover_stuck_funds(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Shows the stuck funds recovery menu with current status."""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Allow both primary and secondary admins
    if not is_any_admin(user_id):
        return await query.answer("Prieiga uždrausta. Admin only.", show_alert=True)
    
    # Determine back menu based on admin type
    back_callback = "admin_menu" if is_primary_admin(user_id) else "viewer_admin_menu"
    
    await query.answer("⏳ Scanning for stuck funds...")
    
    try:
        # Import recovery functions
        from payment_solana import find_stuck_wallets, get_recovery_status, RECOVERY_WALLET, ADMIN_WALLET
        
        # Get recovery status
        status = get_recovery_status()
        target_wallet = RECOVERY_WALLET or ADMIN_WALLET
        
        # Find stuck wallets
        stuck_wallets = await find_stuck_wallets()
        
        if not stuck_wallets:
            msg = "🔄 *Stuck Funds Recovery*\n\n"
            msg += "✅ No stuck funds found in scan!\n\n"
            msg += "*Wallet Configuration:*\n"
            if ADMIN_WALLET:
                msg += f"• Admin Wallet: `{ADMIN_WALLET[:12]}...{ADMIN_WALLET[-8:]}`\n"
            else:
                msg += "• Admin Wallet: ❌ Not Set\n"
            if RECOVERY_WALLET:
                msg += f"• Recovery Wallet: `{RECOVERY_WALLET[:12]}...{RECOVERY_WALLET[-8:]}`\n"
            else:
                msg += "• Recovery Wallet: ❌ Not Set (using Admin)\n"
            msg += f"\n*Target for Recovery:* `{target_wallet[:16]}...` (check Solscan!)\n\n"
            msg += "💡 *Tip:* If you know a specific wallet address with stuck funds, use Quick Recover.\n"
            msg += "💡 Also check Admin Wallet on Solscan - old sweeps went there!"
            
            keyboard = [
                [InlineKeyboardButton("🎯 Quick Recover (Single Wallet)", callback_data="adm_recover_single")],
                [InlineKeyboardButton("🔄 Refresh Full Scan", callback_data="adm_recover_stuck_funds")],
                [InlineKeyboardButton("⬅️ Atgal", callback_data=back_callback)]
            ]
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
            return
        
        # Calculate totals
        total_sol = sum(w['sol_balance'] for w in stuck_wallets)
        total_eur = sum(w['eur_value'] for w in stuck_wallets)
        
        msg = "🔄 *Stuck Funds Recovery*\n\n"
        msg += f"🚨 *Found {len(stuck_wallets)} wallet(s) with stuck funds!*\n\n"
        msg += f"💰 *Total SOL:* `{total_sol:.6f}` SOL\n"
        msg += f"💶 *Total EUR Value:* ~{total_eur:.2f} EUR\n\n"
        msg += f"*Target Wallet:* `{target_wallet[:16]}...{target_wallet[-8:] if target_wallet else 'Not Set'}`\n\n"
        msg += "*Stuck Wallets:*\n"
        
        for i, w in enumerate(stuck_wallets[:10], 1):  # Show max 10
            msg += f"{i}. `{w['public_key'][:12]}...` = {w['sol_balance']:.4f} SOL (~{w['eur_value']:.2f}€)\n"
            msg += f"   User: {w['user_id']} | Status: {w['status']}\n"
        
        if len(stuck_wallets) > 10:
            msg += f"\n... and {len(stuck_wallets) - 10} more wallets\n"
        
        msg += "\n⚠️ *Press 'Recover All' to sweep all funds to target wallet.*"
        
        keyboard = [
            [InlineKeyboardButton(f"✅ Recover All ({total_sol:.4f} SOL)", callback_data="adm_recover_confirm")],
            [InlineKeyboardButton("🎯 Quick Recover (Single Wallet)", callback_data="adm_recover_single")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="adm_recover_stuck_funds")],
            [InlineKeyboardButton("⬅️ Atgal", callback_data=back_callback)]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in handle_recover_stuck_funds: {e}", exc_info=True)
        back_callback = "admin_menu" if is_primary_admin(user_id) else "viewer_admin_menu"
        error_msg = f"❌ Error scanning for stuck funds: {str(e)[:100]}"
        keyboard = [[InlineKeyboardButton("⬅️ Atgal", callback_data=back_callback)]]
        await query.edit_message_text(error_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def handle_recover_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirms and executes the stuck funds recovery."""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Allow both primary and secondary admins
    if not is_any_admin(user_id):
        return await query.answer("Prieiga uždrausta. Admin only.", show_alert=True)
    
    # Determine back menu based on admin type
    back_callback = "admin_menu" if is_primary_admin(user_id) else "viewer_admin_menu"
    
    await query.answer("⏳ Starting recovery process...")
    await query.edit_message_text("🔄 *Recovery in Progress...*\n\nPlease wait, sweeping funds from all stuck wallets...", parse_mode="Markdown")
    
    try:
        # Import and run recovery
        from payment_solana import recover_stuck_funds
        
        result = await recover_stuck_funds()
        
        if result['success']:
            if result.get('recovered'):
                msg = "✅ *Recovery Complete!*\n\n"
                msg += f"💰 *Total Recovered:* `{result['total_sol_recovered']:.6f}` SOL\n"
                msg += f"💶 *EUR Value:* ~{result['total_eur_recovered']:.2f} EUR\n"
                msg += f"📦 *Wallets Recovered:* {result['wallets_recovered']}\n"
                if result['wallets_failed'] > 0:
                    msg += f"⚠️ *Failed:* {result['wallets_failed']}\n"
                msg += f"\n*Target Wallet:* `{result['target_wallet'][:16]}...`\n\n"
                
                # Show first few recovered wallets
                for r in result['recovered'][:5]:
                    msg += f"• `{r['public_key'][:12]}...` → {r['sol_amount']:.4f} SOL\n"
                if len(result['recovered']) > 5:
                    msg += f"... and {len(result['recovered']) - 5} more\n"
            else:
                msg = "✅ *No Stuck Funds Found*\n\nAll wallets have been properly swept."
        else:
            msg = f"❌ *Recovery Failed*\n\nError: {result.get('error', 'Unknown error')}"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Check Again", callback_data="adm_recover_stuck_funds")],
            [InlineKeyboardButton("⬅️ Atgal", callback_data=back_callback)]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in handle_recover_confirm: {e}", exc_info=True)
        back_callback = "admin_menu" if is_primary_admin(user_id) else "viewer_admin_menu"
        error_msg = f"❌ Recovery error: {str(e)[:100]}"
        keyboard = [[InlineKeyboardButton("⬅️ Atgal", callback_data=back_callback)]]
        await query.edit_message_text(error_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def handle_recover_single_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Prompts admin to enter a specific wallet address for direct recovery."""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_any_admin(user_id):
        return await query.answer("Prieiga uždrausta. Admin only.", show_alert=True)
    
    await query.answer()
    
    back_callback = "admin_menu" if is_primary_admin(user_id) else "viewer_admin_menu"
    
    msg = "🎯 *Quick Single Wallet Recovery*\n\n"
    msg += "Enter the Solana wallet address you want to recover funds from.\n\n"
    msg += "This is useful when you know the specific wallet address and want to skip the full scan.\n\n"
    msg += "📋 *Example:* `38RDpfB3zNthSgVCxkLiWEKiV2Mj4MjYUYJZifVyzVda`\n\n"
    msg += "Reply with the wallet address:"
    
    keyboard = [[InlineKeyboardButton("❌ Atšaukti", callback_data="adm_recover_stuck_funds")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    # Set state to await wallet address (using state system for consistency)
    context.user_data['state'] = 'awaiting_recovery_wallet_address'
    context.user_data['recovery_back_callback'] = back_callback


async def handle_recovery_wallet_address_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the wallet address input for single wallet recovery."""
    user_id = update.effective_user.id
    
    if not is_any_admin(user_id):
        return
    
    wallet_address = update.message.text.strip()
    context.user_data['state'] = None  # Clear state
    back_callback = context.user_data.pop('recovery_back_callback', 'admin_menu')
    
    # Validate Solana address format (base58, 32-44 chars)
    if len(wallet_address) < 32 or len(wallet_address) > 44:
        await update.message.reply_text(
            "❌ Invalid wallet address format. Solana addresses are 32-44 characters.\n\nPlease try again.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Try Again", callback_data="adm_recover_single")]])
        )
        return
    
    # Send processing message
    processing_msg = await update.message.reply_text(
        f"⏳ *Checking wallet...*\n\n`{wallet_address}`",
        parse_mode="Markdown"
    )
    
    try:
        from payment_solana import check_single_wallet, recover_single_wallet, RECOVERY_WALLET, ADMIN_WALLET
        
        # First check the wallet balance
        balance_info = await check_single_wallet(wallet_address)
        
        if not balance_info:
            await processing_msg.edit_text(
                f"❌ Could not check wallet balance.\n\nWallet: `{wallet_address}`\n\n"
                "This might be due to RPC rate limiting. Please try again in a moment.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Try Again", callback_data="adm_recover_single")],
                    [InlineKeyboardButton("⬅️ Atgal", callback_data=back_callback)]
                ])
            )
            return
        
        sol_balance = balance_info['sol_balance']
        eur_value = balance_info['eur_value']
        
        if sol_balance <= 0.0001:
            await processing_msg.edit_text(
                f"✅ *Wallet is Empty*\n\n"
                f"Wallet: `{wallet_address}`\n"
                f"Balance: `{sol_balance:.6f}` SOL\n\n"
                "No funds to recover from this wallet.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Check Another", callback_data="adm_recover_single")],
                    [InlineKeyboardButton("⬅️ Atgal", callback_data=back_callback)]
                ])
            )
            return
        
        # Has funds - show confirmation
        target_wallet = RECOVERY_WALLET or ADMIN_WALLET
        
        msg = f"💰 *Funds Found!*\n\n"
        msg += f"*Wallet:* `{wallet_address}`\n"
        msg += f"*Balance:* `{sol_balance:.6f}` SOL (~{eur_value:.2f} EUR)\n\n"
        msg += f"*Target Wallet:* `{target_wallet[:16]}...{target_wallet[-8:]}`\n\n"
        msg += "⚠️ Press *Recover Now* to sweep these funds."
        
        # Store wallet for confirmation
        context.user_data['pending_single_recovery'] = wallet_address
        
        await processing_msg.edit_text(
            msg,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"✅ Recover Now ({sol_balance:.4f} SOL)", callback_data="adm_recover_single_confirm")],
                [InlineKeyboardButton("🔄 Check Another", callback_data="adm_recover_single")],
                [InlineKeyboardButton("⬅️ Atgal", callback_data=back_callback)]
            ])
        )
        
    except Exception as e:
        logger.error(f"Error checking wallet for recovery: {e}", exc_info=True)
        await processing_msg.edit_text(
            f"❌ Error: {str(e)[:100]}\n\nPlease try again.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Try Again", callback_data="adm_recover_single")],
                [InlineKeyboardButton("⬅️ Atgal", callback_data=back_callback)]
            ])
        )


async def handle_recover_single_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Executes the single wallet recovery after confirmation."""
    query = update.callback_query
    user_id = query.from_user.id
    
    if not is_any_admin(user_id):
        return await query.answer("Prieiga uždrausta. Admin only.", show_alert=True)
    
    back_callback = "admin_menu" if is_primary_admin(user_id) else "viewer_admin_menu"
    
    wallet_address = context.user_data.pop('pending_single_recovery', None)
    if not wallet_address:
        await query.answer("No wallet pending recovery. Please start over.", show_alert=True)
        return
    
    await query.answer("⏳ Recovering funds...")
    await query.edit_message_text(
        f"🔄 *Recovering Funds...*\n\nSweeping wallet `{wallet_address}`...",
        parse_mode="Markdown"
    )
    
    try:
        from payment_solana import recover_single_wallet
        
        result = await recover_single_wallet(wallet_address)
        
        if result['success']:
            msg = "✅ *Recovery Successful!*\n\n"
            msg += f"*Wallet:* `{wallet_address}`\n"
            msg += f"*Recovered:* `{result['sol_recovered']:.6f}` SOL (~{result['eur_value']:.2f} EUR)\n"
            msg += f"*User ID:* {result.get('user_id', 'N/A')}\n"
            msg += f"*Order ID:* {result.get('order_id', 'N/A')}\n"
            msg += f"*TX Signature:* `{result['tx_signature'][:20]}...`\n\n"
            msg += f"[View on Solscan](https://solscan.io/tx/{result['tx_signature']})"
        else:
            msg = f"❌ *Recovery Failed*\n\n"
            msg += f"*Wallet:* `{wallet_address}`\n"
            msg += f"*Error:* {result.get('error', 'Unknown error')}"
        
        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Recover Another", callback_data="adm_recover_single")],
                [InlineKeyboardButton("⬅️ Atgal", callback_data=back_callback)]
            ])
        )
        
    except Exception as e:
        logger.error(f"Error in single wallet recovery: {e}", exc_info=True)
        await query.edit_message_text(
            f"❌ Recovery error: {str(e)[:100]}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Try Again", callback_data="adm_recover_single")],
                [InlineKeyboardButton("⬅️ Atgal", callback_data=back_callback)]
            ])
        )