# --- START OF FILE viewer_admin.py ---

import sqlite3
import os
import logging
import asyncio
import shutil
from datetime import datetime, timedelta, timezone # <<< Ensure timezone is imported
from collections import defaultdict
import math # For pagination calculation
from decimal import Decimal # Import Decimal for balance handling

# --- Telegram Imports ---
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaPhoto, InputMediaVideo, InputMediaAnimation
)
from telegram.constants import ParseMode # Keep import for reference
from telegram.ext import ContextTypes
from telegram import helpers # Keep for potential other uses, but not escaping
import telegram.error as telegram_error
# -------------------------

# Import shared elements from utils
from utils import (
    ADMIN_ID, PRIMARY_ADMIN_IDS, LANGUAGES, format_currency, send_message_with_retry,
    SECONDARY_ADMIN_IDS, fetch_reviews,
    get_db_connection, MEDIA_DIR, # Import helper and MEDIA_DIR
    get_user_status, get_progress_bar, # Import user status helpers
    log_admin_action, # <-- IMPORT admin log function
    PRODUCT_TYPES, DEFAULT_PRODUCT_EMOJI, # <<< IMPORT THESE FOR HISTORY
    # Admin authorization helpers
    is_primary_admin, is_secondary_admin, is_any_admin
)
# Import the shared stock handler from stock.py
try:
    from stock import handle_view_stock # <-- IMPORT shared stock handler
except ImportError:
     # Create a logger instance before using it in the dummy handler
    logger_dummy_stock = logging.getLogger(__name__ + "_dummy_stock")
    logger_dummy_stock.error("Could not import handle_view_stock from stock.py. Stock viewing will not work.")
    # Define a dummy handler
    async def handle_view_stock(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
        query = update.callback_query
        msg = "Stock viewing handler not found (stock.py missing or error).\nPlease contact the primary admin."
        if query: await query.edit_message_text(msg, parse_mode=None) # Use None
        else: await send_message_with_retry(context.bot, update.effective_chat.id, msg, parse_mode=None) # Use None

# Logging setup specific to this module
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Constants ---
PRODUCTS_PER_PAGE_LOG = 5 # Number of products to show per page in the log
REVIEWS_PER_PAGE_VIEWER = 5 # Number of reviews to show per page for viewer admin
USERS_PER_PAGE = 10 # Number of users to show per page in Manage Users

# --- Viewer Admin Menu ---
async def handle_viewer_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Displays the limited admin dashboard for secondary admins."""
    user = update.effective_user
    query = update.callback_query

    if not user:
        logger.warning("handle_viewer_admin_menu triggered without effective_user.")
        if query: await query.answer("Error: Could not identify user.", show_alert=True)
        return

    user_id = user.id
    chat_id = update.effective_chat.id

    # --- Authorization Check ---
    primary_admin = (is_primary_admin(user_id))
    secondary_admin = is_secondary_admin(user_id)

    if not primary_admin and not secondary_admin:
        logger.warning(f"Non-admin user {user_id} attempted to access viewer admin menu.")
        if query: await query.answer("Access denied.", show_alert=True)
        else: await send_message_with_retry(context.bot, chat_id, "Access denied.", parse_mode=None)
        return

    # --- Prepare Message Content ---
    total_users, active_products = 0, 0
    conn = None # Initialize conn
    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        # Use column names
        c.execute("SELECT COUNT(*) as count FROM users")
        res_users = c.fetchone(); total_users = res_users['count'] if res_users else 0
        c.execute("SELECT COUNT(*) as count FROM products WHERE available > reserved")
        res_products = c.fetchone(); active_products = res_products['count'] if res_products else 0
    except sqlite3.Error as e:
        logger.error(f"DB error fetching viewer admin dashboard data: {e}", exc_info=True)
        pass # Continue without stats on error
    finally:
        if conn: conn.close() # Close connection if opened

    msg = (
       f"🔧 Admin Valdymas (Peržiūra)\n\n"
       f"👥 Viso vartotojų: {total_users}\n"
       f"📦 Aktyvūs produktai: {active_products}\n\n"
       "Pasirinkite ataskaitą ar žurnalą:"
    )

    # --- Keyboard Definition ---
    keyboard = [
        [InlineKeyboardButton("📦 Peržiūrėti atsargas", callback_data="view_stock")],
        [InlineKeyboardButton("📜 Pridėtų produktų žurnalas", callback_data="viewer_added_products|0")],
        [InlineKeyboardButton("🚫 Peržiūrėti atsiliepimus", callback_data="adm_manage_reviews|0")],
        [InlineKeyboardButton("📋 Analizuoti Render žurnalus", callback_data="adm_analyze_logs_start")],
        [InlineKeyboardButton("🔄 Atkurti įstrigusius SOL", callback_data="adm_recover_stuck_funds")],
        [InlineKeyboardButton("🏠 Pagrindinis meniu", callback_data="back_start")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # --- Send or Edit Message ---
    if query:
        try:
            await query.edit_message_text(msg, reply_markup=reply_markup, parse_mode=None)
        except telegram_error.BadRequest as e:
            if "message is not modified" not in str(e).lower():
                logger.error(f"Error editing viewer admin menu message: {e}")
                await send_message_with_retry(context.bot, chat_id, msg, reply_markup=reply_markup, parse_mode=None)
            else: await query.answer()
        except Exception as e:
            logger.error(f"Unexpected error editing viewer admin menu: {e}", exc_info=True)
            await send_message_with_retry(context.bot, chat_id, msg, reply_markup=reply_markup, parse_mode=None)
    else: # Called by command or other non-callback scenario
        await send_message_with_retry(context.bot, chat_id, msg, reply_markup=reply_markup, parse_mode=None)


# --- Added Products Log Handler ---
async def handle_viewer_added_products(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Displays a paginated log of products added to the database for viewer admin."""
    query = update.callback_query
    user_id = query.from_user.id

    primary_admin = (is_primary_admin(user_id))
    secondary_admin = is_secondary_admin(user_id)
    if not primary_admin and not secondary_admin:
        return await query.answer("Access Denied.", show_alert=True)

    offset = 0
    if params and len(params) > 0 and params[0].isdigit(): offset = int(params[0])

    products = []
    total_products = 0
    conn = None

    try:
        conn = get_db_connection() # Use helper
        # row_factory is set in helper
        c = conn.cursor()

        # Use column names
        c.execute("SELECT COUNT(*) as count FROM products")
        count_res = c.fetchone(); total_products = count_res['count'] if count_res else 0

        c.execute("""
            SELECT p.id, p.city, p.district, p.product_type, p.size, p.price,
                   p.original_text, p.added_date,
                   (SELECT COUNT(*) FROM product_media pm WHERE pm.product_id = p.id) as media_count
            FROM products p ORDER BY p.id DESC LIMIT ? OFFSET ?
        """, (PRODUCTS_PER_PAGE_LOG, offset))
        products = c.fetchall()

    except sqlite3.Error as e:
        logger.error(f"DB error fetching viewer added product log: {e}", exc_info=True)
        await query.edit_message_text("❌ Error fetching product log from database.", parse_mode=None)
        return
    finally:
        if conn: conn.close()

    msg_parts = ["📜 Added Products Log\n"]
    keyboard = []
    item_buttons = []

    if not products:
        if offset == 0: msg_parts.append("\nNo products have been added yet.")
        else: msg_parts.append("\nNo more products to display.")
    else:
        for product in products: # product is now a Row object
            try:
                # Access by column name
                prod_id = product['id']
                city_name, dist_name = product['city'], product['district']
                type_name, size_name = product['product_type'], product['size']
                price_str = format_currency(product['price'])
                media_indicator = "📸" if product['media_count'] > 0 else "🚫"
                added_date_str = "Unknown Date"
                if product['added_date']:
                    try: added_date_str = datetime.fromisoformat(product['added_date']).strftime("%Y-%m-%d %H:%M")
                    except (ValueError, TypeError): pass
                original_text_preview = (product['original_text'] or "")[:150] + ("..." if len(product['original_text'] or "") > 150 else "")
                text_display = original_text_preview if original_text_preview else "No text provided"
                item_msg = (
                    f"\nID {prod_id} | {added_date_str}\n"
                    f"📍 {city_name} / {dist_name}\n"
                    f"📦 {type_name} {size_name} ({price_str} €)\n"
                    f"📝 Text: {text_display}\n"
                    f"{media_indicator} Media Attached: {'Yes' if product['media_count'] > 0 else 'No'}\n"
                    f"---\n"
                )
                msg_parts.append(item_msg)
                # Buttons
                button_text = f"🖼️ View Media & Text #{prod_id}" if product['media_count'] > 0 else f"📄 View Full Text #{prod_id}"
                item_buttons.append([InlineKeyboardButton(button_text, callback_data=f"viewer_view_product_media|{prod_id}|{offset}")])
            except Exception as e:
                 logger.error(f"Error formatting viewer product log item ID {product['id'] if product else 'N/A'}: {e}")
                 msg_parts.append(f"\nID {product['id'] if product else 'N/A'} | (Error displaying item)\n---\n")
        keyboard.extend(item_buttons)
        # Pagination
        total_pages = math.ceil(total_products / PRODUCTS_PER_PAGE_LOG)
        current_page = (offset // PRODUCTS_PER_PAGE_LOG) + 1
        nav_buttons = []
        if current_page > 1: nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"viewer_added_products|{max(0, offset - PRODUCTS_PER_PAGE_LOG)}"))
        if current_page < total_pages: nav_buttons.append(InlineKeyboardButton("➡️ Next", callback_data=f"viewer_added_products|{offset + PRODUCTS_PER_PAGE_LOG}"))
        if nav_buttons: keyboard.append(nav_buttons)
        msg_parts.append(f"\nPage {current_page}/{total_pages}")

    # Determine correct back button based on admin type
    back_callback = "admin_menu" if primary_admin else "viewer_admin_menu"
    keyboard.append([InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data=back_callback)])

    final_msg = "".join(msg_parts)
    try:
        if len(final_msg) > 4090: final_msg = final_msg[:4090] + "\n\n✂️ ... Message truncated."
        await query.edit_message_text(final_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" in str(e).lower(): await query.answer()
        else: logger.error(f"Failed to edit viewer_added_products msg: {e}"); await query.answer("Error displaying product log.", show_alert=True)
    except Exception as e:
        logger.error(f"Unexpected error in handle_viewer_added_products: {e}", exc_info=True)
        await query.edit_message_text("❌ An unexpected error occurred.", parse_mode=None)


# --- View Product Media/Text Handler ---
async def handle_viewer_view_product_media(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Fetches and sends the media and original text for a specific product ID for viewer admin."""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id

    primary_admin = (is_primary_admin(user_id))
    secondary_admin = is_secondary_admin(user_id)
    if not primary_admin and not secondary_admin:
        return await query.answer("Access Denied.", show_alert=True)

    if not params or len(params) < 2 or not params[0].isdigit() or not params[1].isdigit():
        await query.answer("Error: Missing/invalid product ID/offset.", show_alert=True)
        return

    product_id = int(params[0])
    original_offset = int(params[1])
    back_button_callback = f"viewer_added_products|{original_offset}"

    media_items = []
    original_text = ""
    product_name = f"Product ID {product_id}"
    conn = None

    try:
        conn = get_db_connection() # Use helper
        # row_factory set in helper
        c = conn.cursor()
        # Use column names
        c.execute("SELECT name, original_text FROM products WHERE id = ?", (product_id,))
        prod_info = c.fetchone()
        if prod_info:
             original_text = prod_info['original_text'] or ""
             product_name = prod_info['name'] or product_name
        else:
            await query.answer("Product not found.", show_alert=True)
            try: await query.edit_message_text("Product not found.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Log", callback_data=back_button_callback)]]), parse_mode=None)
            except telegram_error.BadRequest: pass
            return
        # Use column names
        c.execute("SELECT media_type, telegram_file_id, file_path FROM product_media WHERE product_id = ?", (product_id,))
        media_items = c.fetchall()

    except sqlite3.Error as e:
        logger.error(f"DB error fetching media/text for product {product_id}: {e}", exc_info=True)
        await query.answer("Error fetching product details.", show_alert=True)
        try: await query.edit_message_text("Error fetching product details.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Log", callback_data=back_button_callback)]]), parse_mode=None)
        except telegram_error.BadRequest: pass
        return
    finally:
        if conn: conn.close()

    await query.answer("Fetching details...")
    try: await query.edit_message_text(f"⏳ Fetching details for product ID {product_id}...", parse_mode=None)
    except telegram_error.BadRequest: pass

    media_sent_count = 0
    media_group = []
    caption_sent_separately = False
    # Use MEDIA_DIR from utils
    first_media_caption = f"Details for {product_name} (ID: {product_id})\n\n{original_text if original_text else 'No text provided'}"
    if len(first_media_caption) > 1020: first_media_caption = first_media_caption[:1020] + "..."

    opened_files = []
    try:
        for i, item in enumerate(media_items): # item is now a Row object
            # Access by column name
            media_type = item['media_type']
            file_id = item['telegram_file_id']
            # file_path already includes MEDIA_DIR from when it was saved
            file_path = item['file_path']
            caption_to_use = first_media_caption if i == 0 else None
            input_media = None
            file_handle = None
            try:
                # Skip file_id completely and go straight to local files for now
                # This avoids the "wrong file identifier" error entirely
                logger.info(f"Using local file for P{product_id} (skipping file_id due to token change)")
                input_media = None
                        
                if not input_media and file_path: # Always try local file if no input_media yet
                    # More robust file existence check
                    try:
                        file_exists = await asyncio.to_thread(os.path.exists, file_path)
                        if not file_exists:
                            logger.warning(f"File not found via os.path.exists: {file_path}")
                            # Try alternative check
                            try:
                                file_stat = await asyncio.to_thread(os.stat, file_path)
                                file_exists = True
                                logger.info(f"File found via os.stat: {file_path} (size: {file_stat.st_size})")
                            except (OSError, FileNotFoundError):
                                logger.warning(f"File also not found via os.stat: {file_path}")
                                file_exists = False
                    except Exception as check_e:
                        logger.error(f"Error checking file existence {file_path}: {check_e}")
                        file_exists = False
                    
                    if file_exists:
                        logger.info(f"Opening media file {file_path} P{product_id}")
                        # Use asyncio.to_thread for blocking file I/O
                        file_handle = await asyncio.to_thread(open, file_path, 'rb')
                        opened_files.append(file_handle) # Keep track to close later
                        if media_type == 'photo': input_media = InputMediaPhoto(media=file_handle, caption=caption_to_use, parse_mode=None)
                        elif media_type == 'video': input_media = InputMediaVideo(media=file_handle, caption=caption_to_use, parse_mode=None)
                        elif media_type == 'gif': input_media = InputMediaAnimation(media=file_handle, caption=caption_to_use, parse_mode=None)
                        else:
                            logger.warning(f"Unsupported media type '{media_type}' from path {file_path}")
                            # Ensure file handle is closed if we skip
                            await asyncio.to_thread(file_handle.close)
                            opened_files.remove(file_handle)
                            continue # Skip adding to media_group
                    else: 
                        logger.warning(f"Media item invalid P{product_id}: No file_id and path '{file_path}' missing or inaccessible."); 
                        continue

                media_group.append(input_media)
                media_sent_count += 1

            except Exception as e:
                logger.error(f"Error preparing media item {i+1} P{product_id}: {e}", exc_info=True)
                # If preparing the first item fails, the caption needs to be sent separately
                if i == 0: caption_sent_separately = True
                # Clean up file handle if opened during failed preparation
                if file_handle and file_handle in opened_files:
                    await asyncio.to_thread(file_handle.close)
                    opened_files.remove(file_handle)

        # Send media group
        if media_group:
            try:
                await context.bot.send_media_group(chat_id, media=media_group)
                logger.info(f"Sent media group with {len(media_group)} items for product {product_id} to chat {chat_id}.")
            except Exception as e:
                 logger.error(f"Failed send media group P{product_id}: {e}")
                 # If sending fails, ensure caption is sent separately if it was attached
                 if media_group and media_group[0].caption:
                      caption_sent_separately = True

    finally:
        # Close ALL originally opened file handles in the finally block
        for f in opened_files:
            try:
                if not f.closed:
                    await asyncio.to_thread(f.close)
                    logger.debug(f"Closed file handle: {getattr(f, 'name', 'unknown')}")
            except Exception as close_e:
                logger.warning(f"Error closing file handle '{getattr(f, 'name', 'unknown')}' during cleanup: {close_e}")

    # Send the text caption separately if it wasn't sent with media or if sending failed
    if media_sent_count == 0 or caption_sent_separately:
         text_to_send = f"Details for {product_name} (ID: {product_id})\n\n{original_text if original_text else 'No text provided'}" # Plain text
         if media_sent_count == 0 and not original_text:
              text_to_send = f"No media or text found for product ID {product_id}" # Plain text

         await send_message_with_retry(
             context.bot,
             chat_id,
             text_to_send,
             parse_mode=None # Use None
         )

    # Send a final message indicating completion, with a back button
    await send_message_with_retry(
        context.bot,
        chat_id,
        f"End of details for product ID {product_id}.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Log", callback_data=back_button_callback)]]),
        parse_mode=None # Use None
    )

# ==================================================
# --- User Management Handlers (NOW PRIMARY ADMIN ONLY) ---
# ==================================================
# Note: These functions are now primarily intended for the main admin (ADMIN_ID).
# The access check inside confirms this. Viewer admins no longer see the button.

async def handle_manage_users_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Displays the first page of users for management (Primary Admin only)."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    offset = 0
    if params and len(params) > 0 and params[0].isdigit(): offset = int(params[0])
    await _display_user_list(update, context, offset)

async def _display_user_list(update: Update, context: ContextTypes.DEFAULT_TYPE, offset: int = 0):
    """Helper function to display a paginated list of users (Primary Admin view)."""
    query = update.callback_query
    chat_id = update.effective_chat.id
    admin_id = query.from_user.id # This will be ADMIN_ID due to check in caller
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    users = []
    total_users = 0
    conn = None

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM users")
        count_res = c.fetchone(); total_users = count_res['count'] if count_res else 0

        # Fetch users, excluding all primary admins
        primary_admin_ids_str = ','.join(['?' for _ in PRIMARY_ADMIN_IDS]) if PRIMARY_ADMIN_IDS else '0'
        c.execute(f"""
            SELECT user_id, username, balance, total_purchases, is_banned
            FROM users
            WHERE user_id NOT IN ({primary_admin_ids_str})
            ORDER BY user_id DESC LIMIT ? OFFSET ?
        """, PRIMARY_ADMIN_IDS + [USERS_PER_PAGE, offset])
        users = c.fetchall()

    except sqlite3.Error as e:
        logger.error(f"DB error fetching user list for admin: {e}", exc_info=True)
        await query.edit_message_text("❌ Error fetching user list.", parse_mode=None)
        return
    finally:
        if conn: conn.close()

    title = lang_data.get("manage_users_title", "👤 Manage Users")
    prompt = lang_data.get("manage_users_prompt", "Select a user to view details or manage:")
    msg_parts = [f"{title}\n\n{prompt}\n"]
    keyboard = []
    item_buttons = []

    if not users and offset == 0:
        msg_parts.append(f"\n{lang_data.get('manage_users_no_users', 'No users found.')}")
    elif not users:
         msg_parts.append(f"\n{lang_data.get('manage_users_no_users', 'No more users found.')}")
    else:
        for user in users:
            user_id_target = user['user_id']
            username = user['username'] or f"ID_{user_id_target}"
            balance_str = format_currency(user['balance'])
            status = get_user_status(user['total_purchases'])
            banned_status = "🚫" if user['is_banned'] else "✅"
            item_msg = f"\n👤 @{username} (ID: {user_id_target})\n  💰 {balance_str}€ | {status} | {banned_status}"
            msg_parts.append(item_msg)
            item_buttons.append([InlineKeyboardButton(f"View @{username}", callback_data=f"adm_view_user|{user_id_target}|{offset}")])
        keyboard.extend(item_buttons)
        # Pagination
        total_pages = math.ceil(max(0, total_users - 1) / USERS_PER_PAGE) # Exclude admin from total pages calc
        current_page = (offset // USERS_PER_PAGE) + 1
        nav_buttons = []
        prev_text = lang_data.get("prev_button", "Prev")
        next_text = lang_data.get("next_button", "Next")
        if current_page > 1: nav_buttons.append(InlineKeyboardButton(f"⬅️ {prev_text}", callback_data=f"adm_manage_users|{max(0, offset - USERS_PER_PAGE)}"))
        if current_page < total_pages: nav_buttons.append(InlineKeyboardButton(f"{next_text} ➡️", callback_data=f"adm_manage_users|{offset + USERS_PER_PAGE}"))
        if nav_buttons: keyboard.append(nav_buttons)
        msg_parts.append(f"\nPage {current_page}/{total_pages}")

    keyboard.append([InlineKeyboardButton("⬅️ Atgal į Admin meniu", callback_data="admin_menu")])
    final_msg = "".join(msg_parts)
    try:
        if len(final_msg) > 4090: final_msg = final_msg[:4090] + "\n\n✂️ ... Message truncated."
        await query.edit_message_text(final_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" in str(e).lower(): await query.answer()
        else: logger.error(f"Failed to edit user list msg: {e}"); await query.answer("Error displaying user list.", show_alert=True)
    except Exception as e:
        logger.error(f"Unexpected error in _display_user_list: {e}", exc_info=True)
        await query.edit_message_text("❌ An unexpected error occurred.", parse_mode=None)

async def handle_view_user_profile(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Displays a specific user's profile with management options for admin."""
    query = update.callback_query
    admin_id = query.from_user.id
    if not is_primary_admin(admin_id): return await query.answer("Access Denied.", show_alert=True)
    if not params or len(params) < 2 or not params[0].isdigit() or not params[1].isdigit():
        await query.answer("Error: Missing user ID or offset.", show_alert=True); return

    target_user_id = int(params[0])
    offset = int(params[1])
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    conn = None

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT user_id, username, balance, total_purchases, is_banned FROM users WHERE user_id = ?", (target_user_id,))
        user_data = c.fetchone()

        if not user_data:
            await query.answer("User not found.", show_alert=True)
            await _display_user_list(update, context, offset) # Go back to list
            return

        username = user_data['username'] or f"ID_{target_user_id}"
        balance = Decimal(str(user_data['balance']))
        purchases_count = user_data['total_purchases'] # Keep the count variable name
        is_banned = user_data['is_banned'] == 1

        # Fetch recent purchase history
        history_limit = 5
        c.execute("""
            SELECT purchase_date, product_name, product_type, product_size, price_paid
            FROM purchases
            WHERE user_id = ?
            ORDER BY purchase_date DESC
            LIMIT ?
        """, (target_user_id, history_limit))
        recent_purchases = c.fetchall()


        status = get_user_status(purchases_count)
        progress_bar = get_progress_bar(purchases_count)
        balance_str = format_currency(balance)
        banned_str = lang_data.get("user_profile_is_banned", "Yes 🚫") if is_banned else lang_data.get("user_profile_not_banned", "No ✅")

        title_template = lang_data.get("view_user_profile_title", "👤 User Profile: @{username} (ID: {user_id})")
        status_label = lang_data.get("user_profile_status", "Status")
        balance_label = lang_data.get("user_profile_balance", "Balance")
        purchases_label = lang_data.get("user_profile_purchases", "Total Purchases")
        banned_label = lang_data.get("user_profile_banned", "Banned Status")

        msg = (f"{title_template.format(username=username, user_id=target_user_id)}\n\n"
               f"👤 {status_label}: {status} {progress_bar}\n"
               f"💰 {balance_label}: {balance_str} EUR\n"
               f"📦 {purchases_label}: {purchases_count}\n" # Show total count still
               f"🚫 {banned_label}: {banned_str}")

        # Format and append purchase history
        history_str = f"\n\n📜 Recent Purchases (Last {history_limit}):\n"
        if not recent_purchases:
            history_str += "  - No purchases found.\n"
        else:
            for purchase in recent_purchases:
                try:
                    dt_obj = datetime.fromisoformat(purchase['purchase_date'].replace('Z', '+00:00'))
                    if dt_obj.tzinfo is None: dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                    date_str = dt_obj.strftime('%y-%m-%d %H:%M')
                except (ValueError, TypeError): date_str = "???"
                p_type = purchase['product_type']
                p_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
                p_name = purchase['product_name'] or 'N/A'
                p_size = purchase['product_size'] or 'N/A'
                p_price = format_currency(purchase['price_paid'])
                history_str += f"  - {date_str}: {p_emoji} {p_size} ({p_price}€)\n"

        msg += history_str

        adjust_balance_btn = lang_data.get("user_profile_button_adjust_balance", "💰 Adjust Balance")
        ban_btn_text = lang_data.get("user_profile_button_unban", "✅ Unban User") if is_banned else lang_data.get("user_profile_button_ban", "🚫 Ban User")
        back_list_btn_text = lang_data.get("user_profile_button_back_list", "⬅️ Back to User List")

        keyboard = [
            [InlineKeyboardButton(adjust_balance_btn, callback_data=f"adm_adjust_balance_start|{target_user_id}|{offset}")],
            [InlineKeyboardButton(ban_btn_text, callback_data=f"adm_toggle_ban|{target_user_id}|{offset}")],
            [InlineKeyboardButton(back_list_btn_text, callback_data=f"adm_manage_users|{offset}")]
        ]

        # Edit message (check length)
        if len(msg) > 4000: msg = msg[:4000] + "\n[... truncated]"
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e:
        logger.error(f"DB error fetching user profile for admin (target: {target_user_id}): {e}", exc_info=True)
        await query.edit_message_text("❌ Error fetching user profile.", parse_mode=None)
    except Exception as e:
        logger.error(f"Unexpected error viewing user profile (target: {target_user_id}): {e}", exc_info=True)
        await query.edit_message_text("❌ An unexpected error occurred.", parse_mode=None)
    finally:
        if conn: conn.close()

async def handle_adjust_balance_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Starts the balance adjustment process."""
    query = update.callback_query
    admin_id = query.from_user.id
    if not is_primary_admin(admin_id): return await query.answer("Access Denied.", show_alert=True)
    if not params or len(params) < 2 or not params[0].isdigit() or not params[1].isdigit():
        await query.answer("Error: Missing user ID or offset.", show_alert=True); return

    target_user_id = int(params[0])
    offset = int(params[1]) # Keep offset to go back to the right page
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    # Fetch username for prompt
    conn = None; username = f"ID_{target_user_id}"
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT username FROM users WHERE user_id=?", (target_user_id,))
        res = c.fetchone()
        if res and res['username']: username = res['username']
    except Exception as e: logger.warning(f"Could not fetch username for balance adjust prompt {target_user_id}: {e}")
    finally:
        if conn: conn.close()

    context.user_data['state'] = 'awaiting_balance_adjustment_amount'
    context.user_data['adjust_balance_target_user_id'] = target_user_id
    context.user_data['adjust_balance_offset'] = offset
    context.user_data['adjust_balance_username'] = username

    prompt_template = lang_data.get("adjust_balance_prompt", "Reply with the amount to adjust balance for @{username} (ID: {user_id}).\nUse a positive number to add (e.g., 10.50) or a negative number to subtract (e.g., -5.00).")
    prompt_msg = prompt_template.format(username=username, user_id=target_user_id)
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data=f"adm_view_user|{target_user_id}|{offset}")]] # Back to user profile

    await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter adjustment amount.")


async def handle_adjust_balance_amount_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the admin entering the balance adjustment amount."""
    admin_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(admin_id): return
    if context.user_data.get("state") != 'awaiting_balance_adjustment_amount': return
    if not update.message or not update.message.text: return

    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    target_user_id = context.user_data.get('adjust_balance_target_user_id')
    username = context.user_data.get('adjust_balance_username', f"ID_{target_user_id}")
    offset = context.user_data.get('adjust_balance_offset', 0)
    back_callback = f"adm_view_user|{target_user_id}|{offset}"

    if target_user_id is None:
        logger.error("State is awaiting_balance_adjustment_amount but target user ID is missing.")
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Context lost. Cannot adjust balance.", parse_mode=None)
        context.user_data.pop('state', None)
        return

    amount_text = update.message.text.strip().replace(',', '.')
    invalid_amount_msg = lang_data.get("adjust_balance_invalid_amount", "❌ Invalid amount. Please enter a non-zero number (e.g., 10.5 or -5).")
    reason_prompt_template = lang_data.get("adjust_balance_reason_prompt", "Please reply with a brief reason for this balance adjustment ({amount} EUR):")

    try:
        amount_decimal = Decimal(amount_text)
        if amount_decimal == Decimal('0.0'): raise ValueError("Amount cannot be zero")

        # Store amount and ask for reason
        context.user_data['adjust_balance_amount'] = float(amount_decimal) # Store as float for logging maybe
        context.user_data['state'] = 'awaiting_balance_adjustment_reason'
        amount_formatted = format_currency(amount_decimal)
        reason_prompt = reason_prompt_template.format(amount=amount_formatted)
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data=back_callback)]]
        await send_message_with_retry(context.bot, chat_id, reason_prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except (ValueError, TypeError):
        await send_message_with_retry(context.bot, chat_id, invalid_amount_msg, parse_mode=None)
        # Keep state awaiting amount


async def handle_adjust_balance_reason_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the admin entering the reason and performs the balance adjustment."""
    admin_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(admin_id): return
    if context.user_data.get("state") != 'awaiting_balance_adjustment_reason': return
    if not update.message or not update.message.text: return

    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    reason = update.message.text.strip()
    target_user_id = context.user_data.get('adjust_balance_target_user_id')
    amount_float = context.user_data.get('adjust_balance_amount')
    offset = context.user_data.get('adjust_balance_offset', 0)
    username = context.user_data.get('adjust_balance_username', f"ID_{target_user_id}")
    back_callback = f"adm_view_user|{target_user_id}|{offset}"

    reason_empty_msg = lang_data.get("adjust_balance_reason_empty", "❌ Reason cannot be empty. Please provide a reason.")
    success_template = lang_data.get("adjust_balance_success", "✅ Balance adjusted successfully for @{username}. New balance: {new_balance} EUR.")
    db_error_msg = lang_data.get("adjust_balance_db_error", "❌ Database error adjusting balance.")

    if not reason:
        await send_message_with_retry(context.bot, chat_id, reason_empty_msg, parse_mode=None)
        return # Keep state awaiting reason

    if target_user_id is None or amount_float is None:
        logger.error("State is awaiting_balance_adjustment_reason but target user ID or amount is missing.")
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Context lost. Cannot adjust balance.", parse_mode=None)
        context.user_data.pop('state', None); context.user_data.pop('adjust_balance_target_user_id', None); context.user_data.pop('adjust_balance_amount', None)
        context.user_data.pop('adjust_balance_offset', None); context.user_data.pop('adjust_balance_username', None)
        return

    conn = None; new_balance_float = 0.0
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN")
        # Get old balance before update for logging
        c.execute("SELECT balance FROM users WHERE user_id=?", (target_user_id,))
        old_balance_res = c.fetchone(); old_balance_float = old_balance_res['balance'] if old_balance_res else 0.0
        # Update balance
        update_res = c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount_float, target_user_id))
        if update_res.rowcount == 0:
             logger.error(f"Failed to adjust balance for user {target_user_id} (not found?).")
             conn.rollback(); raise sqlite3.Error("User not found during balance update.")
        # Fetch new balance
        c.execute("SELECT balance FROM users WHERE user_id = ?", (target_user_id,))
        new_balance_res = c.fetchone(); new_balance_float = new_balance_res['balance'] if new_balance_res else old_balance_float + amount_float
        conn.commit()

        # Log the action using the synchronous helper
        log_admin_action(
            admin_id=admin_id,
            action="BALANCE_ADJUST",
            target_user_id=target_user_id,
            reason=reason,
            amount_change=amount_float,
            old_value=old_balance_float,
            new_value=new_balance_float
        )

        # Clear state
        context.user_data.pop('state', None); context.user_data.pop('adjust_balance_target_user_id', None); context.user_data.pop('adjust_balance_amount', None)
        context.user_data.pop('adjust_balance_offset', None); context.user_data.pop('adjust_balance_username', None)

        success_msg = success_template.format(username=username, new_balance=format_currency(new_balance_float))
        keyboard = [[InlineKeyboardButton("⬅️ Back to User Profile", callback_data=back_callback)]]
        await send_message_with_retry(context.bot, chat_id, success_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e:
        logger.error(f"DB error adjusting balance user {target_user_id}: {e}", exc_info=True)
        if conn and conn.in_transaction: conn.rollback()
        await send_message_with_retry(context.bot, chat_id, db_error_msg, parse_mode=None)
        # Clear state on error
        context.user_data.pop('state', None); context.user_data.pop('adjust_balance_target_user_id', None); context.user_data.pop('adjust_balance_amount', None)
        context.user_data.pop('adjust_balance_offset', None); context.user_data.pop('adjust_balance_username', None)
    finally:
        if conn: conn.close()


async def handle_toggle_ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Bans or unbans a user."""
    query = update.callback_query
    admin_id = query.from_user.id
    if not is_primary_admin(admin_id): return await query.answer("Access Denied.", show_alert=True)
    if not params or len(params) < 2 or not params[0].isdigit() or not params[1].isdigit():
        await query.answer("Error: Missing user ID or offset.", show_alert=True); return

    target_user_id = int(params[0])
    offset = int(params[1])
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    conn = None

    if is_primary_admin(target_user_id):
        cannot_ban_admin_msg = lang_data.get("ban_cannot_ban_admin", "❌ Cannot ban the primary admin.")
        await query.answer(cannot_ban_admin_msg, show_alert=True)
        return

    try:
        conn = get_db_connection()
        c = conn.cursor()
        # Get current ban status and username
        c.execute("SELECT username, is_banned FROM users WHERE user_id = ?", (target_user_id,))
        user_info = c.fetchone()
        if not user_info:
            await query.answer("User not found.", show_alert=True)
            await _display_user_list(update, context, offset) # Go back to list
            return

        current_ban_status = user_info['is_banned']
        username = user_info['username'] or f"ID_{target_user_id}"
        new_ban_status = 1 if current_ban_status == 0 else 0 # Toggle

        # Update DB
        c.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (new_ban_status, target_user_id))
        conn.commit()

        action = "BAN_USER" if new_ban_status == 1 else "UNBAN_USER"
        log_admin_action(
            admin_id=admin_id,
            action=action,
            target_user_id=target_user_id,
            old_value=current_ban_status,
            new_value=new_ban_status
        )

        success_msg_template = lang_data.get("unban_success", "✅ User @{username} (ID: {user_id}) has been unbanned.") if new_ban_status == 0 else lang_data.get("ban_success", "🚫 User @{username} (ID: {user_id}) has been banned.")
        success_msg = success_msg_template.format(username=username, user_id=target_user_id)
        await query.answer(success_msg)
        # Refresh the user profile view
        await handle_view_user_profile(update, context, params=[str(target_user_id), str(offset)])

    except sqlite3.Error as e:
        logger.error(f"DB error toggling ban status for user {target_user_id}: {e}", exc_info=True)
        if conn and conn.in_transaction: conn.rollback()
        error_msg = lang_data.get("ban_db_error", "❌ Database error updating ban status.")
        await query.answer(error_msg, show_alert=True)
    except Exception as e:
        logger.error(f"Unexpected error toggling ban status for user {target_user_id}: {e}", exc_info=True)
        await query.answer("An unexpected error occurred.", show_alert=True)
    finally:
        if conn: conn.close()

# --- END OF FILE viewer_admin.py ---
