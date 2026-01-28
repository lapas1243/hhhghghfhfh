import sqlite3
import time
import logging
import asyncio
import os # Import os for path joining
from datetime import datetime, timezone
from collections import defaultdict, Counter
from decimal import Decimal, ROUND_DOWN # <<< Added ROUND_DOWN

# --- Telegram Imports ---
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from telegram import helpers
import telegram.error as telegram_error
# -------------------------

# Import from utils
from utils import (
    CITIES, DISTRICTS, PRODUCT_TYPES, THEMES, LANGUAGES, BOT_MEDIA, ADMIN_ID, BASKET_TIMEOUT, MIN_DEPOSIT_EUR,
    format_currency, get_progress_bar, send_message_with_retry, format_discount_value,
    clear_expired_basket, fetch_last_purchases, get_user_status, fetch_reviews,
    SOLANA_ADMIN_WALLET, # Check if Solana is configured
    get_db_connection, MEDIA_DIR, # Import helper and MEDIA_DIR
    DEFAULT_PRODUCT_EMOJI, # Import default emoji
    load_active_welcome_message, # <<< Import welcome message loader (though we'll modify its usage)
    DEFAULT_WELCOME_MESSAGE, # <<< Import default welcome message fallback
    _get_lang_data, # <<< IMPORT THE HELPER FROM UTILS >>>
    _unreserve_basket_items, # <<< IMPORT UNRESERVE HELPER >>>
    is_primary_admin, is_secondary_admin, is_any_admin, # Admin helper functions
    SECONDARY_ADMIN_IDS, is_user_banned, # Import ban check helper
    update_user_broadcast_status # Import broadcast tracking helper
)
import json # <<< Make sure json is imported
import payment # <<< Make sure payment module is imported

# --- Import Reseller Helper ---
try:
    from reseller_management import get_reseller_discount
except ImportError:
    logger_dummy_reseller = logging.getLogger(__name__ + "_dummy_reseller")
    logger_dummy_reseller.error("Could not import get_reseller_discount from reseller_management.py. Reseller discounts will not work.")
    # Define a dummy function that always returns zero discount
    def get_reseller_discount(user_id: int, product_type: str) -> Decimal:
        return Decimal('0.0')
# -----------------------------


# Logging setup
logger = logging.getLogger(__name__)

# Emojis (Defaults/Placeholders)
EMOJI_CITY = "🏙️"
EMOJI_DISTRICT = "🏘️"
# EMOJI_PRODUCT = "💎" # No longer primary source
EMOJI_HERB = "🌿" # Keep for potential specific logic if needed
EMOJI_PRICE = "💰"
EMOJI_QUANTITY = "🔢"
EMOJI_BASKET = "🛒"
EMOJI_PROFILE = "👤"
EMOJI_REFILL = "💸"
EMOJI_REVIEW = "📝"
EMOJI_PRICELIST = "📋"
EMOJI_LANG = "🌐"
EMOJI_BACK = "⬅️"
EMOJI_HOME = "🏠"
EMOJI_SHOP = "🛍️"
EMOJI_DISCOUNT = "🏷️"
EMOJI_PAY_NOW = "💳" # <<< ADDED Emoji for Pay Now

# --- Supported Crypto Assets ---
# Only SOL is supported by the Solana payment backend
SUPPORTED_CRYPTO = {
    'sol': '☀️ SOL (Solana)',
}
# --------------------------------------------------------------------


# --- Helper Function to Build Start Menu ---
def _build_start_menu_content(user_id: int, username: str, lang_data: dict, context: ContextTypes.DEFAULT_TYPE) -> tuple[str, InlineKeyboardMarkup]:
    """Builds the text and keyboard for the start menu using provided lang_data."""
    logger.debug(f"_build_start_menu_content: Building menu for user {user_id} with lang_data.")

    balance, purchases, basket_count = Decimal('0.0'), 0, 0
    conn = None
    active_template_name_from_db = None # Variable to store DB setting

    # --- Initial Data Fetch ---
    try:
        conn = get_db_connection()
        c = conn.cursor()
        # Get user stats
        c.execute("SELECT balance, total_purchases FROM users WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        if result:
            balance = Decimal(str(result['balance']))
            purchases = result['total_purchases']

        # Get active welcome template name setting
        c.execute("SELECT setting_value FROM bot_settings WHERE setting_key = ?", ("active_welcome_message_name",)) # LINE 89
        setting_row = c.fetchone()
        if setting_row and setting_row['setting_value']: # Check if value is not None/empty
            active_template_name_from_db = setting_row['setting_value']
            logger.info(f"Active welcome template name from settings: '{active_template_name_from_db}'")
        else:
            active_template_name_from_db = "default" # Fallback to 'default' if setting is missing/empty
            logger.info("Active welcome message name not found in settings, falling back to 'default'.")

        # Call synchronous clear_expired_basket (no await needed)
        clear_expired_basket(context, user_id) # Assuming clear_expired_basket is synchronous
        basket = context.user_data.get("basket", [])
        basket_count = len(basket)
        if not basket: context.user_data.pop('applied_discount', None)

    except sqlite3.Error as e:
        logger.error(f"Database error fetching initial data for start menu build (user {user_id}): {e}", exc_info=True)
        active_template_name_from_db = "default" # Fallback on DB error fetching setting
    finally:
        if conn: conn.close()

    # --- Determine which template text to use ---
    welcome_template_to_use = None # Start with None

    if active_template_name_from_db: # Only try if we have a name (even if it's 'default')
        conn_load = None
        try:
            conn_load = get_db_connection()
            c_load = conn_load.cursor()
            c_load.execute("SELECT template_text FROM welcome_messages WHERE name = ?", (active_template_name_from_db,))
            template_row = c_load.fetchone()
            if template_row:
                welcome_template_to_use = template_row['template_text']
                logger.info(f"Using welcome message template from DB: '{active_template_name_from_db}'")
            else:
                logger.warning(f"Active template '{active_template_name_from_db}' set in DB but not found in templates table. Will fall back.")
                # welcome_template_to_use remains None
        except sqlite3.Error as e:
            logger.error(f"DB error loading specific welcome template '{active_template_name_from_db}': {e}")
            # welcome_template_to_use remains None
        finally:
            if conn_load: conn_load.close()

    # Fallback logic if DB load failed or no active name was determined initially
    if welcome_template_to_use is None:
        logger.warning("Falling back to default welcome message defined in LANGUAGES.")
        welcome_template_to_use = lang_data.get('welcome', DEFAULT_WELCOME_MESSAGE) # Use language file default OR hardcoded default

    # --- Format the chosen template ---
    status = get_user_status(purchases)
    balance_str = format_currency(balance)
    progress_bar_str = get_progress_bar(purchases)

    try:
        # Format using the raw username and placeholders
        full_welcome = welcome_template_to_use.format(
            username=username,
            status=status,
            progress_bar=progress_bar_str,
            balance_str=balance_str,
            purchases=purchases,
            basket_count=basket_count
        )
    except KeyError as e:
        logger.error(f"Placeholder error formatting welcome message template. Missing key: {e}. Template: '{welcome_template_to_use[:100]}...' Using fallback.")
        full_welcome = f"👋 Welcome, {username}!\n\n💰 Balance: {balance_str} EUR"
    except Exception as format_e:
        logger.error(f"Unexpected error formatting welcome message: {format_e}. Template: '{welcome_template_to_use[:100]}...' Using fallback.")
        full_welcome = f"👋 Welcome, {username}!\n\n💰 Balance: {balance_str} EUR"

    # --- Build Keyboard ---
    shop_button_text = lang_data.get("shop_button", "Shop")
    profile_button_text = lang_data.get("profile_button", "Profile")
    top_up_button_text = lang_data.get("top_up_button", "Top Up")
    reviews_button_text = lang_data.get("reviews_button", "Reviews")
    price_list_button_text = lang_data.get("price_list_button", "Price List")
    language_button_text = lang_data.get("language_button", "Language")
    admin_button_text = lang_data.get("admin_button", "🔧 Admin Panel")
    keyboard = [
        [InlineKeyboardButton(f"{EMOJI_SHOP} {shop_button_text}", callback_data="shop")],
        [InlineKeyboardButton(f"{EMOJI_PROFILE} {profile_button_text}", callback_data="profile"),
         InlineKeyboardButton(f"{EMOJI_REFILL} {top_up_button_text}", callback_data="refill")],
        [InlineKeyboardButton(f"{EMOJI_REVIEW} {reviews_button_text}", callback_data="reviews"),
         InlineKeyboardButton(f"{EMOJI_PRICELIST} {price_list_button_text}", callback_data="price_list"),
         InlineKeyboardButton(f"{EMOJI_LANG} {language_button_text}", callback_data="language")]
    ]
    if is_primary_admin(user_id):
        keyboard.insert(0, [InlineKeyboardButton(admin_button_text, callback_data="admin_menu")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    return full_welcome, reply_markup


# --- User Command Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command and the initial welcome message."""
    user = update.effective_user
    chat_id = update.effective_chat.id
    is_callback = update.callback_query is not None
    user_id = user.id
    username = user.username or user.first_name or f"User_{user_id}"
    
    # Ban check is now handled in main.py start_command_wrapper
    # This check is kept for callback queries that might bypass the wrapper
    if is_callback and await is_user_banned(user_id):
        logger.info(f"Banned user {user_id} attempted to use /start command via callback.")
        ban_message = "❌ Your access to this bot has been restricted. If you believe this is an error, please contact support."
        try:
            await update.callback_query.edit_message_text(ban_message, parse_mode=None)
        except Exception as e:
            logger.error(f"Error editing message for banned user {user_id}: {e}")
            try:
                await update.callback_query.answer(ban_message, show_alert=True)
            except Exception as e2:
                logger.error(f"Error answering callback for banned user {user_id}: {e2}")
        return

    # Send Bot Media (Only on direct /start, not callbacks)
    if not is_callback and BOT_MEDIA.get("type") and BOT_MEDIA.get("path"):
        media_path = BOT_MEDIA["path"]
        media_type = BOT_MEDIA["type"]
        logger.info(f"Attempting to send BOT_MEDIA: type={media_type}, path={media_path}")

        # Check if file exists using asyncio.to_thread
        if await asyncio.to_thread(os.path.exists, media_path):
            try:
                # Pass the file path directly to the send_* methods
                if media_type == "photo":
                    await context.bot.send_photo(chat_id=chat_id, photo=media_path)
                elif media_type == "video":
                    await context.bot.send_video(chat_id=chat_id, video=media_path)
                elif media_type == "gif":
                    await context.bot.send_animation(chat_id=chat_id, animation=media_path)
                else:
                    logger.warning(f"Unsupported BOT_MEDIA type for sending: {media_type}")

            except telegram_error.TelegramError as e:
                logger.error(f"Error sending BOT_MEDIA ({media_path}): {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Unexpected error sending BOT_MEDIA ({media_path}): {e}", exc_info=True)
        else:
            logger.warning(f"BOT_MEDIA path {media_path} not found on disk when trying to send.")


    # Ensure user exists and language context is set
    lang = context.user_data.get("lang", None)
    if lang is None:
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            # Ensure user exists
            c.execute("""
                INSERT INTO users (user_id, username, language, is_reseller) VALUES (?, ?, 'en', 0)
                ON CONFLICT(user_id) DO UPDATE SET username=excluded.username
            """, (user_id, username))
            # Get language
            c.execute("SELECT language FROM users WHERE user_id = ?", (user_id,))
            result = c.fetchone()
            db_lang = result['language'] if result else 'en'
            try: from utils import LANGUAGES as UTILS_LANGUAGES_START
            except ImportError: UTILS_LANGUAGES_START = {'en': {}}
            lang = db_lang if db_lang and db_lang in UTILS_LANGUAGES_START else 'en'
            conn.commit()
            context.user_data["lang"] = lang
            logger.info(f"start: Set language for user {user_id} to '{lang}' from DB/default.")
            
            # Update user activity status (they successfully interacted with the bot)
            update_user_broadcast_status(user_id, success=True)
        except sqlite3.Error as e:
            logger.error(f"DB error ensuring user/language in start for {user_id}: {e}")
            lang = 'en'
            context.user_data["lang"] = lang
            logger.warning(f"start: Defaulted language to 'en' for user {user_id} due to DB error.")
        finally:
            if conn: conn.close()
    else:
        logger.info(f"start: Using existing language '{lang}' from context for user {user_id}.")

    # Build and Send/Edit Menu
    lang, lang_data = _get_lang_data(context)
    full_welcome, reply_markup = _build_start_menu_content(user_id, username, lang_data, context)

    if is_callback:
        query = update.callback_query
        try:
             if query.message and (query.message.text != full_welcome or query.message.reply_markup != reply_markup):
                  await query.edit_message_text(full_welcome, reply_markup=reply_markup, parse_mode=None)
             elif query: await query.answer()
        except telegram_error.BadRequest as e:
            if "message is not modified" in str(e).lower():
                await query.answer()
            else:
                logger.warning(f"Failed to edit start message (callback): {e}. Sending new.")
                await send_message_with_retry(context.bot, chat_id, full_welcome, reply_markup=reply_markup, parse_mode=None)
        except Exception as e:
             logger.error(f"Unexpected error editing start message (callback): {e}", exc_info=True)
             await send_message_with_retry(context.bot, chat_id, full_welcome, reply_markup=reply_markup, parse_mode=None)
    else:
        await send_message_with_retry(context.bot, chat_id, full_welcome, reply_markup=reply_markup, parse_mode=None)


# --- Other handlers ---
async def handle_back_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Back' button presses that should return to the main start menu."""
    await start(update, context)

# --- Shopping Handlers ---
async def handle_shop(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    logger.info(f"handle_shop triggered by user {user_id} (lang: {lang}).")

    no_cities_available_msg = lang_data.get("no_cities_available", "No cities available at the moment. Please check back later.")
    choose_city_title = lang_data.get("choose_city_title", "Choose a City")
    select_location_prompt = lang_data.get("select_location_prompt", "Select your location:")
    home_button_text = lang_data.get("home_button", "Home")

    if not CITIES:
        keyboard = [[InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]]
        await query.edit_message_text(f"{EMOJI_CITY} {no_cities_available_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        return

    try:
        sorted_city_ids = sorted(CITIES.keys(), key=lambda city_id: CITIES.get(city_id, ''))
        keyboard = []
        for c_id in sorted_city_ids:
             city_name = CITIES.get(c_id)
             if city_name: keyboard.append([InlineKeyboardButton(f"{EMOJI_CITY} {city_name}", callback_data=f"city|{c_id}")])
             else: logger.warning(f"handle_shop: City name missing for ID {c_id}.")
        keyboard.append([InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = f"{EMOJI_CITY} {choose_city_title}\n\n{select_location_prompt}"
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode=None)
        logger.info(f"handle_shop: Sent city list to user {user_id}.")
    except telegram_error.BadRequest as e:
         if "message is not modified" not in str(e).lower(): logger.error(f"Error editing shop message: {e}"); await query.answer("Error displaying cities.", show_alert=True)
         else: await query.answer()
    except Exception as e:
        logger.error(f"Error in handle_shop for user {user_id}: {e}", exc_info=True)
        try: keyboard = [[InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]]; await query.edit_message_text("❌ An error occurred.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except Exception as inner_e: logger.error(f"Failed fallback in handle_shop: {inner_e}")


# --- Modified handle_city_selection (Corrected Formatting FINAL) ---
async def handle_city_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id # Added for logging
    lang, lang_data = _get_lang_data(context)

    if not params:
        logger.warning(f"handle_city_selection called without city_id for user {user_id}.")
        await query.answer("Error: City ID missing.", show_alert=True)
        return
    city_id = params[0]
    city_name = CITIES.get(city_id)
    if not city_name:
        error_city_not_found = lang_data.get("error_city_not_found", "Error: City not found.")
        logger.warning(f"City ID {city_id} not found in CITIES for user {user_id}.")
        await query.edit_message_text(f"❌ {error_city_not_found}", parse_mode=None)
        return await handle_shop(update, context) # Go back to city selection

    districts_in_city = DISTRICTS.get(city_id, {})
    back_cities_button = lang_data.get("back_cities_button", "Back to Cities")
    home_button = lang_data.get("home_button", "Home")
    no_districts_msg = lang_data.get("no_districts_available", "No districts available yet for this city.")
    no_products_in_districts_msg = lang_data.get("no_products_in_city_districts", "No products currently available in any district of this city.")
    choose_district_prompt = lang_data.get("choose_district_prompt", "Choose a district:")
    error_loading_districts = lang_data.get("error_loading_districts", "Error loading districts. Please try again.")
    available_label_short = lang_data.get("available_label_short", "Av") # Get short available label

    keyboard = []
    message_text_parts = [f"{EMOJI_CITY} {city_name}\n\n"] # Start message
    districts_with_products_info = [] # Store tuples: (d_id, dist_name)

    if not districts_in_city:
        # If no districts are configured AT ALL for the city
        keyboard_nav = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_cities_button}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
        await query.edit_message_text(f"{EMOJI_CITY} {city_name}\n\n{no_districts_msg}", reply_markup=InlineKeyboardMarkup(keyboard_nav), parse_mode=None)
        return
    else:
        # If districts are configured, check each one for products
        sorted_district_ids = sorted(districts_in_city.keys(), key=lambda dist_id: districts_in_city.get(dist_id, ''))
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()

            for d_id in sorted_district_ids:
                dist_name = districts_in_city.get(d_id)
                if dist_name:
                    # NEW Query for detailed product summary in this district
                    c.execute("""
                        SELECT product_type, size, price, COUNT(*) as quantity
                        FROM products
                        WHERE city = ? AND district = ? AND available > reserved
                        GROUP BY product_type, size, price
                        ORDER BY product_type, price, size
                    """, (city_name, dist_name))
                    products_in_district = c.fetchall()

                    if products_in_district:
                        # Add district header to message text (using Markdown for bold)
                        escaped_dist_name = helpers.escape_markdown(dist_name, version=2)
                        message_text_parts.append(f"{EMOJI_DISTRICT} *{escaped_dist_name}*:\n") # Keep newline after district name

                        # --- Build product list string for this district ---
                        for prod in products_in_district:
                            prod_emoji = PRODUCT_TYPES.get(prod['product_type'], DEFAULT_PRODUCT_EMOJI)
                            price_str = format_currency(prod['price'])
                            # Escape parts individually
                            escaped_type = helpers.escape_markdown(prod['product_type'], version=2)
                            escaped_size = helpers.escape_markdown(prod['size'], version=2)
                            escaped_price = helpers.escape_markdown(price_str, version=2)
                            escaped_qty = helpers.escape_markdown(str(prod['quantity']), version=2)
                            # Create the formatted line WITH a standard Python newline \n
                            # Removed the quantity display per admin request
                            message_text_parts.append(f"    • {prod_emoji} {escaped_type} {escaped_size} \\({escaped_price}€\\)\n")

                        # Add a blank line for spacing after the district's products
                        message_text_parts.append("\n")
                        # --- End building product list string ---

                        # Add district to list for button creation
                        districts_with_products_info.append((d_id, dist_name))
                    # else: District has no products, do nothing (it's skipped)
                else:
                    logger.warning(f"District name missing for ID {d_id} in city {city_id} (handle_city_selection)")

        except sqlite3.Error as e:
            logger.error(f"DB error checking product availability for districts in city {city_name} (ID: {city_id}) for user {user_id}: {e}")
            keyboard_error = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_cities_button}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"{EMOJI_CITY} {city_name}\n\n❌ {error_loading_districts}", reply_markup=InlineKeyboardMarkup(keyboard_error), parse_mode=None)
            if conn: conn.close()
            return # Stop processing on DB error
        finally:
            if conn:
                conn.close()

        # After checking all districts:
        if not districts_with_products_info:
            # If we looped through all configured districts but none had products
            keyboard_nav = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_cities_button}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"{EMOJI_CITY} {city_name}\n\n{no_products_in_districts_msg}", reply_markup=InlineKeyboardMarkup(keyboard_nav), parse_mode=None)
        else:
            # Add prompt below details ONLY if there are districts with products
            message_text_parts.append(f"\n{choose_district_prompt}")
            final_message = "".join(message_text_parts)

            # Create buttons ONLY for districts with products
            for d_id, dist_name in districts_with_products_info:
                 keyboard.append([InlineKeyboardButton(f"{EMOJI_DISTRICT} {dist_name}", callback_data=f"dist|{city_id}|{d_id}")])

            keyboard.append([InlineKeyboardButton(f"{EMOJI_BACK} {back_cities_button}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")])

            # Check length and edit message
            try:
                if len(final_message) > 4000:
                    # Find a good place to truncate (e.g., before the last district's details)
                    trunc_point = final_message.rfind(f"\n{EMOJI_DISTRICT}", 0, 3900)
                    if trunc_point != -1:
                        final_message = final_message[:trunc_point] + "\n\n\\[\\.\\.\\. Message truncated \\.\\.\\.\\]"
                    else: # Fallback if no good split point found
                        final_message = final_message[:4000] + "\n\n\\[\\.\\.\\. Message truncated \\.\\.\\.\\]"
                    logger.warning(f"District selection message for user {user_id} city {city_name} truncated.")

                await query.edit_message_text(
                    final_message,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN_V2 # Use Markdown
                )
            except telegram_error.BadRequest as e:
                if "message is not modified" not in str(e).lower():
                    logger.error(f"Error editing district selection message (Markdown): {e}")
                    # Fallback to plain text if Markdown fails
                    try:
                         plain_text_message = "".join(message_text_parts).replace('*','').replace('\\','') # Basic removal of bold and escapes
                         if len(plain_text_message) > 4000: plain_text_message = plain_text_message[:4000] + "\n\n[... Message truncated ...]"
                         await query.edit_message_text(plain_text_message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
                    except Exception as fallback_e:
                         logger.error(f"Failed fallback edit for district selection: {fallback_e}")
                         await query.answer("Error displaying districts.", show_alert=True)
                else:
                    await query.answer() # Acknowledge if not modified
# --- END handle_city_selection ---


async def handle_district_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    if not params or len(params) < 2: logger.warning("handle_district_selection missing params."); await query.answer("Error: City/District ID missing.", show_alert=True); return
    city_id, dist_id = params[0], params[1]
    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)

    if not city or not district: error_district_city_not_found = lang_data.get("error_district_city_not_found", "Error: District or city not found."); await query.edit_message_text(f"❌ {error_district_city_not_found}", parse_mode=None); return await handle_shop(update, context)

    back_districts_button = lang_data.get("back_districts_button", "Back to Districts"); home_button = lang_data.get("home_button", "Home")
    no_types_msg = lang_data.get("no_types_available", "No product types currently available here."); select_type_prompt = lang_data.get("select_type_prompt", "Select product type:")
    error_loading_types = lang_data.get("error_loading_types", "Error: Failed to Load Product Types"); error_unexpected = lang_data.get("error_unexpected", "An unexpected error occurred")

    conn = None
    max_retries = 3
    retry_delay = 0.1  # 100ms
    
    for attempt in range(max_retries):
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT DISTINCT product_type FROM products WHERE city = ? AND district = ? AND available > reserved ORDER BY product_type", (city, district))
            available_types = [row['product_type'] for row in c.fetchall()]
            break  # Success, exit retry loop
        except sqlite3.Error as e:
            if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                logger.warning(f"Database locked for district selection (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                if conn:
                    conn.close()
                    conn = None
                continue
            else:
                logger.error(f"DB error fetching product types {city}/{district}: {e}", exc_info=True)
                await query.edit_message_text(f"❌ {error_loading_types}", parse_mode=None)
                return
        except Exception as e:
            logger.error(f"Unexpected error in handle_district_selection: {e}", exc_info=True)
            await query.edit_message_text(f"❌ {error_unexpected}", parse_mode=None)
            return
        finally:
            if conn:
                conn.close()
                conn = None

    if not available_types:
        keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_districts_button}", callback_data=f"city|{city_id}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
        await query.edit_message_text(f"{EMOJI_CITY} {city}\n{EMOJI_DISTRICT} {district}\n\n{no_types_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    else:
        keyboard = []
        for pt in available_types:
            emoji = PRODUCT_TYPES.get(pt, DEFAULT_PRODUCT_EMOJI)
            keyboard.append([InlineKeyboardButton(f"{emoji} {pt}", callback_data=f"type|{city_id}|{dist_id}|{pt}")])
        # Go back to city selection (which now shows the product list)
        keyboard.append([InlineKeyboardButton(f"{EMOJI_BACK} {back_districts_button}", callback_data=f"city|{city_id}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")])
        try:
            await query.edit_message_text(f"{EMOJI_CITY} {city}\n{EMOJI_DISTRICT} {district}\n\n{select_type_prompt}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except Exception as e:
            # Handle "Message is not modified" error gracefully
            if "Message is not modified" in str(e):
                await query.answer("Already showing this view")
            else:
                logger.error(f"Error editing message in handle_district_selection: {e}")
                await query.answer("Error updating view")


# <<< MODIFIED: Incorporate Reseller Discount Display >>>
async def handle_type_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id # <<< GET USER ID
    lang, lang_data = _get_lang_data(context)
    if not params or len(params) < 3: logger.warning("handle_type_selection missing params."); await query.answer("Error: City/District/Type missing.", show_alert=True); return
    city_id, dist_id, p_type = params
    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)

    if not city or not district: error_district_city_not_found = lang_data.get("error_district_city_not_found", "Error: District or city not found."); await query.edit_message_text(f"❌ {error_district_city_not_found}", parse_mode=None); return await handle_shop(update, context)

    product_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    back_types_button = lang_data.get("back_types_button", "Back to Types"); home_button = lang_data.get("home_button", "Home")
    no_items_of_type = lang_data.get("no_items_of_type", "No items of this type currently available here.")
    available_options_prompt = lang_data.get("available_options_prompt", "Available options:")
    error_loading_products = lang_data.get("error_loading_products", "Error: Failed to Load Products"); error_unexpected = lang_data.get("error_unexpected", "An unexpected error occurred")

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT size, price, COUNT(*) as count_available FROM products WHERE city = ? AND district = ? AND product_type = ? AND available > reserved GROUP BY size, price ORDER BY price", (city, district, p_type))
        products = c.fetchall()

        if not products:
            keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_types_button}", callback_data=f"dist|{city_id}|{dist_id}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"{EMOJI_CITY} {city}\n{EMOJI_DISTRICT} {district}\n{product_emoji} {p_type}\n\n{no_items_of_type}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        else:
            keyboard = []
            available_label_short = lang_data.get("available_label_short", "Av")
            # <<< Fetch reseller discount ONCE >>>
            reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, p_type)
            # <<< End Fetch >>>

            for row in products:
                size, original_price_decimal, count = row['size'], Decimal(str(row['price'])), row['count_available']
                original_price_str = format_currency(original_price_decimal)
                original_price_callback_str = f"{original_price_decimal:.2f}" # Use original price for callback

                # <<< Apply Reseller Discount for Display >>>
                discounted_price_str = original_price_str # Default to original
                if reseller_discount_percent > Decimal('0.0'):
                    discount_amount = (original_price_decimal * reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                    discounted_price_decimal = original_price_decimal - discount_amount
                    discounted_price_str = format_currency(discounted_price_decimal)
                    # Use simple plain text for original price notation
                    button_text = f"{product_emoji} {size} ({discounted_price_str}€ / Orig: {original_price_str}€) - {available_label_short}: {count}"
                else:
                    # No discount, show original price only
                    button_text = f"{product_emoji} {size} ({original_price_str}€) - {available_label_short}: {count}"
                # <<< End Apply >>>

                # Callback still uses original price
                callback_data = f"product|{city_id}|{dist_id}|{p_type}|{size}|{original_price_callback_str}"
                keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

            keyboard.append([InlineKeyboardButton(f"{EMOJI_BACK} {back_types_button}", callback_data=f"dist|{city_id}|{dist_id}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")])
            await query.edit_message_text(f"{EMOJI_CITY} {city}\n{EMOJI_DISTRICT} {district}\n{product_emoji} {p_type}\n\n{available_options_prompt}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e: logger.error(f"DB error fetching products {city}/{district}/{p_type}: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_loading_products}", parse_mode=None)
    except Exception as e: logger.error(f"Unexpected error in handle_type_selection: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_unexpected}", parse_mode=None)
    finally:
        if conn: conn.close()

# --- END OF handle_type_selection ---

# <<< MODIFIED: Add Pay Now Button & Logic for Single Item Discount Flow >>>
async def handle_product_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    if not params or len(params) < 5: logger.warning("handle_product_selection missing params."); await query.answer("Error: Incomplete product data.", show_alert=True); return
    city_id, dist_id, p_type, size, price_str = params # price_str is ORIGINAL price

    try: original_price = Decimal(price_str)
    except ValueError: logger.warning(f"Invalid price format: {price_str}"); await query.edit_message_text("❌ Error: Invalid product data.", parse_mode=None); return

    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city or not district: error_location_mismatch = lang_data.get("error_location_mismatch", "Error: Location data mismatch."); await query.edit_message_text(f"❌ {error_location_mismatch}", parse_mode=None); return await handle_shop(update, context)

    product_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    theme_name = context.user_data.get("theme", "default")
    theme = THEMES.get(theme_name, THEMES["default"])
    basket_emoji = theme.get('basket', EMOJI_BASKET)

    price_label = lang_data.get("price_label", "Price"); available_label_long = lang_data.get("available_label_long", "Available")
    back_options_button = lang_data.get("back_options_button", "Back to Options"); home_button = lang_data.get("home_button", "Home")
    drop_unavailable_msg = lang_data.get("drop_unavailable", "Drop Unavailable! This option just sold out or was reserved.")
    add_to_basket_button = lang_data.get("add_to_basket_button", "Add to Basket")
    pay_now_button_text = lang_data.get("pay_now_button", "Pay Now")
    error_loading_details = lang_data.get("error_loading_details", "Error: Failed to Load Product Details"); error_unexpected = lang_data.get("error_unexpected", "An unexpected error occurred")

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM products WHERE city = ? AND district = ? AND product_type = ? AND size = ? AND price = ? AND available > reserved", (city, district, p_type, size, float(original_price)))
        available_count_result = c.fetchone(); available_count = available_count_result['count'] if available_count_result else 0

        if available_count <= 0:
            keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=f"type|{city_id}|{dist_id}|{p_type}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"❌ {drop_unavailable_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        else:
            original_price_formatted = format_currency(original_price)
            reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, p_type)
            display_price_str = original_price_formatted
            if reseller_discount_percent > Decimal('0.0'):
                discount_amount = (original_price * reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                discounted_price = original_price - discount_amount
                display_price_str = f"{format_currency(discounted_price)} (Orig: {original_price_formatted}€)"

            msg = (f"{EMOJI_CITY} {city} | {EMOJI_DISTRICT} {district}\n"
                   f"{product_emoji} {p_type} - {size}\n"
                   f"{EMOJI_PRICE} {price_label}: {display_price_str} EUR\n"
                   f"{EMOJI_QUANTITY} {available_label_long}: {available_count}")

            add_callback = f"add|{city_id}|{dist_id}|{p_type}|{size}|{price_str}"
            back_callback = f"type|{city_id}|{dist_id}|{p_type}"
            pay_now_callback = f"pay_single_item|{city_id}|{dist_id}|{p_type}|{size}|{price_str}"

            keyboard = [
                [
                    InlineKeyboardButton(f"{basket_emoji} {add_to_basket_button}", callback_data=add_callback),
                    InlineKeyboardButton(f"{EMOJI_PAY_NOW} {pay_now_button_text}", callback_data=pay_now_callback)
                ],
                [InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=back_callback), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]
            ]
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except sqlite3.Error as e: logger.error(f"DB error checking availability {city}/{district}/{p_type}/{size}: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_loading_details}", parse_mode=None)
    except Exception as e: logger.error(f"Unexpected error in handle_product_selection: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_unexpected}", parse_mode=None)
    finally:
        if conn: conn.close()

# --- END handle_product_selection ---

# <<< MODIFIED: Incorporate Reseller Discount Calculation & Display >>>
async def handle_add_to_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id # <<< GET USER ID
    lang, lang_data = _get_lang_data(context)
    if not params or len(params) < 5: logger.warning("handle_add_to_basket missing params."); await query.answer("Error: Incomplete product data.", show_alert=True); return
    city_id, dist_id, p_type, size, price_str = params # price_str is ORIGINAL price

    try: original_price = Decimal(price_str) # <<< Store original price
    except ValueError: logger.warning(f"Invalid price format add_to_basket: {price_str}"); await query.edit_message_text("❌ Error: Invalid product data.", parse_mode=None); return

    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city or not district: error_location_mismatch = lang_data.get("error_location_mismatch", "Error: Location data mismatch."); await query.edit_message_text(f"❌ {error_location_mismatch}", parse_mode=None); return await handle_shop(update, context)

    product_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    theme_name = context.user_data.get("theme", "default"); theme = THEMES.get(theme_name, THEMES["default"])
    basket_emoji = theme.get('basket', EMOJI_BASKET)
    product_id_reserved = None; conn = None

    back_options_button = lang_data.get("back_options_button", "Back to Options"); home_button = lang_data.get("home_button", "Home")
    out_of_stock_msg = lang_data.get("out_of_stock", "Out of Stock! Sorry, the last one was taken or reserved.")
    pay_now_button_text = lang_data.get("pay_now_button", "Pay Now"); top_up_button_text = lang_data.get("top_up_button", "Top Up")
    view_basket_button_text = lang_data.get("view_basket_button", "View Basket"); clear_basket_button_text = lang_data.get("clear_basket_button", "Clear Basket")
    shop_more_button_text = lang_data.get("shop_more_button", "Shop More"); expires_label = lang_data.get("expires_label", "Expires in")
    error_adding_db = lang_data.get("error_adding_db", "Error: Database issue adding item."); error_adding_unexpected = lang_data.get("error_adding_unexpected", "Error: An unexpected issue occurred.")
    added_msg_template = lang_data.get("added_to_basket", "✅ Item Reserved!\n\n{item} is in your basket for {timeout} minutes! ⏳")
    pay_msg_template = lang_data.get("pay", "💳 Total to Pay: {amount} EUR")
    apply_discount_button_text = lang_data.get("apply_discount_button", "Apply Discount Code")
    reseller_discount_label = lang_data.get("reseller_discount_label", "Reseller Discount") # <<< NEW

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN EXCLUSIVE")
        
        # Step 1: Find an available product
        c.execute("SELECT id FROM products WHERE city = ? AND district = ? AND product_type = ? AND size = ? AND price = ? AND available > reserved ORDER BY id LIMIT 1", (city, district, p_type, size, float(original_price)))
        product_row = c.fetchone()

        if not product_row:
            conn.rollback()
            keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=f"type|{city_id}|{dist_id}|{p_type}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"❌ {out_of_stock_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
            return

        product_id_reserved = product_row['id']
        
        # Step 2: Atomically reserve the specific product with availability check
        # This prevents race conditions by ensuring reserved never exceeds available
        update_result = c.execute("UPDATE products SET reserved = reserved + 1 WHERE id = ? AND available > reserved", (product_id_reserved,))
        
        if update_result.rowcount == 0:
            # Race condition: product was taken between SELECT and UPDATE
            conn.rollback()
            keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=f"type|{city_id}|{dist_id}|{p_type}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text("❌ Sorry, this item was just taken by another user! Please try again.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
            return
        c.execute("SELECT basket FROM users WHERE user_id = ?", (user_id,))
        user_basket_row = c.fetchone(); current_basket_str = user_basket_row['basket'] if user_basket_row else ''
        timestamp = time.time(); new_item_str = f"{product_id_reserved}:{timestamp}"
        new_basket_str = f"{current_basket_str},{new_item_str}" if current_basket_str else new_item_str
        c.execute("UPDATE users SET basket = ? WHERE user_id = ?", (new_basket_str, user_id))
        conn.commit()

        if "basket" not in context.user_data or not isinstance(context.user_data["basket"], list): context.user_data["basket"] = []
        # <<< Store product_type along with original price >>>
        context.user_data["basket"].append({
            "product_id": product_id_reserved,
            "price": original_price, # Store original price
            "product_type": p_type, # Store product type
            "timestamp": timestamp
        })
        # <<< End store >>>
        logger.info(f"User {user_id} added product {product_id_reserved} (type: {p_type}) to basket.")

        timeout_minutes = BASKET_TIMEOUT // 60
        current_basket_list = context.user_data["basket"]

        # --- Calculate Totals with Reseller Discount ---
        basket_original_total = Decimal('0.0')
        total_reseller_discount_amount = Decimal('0.0')
        total_after_reseller = Decimal('0.0')

        for item in current_basket_list:
            item_original_price = item.get('price', Decimal('0.0')) # Ensure it's Decimal
            item_type = item.get('product_type', '') # Ensure it exists
            basket_original_total += item_original_price

            item_reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, item_type)
            item_reseller_discount = (item_original_price * item_reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
            total_reseller_discount_amount += item_reseller_discount
            total_after_reseller += (item_original_price - item_reseller_discount)
        # --- End Calculate ---

        # --- Apply General Discount (if any) ---
        final_total = total_after_reseller # Start with reseller-discounted total
        general_discount_amount = Decimal('0.0')
        applied_discount_info = context.user_data.get('applied_discount')
        pay_msg_str = ""

        if applied_discount_info:
            # Validate general code against the total *after* reseller discount
            # Note: For existing applied discounts, we don't re-validate atomically to avoid double-charging
            code_valid, _, discount_details = validate_discount_code(applied_discount_info['code'], float(total_after_reseller))
            if code_valid and discount_details:
                general_discount_amount = Decimal(str(discount_details['discount_amount']))
                final_total = Decimal(str(discount_details['final_total'])) # validate_discount_code returns final total after THIS code
                # Update context with amounts based on the reseller-adjusted total
                context.user_data['applied_discount']['amount'] = float(general_discount_amount)
                context.user_data['applied_discount']['final_total'] = float(final_total)
            else:
                # General discount became invalid (maybe due to reseller discount changing total)
                context.user_data.pop('applied_discount', None)
                await query.answer("General discount removed (basket changed).", show_alert=False)
        # --- End Apply General Discount ---


        # --- Build Message ---
        item_price_str = format_currency(original_price)
        item_desc = f"{product_emoji} {p_type} {size} ({item_price_str}€)"
        expiry_dt = datetime.fromtimestamp(timestamp + BASKET_TIMEOUT); expiry_time_str = expiry_dt.strftime('%H:%M:%S')
        reserved_msg = (added_msg_template.format(timeout=timeout_minutes, item=item_desc) + "\n\n" + f"⏳ {expires_label}: {expiry_time_str}\n\n")

        # Display breakdown
        basket_original_total_str = format_currency(basket_original_total)
        reserved_msg += f"{lang_data.get('subtotal_label', 'Subtotal')}: {basket_original_total_str} EUR\n"
        if total_reseller_discount_amount > Decimal('0.0'):
            reseller_discount_str = format_currency(total_reseller_discount_amount)
            reserved_msg += f"{EMOJI_DISCOUNT} {reseller_discount_label}: -{reseller_discount_str} EUR\n"
        if general_discount_amount > Decimal('0.0'):
            general_discount_str = format_currency(general_discount_amount)
            general_code = applied_discount_info.get('code', 'Discount')
            reserved_msg += f"{EMOJI_DISCOUNT} {lang_data.get('discount_applied_label', 'Discount Applied')} ({general_code}): -{general_discount_str} EUR\n"

        final_total_str = format_currency(final_total)
        reserved_msg += pay_msg_template.format(amount=final_total_str) # Total to pay

        district_btn_text = district[:15]

        keyboard = [
            [InlineKeyboardButton(f"💳 {pay_now_button_text}", callback_data="confirm_pay"), InlineKeyboardButton(f"{EMOJI_REFILL} {top_up_button_text}", callback_data="refill")],
            [InlineKeyboardButton(f"{basket_emoji} {view_basket_button_text} ({len(current_basket_list)})", callback_data="view_basket"), InlineKeyboardButton(f"{basket_emoji} {clear_basket_button_text}", callback_data="clear_basket")],
            [InlineKeyboardButton(f"{EMOJI_DISCOUNT} {apply_discount_button_text}", callback_data="apply_discount_start")],
            [InlineKeyboardButton(f"➕ {shop_more_button_text} ({district_btn_text})", callback_data=f"dist|{city_id}|{dist_id}")],
            [InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=f"type|{city_id}|{dist_id}|{p_type}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]
        ]
        await query.edit_message_text(reserved_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e:
        if conn and conn.in_transaction: conn.rollback()
        logger.error(f"DB error adding product {product_id_reserved if product_id_reserved else 'N/A'} user {user_id}: {e}", exc_info=True)
        await query.edit_message_text(f"❌ {error_adding_db}", parse_mode=None)
    except Exception as e:
        if conn and conn.in_transaction: conn.rollback()
        logger.error(f"Unexpected error adding item user {user_id}: {e}", exc_info=True)
        await query.edit_message_text(f"❌ {error_adding_unexpected}", parse_mode=None)
    finally:
        if conn: conn.close()

# --- END handle_add_to_basket ---


# --- Profile Handlers ---
async def handle_profile(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    theme_name = context.user_data.get("theme", "default")
    theme = THEMES.get(theme_name, THEMES["default"])
    basket_emoji = theme.get('basket', EMOJI_BASKET)

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT balance, total_purchases FROM users WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        if not result: logger.error(f"User {user_id} not found in DB for profile."); await query.edit_message_text("❌ Error: Could not load profile.", parse_mode=None); return
        balance, purchases = Decimal(str(result['balance'])), result['total_purchases']

        # Call synchronous clear_expired_basket (no await needed)
        clear_expired_basket(context, user_id) # Assuming clear_expired_basket is synchronous
        basket_count = len(context.user_data.get("basket", []))
        status = get_user_status(purchases); progress_bar = get_progress_bar(purchases); balance_str = format_currency(balance)
        status_label = lang_data.get("status_label", "Status"); balance_label = lang_data.get("balance_label", "Balance")
        purchases_label = lang_data.get("purchases_label", "Total Purchases"); basket_label = lang_data.get("basket_label", "Basket Items")
        profile_title = lang_data.get("profile_title", "Your Profile")
        top_up_note = lang_data.get("top_up_note", "💡 Top up your balance to make purchases!")
        profile_msg = (f"🎉 {profile_title}\n\n" f"👤 {status_label}: {status} {progress_bar}\n" f"💰 {balance_label}: {balance_str} EUR\n"
                       f"📦 {purchases_label}: {purchases}\n" f"🛒 {basket_label}: {basket_count}\n\n{top_up_note}")

        top_up_button_text = lang_data.get("top_up_button", "Top Up"); view_basket_button_text = lang_data.get("view_basket_button", "View Basket")
        purchase_history_button_text = lang_data.get("purchase_history_button", "Purchase History"); home_button_text = lang_data.get("home_button", "Home")
        keyboard = [
            [InlineKeyboardButton(f"{EMOJI_REFILL} {top_up_button_text}", callback_data="refill"), InlineKeyboardButton(f"{basket_emoji} {view_basket_button_text} ({basket_count})", callback_data="view_basket")],
            [InlineKeyboardButton(f"📜 {purchase_history_button_text}", callback_data="view_history")],
            [InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]
        ]
        await query.edit_message_text(profile_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e: logger.error(f"DB error loading profile user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Failed to Load Profile.", parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Unexpected BadRequest handle_profile user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue.", parse_mode=None)
        else: await query.answer()
    except Exception as e: logger.error(f"Unexpected error handle_profile user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue.", parse_mode=None)
    finally:
        if conn: conn.close()

# --- Discount Validation (Synchronous - Adjusted for base total) ---
def validate_discount_code(code_text: str, base_total_float: float) -> tuple[bool, str, dict | None]:
    """ Validates a general discount code against a base total (which should be after reseller discounts). """
    lang_data = LANGUAGES.get('en', {}) # Use English for internal messages
    no_code_msg = lang_data.get("no_code_provided", "No code provided.")
    not_found_msg = lang_data.get("discount_code_not_found", "Discount code not found.")
    inactive_msg = lang_data.get("discount_code_inactive", "This discount code is inactive.")
    expired_msg = lang_data.get("discount_code_expired", "This discount code has expired.")
    invalid_expiry_msg = lang_data.get("invalid_code_expiry_data", "Invalid code expiry data.")
    limit_reached_msg = lang_data.get("code_limit_reached", "Code reached usage limit.")
    internal_error_type_msg = lang_data.get("internal_error_discount_type", "Internal error processing discount type.")
    db_error_msg = lang_data.get("db_error_validating_code", "Database error validating code.")
    unexpected_error_msg = lang_data.get("unexpected_error_validating_code", "An unexpected error occurred.")
    code_applied_msg_template = lang_data.get("code_applied_message", "Code '{code}' ({value}) applied. Discount: -{amount} EUR")

    if not code_text: return False, no_code_msg, None
    
    # Normalize the code: strip whitespace and convert to uppercase for case-insensitive lookup
    normalized_code = code_text.strip().upper()
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        # Use case-insensitive search by converting both to uppercase
        c.execute("SELECT * FROM discount_codes WHERE UPPER(code) = ?", (normalized_code,))
        code_data = c.fetchone()

        if not code_data: return False, not_found_msg, None
        if not code_data['is_active']: return False, inactive_msg, None
        # Enhanced expiry date validation
        if code_data['expiry_date']:
            try:
                expiry_date_str = code_data['expiry_date']
                # Handle different date formats and ensure UTC
                if 'T' in expiry_date_str:
                    # ISO format
                    if expiry_date_str.endswith('Z'):
                        expiry_dt = datetime.fromisoformat(expiry_date_str.replace('Z', '+00:00'))
                    elif '+' in expiry_date_str or expiry_date_str.count('-') > 2:
                        expiry_dt = datetime.fromisoformat(expiry_date_str)
                    else:
                        # No timezone info, assume UTC
                        expiry_dt = datetime.fromisoformat(expiry_date_str).replace(tzinfo=timezone.utc)
                else:
                    # Date only format, set to end of day UTC
                    date_only = datetime.strptime(expiry_date_str, '%Y-%m-%d')
                    expiry_dt = date_only.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                
                if datetime.now(timezone.utc) > expiry_dt: 
                    logger.info(f"Discount code '{code_data['code']}' expired on {expiry_dt}")
                    return False, expired_msg, None
            except (ValueError, TypeError) as e: 
                logger.warning(f"Invalid expiry_date format DB code {code_data['code']}: {e}")
                return False, invalid_expiry_msg, None
        if code_data['max_uses'] is not None and code_data['uses_count'] >= code_data['max_uses']: 
            logger.info(f"Discount code '{code_data['code']}' reached usage limit: {code_data['uses_count']}/{code_data['max_uses']}")
            return False, limit_reached_msg, None

        # Check minimum order amount if specified (add this column to DB if needed)
        try:
            min_order_amount = code_data['min_order_amount']
        except (KeyError, IndexError):
            min_order_amount = None
        if min_order_amount is not None and base_total_float < float(min_order_amount):
            logger.info(f"Discount code '{code_data['code']}' minimum order not met: {base_total_float} < {min_order_amount}")
            return False, lang_data.get("discount_min_order_not_met", "Minimum order amount not met for this discount code."), None

        discount_amount = Decimal('0.0')
        dtype = code_data['discount_type']; value = Decimal(str(code_data['value']))
        base_total_decimal = Decimal(str(base_total_float)) # Use the passed base total

        if dtype == 'percentage': discount_amount = (base_total_decimal * value) / Decimal('100.0')
        elif dtype == 'fixed': discount_amount = value
        else: logger.error(f"Unknown discount type '{dtype}' code {code_data['code']}"); return False, internal_error_type_msg, None

        # Ensure discount doesn't exceed the (potentially already reseller-discounted) base total
        discount_amount = min(discount_amount, base_total_decimal).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        final_total_decimal = (base_total_decimal - discount_amount).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        # Ensure final total is not negative
        final_total_decimal = max(Decimal('0.0'), final_total_decimal)

        discount_amount_float = float(discount_amount)
        final_total_float = float(final_total_decimal)

        details = {'code': code_data['code'], 'type': dtype, 'value': float(value), 'discount_amount': discount_amount_float, 'final_total': final_total_float}
        code_display = code_data['code']; value_str_display = format_discount_value(dtype, float(value))
        amount_str_display = format_currency(discount_amount_float)
        message = code_applied_msg_template.format(code=code_display, value=value_str_display, amount=amount_str_display)
        return True, message, details

    except sqlite3.Error as e: logger.error(f"DB error validating discount code '{code_text}': {e}", exc_info=True); return False, db_error_msg, None
    except Exception as e: logger.error(f"Unexpected error validating code '{code_text}': {e}", exc_info=True); return False, unexpected_error_msg, None
    finally:
        if conn: conn.close()

# --- END Discount Validation ---

# --- ATOMIC DISCOUNT CODE VALIDATION AND APPLICATION ---
def validate_and_apply_discount_atomic(code_text: str, base_total_float: float, user_id: int) -> tuple[bool, str, dict | None]:
    """
    ATOMIC: Validates AND applies discount code in a single database transaction.
    This prevents race conditions and multiple applications of the same code.
    """
    lang_data = LANGUAGES.get('en', {})
    no_code_msg = lang_data.get("no_code_provided", "No code provided.")
    not_found_msg = lang_data.get("discount_code_not_found", "Discount code not found.")
    inactive_msg = lang_data.get("discount_code_inactive", "This discount code is inactive.")
    expired_msg = lang_data.get("discount_code_expired", "This discount code has expired.")
    limit_reached_msg = lang_data.get("code_limit_reached", "Code reached usage limit.")
    already_used_msg = lang_data.get("code_already_used", "Discount code is not available. Please check the code or try again later.")
    internal_error_msg = lang_data.get("internal_error_discount_type", "Internal error processing discount type.")
    db_error_msg = lang_data.get("db_error_validating_code", "Database error validating code.")
    unexpected_error_msg = lang_data.get("unexpected_error_validating_code", "An unexpected error occurred.")
    code_applied_msg_template = lang_data.get("code_applied_message", "Code '{code}' ({value}) applied. Discount: -{amount} EUR")

    if not code_text: 
        return False, no_code_msg, None
    
    # Normalize the code
    normalized_code = code_text.strip().upper()
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # ATOMIC TRANSACTION: Validate and apply in one operation
        c.execute("BEGIN IMMEDIATE")
        
        # Note: Users can reuse discount codes multiple times until admin disables/deletes the code
        # No need to check for previous usage - allow unlimited reuse per user
        
        # Get and validate discount code
        c.execute("""
            SELECT * FROM discount_codes 
            WHERE UPPER(code) = ?
        """, (normalized_code,))
        code_data = c.fetchone()

        if not code_data: 
            conn.rollback()
            return False, not_found_msg, None
            
        if not code_data['is_active']: 
            conn.rollback()
            return False, inactive_msg, None
            
        # Check expiry
        if code_data['expiry_date']:
            try:
                expiry_date_str = code_data['expiry_date']
                if 'T' in expiry_date_str:
                    if expiry_date_str.endswith('Z'):
                        expiry_dt = datetime.fromisoformat(expiry_date_str.replace('Z', '+00:00'))
                    elif '+' in expiry_date_str or expiry_date_str.count('-') > 2:
                        expiry_dt = datetime.fromisoformat(expiry_date_str)
                    else:
                        expiry_dt = datetime.fromisoformat(expiry_date_str).replace(tzinfo=timezone.utc)
                else:
                    date_only = datetime.strptime(expiry_date_str, '%Y-%m-%d')
                    expiry_dt = date_only.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                
                if datetime.now(timezone.utc) > expiry_dt: 
                    logger.info(f"Discount code '{code_data['code']}' expired on {expiry_dt}")
                    conn.rollback()
                    return False, expired_msg, None
            except (ValueError, TypeError) as e: 
                logger.warning(f"Invalid expiry_date format DB code {code_data['code']}: {e}")
                conn.rollback()
                return False, "Invalid code expiry data.", None
                
        # Check usage limit
        if code_data['max_uses'] is not None and code_data['uses_count'] >= code_data['max_uses']: 
            logger.info(f"Discount code '{code_data['code']}' reached usage limit: {code_data['uses_count']}/{code_data['max_uses']}")
            conn.rollback()
            return False, limit_reached_msg, None

        # Check minimum order amount
        try:
            min_order_amount = code_data['min_order_amount']
        except (KeyError, IndexError):
            min_order_amount = None
        if min_order_amount is not None and base_total_float < float(min_order_amount):
            logger.info(f"Discount code '{code_data['code']}' minimum order not met: {base_total_float} < {min_order_amount}")
            conn.rollback()
            return False, "Minimum order amount not met for this discount code.", None

        # Calculate discount
        discount_amount = Decimal('0.0')
        dtype = code_data['discount_type']
        value = Decimal(str(code_data['value']))
        base_total_decimal = Decimal(str(base_total_float))

        if dtype == 'percentage': 
            discount_amount = (base_total_decimal * value) / Decimal('100.0')
        elif dtype == 'fixed': 
            discount_amount = value
        else: 
            logger.error(f"Unknown discount type '{dtype}' code {code_data['code']}")
            conn.rollback()
            return False, internal_error_msg, None

        # Ensure discount doesn't exceed base total
        discount_amount = min(discount_amount, base_total_decimal).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        final_total_decimal = (base_total_decimal - discount_amount).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        final_total_decimal = max(Decimal('0.0'), final_total_decimal)

        # ATOMIC: Record usage and increment usage count
        c.execute("""
            INSERT INTO discount_code_usage (user_id, code, used_at, discount_amount)
            VALUES (?, ?, ?, ?)
        """, (user_id, normalized_code, datetime.now(timezone.utc).isoformat(), float(discount_amount)))
        
        c.execute("""
            UPDATE discount_codes 
            SET uses_count = uses_count + 1 
            WHERE UPPER(code) = ?
        """, (normalized_code,))
        
        # Commit the transaction
        conn.commit()
        
        # Prepare response
        discount_amount_float = float(discount_amount)
        final_total_float = float(final_total_decimal)
        details = {
            'code': code_data['code'], 
            'type': dtype, 
            'value': float(value), 
            'discount_amount': discount_amount_float, 
            'final_total': final_total_float
        }
        
        code_display = code_data['code']
        value_str_display = format_discount_value(dtype, float(value))
        amount_str_display = format_currency(discount_amount_float)
        message = code_applied_msg_template.format(code=code_display, value=value_str_display, amount=amount_str_display)
        
        logger.info(f"ATOMIC: User {user_id} successfully applied discount code '{normalized_code}'. Discount: {discount_amount_float:.2f} EUR")
        return True, message, details

    except sqlite3.Error as e: 
        logger.error(f"DB error in atomic discount validation '{code_text}': {e}", exc_info=True)
        if conn: conn.rollback()
        return False, db_error_msg, None
    except Exception as e: 
        logger.error(f"Unexpected error in atomic discount validation '{code_text}': {e}", exc_info=True)
        if conn: conn.rollback()
        return False, unexpected_error_msg, None
    finally:
        if conn: conn.close()

# --- END ATOMIC DISCOUNT VALIDATION ---

# --- Basket Handlers ---
# <<< MODIFIED: Incorporate Reseller Discount Calculation & Display >>>
async def handle_view_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    theme_name = context.user_data.get("theme", "default"); theme = THEMES.get(theme_name, THEMES["default"]); basket_emoji = theme.get('basket', EMOJI_BASKET)
    reseller_discount_label = lang_data.get("reseller_discount_label", "Reseller Discount")

    clear_expired_basket(context, user_id)
    basket = context.user_data.get("basket", [])

    if not basket:
        context.user_data.pop('applied_discount', None)
        basket_empty_msg = lang_data.get("basket_empty", "🛒 Your Basket is Empty!")
        add_items_prompt = lang_data.get("add_items_prompt", "Add items to start shopping!")
        shop_button_text = lang_data.get("shop_button", "Shop"); home_button_text = lang_data.get("home_button", "Home")
        full_empty_msg = basket_empty_msg + "\n\n" + add_items_prompt + " 😊"
        keyboard = [[InlineKeyboardButton(f"{EMOJI_SHOP} {shop_button_text}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]]
        try: await query.edit_message_text(full_empty_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except telegram_error.BadRequest as e:
             if "message is not modified" not in str(e).lower(): logger.error(f"Error editing empty basket msg: {e}")
             else: await query.answer()
        return

    msg = f"{basket_emoji} {lang_data.get('your_basket_title', 'Your Basket')}\n\n"
    keyboard_items = []
    product_db_details = {}
    conn = None

    basket_original_total = Decimal('0.0')
    total_reseller_discount_amount = Decimal('0.0')
    total_after_reseller = Decimal('0.0')
    basket_items_with_details = []

    product_ids_in_basket = list(set(item.get('product_id') for item in basket if item.get('product_id')))
    if product_ids_in_basket:
        try:
            conn = get_db_connection()
            c = conn.cursor()
            placeholders = ','.join('?' for _ in product_ids_in_basket)
            c.execute(f"SELECT id, name, size FROM products WHERE id IN ({placeholders})", product_ids_in_basket)
            product_db_details = {row['id']: {'name': row['name'], 'size': row['size']} for row in c.fetchall()}
        except sqlite3.Error as e:
            logger.error(f"DB error fetching product names/sizes for basket view user {user_id}: {e}")
        finally:
            if conn: conn.close(); conn = None

    items_to_process_count = 0
    for item in basket:
        prod_id = item.get('product_id')
        original_price = item.get('price')
        product_type = item.get('product_type')
        timestamp = item.get('timestamp')

        if prod_id is None or original_price is None or product_type is None or timestamp is None:
            logger.warning(f"Skipping malformed item in basket context user {user_id}: {item}")
            continue

        items_to_process_count += 1
        basket_original_total += original_price
        item_reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, product_type)
        item_reseller_discount = (original_price * item_reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        item_price_after_reseller = original_price - item_reseller_discount
        total_reseller_discount_amount += item_reseller_discount
        total_after_reseller += item_price_after_reseller
        db_info = product_db_details.get(prod_id, {})
        basket_items_with_details.append({
            'id': prod_id, 'type': product_type, 'name': db_info.get('name', f'P{prod_id}'),
            'size': db_info.get('size', '?'), 'original_price': original_price,
            'discounted_price': item_price_after_reseller, 'timestamp': timestamp,
            'has_reseller_discount': item_reseller_discount > Decimal('0.0')
        })

    if items_to_process_count == 0:
        context.user_data['basket'] = []
        context.user_data.pop('applied_discount', None);
        basket_empty_msg = lang_data.get("basket_empty", "🛒 Your Basket is Empty!"); items_expired_note = lang_data.get("items_expired_note", "Items may have expired or were removed.")
        shop_button_text = lang_data.get("shop_button", "Shop"); home_button_text = lang_data.get("home_button", "Home")
        full_empty_msg = basket_empty_msg + "\n\n" + items_expired_note
        keyboard = [[InlineKeyboardButton(f"🛍️ {shop_button_text}", callback_data="shop"), InlineKeyboardButton(f"🏠 {home_button_text}", callback_data="back_start")]]; await query.edit_message_text(full_empty_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None); return


    final_total = total_after_reseller
    general_discount_amount = Decimal('0.0')
    applied_discount_info = context.user_data.get('applied_discount')
    discount_code_to_revalidate = applied_discount_info.get('code') if applied_discount_info else None
    discount_applied_str = ""
    discount_removed_note_template = lang_data.get("discount_removed_note", "Discount code {code} removed: {reason}")

    if discount_code_to_revalidate:
        # Note: For revalidation of existing discounts, we don't use atomic to avoid double-charging
        code_valid, validation_message, discount_details = validate_discount_code(discount_code_to_revalidate, float(total_after_reseller))
        if code_valid and discount_details:
            general_discount_amount = Decimal(str(discount_details['discount_amount']))
            final_total = Decimal(str(discount_details['final_total']))
            discount_code = discount_code_to_revalidate
            discount_value_str = format_discount_value(discount_details['type'], discount_details['value'])
            discount_amount_str = format_currency(general_discount_amount)
            discount_applied_str = (f"\n{EMOJI_DISCOUNT} {lang_data.get('discount_applied_label', 'Discount Applied')} ({discount_code}: {discount_value_str}): -{discount_amount_str} EUR")
            context.user_data['applied_discount'] = {'code': discount_code_to_revalidate, 'amount': float(general_discount_amount), 'final_total': float(final_total)}
        else:
            context.user_data.pop('applied_discount', None)
            logger.info(f"General Discount '{discount_code_to_revalidate}' invalidated for user {user_id} in basket view. Reason: {validation_message}")
            discount_applied_str = f"\n{discount_removed_note_template.format(code=discount_code_to_revalidate, reason=validation_message)}"
            await query.answer("Applied discount code removed (basket changed).", show_alert=False)

    expires_in_label = lang_data.get("expires_in_label", "Expires in"); remove_button_label = lang_data.get("remove_button_label", "Remove")
    current_time = time.time()

    for index, item_detail in enumerate(basket_items_with_details):
        prod_id = item_detail['id']
        product_emoji = PRODUCT_TYPES.get(item_detail['type'], DEFAULT_PRODUCT_EMOJI)
        item_desc_base = f"{product_emoji} {item_detail['name']} {item_detail['size']}"
        price_display = format_currency(item_detail['discounted_price'])
        if item_detail['has_reseller_discount']:
            original_price_formatted = format_currency(item_detail['original_price'])
            price_display += f" (Orig: {original_price_formatted}€)"
        timestamp = item_detail['timestamp']
        remaining_time = max(0, int(BASKET_TIMEOUT - (current_time - timestamp)))
        time_str = f"{remaining_time // 60} min {remaining_time % 60} sec"
        msg += (f"{index + 1}. {item_desc_base} ({price_display})\n"
                f"   ⏳ {expires_in_label}: {time_str}\n")
        remove_button_text = f"🗑️ {remove_button_label} {item_desc_base}"[:60]
        keyboard_items.append([InlineKeyboardButton(remove_button_text, callback_data=f"remove|{prod_id}")])

    subtotal_label = lang_data.get("subtotal_label", "Subtotal"); total_label = lang_data.get("total_label", "Total")
    basket_original_total_str = format_currency(basket_original_total)
    final_total_str = format_currency(final_total)
    msg += f"\n{subtotal_label}: {basket_original_total_str} EUR"
    if total_reseller_discount_amount > Decimal('0.0'):
        reseller_discount_str = format_currency(total_reseller_discount_amount)
        msg += f"\n{EMOJI_DISCOUNT} {reseller_discount_label}: -{reseller_discount_str} EUR"
    msg += discount_applied_str
    msg += f"\n💳 **{total_label}: {final_total_str} EUR**"
    
    # Get user balance and show status
    user_balance = Decimal('0.0')
    try:
        conn_bal = get_db_connection()
        c_bal = conn_bal.cursor()
        c_bal.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        bal_row = c_bal.fetchone()
        if bal_row:
            user_balance = Decimal(str(bal_row['balance']))
        conn_bal.close()
    except Exception as e:
        logger.warning(f"Error checking balance in basket view: {e}")
    
    balance_str = format_currency(user_balance)
    msg += f"\n\n💰 Your Balance: {balance_str} EUR"
    
    if user_balance >= final_total:
        msg += "\n✅ Ready to pay!"
    else:
        needed = final_total - user_balance
        needed_str = format_currency(needed)
        msg += f"\n⚠️ You need {needed_str} EUR more - Please top up first!"

    pay_now_button_text = lang_data.get("pay_now_button", "Pay Now"); clear_all_button_text = lang_data.get("clear_all_button", "Clear All")
    remove_discount_button_text = lang_data.get("remove_discount_button", "Remove Discount"); apply_discount_button_text = lang_data.get("apply_discount_button", "Apply Discount Code")
    shop_more_button_text = lang_data.get("shop_more_button", "Shop More"); home_button_text = lang_data.get("home_button", "Home")
    top_up_button_text = lang_data.get("top_up_button", "Top Up Balance")

    # Show different buttons based on balance
    if user_balance >= final_total:
        action_buttons = [
            [InlineKeyboardButton(f"✅ {pay_now_button_text}", callback_data="confirm_pay"), InlineKeyboardButton(f"{basket_emoji} {clear_all_button_text}", callback_data="clear_basket")],
            *([[InlineKeyboardButton(f"❌ {remove_discount_button_text}", callback_data="remove_discount")]] if context.user_data.get('applied_discount') else []),
            [InlineKeyboardButton(f"{EMOJI_DISCOUNT} {apply_discount_button_text}", callback_data="apply_discount_start")],
            [InlineKeyboardButton(f"{EMOJI_SHOP} {shop_more_button_text}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]
        ]
    else:
        action_buttons = [
            [InlineKeyboardButton(f"{EMOJI_REFILL} {top_up_button_text}", callback_data="refill")],
            [InlineKeyboardButton(f"{EMOJI_DISCOUNT} {apply_discount_button_text}", callback_data="apply_discount_start"), InlineKeyboardButton(f"{basket_emoji} {clear_all_button_text}", callback_data="clear_basket")],
            [InlineKeyboardButton(f"{EMOJI_SHOP} {shop_more_button_text}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]
        ]
    final_keyboard = keyboard_items + action_buttons

    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(final_keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
         if "message is not modified" not in str(e).lower():
             logger.error(f"Error editing basket view message: {e}")
         else:
             try: await query.answer()
             except telegram_error.BadRequest: logger.debug(f"Query answer failed after 'message not modified' for user {user_id} (likely too old).")
             except Exception as ans_e: logger.warning(f"Error answering query after 'message not modified' for user {user_id}: {ans_e}")
    except Exception as e:
         logger.error(f"Unexpected error viewing basket user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue.", parse_mode=None)

# --- END handle_view_basket ---


# --- Discount Application Handlers ---
async def apply_discount_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    clear_expired_basket(context, user_id)
    basket = context.user_data.get("basket", [])
    if not basket: no_items_message = lang_data.get("discount_no_items", "Your basket is empty."); await query.answer(no_items_message, show_alert=True); return await handle_view_basket(update, context)

    context.user_data['state'] = 'awaiting_user_discount_code'
    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="view_basket")]]
    enter_code_prompt = lang_data.get("enter_discount_code_prompt", "Please enter your discount code:")
    await query.edit_message_text(f"{EMOJI_DISCOUNT} {enter_code_prompt}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer(lang_data.get("enter_code_answer", "Enter code in chat."))

async def remove_discount(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Removes a *general* discount code."""
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    if 'applied_discount' in context.user_data:
        removed_code = context.user_data.pop('applied_discount')['code']
        logger.info(f"User {user_id} removed general discount code '{removed_code}'.")
        discount_removed_answer = lang_data.get("discount_removed_answer", "Discount removed.")
        await query.answer(discount_removed_answer)
    else: no_discount_answer = lang_data.get("no_discount_answer", "No discount applied."); await query.answer(no_discount_answer, show_alert=False)
    await handle_view_basket(update, context) # Refresh basket view

# <<< MODIFIED: Calculate base total AFTER reseller discounts >>>
async def handle_user_discount_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles user entering a general discount code."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    state = context.user_data.get("state")
    lang, lang_data = _get_lang_data(context)

    if state != "awaiting_user_discount_code": return
    if not update.message or not update.message.text: send_text_please = lang_data.get("send_text_please", "Please send the code as text."); await send_message_with_retry(context.bot, chat_id, send_text_please, parse_mode=None); return

    entered_code = update.message.text.strip()
    context.user_data.pop('state', None)
    view_basket_button_text = lang_data.get("view_basket_button", "View Basket"); returning_to_basket_msg = lang_data.get("returning_to_basket", "Returning to basket.")

    if not entered_code: no_code_entered_msg = lang_data.get("no_code_entered", "No code entered."); await send_message_with_retry(context.bot, chat_id, no_code_entered_msg, parse_mode=None); keyboard = [[InlineKeyboardButton(view_basket_button_text, callback_data="view_basket")]]; await send_message_with_retry(context.bot, chat_id, returning_to_basket_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None); return

    clear_expired_basket(context, user_id)
    basket = context.user_data.get("basket", [])
    total_after_reseller_decimal = Decimal('0.0') # <<< Base total for validation

    if basket:
         try:
            for item in basket:
                original_price = item.get('price', Decimal('0.0'))
                product_type = item.get('product_type', '')
                reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, product_type)
                item_reseller_discount = (original_price * reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                total_after_reseller_decimal += (original_price - item_reseller_discount)
         except Exception as e:
             logger.error(f"Error recalculating reseller-adjusted total user {user_id}: {e}"); error_calc_total = lang_data.get("error_calculating_total", "Error calculating total."); await send_message_with_retry(context.bot, chat_id, f"❌ {error_calc_total}", parse_mode=None); kb = [[InlineKeyboardButton(view_basket_button_text, callback_data="view_basket")]]; await send_message_with_retry(context.bot, chat_id, returning_to_basket_msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None); return
    else:
        basket_empty_no_discount = lang_data.get("basket_empty_no_discount", "Basket empty. Cannot apply code."); await send_message_with_retry(context.bot, chat_id, basket_empty_no_discount, parse_mode=None); kb = [[InlineKeyboardButton(view_basket_button_text, callback_data="view_basket")]]; await send_message_with_retry(context.bot, chat_id, returning_to_basket_msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None); return

    # SECURITY: Use atomic validation to prevent race conditions and multiple uses
    code_valid, validation_message, discount_details = validate_and_apply_discount_atomic(entered_code, float(total_after_reseller_decimal), user_id)

    if code_valid and discount_details:
        context.user_data['applied_discount'] = {'code': entered_code, 'amount': discount_details['discount_amount'], 'final_total': discount_details['final_total']}
        logger.info(f"User {user_id} applied general discount code '{entered_code}'.")
        success_label = lang_data.get("success_label", "Success!")
        feedback_msg = f"✅ {success_label} {validation_message}"
    else:
        context.user_data.pop('applied_discount', None)
        logger.warning(f"User {user_id} failed to apply general code '{entered_code}': {validation_message}")
        feedback_msg = f"❌ {validation_message}"

    keyboard = [[InlineKeyboardButton(view_basket_button_text, callback_data="view_basket")]]
    await send_message_with_retry(context.bot, chat_id, feedback_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

# --- END handle_user_discount_code_message ---


# --- Remove From Basket ---
async def handle_remove_from_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    if not params: logger.warning(f"handle_remove_from_basket no product_id user {user_id}."); await query.answer("Error: Product ID missing.", show_alert=True); return
    try: product_id_to_remove = int(params[0])
    except ValueError: logger.warning(f"Invalid product_id format user {user_id}: {params[0]}"); await query.answer("Error: Invalid product data.", show_alert=True); return

    logger.info(f"Attempting remove product {product_id_to_remove} user {user_id}.")
    item_removed_from_context = False; item_to_remove_str = None; conn = None
    current_basket_context = context.user_data.get("basket", []); new_basket_context = []
    found_item_index = -1

    for index, item in enumerate(current_basket_context):
        if item.get('product_id') == product_id_to_remove:
            found_item_index = index
            try: timestamp_float = float(item['timestamp']); item_to_remove_str = f"{item['product_id']}:{timestamp_float}"
            except (ValueError, TypeError, KeyError) as e: logger.error(f"Invalid format in context item {item}: {e}"); item_to_remove_str = None
            break

    if found_item_index != -1:
        item_removed_from_context = True
        new_basket_context = current_basket_context[:found_item_index] + current_basket_context[found_item_index+1:]
        logger.debug(f"Found item {product_id_to_remove} in context user {user_id}. DB String: {item_to_remove_str}")
    else: logger.warning(f"Product {product_id_to_remove} not in user_data basket user {user_id}."); new_basket_context = list(current_basket_context)

    try:
        conn = get_db_connection()
        c = conn.cursor(); c.execute("BEGIN")
        if item_removed_from_context:
             update_result = c.execute("UPDATE products SET reserved = MAX(0, reserved - 1) WHERE id = ?", (product_id_to_remove,))
             if update_result.rowcount > 0: logger.debug(f"Decremented reservation P{product_id_to_remove}.")
             else: logger.warning(f"Could not find P{product_id_to_remove} to decrement reservation (maybe already cleared?).")
        c.execute("SELECT basket FROM users WHERE user_id = ?", (user_id,))
        db_basket_result = c.fetchone(); db_basket_str = db_basket_result['basket'] if db_basket_result else ''
        if db_basket_str and item_to_remove_str:
            items_list = db_basket_str.split(',')
            if item_to_remove_str in items_list:
                items_list.remove(item_to_remove_str); new_db_basket_str = ','.join(items_list)
                c.execute("UPDATE users SET basket = ? WHERE user_id = ?", (new_db_basket_str, user_id)); logger.debug(f"Updated DB basket user {user_id} to: {new_db_basket_str}")
            else: logger.warning(f"Item string '{item_to_remove_str}' not found in DB basket '{db_basket_str}' user {user_id}.")
        elif item_removed_from_context and not item_to_remove_str: logger.warning(f"Could not construct item string for DB removal P{product_id_to_remove}.")
        elif not item_removed_from_context: logger.debug(f"Item {product_id_to_remove} not in context, DB basket not modified.")
        conn.commit()
        logger.info(f"DB ops complete remove P{product_id_to_remove} user {user_id}.")
        context.user_data['basket'] = new_basket_context

        if not context.user_data['basket']:
            context.user_data.pop('applied_discount', None)
        elif context.user_data.get('applied_discount'):
            applied_discount_info = context.user_data['applied_discount']
            total_after_reseller_decimal = Decimal('0.0')
            for item in context.user_data['basket']:
                original_price = item.get('price', Decimal('0.0'))
                product_type = item.get('product_type', '')
                reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, product_type)
                item_reseller_discount = (original_price * reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                total_after_reseller_decimal += (original_price - item_reseller_discount)
            code_valid, validation_message, _ = validate_discount_code(applied_discount_info['code'], float(total_after_reseller_decimal))
            if not code_valid:
                reason_removed = lang_data.get("discount_removed_invalid_basket", "Discount removed (basket changed).")
                logger.info(f"Removing invalid general discount '{applied_discount_info['code']}' for user {user_id} after item removal.")
                context.user_data.pop('applied_discount', None);
                await query.answer(reason_removed, show_alert=False)

    except sqlite3.Error as e:
        if conn and conn.in_transaction: conn.rollback()
        logger.error(f"DB error removing item {product_id_to_remove} user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Failed to remove item (DB).", parse_mode=None); return
    except Exception as e:
        if conn and conn.in_transaction: conn.rollback()
        logger.error(f"Unexpected error removing item {product_id_to_remove} user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue removing item.", parse_mode=None); return
    finally:
        if conn: conn.close()
    await handle_view_basket(update, context)
# --- END handle_remove_from_basket ---


async def handle_clear_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    conn = None

    current_basket_context = context.user_data.get("basket", [])
    if not current_basket_context: already_empty_msg = lang_data.get("basket_already_empty", "Basket already empty."); await query.answer(already_empty_msg, show_alert=False); return await handle_view_basket(update, context)

    product_ids_to_release_counts = Counter(item['product_id'] for item in current_basket_context)

    try:
        conn = get_db_connection()
        c = conn.cursor(); c.execute("BEGIN"); c.execute("UPDATE users SET basket = '' WHERE user_id = ?", (user_id,))
        if product_ids_to_release_counts:
             decrement_data = [(count, pid) for pid, count in product_ids_to_release_counts.items()]
             c.executemany("UPDATE products SET reserved = MAX(0, reserved - ?) WHERE id = ?", decrement_data)
             total_items_released = sum(product_ids_to_release_counts.values()); logger.info(f"Released {total_items_released} reservations user {user_id} clear.")
        conn.commit()
        context.user_data["basket"] = []; context.user_data.pop('applied_discount', None)
        logger.info(f"Cleared basket/discount user {user_id}.")
        shop_button_text = lang_data.get("shop_button", "Shop"); home_button_text = lang_data.get("home_button", "Home")
        cleared_msg = lang_data.get("basket_cleared", "🗑️ Basket Cleared!")
        keyboard = [[InlineKeyboardButton(f"{EMOJI_SHOP} {shop_button_text}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]]
        await query.edit_message_text(cleared_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e:
        if conn and conn.in_transaction: conn.rollback()
        logger.error(f"DB error clearing basket user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: DB issue clearing basket.", parse_mode=None)
    except Exception as e:
        if conn and conn.in_transaction: conn.rollback()
        logger.error(f"Unexpected error clearing basket user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue.", parse_mode=None)
    finally:
        if conn: conn.close()


# --- Confirm Pay Handler (Modified for Reseller Discount) ---
async def handle_confirm_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the 'Pay Now' button press from the basket."""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang, lang_data = _get_lang_data(context)

    clear_expired_basket(context, user_id)
    basket = context.user_data.get("basket", [])
    applied_discount_info = context.user_data.get('applied_discount')

    if not basket:
        await query.answer("Your basket is empty!", show_alert=True)
        await handle_view_basket(update, context)
        return

    conn = None
    original_total = Decimal('0.0')
    total_after_reseller = Decimal('0.0')
    final_total = Decimal('0.0')
    valid_basket_items_snapshot = []
    discount_code_to_use = None
    user_balance = Decimal('0.0')
    error_occurred = False

    try:
        conn = get_db_connection()
        c = conn.cursor()

        product_ids_in_basket = list(set(item['product_id'] for item in basket))
        if not product_ids_in_basket:
             logger.warning(f"Basket context had items, but no product IDs found for user {user_id}.")
             await query.answer("Basket empty after validation.", show_alert=True)
             await handle_view_basket(update, context)
             return

        placeholders = ','.join('?' for _ in product_ids_in_basket)
        # MODIFIED: Fetch city, district, original_text
        c.execute(f"SELECT id, price, name, size, product_type, city, district, original_text FROM products WHERE id IN ({placeholders})", product_ids_in_basket)
        product_db_details = {row['id']: dict(row) for row in c.fetchall()}

        for item_context in basket:
             prod_id = item_context.get('product_id')
             if prod_id in product_db_details:
                 details = product_db_details[prod_id]
                 item_original_price = Decimal(str(details['price']))
                 item_product_type = details['product_type']
                 original_total += item_original_price
                 item_reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, item_product_type)
                 item_reseller_discount = (item_original_price * item_reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                 item_price_after_reseller = item_original_price - item_reseller_discount
                 total_after_reseller += item_price_after_reseller
                 # MODIFIED: Enrich item_snapshot
                 item_snapshot = {
                     "product_id": prod_id, "price": float(item_original_price),
                     "name": details['name'], "size": details['size'],
                     "product_type": item_product_type,
                     "city": details['city'],
                     "district": details['district'],
                     "original_text": details.get('original_text')
                 }
                 valid_basket_items_snapshot.append(item_snapshot)
             else: logger.warning(f"Product {prod_id} missing during payment confirm user {user_id} (DB fetch).")

        if not valid_basket_items_snapshot:
             context.user_data['basket'] = []
             context.user_data.pop('applied_discount', None)
             logger.warning(f"All items unavailable user {user_id} payment confirm.")
             keyboard_back = [[InlineKeyboardButton("⬅️ Back", callback_data="view_basket")]]
             try: await query.edit_message_text("❌ Error: All items unavailable.", reply_markup=InlineKeyboardMarkup(keyboard_back), parse_mode=None)
             except telegram_error.BadRequest: await send_message_with_retry(context.bot, chat_id, "❌ Error: All items unavailable.", reply_markup=InlineKeyboardMarkup(keyboard_back), parse_mode=None)
             return

        final_total = total_after_reseller
        if applied_discount_info:
            # Note: For existing applied discounts, we don't re-validate atomically to avoid double-charging
            code_valid, _, discount_details = validate_discount_code(applied_discount_info['code'], float(total_after_reseller))
            if code_valid and discount_details:
                final_total = Decimal(str(discount_details['final_total']))
                discount_code_to_use = applied_discount_info.get('code')
                context.user_data['applied_discount']['final_total'] = float(final_total)
                context.user_data['applied_discount']['amount'] = discount_details['discount_amount']
            else:
                final_total = total_after_reseller
                discount_code_to_use = None
                context.user_data.pop('applied_discount', None)
                await query.answer("Applied discount code became invalid.", show_alert=True)

        if final_total < Decimal('0.0'): final_total = Decimal('0.0')
        c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        balance_result = c.fetchone()
        user_balance = Decimal(str(balance_result['balance'])) if balance_result else Decimal('0.0')
    except (sqlite3.Error, Exception) as e:
        logger.error(f"Error during payment confirm data processing user {user_id}: {e}", exc_info=True)
        error_occurred = True
        kb = [[InlineKeyboardButton("⬅️ Back", callback_data="view_basket")]]
        try: await query.edit_message_text("❌ Error preparing payment.", reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
        except Exception as edit_err: logger.error(f"Failed to edit message in error handler: {edit_err}")
    finally:
        if conn: conn.close(); logger.debug("DB connection closed in handle_confirm_pay.")

    if error_occurred: return

    logger.info(f"Payment confirm user {user_id}. Final Total (after all discounts): {final_total:.2f}, Balance: {user_balance:.2f}. Basket Snapshot: {valid_basket_items_snapshot}")

    if user_balance >= final_total:
        logger.info(f"User {user_id} has sufficient balance ({user_balance:.2f} EUR) for purchase ({final_total:.2f} EUR). Processing balance payment.")
        success = await payment.process_purchase_with_balance(user_id, final_total, valid_basket_items_snapshot, discount_code_to_use, context)
        if success:
            try: pass # User notification handled in process_purchase_with_balance
            except telegram_error.BadRequest: pass
    else:
        logger.info(f"Insufficient balance user {user_id}. Prompting to top up first.")
        
        # Unreserve items since user needs to top up first
        try:
            _unreserve_basket_items(valid_basket_items_snapshot)
            logger.info(f"Un-reserved items for user {user_id} - needs to top up first")
        except Exception as unreserve_e:
            logger.warning(f"Error unreserving items for user {user_id}: {unreserve_e}")
        
        # Calculate how much more they need
        needed_amount = final_total - user_balance
        
        # Build clear message
        insufficient_msg = f"⚠️ <b>Insufficient Balance!</b>\n\n"
        insufficient_msg += f"💰 Your Balance: <b>{format_currency(user_balance)} EUR</b>\n"
        insufficient_msg += f"🛒 Cart Total: <b>{format_currency(final_total)} EUR</b>\n"
        insufficient_msg += f"📊 You need: <b>{format_currency(needed_amount)} EUR</b> more\n\n"
        insufficient_msg += f"💡 <b>Please top up your balance first, then come back to complete your purchase!</b>"
        
        top_up_button = lang_data.get("top_up_button", "💳 Top Up Balance")
        apply_discount_button = lang_data.get("apply_discount_pay_button", "🏷️ Apply Discount Code")
        back_basket_button = lang_data.get("back_basket_button", "Back to Basket")
        
        keyboard = [
             [InlineKeyboardButton(top_up_button, callback_data="refill")],
             [InlineKeyboardButton(apply_discount_button, callback_data="apply_discount_basket_pay")],
             [InlineKeyboardButton(f"⬅️ {back_basket_button}", callback_data="view_basket")]
        ]
        
        # Store discount code in case they apply one after topping up
        context.user_data['pending_discount_code'] = discount_code_to_use
        
        await query.edit_message_text(insufficient_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
        await query.answer("Please top up first!", show_alert=False)
# --- END handle_confirm_pay ---


# --- NEW: Handler to Ask for Discount Code in Basket Pay Flow ---
async def handle_apply_discount_basket_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    if 'basket_pay_snapshot' not in context.user_data or 'basket_pay_total_eur' not in context.user_data:
        logger.error(f"User {user_id} clicked apply_discount_basket_pay but context is missing.")
        await query.answer("Error: Context lost. Please go back to basket.", show_alert=True)
        return await handle_view_basket(update, context)

    context.user_data['state'] = 'awaiting_basket_discount_code'
    prompt_msg = lang_data.get("basket_pay_enter_discount", "Please enter discount code for this purchase:")
    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="skip_discount_basket_pay")]]

    await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter discount code in chat.")


# --- NEW: Message Handler for Basket Pay Discount Code ---
async def handle_basket_discount_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    state = context.user_data.get("state")
    lang, lang_data = _get_lang_data(context)

    if state != "awaiting_basket_discount_code": return
    if not update.message or not update.message.text: return

    entered_code = update.message.text.strip()
    context.user_data.pop('state', None)

    basket_snapshot = context.user_data.get('basket_pay_snapshot')
    total_after_reseller_float = context.user_data.get('basket_pay_total_eur')

    if basket_snapshot is None or total_after_reseller_float is None:
        logger.error(f"User {user_id} sent basket discount code but snapshot/total context is missing.")
        await send_message_with_retry(context.bot, chat_id, "Error: Context lost. Returning to basket.", parse_mode=None)
        context.user_data.pop('basket_pay_snapshot', None); context.user_data.pop('basket_pay_total_eur', None); context.user_data.pop('basket_pay_discount_code', None)
        return await handle_view_basket(update, context)

    if not entered_code:
        await send_message_with_retry(context.bot, chat_id, lang_data.get("no_code_entered", "No code entered. Please try again or go back to basket."), parse_mode=None)
        return

    # SECURITY: Use atomic validation to prevent race conditions and multiple uses
    code_valid, validation_message, discount_details = validate_and_apply_discount_atomic(entered_code, total_after_reseller_float, user_id)
    
    try: await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except Exception as e: logger.warning(f"Could not delete user's discount code message: {e}")
    
    if code_valid and discount_details:
        new_final_total = Decimal(str(discount_details['final_total']))
        context.user_data['basket_pay_total_eur'] = float(new_final_total)
        context.user_data['basket_pay_discount_code'] = entered_code
        logger.info(f"User {user_id} applied valid basket discount '{entered_code}'. New total: {new_final_total:.2f} EUR")
        
        # Check balance after discount
        user_balance = Decimal('0.0')
        try:
            conn_bal = get_db_connection()
            c_bal = conn_bal.cursor()
            c_bal.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
            bal_row = c_bal.fetchone()
            if bal_row:
                user_balance = Decimal(str(bal_row['balance']))
            conn_bal.close()
        except Exception as e:
            logger.error(f"Error checking balance after discount: {e}")
        
        if user_balance >= new_final_total:
            # Sufficient balance after discount - process payment!
            logger.info(f"User {user_id} now has sufficient balance after discount. Processing payment.")
            await send_message_with_retry(context.bot, chat_id, f"✅ Code '{entered_code}' applied! New total: {format_currency(new_final_total)} EUR. Processing payment...", parse_mode=None)
            
            snapshot = context.user_data.get('basket_pay_snapshot')
            success = await payment.process_purchase_with_balance(user_id, new_final_total, snapshot, entered_code, context)
            
            from utils import clear_reservation_tracking
            clear_reservation_tracking(user_id)
            
            context.user_data.pop('basket_pay_snapshot', None)
            context.user_data.pop('basket_pay_total_eur', None)
            context.user_data.pop('basket_pay_discount_code', None)
        else:
            # Still insufficient - show top up prompt
            needed_amount = new_final_total - user_balance
            
            msg = f"✅ Code '<b>{entered_code}</b>' applied!\n\n"
            msg += f"💰 New Total: <b>{format_currency(new_final_total)} EUR</b>\n"
            msg += f"💳 Your Balance: <b>{format_currency(user_balance)} EUR</b>\n"
            msg += f"📊 You still need: <b>{format_currency(needed_amount)} EUR</b>\n\n"
            msg += f"💡 <b>Please top up to complete your purchase!</b>"
            
            top_up_button = lang_data.get("top_up_button", "💳 Top Up Balance")
            back_basket_button = lang_data.get("back_basket_button", "Back to Basket")
            
            keyboard = [
                [InlineKeyboardButton(top_up_button, callback_data="refill")],
                [InlineKeyboardButton(f"⬅️ {back_basket_button}", callback_data="view_basket")]
            ]
            await send_message_with_retry(context.bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    else:
        # Invalid code
        context.user_data['basket_pay_discount_code'] = None
        logger.warning(f"User {user_id} entered invalid basket discount '{entered_code}': {validation_message}")
        
        msg = f"❌ Code invalid: {validation_message}\n\nPlease try another code or top up your balance."
        
        apply_discount_button = lang_data.get("apply_discount_pay_button", "🏷️ Try Another Code")
        top_up_button = lang_data.get("top_up_button", "💳 Top Up Balance")
        back_basket_button = lang_data.get("back_basket_button", "Back to Basket")
        
        keyboard = [
            [InlineKeyboardButton(apply_discount_button, callback_data="apply_discount_basket_pay")],
            [InlineKeyboardButton(top_up_button, callback_data="refill")],
            [InlineKeyboardButton(f"⬅️ {back_basket_button}", callback_data="view_basket")]
        ]
        await send_message_with_retry(context.bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


# --- Handler to Skip Discount in Basket Pay Flow (Legacy - redirects to basket) ---
async def handle_skip_discount_basket_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """This handler is now legacy since we removed direct crypto payments for purchases.
    Redirects users to the basket view."""
    query = update.callback_query
    user_id = query.from_user.id
    logger.info(f"User {user_id} triggered legacy skip_discount_basket_pay. Redirecting to basket.")
    await query.answer("Please top up first to make purchases.", show_alert=True)
    return await handle_view_basket(update, context)


# --- NEW: Helper to Show Crypto Choices for Basket Payment ---
# <<< MODIFIED: Use SUPPORTED_CRYPTO dictionary >>>
async def _show_crypto_choices_for_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, edit_message: bool = False):
    """Displays cryptocurrency selection buttons for the basket total."""
    query = update.callback_query # May be None if called from message handler
    chat_id = update.effective_chat.id
    lang, lang_data = _get_lang_data(context)

    # Determine if this is for single item or full basket based on context keys
    is_single_item_flow = 'single_item_pay_final_eur' in context.user_data and 'single_item_pay_snapshot' in context.user_data
    
    if is_single_item_flow:
        total_eur_float = context.user_data.get('single_item_pay_final_eur')
        # Map single item context to basket_pay context for NOWPayments call consistency
        context.user_data['basket_pay_snapshot'] = context.user_data['single_item_pay_snapshot']
        context.user_data['basket_pay_total_eur'] = context.user_data['single_item_pay_final_eur']
        context.user_data['basket_pay_discount_code'] = context.user_data.get('single_item_pay_discount_code')
        cancel_callback = f"product|{context.user_data['single_item_pay_back_params'][0]}|{context.user_data['single_item_pay_back_params'][1]}|{context.user_data['single_item_pay_back_params'][2]}|{context.user_data['single_item_pay_back_params'][3]}|{context.user_data['single_item_pay_back_params'][4]}"

    else: # Full basket flow
        total_eur_float = context.user_data.get('basket_pay_total_eur')
        cancel_callback = "view_basket"


    if total_eur_float is None:
        logger.error("Cannot show crypto choices: total EUR missing from context.")
        msg = "Error: Payment amount missing. Returning to previous screen."
        kb = [[InlineKeyboardButton("⬅️ Back", callback_data=cancel_callback)]] # Use dynamic cancel
        if query and edit_message: await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
        else: await send_message_with_retry(context.bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
        if query: await query.answer("Error: Amount missing.", show_alert=True)
        return

    asset_buttons = []
    row = []
    for code, display_name in SUPPORTED_CRYPTO.items():
        row.append(InlineKeyboardButton(display_name, callback_data=f"select_basket_crypto|{code}")) # select_basket_crypto handles both
        if len(row) >= 3:
            asset_buttons.append(row)
            row = []
    if row: asset_buttons.append(row)

    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    asset_buttons.append([InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data=cancel_callback)])

    amount_str = format_currency(total_eur_float)
    prompt_template = lang_data.get("choose_crypto_for_purchase", "Choose crypto to pay {amount} EUR for your items:")
    prompt_msg = prompt_template.format(amount=amount_str)

    if query and edit_message:
        try:
            await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(asset_buttons), parse_mode=None)
        except telegram_error.BadRequest as e:
            if "message is not modified" not in str(e).lower():
                 logger.error(f"Error editing message for crypto choice (basket/single): {e}")
                 await send_message_with_retry(context.bot, chat_id, prompt_msg, reply_markup=InlineKeyboardMarkup(asset_buttons), parse_mode=None)
            elif query: await query.answer()
    else:
        await send_message_with_retry(context.bot, chat_id, prompt_msg, reply_markup=InlineKeyboardMarkup(asset_buttons), parse_mode=None)

# --- END _show_crypto_choices_for_basket ---

# --- CORRECTED Handler for Pay Single Item Button ---
async def handle_pay_single_item(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the 'Pay Now' button directly from product selection."""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang, lang_data = _get_lang_data(context)

    if not params or len(params) < 5:
        logger.warning("handle_pay_single_item missing params."); await query.answer("Error: Incomplete product data.", show_alert=True); return
    city_id, dist_id, p_type, size, price_str = params # price_str is ORIGINAL price

    try: original_price = Decimal(price_str)
    except ValueError: logger.warning(f"Invalid price format pay_single_item: {price_str}"); await query.edit_message_text("❌ Error: Invalid product data.", parse_mode=None); return

    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city or not district: error_location_mismatch = lang_data.get("error_location_mismatch", "Error: Location data mismatch."); await query.edit_message_text(f"❌ {error_location_mismatch}", parse_mode=None); return await handle_shop(update, context)

    await query.answer("⏳ Reserving & preparing payment options...")

    reserved_id = None
    conn = None
    product_details_for_snapshot = None
    error_occurred_reservation = False

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN EXCLUSIVE")
        # MODIFIED: Fetch city, district, original_text for snapshot
        c.execute("SELECT id, name, price, size, product_type, city, district, original_text FROM products WHERE city = ? AND district = ? AND product_type = ? AND size = ? AND price = ? AND available > reserved ORDER BY id LIMIT 1", (city, district, p_type, size, float(original_price)))
        product_to_reserve = c.fetchone()

        if not product_to_reserve:
            conn.rollback()
            logger.warning(f"Item {p_type} {size} in {city}/{district} taken before pay_single user {user_id}.")
            try: await query.edit_message_text("❌ Sorry, this item was just taken!", parse_mode=None)
            except Exception: pass
            error_occurred_reservation = True
        else:
            reserved_id = product_to_reserve['id']
            product_details_for_snapshot = dict(product_to_reserve) # Now contains enriched data
            update_result = c.execute("UPDATE products SET reserved = reserved + 1 WHERE id = ? AND available > reserved", (reserved_id,))
            if update_result.rowcount == 1:
                conn.commit()
                logger.info(f"Successfully reserved product {reserved_id} for single item payment by user {user_id}.")
            else:
                conn.rollback()
                logger.warning(f"Failed to reserve product {reserved_id} (race condition?) for single item payment user {user_id}.")
                try: await query.edit_message_text("❌ Sorry, this item was just taken!", parse_mode=None)
                except Exception: pass
                error_occurred_reservation = True
    except sqlite3.Error as e:
        logger.error(f"DB error reserving single item {p_type} {size} user {user_id}: {e}")
        if conn and conn.in_transaction: conn.rollback()
        try: await query.edit_message_text("❌ Database error during reservation.", parse_mode=None)
        except Exception: pass
        error_occurred_reservation = True
    finally:
        if conn: conn.close()

    if error_occurred_reservation:
        return

    if reserved_id and product_details_for_snapshot:
        # MODIFIED: Enrich single_item_snapshot
        single_item_snapshot = [{
            "product_id": reserved_id,
            "price": float(original_price),
            "name": product_details_for_snapshot['name'],
            "size": product_details_for_snapshot['size'],
            "product_type": product_details_for_snapshot['product_type'],
            "city": product_details_for_snapshot['city'],
            "district": product_details_for_snapshot['district'],
            "original_text": product_details_for_snapshot.get('original_text')
        }]

        reseller_discount_percent = await asyncio.to_thread(get_reseller_discount, user_id, p_type)
        reseller_discount_amount = (original_price * reseller_discount_percent / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        price_after_reseller = original_price - reseller_discount_amount

        # Check user balance FIRST before proceeding
        user_balance = Decimal('0.0')
        try:
            conn_bal = get_db_connection()
            c_bal = conn_bal.cursor()
            c_bal.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
            bal_row = c_bal.fetchone()
            if bal_row:
                user_balance = Decimal(str(bal_row['balance']))
            conn_bal.close()
        except Exception as e:
            logger.error(f"Error checking balance for single item pay: {e}")

        item_name_display = f"{PRODUCT_TYPES.get(p_type, '')} {product_details_for_snapshot['name']} {product_details_for_snapshot['size']}"
        price_display_str = format_currency(price_after_reseller)

        # If user has sufficient balance, proceed with payment
        if user_balance >= price_after_reseller:
            context.user_data['single_item_pay_snapshot'] = single_item_snapshot
            context.user_data['single_item_pay_final_eur'] = float(price_after_reseller)
            context.user_data['single_item_pay_discount_code'] = None
            context.user_data['single_item_pay_back_params'] = params
            
            # Track reservation for abandonment cleanup
            from utils import track_reservation
            track_reservation(user_id, single_item_snapshot, "single")

            prompt_msg = (f"✅ You are about to pay for:\n\n"
                          f"📦 <b>{item_name_display}</b>\n"
                          f"💰 Price: <b>{price_display_str} EUR</b>\n"
                          f"💳 Your Balance: <b>{format_currency(user_balance)} EUR</b>\n\n"
                          f"Do you have a discount code to apply?")
            pay_now_direct_button_text = lang_data.get("pay_now_button", "Pay Now")
            apply_discount_button_text = lang_data.get("apply_discount_pay_button", "🏷️ Apply Discount Code")
            back_to_product_button_text = lang_data.get("back_options_button", "Back to Product")

            keyboard = [
                 [InlineKeyboardButton(f"✅ {pay_now_direct_button_text}", callback_data="skip_discount_single_pay")],
                 [InlineKeyboardButton(apply_discount_button_text, callback_data="apply_discount_single_pay")],
                 [InlineKeyboardButton(f"⬅️ {back_to_product_button_text}", callback_data=f"product|{city_id}|{dist_id}|{p_type}|{size}|{price_str}")]
            ]
            try:
                await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
            except telegram_error.BadRequest as e:
                if "message is not modified" not in str(e).lower():
                    logger.error(f"Error editing message for single item discount prompt: {e}")
                    await send_message_with_retry(context.bot, chat_id, prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
                else:
                    await query.answer()
        else:
            # INSUFFICIENT BALANCE - Unreserve and prompt to top up
            logger.info(f"User {user_id} has insufficient balance for single item. Unreserving and prompting top up.")
            
            # Unreserve the item
            try:
                _unreserve_basket_items(single_item_snapshot)
            except Exception as unreserve_e:
                logger.warning(f"Error unreserving single item for user {user_id}: {unreserve_e}")
            
            needed_amount = price_after_reseller - user_balance
            
            insufficient_msg = f"⚠️ <b>Insufficient Balance!</b>\n\n"
            insufficient_msg += f"📦 Item: <b>{item_name_display}</b>\n"
            insufficient_msg += f"💰 Price: <b>{price_display_str} EUR</b>\n\n"
            insufficient_msg += f"💳 Your Balance: <b>{format_currency(user_balance)} EUR</b>\n"
            insufficient_msg += f"📊 You need: <b>{format_currency(needed_amount)} EUR</b> more\n\n"
            insufficient_msg += f"💡 <b>Please top up your balance first, then come back to purchase!</b>"
            
            top_up_button = lang_data.get("top_up_button", "💳 Top Up Balance")
            back_to_product_button_text = lang_data.get("back_options_button", "Back to Product")
            
            keyboard = [
                 [InlineKeyboardButton(top_up_button, callback_data="refill")],
                 [InlineKeyboardButton(f"⬅️ {back_to_product_button_text}", callback_data=f"product|{city_id}|{dist_id}|{p_type}|{size}|{price_str}")]
            ]
            try:
                await query.edit_message_text(insufficient_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
            except telegram_error.BadRequest as e:
                if "message is not modified" not in str(e).lower():
                    logger.error(f"Error editing message for insufficient balance prompt: {e}")
                    await send_message_with_retry(context.bot, chat_id, insufficient_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
            await query.answer("Please top up first!", show_alert=False)
            return
    else:
        logger.error(f"Reached end of handle_pay_single_item without valid reservation for user {user_id}")
        try: await query.edit_message_text("❌ An internal error occurred during payment initiation.", parse_mode=None)
        except Exception: pass
# --- END handle_pay_single_item ---


# --- Other User Handlers ---
async def handle_view_history(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    history = fetch_last_purchases(user_id, limit=10)

    history_title = lang_data.get("purchase_history_title", "Purchase History"); no_history_msg = lang_data.get("no_purchases_yet", "No purchases yet.")
    recent_purchases_title = lang_data.get("recent_purchases_title", "Recent Purchases"); back_profile_button = lang_data.get("back_profile_button", "Back to Profile")
    home_button = lang_data.get("home_button", "Home"); unknown_date_label = lang_data.get("unknown_date_label", "Unknown Date")

    if not history: msg = f"📜 {history_title}\n\n{no_history_msg}"; keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_profile_button}", callback_data="profile"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
    else:
        msg = f"📜 {recent_purchases_title}\n\n"
        for i, purchase in enumerate(history):
            try:
                # Ensure purchase_date is treated as UTC if no timezone info
                dt_obj = datetime.fromisoformat(purchase['purchase_date'].replace('Z', '+00:00'))
                if dt_obj.tzinfo is None: dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                # Convert to local time if needed, or keep as UTC/formatted
                date_str = dt_obj.strftime('%y-%m-%d %H:%M') # Shorter date format
            except (ValueError, TypeError):
                date_str = "???"
            p_type = purchase.get('product_type', 'Product') # Use get with fallback
            p_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
            p_name = purchase.get('product_name', 'N/A') # Use name from purchase record if available
            p_size = purchase.get('product_size', 'N/A')
            p_price = format_currency(purchase.get('price_paid', 0))
            msg += f"  - {date_str}: {p_emoji} {p_size} ({p_price}€)\n" # Simplified item display
        keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_profile_button}", callback_data="profile"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]

    try: await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Error editing history msg: {e}")
        else: await query.answer()


# --- Language Selection ---
async def handle_language_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Allows the user to select language and immediately refreshes the start menu."""
    query = update.callback_query
    user_id = query.from_user.id
    current_lang, current_lang_data = _get_lang_data(context)
    username = update.effective_user.username or update.effective_user.first_name or f"User_{user_id}"
    conn = None

    if params:
        new_lang = params[0]
        try:
            from utils import LANGUAGES as UTILS_LANGUAGES_SELECT
        except ImportError:
             UTILS_LANGUAGES_SELECT = {'en': {}}
             logger.error("Could not import LANGUAGES from utils in handle_language_selection")

        if new_lang in UTILS_LANGUAGES_SELECT:
            try:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("UPDATE users SET language = ? WHERE user_id = ?", (new_lang, user_id))
                conn.commit()
                logger.info(f"User {user_id} DB language updated to {new_lang}")

                context.user_data["lang"] = new_lang
                logger.info(f"User {user_id} context language updated to {new_lang}")

                # Use the just loaded LANGUAGES dict
                new_lang_data = UTILS_LANGUAGES_SELECT.get(new_lang, UTILS_LANGUAGES_SELECT['en'])
                language_set_answer = new_lang_data.get("language_set_answer", "Language set!")
                await query.answer(language_set_answer.format(lang=new_lang.upper()))

                # <<< FIX: Rebuild and edit start menu >>>
                logger.info(f"Rebuilding start menu in {new_lang} for user {user_id}")
                start_menu_text, start_menu_markup = _build_start_menu_content(user_id, username, new_lang_data, context)
                await query.edit_message_text(start_menu_text, reply_markup=start_menu_markup, parse_mode=None)
                logger.info(f"Successfully edited message to show start menu in {new_lang}")
                # <<< END FIX >>>

            except sqlite3.Error as e:
                logger.error(f"DB error updating language user {user_id}: {e}");
                if conn and conn.in_transaction: conn.rollback()
                error_saving_lang = current_lang_data.get("error_saving_language", "Error saving.")
                await query.answer(error_saving_lang, show_alert=True)
                await _display_language_menu(update, context, current_lang, current_lang_data)
            except Exception as e:
                logger.error(f"Unexpected error in language selection update for user {user_id}: {e}", exc_info=True)
                await query.answer("An error occurred.", show_alert=True)
                await _display_language_menu(update, context, current_lang, current_lang_data)
            finally:
                if conn: conn.close()
        else:
             invalid_lang_answer = current_lang_data.get("invalid_language_answer", "Invalid language selected.")
             await query.answer(invalid_lang_answer, show_alert=True)
    else:
        await _display_language_menu(update, context, current_lang, current_lang_data)

async def _display_language_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, current_lang: str, current_lang_data: dict):
     """Helper function to display the language selection keyboard."""
     query = update.callback_query
     # Need LANGUAGES here
     try: from utils import LANGUAGES as UTILS_LANGUAGES_DISPLAY
     except ImportError: UTILS_LANGUAGES_DISPLAY = {'en': {}}

     keyboard = []
     for lang_code, lang_dict_for_name in UTILS_LANGUAGES_DISPLAY.items():
         lang_name = lang_dict_for_name.get("native_name", lang_code.upper())
         keyboard.append([InlineKeyboardButton(f"{lang_name} {'✅' if lang_code == current_lang else ''}", callback_data=f"language|{lang_code}")])
     back_button_text = current_lang_data.get("back_button", "Back")
     keyboard.append([InlineKeyboardButton(f"{EMOJI_BACK} {back_button_text}", callback_data="back_start")])
     lang_select_prompt = current_lang_data.get("language", "🌐 Select Language:")
     try:
        if query and query.message:
            await query.edit_message_text(lang_select_prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        else:
             await send_message_with_retry(context.bot, update.effective_chat.id, lang_select_prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
     except Exception as e:
         logger.error(f"Error displaying language menu: {e}")
         try:
             await send_message_with_retry(context.bot, update.effective_chat.id, lang_select_prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
         except Exception as send_e:
             logger.error(f"Failed to send language menu after edit error: {send_e}")


# --- Price List ---
async def handle_price_list(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)

    if not CITIES: no_cities_msg = lang_data.get("no_cities_for_prices", "No cities available."); keyboard = [[InlineKeyboardButton(f"{EMOJI_HOME} {lang_data.get('home_button', 'Home')}", callback_data="back_start")]]; await query.edit_message_text(f"{EMOJI_CITY} {no_cities_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None); return

    sorted_city_ids = sorted(CITIES.keys(), key=lambda city_id: CITIES.get(city_id, ''))
    home_button_text = lang_data.get("home_button", "Home")
    keyboard = [[InlineKeyboardButton(f"{EMOJI_CITY} {CITIES.get(c, 'N/A')}", callback_data=f"price_list_city|{c}")] for c in sorted_city_ids if CITIES.get(c)]
    keyboard.append([InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")])
    price_list_title = lang_data.get("price_list_title", "Price List"); select_city_prompt = lang_data.get("select_city_prices_prompt", "Select a city:")
    
    try:
        await query.edit_message_text(f"{EMOJI_PRICELIST} {price_list_title}\n\n{select_city_prompt}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" in str(e).lower():
            # Message is already the same, just answer the callback
            await query.answer()
        else:
            logger.error(f"Error editing price list message: {e}")
            await query.answer("Error displaying price list.", show_alert=True)

async def handle_price_list_city(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    if not params: logger.warning("handle_price_list_city no city_id."); await query.answer("Error: City ID missing.", show_alert=True); return

    city_id = params[0]; city_name = CITIES.get(city_id)
    if not city_name: error_city_not_found = lang_data.get("error_city_not_found", "Error: City not found."); await query.edit_message_text(f"❌ {error_city_not_found}", parse_mode=None); return await handle_price_list(update, context)

    price_list_title_city_template = lang_data.get("price_list_title_city", "Price List: {city_name}"); msg = f"{EMOJI_PRICELIST} {price_list_title_city_template.format(city_name=city_name)}\n\n"
    found_products = False; conn = None

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT product_type, size, price, district, COUNT(*) as quantity FROM products WHERE city = ? AND available > reserved GROUP BY product_type, size, price, district ORDER BY product_type, price, size, district", (city_name,))
        results = c.fetchall()
        no_products_in_city = lang_data.get("no_products_in_city", "No products available here."); available_label = lang_data.get("available_label", "available")

        if not results: msg += no_products_in_city
        else:
            found_products = True
            grouped_data = defaultdict(lambda: defaultdict(list))
            for row in results: price_size_key = (Decimal(str(row['price'])), row['size']); grouped_data[row['product_type']][price_size_key].append((row['district'], row['quantity']))

            for p_type in sorted(grouped_data.keys()):
                type_data = grouped_data[p_type]; sorted_price_size = sorted(type_data.keys(), key=lambda x: (x[0], x[1]))
                prod_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
                for price, size in sorted_price_size:
                    districts_list = type_data[(price, size)]; price_str = format_currency(price)
                    msg += f"\n{prod_emoji} {p_type} {size} ({price_str}€)\n"
                    districts_list.sort(key=lambda x: x[0])
                    for district, quantity in districts_list: msg += f"  • {EMOJI_DISTRICT} {district}\n"

        back_city_list_button = lang_data.get("back_city_list_button", "Back to City List"); home_button = lang_data.get("home_button", "Home")
        keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_city_list_button}", callback_data="price_list"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]

        try:
            if len(msg) > 4000: truncated_note = lang_data.get("message_truncated_note", "Message truncated."); msg = msg[:4000] + f"\n\n✂️ ... {truncated_note}"; logger.warning(f"Price list message truncated {city_name}.")
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except telegram_error.BadRequest as e:
             if "message is not modified" not in str(e).lower():
                 logger.error(f"Error editing price list: {e}. Snippet: {msg[:200]}")
                 error_displaying_prices = lang_data.get("error_displaying_prices", "Error displaying prices.")
                 await query.answer(error_displaying_prices, show_alert=True)
             else:
                 await query.answer()

    except sqlite3.Error as e:
        logger.error(f"DB error fetching price list city {city_name}: {e}", exc_info=True)
        error_loading_prices_db_template = lang_data.get("error_loading_prices_db", "Error: DB Load Error {city_name}")
        await query.edit_message_text(f"❌ {error_loading_prices_db_template.format(city_name=city_name)}", parse_mode=None)
    except Exception as e:
        logger.error(f"Unexpected error price list city {city_name}: {e}", exc_info=True)
        error_unexpected_prices = lang_data.get("error_unexpected_prices", "Error: Unexpected issue.")
        await query.edit_message_text(f"❌ {error_unexpected_prices}", parse_mode=None)
    finally:
         if conn: conn.close()


# --- Review Handlers ---
async def handle_reviews_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    review_prompt = lang_data.get("reviews", "📝 Reviews Menu")
    view_reviews_button = lang_data.get("view_reviews_button", "View Reviews")
    leave_review_button = lang_data.get("leave_review_button", "Leave a Review")
    home_button = lang_data.get("home_button", "Home")
    keyboard = [
        [InlineKeyboardButton(f"👀 {view_reviews_button}", callback_data="view_reviews|0")],
        [InlineKeyboardButton(f"✍️ {leave_review_button}", callback_data="leave_review")],
        [InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(review_prompt, reply_markup=reply_markup, parse_mode=None)


async def handle_leave_review(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    context.user_data["state"] = "awaiting_review"
    enter_review_prompt = lang_data.get("enter_review_prompt", "Please type your review message and send it."); cancel_button_text = lang_data.get("cancel_button", "Cancel"); prompt_msg = f"✍️ {enter_review_prompt}"
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="reviews")]]
    try:
        await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        enter_review_answer = lang_data.get("enter_review_answer", "Enter your review in the chat.")
        await query.answer(enter_review_answer)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Error editing leave review prompt: {e}"); await send_message_with_retry(context.bot, update.effective_chat.id, prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None); await query.answer()
        else: await query.answer()
    except Exception as e: logger.error(f"Unexpected error handle_leave_review: {e}", exc_info=True); await query.answer("Error occurred.", show_alert=True)


async def handle_leave_review_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    state = context.user_data.get("state")
    lang, lang_data = _get_lang_data(context)

    if state != "awaiting_review": return

    send_text_review_please = lang_data.get("send_text_review_please", "Please send text only for your review.")
    review_not_empty = lang_data.get("review_not_empty", "Review cannot be empty. Please try again or cancel.")
    review_too_long = lang_data.get("review_too_long", "Review is too long (max 1000 characters). Please shorten it.")
    review_thanks = lang_data.get("review_thanks", "Thank you for your review! Your feedback helps us improve.")
    error_saving_review_db = lang_data.get("error_saving_review_db", "Error: Could not save your review due to a database issue.")
    error_saving_review_unexpected = lang_data.get("error_saving_review_unexpected", "Error: An unexpected issue occurred while saving your review.")
    view_reviews_button = lang_data.get("view_reviews_button", "View Reviews")
    home_button = lang_data.get("home_button", "Home")

    if not update.message or not update.message.text:
        await send_message_with_retry(context.bot, chat_id, send_text_review_please, parse_mode=None)
        return

    review_text = update.message.text.strip()
    if not review_text:
        await send_message_with_retry(context.bot, chat_id, review_not_empty, parse_mode=None)
        return

    if len(review_text) > 1000:
         await send_message_with_retry(context.bot, chat_id, review_too_long, parse_mode=None)
         return

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            "INSERT INTO reviews (user_id, review_text, review_date) VALUES (?, ?, ?)",
            (user_id, review_text, datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
        logger.info(f"User {user_id} left a review.")
        context.user_data.pop("state", None)

        success_msg = f"✅ {review_thanks}"
        keyboard = [[InlineKeyboardButton(f"👀 {view_reviews_button}", callback_data="view_reviews|0"),
                     InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
        await send_message_with_retry(context.bot, chat_id, success_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e:
        logger.error(f"DB error saving review user {user_id}: {e}", exc_info=True)
        if conn and conn.in_transaction: conn.rollback()
        context.user_data.pop("state", None)
        await send_message_with_retry(context.bot, chat_id, f"❌ {error_saving_review_db}", parse_mode=None)

    except Exception as e:
        logger.error(f"Unexpected error saving review user {user_id}: {e}", exc_info=True)
        if conn and conn.in_transaction: conn.rollback()
        context.user_data.pop("state", None)
        await send_message_with_retry(context.bot, chat_id, f"❌ {error_saving_review_unexpected}", parse_mode=None)

    finally:
        if conn: conn.close()

async def handle_view_reviews(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    offset = 0; reviews_per_page = 5
    if params and len(params) > 0 and params[0].isdigit(): offset = int(params[0])
    reviews_data = fetch_reviews(offset=offset, limit=reviews_per_page + 1)
    user_reviews_title = lang_data.get("user_reviews_title", "User Reviews"); no_reviews_yet = lang_data.get("no_reviews_yet", "No reviews yet."); no_more_reviews = lang_data.get("no_more_reviews", "No more reviews."); prev_button = lang_data.get("prev_button", "Prev"); next_button = lang_data.get("next_button", "Next"); back_review_menu_button = lang_data.get("back_review_menu_button", "Back to Reviews"); unknown_date_label = lang_data.get("unknown_date_label", "Unknown Date"); error_displaying_review = lang_data.get("error_displaying_review", "Error display"); error_updating_review_list = lang_data.get("error_updating_review_list", "Error updating list.")
    msg = f"{EMOJI_REVIEW} {user_reviews_title}\n\n"; keyboard = []
    if not reviews_data:
        if offset == 0: msg += no_reviews_yet; keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_review_menu_button}", callback_data="reviews")]]
        else: msg += no_more_reviews; keyboard = [[InlineKeyboardButton(f"⬅️ {prev_button}", callback_data=f"view_reviews|{max(0, offset - reviews_per_page)}")], [InlineKeyboardButton(f"{EMOJI_BACK} {back_review_menu_button}", callback_data="reviews")]]
    else:
        has_more = len(reviews_data) > reviews_per_page; reviews_to_show = reviews_data[:reviews_per_page]
        for review in reviews_to_show:
            try:
                date_str = review.get('review_date', '')
                formatted_date = unknown_date_label
                if date_str:
                    try: formatted_date = datetime.fromisoformat(date_str.replace('Z', '+00:00')).strftime("%Y-%m-%d")
                    except ValueError: pass
                username = review.get('username', 'anonymous'); username_display = f"@{username}" if username and username != 'anonymous' else username
                review_text = review.get('review_text', ''); msg += f"{EMOJI_PROFILE} {username_display} ({formatted_date}):\n{review_text}\n\n"
            except Exception as e: logger.error(f"Error formatting review: {review}, Error: {e}"); msg += f"({error_displaying_review})\n\n"
        nav_buttons = []
        if offset > 0: nav_buttons.append(InlineKeyboardButton(f"⬅️ {prev_button}", callback_data=f"view_reviews|{max(0, offset - reviews_per_page)}"))
        if has_more: nav_buttons.append(InlineKeyboardButton(f"➡️ {next_button}", callback_data=f"view_reviews|{offset + reviews_per_page}"))
        if nav_buttons: keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton(f"{EMOJI_BACK} {back_review_menu_button}", callback_data="reviews")])
    try: await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.warning(f"Failed edit view_reviews: {e}"); await query.answer(error_updating_review_list, show_alert=True)
        else: await query.answer()

async def handle_leave_review_now(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Callback handler specifically for the 'Leave Review Now' button after purchase."""
    await handle_leave_review(update, context, params)

# --- Refill Handlers ---
async def handle_refill(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang, lang_data = _get_lang_data(context)

    if not SOLANA_ADMIN_WALLET:
        crypto_disabled_msg = lang_data.get("crypto_payment_disabled", "Top Up is currently disabled.")
        await query.answer(crypto_disabled_msg, show_alert=True)
        logger.warning(f"User {user_id} tried to refill, but SOLANA_ADMIN_WALLET is not configured.")
        return

    context.user_data['state'] = 'awaiting_refill_amount'
    logger.info(f"User {user_id} initiated refill process. State -> awaiting_refill_amount.")

    top_up_title = lang_data.get("top_up_title", "Top Up Balance")
    enter_refill_amount_prompt = lang_data.get("enter_refill_amount_prompt", "Please reply with the amount in EUR you wish to add to your balance (e.g., 10 or 25.50).")
    min_top_up_note_template = lang_data.get("min_top_up_note", "Minimum top up: {amount} EUR")
    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    enter_amount_answer = lang_data.get("enter_amount_answer", "Enter the top-up amount.")

    min_amount_str = format_currency(MIN_DEPOSIT_EUR)
    min_top_up_note = min_top_up_note_template.format(amount=min_amount_str)
    prompt_msg = (f"{EMOJI_REFILL} {top_up_title}\n\n{enter_refill_amount_prompt}\n\n{min_top_up_note}")
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="profile")]]

    try:
        await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        await query.answer(enter_amount_answer)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Error editing refill prompt: {e}"); await send_message_with_retry(context.bot, chat_id, prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None); await query.answer()
        else: await query.answer(enter_amount_answer)
    except Exception as e: logger.error(f"Unexpected error handle_refill: {e}", exc_info=True); error_occurred_answer = lang_data.get("error_occurred_answer", "An error occurred."); await query.answer(error_occurred_answer, show_alert=True)

# <<< MODIFIED: Use SUPPORTED_CRYPTO dictionary >>>
async def handle_refill_amount_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    state = context.user_data.get("state")
    lang, lang_data = _get_lang_data(context)

    if state != "awaiting_refill_amount": logger.debug(f"Ignore msg user {user_id}, state: {state}"); return

    send_amount_as_text = lang_data.get("send_amount_as_text", "Send amount as text (e.g., 10).")
    amount_too_low_msg_template = lang_data.get("amount_too_low_msg", "Amount too low. Min: {amount} EUR.")
    amount_too_high_msg = lang_data.get("amount_too_high_msg", "Amount too high. Max: 10000 EUR.")
    invalid_amount_format_msg = lang_data.get("invalid_amount_format_msg", "Invalid amount format (e.g., 10.50).")
    unexpected_error_msg = lang_data.get("unexpected_error_msg", "Unexpected error. Try again.")
    choose_crypto_prompt_template = lang_data.get("choose_crypto_prompt", "Top up {amount} EUR. Choose crypto:")
    cancel_top_up_button = lang_data.get("cancel_top_up_button", "Cancel Top Up")

    if not update.message or not update.message.text:
        await send_message_with_retry(context.bot, chat_id, f"❌ {send_amount_as_text}", parse_mode=None)
        return

    amount_text = update.message.text.strip().replace(',', '.')

    try:
        refill_amount_decimal = Decimal(amount_text)
        if refill_amount_decimal < MIN_DEPOSIT_EUR:
            min_amount_str = format_currency(MIN_DEPOSIT_EUR)
            amount_too_low_msg = amount_too_low_msg_template.format(amount=min_amount_str)
            await send_message_with_retry(context.bot, chat_id, f"❌ {amount_too_low_msg}", parse_mode=None)
            return
        if refill_amount_decimal > Decimal('10000.00'):
            await send_message_with_retry(context.bot, chat_id, f"❌ {amount_too_high_msg}", parse_mode=None)
            return

        context.user_data['refill_eur_amount'] = float(refill_amount_decimal)
        context.user_data['state'] = 'awaiting_refill_crypto_choice' # State remains specific to refill
        logger.info(f"User {user_id} entered refill EUR: {refill_amount_decimal:.2f}. State -> awaiting_refill_crypto_choice")

        # --- Generate buttons using SUPPORTED_CRYPTO ---
        asset_buttons = []
        row = []
        # Iterate through the globally defined dictionary
        for code, display_name in SUPPORTED_CRYPTO.items():
            # Use specific refill callback, passing the NOWPayments code
            row.append(InlineKeyboardButton(display_name, callback_data=f"select_refill_crypto|{code}"))
            if len(row) >= 3: # Adjust number per row if needed
                asset_buttons.append(row)
                row = []
        if row:
            asset_buttons.append(row)
        # --- End Button Generation ---

        asset_buttons.append([InlineKeyboardButton(f"❌ {cancel_top_up_button}", callback_data="profile")])

        refill_amount_str = format_currency(refill_amount_decimal)
        choose_crypto_msg = choose_crypto_prompt_template.format(amount=refill_amount_str)

        await send_message_with_retry(context.bot, chat_id, choose_crypto_msg, reply_markup=InlineKeyboardMarkup(asset_buttons), parse_mode=None)

    except ValueError:
        await send_message_with_retry(context.bot, chat_id, f"❌ {invalid_amount_format_msg}", parse_mode=None)
        return
    except Exception as e:
        logger.error(f"Error processing refill amount user {user_id}: {e}", exc_info=True)
        await send_message_with_retry(context.bot, chat_id, f"❌ {unexpected_error_msg}", parse_mode=None)
        context.user_data.pop('state', None)
        context.user_data.pop('refill_eur_amount', None)

# --- Single Item Discount Code Message Handler ---
async def handle_single_item_discount_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Handles the general discount code entered during the single item crypto pay flow """
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    state = context.user_data.get("state")
    lang, lang_data = _get_lang_data(context)

    if state != "awaiting_single_item_discount_code": return
    if not update.message or not update.message.text: return

    entered_code = update.message.text.strip()
    context.user_data.pop('state', None) # Clear state

    # Retrieve context for this flow
    single_item_snapshot = context.user_data.get('single_item_pay_snapshot')
    price_after_reseller_float = context.user_data.get('single_item_pay_final_eur')

    if single_item_snapshot is None or price_after_reseller_float is None:
        logger.error(f"User {user_id} sent single item discount code but snapshot/total context is missing.")
        await send_message_with_retry(context.bot, chat_id, "Error: Context lost. Returning to product selection.", parse_mode=None)
        context.user_data.pop('single_item_pay_snapshot', None)
        context.user_data.pop('single_item_pay_final_eur', None)
        context.user_data.pop('single_item_pay_discount_code', None)
        back_params = context.user_data.pop('single_item_pay_back_params', None)
        if back_params:
            return await handle_product_selection(update, context, params=back_params) # Go back to product selection
        return

    if not entered_code:
        await send_message_with_retry(context.bot, chat_id, lang_data.get("no_code_entered", "No code entered."), parse_mode=None)
        await handle_skip_discount_single_pay(update, context) # Proceed as if skipped
        return

    # SECURITY: Use atomic validation to prevent race conditions and multiple uses
    code_valid, validation_message, discount_details = validate_and_apply_discount_atomic(entered_code, price_after_reseller_float, user_id)
    feedback_msg_template = ""
    new_final_total_for_single_item_float = price_after_reseller_float

    if code_valid and discount_details:
        new_final_total_for_single_item_float = discount_details['final_total']
        context.user_data['single_item_pay_final_eur'] = new_final_total_for_single_item_float
        context.user_data['single_item_pay_discount_code'] = entered_code
        logger.info(f"User {user_id} applied valid single item discount '{entered_code}'. New FINAL price: {new_final_total_for_single_item_float:.2f} EUR")
        feedback_msg_template = lang_data.get("basket_pay_code_applied", "✅ Code '{code}' applied. New total: {total} EUR. Choose payment method:")
        feedback_msg = feedback_msg_template.format(code=entered_code, total=format_currency(new_final_total_for_single_item_float))
    else:
        context.user_data['single_item_pay_discount_code'] = None
        logger.warning(f"User {user_id} entered invalid single item discount '{entered_code}': {validation_message}")
        price_to_pay_str = format_currency(price_after_reseller_float)
        feedback_msg_template = lang_data.get("basket_pay_code_invalid", "❌ Code invalid: {reason}. Choose payment method to pay {total} EUR:")
        feedback_msg = feedback_msg_template.format(reason=validation_message, total=price_to_pay_str)

    try: await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except Exception as e: logger.warning(f"Could not delete user's single item discount code message: {e}")

    await send_message_with_retry(context.bot, chat_id, feedback_msg, parse_mode=None)

    # Re-evaluate payment options
    final_total_decimal = Decimal(str(context.user_data['single_item_pay_final_eur']))
    snapshot = context.user_data['single_item_pay_snapshot']
    discount_code_to_use = context.user_data.get('single_item_pay_discount_code')

    conn_balance = None; user_balance = Decimal('0.0'); balance_check_error = False
    try:
        conn_balance = get_db_connection()
        c_balance = conn_balance.cursor()
        c_balance.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        balance_result = c_balance.fetchone()
        user_balance = Decimal(str(balance_result['balance'])) if balance_result else Decimal('0.0')
    except sqlite3.Error as e:
        logger.error(f"DB error fetching balance after single item discount: {e}")
        await asyncio.to_thread(_unreserve_basket_items, snapshot)
        await send_message_with_retry(context.bot, chat_id, "❌ Error checking balance. Item released.", parse_mode=None)
        balance_check_error = True
    finally:
        if conn_balance: conn_balance.close()

    if balance_check_error:
        context.user_data.pop('single_item_pay_snapshot', None)
        context.user_data.pop('single_item_pay_final_eur', None)
        context.user_data.pop('single_item_pay_discount_code', None)
        context.user_data.pop('single_item_pay_back_params', None)
        return

    if user_balance >= final_total_decimal:
        logger.info(f"User {user_id} has sufficient balance ({user_balance:.2f} EUR) for single item purchase ({final_total_decimal:.2f} EUR). Processing balance payment.")
        success = await payment.process_purchase_with_balance(user_id, final_total_decimal, snapshot, discount_code_to_use, context)
        
        # Clear reservation tracking since payment completed
        from utils import clear_reservation_tracking
        clear_reservation_tracking(user_id)
        
        context.user_data.pop('single_item_pay_snapshot', None)
        context.user_data.pop('single_item_pay_final_eur', None)
        context.user_data.pop('single_item_pay_discount_code', None)
        context.user_data.pop('single_item_pay_back_params', None)
    else:
        # Still insufficient balance even after discount - unreserve and prompt to top up
        logger.info(f"User {user_id} still has insufficient balance after discount. Prompting to top up.")
        await asyncio.to_thread(_unreserve_basket_items, snapshot)
        
        # Clear single item context
        back_params = context.user_data.pop('single_item_pay_back_params', None)
        context.user_data.pop('single_item_pay_snapshot', None)
        context.user_data.pop('single_item_pay_final_eur', None)
        context.user_data.pop('single_item_pay_discount_code', None)
        
        needed_amount = final_total_decimal - user_balance
        
        insufficient_msg = f"⚠️ <b>Still Insufficient Balance!</b>\n\n"
        insufficient_msg += f"💰 Price after discount: <b>{format_currency(final_total_decimal)} EUR</b>\n"
        insufficient_msg += f"💳 Your Balance: <b>{format_currency(user_balance)} EUR</b>\n"
        insufficient_msg += f"📊 You need: <b>{format_currency(needed_amount)} EUR</b> more\n\n"
        insufficient_msg += f"💡 <b>Please top up your balance first!</b>"
        
        top_up_button = lang_data.get("top_up_button", "💳 Top Up Balance")
        back_button_text = lang_data.get("back_options_button", "Back to Product")
        
        keyboard = [[InlineKeyboardButton(top_up_button, callback_data="refill")]]
        if back_params:
            keyboard.append([InlineKeyboardButton(f"⬅️ {back_button_text}", callback_data=f"product|{back_params[0]}|{back_params[1]}|{back_params[2]}|{back_params[3]}|{back_params[4]}")])
        
        await send_message_with_retry(context.bot, chat_id, insufficient_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

# --- NEW: Handler to Ask for Discount Code in Single Item Pay Flow ---
async def handle_apply_discount_single_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    # Check if the single item payment context exists
    if 'single_item_pay_snapshot' not in context.user_data or \
       'single_item_pay_final_eur' not in context.user_data or \
       'single_item_pay_back_params' not in context.user_data:
        logger.error(f"User {user_id} clicked apply_discount_single_pay but single_item context is missing.")
        await query.answer("Error: Payment context lost. Please try again.", show_alert=True)
        # Attempt to go back to the product selection
        back_params = context.user_data.get('single_item_pay_back_params')
        if back_params:
            # Ensure handle_product_selection is awaitable if called directly
            return await handle_product_selection(update, context, params=back_params)
        else: # Fallback if even back_params are lost
            return await handle_shop(update, context)


    context.user_data['state'] = 'awaiting_single_item_discount_code'
    prompt_msg = lang_data.get("basket_pay_enter_discount", "Please enter discount code for this purchase:") # Re-use existing lang string
    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    
    # The cancel button should skip discount and go to crypto choices for single item
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="skip_discount_single_pay")]]

    await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter discount code in chat.")

# --- NEW: Handler to Skip Discount in Single Item Pay Flow - Now pays with balance ---
async def handle_skip_discount_single_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang, lang_data = _get_lang_data(context)

    # Check if the single item payment context exists
    if 'single_item_pay_snapshot' not in context.user_data or 'single_item_pay_final_eur' not in context.user_data or 'single_item_pay_back_params' not in context.user_data:
        logger.warning(f"User {user_id} clicked skip_discount_single_pay but single_item context is missing. Redirecting to shop.")
        await query.answer("Session expired. Redirecting to shop...", show_alert=True)
        back_params = context.user_data.get('single_item_pay_back_params')
        if back_params:
            return await handle_product_selection(update, context, params=back_params)
        else:
            return await handle_shop(update, context)

    # Get payment details
    snapshot = context.user_data['single_item_pay_snapshot']
    final_total = Decimal(str(context.user_data['single_item_pay_final_eur']))
    discount_code = context.user_data.get('single_item_pay_discount_code')

    await query.answer("Processing payment...")
    
    # Process payment with balance
    logger.info(f"User {user_id} proceeding with balance payment for single item ({final_total:.2f} EUR)")
    success = await payment.process_purchase_with_balance(user_id, final_total, snapshot, discount_code, context)
    
    # Clear reservation tracking since payment completed
    from utils import clear_reservation_tracking
    clear_reservation_tracking(user_id)
    
    # Clear context
    context.user_data.pop('single_item_pay_snapshot', None)
    context.user_data.pop('single_item_pay_final_eur', None)
    context.user_data.pop('single_item_pay_discount_code', None)
    context.user_data.pop('single_item_pay_back_params', None)
