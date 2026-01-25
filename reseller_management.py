# --- START OF FILE reseller_management.py ---

import sqlite3
import logging
import time
from decimal import Decimal, ROUND_DOWN # Use Decimal for precision
import math # For pagination calculation

# --- Telegram Imports ---
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import telegram.error as telegram_error
# -------------------------

# Import shared elements from utils
from utils import (
    ADMIN_ID, LANGUAGES, get_db_connection, send_message_with_retry,
    PRODUCT_TYPES, format_currency, log_admin_action, load_all_data,
    DEFAULT_PRODUCT_EMOJI,
    # Import action constants for logging
    ACTION_RESELLER_ENABLED, ACTION_RESELLER_DISABLED,
    ACTION_RESELLER_DISCOUNT_ADD, ACTION_RESELLER_DISCOUNT_EDIT,
    ACTION_RESELLER_DISCOUNT_DELETE,
    # Admin helper functions
    is_primary_admin, is_secondary_admin, is_any_admin
)

# Logging setup specific to this module
logger = logging.getLogger(__name__)

# Constants
USERS_PER_PAGE_DISCOUNT_SELECT = 10 # Keep for selecting reseller for discount mgmt

# --- Helper Function to Get Reseller Discount ---
# (Keep this function as is)
async def get_reseller_discount_with_connection(cursor, user_id: int, product_type: str) -> Decimal:
    """Fetches the discount percentage for a specific reseller and product type using existing cursor."""
    discount = Decimal('0.0')
    
    try:
        # Enhanced logging for debugging
        logger.info(f"Checking reseller discount for user {user_id}, product type '{product_type}'")
        
        cursor.execute("SELECT is_reseller FROM users WHERE user_id = ?", (user_id,))
        res = cursor.fetchone()
        
        if not res:
            logger.warning(f"User {user_id} not found in database for reseller discount check")
            return discount
            
        is_reseller = res['is_reseller']
        logger.info(f"User {user_id} reseller status: {is_reseller} (1=reseller, 0=not reseller)")
        
        if res and res['is_reseller'] == 1:
            # User is a reseller, get their discount for this product type
            cursor.execute("""
                SELECT discount_percentage FROM reseller_discounts 
                WHERE reseller_user_id = ? AND product_type = ?
            """, (user_id, product_type))
            
            discount_result = cursor.fetchone()
            if discount_result:
                discount = Decimal(str(discount_result['discount_percentage']))
                logger.info(f"Reseller discount for user {user_id}, type '{product_type}': {discount}%")
            else:
                logger.info(f"No specific discount found for reseller {user_id}, type '{product_type}'. Using 0%")
        else:
            logger.info(f"User {user_id} is not a reseller (is_reseller={is_reseller}), returning 0% discount")
            
    except sqlite3.Error as e:
        logger.error(f"DB error fetching reseller discount for user {user_id}, type {product_type}: {e}")
        return Decimal('0.0')  # Return 0% discount on error
    except Exception as e:
        logger.error(f"Unexpected error in reseller discount check for user {user_id}: {e}", exc_info=True)
        return Decimal('0.0')
    
    return discount

def get_reseller_discount(user_id: int, product_type: str) -> Decimal:
    """Fetches the discount percentage for a specific reseller and product type."""
    discount = Decimal('0.0')
    conn = None
    max_retries = 3
    retry_delay = 0.1  # 100ms
    
    for attempt in range(max_retries):
        try:
            conn = get_db_connection()
            c = conn.cursor()
            
            # Enhanced logging for debugging
            logger.info(f"Checking reseller discount for user {user_id}, product type '{product_type}'")
            
            c.execute("SELECT is_reseller FROM users WHERE user_id = ?", (user_id,))
            res = c.fetchone()
            
            if not res:
                logger.warning(f"User {user_id} not found in database for reseller discount check")
                return discount
                
            is_reseller = res['is_reseller']
            logger.info(f"User {user_id} reseller status: {is_reseller} (1=reseller, 0=not reseller)")
            
            if res and res['is_reseller'] == 1:
                # Check what discount records exist for this user
                c.execute("SELECT product_type, discount_percentage FROM reseller_discounts WHERE reseller_user_id = ?", (user_id,))
                all_discounts = c.fetchall()
                logger.info(f"User {user_id} has {len(all_discounts)} discount records: {[(d['product_type'], d['discount_percentage']) for d in all_discounts]}")
                
                c.execute("""
                    SELECT discount_percentage FROM reseller_discounts
                    WHERE reseller_user_id = ? AND product_type = ?
                """, (user_id, product_type))
                discount_res = c.fetchone()
                if discount_res:
                    discount = Decimal(str(discount_res['discount_percentage']))
                    logger.info(f"✅ Found reseller discount for user {user_id}, type '{product_type}': {discount}%")
                else:
                    logger.info(f"❌ No reseller discount found for user {user_id}, type '{product_type}' (user is reseller but no specific discount set)")
            else:
                logger.info(f"User {user_id} is not a reseller (is_reseller={is_reseller}), returning 0% discount")
            
            # Success - break out of retry loop
            break
            
        except sqlite3.Error as e:
            if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                logger.warning(f"Database locked for reseller discount check (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay}s...")
                if conn: 
                    conn.close()
                    conn = None
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
            else:
                logger.error(f"DB error fetching reseller discount for user {user_id}, type {product_type}: {e}")
                break
        except Exception as e:
            logger.error(f"Unexpected error fetching reseller discount: {e}", exc_info=True)
            break
        finally:
            if conn: 
                conn.close()
                conn = None
    
    return discount


# ==================================
# --- Admin: Manage Reseller Status --- (REVISED FLOW)
# ==================================

async def handle_manage_resellers_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Prompts admin to enter the User ID to manage reseller status."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)

    # Set state to expect a user ID message
    context.user_data['state'] = 'awaiting_reseller_manage_id'

    prompt_msg = ("👤 Manage Reseller Status\n\n"
                  "Please reply with the Telegram User ID of the person you want to manage as a reseller.")
    keyboard = [[InlineKeyboardButton("⬅️ Back to Admin Menu", callback_data="admin_menu")]]

    await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter User ID in chat.")


async def handle_reseller_manage_id_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the admin entering a User ID for reseller status management."""
    admin_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(admin_id): return
    if context.user_data.get("state") != 'awaiting_reseller_manage_id': return
    if not update.message or not update.message.text: return

    entered_id_text = update.message.text.strip()

    try:
        target_user_id = int(entered_id_text)
        if target_user_id == admin_id:
            await send_message_with_retry(context.bot, chat_id, "❌ You cannot manage your own reseller status.", parse_mode=None)
            # Keep state awaiting another ID
            return

    except ValueError:
        await send_message_with_retry(context.bot, chat_id, "❌ Invalid User ID. Please enter a number.", parse_mode=None)
        # Keep state awaiting another ID
        return

    # Clear state now that we have a potential ID
    context.user_data.pop('state', None)

    # Fetch user info
    conn = None
    user_info = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT user_id, username, is_reseller FROM users WHERE user_id = ?", (target_user_id,))
        user_info = c.fetchone()
    except sqlite3.Error as e:
        logger.error(f"DB error fetching user {target_user_id} for reseller check: {e}")
        await send_message_with_retry(context.bot, chat_id, "❌ Database error checking user.", parse_mode=None)
        # Go back to admin menu on error
        await send_message_with_retry(context.bot, chat_id, "Returning to menu...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin Menu", callback_data="admin_menu")]]), parse_mode=None)
        return
    finally:
        if conn: conn.close()

    if not user_info:
        await send_message_with_retry(context.bot, chat_id, f"❌ User ID {target_user_id} not found in the bot's database.", parse_mode=None)
        # Go back to admin menu
        await send_message_with_retry(context.bot, chat_id, "Returning to menu...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin Menu", callback_data="admin_menu")]]), parse_mode=None)
        return

    # Display user info and toggle buttons
    username = user_info['username'] or f"ID_{target_user_id}"
    is_reseller = user_info['is_reseller'] == 1
    current_status_text = "✅ IS currently a Reseller" if is_reseller else "❌ Is NOT currently a Reseller"

    msg = (f"👤 Manage Reseller: @{username} (ID: {target_user_id})\n\n"
           f"Current Status: {current_status_text}")

    keyboard = []
    if is_reseller:
        keyboard.append([InlineKeyboardButton("🚫 Disable Reseller Status", callback_data=f"reseller_toggle_status|{target_user_id}|0")]) # Offset 0 as placeholder
    else:
        keyboard.append([InlineKeyboardButton("✅ Enable Reseller Status", callback_data=f"reseller_toggle_status|{target_user_id}|0")]) # Offset 0 as placeholder

    keyboard.append([InlineKeyboardButton("⬅️ Manage Another User", callback_data="manage_resellers_menu")]) # Back to the prompt
    keyboard.append([InlineKeyboardButton("⬅️ Back to Admin Menu", callback_data="admin_menu")])

    await send_message_with_retry(context.bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def handle_reseller_toggle_status(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggles the is_reseller flag for a user (called from user display)."""
    query = update.callback_query
    admin_id = query.from_user.id
    chat_id = query.message.chat_id # Get chat_id for sending messages

    if not is_primary_admin(admin_id): return await query.answer("Access Denied.", show_alert=True)
    # Params now only need target user ID, offset is irrelevant here
    if not params or not params[0].isdigit():
        await query.answer("Error: Invalid data.", show_alert=True); return

    target_user_id = int(params[0])
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT username, is_reseller FROM users WHERE user_id = ?", (target_user_id,))
        user_data = c.fetchone()
        if not user_data:
            await query.answer("User not found.", show_alert=True)
            # Go back to the prompt to enter another ID
            return await handle_manage_resellers_menu(update, context)

        current_status = user_data['is_reseller']
        username = user_data['username'] or f"ID_{target_user_id}"
        new_status = 0 if current_status == 1 else 1
        c.execute("UPDATE users SET is_reseller = ? WHERE user_id = ?", (new_status, target_user_id))
        conn.commit()

        # Log action using constants from utils
        action_desc = ACTION_RESELLER_ENABLED if new_status == 1 else ACTION_RESELLER_DISABLED
        log_admin_action(admin_id, action_desc, target_user_id=target_user_id, old_value=current_status, new_value=new_status)

        status_text = "enabled" if new_status == 1 else "disabled"
        await query.answer(f"Reseller status {status_text} for user {target_user_id}.")

        # Refresh the user info display after toggling
        new_status_text = "✅ IS currently a Reseller" if new_status == 1 else "❌ Is NOT currently a Reseller"
        msg = (f"👤 Manage Reseller: @{username} (ID: {target_user_id})\n\n"
               f"Status Updated: {new_status_text}")

        keyboard = []
        if new_status == 1: # Now a reseller
            keyboard.append([InlineKeyboardButton("🚫 Disable Reseller Status", callback_data=f"reseller_toggle_status|{target_user_id}|0")])
        else: # Not a reseller
            keyboard.append([InlineKeyboardButton("✅ Enable Reseller Status", callback_data=f"reseller_toggle_status|{target_user_id}|0")])

        keyboard.append([InlineKeyboardButton("⬅️ Manage Another User", callback_data="manage_resellers_menu")])
        keyboard.append([InlineKeyboardButton("⬅️ Back to Admin Menu", callback_data="admin_menu")])

        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


    except sqlite3.Error as e:
        logger.error(f"DB error toggling reseller status {target_user_id}: {e}")
        await query.answer("DB Error.", show_alert=True)
    except Exception as e:
        logger.error(f"Error toggling reseller status {target_user_id}: {e}", exc_info=True)
        await query.answer("Error.", show_alert=True)
    finally:
        if conn: conn.close()


# ========================================
# --- Admin: Manage Reseller Discounts --- (Pagination kept)
# ========================================

async def handle_manage_reseller_discounts_select_reseller(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin selects which active reseller to manage discounts for (PAGINATED)."""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id): return await query.answer("Access Denied.", show_alert=True)
    offset = 0
    if params and len(params) > 0 and params[0].isdigit(): offset = int(params[0])

    resellers = []
    total_resellers = 0
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM users WHERE is_reseller = 1")
        count_res = c.fetchone(); total_resellers = count_res['count'] if count_res else 0
        c.execute("""
            SELECT user_id, username FROM users
            WHERE is_reseller = 1 ORDER BY user_id DESC LIMIT ? OFFSET ?
        """, (USERS_PER_PAGE_DISCOUNT_SELECT, offset)) # Use specific constant
        resellers = c.fetchall()
    except sqlite3.Error as e:
        logger.error(f"DB error fetching active resellers: {e}")
        await query.edit_message_text("❌ DB Error fetching resellers.", parse_mode=None)
        return
    finally:
        if conn: conn.close()

    msg = "👤 Manage Reseller Discounts\n\nSelect an active reseller to set their discounts:\n"
    keyboard = []
    item_buttons = []

    if not resellers and offset == 0: msg += "\nNo active resellers found."
    elif not resellers: msg += "\nNo more resellers."
    else:
        for r in resellers:
            username = r['username'] or f"ID_{r['user_id']}"
            item_buttons.append([InlineKeyboardButton(f"👤 @{username}", callback_data=f"reseller_manage_specific|{r['user_id']}")])
        keyboard.extend(item_buttons)
        # Pagination
        total_pages = math.ceil(max(0, total_resellers) / USERS_PER_PAGE_DISCOUNT_SELECT)
        current_page = (offset // USERS_PER_PAGE_DISCOUNT_SELECT) + 1
        nav_buttons = []
        if current_page > 1: nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"manage_reseller_discounts_select_reseller|{max(0, offset - USERS_PER_PAGE_DISCOUNT_SELECT)}"))
        if current_page < total_pages: nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"manage_reseller_discounts_select_reseller|{offset + USERS_PER_PAGE_DISCOUNT_SELECT}"))
        if nav_buttons: keyboard.append(nav_buttons)
        msg += f"\nPage {current_page}/{total_pages}"

    keyboard.append([InlineKeyboardButton("⬅️ Back to Admin Menu", callback_data="admin_menu")])
    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.error(f"Error editing reseller selection list: {e}")
            await query.answer("Error updating list.", show_alert=True)
        else: await query.answer()
    except Exception as e:
        logger.error(f"Error display reseller selection list: {e}", exc_info=True)
        await query.edit_message_text("❌ Error displaying list.", parse_mode=None)


# --- Manage Specific Reseller Discounts ---

async def handle_manage_specific_reseller_discounts(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Displays current discounts for a specific reseller and allows adding/editing."""
    query = update.callback_query
    admin_id = query.from_user.id
    if not is_primary_admin(admin_id): return await query.answer("Access Denied.", show_alert=True)
    if not params or not params[0].isdigit():
        await query.answer("Error: Invalid user ID.", show_alert=True); return

    target_reseller_id = int(params[0])
    discounts = []
    username = f"ID_{target_reseller_id}"
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT username FROM users WHERE user_id = ?", (target_reseller_id,))
        user_res = c.fetchone(); username = user_res['username'] if user_res and user_res['username'] else username
        c.execute("""
            SELECT product_type, discount_percentage FROM reseller_discounts
            WHERE reseller_user_id = ? ORDER BY product_type
        """, (target_reseller_id,))
        discounts = c.fetchall()
    except sqlite3.Error as e:
        logger.error(f"DB error fetching discounts for reseller {target_reseller_id}: {e}")
        await query.edit_message_text("❌ DB Error fetching discounts.", parse_mode=None)
        return
    finally:
        if conn: conn.close()

    msg = f"🏷️ Discounts for Reseller @{username} (ID: {target_reseller_id})\n\n"
    keyboard = []

    if not discounts: msg += "No specific discounts set yet."
    else:
        msg += "Current Discounts:\n"
        for discount in discounts:
            p_type = discount['product_type']
            emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
            percentage = Decimal(str(discount['discount_percentage']))
            msg += f" • {emoji} {p_type}: {percentage:.1f}%\n"
            keyboard.append([
                 InlineKeyboardButton(f"✏️ Edit {p_type} ({percentage:.1f}%)", callback_data=f"reseller_edit_discount|{target_reseller_id}|{p_type}"),
                 InlineKeyboardButton(f"🗑️ Delete", callback_data=f"reseller_delete_discount_confirm|{target_reseller_id}|{p_type}")
            ])

    keyboard.append([InlineKeyboardButton("➕ Add New Discount Rule", callback_data=f"reseller_add_discount_select_type|{target_reseller_id}")])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Reseller List", callback_data="manage_reseller_discounts_select_reseller|0")])

    try:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.error(f"Error editing specific reseller discounts: {e}")
            await query.answer("Error updating view.", show_alert=True)
        else: await query.answer()
    except Exception as e:
        logger.error(f"Error display specific reseller discounts: {e}", exc_info=True)
        await query.edit_message_text("❌ Error displaying discounts.", parse_mode=None)


# <<< FIXED >>>
async def handle_reseller_add_discount_select_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin selects product type for a new reseller discount rule."""
    query = update.callback_query
    admin_id = query.from_user.id
    if not is_primary_admin(admin_id): return await query.answer("Access Denied.", show_alert=True)
    if not params or not params[0].isdigit():
        await query.answer("Error: Invalid user ID.", show_alert=True); return

    target_reseller_id = int(params[0])
    # <<< STORE the target ID in context >>>
    context.user_data['reseller_mgmt_target_id'] = target_reseller_id

    load_all_data() # Ensure PRODUCT_TYPES is fresh

    if not PRODUCT_TYPES:
        await query.edit_message_text("❌ No product types configured. Please add types via 'Manage Product Types'.", parse_mode=None)
        return

    keyboard = []
    for type_name, emoji in sorted(PRODUCT_TYPES.items()):
        # <<< MODIFIED callback_data: Only command and type_name >>>
        callback_data_short = f"reseller_add_discount_enter_percent|{type_name}"
        # <<< ADDED length check >>>
        if len(callback_data_short.encode('utf-8')) > 64:
            logger.warning(f"Callback data for type '{type_name}' is too long ({len(callback_data_short.encode('utf-8'))} bytes) and will be skipped: {callback_data_short}")
            continue # Skip this button if the data is too long
        keyboard.append([InlineKeyboardButton(f"{emoji} {type_name}", callback_data=callback_data_short)])

    # Cancel button still needs the target_id to go back correctly
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data=f"reseller_manage_specific|{target_reseller_id}")])
    await query.edit_message_text("Select Product Type for new discount rule:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


# <<< FIXED >>>
async def handle_reseller_add_discount_enter_percent(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin needs to enter the percentage for the new rule."""
    query = update.callback_query
    admin_id = query.from_user.id
    if not is_primary_admin(admin_id): return await query.answer("Access Denied.", show_alert=True)

    # <<< RETRIEVE target ID from context >>>
    target_reseller_id = context.user_data.get('reseller_mgmt_target_id')

    # <<< Params now only contain the product_type >>>
    if not params or len(params) < 1 or target_reseller_id is None:
        logger.error("handle_reseller_add_discount_enter_percent missing context or params.")
        await query.answer("Error: Missing data.", show_alert=True); return

    product_type = params[0] # Get type from params
    emoji = PRODUCT_TYPES.get(product_type, DEFAULT_PRODUCT_EMOJI)

    context.user_data['state'] = 'awaiting_reseller_discount_percent'
    # reseller_mgmt_target_id is already in context
    context.user_data['reseller_mgmt_product_type'] = product_type
    context.user_data['reseller_mgmt_mode'] = 'add'

    # Cancel button still needs the target_id
    cancel_callback = f"reseller_manage_specific|{target_reseller_id}"

    await query.edit_message_text(
        f"Enter discount percentage for {emoji} {product_type} (e.g., 10 or 15.5):",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=cancel_callback)]]),
        parse_mode=None
    )
    await query.answer("Enter percentage in chat.")


async def handle_reseller_edit_discount(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Admin wants to edit an existing discount percentage."""
    query = update.callback_query
    admin_id = query.from_user.id
    if not is_primary_admin(admin_id): return await query.answer("Access Denied.", show_alert=True)
    if not params or len(params) < 2 or not params[0].isdigit():
        await query.answer("Error: Invalid data.", show_alert=True); return

    target_reseller_id = int(params[0])
    product_type = params[1]
    emoji = PRODUCT_TYPES.get(product_type, DEFAULT_PRODUCT_EMOJI)

    context.user_data['state'] = 'awaiting_reseller_discount_percent'
    context.user_data['reseller_mgmt_target_id'] = target_reseller_id
    context.user_data['reseller_mgmt_product_type'] = product_type
    context.user_data['reseller_mgmt_mode'] = 'edit'

    # Cancel button still needs the target_id
    cancel_callback = f"reseller_manage_specific|{target_reseller_id}"

    await query.edit_message_text(
        f"Enter *new* discount percentage for {emoji} {product_type} (e.g., 10 or 15.5):",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=cancel_callback)]]),
        parse_mode=None
    )
    await query.answer("Enter new percentage in chat.")


async def handle_reseller_percent_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the admin entering the discount percentage via message."""
    admin_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_primary_admin(admin_id): return
    if context.user_data.get("state") != 'awaiting_reseller_discount_percent': return
    if not update.message or not update.message.text: return

    percent_text = update.message.text.strip()
    target_user_id = context.user_data.get('reseller_mgmt_target_id')
    product_type = context.user_data.get('reseller_mgmt_product_type')
    mode = context.user_data.get('reseller_mgmt_mode', 'add')

    if target_user_id is None or not product_type:
        logger.error("State awaiting_reseller_discount_percent missing context data.")
        await send_message_with_retry(context.bot, chat_id, "❌ Error: Context lost. Please start again.", parse_mode=None)
        context.user_data.pop('state', None)
        # Clean up other related context data as well
        context.user_data.pop('reseller_mgmt_target_id', None)
        context.user_data.pop('reseller_mgmt_product_type', None)
        context.user_data.pop('reseller_mgmt_mode', None)
        fallback_cb = "manage_reseller_discounts_select_reseller|0"
        await send_message_with_retry(context.bot, chat_id, "Returning...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=fallback_cb)]]), parse_mode=None)
        return

    back_callback = f"reseller_manage_specific|{target_user_id}"

    try:
        percentage = Decimal(percent_text)
        if not (Decimal('0.0') <= percentage <= Decimal('100.0')):
            raise ValueError("Percentage must be between 0 and 100.")

        conn = None
        old_value = None # For logging edits
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("BEGIN")

            if mode == 'edit':
                c.execute("SELECT discount_percentage FROM reseller_discounts WHERE reseller_user_id = ? AND product_type = ?", (target_user_id, product_type))
                old_res = c.fetchone()
                old_value = old_res['discount_percentage'] if old_res else None

            # Use INSERT OR REPLACE for both add and edit to simplify logic
            # If it's an 'edit' but the row doesn't exist, it becomes an 'add'
            sql = "INSERT OR REPLACE INTO reseller_discounts (reseller_user_id, product_type, discount_percentage) VALUES (?, ?, ?)"
            # Use quantize before converting to float for DB storage if needed, or store as TEXT
            # Storing as REAL (float) is generally fine for percentages if precision issues are acceptable,
            # but TEXT is safer if exact Decimal values are critical. Let's stick with REAL for now.
            params_sql = (target_user_id, product_type, float(percentage.quantize(Decimal("0.1")))) # Store with one decimal place

            # Determine action description based on whether old value existed
            action_desc = ACTION_RESELLER_DISCOUNT_ADD if old_value is None else ACTION_RESELLER_DISCOUNT_EDIT

            result = c.execute(sql, params_sql)
            conn.commit()

            # Log the action
            log_admin_action(
                admin_id=admin_id, action=action_desc, target_user_id=target_user_id,
                reason=f"Type: {product_type}", old_value=old_value, new_value=params_sql[2] # Log the value stored
            )

            action_verb = "set" if old_value is None else "updated"
            await send_message_with_retry(context.bot, chat_id, f"✅ Discount rule {action_verb} for {product_type}: {percentage:.1f}%",
                                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=back_callback)]]), parse_mode=None)

            # Clean up context after successful operation
            context.user_data.pop('state', None); context.user_data.pop('reseller_mgmt_target_id', None)
            context.user_data.pop('reseller_mgmt_product_type', None); context.user_data.pop('reseller_mgmt_mode', None)

        except sqlite3.Error as e: # Catch potential DB errors like IntegrityError implicitly
            logger.error(f"DB error {mode} reseller discount: {e}", exc_info=True)
            if conn and conn.in_transaction: conn.rollback()
            await send_message_with_retry(context.bot, chat_id, "❌ DB Error saving discount rule.", parse_mode=None)
            context.user_data.pop('state', None) # Clear state on error
            # Clean up other related context data on error
            context.user_data.pop('reseller_mgmt_target_id', None)
            context.user_data.pop('reseller_mgmt_product_type', None)
            context.user_data.pop('reseller_mgmt_mode', None)
        finally:
            if conn: conn.close()

    except ValueError:
        await send_message_with_retry(context.bot, chat_id, "❌ Invalid percentage. Enter a number between 0 and 100 (e.g., 10 or 15.5).", parse_mode=None)
        # Keep state awaiting percentage


async def handle_reseller_delete_discount_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Delete Discount' button press, shows confirmation."""
    query = update.callback_query
    admin_id = query.from_user.id
    if not is_primary_admin(admin_id): return await query.answer("Access Denied.", show_alert=True)
    if not params or len(params) < 2 or not params[0].isdigit():
        await query.answer("Error: Invalid data.", show_alert=True); return

    target_reseller_id = int(params[0])
    product_type = params[1]
    emoji = PRODUCT_TYPES.get(product_type, DEFAULT_PRODUCT_EMOJI)

    # Set confirm action for handle_confirm_yes
    context.user_data["confirm_action"] = f"confirm_delete_reseller_discount|{target_reseller_id}|{product_type}"

    msg = (f"⚠️ Confirm Deletion\n\n"
           f"Delete the discount rule for {emoji} {product_type} for user ID {target_reseller_id}?\n\n"
           f"🚨 This action is irreversible!")
    keyboard = [[InlineKeyboardButton("✅ Yes, Delete Rule", callback_data="confirm_yes"),
                 InlineKeyboardButton("❌ No, Cancel", callback_data=f"reseller_manage_specific|{target_reseller_id}")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


# --- END OF FILE reseller_management.py ---
