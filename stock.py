# --- START OF FILE stock.py ---

import sqlite3
import logging
from collections import defaultdict
# --- Telegram Imports ---
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode # Keep for reference if needed elsewhere
from telegram.ext import ContextTypes # Use ContextTypes
from telegram import helpers # Keep for potential other uses, but not escaping
import telegram.error as telegram_error # Use telegram.error
# -------------------------

# Import necessary items from utils
from utils import (
    ADMIN_ID, format_currency, send_message_with_retry, SECONDARY_ADMIN_IDS,
    get_db_connection, # Import DB helper
    is_primary_admin, is_secondary_admin, is_any_admin # Admin helper functions
)

# Setup logger for this file
logger = logging.getLogger(__name__)

# Note: The 'params' argument isn't used in this handler but kept for consistency
async def handle_view_stock(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Displays a formatted list of all available products in stock."""
    query = update.callback_query
    user_id = query.from_user.id

    # --- Authorization Check ---
    primary_admin = is_primary_admin(user_id)
    secondary_admin = is_secondary_admin(user_id)

    if not primary_admin and not secondary_admin:
        await query.answer("Access Denied.", show_alert=True)
        return
    # --- END Check ---

    # Structure: {city: {district: {product_type: [(size, price, avail, res), ...]}}}
    stock_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    conn = None

    try:
        conn = get_db_connection() # Use helper
        # row_factory is set in helper
        c = conn.cursor()
        # Fetch all products that have *any* stock (available OR reserved)
        # Use column names
        # >>> MODIFIED QUERY HERE <<<
        c.execute("""
            SELECT city, district, product_type, size, price, available, reserved
            FROM products WHERE available > 0 OR reserved > 0
            ORDER BY city, district, product_type, price, size
        """)
        # >>> END MODIFICATION <<<
        products = c.fetchall()

        if not products:
            msg = "📦 Bot Stock\n\nNo products currently in stock (neither available nor reserved)." # Clarified message
            back_callback = "admin_menu" if primary_admin else "viewer_admin_menu"
            keyboard = [[InlineKeyboardButton("⬅️ Back to Admin Menu", callback_data=back_callback)]]
        else:
            msg = "📦 Current Bot Stock\n\n"
            # Group products by location and type
            for p in products:
                # Access by column name
                stock_data[p['city']][p['district']][p['product_type']].append(
                    (p['size'], p['price'], p['available'], p['reserved'])
                )

            # Format the message (Plain Text)
            for city, districts in sorted(stock_data.items()):
                msg += f"🏙️ {city}\n"
                for district, types in sorted(districts.items()):
                    msg += f"  🏘️ {district}\n"
                    for p_type, items in sorted(types.items()):
                        msg += f"    💎 {p_type}\n"
                        items.sort(key=lambda x: x[1]) # Sort by price (index 1)
                        for size, price, avail, res in items:
                            price_str = format_currency(price)
                            # Ensure display reflects reality (Avail cannot be less than Reserved after reservation)
                            # Although the reservation logic should prevent Avail < Reserved, this adds safety.
                            # It's generally better to rely on the actual DB values.
                            msg += f"      - {size} ({price_str} €) | Av: {avail} / Res: {res}\n"
                    msg += "\n" # Add a newline between product types
                msg += "\n" # Add a newline between districts

            if len(msg) > 4000:
                msg = msg[:4000] + "\n\n✂️ ... Message truncated due to length limit."
                logger.warning("Stock list message truncated due to length.")

            back_callback = "admin_menu" if primary_admin else "viewer_admin_menu"
            keyboard = [[InlineKeyboardButton("⬅️ Back to Admin Menu", callback_data=back_callback)]]

        # Try sending/editing the message
        try:
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except telegram_error.BadRequest as e:
            if "message is not modified" in str(e).lower(): await query.answer()
            else:
                logger.error(f"Error editing stock list message: {e}.")
                fallback_msg = "❌ Error displaying stock list."
                try: await query.edit_message_text(fallback_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
                except Exception: await query.answer("Error displaying stock list.", show_alert=True)

    except sqlite3.Error as e:
        logger.error(f"DB error fetching stock list: {e}", exc_info=True)
        await query.edit_message_text("❌ Error fetching stock data from database.", parse_mode=None)
    except Exception as e:
         logger.error(f"Unexpected error in handle_view_stock: {e}", exc_info=True)
         await query.edit_message_text("❌ An unexpected error occurred while generating the stock list.", parse_mode=None)
    finally:
        if conn: conn.close() # Close connection if opened

# --- END OF FILE stock.py ---
