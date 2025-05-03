# app.py (v4.0 - Refactored & Enhanced)
import streamlit as st
import pandas as pd
import camelot
from deep_translator import GoogleTranslator
from langdetect import detect, DetectorFactory, LangDetectException
from io import BytesIO
from openpyxl.styles import Alignment
import json
import os
import re
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List, Union

# --- Required for DB Interaction ---
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from passlib.context import CryptContext
# ----------------------------------

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# === Configuration ===
class AppConfig:
    APP_VERSION = "4.0-Refactored"
    APP_TITLE = "PDF Table Extractor Pro"
    SUPPORT_EMAIL = "lovinquesaba17@gmail.com"
    BASE_DIR = Path(__file__).resolve().parent
    # --- Trial File Config (JSON) ---
    TRIAL_FILE_PATH = BASE_DIR / "trial_users.json"
    TRIAL_DAILY_LIMIT = 20
    TRIAL_EMAIL = "freetrial@example.com" # Use a more realistic placeholder if needed
    # --- ---
    LOGO_PATH = BASE_DIR / "logo.png"
    MAX_SHEET_NAME_LEN = 31
    # --- Languages ---
    SUPPORTED_LANGUAGES = {
        'en': 'English', 'es': 'Spanish', 'fr': 'French', 'de': 'German', 'it': 'Italian',
        'pt': 'Portuguese', 'ja': 'Japanese', 'ko': 'Korean', 'zh-cn': 'Chinese (Simplified)',
        'ar': 'Arabic', 'ru': 'Russian', 'hi': 'Hindi', 'bn': 'Bengali', 'nl': 'Dutch',
        'sv': 'Swedish', 'fi': 'Finnish', 'da': 'Danish', 'no': 'Norwegian', 'pl': 'Polish',
        'tr': 'Turkish', 'el': 'Greek', 'he': 'Hebrew', 'th': 'Thai', 'vi': 'Vietnamese',
        # Add more as needed
    }
    DEFAULT_TRANSLATE_LANG_NAME = "English"
    # --- Database & Security ---
    DATABASE_URL = os.environ.get('DATABASE_URL')
    FLASK_SECRET_KEY = os.environ.get('FLASK_SECRET_KEY', 'default-secret-key-change-me')

CONFIG = AppConfig()

# --- Seed langdetect ---
try:
    DetectorFactory.seed = 0
except NameError:
    log.warning("Could not seed DetectorFactory (NameError).")
except Exception as seed_err:
    log.warning(f"Error seeding langdetect: {seed_err}")

# === Database Setup ===
db_config_app = Flask(__name__)

if not CONFIG.DATABASE_URL:
    log.critical("FATAL ERROR: DATABASE_URL environment variable is not set.")
    st.error("FATAL ERROR: Database configuration is missing. App cannot start.")
    st.stop()
if CONFIG.FLASK_SECRET_KEY == 'default-secret-key-change-me':
    log.warning("SECURITY WARNING: Using default FLASK_SECRET_KEY. Change this in production!")

db_config_app.config['SQLALCHEMY_DATABASE_URI'] = CONFIG.DATABASE_URL
db_config_app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {"pool_pre_ping": True, "pool_recycle": 300}
db_config_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db_config_app.config['SECRET_KEY'] = CONFIG.FLASK_SECRET_KEY

db: Optional[SQLAlchemy] = None
try:
    db = SQLAlchemy(db_config_app)
    log.info("SQLAlchemy initialized successfully.")
except Exception as db_init_err:
    log.critical(f"FATAL ERROR: SQLAlchemy initialization failed: {db_init_err}", exc_info=True)
    st.error("FATAL ERROR: Database initialization failed. App cannot start.")
    st.stop()

# === Password Hashing Context ===
try:
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    log.info("Passlib context initialized.")
except Exception as pwd_err:
    log.critical(f"FATAL ERROR: Security component (Passlib) failed: {pwd_err}", exc_info=True)
    st.error("FATAL ERROR: Security component initialization failed. App cannot start.")
    st.stop()

# === Database Model ===
if db:
    class User(db.Model):
        __tablename__ = 'users' # Explicit table name is good practice
        id = db.Column(db.Integer, primary_key=True)
        email = db.Column(db.String(120), unique=True, nullable=False, index=True)
        password_hash = db.Column(db.String(128), nullable=False)
        credits = db.Column(db.Integer, nullable=False, default=100)
        created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
        last_login_at = db.Column(db.DateTime(timezone=True), nullable=True)

        def set_password(self, password: str):
            """Hashes the password and stores it."""
            self.password_hash = pwd_context.hash(password)

        def check_password(self, password: str) -> bool:
            """Verifies a given password against the stored hash."""
            try:
                # Ensure hash is a string before verifying
                if not isinstance(self.password_hash, str):
                    log.warning(f"Password hash is not a string for user {self.email}. Type: {type(self.password_hash)}")
                    return False
                return pwd_context.verify(password, self.password_hash)
            except Exception as e:
                log.error(f"Password check error for {self.email}: {e}", exc_info=True)
                return False

        def __repr__(self):
            return f'<User {self.email} (Credits: {self.credits})>'

    # Optional: Create tables if they don't exist (useful for initial setup)
    # Be cautious using this in production without proper migration tools (like Alembic)
    # with db_config_app.app_context():
    #    try:
    #        db.create_all()
    #        log.info("Database tables checked/created (if necessary).")
    #    except OperationalError as op_err:
    #        log.error(f"Database operation error during table creation check: {op_err}")
    #    except Exception as e:
    #        log.error(f"Unexpected error during table creation check: {e}", exc_info=True)

else:
    # This case should theoretically not be reached due to st.stop() earlier
    log.critical("Database object 'db' is None. Skipping User model definition.")
    st.error("Internal Error: Database connection not available for model definition.")
    st.stop()

# === JSON File Utilities (Only for Trial Data) ===
def load_json_data(filepath: Path) -> Dict[str, Any]:
    """Loads trial data from a JSON file."""
    try:
        if not filepath.exists():
            log.warning(f"Trial file {filepath} not found, creating an empty one.")
            filepath.write_text("{}", encoding='utf-8')
            return {}
        with filepath.open("r", encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            log.error(f"Trial file {filepath} content is not a valid JSON object (dictionary). Returning empty.")
            return {}
        return data
    except json.JSONDecodeError as json_err:
        log.error(f"Error decoding JSON from {filepath}: {json_err}. Returning empty.")
        return {}
    except IOError as io_err:
        log.error(f"I/O error reading trial file {filepath}: {io_err}. Returning empty.")
        return {}
    except Exception as e:
        log.error(f"Unexpected error loading trial JSON {filepath}: {e}", exc_info=True)
        return {}

def save_json_data(filepath: Path, data: Dict[str, Any]) -> bool:
    """Saves trial data to a JSON file."""
    try:
        with filepath.open("w", encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except IOError as io_err:
        log.error(f"I/O error writing trial file {filepath}: {io_err}.")
        st.warning("Could not save trial usage data due to a file system error.")
        return False
    except TypeError as type_err:
         log.error(f"Type error saving JSON to {filepath} (data might not be serializable): {type_err}")
         st.warning("Could not save trial usage data due to an internal data error.")
         return False
    except Exception as e:
        log.error(f"Unexpected error saving trial JSON {filepath}: {e}", exc_info=True)
        st.warning("An unexpected error occurred while saving trial usage data.")
        return False

# === Database Access Helper Functions ===
def get_user_by_email(email: str) -> Optional[User]:
    """Fetches a user from the database by email (case-insensitive)."""
    if not db: return None # Should not happen if init succeeded
    try:
        with db_config_app.app_context():
            # Using func.lower for case-insensitive comparison at the DB level
            return db.session.query(User).filter(db.func.lower(User.email) == email.lower()).first()
    except OperationalError as db_op_err:
        log.error(f"DB OperationalError fetching user {email}: {db_op_err}", exc_info=True)
        st.error("Database connection error while retrieving user data.")
        return None
    except SQLAlchemyError as db_err:
        log.error(f"SQLAlchemyError fetching user {email}: {db_err}", exc_info=True)
        st.error("Database error while retrieving user data.")
        return None
    except Exception as e:
        log.error(f"Unexpected error fetching user {email}: {e}", exc_info=True)
        st.error("An unexpected server error occurred while retrieving user data.")
        return None

def update_user_last_login(user: User) -> bool:
    """Updates the last_login_at timestamp for a given user."""
    if not db: return False
    try:
        with db_config_app.app_context():
            user.last_login_at = datetime.now(timezone.utc)
            db.session.commit()
            log.info(f"Updated last_login_at for user {user.email}")
            return True
    except OperationalError as db_op_err:
        db.session.rollback()
        log.warning(f"DB OperationalError updating last_login for {user.email}: {db_op_err}")
        # Don't show error to user for this non-critical update
        return False
    except SQLAlchemyError as db_err:
        db.session.rollback()
        log.warning(f"SQLAlchemyError updating last_login for {user.email}: {db_err}")
        return False
    except Exception as e:
        db.session.rollback()
        log.warning(f"Unexpected error updating last_login for {user.email}: {e}", exc_info=True)
        return False

def update_user_credits(user: User, change: int) -> bool:
    """Updates the credit count for a user. Use negative change to deduct."""
    if not db: return False
    try:
        with db_config_app.app_context():
            # Ensure credits don't go below zero if deducting
            if change < 0 and user.credits < abs(change):
                 log.warning(f"Attempted to deduct {abs(change)} credits from {user.email}, but only {user.credits} available. Setting to 0.")
                 user.credits = 0
            else:
                user.credits += change
            db.session.commit()
            st.session_state.credits = user.credits # Update session state immediately
            log.info(f"Updated credits for {user.email}. Change: {change}, New Balance: {user.credits}")
            return True
    except OperationalError as db_op_err:
        db.session.rollback()
        log.error(f"DB OperationalError updating credits for {user.email}: {db_op_err}", exc_info=True)
        st.error("⚠️ Database connection error while updating credits.")
        return False
    except SQLAlchemyError as db_err:
        db.session.rollback()
        log.error(f"SQLAlchemyError updating credits for {user.email}: {db_err}", exc_info=True)
        st.error("⚠️ Database error while updating credits.")
        return False
    except Exception as e:
        db.session.rollback()
        log.error(f"Unexpected error updating credits for {user.email}: {e}", exc_info=True)
        st.error("⚠️ An unexpected server error occurred while updating credits.")
        return False

# === Authentication Functions ===
def authenticate_user(email: str, password: str) -> Optional[Dict[str, Any]]:
    """Verifies user credentials against the database."""
    if not email or not password:
        log.warning("Authentication attempt with empty email or password.")
        return None

    log.info(f"Attempting authentication for: {email}")
    user = get_user_by_email(email)

    if user and user.check_password(password):
        log.info(f"User '{email}' authenticated successfully.")
        update_user_last_login(user) # Attempt to update last login, non-critical
        return {"email": user.email, "credits": user.credits}
    elif user:
        log.warning(f"Authentication failed for '{email}': Invalid password.")
        return None
    else:
        # get_user_by_email already logs DB errors if they occur
        log.warning(f"Authentication failed for '{email}': User not found.")
        return None

# === Session State Management ===
def initialize_session(user_data: Dict[str, Any]) -> bool:
    """Initializes Streamlit session state after successful login."""
    email = user_data.get("email")
    if not email:
        log.error("Session initialization failed: Email missing in user_data.")
        st.error("Login failed due to an internal error (missing email).")
        return False

    try:
        st.session_state.logged_in = True
        st.session_state.user_email = email
        is_trial = (email.lower() == CONFIG.TRIAL_EMAIL.lower())
        st.session_state.is_trial_user = is_trial

        if is_trial:
            log.info(f"Initializing session for TRIAL user: {email}")
            trial_data = load_json_data(CONFIG.TRIAL_FILE_PATH)
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            user_trial_info = trial_data.get(email, {"date": today_str, "uses": 0})

            # Reset daily uses if the date has changed
            if user_trial_info.get("date") != today_str:
                log.info(f"Resetting trial uses for {email} for new day {today_str}.")
                user_trial_info = {"date": today_str, "uses": 0}
                trial_data[email] = user_trial_info
                save_json_data(CONFIG.TRIAL_FILE_PATH, trial_data) # Save the reset

            st.session_state.trial_uses_today = user_trial_info.get("uses", 0)

            # Check limit *during* initialization
            if st.session_state.trial_uses_today >= CONFIG.TRIAL_DAILY_LIMIT:
                log.warning(f"Trial login blocked for {email}, daily limit ({CONFIG.TRIAL_DAILY_LIMIT}) reached.")
                st.error(f"Login failed: Your daily trial limit of {CONFIG.TRIAL_DAILY_LIMIT} uses has been reached.")
                st.session_state.logged_in = False # Prevent login
                return False
            st.session_state.credits = float('inf') # Trial users have "infinite" credits conceptually
        else:
            log.info(f"Initializing session for PREMIUM user: {email}")
            st.session_state.credits = user_data.get("credits", 0)
            st.session_state.trial_uses_today = 0 # Not applicable

        log.info(f"Session initialized for {email}. Trial: {is_trial}, Credits/Uses: {st.session_state.credits if not is_trial else st.session_state.trial_uses_today}/{CONFIG.TRIAL_DAILY_LIMIT if is_trial else 'N/A'}")
        return True

    except Exception as e:
        log.error(f"Unexpected error during session initialization for {email}: {e}", exc_info=True)
        st.error("An unexpected error occurred during session setup.")
        st.session_state.logged_in = False # Ensure failed init doesn't leave user logged in
        return False

def clear_session_state():
    """Clears relevant keys from session state upon logout."""
    keys_to_clear = ["logged_in", "user_email", "is_trial_user", "credits", "trial_uses_today"]
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]
    st.session_state.logged_in = False # Explicitly set logged_in to False

# === Usage Update Logic ===
def update_usage() -> bool:
    """
    Updates usage counts based on user type (trial or paid).
    Deducts credits for paid users, increments uses for trial users.
    Returns True if usage was updated successfully, False otherwise.
    """
    user_email = st.session_state.get("user_email")
    is_trial = st.session_state.get("is_trial_user", False)

    if not user_email:
        log.error("Cannot update usage: user_email missing from session state.")
        st.error("Error recording usage: User session data is missing.")
        return False

    log.info(f"Updating usage for user: {user_email} (Trial: {is_trial})")

    if is_trial:
        try:
            current_uses = st.session_state.get("trial_uses_today", 0) + 1
            st.session_state.trial_uses_today = current_uses # Update session state first

            trial_data = load_json_data(CONFIG.TRIAL_FILE_PATH)
            user_trial_info = trial_data.get(user_email, {})
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            user_trial_info.update({"uses": current_uses, "date": today_str})
            trial_data[user_email] = user_trial_info

            if save_json_data(CONFIG.TRIAL_FILE_PATH, trial_data):
                log.info(f"[Trial] Usage updated for {user_email}. Uses today: {current_uses}/{CONFIG.TRIAL_DAILY_LIMIT}")
                st.toast(f"Trial use recorded ({current_uses}/{CONFIG.TRIAL_DAILY_LIMIT} today).", icon="⏳")
                return True
            else:
                # save_json_data logs and shows st.warning
                log.error(f"Failed to save updated trial data for {user_email}.")
                # Revert session state if save failed? Maybe not necessary, prevents double counting if they retry.
                # st.session_state.trial_uses_today = current_uses - 1 # Optional revert
                return False
        except Exception as e:
            log.error(f"Unexpected error updating trial usage for {user_email}: {e}", exc_info=True)
            st.error("⚠️ An unexpected error occurred while updating trial usage count.")
            return False
    else:
        # Premium user credit deduction
        user = get_user_by_email(user_email)
        if not user:
            log.error(f"Cannot update credits for {user_email}: User not found in DB during usage update.")
            st.error("Error: Could not find your user record to update credits.")
            return False

        if user.credits <= 0:
            log.warning(f"Credit deduction skipped for {user_email}, already at 0 credits.")
            st.warning("Credit deduction skipped: You are already at 0 credits.")
            return True # No change needed, but not an error preventing processing

        if update_user_credits(user, -1):
             st.toast("1 credit deducted.", icon="🪙")
             return True
        else:
             # update_user_credits handles logging and st.error display
             log.error(f"Failed to deduct credit for premium user {user_email} via update_user_credits.")
             return False


# === UI Components ===

def display_login_form() -> bool:
    """Displays the login form and handles authentication."""
    if st.session_state.get("logged_in", False):
        return True # Already logged in

    # Display Logo
    try:
        if CONFIG.LOGO_PATH.exists():
            st.image(str(CONFIG.LOGO_PATH), width=150)
        else:
            log.info(f"Logo file not found at {CONFIG.LOGO_PATH}")
    except Exception as img_err:
        log.warning(f"Could not load or display logo: {img_err}")

    st.title(f"{CONFIG.APP_TITLE} - Login")
    st.markdown("Please log in to access the application.")

    # Center the form using columns
    _, form_col, _ = st.columns([1, 1.5, 1])

    with form_col:
        with st.form("login_form"):
            st.subheader("🔐 Secure Login")
            email = st.text_input("Email Address", key="login_email").strip()
            password = st.text_input("Password", type="password", key="login_password")
            submitted = st.form_submit_button("Sign In", use_container_width=True)

            if submitted:
                if not email or not password:
                    st.warning("Please enter both email and password.")
                    return False

                user_data = authenticate_user(email, password)

                if user_data:
                    if initialize_session(user_data):
                        st.toast(f"Welcome back, {email}!", icon="🎉")
                        # Use st.rerun() to immediately reflect the logged-in state
                        st.rerun()
                    else:
                        # Initialization failed, error message shown by initialize_session
                        # Make sure logged_in is false if init fails
                        st.session_state.logged_in = False
                        return False
                else:
                    # Authentication failed, error message shown by authenticate_user OR generic one here
                    if "last_auth_error_time" not in st.session_state or \
                       (datetime.now() - st.session_state.last_auth_error_time).total_seconds() > 2:
                       st.error("❌ Invalid email or password.")
                       st.session_state.last_auth_error_time = datetime.now() # Prevent spamming error
                    log.warning(f"Failed login attempt for email: {email}")
                    return False
    return False # Not logged in yet

def display_sidebar():
    """Displays the sidebar with user info, credits/usage, and logout."""
    with st.sidebar:
        st.title("⚙️ Account & Info")
        st.divider()

        user_email = st.session_state.get("user_email", "N/A")
        is_trial = st.session_state.get("is_trial_user", False)

        st.write(f"👤 **User:** `{user_email}`")

        if is_trial:
            st.info("🧪 Free Trial Account", icon="🧪")
            uses_today = st.session_state.get("trial_uses_today", 0)
            limit = CONFIG.TRIAL_DAILY_LIMIT
            st.metric(label="Uses Today", value=f"{uses_today} / {limit}")
            progress_val = min(uses_today / limit, 1.0) if limit > 0 else 0.0
            st.progress(progress_val)
            if uses_today >= limit:
                st.error("❌ Daily trial limit reached.")
        else:
            st.success("✅ Premium Account", icon="💳")
            credits = st.session_state.get('credits', 0)
            st.metric("Remaining Credits", f"{credits}")
            if credits <= 0:
                st.error("❌ No credits remaining.")
            # Add link to purchase credits if applicable
            # st.link_button("Buy More Credits", "YOUR_PURCHASE_LINK_HERE", use_container_width=True)

        st.divider()
        st.markdown("### 🔐 Session")
        if st.button("Log Out", use_container_width=True, key="logout_button"):
            log.info(f"User logged out: {user_email}")
            clear_session_state()
            st.toast("You have been logged out.", icon="👋")
            st.rerun() # Rerun to immediately show the login page

        st.divider()
        st.caption(f"App Version: {CONFIG.APP_VERSION}")
        st.caption(f"Support: {CONFIG.SUPPORT_EMAIL}")

# === PDF Processing & Translation Helpers ===

# Cache translation results to avoid repeated API calls for the same text
# Consider potential cache size limits if memory is a concern.
@st.cache_data(show_spinner=False, ttl=3600) # Cache for 1 hour
def translate_text_cached(text: str, target_lang: str) -> str:
    """
    Translates a single piece of text using GoogleTranslator.
    Handles empty text, detection errors, and translation errors gracefully.
    Uses st.cache_data for efficiency.
    """
    original_text = str(text).strip()
    if not original_text:
        return original_text # Return empty/whitespace strings as is

    # Avoid translating if already in the target language (basic check)
    try:
        detected_lang = detect(original_text)
        if detected_lang == target_lang:
            return original_text
    except LangDetectException:
        log.debug(f"Language detection failed for text: '{original_text[:50]}...' Proceeding with translation.")
        pass # Ignore detection errors and proceed
    except Exception as detect_err:
        log.warning(f"Unexpected language detection error: {detect_err}", exc_info=False)
        pass # Proceed with translation attempt

    # Perform translation
    try:
        # Note: Caching happens *before* this function is called by Streamlit's decorator
        translated = GoogleTranslator(source='auto', target=target_lang).translate(original_text)
        # Return original text if translation result is empty or None
        return translated if translated else original_text
    except Exception as e:
        log.warning(f"Translation failed for text '{original_text[:50]}...' to lang '{target_lang}'. Error: {e}", exc_info=False)
        return original_text # Return original text on error

def translate_dataframe(df: pd.DataFrame, target_lang: str) -> pd.DataFrame:
    """Applies cached translation to all string cells in a DataFrame."""
    log.info(f"Translating DataFrame to '{target_lang}'...")
    start_time = datetime.now()
    # Use applymap, but only on object columns (likely strings) for efficiency
    translated_df = df.copy()
    for col in df.select_dtypes(include=['object']).columns:
         translated_df[col] = df[col].apply(lambda x: translate_text_cached(x, target_lang) if pd.notna(x) else x)

    duration = (datetime.now() - start_time).total_seconds()
    log.info(f"DataFrame translation completed in {duration:.2f} seconds.")
    return translated_df

def split_merged_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Splits DataFrame rows where cells contain newline characters,
    expanding them into multiple rows.
    """
    new_rows: List[List[Any]] = []
    df = df.fillna('') # Replace NaN with empty string for consistent splitting
    original_columns = df.columns

    for _, row in df.iterrows():
        row_list = row.tolist()
        # Check if any cell in the row contains a newline
        if any('\n' in str(cell) for cell in row_list):
            # Split each cell by newline, handling potential non-string types
            parts: List[List[str]] = [str(cell).split('\n') for cell in row_list]
            try:
                # Find the maximum number of lines in any cell of this row
                max_len = max(len(p) for p in parts) if parts else 0
            except ValueError:
                 log.warning(f"ValueError encountered during max_len calculation for row: {row_list}. Setting max_len to 0.")
                 max_len = 0 # Handle potential issues if parts is empty unexpectedly

            # Create new rows based on the split parts
            for i in range(max_len):
                new_row = [p[i] if i < len(p) else '' for p in parts]
                new_rows.append(new_row)
        else:
            # No newlines in this row, append it as is
            new_rows.append(row_list)

    # Return a new DataFrame with the original columns
    if not new_rows: # Handle case where input df was empty or resulted in no rows
        return pd.DataFrame(columns=original_columns)
    return pd.DataFrame(new_rows, columns=original_columns)

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Applies cleaning steps like stripping headers and splitting rows."""
    if df.empty:
        return df
    # Clean column headers (strip whitespace)
    df.columns = [str(col).strip() for col in df.columns]
    # Split rows with newlines
    df = split_merged_rows(df)
    # Ensure all data is string type for consistency before potential translation
    df = df.astype(str)
    return df

def generate_unique_sheet_name(base_name: str, existing_names: List[str]) -> str:
    """Generates a unique Excel sheet name, handling length limits and duplicates."""
    # Truncate base name if too long
    candidate = base_name[:CONFIG.MAX_SHEET_NAME_LEN]
    final_name = candidate
    count = 1
    # Lowercase comparison for case-insensitivity
    existing_lower = {name.lower() for name in existing_names}

    while final_name.lower() in existing_lower:
        suffix = f"_{count}"
        max_base_len = CONFIG.MAX_SHEET_NAME_LEN - len(suffix)
        if max_base_len <= 0:
            # Should be rare, but handles extreme cases
            final_name = f"Sheet_{datetime.now().timestamp()}"[-CONFIG.MAX_SHEET_NAME_LEN:] # Fallback
            log.warning(f"Could not generate unique name for base '{base_name}'. Using fallback: {final_name}")
            break
        candidate = base_name[:max_base_len] + suffix
        final_name = candidate
        count += 1
        if count > 1000: # Safety break
            log.error(f"Infinite loop detected generating sheet name for base '{base_name}'. Using fallback.")
            final_name = f"Sheet_{datetime.now().timestamp()}"[-CONFIG.MAX_SHEET_NAME_LEN:]
            break
    return final_name

def format_excel_sheet(worksheet):
    """Applies formatting (auto-width, wrap text) to an openpyxl worksheet."""
    for row in worksheet.iter_rows():
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical='top', horizontal='left')

    for col in worksheet.columns:
        max_length = 8 # Minimum width
        column_letter = col[0].column_letter
        for cell in col:
            try:
                if cell.value:
                    cell_str = str(cell.value)
                    # Consider wrapped lines for width calculation
                    lines = cell_str.split('\n')
                    cell_len = max(len(line) for line in lines) if lines else 0
                    if cell_len > max_length:
                        max_length = cell_len
            except Exception as fmt_err:
                 log.warning(f"Could not evaluate cell length in col {column_letter}. Error: {fmt_err}")
                 continue # Skip problematic cell

        # Set adjusted width with padding, min/max limits
        adjusted_width = min(max((max_length + 2) * 1.1, 10), 70) # Increased max width slightly
        try:
            worksheet.column_dimensions[column_letter].width = adjusted_width
        except Exception as width_err:
             log.warning(f"Failed to set column width for {column_letter}: {width_err}")


# === Main Application Logic ===

def run_extraction_process(
    uploaded_file: BytesIO,
    pages_to_process: str,
    enable_translation: bool,
    target_lang_code: Optional[str],
    target_lang_name: Optional[str],
    camelot_flavor: str,
    edge_tolerance: int,
    row_tolerance: int
):
    """Orchestrates the PDF table extraction, processing, and Excel generation."""
    status_placeholder = st.empty()
    progress_bar = st.progress(0.0, text="Initializing extraction...")
    start_time = datetime.now()

    try:
        # --- 1. Extract Tables using Camelot ---
        status_placeholder.info(f"⏳ Reading PDF (Pages: '{pages_to_process}', Method: {camelot_flavor})...")
        progress_bar.progress(0.05, text="Reading PDF...")
        log.info(f"Starting Camelot extraction. File: {uploaded_file.name}, Pages: {pages_to_process}, Flavor: {camelot_flavor}")

        camelot_kwargs = {"pages": pages_to_process.lower(), "flavor": camelot_flavor, "strip_text": '\n'}
        if camelot_flavor == 'stream':
            camelot_kwargs['edge_tol'] = edge_tolerance
            camelot_kwargs['row_tol'] = row_tolerance

        try:
            tables = camelot.read_pdf(uploaded_file, **camelot_kwargs)
        except Exception as camelot_err:
             # Catch specific known errors if possible, e.g., file corruption, password protection
             log.error(f"Camelot extraction failed: {camelot_err}", exc_info=True)
             if "file has not been decrypted" in str(camelot_err).lower():
                  status_placeholder.error("❌ Extraction Failed: The PDF file is password-protected and cannot be processed.")
             elif "invalid pdf" in str(camelot_err).lower():
                  status_placeholder.error("❌ Extraction Failed: The uploaded file is not a valid or readable PDF.")
             else:
                  status_placeholder.error(f"❌ PDF Extraction Failed: An error occurred during table detection ({type(camelot_err).__name__}).")
             st.stop()


        log.info(f"Camelot found {len(tables)} table(s).")
        if not tables:
            status_placeholder.warning(f"⚠️ No tables detected on the specified pages ('{pages_to_process}') using the '{camelot_flavor}' method. Try adjusting pages or parsing method.")
            st.stop()

        total_tables = len(tables)
        status_placeholder.info(f"✅ Found {total_tables} tables. Preparing for processing...")
        progress_bar.progress(0.1, text=f"Found {total_tables} tables...")

        # --- 2. Process Each Table ---
        processed_data: List[Tuple[str, pd.DataFrame]] = [] # List of (sheet_name, dataframe)
        processed_sheet_names: List[str] = []
        table_counts_per_page: Dict[int, int] = {}
        has_content = False

        for i, table in enumerate(tables):
            current_progress = 0.1 + 0.7 * ((i + 1) / total_tables)
            page_num = table.page
            table_counts_per_page[page_num] = table_counts_per_page.get(page_num, 0) + 1
            table_num_on_page = table_counts_per_page[page_num]

            base_sheet_name = f"Page_{page_num}_Table_{table_num_on_page}"
            sheet_name = generate_unique_sheet_name(base_sheet_name, processed_sheet_names)

            status_placeholder.info(f"⚙️ Processing table {i+1}/{total_tables} ({sheet_name})...")
            progress_bar.progress(current_progress, text=f"Processing {sheet_name}...")

            try:
                df = table.df
                if df.empty:
                    log.info(f"Skipping empty raw table: {sheet_name}")
                    continue

                # Clean DataFrame
                df_cleaned = clean_dataframe(df)
                if df_cleaned.empty:
                    log.info(f"Skipping table {sheet_name} as it became empty after cleaning.")
                    continue

                # Translate DataFrame if enabled
                df_final = df_cleaned
                if enable_translation and target_lang_code:
                    df_final = translate_dataframe(df_cleaned, target_lang_code) # Uses cached function

                processed_data.append((sheet_name, df_final))
                processed_sheet_names.append(sheet_name)
                has_content = True
                log.debug(f"Successfully processed table {sheet_name} (Page {page_num}).")

            except Exception as process_err:
                 log.error(f"Error processing table {sheet_name} (Page {page_num}): {process_err}", exc_info=True)
                 st.warning(f"⚠️ Skipped table from Page {page_num} (Table {table_num_on_page}) due to a processing error.")
                 continue # Skip to the next table

        if not has_content:
            status_placeholder.warning("⚠️ No data could be extracted from the detected tables after cleaning/processing.")
            st.stop()

        # --- 3. Generate Excel File ---
        status_placeholder.info("💾 Generating Excel file...")
        progress_bar.progress(0.85, text="Generating Excel file...")
        output_buffer = BytesIO()
        try:
            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                for sheet_name, df_content in processed_data:
                    df_content.to_excel(writer, sheet_name=sheet_name, index=False)

                # Apply formatting after writing all data
                progress_bar.progress(0.90, text="Formatting Excel sheet(s)...")
                status_placeholder.info("🎨 Applying formatting...")
                workbook = writer.book
                for sheet_name in processed_sheet_names:
                     if sheet_name in workbook.sheetnames:
                         ws = workbook[sheet_name]
                         format_excel_sheet(ws)
                     else:
                         log.warning(f"Sheet '{sheet_name}' not found in workbook during formatting step.")

            output_buffer.seek(0)
            end_time = datetime.now()
            duration = end_time - start_time
            progress_bar.progress(1.0, text="Extraction Complete!")
            status_placeholder.success(f"✅ Success! Processed {len(processed_data)} table(s) into {len(processed_sheet_names)} sheet(s) in {duration.total_seconds():.2f} seconds.")

            # --- 4. Provide Download ---
            download_filename = f"extracted_{Path(uploaded_file.name).stem}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            st.download_button(
                label=f"📥 Download Excel File ({len(processed_sheet_names)} Sheet(s))",
                data=output_buffer,
                file_name=download_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_excel_button",
                use_container_width=True
            )

            # --- 5. Update Usage Count ---
            # This should happen *after* successful processing and file generation
            if not update_usage():
                 log.warning("Usage update failed after successful extraction.")
                 # Decide if this is critical enough to warn the user, perhaps not.
                 # st.warning("Could not record usage count for this extraction.")

        except Exception as excel_err:
            log.error(f"Error generating or formatting the Excel file: {excel_err}", exc_info=True)
            status_placeholder.error("❌ Error: Failed to generate the final Excel file.")
            st.stop()

    # --- General Error Handling for the Whole Process ---
    except MemoryError:
         log.error("MemoryError during extraction process.", exc_info=True)
         status_placeholder.error("❌ Processing Failed: Insufficient memory to handle the PDF or data. Try processing fewer pages or a smaller file.")
         st.stop()
    except ImportError as import_err:
         # Should ideally be caught at startup, but as a fallback
         log.critical(f"ImportError during processing: {import_err}", exc_info=True)
         status_placeholder.error("❌ Internal Error: A required library is missing.")
         st.stop()
    except Exception as e:
        # Catch-all for truly unexpected errors during the flow
        log.error(f"Unexpected error during extraction process: {e}", exc_info=True)
        # Avoid showing overly technical details unless necessary
        error_type = type(e).__name__
        if "relation \"users\" does not exist" in str(e).lower(): # Specific DB schema error
             status_placeholder.error("❌ Database Error: The application's user table is missing. Please contact support.")
             log.critical("DB schema error: 'users' table missing.", exc_info=True)
        elif "flavor='lattice'" in str(e) and ("edge_tol" in str(e) or "row_tol" in str(e)):
             status_placeholder.error("❌ Configuration Error: Tolerance settings (Edge/Row) are only applicable for the 'stream' parsing method, not 'lattice'.")
             log.error(f"Configuration error - tolerances used with lattice: {e}")
        else:
             status_placeholder.error(f"❌ An unexpected error occurred during processing ({error_type}). Please try again or contact support if the issue persists.")
        st.stop()


def main_app_interface():
    """Displays the main part of the application after login."""
    st.title(f"📄 {CONFIG.APP_TITLE}")
    st.markdown("Effortlessly extract tables from PDF files to Excel. Translate content on the fly.")
    st.divider()

    # --- Step 1: Upload ---
    st.subheader("1. Upload Your PDF File")
    uploaded_file = st.file_uploader(
        "Select a PDF file from your computer.",
        type=["pdf"],
        key="pdf_uploader",
        label_visibility="collapsed" # More concise label
    )

    if uploaded_file:
        file_size_kb = uploaded_file.size / 1024
        st.success(f"✅ PDF ready: '{uploaded_file.name}' ({file_size_kb:.1f} KB)")
        st.divider()

        # --- Step 2: Configure Options ---
        st.subheader("2. Configure Extraction Options")
        col_pages, col_translate = st.columns(2)

        with col_pages:
            st.markdown("📄 **Page Selection**")
            pages_to_process = st.text_input(
                "Specify pages",
                value="all",
                key="pages_input",
                help="Enter page numbers like '1', '3-5', '1,3,7', or 'all'."
            ).strip()
            # Improved regex for validation (allows spaces, case-insensitive 'all')
            if not re.fullmatch(r"^\s*(all|\d+(\s*-\s*\d+)?(\s*,\s*\d+(\s*-\s*\d+)?)*)\s*$", pages_to_process, re.IGNORECASE):
                st.error("Invalid page format. Use numbers, commas, hyphens (e.g., '1, 3-5') or 'all'.")
                st.stop() # Stop if format is invalid

        with col_translate:
            st.markdown("🌍 **Translation (Optional)**")
            enable_translation = st.checkbox("Translate extracted table text?", key="translate_cb", value=False)
            selected_lang_code: Optional[str] = None
            target_lang_name: Optional[str] = None

            if enable_translation:
                # Prepare sorted list of language names for dropdown
                lang_name_to_code = {v.title(): k for k, v in CONFIG.SUPPORTED_LANGUAGES.items()}
                sorted_lang_names = sorted(lang_name_to_code.keys())

                try:
                    # Find index of default language, fallback to 0 if not found
                    default_index = sorted_lang_names.index(CONFIG.DEFAULT_TRANSLATE_LANG_NAME.title())
                except ValueError:
                    default_index = 0
                    log.warning(f"Default translation language '{CONFIG.DEFAULT_TRANSLATE_LANG_NAME}' not found in supported list.")

                selected_lang_name = st.selectbox(
                    "Select target language:",
                    options=sorted_lang_names,
                    index=default_index,
                    key="lang_select"
                )

                selected_lang_code = lang_name_to_code.get(selected_lang_name)
                if selected_lang_code:
                    target_lang_name = selected_lang_name # Store the display name
                    st.info(f"Translation enabled: Target is **{selected_lang_name}**.", icon="ℹ️")
                else:
                    # Should not happen if list is generated correctly, but safety check
                    st.warning("Selected language code not found. Disabling translation.")
                    log.error(f"Could not find language code for selected name: {selected_lang_name}")
                    enable_translation = False

        # --- Advanced Options ---
        with st.expander("🔧 Advanced PDF Parsing Settings (Optional)"):
            st.markdown("Adjust these settings if the default extraction results are not satisfactory.")
            camelot_flavor = st.selectbox(
                "Parsing Method",
                ['stream', 'lattice'],
                index=0, # Default to stream
                help="'stream' is generally better for tables without clear grid lines. 'lattice' requires clear lines."
            )
            st.caption("Tolerance settings below apply **only** when using the 'stream' method.")
            c1, c2 = st.columns(2)
            with c1:
                edge_tolerance = st.slider(
                    "Edge Tolerance (Stream)", 0, 1000, 200, step=25, # Adjusted step
                    help="Distance tolerance (in points) near page edges for detecting elements. Increase if content near edges is missed."
                )
            with c2:
                row_tolerance = st.slider(
                    "Row Tolerance (Stream)", 0, 50, 10, step=1,
                    help="Vertical distance tolerance (in points) for grouping text into rows. Increase if rows are incorrectly split."
                 )
        st.divider()

        # --- Step 3: Process ---
        st.subheader("3. Process and Download")
        process_button_label = f"🚀 Extract Tables from '{pages_to_process}' Pages"
        if enable_translation and target_lang_name:
            process_button_label += f" & Translate to {target_lang_name}"

        # Disable button slightly before checking limits/credits to provide immediate feedback
        # We will re-enable it implicitly via st.button's behaviour or st.stop()
        process_button = st.button(
            process_button_label,
            key="process_button",
            type="primary",
            use_container_width=True,
            # disabled=st.session_state.get("processing_active", False) # Add later if needed
        )

        if process_button:
            # --- Pre-processing Checks ---
            process_allowed = True
            user_email = st.session_state.get("user_email")
            is_trial = st.session_state.get("is_trial_user", False)

            if not user_email:
                st.error("Critical Error: User session lost. Please log in again.")
                log.error("Processing attempted without user_email in session state.")
                process_allowed = False

            elif is_trial:
                if st.session_state.get("trial_uses_today", 0) >= CONFIG.TRIAL_DAILY_LIMIT:
                    st.error(f"Cannot process: Your daily trial limit of {CONFIG.TRIAL_DAILY_LIMIT} uses has been reached.")
                    log.warning(f"Processing blocked for trial user {user_email}: Limit reached.")
                    process_allowed = False
            else: # Premium user
                if st.session_state.get('credits', 0) <= 0:
                    st.error("Cannot process: You have no remaining credits. Please purchase more.")
                    log.warning(f"Processing blocked for premium user {user_email}: No credits.")
                    process_allowed = False

            if not process_allowed:
                st.stop() # Stop execution if checks fail

            # --- Run the Main Processing Function ---
            # Set a flag to potentially disable button during run (optional)
            # st.session_state.processing_active = True
            run_extraction_process(
                uploaded_file=uploaded_file,
                pages_to_process=pages_to_process,
                enable_translation=enable_translation,
                target_lang_code=selected_lang_code,
                target_lang_name=target_lang_name,
                camelot_flavor=camelot_flavor,
                edge_tolerance=edge_tolerance,
                row_tolerance=row_tolerance
            )
            # Reset flag after processing finishes or errors out (implicitly handled by Streamlit rerun/stop)
            # if "processing_active" in st.session_state:
            #     del st.session_state.processing_active

    else: # No file uploaded yet
        st.info("👋 Welcome! Please upload a PDF file using the uploader above to begin.")

# === Page Config & Main Execution ===
def set_page_config():
    """Sets the Streamlit page configuration."""
    st.set_page_config(
        page_title=f"{CONFIG.APP_TITLE} - v{CONFIG.APP_VERSION}",
        page_icon="📄",
        layout="wide",
        initial_sidebar_state="expanded"
    )

def display_footer():
    """Displays the application footer."""
    st.divider()
    st.caption(f"© {datetime.now().year} {CONFIG.APP_TITLE} | Version: {CONFIG.APP_VERSION} | Support: {CONFIG.SUPPORT_EMAIL}")

if __name__ == "__main__":
    set_page_config()

    # Initialize session state keys if they don't exist
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if "user_email" not in st.session_state:
        st.session_state.user_email = None
    # Add others as needed for default state before login attempt
    if "is_trial_user" not in st.session_state:
         st.session_state.is_trial_user = False
    if "credits" not in st.session_state:
         st.session_state.credits = 0
    if "trial_uses_today" not in st.session_state:
         st.session_state.trial_uses_today = 0


    # --- Primary Application Flow ---
    if not display_login_form():
        # If login form is displayed and user is not yet logged in, stop execution here.
        # The form submission will trigger a rerun if successful.
        st.stop()

    # --- If Logged In ---
    display_sidebar()
    main_app_interface()
    display_footer()
