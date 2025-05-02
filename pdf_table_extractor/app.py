# app.py (Streamlit App - DB Auth/Credits + JSON Trial - Refined v2.8-diag)
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

# --- Required for DB Interaction ---
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import OperationalError # Import specifically for catching DB connection errors
from passlib.context import CryptContext
from typing import Dict, Any, Optional, Tuple, List
# ----------------------------------

# --- Seed langdetect ---
try:
    DetectorFactory.seed = 0
except NameError:
    logging.warning("Could not seed DetectorFactory for langdetect.")
# ----------------------

# --- Page Config (MUST be first Streamlit command) ---
st.set_page_config(
    page_title="PDF Table Extractor Pro v2.8-diag", # Updated version
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)
# ----------------------------------------------------

# === Logging Setup ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')

# === Configuration Constants ===
TRIAL_DAILY_LIMIT = 20
TRIAL_EMAIL = "freetrial@example.com" # Case-insensitive comparison used later
APP_VERSION = "2.8-diag" # Updated version reflects diagnostic changes
APP_TITLE = "PDF Table Extractor Pro"
SUPPORT_EMAIL = "lovinquesaba17@gmail.com"
# Using the extensive language list (ensure this matches deep_translator's capabilities if needed)
SUPPORTED_LANGUAGES = {
    'af': 'Afrikaans', 'sq': 'Albanian', 'am': 'Amharic', 'ar': 'Arabic', 'hy': 'Armenian', 'as': 'Assamese', 'ay': 'Aymara', 'az': 'Azerbaijani',
    'bm': 'Bambara', 'eu': 'Basque', 'be': 'Belarusian', 'bn': 'Bengali', 'bho': 'Bhojpuri', 'bs': 'Bosnian', 'bg': 'Bulgarian', 'ca': 'Catalan',
    'ceb': 'Cebuano', 'ny': 'Chichewa', 'zh-cn': 'Chinese (Simplified)', 'zh-tw': 'Chinese (Traditional)', 'co': 'Corsican', 'hr': 'Croatian',
    'cs': 'Czech', 'da': 'Danish', 'dv': 'Dhivehi', 'doi': 'Dogri', 'nl': 'Dutch', 'en': 'English', 'eo': 'Esperanto', 'et': 'Estonian', 'ee': 'Ewe',
    'tl': 'Filipino', 'fi': 'Finnish', 'fr': 'French', 'fy': 'Frisian', 'gl': 'Galician', 'lg': 'Ganda', 'ka': 'Georgian', 'de': 'German', 'el': 'Greek',
    'gn': 'Guarani', 'gu': 'Gujarati', 'ht': 'Haitian Creole', 'ha': 'Hausa', 'haw': 'Hawaiian', 'iw': 'Hebrew', 'he': 'Hebrew', 'hi': 'Hindi',
    'hmn': 'Hmong', 'hu': 'Hungarian', 'is': 'Icelandic', 'ig': 'Igbo', 'ilo': 'Ilocano', 'id': 'Indonesian', 'ga': 'Irish', 'it': 'Italian',
    'ja': 'Japanese', 'jw': 'Javanese', 'kn': 'Kannada', 'kk': 'Kazakh', 'km': 'Khmer', 'rw': 'Kinyarwanda', 'gom': 'Konkani', 'ko': 'Korean',
    'kri': 'Krio', 'ku': 'Kurdish (Kurmanji)', 'ckb': 'Kurdish (Sorani)', 'ky': 'Kyrgyz', 'lo': 'Lao', 'la': 'Latin', 'lv': 'Latvian', 'ln': 'Lingala',
    'lt': 'Lithuanian', 'lb': 'Luxembourgish', 'mk': 'Macedonian', 'mai': 'Maithili', 'mg': 'Malagasy', 'ms': 'Malay', 'ml': 'Malayalam', 'mt': 'Maltese',
    'mi': 'Maori', 'mr': 'Marathi', 'mni-mtei': 'Meiteilon (Manipuri)', 'lus': 'Mizo', 'mn': 'Mongolian', 'my': 'Myanmar (Burmese)', 'ne': 'Nepali',
    'no': 'Norwegian', 'or': 'Odia (Oriya)', 'om': 'Oromo', 'ps': 'Pashto', 'fa': 'Persian', 'pl': 'Polish', 'pt': 'Portuguese', 'pa': 'Punjabi',
    'qu': 'Quechua', 'ro': 'Romanian', 'ru': 'Russian', 'sm': 'Samoan', 'sa': 'Sanskrit', 'gd': 'Scots Gaelic', 'nso': 'Sepedi', 'sr': 'Serbian',
    'st': 'Sesotho', 'sn': 'Shona', 'sd': 'Sindhi', 'si': 'Sinhala', 'sk': 'Slovak', 'sl': 'Slovenian', 'so': 'Somali', 'es': 'Spanish', 'su': 'Sundanese',
    'sw': 'Swahili', 'sv': 'Swedish', 'tg': 'Tajik', 'ta': 'Tamil', 'tt': 'Tatar', 'te': 'Telugu', 'th': 'Thai', 'ti': 'Tigrinya', 'ts': 'Tsonga',
    'tr': 'Turkish', 'tk': 'Turkmen', 'ak': 'Twi', 'uk': 'Ukrainian', 'ur': 'Urdu', 'ug': 'Uyghur', 'uz': 'Uzbek', 'vi': 'Vietnamese', 'cy': 'Welsh',
    'xh': 'Xhosa', 'yi': 'Yiddish', 'yo': 'Yoruba', 'zu': 'Zulu'
}
DEFAULT_TRANSLATE_LANG_NAME = "English" # Use the name for consistency
DEFAULT_CAMELOT_FLAVOR='stream'
DEFAULT_EDGE_TOLERANCE = 200
DEFAULT_ROW_TOLERANCE = 10
MAX_SHEET_NAME_LEN = 31
FIXED_COLUMN_WIDTH = 35 # *** New constant for fixed width ***
LOGO_PATH = "logo.png" # Relative path
# --- JSON File Paths (Only for Trial Logic) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRIAL_FILE_PATH = os.path.join(BASE_DIR, "trial_users.json") # Full path
# --- ---

# === Database Setup ===
db_config_app = Flask(__name__) # Dummy Flask app for config
DATABASE_URL = os.environ.get('DATABASE_URL')
FLASK_SECRET_KEY = os.environ.get('FLASK_SECRET_KEY', 'default-secret-key-change-me') # Use a default but warn

if not DATABASE_URL:
    st.error("FATAL ERROR: DATABASE_URL environment variable is not set. Application cannot connect to the database. Contact support.")
    logging.critical("DATABASE_URL environment variable missing.")
    st.stop()
if FLASK_SECRET_KEY == 'default-secret-key-change-me':
     logging.warning("Using default FLASK_SECRET_KEY. Set a proper secret key environment variable for production.")

db_config_app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
# Add options for connection pooling and handling disconnects (recommended for production)
db_config_app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    "pool_pre_ping": True, # Check connection before using from pool
    "pool_recycle": 300,   # Recycle connections after 5 minutes
}
db_config_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db_config_app.config['SECRET_KEY'] = FLASK_SECRET_KEY

db = None
try:
    db = SQLAlchemy(db_config_app)
    logging.info("SQLAlchemy initialized successfully.")
except Exception as db_init_err:
    st.error(f"FATAL ERROR: Database initialization failed. Please contact support.")
    logging.critical(f"SQLAlchemy initialization failed: {db_init_err}", exc_info=True)
    st.stop()

# === Password Hashing Context ===
try:
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    logging.info("Passlib context initialized.")
except Exception as pwd_err:
     st.error("FATAL ERROR: Security component failed. Contact support.")
     logging.critical(f"Passlib context initialization failed: {pwd_err}", exc_info=True)
     st.stop()

# === Database Model ===
if db:
    class User(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        email = db.Column(db.String(120), unique=True, nullable=False, index=True)
        password_hash = db.Column(db.String(128), nullable=False)
        credits = db.Column(db.Integer, nullable=False, default=100)
        created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
        last_login_at = db.Column(db.DateTime(timezone=True), nullable=True)

        def check_password(self, password: str) -> bool:
            """Verifies the provided password against the stored hash."""
            try:
                if not isinstance(self.password_hash, str):
                    logging.warning(f"Password hash for {self.email} is not a string.")
                    return False
                return pwd_context.verify(password, self.password_hash)
            except Exception as e:
                logging.error(f"Password verification error for {self.email}: {e}", exc_info=True)
                return False

        def __repr__(self):
            return f'<User {self.email} (Credits: {self.credits})>'
else:
    st.error("Database connection failed earlier. Cannot define User model.")
    logging.critical("Skipping User model definition due to prior DB init failure.")
    st.stop()

# === JSON File Utilities (Only for Trial Data) ===
def load_trial_json(filepath: str) -> Dict[str, Any]:
    """Loads trial data from a JSON file, creating it if it doesn't exist."""
    try:
        if not os.path.exists(filepath):
            logging.warning(f"Trial file not found: {filepath}. Creating an empty one.")
            with open(filepath, "w", encoding='utf-8') as f: json.dump({}, f)
            return {}
        with open(filepath, "r", encoding='utf-8') as f:
            data = json.load(f)
            if not isinstance(data, dict):
                logging.error(f"Trial file {filepath} does not contain a valid JSON object. Returning empty dict.")
                return {}
            return data
    except json.JSONDecodeError as json_err:
        logging.error(f"Failed to decode JSON from {filepath}: {json_err}. Returning empty dict.")
        return {}
    except Exception as e:
        logging.error(f"Failed to load trial JSON {filepath}: {e}", exc_info=True)
        return {}

def save_trial_json(filepath: str, data: Dict[str, Any]) -> None:
    """Saves trial data to a JSON file."""
    try:
        with open(filepath, "w", encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Failed to save trial JSON {filepath}: {e}", exc_info=True)
        st.warning("Could not save trial usage data.")

# === Authentication & Session Logic ===
def authenticate_user_db(email: str, password: str) -> Optional[Dict[str, Any]]:
    """Verifies user credentials against the database."""
    if not email or not password:
        logging.warning("Authentication attempt with empty email or password.")
        return None
    logging.info(f"Attempting DB authentication for user: {email}")
    try:
        with db_config_app.app_context():
            user = db.session.query(User).filter(db.func.lower(User.email) == email.lower()).first()
            if user and user.check_password(password):
                logging.info(f"User '{email}' authenticated successfully via DB.")
                try:
                    user.last_login_at = datetime.now(timezone.utc)
                    db.session.commit()
                    logging.info(f"Updated last_login_at for user: {email}")
                except Exception as update_err:
                    db.session.rollback()
                    logging.warning(f"Could not update last_login_at for {email}: {update_err}", exc_info=True)
                return {"email": user.email, "credits": user.credits}
            elif user:
                logging.warning(f"DB Authentication failed for '{email}': Invalid password.")
                return None
            else:
                logging.warning(f"DB Authentication failed for '{email}': User not found.")
                return None
    except OperationalError as db_op_err:
        logging.error(f"Database OperationalError during authentication for {email}: {db_op_err}", exc_info=True)
        st.error("Login failed: Could not connect to the database. Please try again later.")
        return None
    except Exception as e:
        logging.error(f"Unexpected error during database authentication for {email}: {e}", exc_info=True)
        st.error("Login failed due to an unexpected server error. Please contact support.")
        return None

def initialize_user_session(user_data: Dict[str, Any]) -> bool:
    """Initializes Streamlit session state using DB data and trial file."""
    try:
        email = user_data.get("email")
        if not email:
            logging.error("Cannot initialize session: email missing from user_data.")
            return False

        st.session_state.logged_in = True
        st.session_state.user_email = email
        is_trial = (email.lower() == TRIAL_EMAIL.lower())
        st.session_state.is_trial_user = is_trial

        if is_trial:
            trial_data = load_trial_json(TRIAL_FILE_PATH)
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            user_trial_info = trial_data.get(email, {"date": today_str, "uses": 0})
            if user_trial_info.get("date") != today_str:
                logging.info(f"Resetting trial uses for {email} for new day {today_str}.")
                user_trial_info = {"date": today_str, "uses": 0}
                trial_data[email] = user_trial_info
                save_trial_json(TRIAL_FILE_PATH, trial_data)
            st.session_state.trial_uses_today = user_trial_info.get("uses", 0)
            if st.session_state.trial_uses_today >= TRIAL_DAILY_LIMIT:
                 st.error(f"Cannot log in: Trial limit of {TRIAL_DAILY_LIMIT} uses reached for today.")
                 logging.warning(f"Trial user {email} attempted login but daily limit reached.")
                 st.session_state.logged_in = False
                 return False
            st.session_state.credits = float('inf')
        else:
            st.session_state.credits = user_data.get("credits", 0)
            st.session_state.trial_uses_today = 0

        logging.info(f"Session initialized successfully for {email}. Trial: {is_trial}, Credits/Uses: {st.session_state.credits if not is_trial else st.session_state.trial_uses_today}")
        return True
    except Exception as e:
        logging.error(f"Session initialization error for {user_data.get('email', 'UNKNOWN_USER')}: {e}", exc_info=True)
        st.error("An error occurred while setting up your session.")
        return False

# === Login UI ===
def display_login_form() -> bool:
    """Displays login form, uses database authentication, and initializes session."""
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if st.session_state.logged_in:
        return True

    try:
        logo_filepath = os.path.join(BASE_DIR, LOGO_PATH)
        if os.path.exists(logo_filepath): st.image(logo_filepath, width=150)
        else: logging.info(f"Optional logo file not found at {logo_filepath}, skipping.")
    except Exception as logo_err: logging.warning(f"Could not load logo: {logo_err}")

    st.title(f"Welcome to {APP_TITLE}")
    st.markdown("Please log in to access the tool.")
    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col2:
        with st.form("login_form"):
            st.subheader("🔐 Secure Login")
            email = st.text_input("Email Address", key="login_email").strip()
            password = st.text_input("Password", type="password", key="login_password")
            submitted = st.form_submit_button("Sign In", use_container_width=True)
            if submitted:
                if not email or not password:
                    st.warning("Please enter both email and password."); return False
                user_data = authenticate_user_db(email, password)
                if user_data:
                    if initialize_user_session(user_data):
                        st.toast(f"Login successful! Welcome back.", icon="✅"); st.rerun()
                    else:
                        st.session_state.logged_in = False; return False
                else:
                    if "last_login_at" not in st.session_state: st.error("❌ Invalid email or password.")
                    return False
    return False

# === Sidebar UI ===
def display_sidebar() -> None:
    """Displays sidebar with user info and logout button."""
    with st.sidebar:
        st.title("⚙️ Account & Info")
        st.divider()
        user_email = st.session_state.get("user_email", "N/A")
        st.write(f"👤 **User:** `{user_email}`")
        if st.session_state.get("is_trial_user", False):
            st.info("🧪 Free Trial Account", icon="🧪")
            uses_today = st.session_state.get("trial_uses_today", 0)
            st.metric(label="Uses Today", value=f"{uses_today} / {TRIAL_DAILY_LIMIT}")
            prog = uses_today / TRIAL_DAILY_LIMIT if TRIAL_DAILY_LIMIT > 0 else 0
            st.progress(min(prog, 1.0))
            if uses_today >= TRIAL_DAILY_LIMIT: st.error("❌ Daily limit reached.")
        else:
            st.success("✅ Premium Account", icon="💳")
            credits = st.session_state.get('credits', 0)
            st.metric("Remaining Credits", credits)
            if credits <= 0: st.error("❌ No credits remaining.")
            # st.link_button("Buy More Credits", "YOUR_PURCHASE_LINK", use_container_width=True)
        st.divider()
        st.markdown("### 🔐 Session")
        if st.button("Log Out", use_container_width=True, key="logout_button"):
            logging.info(f"User logged out: {user_email}")
            keys_to_clear = ["logged_in", "user_email", "is_trial_user", "credits", "trial_uses_today"]
            for key in keys_to_clear:
                if key in st.session_state: del st.session_state[key]
            st.session_state.logged_in = False
            st.toast("You have been logged out.", icon="👋"); st.rerun()
        st.divider()
        st.caption(f"Version: {APP_VERSION}")
        st.caption(f"Support: {SUPPORT_EMAIL}")

# === Usage Update ===
def update_usage_count(user_email: str, is_trial: bool) -> None:
    """Updates usage counts: Decrements credits in DB for paid users, increments uses in JSON for trial."""
    if not user_email:
        logging.error("Cannot update usage: user_email is missing.")
        st.error("Error recording usage: Session data missing."); return
    try:
        if is_trial:
            current_uses = st.session_state.get("trial_uses_today", 0) + 1
            st.session_state.trial_uses_today = current_uses
            trial_data = load_trial_json(TRIAL_FILE_PATH)
            user_trial_info = trial_data.get(user_email, {})
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            user_trial_info.update({"uses": current_uses, "date": today_str})
            trial_data[user_email] = user_trial_info
            save_trial_json(TRIAL_FILE_PATH, trial_data)
            logging.info(f"[Trial] Usage updated for {user_email}. Uses today: {current_uses}")
            st.toast(f"Trial use recorded ({current_uses}/{TRIAL_DAILY_LIMIT} today).", icon="⏳")
        else:
            with db_config_app.app_context():
                user = db.session.query(User).filter(db.func.lower(User.email) == user_email.lower()).first()
                if user:
                    if user.credits > 0:
                        user.credits -= 1; db.session.commit()
                        st.session_state.credits = user.credits
                        logging.info(f"[Premium] Credit deducted for {user_email}. Remaining: {user.credits}")
                        st.toast("1 credit deducted.", icon="🪙")
                    else:
                        logging.warning(f"Attempted to deduct credit for {user_email}, but credits were already zero.")
                        st.warning("Credit deduction skipped: Already at 0 credits.")
                else:
                    logging.error(f"Cannot update credits for {user_email}: User not found in database during usage update.")
                    st.error("Error: Could not find user record to update credits.")
    except OperationalError as db_op_err:
         if not is_trial:
              with db_config_app.app_context(): db.session.rollback()
         logging.error(f"Database OperationalError during usage update for {user_email}: {db_op_err}", exc_info=True)
         st.error("⚠️ Error connecting to database while updating usage count.")
    except Exception as e:
        if not is_trial:
             try:
                 with db_config_app.app_context(): db.session.rollback()
             except Exception as rb_err: logging.error(f"Error during rollback attempt: {rb_err}")
        logging.error(f"Failed to update usage count for {user_email} (Trial={is_trial}): {e}", exc_info=True)
        st.error("⚠️ An unexpected error occurred while updating usage count.")

# === PDF Processing & Helper Functions ===
@st.cache_data(show_spinner=False)
def translate_text(_text: Any, target_lang: str) -> str:
    """Translates a single text string, returns original on error or if already target lang."""
    original_text = str(_text).strip()
    if not original_text or not target_lang: return original_text
    try:
        detected_lang = detect(original_text)
        if detected_lang.split('-')[0] == target_lang.split('-')[0]: return original_text
    except LangDetectException: pass
    except Exception as detect_err: logging.warning(f"Language detection failed: {detect_err}")
    try:
        translated = GoogleTranslator(source='auto', target=target_lang).translate(original_text)
        return translated if translated else original_text
    except Exception as translate_err:
        logging.warning(f"Translation failed for text snippet '{original_text[:50]}...' to lang '{target_lang}'. Error: {translate_err}")
        return original_text

def translate_dataframe(df: pd.DataFrame, target_lang: str) -> pd.DataFrame:
    """Translates text in each cell of a DataFrame using .map."""
    if target_lang and not df.empty:
        logging.info(f"Translating DataFrame to {target_lang}...")
        translated_df = df.copy().map(lambda x: translate_text(x, target_lang))
        logging.info("DataFrame translation finished.")
        return translated_df
    return df

def split_merged_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Splits cells containing newline characters into multiple rows."""
    new_rows = []
    df_filled = df.fillna('')
    original_columns = df_filled.columns
    for index, row in df_filled.iterrows():
        row_list = row.tolist()
        if any('\n' in str(cell) for cell in row_list):
            parts = [str(cell).split('\n') for cell in row_list]
            try: max_len = max(len(p) for p in parts) if parts else 0
            except ValueError: logging.warning(f"ValueError calculating max_len for row {index}, defaulting to 0."); max_len = 0
            for i in range(max_len): new_rows.append([p[i] if i < len(p) else '' for p in parts])
        else: new_rows.append(row_list)
    if not new_rows: return pd.DataFrame(columns=original_columns)
    else: return pd.DataFrame(new_rows, columns=original_columns)

def generate_unique_sheet_name(base_name: str, existing_names: List[str]) -> str:
    """Generates a unique Excel sheet name within the 31-character limit."""
    sanitized_base = re.sub(r'[\\/*?:\[\]]', '_', base_name)
    sheet_name = sanitized_base[:MAX_SHEET_NAME_LEN]
    count = 1; temp_name = sheet_name
    while temp_name.lower() in [name.lower() for name in existing_names]:
        suffix = f"_{count}"
        max_base_len = MAX_SHEET_NAME_LEN - len(suffix)
        if max_base_len <= 0:
            fallback_suffix = f"_{datetime.now().strftime('%f')}"
            max_fallback_len = MAX_SHEET_NAME_LEN - len(fallback_suffix)
            temp_name = sanitized_base[:max_fallback_len] + fallback_suffix
            temp_name = temp_name[:MAX_SHEET_NAME_LEN]
            logging.warning(f"Sheet name collision resolution needed fallback for base '{base_name}'")
            break
        temp_name = sanitized_base[:max_base_len] + suffix
        count += 1
        if count > 999:
            logging.error(f"Could not generate unique sheet name for base '{base_name}' after 999 attempts.")
            temp_name = f"Err_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"[:MAX_SHEET_NAME_LEN]
            break
    return temp_name

def format_excel_sheet(ws) -> None:
    """Applies formatting: wrap text, alignment, and FIXED column width."""
    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical='top', horizontal='left')

    # *** Set FIXED column width ***
    for col in ws.columns:
        column_letter = col[0].column_letter
        try:
            # Set the fixed width defined by the constant
            ws.column_dimensions[column_letter].width = FIXED_COLUMN_WIDTH
        except Exception as width_err:
             logging.warning(f"Could not set fixed width ({FIXED_COLUMN_WIDTH}) for column {column_letter}: {width_err}")

# === Main Application Logic ===
def main_app() -> None:
    """Renders the main UI and handles PDF processing."""
    st.title(f"📄 {APP_TITLE}")
    st.markdown("Extract tables from PDF files, translate content, and download as Excel.")
    st.divider()

    # Step 1: Upload
    st.subheader("1. Upload Your PDF")
    uploaded_file = st.file_uploader("Select a PDF file", type="pdf", key="pdf_uploader", label_visibility="collapsed")
    if not uploaded_file:
        st.info("👋 Upload a PDF file to begin the extraction process."); st.stop()
    st.success(f"✅ Ready to process: '{uploaded_file.name}'"); st.divider()

    # Step 2: Configure Extraction
    st.subheader("2. Configure Extraction")
    col_pages, col_translate = st.columns(2)
    with col_pages:
        st.markdown("📄 **Pages**")
        pages_to_process = st.text_input("Specify pages", value="all", key="pages_input", help="Enter page numbers (e.g., '1,3,5-7') or 'all'.").strip().lower()
        if not re.fullmatch(r"^\s*(all|\d+(\s*-\s*\d+)?(\s*,\s*\d+(\s*-\s*\d+)?)*)\s*$", pages_to_process):
            st.error("Invalid page format. Use '1', '1,3', '1-5', '1,3-5', or 'all'."); st.stop()
    with col_translate:
        st.markdown("🌍 **Translate**")
        enable_translation = st.checkbox("Translate extracted content?", key="translate_cb", value=False)
        selected_lang_code = None; target_lang_name = None
        if enable_translation:
            lang_name_to_code = {v: k for k, v in SUPPORTED_LANGUAGES.items()}
            sorted_lang_names = sorted(SUPPORTED_LANGUAGES.values())
            try: default_index = sorted_lang_names.index(DEFAULT_TRANSLATE_LANG_NAME)
            except ValueError: logging.warning(f"Default lang '{DEFAULT_TRANSLATE_LANG_NAME}' not found."); default_index = 0
            selected_lang_name = st.selectbox("Translate to language:", sorted_lang_names, index=default_index, key="lang_select")
            selected_lang_code = lang_name_to_code.get(selected_lang_name)
            if selected_lang_code: target_lang_name = selected_lang_name; st.info(f"Content will be translated to **{selected_lang_name}**.", icon="ℹ️")
            else: st.warning("Selected language not found, translation disabled."); enable_translation = False
    with st.expander("🔧 Advanced Settings"):
        st.markdown("Adjust parsing method and tolerances if default results are not optimal. Hover '?' for info.")
        camelot_flavor = st.selectbox("Parsing Method", ['stream', 'lattice'], index=['stream', 'lattice'].index(DEFAULT_CAMELOT_FLAVOR), help="'stream' uses whitespace alignment. 'lattice' uses explicit lines.")
        st.caption("_Tolerance sliders apply only when 'stream' method is selected._")
        c1, c2 = st.columns(2)
        with c1: edge_tolerance = st.slider("Edge Tolerance", 0, 1000, DEFAULT_EDGE_TOLERANCE, 50, help="Affects detection near page edges (Stream only).", disabled=(camelot_flavor != 'stream'))
        with c2: row_tolerance = st.slider("Row Tolerance", 0, 50, DEFAULT_ROW_TOLERANCE, 1, help="Affects vertical grouping of text (Stream only).", disabled=(camelot_flavor != 'stream'))
    st.divider()

    # Step 3: Process
    st.subheader("3. Process & Download")
    process_button_label = f"🚀 Extract ('{pages_to_process}' pages, method: {camelot_flavor})"
    if st.button(process_button_label, key="process_button", type="primary", use_container_width=True):
        user_email = st.session_state.get("user_email")
        is_trial = st.session_state.get("is_trial_user", False)
        process_allowed = True
        if not user_email: st.error("❌ Session error: User email not found."); logging.error("Processing aborted: user_email not found."); process_allowed = False; st.stop()
        if is_trial:
            if st.session_state.get("trial_uses_today", 0) >= TRIAL_DAILY_LIMIT: st.error(f"❌ Cannot process: Trial limit reached."); process_allowed = False
        elif st.session_state.get('credits', 0) <= 0: st.error("❌ Cannot process: No credits remaining."); process_allowed = False
        if not process_allowed: logging.warning(f"Processing denied for {user_email} (Trial: {is_trial}, Limit/Credits reached)."); st.stop()

        status_placeholder = st.empty(); progress_bar = st.progress(0.0, text="Initializing...")
        start_time = datetime.now()
        try:
            status_placeholder.info(f"⏳ Reading PDF '{uploaded_file.name}'..."); progress_bar.progress(0.05, text="Reading PDF...")
            camelot_kwargs = {"pages": pages_to_process, "flavor": camelot_flavor, "strip_text": '\n'}
            if camelot_flavor == 'stream': camelot_kwargs.update({'edge_tol': edge_tolerance, 'row_tol': row_tolerance})
            logging.info(f"Calling Camelot.read_pdf with args: {camelot_kwargs}")
            tables = camelot.read_pdf(uploaded_file, **camelot_kwargs)

            if not tables:
                status_placeholder.warning(f"⚠️ No tables found by Camelot on pages '{pages_to_process}' using '{camelot_flavor}' method. Try adjusting settings."); logging.warning(f"Camelot found 0 tables for {user_email} with settings: {camelot_kwargs}"); st.stop()
            total_tables = len(tables); logging.info(f"Camelot found {total_tables} potential tables for {user_email}.")
            status_placeholder.info(f"✅ Found {total_tables} potential tables. Processing..."); progress_bar.progress(0.1, text=f"Processing {total_tables} tables...")
            output_buffer = BytesIO(); processed_sheets: List[str] = []; table_counts_per_page: Dict[int, int] = {}; has_content = False
            interval = max(1, total_tables // 10)

            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                for i, table in enumerate(tables):
                    if i % interval == 0 or i == total_tables - 1:
                        progress = 0.1 + 0.7 * ((i + 1) / total_tables); progress_bar.progress(progress, text=f"Processing Table {i+1}/{total_tables}...")
                    page_num = table.page; table_counts_per_page[page_num] = table_counts_per_page.get(page_num, 0) + 1
                    table_num = table_counts_per_page[page_num]; base_sheet_name = f"Page_{page_num}_Table_{table_num}"
                    sheet_name = generate_unique_sheet_name(base_sheet_name, processed_sheets)
                    df = table.df

                    # --- DIAGNOSTIC LOG 1: Check initial DataFrame from Camelot ---
                    if df.empty:
                        logging.info(f"Table {i+1} ({sheet_name}): DataFrame directly from Camelot is empty. Skipping.")
                        continue

                    # --- DIAGNOSTIC LOG 2: Log raw content before cleaning ---
                    logging.info(f"--- RAW DataFrame Content for Sheet: {sheet_name} ---")
                    try: logging.info(df.head().to_string()) # Log first 5 rows
                    except Exception as log_err: logging.info(f"Could not log raw df head: {log_err}")
                    logging.info("-----------------------------------------------------")
                    # --- End Diagnostic Logging ---

                    try:
                        df.columns = [str(c).strip() for c in df.columns]
                        df = split_merged_rows(df); df = df.astype(str)
                        if df.empty:
                            logging.info(f"Table {i+1} ({sheet_name}): DataFrame became empty *after* cleaning. Skipping.")
                            continue
                        has_content = True # Mark that we found usable data
                        if enable_translation and selected_lang_code: df = translate_dataframe(df, selected_lang_code)
                        df.to_excel(writer, sheet_name=sheet_name, index=False); processed_sheets.append(sheet_name)
                        logging.info(f"Successfully processed and added sheet: {sheet_name}")
                    except Exception as process_err:
                        logging.error(f"Error processing table {i+1} ({sheet_name}): {process_err}", exc_info=True)
                        st.warning(f"⚠️ Skipped table on Page {page_num} (Table {table_num}) due to processing error."); continue

            if not has_content:
                status_placeholder.warning("⚠️ No data extracted. Detected tables were empty after processing. Check logs and PDF/settings."); logging.warning(f"Processing completed for {user_email}, but has_content flag remained False."); st.stop()

            progress_bar.progress(0.85, text="Formatting Excel..."); status_placeholder.info("🎨 Formatting Excel sheet...")
            workbook = writer.book
            for sheet_title in processed_sheets:
                try: format_excel_sheet(workbook[sheet_title])
                except Exception as fmt_e: logging.warning(f"Could not format sheet '{sheet_title}': {fmt_e}")
            output_buffer.seek(0); duration = datetime.now() - start_time
            progress_bar.progress(1.0, text="Complete!"); status_placeholder.success(f"✅ Processed {len(processed_sheets)} non-empty tables in {duration.total_seconds():.1f} seconds.")
            download_filename = f"extracted_{os.path.splitext(uploaded_file.name)[0]}_{datetime.now():%Y%m%d_%H%M}.xlsx"
            st.download_button(label=f"📥 Download Excel ({len(processed_sheets)} Sheets)", data=output_buffer, file_name=download_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_btn", use_container_width=True)
            update_usage_count(user_email, is_trial) # Update usage after success

        except ImportError as imp_err: status_placeholder.error(f"❌ Error: Missing library ({imp_err}). Contact support."); logging.critical(f"ImportError: {imp_err}", exc_info=True); st.stop()
        except OperationalError as db_op_err: status_placeholder.error("❌ Database Error during usage update."); logging.error(f"DB OperationalError during usage update: {db_op_err}", exc_info=True) # Don't stop, allow download
        except Exception as e:
            if "edge_tol,row_tol cannot be used with flavor='lattice'" in str(e): status_placeholder.error("❌ Config Error: Tolerances incompatible with 'lattice'."); logging.error(f"Camelot config error: {e}")
            elif "relation \"user\" does not exist" in str(e).lower(): status_placeholder.error("❌ DB Error: User table missing."); logging.critical("DB schema error: 'user' table missing.", exc_info=True)
            else: status_placeholder.error("❌ Unexpected processing error."); logging.error(f"Unhandled exception: {e}", exc_info=True)
            st.stop()

# === Entry Point ===
if __name__ == "__main__":
    if display_login_form():
        display_sidebar()
        main_app()
    st.divider()
    st.caption(f"© {datetime.now().year} {APP_TITLE}")
