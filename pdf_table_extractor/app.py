# app.py (v3.4.1 - Stream Only)
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
from sqlalchemy.exc import OperationalError
from passlib.context import CryptContext
from typing import Dict, Any, Optional, Tuple, List
# ----------------------------------

# --- Seed langdetect ---
try: DetectorFactory.seed = 0
except NameError: logging.warning("Could not seed DetectorFactory.")
except Exception as seed_err: logging.warning(f"Error seeding langdetect: {seed_err}")
# ----------------------

# --- Page Config ---
st.set_page_config(
    page_title="PDF Table Data Extractor + Multi Language Translator",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)
# ----------------------------------------------------

# === Logging Setup ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
log = logging.getLogger(__name__) # Use a named logger

# === Configuration Constants ===
APP_VERSION = "1.0-StreamOnly" # New version marker based on v3.4
APP_TITLE = "PDF Table Data Extractor + Multi Language Translator"
SUPPORT_EMAIL = "lovinquesaba17@gmail.com"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# --- Trial File Config (JSON) ---
TRIAL_FILE_PATH = os.path.join(BASE_DIR, "trial_users.json")
TRIAL_DAILY_LIMIT = 20
TRIAL_EMAIL = "freetrial@example.com"
# --- ---
LOGO_PATH = "logo.png"
MAX_SHEET_NAME_LEN = 31
# Language List
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

DEFAULT_TRANSLATE_LANG_NAME = "English"

# === Database Setup ===
db_config_app = Flask(__name__)
DATABASE_URL = os.environ.get('DATABASE_URL')
FLASK_SECRET_KEY = os.environ.get('FLASK_SECRET_KEY', 'default-secret-key-change-me')
if not DATABASE_URL: st.error("FATAL ERROR: DATABASE_URL missing."); log.critical("DATABASE_URL missing."); st.stop()
if FLASK_SECRET_KEY == 'default-secret-key-change-me': log.warning("Using default FLASK_SECRET_KEY.")
db_config_app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
db_config_app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {"pool_pre_ping": True, "pool_recycle": 300}
db_config_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db_config_app.config['SECRET_KEY'] = FLASK_SECRET_KEY
db = None
try: db = SQLAlchemy(db_config_app); log.info("SQLAlchemy initialized.")
except Exception as db_init_err: st.error("FATAL ERROR: DB initialization failed."); log.critical(f"SQLAlchemy init failed: {db_init_err}", exc_info=True); st.stop()

# === Password Hashing Context ===
try: pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto"); log.info("Passlib context initialized.")
except Exception as pwd_err: st.error("FATAL ERROR: Security component failed."); log.critical(f"Passlib init failed: {pwd_err}", exc_info=True); st.stop()

# === Database Model ===
# NOTE: This script version *does not* include db.create_all().
# It assumes the 'users' table already exists in the database.
if db:
    class User(db.Model):
        id = db.Column(db.Integer, primary_key=True); email = db.Column(db.String(120), unique=True, nullable=False, index=True); password_hash = db.Column(db.String(128), nullable=False); credits = db.Column(db.Integer, nullable=False, default=100); created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)); last_login_at = db.Column(db.DateTime(timezone=True), nullable=True)
        def check_password(self, password: str) -> bool:
            try:
                if not isinstance(self.password_hash, str): log.warning(f"Hash not string for {self.email}."); return False
                return pwd_context.verify(password, self.password_hash)
            except Exception as e: log.error(f"Password check error for {self.email}: {e}", exc_info=True); return False
        def __repr__(self): return f'<User {self.email} (Credits: {self.credits})>'
else: st.error("DB connection failed earlier."); log.critical("Skipping User model def."); st.stop()

# === JSON File Utilities (Only for Trial Data) ===
def load_json(filepath: str) -> Dict[str, Any]:
    """Loads trial data from JSON file"""
    try:
        if not os.path.exists(filepath): log.warning(f"Trial file {filepath} missing, creating."); f = open(filepath, "w", encoding='utf-8'); json.dump({}, f); f.close(); return {}
        with open(filepath, "r", encoding='utf-8') as f: data = json.load(f)
        if not isinstance(data, dict): log.error(f"Trial file {filepath} invalid content."); return {}
        return data
    except json.JSONDecodeError as json_err: log.error(f"Trial file {filepath} decode error: {json_err}."); return {}
    except Exception as e: log.error(f"Load trial JSON {filepath} error: {e}", exc_info=True); return {}
def save_json(filepath: str, data: Dict[str, Any]) -> None:
    """Saves trial data to JSON file"""
    try:
        with open(filepath, "w", encoding='utf-8') as f: json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e: log.error(f"Save trial JSON {filepath} error: {e}", exc_info=True); st.warning("Could not save trial usage.")

# === Authentication Functions (DB Based) ===
def authenticate_user_db(email: str, password: str) -> Optional[Dict[str, Any]]:
    """Verifies user credentials against the database."""
    if not email or not password: log.warning("Auth attempt empty email/pass."); return None
    log.info(f"Attempting DB auth for: {email}")
    try:
        with db_config_app.app_context():
            # This query will fail if the 'users' table doesn't exist
            user = db.session.query(User).filter(db.func.lower(User.email) == email.lower()).first()
            if user and user.check_password(password):
                log.info(f"User '{email}' authenticated via DB.")
                try: user.last_login_at = datetime.now(timezone.utc); db.session.commit(); log.info(f"Updated last_login_at for {email}")
                except Exception as update_err: db.session.rollback(); log.warning(f"Could not update last_login_at for {email}: {update_err}", exc_info=True)
                return {"email": user.email, "credits": user.credits}
            elif user: log.warning(f"DB Auth failed for '{email}': Invalid password."); return None
            else: log.warning(f"DB Auth failed for '{email}': User not found."); return None
    except OperationalError as db_op_err:
        # This will catch the "relation 'users' does not exist" error if table wasn't created
        log.error(f"DB OperationalError during auth for {email}: {db_op_err}", exc_info=True)
        st.error("Login failed: Database connection issue or setup incomplete.") # Modified error
        return None
    except Exception as e: log.error(f"Unexpected error during DB auth for {email}: {e}", exc_info=True); st.error("Login failed: Server error."); return None

# === Session Initialization ===
def initialize_user_session(user_data: Dict[str, Any]) -> bool:
    """Initializes Streamlit session state using DB data (for credits) and trial file."""
    try:
        email = user_data.get("email");
        if not email: log.error("Session init failed: email missing."); return False
        st.session_state.logged_in = True; st.session_state.user_email = email
        is_trial = (email.lower() == TRIAL_EMAIL.lower()); st.session_state.is_trial_user = is_trial
        if is_trial:
            log.info(f"Initializing session for TRIAL user: {email}")
            trial_data = load_json(TRIAL_FILE_PATH); today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            user_trial_info = trial_data.get(email, {"date": today_str, "uses": 0})
            if user_trial_info.get("date") != today_str: log.info(f"Resetting trial uses for {email} day {today_str}."); user_trial_info = {"date": today_str, "uses": 0}; trial_data[email] = user_trial_info; save_json(TRIAL_FILE_PATH, trial_data)
            st.session_state.trial_uses_today = user_trial_info.get("uses", 0)
            if st.session_state.trial_uses_today >= TRIAL_DAILY_LIMIT: st.error(f"Login failed: Trial limit reached."); log.warning(f"Trial login blocked {email}, limit reached."); st.session_state.logged_in = False; return False
            st.session_state.credits = float('inf')
        else:
            log.info(f"Initializing session for PAID user: {email}")
            st.session_state.credits = user_data.get("credits", 0); st.session_state.trial_uses_today = 0
        log.info(f"Session initialized. Trial: {is_trial}, Credits/Uses: {st.session_state.credits if not is_trial else st.session_state.trial_uses_today}")
        return True
    except Exception as e: log.error(f"Session init error for {user_data.get('email', '??')}: {e}", exc_info=True); st.error("Session setup error."); return False

# === Usage Update ===
def update_usage_count(user_email: str, is_trial: bool) -> None:
    """Updates usage counts: Decrements credits in DB for paid users, increments uses in JSON for trial."""
    if not user_email:
        log.error("Cannot update usage: user_email is missing.")
        st.error("Error recording usage: Session data missing."); return
    try:
        if is_trial:
            current_uses = st.session_state.get("trial_uses_today", 0) + 1
            st.session_state.trial_uses_today = current_uses
            trial_data = load_json(TRIAL_FILE_PATH)
            user_trial_info = trial_data.get(user_email, {})
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            user_trial_info.update({"uses": current_uses, "date": today_str})
            trial_data[user_email] = user_trial_info
            save_json(TRIAL_FILE_PATH, trial_data)
            log.info(f"[Trial] Usage updated for {user_email}. Uses today: {current_uses}")
            st.toast(f"Trial use recorded ({current_uses}/{TRIAL_DAILY_LIMIT} today).", icon="⏳")
        else:
            with db_config_app.app_context():
                user = db.session.query(User).filter(db.func.lower(User.email) == user_email.lower()).first()
                if user:
                    if user.credits > 0:
                        user.credits -= 1; db.session.commit()
                        st.session_state.credits = user.credits
                        log.info(f"[Premium] Credit deducted for {user_email}. Remaining: {user.credits}")
                        st.toast("1 credit deducted.", icon="🪙")
                    else:
                        log.warning(f"Attempted to deduct credit for {user_email}, but credits were already zero.")
                        st.warning("Credit deduction skipped: Already at 0 credits.")
                else:
                    log.error(f"Cannot update credits for {user_email}: User not found in database during usage update.")
                    st.error("Error: Could not find user record to update credits.")
    except OperationalError as db_op_err:
         if not is_trial:
              try: # Attempt rollback safely
                  with db_config_app.app_context(): db.session.rollback()
              except Exception as rb_err: log.error(f"Rollback error: {rb_err}")
         log.error(f"Database OperationalError during usage update for {user_email}: {db_op_err}", exc_info=True)
         st.error("⚠️ Error connecting to database while updating usage count.")
    except Exception as e:
        # --- CORRECTED Nested Try/Except for Rollback ---
        if not is_trial:
             try:
                 with db_config_app.app_context():
                     db.session.rollback()
                     log.info(f"Rollback attempted for {user_email} due to usage update error.")
             except Exception as rb_err:
                 log.error(f"Rollback attempt itself failed for {user_email}: {rb_err}")
        # --- END CORRECTION ---
        log.error(f"Failed to update usage count for {user_email} (Trial={is_trial}): {e}", exc_info=True)
        st.error("⚠️ An unexpected error occurred while updating usage count.")

# === Login UI ===
def display_login_form():
    if "logged_in" not in st.session_state: st.session_state.logged_in = False
    if st.session_state.logged_in: return True
    try:
        logo_filepath = os.path.join(BASE_DIR, LOGO_PATH);
        if os.path.exists(logo_filepath): st.image(logo_filepath, width=150)
        else: log.info(f"Logo file {logo_filepath} not found.")
    except Exception as logo_err: log.warning(f"Could not load logo: {logo_err}")
    st.title("PDF Table Data Extractor + Multi Language Translator"); st.markdown("Please log in.")
    _, col2, _ = st.columns([1, 1.5, 1])
    with col2:
        with st.form("login_form"):
            st.subheader("🔐 Secure Login"); email = st.text_input("Email Address", key="login_email").strip(); password = st.text_input("Password", type="password", key="login_password"); submitted = st.form_submit_button("Sign In", use_container_width=True)
            if submitted:
                if not email or not password: st.warning("Enter email and password."); return False
                user_data = authenticate_user_db(email, password) # This might return None if DB error occurs
                if user_data:
                    if initialize_user_session(user_data): st.toast(f"Welcome back, {email}!", icon="🎉"); st.rerun()
                    else: st.session_state.logged_in = False; return False
                else:
                    # Generic error shown here if authenticate_user_db returned None (due to DB error or failed auth)
                    # Specific DB error message is shown within authenticate_user_db now.
                    if "last_auth_error_time" not in st.session_state or \
                       (datetime.now() - st.session_state.last_auth_error_time).total_seconds() > 2:
                         st.error("❌ Invalid email or password, or login service unavailable.")
                         st.session_state.last_auth_error_time = datetime.now()
                    log.warning(f"Failed login attempt for email: {email}")
                    return False
    return False

# === Initial Login Check ===
if not display_login_form(): st.stop()

# === Sidebar UI ===
with st.sidebar:
    st.title("⚙️ Account & Info"); st.divider(); user_email = st.session_state.get("user_email", "N/A"); st.write(f"👤 **User:** `{user_email}`")
    if st.session_state.get("is_trial_user", False):
        st.info("🧪 Free Trial Account Active", icon="🧪"); uses_today = st.session_state.get("trial_uses_today", 0); st.metric(label="Uses Today", value=f"{uses_today} / {TRIAL_DAILY_LIMIT}"); prog = uses_today / TRIAL_DAILY_LIMIT if TRIAL_DAILY_LIMIT > 0 else 0; st.progress(min(prog, 1.0))
        if uses_today >= TRIAL_DAILY_LIMIT: st.error("❌ Daily limit reached.")
    else:
        st.success("✅ Premium Account", icon="💳"); credits = st.session_state.get('credits', 0); st.metric("Remaining Credits", credits)
        if credits <= 0: st.error("❌ No credits remaining.")
        # st.link_button("Buy Credits", "YOUR_LINK", use_container_width=True)
    st.divider(); st.markdown("### 🔐 Session")
    if st.button("Log Out", use_container_width=True, key="logout_button"):
        log.info(f"User logged out: {user_email}"); keys_to_clear = ["logged_in", "user_email", "is_trial_user", "credits", "trial_uses_today"]
        for key in keys_to_clear:
            if key in st.session_state: del st.session_state[key]
        st.session_state.logged_in = False; st.toast("Logged out.", icon="👋"); st.rerun()
    st.divider(); st.caption(f"App Version: {APP_VERSION}"); st.caption(f"Support: {SUPPORT_EMAIL}")

# === Main App UI ===
st.title("📄 PDF Table Extractor Pro")
st.markdown("Effortlessly extract tables from PDF files to Excel. Translate content on the fly.")
st.divider()

# --- Step 1: Upload ---
st.subheader("1. Upload Your PDF")
uploaded_file = st.file_uploader("Click or drag to upload PDF.", type=["pdf"], key="pdf_uploader", label_visibility="collapsed")

# === Main Processing Logic ===
if uploaded_file:
    st.success(f"✅ File ready: '{uploaded_file.name}' ({uploaded_file.size / 1024:.1f} KB)"); st.divider()
    # --- Step 2: Configure Options ---
    st.subheader("2. Configure Extraction Options"); col_pages, col_translate = st.columns(2)
    with col_pages:
        st.markdown("📄 **Page Selection**"); pages_to_process = st.text_input("Pages", "all", key="pages_input", help="E.g., '1,3,5-7' or 'all'.").strip()
        if not re.fullmatch(r"^\s*(all|\d+(\s*-\s*\d+)?(\s*,\s*\d+(\s*-\s*\d+)?)*)\s*$", pages_to_process, re.IGNORECASE): st.error("Invalid page format."); st.stop()
    with col_translate:
        st.markdown("🌍 **Translation (Optional)**"); enable_translation = st.checkbox("Translate?", key="translate_cb", value=False); selected_lang_code = None; target_lang_name = None
        if enable_translation:
            full_language_names = {k: v.title() for k, v in SUPPORTED_LANGUAGES.items()}; lang_code_to_name = {v: k for k, v in full_language_names.items()}; sorted_lang_names = sorted(full_language_names.values())
            try: default_index = sorted_lang_names.index(DEFAULT_TRANSLATE_LANG_NAME.title()) # Ensure title case match
            except ValueError: default_index = 0; log.warning(f"Default language '{DEFAULT_TRANSLATE_LANG_NAME}' not in list.")
            selected_lang_name = st.selectbox("Target language:", sorted_lang_names, index=default_index, key="lang_select")
            selected_lang_code = lang_code_to_name.get(selected_lang_name)
            if selected_lang_code: target_lang_name = selected_lang_name; st.info(f"Translate to **{selected_lang_name}**.", icon="ℹ️")
            else: st.warning("Lang not found, disabling translation."); log.error(f"Code not found for lang: {selected_lang_name}"); enable_translation = False

    # --- Advanced Options Expander --- MODIFIED ---
    with st.expander("🔧 Advanced Extraction Settings (Optional)"):
        # Removed the flavor selectbox
        st.markdown("Adjust tolerance settings to fine-tune table detection (especially useful if rows or columns are merged/split incorrectly).")
        # Removed the caption about tolerances applying only to stream
        c1, c2 = st.columns(2)
        with c1:
            edge_tolerance = st.slider(
                "Edge Tolerance", 0, 1000, 200, step=50, # Keep slider
                help="Distance from page edges (points) to ignore. Increase if content near edges is missed."
            )
        with c2:
             row_tolerance = st.slider(
                 "Row Tolerance", 0, 50, 10, step=1, # Keep slider
                 help="Vertical distance (points) to group text lines into rows. Increase if rows are incorrectly split."
             )
    # --- END MODIFICATION ---
    st.divider()

    # --- Step 3: Process ---
    st.subheader("3. Process and Download"); process_button_label = f"🚀 Extract '{pages_to_process}' Pages"
    if enable_translation and target_lang_name: process_button_label += f" & Translate to {target_lang_name}" # Add translation info
    if st.button(process_button_label, key="process_button", type="primary", use_container_width=True):
        process_allowed = True; user_email = st.session_state.get("user_email"); is_trial = st.session_state.get("is_trial_user", False)
        if not user_email: st.error("Session error."); process_allowed = False
        elif is_trial:
            if st.session_state.get("trial_uses_today", 0) >= TRIAL_DAILY_LIMIT: st.error(f"Trial limit reached."); process_allowed = False
        elif st.session_state.get('credits', 0) <= 0: st.error("No credits remaining."); process_allowed = False
        if not process_allowed: st.stop()

        status_placeholder = st.empty(); progress_bar = st.progress(0.0, text="Initializing...")
        start_time = datetime.now()

        # --- Helper Functions (Exact copies from working script v1.9) ---
        @st.cache_data(show_spinner=False, ttl=3600) # Added ttl
        def translate_text(text, target_lang):
            original_text = str(text).strip();
            if not original_text: return original_text
            detected_lang = None
            try: detected_lang = detect(original_text);
            except LangDetectException: pass
            except Exception as detect_err_inner: log.warning(f"Inner lang detect error: {detect_err_inner}"); pass
            if detected_lang and detected_lang == target_lang: return original_text
            try: translated = GoogleTranslator(source='auto', target=target_lang).translate(original_text); return translated if translated else original_text
            except Exception as e: log.warning(f"Translate fail: '{original_text[:30]}...'. Err: {e}"); return original_text
        def translate_df_applymap(df, target_lang): # Renamed
             if target_lang: return df.copy().applymap(lambda x: translate_text(x, target_lang) if pd.notna(x) else x) # Use applymap
             return df
        def split_merged_rows(df):
            new_rows = []; df = df.fillna(''); cols = df.columns
            for _, row in df.iterrows():
                row_list = row.tolist()
                if any('\n' in str(cell) for cell in row_list):
                    parts = [str(cell).split('\n') for cell in row_list];
                    try: max_len = max(len(p) for p in parts) if parts else 0
                    except ValueError: max_len = 0
                    for i in range(max_len): new_rows.append([p[i] if i < len(p) else '' for p in parts])
                else: new_rows.append(row_list)
            return pd.DataFrame(new_rows, columns=cols) if new_rows else pd.DataFrame(columns=cols)
        # --- End Helpers ---

        try: # Main processing try block
            status_placeholder.info(f"⏳ Reading PDF (Pages: {pages_to_process})..."); progress_bar.progress(0.05, text="Reading PDF...")

            # --- MODIFIED: Hardcode flavor to 'stream' and include tolerances ---
            camelot_kwargs = {
                "pages": pages_to_process.lower(),
                "flavor": "stream", # Force stream
                "strip_text": '\n',
                "edge_tol": edge_tolerance, # Pass the value from the slider
                "row_tol": row_tolerance    # Pass the value from the slider
            }
            # --- END MODIFICATION ---

            log.info(f"Calling Camelot (Flavor: stream, Pages: {pages_to_process})") # Log forced flavor
            tables = camelot.read_pdf(uploaded_file, **camelot_kwargs)

            log.info(f"Camelot found {len(tables)} tables.") # Removed flavor from log
            if not tables:
                status_placeholder.warning(f"⚠️ No tables detected on pages '{pages_to_process}'. Try adjusting pages or tolerance settings.") # Adjusted message
                st.stop()

            total_tables = len(tables); status_placeholder.info(f"✅ Found {total_tables} tables. Preparing Excel..."); progress_bar.progress(0.1, text=f"Found {total_tables} tables...")
            output_buffer = BytesIO(); processed_sheets = []; table_counts_per_page = {}; has_content = False

            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                for i, table in enumerate(tables): # Table Processing Loop
                    current_progress = 0.1 + 0.7 * ((i + 1) / total_tables); page_num = table.page; table_counts_per_page[page_num] = table_counts_per_page.get(page_num, 0) + 1; table_num_on_page = table_counts_per_page[page_num]
                    base_sheet_name = f"Page_{page_num}_Table_{table_num_on_page}"
                    # Sheet name generation loop (fixed in v3.3)
                    sheet_name_candidate = base_sheet_name[:MAX_SHEET_NAME_LEN]; count = 1; temp_sheet_name = sheet_name_candidate
                    processed_lower = [name.lower() for name in processed_sheets]
                    while temp_sheet_name.lower() in processed_lower:
                        suffix = f"_{count}"; max_base_len = MAX_SHEET_NAME_LEN - len(suffix)
                        if max_base_len <= 0: temp_sheet_name = f"Sheet_Err_{i}"; log.warning(f"Sheet name fallback: {base_sheet_name} -> {temp_sheet_name}"); break
                        temp_sheet_name = base_sheet_name[:max_base_len] + suffix; count += 1
                        if count > 100: log.error(f"Unique sheet name fail: '{base_sheet_name}'"); temp_sheet_name = f"Sheet_Err_{i}"; break
                    sheet_name = temp_sheet_name

                    status_placeholder.info(f"⚙️ Processing {sheet_name} ({i+1}/{total_tables})..."); progress_bar.progress(current_progress, text=f"Processing {sheet_name}...")
                    df = table.df
                    if df.empty: log.info(f"Skip empty table: {sheet_name}"); continue
                    try:
                        df.columns = [str(col).strip() for col in df.columns]; df = split_merged_rows(df); df = df.astype(str)
                        if df.empty: log.info(f"Table empty post-clean: {sheet_name}"); continue
                        has_content = True
                        if selected_lang_code: df = translate_df_applymap(df, selected_lang_code) # Uses applymap function
                    except Exception as clean_err: log.error(f"Error clean/translate {sheet_name}: {clean_err}", exc_info=True); st.warning(f"⚠️ Skipped {sheet_name} due to error."); continue
                    df.to_excel(writer, sheet_name=sheet_name, index=False); processed_sheets.append(sheet_name)
                if not has_content: status_placeholder.warning("⚠️ No data extracted after cleaning."); st.stop()

                # Excel Formatting (Dynamic width)
                progress_bar.progress(0.85, text="Formatting Excel..."); status_placeholder.info("🎨 Formatting..."); workbook = writer.book
                for sheet_title in processed_sheets:
                    ws = workbook[sheet_title]
                    for row in ws.iter_rows():
                        for cell in row: cell.alignment = Alignment(wrap_text=True, vertical='top')
                    for col in ws.columns:
                        max_length = 8; column_letter = col[0].column_letter # Min width
                        for cell in col:
                            try:
                                cell_str = str(cell.value)
                                if cell.value and isinstance(cell.value, str) and '\n' in cell_str:
                                    length = max(len(line) for line in cell_str.split('\n'))
                                else: length = len(cell_str)
                            except: length = 0
                            if length > max_length: max_length = length
                        adjusted_width = min(max((max_length + 2) * 1.1, 10), 70) # Clamp width
                        try: ws.column_dimensions[column_letter].width = adjusted_width
                        except Exception as width_err: log.warning(f"Width fail col {column_letter}: {width_err}")

            output_buffer.seek(0); end_time = datetime.now(); duration = end_time - start_time; progress_bar.progress(1.0, text="Complete!"); status_placeholder.success(f"✅ Success! Processed {len(processed_sheets)} tables in {duration.total_seconds():.1f}s.")
            download_filename = f"extracted_{os.path.splitext(uploaded_file.name)[0]}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            st.download_button( label=f"📥 Download ({len(processed_sheets)} Sheets)", data=output_buffer, file_name=download_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_excel_button", use_container_width=True)

            # --- Usage Update Call ---
            update_usage_count(user_email, is_trial) # Calls the function defined earlier
            # --- End Usage Update Call ---

        # Error Handling --- MODIFIED ---
        except MemoryError: # Explicitly catch MemoryError
            status_placeholder.error(
                "❌ Processing Failed: Ran out of memory. "
                "This can happen with very large PDFs even using the 'stream' method. "
                "Try processing fewer pages at a time."
            )
            log.error("MemoryError occurred during processing.", exc_info=True)
            st.stop()
        except ImportError:
            status_placeholder.error("❌ Lib missing."); log.error("ImportError."); st.stop()
        except Exception as e:
            # Removed lattice/tolerance check
            if "relation \"user\" does not exist" in str(e).lower():
                # This error suggests the DB table wasn't created manually or via another script
                status_placeholder.error("❌ DB Error: User table missing. Database setup may be incomplete.");
                log.critical("DB schema error: 'user' table missing.", exc_info=True)
            elif "invalid pdf" in str(e).lower() or "file has not been decrypted" in str(e).lower():
                 status_placeholder.error(f"❌ PDF Error: Cannot read the PDF file. It might be corrupted or password-protected."); log.error(f"PDF read error: {e}")
            else:
                status_placeholder.error(f"❌ Unexpected processing error: {type(e).__name__}"); log.error(f"Processing failed: {e}", exc_info=True)
            st.stop()
        # --- END MODIFICATION ---

else: # No file uploaded
    st.info("👋 Welcome! Please upload PDF to start.")

# --- Footer ---
st.divider(); st.caption("© {} PDF Table Extractor Pro | Version: {} | Support: {}.".format(datetime.now().year, APP_VERSION, SUPPORT_EMAIL))
