# app.py (Streamlit App - DB Auth/Credits + JSON Trial - Refined v2.9-diag-trans)
import streamlit as st
import pandas as pd
import camelot
from deep_translator import GoogleTranslator # Still import it, just don't use in diagnostic func
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
try:
    DetectorFactory.seed = 0
except NameError:
    logging.warning("Could not seed DetectorFactory for langdetect.")
# ----------------------

# --- Page Config ---
st.set_page_config(
    page_title="PDF Table Extractor Pro v2.9-diag-trans", # Updated version
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)
# ----------------------------------------------------

# === Logging Setup ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')

# === Configuration Constants ===
TRIAL_DAILY_LIMIT = 20
TRIAL_EMAIL = "freetrial@example.com"
APP_VERSION = "2.9-diag-trans" # Reflects translation diagnostic
APP_TITLE = "PDF Table Extractor Pro"
SUPPORT_EMAIL = "lovinquesaba17@gmail.com"
SUPPORTED_LANGUAGES = { # Keep your list
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
DEFAULT_CAMELOT_FLAVOR='stream'
DEFAULT_EDGE_TOLERANCE = 200
DEFAULT_ROW_TOLERANCE = 10
MAX_SHEET_NAME_LEN = 31
FIXED_COLUMN_WIDTH = 35
LOGO_PATH = "logo.png"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRIAL_FILE_PATH = os.path.join(BASE_DIR, "trial_users.json")

# === Database Setup === (Identical to v2.8)
db_config_app = Flask(__name__)
DATABASE_URL = os.environ.get('DATABASE_URL')
FLASK_SECRET_KEY = os.environ.get('FLASK_SECRET_KEY', 'default-secret-key-change-me')
if not DATABASE_URL: st.error("FATAL ERROR: DATABASE_URL missing."); logging.critical("DATABASE_URL missing."); st.stop()
if FLASK_SECRET_KEY == 'default-secret-key-change-me': logging.warning("Using default FLASK_SECRET_KEY.")
db_config_app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
db_config_app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {"pool_pre_ping": True, "pool_recycle": 300}
db_config_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db_config_app.config['SECRET_KEY'] = FLASK_SECRET_KEY
db = None
try: db = SQLAlchemy(db_config_app); logging.info("SQLAlchemy initialized.")
except Exception as db_init_err: st.error("FATAL ERROR: DB initialization failed."); logging.critical(f"SQLAlchemy init failed: {db_init_err}", exc_info=True); st.stop()

# === Password Hashing Context === (Identical to v2.8)
try: pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto"); logging.info("Passlib context initialized.")
except Exception as pwd_err: st.error("FATAL ERROR: Security component failed."); logging.critical(f"Passlib init failed: {pwd_err}", exc_info=True); st.stop()

# === Database Model === (Identical to v2.8)
if db:
    class User(db.Model):
        id = db.Column(db.Integer, primary_key=True); email = db.Column(db.String(120), unique=True, nullable=False, index=True); password_hash = db.Column(db.String(128), nullable=False); credits = db.Column(db.Integer, nullable=False, default=100); created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)); last_login_at = db.Column(db.DateTime(timezone=True), nullable=True)
        def check_password(self, password: str) -> bool:
            try:
                if not isinstance(self.password_hash, str): logging.warning(f"Hash not string for {self.email}."); return False
                return pwd_context.verify(password, self.password_hash)
            except Exception as e: logging.error(f"Password check error for {self.email}: {e}", exc_info=True); return False
        def __repr__(self): return f'<User {self.email} (Credits: {self.credits})>'
else: st.error("DB connection failed earlier."); logging.critical("Skipping User model def."); st.stop()

# === JSON File Utilities === (Identical to v2.8)
def load_trial_json(filepath: str) -> Dict[str, Any]:
    try:
        if not os.path.exists(filepath): logging.warning(f"Trial file {filepath} missing, creating."); f = open(filepath, "w", encoding='utf-8'); json.dump({}, f); f.close(); return {}
        with open(filepath, "r", encoding='utf-8') as f: data = json.load(f)
        if not isinstance(data, dict): logging.error(f"Trial file {filepath} invalid content."); return {}
        return data
    except json.JSONDecodeError as json_err: logging.error(f"Trial file {filepath} decode error: {json_err}."); return {}
    except Exception as e: logging.error(f"Load trial JSON {filepath} error: {e}", exc_info=True); return {}
def save_trial_json(filepath: str, data: Dict[str, Any]) -> None:
    try:
        with open(filepath, "w", encoding='utf-8') as f: json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e: logging.error(f"Save trial JSON {filepath} error: {e}", exc_info=True); st.warning("Could not save trial usage.")

# === Authentication & Session Logic === (Identical to v2.8)
def authenticate_user_db(email: str, password: str) -> Optional[Dict[str, Any]]:
    if not email or not password: logging.warning("Auth attempt empty email/pass."); return None
    logging.info(f"Attempting DB auth for: {email}")
    try:
        with db_config_app.app_context():
            user = db.session.query(User).filter(db.func.lower(User.email) == email.lower()).first()
            if user and user.check_password(password):
                logging.info(f"User '{email}' authenticated via DB.")
                try: user.last_login_at = datetime.now(timezone.utc); db.session.commit(); logging.info(f"Updated last_login_at for {email}")
                except Exception as update_err: db.session.rollback(); logging.warning(f"Could not update last_login_at for {email}: {update_err}", exc_info=True)
                return {"email": user.email, "credits": user.credits}
            elif user: logging.warning(f"DB Auth failed for '{email}': Invalid password."); return None
            else: logging.warning(f"DB Auth failed for '{email}': User not found."); return None
    except OperationalError as db_op_err: logging.error(f"DB OperationalError during auth for {email}: {db_op_err}", exc_info=True); st.error("Login failed: DB connection issue."); return None
    except Exception as e: logging.error(f"Unexpected error during DB auth for {email}: {e}", exc_info=True); st.error("Login failed: Server error."); return None
def initialize_user_session(user_data: Dict[str, Any]) -> bool:
    try:
        email = user_data.get("email");
        if not email: logging.error("Session init failed: email missing."); return False
        st.session_state.logged_in = True; st.session_state.user_email = email
        is_trial = (email.lower() == TRIAL_EMAIL.lower()); st.session_state.is_trial_user = is_trial
        if is_trial:
            trial_data = load_trial_json(TRIAL_FILE_PATH); today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            user_trial_info = trial_data.get(email, {"date": today_str, "uses": 0})
            if user_trial_info.get("date") != today_str: logging.info(f"Resetting trial uses for {email} day {today_str}."); user_trial_info = {"date": today_str, "uses": 0}; trial_data[email] = user_trial_info; save_trial_json(TRIAL_FILE_PATH, trial_data)
            st.session_state.trial_uses_today = user_trial_info.get("uses", 0)
            if st.session_state.trial_uses_today >= TRIAL_DAILY_LIMIT: st.error(f"Login failed: Trial limit reached."); logging.warning(f"Trial login blocked {email}, limit reached."); st.session_state.logged_in = False; return False
            st.session_state.credits = float('inf')
        else: st.session_state.credits = user_data.get("credits", 0); st.session_state.trial_uses_today = 0
        logging.info(f"Session initialized for {email}. Trial: {is_trial}, Credits/Uses: {st.session_state.credits if not is_trial else st.session_state.trial_uses_today}")
        return True
    except Exception as e: logging.error(f"Session init error for {user_data.get('email', '??')}: {e}", exc_info=True); st.error("Session setup error."); return False

# === Login UI === (Identical to v2.8)
def display_login_form() -> bool:
    if "logged_in" not in st.session_state: st.session_state.logged_in = False
    if st.session_state.logged_in: return True
    try:
        logo_filepath = os.path.join(BASE_DIR, LOGO_PATH);
        if os.path.exists(logo_filepath): st.image(logo_filepath, width=150)
        else: logging.info(f"Logo file {logo_filepath} not found.")
    except Exception as logo_err: logging.warning(f"Could not load logo: {logo_err}")
    st.title(f"Welcome to {APP_TITLE}"); st.markdown("Please log in.")
    _, col2, _ = st.columns([1, 1.5, 1])
    with col2:
        with st.form("login_form"):
            st.subheader("🔐 Secure Login"); email = st.text_input("Email", key="login_email").strip(); password = st.text_input("Password", type="password", key="login_password"); submitted = st.form_submit_button("Sign In", use_container_width=True)
            if submitted:
                if not email or not password: st.warning("Enter email and password."); return False
                user_data = authenticate_user_db(email, password)
                if user_data:
                    if initialize_user_session(user_data): st.toast("Login successful!", icon="✅"); st.rerun()
                    else: st.session_state.logged_in = False; return False
                else:
                    if "last_login_at" not in st.session_state: st.error("❌ Invalid email or password."); return False
    return False

# === Sidebar UI === (Identical to v2.8)
def display_sidebar() -> None:
    with st.sidebar:
        st.title("⚙️ Account & Info"); st.divider(); user_email = st.session_state.get("user_email", "N/A"); st.write(f"👤 **User:** `{user_email}`")
        if st.session_state.get("is_trial_user", False):
            st.info("🧪 Free Trial Account", icon="🧪"); uses_today = st.session_state.get("trial_uses_today", 0); st.metric(label="Uses Today", value=f"{uses_today} / {TRIAL_DAILY_LIMIT}"); prog = uses_today / TRIAL_DAILY_LIMIT if TRIAL_DAILY_LIMIT > 0 else 0; st.progress(min(prog, 1.0))
            if uses_today >= TRIAL_DAILY_LIMIT: st.error("❌ Daily limit reached.")
        else:
            st.success("✅ Premium Account", icon="💳"); credits = st.session_state.get('credits', 0); st.metric("Remaining Credits", credits)
            if credits <= 0: st.error("❌ No credits remaining.")
            # st.link_button("Buy Credits", "YOUR_LINK", use_container_width=True)
        st.divider(); st.markdown("### 🔐 Session")
        if st.button("Log Out", use_container_width=True, key="logout_button"):
            logging.info(f"User logged out: {user_email}"); keys_to_clear = ["logged_in", "user_email", "is_trial_user", "credits", "trial_uses_today"]
            for key in keys_to_clear:
                if key in st.session_state: del st.session_state[key]
            st.session_state.logged_in = False; st.toast("Logged out.", icon="👋"); st.rerun()
        st.divider(); st.caption(f"Version: {APP_VERSION}"); st.caption(f"Support: {SUPPORT_EMAIL}")

# === Usage Update === (Identical to v2.8)
def update_usage_count(user_email: str, is_trial: bool) -> None:
    if not user_email: logging.error("Usage update failed: email missing."); st.error("Usage record error."); return
    try:
        if is_trial:
            current_uses = st.session_state.get("trial_uses_today", 0) + 1; st.session_state.trial_uses_today = current_uses
            trial_data = load_trial_json(TRIAL_FILE_PATH); user_trial_info = trial_data.get(user_email, {})
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d"); user_trial_info.update({"uses": current_uses, "date": today_str})
            trial_data[user_email] = user_trial_info; save_trial_json(TRIAL_FILE_PATH, trial_data)
            logging.info(f"[Trial] Usage update {user_email}. Uses: {current_uses}"); st.toast(f"Trial use recorded ({current_uses}/{TRIAL_DAILY_LIMIT}).", icon="⏳")
        else:
            with db_config_app.app_context():
                user = db.session.query(User).filter(db.func.lower(User.email) == user_email.lower()).first()
                if user:
                    if user.credits > 0: user.credits -= 1; db.session.commit(); st.session_state.credits = user.credits; logging.info(f"[Premium] Credit update {user_email}. Remaining: {user.credits}"); st.toast("1 credit deducted.", icon="🪙")
                    else: logging.warning(f"Credit deduct attempt {user_email} at 0 credits."); st.warning("Credit deduction skipped: 0 credits.")
                else: logging.error(f"Credit update failed {user_email}: User not found."); st.error("Credit update failed: User not found.")
    except OperationalError as db_op_err:
         if not is_trial:
              with db_config_app.app_context(): db.session.rollback()
         logging.error(f"DB OperationalError during usage update {user_email}: {db_op_err}", exc_info=True); st.error("⚠️ DB Error updating usage count.")
    except Exception as e:
        if not is_trial:
             try:
                 with db_config_app.app_context(): db.session.rollback()
             except Exception as rb_err: logging.error(f"Rollback error: {rb_err}")
        logging.error(f"Usage update failed {user_email} (Trial={is_trial}): {e}", exc_info=True); st.error("⚠️ Usage update error.")

# === PDF Processing & Helper Functions ===

# ***** MODIFIED FUNCTION *****
@st.cache_data(show_spinner=False)
def translate_text(_text: Any, target_lang: str) -> str:
    """[DIAGNOSTIC v2.9] Temporarily bypasses actual translation to isolate deep-translator."""
    original_text = str(_text).strip() # Ensure input is string
    if not original_text or not target_lang:
        return original_text

    # --- DIAGNOSTIC: Bypass actual translation ---
    logging.info(f"[DIAG v2.9] translate_text called for: '{original_text[:50]}...' -> Lang: {target_lang}. Returning modified original.")
    # Return original text appended with the target language code for verification
    return f"{original_text} [{target_lang.upper()}]"
    # --- END DIAGNOSTIC ---

    # # --- Original translation logic (Commented out for diagnostics) ---
    # # Optional: Basic language detection to avoid translating if already correct
    # try:
    #     detected_lang = detect(original_text)
    #     # Normalize language codes if necessary (e.g., 'en-US' vs 'en')
    #     if detected_lang.split('-')[0] == target_lang.split('-')[0]:
    #         # logging.info(f"Skipping translation, detected lang '{detected_lang}' matches target '{target_lang}'")
    #         return original_text
    # except LangDetectException:
    #     # logging.debug(f"LangDetectException for text: {original_text[:50]}...")
    #     pass # Ignore detection errors, proceed to translation attempt
    # except Exception as detect_err:
    #     logging.warning(f"Language detection failed for text snippet: {detect_err}")

    # # Attempt translation
    # try:
    #     # Consider adding retry logic or checking language support if errors are frequent
    #     # logging.debug(f"Attempting translation: '{original_text[:50]}...' to {target_lang}")
    #     translated = GoogleTranslator(source='auto', target=target_lang).translate(original_text)
    #     # logging.debug(f"Translation result: '{translated}'")
    #     return translated if translated else original_text # Return original if translation returns None/empty
    # except Exception as translate_err:
    #     logging.warning(f"Translation failed for text snippet '{original_text[:50]}...' to lang '{target_lang}'. Error: {translate_err}")
    #     return original_text # Return original text on translation failure
    # # --- End Original ---
# ***** END MODIFIED FUNCTION *****

# translate_dataframe remains the same, it calls the modified translate_text
def translate_dataframe(df: pd.DataFrame, target_lang: str) -> pd.DataFrame:
    """Translates text in each cell of a DataFrame using .map and the (potentially modified) translate_text."""
    if target_lang and not df.empty:
        logging.info(f"Translating DataFrame to {target_lang} (using modified translate_text)...")
        translated_df = df.copy().map(lambda x: translate_text(x, target_lang))
        logging.info("DataFrame pseudo-translation finished.")
        return translated_df
    return df

# split_merged_rows remains the same (Identical to v2.8)
def split_merged_rows(df: pd.DataFrame) -> pd.DataFrame:
    new_rows = []; df_filled = df.fillna(''); original_columns = df_filled.columns
    for index, row in df_filled.iterrows():
        row_list = row.tolist()
        if any('\n' in str(cell) for cell in row_list):
            parts = [str(cell).split('\n') for cell in row_list]
            try: max_len = max(len(p) for p in parts) if parts else 0
            except ValueError: logging.warning(f"ValueError max_len row {index}"); max_len = 0
            for i in range(max_len): new_rows.append([p[i] if i < len(p) else '' for p in parts])
        else: new_rows.append(row_list)
    if not new_rows: return pd.DataFrame(columns=original_columns)
    else: return pd.DataFrame(new_rows, columns=original_columns)

# generate_unique_sheet_name remains the same (Identical to v2.8)
def generate_unique_sheet_name(base_name: str, existing_names: List[str]) -> str:
    sanitized_base = re.sub(r'[\\/*?:\[\]]', '_', base_name); sheet_name = sanitized_base[:MAX_SHEET_NAME_LEN]
    count = 1; temp_name = sheet_name
    while temp_name.lower() in [name.lower() for name in existing_names]:
        suffix = f"_{count}"; max_base_len = MAX_SHEET_NAME_LEN - len(suffix)
        if max_base_len <= 0:
            fallback_suffix = f"_{datetime.now().strftime('%f')}"; max_fallback_len = MAX_SHEET_NAME_LEN - len(fallback_suffix)
            temp_name = sanitized_base[:max_fallback_len] + fallback_suffix; temp_name = temp_name[:MAX_SHEET_NAME_LEN]
            logging.warning(f"Sheet name fallback for '{base_name}'"); break
        temp_name = sanitized_base[:max_base_len] + suffix; count += 1
        if count > 999: logging.error(f"Unique sheet name fail '{base_name}'"); temp_name = f"Err_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"[:MAX_SHEET_NAME_LEN]; break
    return temp_name

# format_excel_sheet uses FIXED_COLUMN_WIDTH (Identical to v2.8)
def format_excel_sheet(ws) -> None:
    """Applies formatting: wrap text, alignment, and FIXED column width."""
    for row in ws.iter_rows():
        for cell in row: cell.alignment = Alignment(wrap_text=True, vertical='top', horizontal='left')
    for col in ws.columns:
        column_letter = col[0].column_letter
        try: ws.column_dimensions[column_letter].width = FIXED_COLUMN_WIDTH
        except Exception as width_err: logging.warning(f"Could not set fixed width {FIXED_COLUMN_WIDTH} for col {column_letter}: {width_err}")

# === Main Application Logic === (Mostly Identical to v2.8, note diagnostic logging)
def main_app() -> None:
    """Renders the main UI and handles PDF processing."""
    st.title(f"📄 {APP_TITLE}"); st.markdown("Extract tables, translate content, download as Excel."); st.divider()
    st.subheader("1. Upload Your PDF"); uploaded_file = st.file_uploader("Select PDF", type="pdf", key="pdf_uploader", label_visibility="collapsed")
    if not uploaded_file: st.info("👋 Upload PDF to begin."); st.stop()
    st.success(f"✅ Ready: '{uploaded_file.name}'"); st.divider()
    st.subheader("2. Configure Extraction"); col_pages, col_translate = st.columns(2)
    with col_pages:
        st.markdown("📄 **Pages**"); pages_to_process = st.text_input("Specify pages", "all", key="pages_input", help="'1,3,5-7' or 'all'.").strip().lower()
        if not re.fullmatch(r"^\s*(all|\d+(\s*-\s*\d+)?(\s*,\s*\d+(\s*-\s*\d+)?)*)\s*$", pages_to_process): st.error("Invalid page format."); st.stop()
    with col_translate:
        st.markdown("🌍 **Translate**"); enable_translation = st.checkbox("Translate content?", key="translate_cb", value=False); selected_lang_code = None; target_lang_name = None
        if enable_translation:
            lang_name_to_code = {v: k for k, v in SUPPORTED_LANGUAGES.items()}; sorted_lang_names = sorted(SUPPORTED_LANGUAGES.values())
            try: default_index = sorted_lang_names.index(DEFAULT_TRANSLATE_LANG_NAME)
            except ValueError: logging.warning(f"Default lang '{DEFAULT_TRANSLATE_LANG_NAME}' missing."); default_index = 0
            selected_lang_name = st.selectbox("Translate to:", sorted_lang_names, index=default_index, key="lang_select")
            selected_lang_code = lang_name_to_code.get(selected_lang_name)
            if selected_lang_code: target_lang_name = selected_lang_name; st.info(f"Content translation to **{selected_lang_name}** enabled.", icon="ℹ️")
            else: st.warning("Lang not found, translation disabled."); enable_translation = False
    with st.expander("🔧 Advanced Settings"):
        st.markdown("Adjust parsing method/tolerances."); camelot_flavor = st.selectbox("Parsing Method", ['stream', 'lattice'], index=['stream', 'lattice'].index(DEFAULT_CAMELOT_FLAVOR), help="'stream' vs 'lattice'."); st.caption("_Tolerances apply only to 'stream'._")
        c1, c2 = st.columns(2)
        with c1: edge_tolerance = st.slider("Edge Tol", 0, 1000, DEFAULT_EDGE_TOLERANCE, 50, help="(Stream only)", disabled=(camelot_flavor != 'stream'))
        with c2: row_tolerance = st.slider("Row Tol", 0, 50, DEFAULT_ROW_TOLERANCE, 1, help="(Stream only)", disabled=(camelot_flavor != 'stream'))
    st.divider()
    st.subheader("3. Process & Download"); process_button_label = f"🚀 Extract ('{pages_to_process}' pages, method: {camelot_flavor})"
    if st.button(process_button_label, key="process_button", type="primary", use_container_width=True):
        user_email = st.session_state.get("user_email"); is_trial = st.session_state.get("is_trial_user", False); process_allowed = True
        if not user_email: st.error("❌ Session error."); logging.error("Processing abort: email missing."); process_allowed = False; st.stop()
        if is_trial:
            if st.session_state.get("trial_uses_today", 0) >= TRIAL_DAILY_LIMIT: st.error(f"❌ Trial limit reached."); process_allowed = False
        elif st.session_state.get('credits', 0) <= 0: st.error("❌ No credits remaining."); process_allowed = False
        if not process_allowed: logging.warning(f"Processing denied {user_email} (Trial={is_trial})."); st.stop()

        status_placeholder = st.empty(); progress_bar = st.progress(0.0, text="Initializing...")
        start_time = datetime.now()
        try:
            status_placeholder.info(f"⏳ Reading PDF '{uploaded_file.name}'..."); progress_bar.progress(0.05, text="Reading PDF...")
            camelot_kwargs = {"pages": pages_to_process, "flavor": camelot_flavor, "strip_text": '\n'}
            if camelot_flavor == 'stream': camelot_kwargs.update({'edge_tol': edge_tolerance, 'row_tol': row_tolerance})
            logging.info(f"Calling Camelot with args: {camelot_kwargs}")
            tables = camelot.read_pdf(uploaded_file, **camelot_kwargs)

            if not tables: status_placeholder.warning(f"⚠️ No tables found by Camelot (pages='{pages_to_process}', method='{camelot_flavor}')."); logging.warning(f"Camelot found 0 tables for {user_email} with settings: {camelot_kwargs}"); st.stop()
            total_tables = len(tables); logging.info(f"Camelot found {total_tables} potential tables for {user_email}.")
            status_placeholder.info(f"✅ Found {total_tables} tables. Processing..."); progress_bar.progress(0.1, text=f"Processing {total_tables} tables...")
            output_buffer = BytesIO(); processed_sheets: List[str] = []; table_counts_per_page: Dict[int, int] = {}; has_content = False; interval = max(1, total_tables // 10)

            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                for i, table in enumerate(tables):
                    if i % interval == 0 or i == total_tables - 1: progress = 0.1 + 0.7 * ((i + 1) / total_tables); progress_bar.progress(progress, text=f"Table {i+1}/{total_tables}...")
                    page_num = table.page; table_counts_per_page[page_num] = table_counts_per_page.get(page_num, 0) + 1
                    table_num = table_counts_per_page[page_num]; base_sheet_name = f"Page_{page_num}_Table_{table_num}"; sheet_name = generate_unique_sheet_name(base_sheet_name, processed_sheets)
                    df = table.df

                    if df.empty: logging.info(f"Table {i+1} ({sheet_name}): Raw Camelot DF empty. Skipping."); continue
                    logging.info(f"--- RAW DF Content: {sheet_name} ---");
                    try: logging.info(df.head().to_string())
                    except Exception as log_err: logging.info(f"Could not log raw df head: {log_err}")
                    logging.info("--- End RAW DF ---")

                    try:
                        df.columns = [str(c).strip() for c in df.columns]; df = split_merged_rows(df); df = df.astype(str)
                        if df.empty: logging.info(f"Table {i+1} ({sheet_name}): DF empty *after* cleaning. Skipping."); continue
                        has_content = True
                        # This will now call the MODIFIED translate_text if enabled
                        if enable_translation and selected_lang_code: df = translate_dataframe(df, selected_lang_code)
                        df.to_excel(writer, sheet_name=sheet_name, index=False); processed_sheets.append(sheet_name); logging.info(f"Processed sheet: {sheet_name}")
                    except Exception as process_err: logging.error(f"Error processing table {i+1} ({sheet_name}): {process_err}", exc_info=True); st.warning(f"⚠️ Skipped table Page {page_num} (T{table_num}) error."); continue

            if not has_content: status_placeholder.warning("⚠️ No data extracted. Detected tables empty after processing."); logging.warning(f"Processing done {user_email}, has_content=False."); st.stop()

            progress_bar.progress(0.85, text="Formatting..."); status_placeholder.info("🎨 Formatting Excel..."); workbook = writer.book
            for sheet_title in processed_sheets:
                try: format_excel_sheet(workbook[sheet_title])
                except Exception as fmt_e: logging.warning(f"Format sheet '{sheet_title}' error: {fmt_e}")
            output_buffer.seek(0); duration = datetime.now() - start_time; progress_bar.progress(1.0, text="Complete!"); status_placeholder.success(f"✅ Processed {len(processed_sheets)} tables in {duration.total_seconds():.1f}s.")
            download_filename = f"extracted_{os.path.splitext(uploaded_file.name)[0]}_{datetime.now():%Y%m%d_%H%M}.xlsx"
            st.download_button(label=f"📥 Download Excel ({len(processed_sheets)} Sheets)", data=output_buffer, file_name=download_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_btn", use_container_width=True)
            update_usage_count(user_email, is_trial)

        except ImportError as imp_err: status_placeholder.error(f"❌ Error: Missing library ({imp_err})."); logging.critical(f"ImportError: {imp_err}", exc_info=True); st.stop()
        except OperationalError as db_op_err: status_placeholder.error("❌ DB Error during usage update."); logging.error(f"DB OpError during usage update: {db_op_err}", exc_info=True)
        except Exception as e:
            if "edge_tol,row_tol cannot be used with flavor='lattice'" in str(e): status_placeholder.error("❌ Config Error: Tolerances incompatible with 'lattice'."); logging.error(f"Camelot config error: {e}")
            elif "relation \"user\" does not exist" in str(e).lower(): status_placeholder.error("❌ DB Error: User table missing."); logging.critical("DB schema error: 'user' missing.", exc_info=True)
            else: status_placeholder.error("❌ Unexpected processing error."); logging.error(f"Unhandled exception: {e}", exc_info=True)
            st.stop()

# === Entry Point ===
if __name__ == "__main__":
    if display_login_form(): display_sidebar(); main_app()
    st.divider(); st.caption(f"© {datetime.now().year} {APP_TITLE}")
