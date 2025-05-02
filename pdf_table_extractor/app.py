# app.py (Streamlit App - Merged DB Auth/Credits + JSON Trial)
import streamlit as st
import pandas as pd
import camelot
from deep_translator import GoogleTranslator
from langdetect import detect, DetectorFactory, LangDetectException
from io import BytesIO
from openpyxl.styles import Alignment
import json # Kept for trial_users.json
import os
import re
from datetime import datetime, timezone # Added timezone
import logging

# --- Required for DB Interaction ---
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
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
    page_title="PDF Table Extractor Pro v2.6", # Updated title slightly
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)
# ----------------------------------------------------

# === Logging Setup ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')

# === Configuration Constants (Define directly or use config.py) ===
TRIAL_DAILY_LIMIT = 20
TRIAL_EMAIL = "freetrial@example.com" # Case-insensitive comparison used later
APP_VERSION = "2.6-DB-Merge"
APP_TITLE = "PDF Table Extractor Pro"
SUPPORT_EMAIL = "lovinquesaba17@gmail.com"
# Using the extensive language list
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
DEFAULT_TRANSLATE_LANG = "English"
DEFAULT_CAMELOT_FLAVOR='stream'
DEFAULT_EDGE_TOLERANCE = 200
DEFAULT_ROW_TOLERANCE = 10
MAX_SHEET_NAME_LEN = 31
LOGO_PATH = "logo.png" # Relative path
# --- JSON File Paths (Only for Trial Logic) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRIAL_FILE = os.path.join(BASE_DIR, "trial_users.json") # Path to trial user data
# --- ---

# === Database Setup ===
db_config_app = Flask(__name__) # Dummy Flask app for config
db_config_app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
db_config_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db_config_app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'dummy-key-for-streamlit')

db = None # Initialize db as None
if not db_config_app.config['SQLALCHEMY_DATABASE_URI']:
    st.error("FATAL ERROR: Application requires database configuration (DATABASE_URL). Contact support.")
    logging.critical("DATABASE_URL environment variable missing.")
    st.stop()
else:
    try:
        db = SQLAlchemy(db_config_app)
    except Exception as db_init_err:
        st.error(f"FATAL ERROR: Database connection failed. Please contact support.")
        logging.critical(f"SQLAlchemy initialization failed: {db_init_err}", exc_info=True)
        st.stop()

# === Password Hashing Context (Must match gumroad-server) ===
try:
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
except Exception as pwd_err:
     st.error("FATAL ERROR: Security component failed. Contact support.")
     logging.critical(f"Passlib context initialization failed: {pwd_err}", exc_info=True)
     st.stop()

# === Database Model (MUST match gumroad-server's User model) ===
# Ensures SQLAlchemy knows the table structure when querying
# Only define if db initialization succeeded
if db:
    class User(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        email = db.Column(db.String(120), unique=True, nullable=False, index=True)
        password_hash = db.Column(db.String(128), nullable=False)
        credits = db.Column(db.Integer, nullable=False, default=100) # Default potentially set by gumroad_server
        created_at = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
        last_login_at = db.Column(db.DateTime, nullable=True)
        # is_trial = db.Column(db.Boolean, default=False, nullable=False) # Optional: Add later

        def check_password(self, password):
            try:
                if not isinstance(self.password_hash, str): return False
                return pwd_context.verify(password, self.password_hash)
            except Exception as e:
                logging.error(f"Password check error for {self.email}: {e}")
                return False

        def __repr__(self):
            return f'<User {self.email}>'
else:
    # If DB connection failed, we cannot proceed with DB-dependent logic
    st.error("Database connection failed. Cannot proceed with authentication.")
    st.stop()


# === JSON File Utilities (Only for Trial Data currently) ===
def load_trial_json(filename: str) -> Dict[str, Any]:
    """Loads trial data from a JSON file."""
    try:
        filepath = os.path.join(BASE_DIR, filename)
        if not os.path.exists(filepath):
            logging.warning(f"Trial file not found: {filepath}. Creating.")
            with open(filepath, "w", encoding='utf-8') as f: json.dump({}, f)
            return {}
        with open(filepath, "r", encoding='utf-8') as f: return json.load(f)
    except Exception as e: logging.error(f"Failed to load trial JSON {filename}: {e}"); return {}

def save_trial_json(filename: str, data: Dict[str, Any]) -> None:
    """Saves trial data to a JSON file."""
    try:
        filepath = os.path.join(BASE_DIR, filename)
        with open(filepath, "w", encoding='utf-8') as f: json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e: logging.error(f"Failed to save trial JSON {filename}: {e}")

# === Authentication & Session Logic (Using Database + Trial JSON) ===
def authenticate_user_db(email: str, password: str) -> Optional[Dict[str, Any]]:
    """Verifies user credentials against the database."""
    if not email or not password: return None
    logging.debug(f"Attempting DB authentication for {email}")
    try:
        with db_config_app.app_context(): # Required for DB session
            user = User.query.filter(db.func.lower(User.email) == email.lower()).first() # Case-insensitive lookup
            if user and user.check_password(password):
                logging.info(f"User '{email}' authenticated successfully via DB.")
                # Update last login time
                try: user.last_login_at = datetime.now(timezone.utc); db.session.commit()
                except Exception as update_err: db.session.rollback(); logging.warning(f"Could not update last_login_at: {update_err}")
                # Return essential data
                return {"email": user.email, "credits": user.credits}
            else: logging.warning(f"DB Auth failed: {'Invalid password' if user else 'User not found'} for '{email}'."); return None
    except Exception as e:
        logging.error(f"Database error during authentication for {email}: {e}", exc_info=True)
        st.error("Login failed due to a server error."); return None

def initialize_user_session(user_data: Dict[str, Any]) -> bool:
    """Initializes Streamlit session state using DB data and trial file."""
    try:
        email = user_data["email"]
        st.session_state.logged_in = True
        st.session_state.user_email = email

        # --- Determine Trial Status (based on email) ---
        is_trial = (email.lower() == TRIAL_EMAIL.lower())
        st.session_state.is_trial_user = is_trial
        # --- ---

        if is_trial:
            # --- Load Trial Usage from JSON ---
            trial_data = load_trial_json(TRIAL_FILE)
            today_str = datetime.today().strftime("%Y-%m-%d")
            user_trial_info = trial_data.get(email, {"date": today_str, "uses": 0})
            if user_trial_info.get("date") != today_str: # Reset daily count
                user_trial_info = {"date": today_str, "uses": 0}
                trial_data[email] = user_trial_info; save_trial_json(TRIAL_FILE, trial_data)
            st.session_state.trial_uses_today = user_trial_info.get("uses", 0)
            if st.session_state.trial_uses_today >= TRIAL_DAILY_LIMIT:
                 st.error(f"Trial limit reached."); return False # Fail init if limit reached
            st.session_state.credits = float('inf') # Visual indicator
            # --- ---
        else:
            # --- Load Credits from DB Data passed in user_data ---
            st.session_state.credits = user_data.get("credits", 0)
            # --- ---
        logging.info(f"Session initialized for {email}. Trial: {is_trial}.")
        return True
    except Exception as e:
        logging.error(f"Session init error for {user_data.get('email', '??')}: {e}", exc_info=True)
        st.error("Error initializing session."); return False

# === Login UI (Uses DB Auth) ===
def display_login_form() -> bool:
    """Displays login form, uses database authentication."""
    if "logged_in" not in st.session_state: st.session_state.logged_in = False
    if st.session_state.logged_in: return True

    logo_filepath = os.path.join(BASE_DIR, LOGO_PATH)
    if os.path.exists(logo_filepath): st.image(logo_filepath, width=150)

    st.title(f"Welcome to {APP_TITLE}")
    st.markdown("Please log in.")
    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col2:
        with st.form("login_form"):
            st.subheader("🔐 Secure Login")
            email = st.text_input("Email Address", key="login_email").strip()
            password = st.text_input("Password", type="password", key="login_password")
            submitted = st.form_submit_button("Sign In", use_container_width=True)
            if submitted:
                if not email or not password: st.warning("Enter email and password."); return False
                user_data = authenticate_user_db(email, password) # Use DB Auth
                if user_data: # Auth Success
                    if initialize_user_session(user_data): # Session Init Success
                        st.toast(f"Login successful!", icon="✅"); st.rerun()
                    else: # Session Init Failed
                        st.session_state.logged_in = False; return False
                else: # Auth Failed
                    st.error("❌ Invalid email or password."); return False
    return False

# === Sidebar UI ===
def display_sidebar() -> None:
    """Displays sidebar using data from session state."""
    with st.sidebar:
        st.title("⚙️ Account & Info"); st.divider()
        user_email = st.session_state.get("user_email", "N/A"); st.write(f"👤 **User:** `{user_email}`")
        if st.session_state.get("is_trial_user", False):
            st.info("🧪 Free Trial Account", icon="🧪")
            uses_today = st.session_state.get("trial_uses_today", 0)
            st.metric(label="Uses Today", value=f"{uses_today} / {TRIAL_DAILY_LIMIT}")
            prog = uses_today / TRIAL_DAILY_LIMIT if TRIAL_DAILY_LIMIT > 0 else 0; st.progress(min(prog, 1.0))
            if uses_today >= TRIAL_DAILY_LIMIT: st.error("❌ Daily limit reached.")
        else:
            st.success("✅ Premium Account", icon="💳")
            credits = st.session_state.get('credits', 0)
            st.metric("Remaining Credits", credits)
            if credits <= 0: st.error("❌ No credits remaining.")
            # st.link_button("Buy More Credits", "YOUR_GUMROAD_LINK", use_container_width=True)
        st.divider(); st.markdown("### 🔐 Session")
        if st.button("Log Out", use_container_width=True, key="logout_button"):
            logging.info(f"User logged out: {user_email}")
            for key in list(st.session_state.keys()): del st.session_state[key]
            st.session_state.logged_in = False; st.toast("Logged out.", icon="👋"); st.rerun()
        st.divider(); st.caption(f"Version: {APP_VERSION}")

# === Usage Update (Handles DB for Paid, JSON for Trial) ===
def update_usage_count(user_email: str, is_trial: bool) -> None:
    """Updates usage counts: DB for paid users, JSON for trial."""
    try:
        if is_trial:
            # --- Update Trial JSON file ---
            current_uses = st.session_state.get("trial_uses_today", 0) + 1
            st.session_state.trial_uses_today = current_uses
            trial_data = load_trial_json(TRIAL_FILE)
            user_trial_info = trial_data.get(user_email, {})
            user_trial_info.update({"uses": current_uses, "date": datetime.today().strftime("%Y-%m-%d")})
            trial_data[user_email] = user_trial_info; save_trial_json(TRIAL_FILE, trial_data)
            logging.info(f"[Trial] Usage updated for {user_email}. Uses: {current_uses}")
            st.toast("Trial use recorded.", icon="⏳")
            # --- ---
        else:
            # --- Update Credits in Database ---
            with db_config_app.app_context(): # Need context
                user = User.query.filter_by(email=user_email).first()
                if user:
                    current_credits = user.credits - 1
                    user.credits = max(current_credits, 0) # Prevent negative
                    db.session.commit() # Save change
                    st.session_state.credits = user.credits # Update session state
                    logging.info(f"[Premium] Credit updated for {user_email}. Remaining: {user.credits}")
                    st.toast("1 credit deducted.", icon="🪙")
                else: logging.error(f"Cannot update credits for {user_email}: User not found."); st.error("Error: User not found.")
            # --- ---
    except Exception as e:
        if not is_trial: # Only rollback DB if it was a DB operation
             with db_config_app.app_context(): db.session.rollback()
        logging.error(f"Failed usage update for {user_email}: {e}", exc_info=True)
        st.error("⚠️ Error updating usage count.")


# === PDF Processing & Helper Functions ===
# (Keep translate_text, translate_dataframe, split_merged_rows,
#  generate_unique_sheet_name, format_excel_sheet as defined in previous versions)
@st.cache_data(show_spinner=False)
def translate_text(text, target_lang):
    original_text = str(text).strip();
    if not original_text: return original_text
    try:
        detected_lang = detect(original_text)
        if detected_lang == target_lang: return original_text
    except LangDetectException: pass
    except Exception as e: logging.warning(f"Lang detect error: {e}")
    try:
        translated = GoogleTranslator(source='auto', target=target_lang).translate(original_text)
        return translated if translated else original_text
    except Exception as e:
        logging.warning(f"Translate failed: '{original_text[:30]}...'. Err: {e}")
        return original_text

def translate_dataframe(df, target_lang):
    if target_lang and not df.empty:
        return df.copy().applymap(lambda x: translate_text(x, target_lang))
    return df

def split_merged_rows(df):
    new_rows = []; df_filled = df.fillna(''); cols = df_filled.columns
    for _, row in df_filled.iterrows():
        row_list = row.tolist()
        if any('\n' in str(cell) for cell in row_list):
            parts = [str(cell).split('\n') for cell in row_list];
            try: max_len = max(len(p) for p in parts) if parts else 0
            except ValueError: max_len = 0
            for i in range(max_len): new_rows.append([p[i] if i < len(p) else '' for p in parts])
        else: new_rows.append(row_list)
    return pd.DataFrame(new_rows, columns=cols)

def generate_unique_sheet_name(base_name, existing_names):
    sheet_name = base_name[:MAX_SHEET_NAME_LEN]; count = 1; temp_name = sheet_name
    while temp_name in existing_names:
        suffix = f"_{count}"; max_base_len = MAX_SHEET_NAME_LEN - len(suffix)
        if max_base_len <= 0: temp_name = f"S_{datetime.now().microsecond}"[:MAX_SHEET_NAME_LEN]; break
        temp_name = base_name[:max_base_len] + suffix; count += 1
        if count > 100: temp_name = f"S_Err_{datetime.now().microsecond}"[:MAX_SHEET_NAME_LEN]; break
    return temp_name

def format_excel_sheet(ws):
    for row in ws.iter_rows():
        for cell in row: cell.alignment = Alignment(wrap_text=True, vertical='top', horizontal='left')
    for col in ws.columns:
        max_length = 0; column_letter = col[0].column_letter
        for cell in col:
            try:
                cell_str = str(cell.value); length = max(len(line) for line in cell_str.split('\n')) if cell.value and isinstance(cell.value, str) and '\n' in cell_str else len(cell_str)
                if length > max_length: max_length = length
            except: pass
        adjusted_width = min(max((max_length + 2) * 1.2, 12), 70)
        ws.column_dimensions[column_letter].width = adjusted_width

# === Main Application Logic ===
def main_app() -> None:
    """Renders the main UI and handles PDF processing."""
    st.title(f"📄 {APP_TITLE}"); st.markdown("Extract tables, translate, fine-tune.")
    st.divider()

    # Step 1: Upload
    st.subheader("1. Upload PDF"); uploaded_file = st.file_uploader("Select PDF", type="pdf", key="pdf_uploader", label_visibility="collapsed")
    if not uploaded_file: st.info("👋 Upload a PDF to start."); st.stop()
    st.success(f"✅ Ready: '{uploaded_file.name}'"); st.divider()

    # Step 2: Configure
    st.subheader("2. Configure Extraction"); col_pages, col_translate = st.columns(2)
    with col_pages:
        st.markdown("📄 **Pages**"); pages_to_process = st.text_input("Pages to Process", "all", key="pages_input", help="'1,3,5-7' or 'all'.").strip().lower()
        if not re.fullmatch(r"^\s*(all|\d+(\s*-\s*\d+)?(\s*,\s*\d+(\s*-\s*\d+)?)*)\s*$", pages_to_process): st.error("Invalid page format."); st.stop()
    with col_translate:
        st.markdown("🌍 **Translate**"); enable_translation = st.checkbox("Translate content?", key="translate_cb", value=False)
        selected_lang_code = None
        if enable_translation:
            sorted_lang_names = sorted(SUPPORTED_LANGUAGES.values()); lang_name_to_code = {v: k for k, v in SUPPORTED_LANGUAGES.items()}
            try: default_index = sorted_lang_names.index(DEFAULT_TRANSLATE_LANG)
            except ValueError: default_index = 0
            selected_lang_name = st.selectbox("To language:", sorted_lang_names, index=default_index, key="lang_select")
            selected_lang_code = lang_name_to_code[selected_lang_name]; st.info(f"Translate to **{selected_lang_name}**.", icon="ℹ️")
    with st.expander("🔧 Advanced Settings"):
        st.markdown("Adjust if needed. Hover '?' for info."); camelot_flavor = st.selectbox("Method", ['stream', 'lattice'], index=['stream', 'lattice'].index(DEFAULT_CAMELOT_FLAVOR), help="'stream'(whitespace) vs 'lattice'(lines).")
        st.caption("_Tolerance sliders apply only to 'stream'._"); c1, c2 = st.columns(2);
        with c1: edge_tolerance = st.slider("Edge Tolerance", 0, 1000, DEFAULT_EDGE_TOLERANCE, 50, help="Margin sensitivity.")
        with c2: row_tolerance = st.slider("Row Tolerance", 0, 50, DEFAULT_ROW_TOLERANCE, 1, help="Vertical grouping sensitivity.")
    st.divider()

    # Step 3: Process
    st.subheader("3. Process & Download"); process_button_label = f"🚀 Extract ('{pages_to_process}' pages)"
    if st.button(process_button_label, key="process_button", type="primary", use_container_width=True):
        user_email = st.session_state.get("user_email"); is_trial = st.session_state.get("is_trial_user", False); process_allowed = True
        if is_trial and st.session_state.get("trial_uses_today", 0) >= TRIAL_DAILY_LIMIT: st.error("❌ Trial limit reached."); process_allowed = False
        elif not is_trial and st.session_state.get('credits', 0) <= 0: st.error("❌ No credits."); process_allowed = False
        if not user_email: st.error("❌ Session error."); process_allowed = False
        if not process_allowed: st.stop()

        status_placeholder = st.empty(); progress_bar = st.progress(0.0, text="Initializing...")
        start_time = datetime.now()
        try:
            status_placeholder.info(f"⏳ Reading PDF..."); progress_bar.progress(0.05)
            camelot_kwargs = {"pages": pages_to_process,"flavor": camelot_flavor,"strip_text": '\n'}
            if camelot_flavor == 'stream': camelot_kwargs.update({'edge_tol': edge_tolerance, 'row_tol': row_tolerance})
            tables = camelot.read_pdf( uploaded_file, **camelot_kwargs)
            if not tables: status_placeholder.warning(f"⚠️ No tables found."); st.stop()
            total_tables = len(tables); logging.info(f"Found {total_tables} tables for {user_email}.")
            status_placeholder.info(f"✅ Found {total_tables} tables. Processing..."); progress_bar.progress(0.1)

            output_buffer = BytesIO(); processed_sheets: List[str] = []; table_counts = {}; has_content = False; interval = max(1, total_tables // 5)
            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                for i, table in enumerate(tables):
                    if i % interval == 0 or i == total_tables - 1: progress_bar.progress(0.1 + 0.7 * ((i + 1) / total_tables), text=f"Table {i+1}/{total_tables}...")
                    pg = table.page; table_counts[pg] = table_counts.get(pg, 0) + 1; num = table_counts[pg]
                    base = f"Page_{pg}_Table_{num}"; sheet = generate_unique_sheet_name(base, processed_sheets); df = table.df
                    if df.empty: continue
                    try:
                        df.columns = [str(c).strip() for c in df.columns]; df = split_merged_rows(df); df = df.astype(str)
                        if df.empty: continue; has_content = True
                        if selected_lang_code: df = translate_dataframe(df, selected_lang_code) # Use updated function
                    except Exception as e: logging.error(f"Err processing {sheet}: {e}",exc_info=True); st.warning(f"⚠️ Skipped {sheet} (error)."); continue
                    df.to_excel(writer, sheet_name=sheet, index=False); processed_sheets.append(sheet)
                if not has_content: status_placeholder.warning("⚠️ No data extracted."); st.stop()
                progress_bar.progress(0.85, text="Formatting..."); status_placeholder.info("🎨 Formatting Excel...")
                workbook = writer.book; [format_excel_sheet(workbook[s]) for s in processed_sheets]

            output_buffer.seek(0); duration = datetime.now() - start_time
            progress_bar.progress(1.0, text="Complete!"); status_placeholder.success(f"✅ Processed {len(processed_sheets)} tables ({duration.total_seconds():.1f}s).")
            dl_fn = f"extracted_{os.path.splitext(uploaded_file.name)[0]}_{datetime.now():%Y%m%d_%H%M}.xlsx"
            st.download_button(f"📥 Download ({len(processed_sheets)} Sheets)", output_buffer, dl_fn, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_btn", use_container_width=True)

            update_usage_count(user_email, is_trial) # Call the updated usage function

        except Exception as e: # Catch processing errors
            if "relation \"user\" does not exist" in str(e): status_placeholder.error("❌ DB Error: User table missing. Please contact support."); logging.critical("User table not found!", exc_info=True)
            elif "OperationalError" in str(type(e).__name__): status_placeholder.error("❌ DB Error. Please try again later."); logging.error(f"DB Error: {e}", exc_info=True)
            elif "edge_tol,row_tol cannot be used with flavor='lattice'" in str(e): status_placeholder.error("❌ Config Error: Tolerances incompatible with 'lattice'.")
            else: status_placeholder.error("❌ Unexpected processing error."); logging.error(f"Processing failed: {e}", exc_info=True)
            st.stop()

# === Entry Point ===
if __name__ == "__main__":
    # display_login_form handles auth and stops execution if login fails/not logged in
    if display_login_form():
        display_sidebar()    # Display sidebar because user is logged in
        main_app()           # Display main app content because user is logged in

    # Footer displayed at the end if script execution reaches here
    st.divider()
    st.caption(f"© {datetime.now().year} {APP_TITLE} | Support: {SUPPORT_EMAIL}")
