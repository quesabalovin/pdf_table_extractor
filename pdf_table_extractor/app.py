# app.py (v3.1 - Cleaned Hybrid DB/JSON)
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
from datetime import datetime, timezone # Added timezone
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
    page_title="PDF Table Data Extractor + Multi Language Translator", # from working script
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)
# ----------------------------------------------------

# === Logging Setup ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')

# === Configuration Constants ===
APP_VERSION = "3.1-Cleaned-Hybrid" # New version marker
APP_TITLE = "PDF Table Extractor Pro"
SUPPORT_EMAIL = "lovinquesaba17@gmail.com"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# --- Trial File Config (JSON) ---
TRIAL_FILE_PATH = os.path.join(BASE_DIR, "trial_users.json") # JSON file ONLY for trial usage tracking
TRIAL_DAILY_LIMIT = 20
TRIAL_EMAIL = "freetrial@example.com" # The specific email that uses the JSON tracking
# --- ---
LOGO_PATH = "logo.png"
MAX_SHEET_NAME_LEN = 31
# Language List
SUPPORTED_LANGUAGES = { # Shortened for brevity, use your full list
    'en': 'English', 'es': 'Spanish', 'fr': 'French', 'de': 'German', 'it': 'Italian', 'pt': 'Portuguese',
    'ja': 'Japanese', 'ko': 'Korean', 'zh-cn': 'Chinese (Simplified)', 'ar': 'Arabic', 'ru': 'Russian'
    # Add your full list back here
}
DEFAULT_TRANSLATE_LANG_NAME = "English"

# === Database Setup (Uses DB for Auth & Paid Credits) ===
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

# === Password Hashing Context ===
try: pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto"); logging.info("Passlib context initialized.")
except Exception as pwd_err: st.error("FATAL ERROR: Security component failed."); logging.critical(f"Passlib init failed: {pwd_err}", exc_info=True); st.stop()

# === Database Model (User table definition) ===
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

# === JSON File Utilities (Only for Trial Data) ===
def load_json(filepath: str) -> Dict[str, Any]:
    """Loads trial data from JSON file"""
    try:
        if not os.path.exists(filepath): logging.warning(f"Trial file {filepath} missing, creating."); f = open(filepath, "w", encoding='utf-8'); json.dump({}, f); f.close(); return {}
        with open(filepath, "r", encoding='utf-8') as f: data = json.load(f)
        if not isinstance(data, dict): logging.error(f"Trial file {filepath} invalid content."); return {}
        return data
    except json.JSONDecodeError as json_err: logging.error(f"Trial file {filepath} decode error: {json_err}."); return {}
    except Exception as e: logging.error(f"Load trial JSON {filepath} error: {e}", exc_info=True); return {}
def save_json(filepath: str, data: Dict[str, Any]) -> None:
    """Saves trial data to JSON file"""
    try:
        with open(filepath, "w", encoding='utf-8') as f: json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e: logging.error(f"Save trial JSON {filepath} error: {e}", exc_info=True); st.warning("Could not save trial usage.")

# === Authentication Functions (DB Based) ===
def authenticate_user_db(email: str, password: str) -> Optional[Dict[str, Any]]:
    """Verifies user credentials against the database."""
    if not email or not password: logging.warning("Auth attempt empty email/pass."); return None
    logging.info(f"Attempting DB auth for: {email}")
    try:
        with db_config_app.app_context():
            user = db.session.query(User).filter(db.func.lower(User.email) == email.lower()).first()
            if user and user.check_password(password):
                logging.info(f"User '{email}' authenticated via DB.")
                try: user.last_login_at = datetime.now(timezone.utc); db.session.commit(); logging.info(f"Updated last_login_at for {email}")
                except Exception as update_err: db.session.rollback(); logging.warning(f"Could not update last_login_at for {email}: {update_err}", exc_info=True)
                return {"email": user.email, "credits": user.credits} # Pass email/credits to session init
            elif user: logging.warning(f"DB Auth failed for '{email}': Invalid password."); return None
            else: logging.warning(f"DB Auth failed for '{email}': User not found."); return None
    except OperationalError as db_op_err: logging.error(f"DB OperationalError during auth for {email}: {db_op_err}", exc_info=True); st.error("Login failed: DB connection issue."); return None
    except Exception as e: logging.error(f"Unexpected error during DB auth for {email}: {e}", exc_info=True); st.error("Login failed: Server error."); return None

# === Session Initialization (Handles DB credits OR JSON trial tracking) ===
def initialize_user_session(user_data: Dict[str, Any]) -> bool:
    """Initializes Streamlit session state using DB data (for credits) and trial file."""
    try:
        email = user_data.get("email");
        if not email: logging.error("Session init failed: email missing."); return False
        st.session_state.logged_in = True; st.session_state.user_email = email
        # --- Check if the authenticated user is the designated TRIAL user ---
        is_trial = (email.lower() == TRIAL_EMAIL.lower())
        st.session_state.is_trial_user = is_trial

        if is_trial:
            # --- TRIAL USER: Use JSON file for usage ---
            logging.info(f"Initializing session for TRIAL user: {email}")
            trial_data = load_json(TRIAL_FILE_PATH)
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            user_trial_info = trial_data.get(email, {"date": today_str, "uses": 0})
            if user_trial_info.get("date") != today_str: logging.info(f"Resetting trial uses for {email} day {today_str}."); user_trial_info = {"date": today_str, "uses": 0}; trial_data[email] = user_trial_info; save_json(TRIAL_FILE_PATH, trial_data)
            st.session_state.trial_uses_today = user_trial_info.get("uses", 0)
            if st.session_state.trial_uses_today >= TRIAL_DAILY_LIMIT: st.error(f"Login failed: Trial limit reached."); logging.warning(f"Trial login blocked {email}, limit reached."); st.session_state.logged_in = False; return False
            st.session_state.credits = float('inf') # Indicate trial for display
        else:
            # --- PAID USER: Use credits from database data ---
            logging.info(f"Initializing session for PAID user: {email}")
            st.session_state.credits = user_data.get("credits", 0)
            st.session_state.trial_uses_today = 0 # Not applicable

        logging.info(f"Session initialized. Trial: {is_trial}, Credits/Uses: {st.session_state.credits if not is_trial else st.session_state.trial_uses_today}")
        return True
    except Exception as e: logging.error(f"Session init error for {user_data.get('email', '??')}: {e}", exc_info=True); st.error("Session setup error."); return False

# === Login UI (Uses DB Auth) ===
def display_login_form():
    if "logged_in" not in st.session_state: st.session_state.logged_in = False
    if st.session_state.logged_in: return True
    try:
        logo_filepath = os.path.join(BASE_DIR, LOGO_PATH);
        if os.path.exists(logo_filepath): st.image(logo_filepath, width=150)
        else: logging.info(f"Logo file {logo_filepath} not found.")
    except Exception as logo_err: logging.warning(f"Could not load logo: {logo_err}")
    st.title("PDF Table Data Extractor + Multi Language Translator"); st.markdown("Please log in.")
    _, col2, _ = st.columns([1, 1.5, 1])
    with col2:
        with st.form("login_form"):
            st.subheader("🔐 Secure Login"); email = st.text_input("Email Address", key="login_email").strip(); password = st.text_input("Password", type="password", key="login_password"); submitted = st.form_submit_button("Sign In", use_container_width=True)
            if submitted:
                if not email or not password: st.warning("Enter email and password."); return False
                # --- Authenticate via Database ---
                user_data = authenticate_user_db(email, password)
                if user_data:
                    # --- Initialize Session (handles trial JSON vs DB credits) ---
                    if initialize_user_session(user_data): st.toast(f"Welcome back, {email}!", icon="🎉"); st.rerun()
                    else: st.session_state.logged_in = False; return False # Init failed (e.g. trial limit)
                else: # Auth failed
                    if "last_login_at" not in st.session_state: st.error("❌ Invalid email or password."); # Avoid double errors
                    logging.warning(f"Failed login attempt for email: {email}")
                    return False
    return False

# === Initial Login Check ===
if not display_login_form(): st.stop()

# === Sidebar UI (Reads session state set by init) ===
with st.sidebar:
    st.title("⚙️ Account & Info"); st.divider(); user_email = st.session_state.get("user_email", "N/A"); st.write(f"👤 **User:** `{user_email}`")
    # --- Display based on is_trial_user flag set during session init ---
    if st.session_state.get("is_trial_user", False):
        # --- Trial User Display (Uses trial_uses_today from JSON) ---
        st.info("🧪 Free Trial Account Active", icon="🧪"); uses_today = st.session_state.get("trial_uses_today", 0); st.metric(label="Uses Today", value=f"{uses_today} / {TRIAL_DAILY_LIMIT}"); prog = uses_today / TRIAL_DAILY_LIMIT if TRIAL_DAILY_LIMIT > 0 else 0; st.progress(min(prog, 1.0))
        if uses_today >= TRIAL_DAILY_LIMIT: st.error("❌ Daily limit reached.")
    else:
        # --- Paid User Display (Uses credits from DB via session state) ---
        st.success("✅ Premium Account", icon="💳"); credits = st.session_state.get('credits', 0); st.metric("Remaining Credits", credits)
        if credits <= 0: st.error("❌ No credits remaining.")
        # st.link_button("Buy Credits", "YOUR_LINK", use_container_width=True)
    st.divider(); st.markdown("### 🔐 Session")
    if st.button("Log Out", use_container_width=True, key="logout_button"):
        logging.info(f"User logged out: {user_email}"); keys_to_clear = ["logged_in", "user_email", "is_trial_user", "credits", "trial_uses_today"]
        for key in keys_to_clear:
            if key in st.session_state: del st.session_state[key]
        st.session_state.logged_in = False; st.toast("Logged out.", icon="👋"); st.rerun()
    st.divider(); st.caption(f"App Version: {APP_VERSION}"); st.caption(f"Support: {SUPPORT_EMAIL}")


# === Main App UI (From working script) ===
st.title("📄 PDF Table Extractor Pro")
st.markdown("Effortlessly extract tables from PDF files to Excel. Translate content on the fly.")
st.divider()

# --- Step 1: Upload ---
st.subheader("1. Upload Your PDF")
uploaded_file = st.file_uploader("Click or drag to upload PDF.", type=["pdf"], key="pdf_uploader", label_visibility="collapsed")

# === Main Processing Logic (From working script, uses DB/JSON usage update at end) ===
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
            try: default_index = sorted_lang_names.index(DEFAULT_TRANSLATE_LANG_NAME)
            except ValueError: default_index = 0
            selected_lang_name = st.selectbox("Target language:", sorted_lang_names, index=default_index, key="lang_select")
            selected_lang_code = lang_code_to_name.get(selected_lang_name)
            if selected_lang_code: target_lang_name = selected_lang_name; st.info(f"Translate to **{selected_lang_name}**.", icon="ℹ️")
            else: st.warning("Lang not found, disabling translation."); enable_translation = False
    with st.expander("🔧 Advanced PDF Parsing Settings (Optional)"):
        st.markdown("Adjust if default results inconsistent."); camelot_flavor = st.selectbox( "Parsing Method", ['stream', 'lattice'], index=0, help="'stream' vs 'lattice'."); st.caption("_Tolerances apply only to 'stream'._")
        c1, c2 = st.columns(2)
        with c1: edge_tolerance = st.slider("Edge Tol (Stream)", 0, 1000, 200, step=50, help="Page edge distance.")
        with c2: row_tolerance = st.slider( "Row Tol (Stream)", 0, 50, 10, step=1, help="Vertical text grouping.")
    st.divider()

    # --- Step 3: Process ---
    st.subheader("3. Process and Download"); process_button_label = f"🚀 Extract '{pages_to_process}' Pages"
    if st.button(process_button_label, key="process_button", type="primary", use_container_width=True):
        process_allowed = True; user_email = st.session_state.get("user_email"); is_trial = st.session_state.get("is_trial_user", False) # Get state
        if not user_email: st.error("Session error."); process_allowed = False
        elif is_trial:
            if st.session_state.get("trial_uses_today", 0) >= TRIAL_DAILY_LIMIT: st.error(f"Trial limit reached."); process_allowed = False
        elif st.session_state.get('credits', 0) <= 0: st.error("No credits remaining."); process_allowed = False
        if not process_allowed: st.stop()

        status_placeholder = st.empty(); progress_bar = st.progress(0.0, text="Initializing...")
        start_time = datetime.now()

        # --- Helper Functions (Exact copies from working script) ---
        @st.cache_data(show_spinner=False)
        def translate_text(text, target_lang):
            original_text = str(text).strip();
            if not original_text: return original_text
            try: detected_lang = detect(original_text);
            # Use specific exception
            except LangDetectException: pass
            except Exception as detect_err_inner: logging.warning(f"Inner lang detect error: {detect_err_inner}"); pass
            if 'detected_lang' in locals() and detected_lang == target_lang: return original_text
            try: translated = GoogleTranslator(source='auto', target=target_lang).translate(original_text); return translated if translated else original_text
            except Exception as e: logging.warning(f"Translate fail: '{original_text[:30]}...'. Err: {e}"); return original_text
        def translate_df_silent(df, target_lang):
             if target_lang: return df.copy().applymap(lambda x: translate_text(x, target_lang)) # Uses applymap
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
            return pd.DataFrame(new_rows, columns=cols)
        # --- End Helpers ---

        try: # Main processing try block
            status_placeholder.info(f"⏳ Reading PDF (Pages: {pages_to_process})..."); progress_bar.progress(0.05, text="Reading PDF...")
            camelot_kwargs = {"pages": pages_to_process.lower(), "flavor": camelot_flavor, "strip_text": '\n'}
            if camelot_flavor == 'stream': camelot_kwargs['edge_tol'] = edge_tolerance; camelot_kwargs['row_tol'] = row_tolerance
            tables = camelot.read_pdf(uploaded_file, **camelot_kwargs)
            logging.info(f"Camelot found {len(tables)} tables (pg: {pages_to_process}, flav: {camelot_flavor}).")
            if not tables: status_placeholder.warning(f"⚠️ No tables detected (pg='{pages_to_process}', flav='{camelot_flavor}')."); st.stop()

            total_tables = len(tables); status_placeholder.info(f"✅ Found {total_tables} tables. Preparing Excel..."); progress_bar.progress(0.1, text=f"Found {total_tables} tables...")
            output_buffer = BytesIO(); processed_sheets = []; table_counts_per_page = {}; has_content = False

            # --- Table Processing Loop (Exact copy from working script) ---
            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                for i, table in enumerate(tables):
                    current_progress = 0.1 + 0.7 * ((i + 1) / total_tables); page_num = table.page; table_counts_per_page[page_num] = table_counts_per_page.get(page_num, 0) + 1; table_num_on_page = table_counts_per_page[page_num]
                    base_sheet_name = f"Page_{page_num}_Table_{table_num_on_page}"; sheet_name = base_sheet_name[:MAX_SHEET_NAME_LEN]; count = 1; temp_sheet_name = sheet_name
                    while temp_sheet_name in processed_sheets:
                        suffix = f"_{count}"; max_len = MAX_SHEET_NAME_LEN - len(suffix)
                        if max_len <=0: temp_sheet_name = f"Sheet_Err_{i}"; break
                        temp_sheet_name = base_sheet_name[:max_len] + suffix; count += 1
                        if count > 100: temp_sheet_name = f"Sheet_Err_{i}"; break
                    sheet_name = temp_sheet_name
                    status_placeholder.info(f"⚙️ Processing {sheet_name} ({i+1}/{total_tables})..."); progress_bar.progress(current_progress, text=f"Processing {sheet_name}...")
                    df = table.df
                    if df.empty: logging.info(f"Skip empty table: {sheet_name}"); continue
                    try:
                        df.columns = [str(col).strip() for col in df.columns]; df = split_merged_rows(df); df = df.astype(str)
                        if df.empty: logging.info(f"Table empty post-clean: {sheet_name}"); continue
                        has_content = True
                        if selected_lang_code: df = translate_df_silent(df, selected_lang_code) # Uses applymap via helper
                    except Exception as clean_err: logging.error(f"Error clean/translate {sheet_name}: {clean_err}", exc_info=True); st.warning(f"⚠️ Skipped {sheet_name} due to error."); continue
                    df.to_excel(writer, sheet_name=sheet_name, index=False); processed_sheets.append(sheet_name)
                if not has_content: status_placeholder.warning("⚠️ No data extracted after cleaning."); st.stop()
                # --- End Table Loop ---

                # --- Excel Formatting (Exact copy - dynamic width) ---
                progress_bar.progress(0.85, text="Formatting Excel..."); status_placeholder.info("🎨 Formatting..."); workbook = writer.book
                for sheet_title in processed_sheets:
                    ws = workbook[sheet_title]
                    for row in ws.iter_rows():
                        for cell in row: cell.alignment = Alignment(wrap_text=True, vertical='top')
                    for col in ws.columns:
                        max_length = 0; column_letter = col[0].column_letter
                        for cell in col:
                            try: cell_str = str(cell.value); length = max(len(line) for line in cell_str.split('\n')) if cell.value and isinstance(cell.value, str) and '\n' in cell_str else len(cell_str)
                            except: length = 0
                            if length > max_length: max_length = length
                        adjusted_width = min(max((max_length + 2) * 1.1, 12), 60)
                        try: ws.column_dimensions[column_letter].width = adjusted_width
                        except Exception as width_err: logging.warning(f"Width fail col {column_letter}: {width_err}")
                # --- End Formatting ---

            output_buffer.seek(0); end_time = datetime.now(); duration = end_time - start_time; progress_bar.progress(1.0, text="Complete!"); status_placeholder.success(f"✅ Success! Processed {len(processed_sheets)} tables in {duration.total_seconds():.1f}s.")
            download_filename = f"extracted_{os.path.splitext(uploaded_file.name)[0]}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            st.download_button( label=f"📥 Download ({len(processed_sheets)} Sheets)", data=output_buffer, file_name=download_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_excel_button", use_container_width=True)

            # --- Usage Update (Uses DB for paid, JSON for trial) ---
            # Uses update_usage_count defined earlier which handles the split
            update_usage_count(user_email, is_trial)
            # --- End Usage Update ---

        # Error Handling (from working script + DB check)
        except ImportError: status_placeholder.error("❌ Lib missing."); logging.error("ImportError."); st.stop()
        except Exception as e:
            if "edge_tol,row_tol cannot be used with flavor='lattice'" in str(e): status_placeholder.error("❌ Config Error: Tolerances incompatible with 'lattice'."); logging.error(f"Config error: {e}")
            elif "relation \"user\" does not exist" in str(e).lower(): status_placeholder.error("❌ DB Error: User table missing."); logging.critical("DB schema error: 'user' missing.", exc_info=True)
            else: status_placeholder.error(f"❌ Unexpected processing error."); logging.error(f"Processing failed: {e}", exc_info=True)
            st.stop()

else: # No file uploaded
    st.info("👋 Welcome! Please upload PDF to start.")

# --- Footer ---
st.divider(); st.caption("© {} PDF Table Extractor Pro | Support: {}.".format(datetime.now().year, SUPPORT_EMAIL))
