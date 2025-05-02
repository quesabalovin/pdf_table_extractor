# app.py
import streamlit as st
import pandas as pd
import camelot
from deep_translator import GoogleTranslator
from langdetect import detect, DetectorFactory
from io import BytesIO
from openpyxl.styles import Alignment
import json
import os
import re
from datetime import datetime
import logging

# --- Seed langdetect ---
try:
    DetectorFactory.seed = 0
except NameError:
    logging.warning("Could not seed DetectorFactory for langdetect.")
# ----------------------

# --- Page Config (MUST be first Streamlit command) ---
st.set_page_config(
    page_title="PDF Table Data Extractor + Multi Language Translator",
    page_icon="📄",  # Add a relevant icon
    layout="wide",
    initial_sidebar_state="expanded"
)
# ----------------------------------------------------

# === Logging Setup ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# === File Constants & Utilities ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(BASE_DIR, "credentials.json")
CREDIT_FILE = os.path.join(BASE_DIR, "credits.json")
TRIAL_FILE = os.path.join(BASE_DIR, "trial_users.json")
TRIAL_DAILY_LIMIT = 20

def load_json(filename):
    try:
        if not os.path.exists(filename):
            logging.warning(f"File not found: {filename}. Creating.")
            with open(filename, "w", encoding='utf-8') as f: json.dump({}, f)
        with open(filename, "r", encoding='utf-8') as f: return json.load(f)
    except Exception as e:
        logging.error(f"Error loading {filename}: {e}")
        return {}

def save_json(filename, data):
    try:
        with open(filename, "w", encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Error saving {filename}: {e}")

def load_users(): return load_json(CREDENTIALS_FILE)
USERS = load_users()

# === Login UI ===
def login():
    if "logged_in" not in st.session_state: st.session_state.logged_in = False
    if not st.session_state.logged_in:
        logo_path = os.path.join(BASE_DIR, "logo.png")
        if os.path.exists(logo_path): st.image(logo_path, width=150)
        else: logging.warning(f"Optional logo file not found at {logo_path}, skipping display.")

        st.title("PDF Table Data Extractor + Multi Language Translator")
        st.markdown("Please log in to access the tool.")
        col1, col2, col3 = st.columns([1, 1.5, 1])
        with col2:
            with st.form("login_form"):
                st.subheader("🔐 Secure Login")
                email = st.text_input("Email Address", key="login_email")
                password = st.text_input("Password", type="password", key="login_password")
                submitted = st.form_submit_button("Sign In", use_container_width=True)
                if submitted:
                    current_users = load_users()
                    # Simplified check: Ensure email exists and password matches
                    if email in current_users and current_users[email].get("password") == password:
                        st.session_state.logged_in = True
                        st.session_state.user_email = email
                        logging.info(f"User logged in: {email}")
                        is_trial = (email == "freetrial@example.com")
                        st.session_state.is_trial_user = is_trial
                        if is_trial:
                            trial_data = load_json(TRIAL_FILE)
                            today = datetime.today().strftime("%Y-%m-%d")
                            user_trial_info = trial_data.get(email, {"date": today, "uses": 0})
                            if user_trial_info.get("date") != today:
                                user_trial_info = {"date": today, "uses": 0}
                                trial_data[email] = user_trial_info; save_json(TRIAL_FILE, trial_data)
                            if user_trial_info.get("uses", 0) >= TRIAL_DAILY_LIMIT:
                                st.error(f"❌ Free trial daily limit ({TRIAL_DAILY_LIMIT}) reached."); st.session_state.logged_in = False; return False
                            st.session_state.trial_uses_today = user_trial_info.get("uses", 0)
                            st.session_state.user_credits = float('inf') # Indicate trial for potential display logic
                        else:
                            credit_data = load_json(CREDIT_FILE)
                            if email not in credit_data:
                                default_credits = current_users[email].get("credits", 10)
                                credit_data[email] = default_credits; save_json(CREDIT_FILE, credit_data)
                                logging.info(f"Initialized credits for {email} to {default_credits}")
                            st.session_state.user_credits = credit_data.get(email, 0)
                        st.toast(f"Welcome back, {email}!", icon="🎉")
                        st.rerun()
                    else:
                        st.error("❌ Invalid email or password. Please try again.")
                        logging.warning(f"Failed login attempt for email: {email}")
        return False
    return True

if not login(): st.stop()

# === Sidebar UI ===
with st.sidebar:
    st.title("⚙️ Account & Info")
    st.divider()
    user_email = st.session_state.get("user_email", "N/A")
    st.write(f"👤 **User:** `{user_email}`")

    if st.session_state.get("is_trial_user", False):
        st.info("🧪 Free Trial Account Active", icon="🧪")
        uses_today = st.session_state.get("trial_uses_today", 0)
        st.metric(label="Uses Today", value=f"{uses_today} / {TRIAL_DAILY_LIMIT}")
        prog = uses_today / TRIAL_DAILY_LIMIT if TRIAL_DAILY_LIMIT > 0 else 0
        st.progress(min(prog, 1.0)) # Ensure progress doesn't exceed 1.0
        if uses_today >= TRIAL_DAILY_LIMIT: st.error("❌ Daily limit reached.")
    else:
        st.success("✅ Premium Account", icon="💳")
        credits = st.session_state.get('user_credits', 0)
        st.metric("Remaining Credits", credits)
        if credits <= 0: st.error("❌ No credits remaining. Please purchase more.")
        # Optional: Add a link to purchase more credits
        # st.link_button("Buy More Credits", "YOUR_GUMROAD_PURCHASE_LINK_HERE")

    st.divider()
    st.markdown("### 🔐 Session")
    if st.button("Log Out", use_container_width=True, key="logout_button"): # Added key
        logging.info(f"User logged out: {user_email}")
        for key in list(st.session_state.keys()): del st.session_state[key]
        st.session_state.logged_in = False
        st.toast("You have been logged out.", icon="👋")
        st.rerun()
    st.divider()
    st.caption(f"App Version: 1.9") # Incremented version

# === Main App UI ===
st.title("📄 PDF Table Extractor Pro")
st.markdown("Effortlessly extract tables from PDF files to Excel. Translate content on the fly.")
st.divider()

# --- Step 1: Upload ---
st.subheader("1. Upload Your PDF")
uploaded_file = st.file_uploader(
    "Click or drag to upload a PDF file.",
    type=["pdf"],
    key="pdf_uploader",
    label_visibility="collapsed"
)

if uploaded_file:
    st.success(f"✅ File ready: '{uploaded_file.name}' ({uploaded_file.size / 1024:.1f} KB)")
    st.divider()

    # --- Step 2: Configure Options ---
    st.subheader("2. Configure Extraction Options")
    col_pages, col_translate = st.columns(2)

    with col_pages:
        st.markdown("📄 **Page Selection**")
        pages_to_process = st.text_input(
            "Pages to Process",
            value="all",
            key="pages_input",
            help="Enter specific pages (e.g., '1,3,5-7') or use 'all' for the entire document."
        ).strip()
        # Use lower() for case-insensitive 'all' match
        if not re.fullmatch(r"^\s*(all|\d+(\s*-\s*\d+)?(\s*,\s*\d+(\s*-\s*\d+)?)*)\s*$", pages_to_process, re.IGNORECASE):
            st.error("Invalid page format. Use numbers, commas, hyphens (e.g., '1,3,5-7') or 'all'.")
            st.stop()

    with col_translate:
        st.markdown("🌍 **Translation (Optional)**")
        enable_translation = st.checkbox("Translate extracted table content?", key="translate_cb", value=False)
        selected_lang_code = None
        if enable_translation:
            # Consider making this list dynamic or loading from config
            full_language_names = {
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
            # Use title case for display names for consistency
            full_language_names = {k: v.title() for k, v in full_language_names.items()}
            lang_code_to_name = {v: k for k, v in full_language_names.items()} # Reverse map
            sorted_lang_names = sorted(full_language_names.values())
            try: default_index = sorted_lang_names.index("English")
            except ValueError: default_index = 0

            selected_lang_name = st.selectbox(
                "Target language:",
                sorted_lang_names, index=default_index, key="lang_select"
            )
            selected_lang_code = lang_code_to_name[selected_lang_name] # Get code from selected name
            st.info(f"Translate to **{selected_lang_name}** (skips if already target).", icon="ℹ️")

    # --- Advanced Settings Expander ---
    with st.expander("🔧 Advanced PDF Parsing Settings (Optional)"):
        st.markdown("Adjust if default extraction results have inconsistencies. Hover over (?) for details.")
        camelot_flavor = st.selectbox( "Parsing Method (Flavor)", ['stream', 'lattice'], index=0, help="'stream' uses whitespace (no lines needed). 'lattice' uses lines (requires borders).")
        c1, c2 = st.columns(2)
        # Add caption to explain when sliders apply
        st.caption("_Note: Tolerance sliders below apply only when 'stream' flavor is selected._")
        with c1: edge_tolerance = st.slider("Edge Tolerance (Stream only)", 0, 1000, 200, step=50, help="Detection distance from page edges.")
        with c2: row_tolerance = st.slider( "Row Tolerance (Stream only)", 0, 50, 10, step=1, help="Vertical distance to group text in rows.")

    st.divider()

    # --- Step 3: Process ---
    st.subheader("3. Process and Download")
    process_button_label = f"🚀 Extract Tables from '{pages_to_process}' Pages"
    if st.button(process_button_label, key="process_button", type="primary", use_container_width=True):

        # --- Pre-processing Checks ---
        process_allowed = True
        if st.session_state.get("is_trial_user", False):
             if st.session_state.get("trial_uses_today", 0) >= TRIAL_DAILY_LIMIT:
                 st.error(f"❌ Cannot process: Free trial limit reached."); process_allowed = False
        elif st.session_state.get('user_credits', 0) <= 0:
             st.error("❌ Cannot process: No credits remaining."); process_allowed = False
        if not process_allowed: st.stop()

        # --- Processing Logic ---
        status_placeholder = st.empty()
        progress_bar = st.progress(0.0, text="Initializing...")
        start_time = datetime.now()

        # Define Helpers within scope or ensure they are globally available if needed elsewhere
        @st.cache_data(show_spinner=False)
        def translate_text(text, target_lang):
            original_text = str(text).strip();
            if not original_text: return original_text
            try:
                detected_lang = detect(original_text)
                if detected_lang == target_lang: return original_text
            except Exception: pass # Ignore lang detect errors, attempt translation anyway
            try:
                # Ensure target_lang is valid before calling translator if needed
                translated = GoogleTranslator(source='auto', target=target_lang).translate(original_text)
                return translated if translated else original_text
            except Exception as e:
                logging.warning(f"Translate failed: '{original_text[:30]}...'. Err: {e}")
                return original_text

        def translate_df_silent(df, target_lang):
             # Ensure target_lang is valid before applying
             if target_lang:
                 return df.copy().applymap(lambda x: translate_text(x, target_lang))
             return df # Return original if no valid target_lang

        def split_merged_rows(df):
            new_rows = []; df = df.fillna(''); cols = df.columns
            for _, row in df.iterrows():
                row_list = row.tolist()
                if any('\n' in str(cell) for cell in row_list):
                    parts = [str(cell).split('\n') for cell in row_list];
                    try: # Handle potential error if parts is empty or malformed
                        max_len = max(len(p) for p in parts) if parts else 0
                    except ValueError:
                        max_len = 0
                    for i in range(max_len): new_rows.append([p[i] if i < len(p) else '' for p in parts])
                else: new_rows.append(row_list)
            # Return DataFrame with original columns, handles empty new_rows case
            return pd.DataFrame(new_rows, columns=cols)


        try:
            status_placeholder.info(f"⏳ Reading PDF (Pages: {pages_to_process})...")
            progress_bar.progress(0.05, text="Reading PDF...")

            # --- Build Camelot Keyword Arguments Conditionally --- ## FIX STARTS HERE ##
            camelot_kwargs = {
                "pages": pages_to_process.lower(), # Use lowercased 'all' if provided
                "flavor": camelot_flavor,
                "strip_text": '\n'
                # Add other common args here if needed
            }

            if camelot_flavor == 'stream':
                camelot_kwargs['edge_tol'] = edge_tolerance
                camelot_kwargs['row_tol'] = row_tolerance
            elif camelot_flavor == 'lattice':
                # No edge_tol or row_tol for lattice
                # Optionally add lattice-specific args like:
                # camelot_kwargs['line_scale'] = 40
                pass
            # ----------------------------------------------------- ## FIX ENDS HERE ##

            # Call Camelot with the constructed arguments
            tables = camelot.read_pdf(uploaded_file, **camelot_kwargs) # Unpack kwargs dictionary

            logging.info(f"Camelot found {len(tables)} tables in '{uploaded_file.name}' (pages: {pages_to_process}, flavor: {camelot_flavor}).")

            if not tables:
                 status_placeholder.warning(f"⚠️ No tables detected on pages '{pages_to_process}' with flavor '{camelot_flavor}'. Try different settings?"); st.stop()

            total_tables = len(tables)
            status_placeholder.info(f"✅ Found {total_tables} tables. Preparing Excel...")
            progress_bar.progress(0.1, text=f"Found {total_tables} tables...")

            output_buffer = BytesIO()
            processed_sheets = []
            table_counts_per_page = {}
            has_content = False
            MAX_SHEET_NAME_LEN = 31 # Excel sheet name limit

            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                for i, table in enumerate(tables):
                    current_progress = 0.1 + 0.7 * ((i + 1) / total_tables)
                    page_num = table.page
                    table_counts_per_page[page_num] = table_counts_per_page.get(page_num, 0) + 1
                    table_num_on_page = table_counts_per_page[page_num]

                    # Generate unique sheet name within length limit
                    base_sheet_name = f"Page_{page_num}_Table_{table_num_on_page}"
                    sheet_name = base_sheet_name[:MAX_SHEET_NAME_LEN]
                    count = 1
                    temp_sheet_name = sheet_name
                    while temp_sheet_name in processed_sheets:
                        suffix = f"_{count}"
                        max_len = MAX_SHEET_NAME_LEN - len(suffix)
                        if max_len <=0: # Safety for extremely long suffixes (unlikely)
                            temp_sheet_name = f"Sheet_Err_{i}"; break
                        temp_sheet_name = base_sheet_name[:max_len] + suffix
                        count += 1
                        if count > 100: # Prevent infinite loop
                           temp_sheet_name = f"Sheet_Err_{i}"; break
                    sheet_name = temp_sheet_name

                    status_placeholder.info(f"⚙️ Processing {sheet_name} ({i+1}/{total_tables})...")
                    progress_bar.progress(current_progress, text=f"Processing {sheet_name}...")
                    df = table.df
                    if df.empty: logging.info(f"Skipping empty table: {sheet_name}"); continue

                    try:
                        df.columns = [str(col).strip() for col in df.columns]
                        df = split_merged_rows(df); df = df.astype(str)
                        if df.empty: logging.info(f"Table empty after cleaning: {sheet_name}"); continue
                        has_content = True
                        if selected_lang_code:
                            df = translate_df_silent(df, selected_lang_code)
                    except Exception as clean_err:
                        logging.error(f"Error cleaning/translating table {sheet_name}: {clean_err}", exc_info=True) # Log traceback
                        st.warning(f"⚠️ Skipped table {sheet_name} due to processing error."); continue

                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    processed_sheets.append(sheet_name)

                if not has_content: status_placeholder.warning("⚠️ No data extracted after cleaning tables."); st.stop()

                progress_bar.progress(0.85, text="Formatting Excel...")
                status_placeholder.info("🎨 Applying final formatting...")
                workbook = writer.book
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
                        ws.column_dimensions[column_letter].width = min(max((max_length + 2) * 1.1, 12), 60)

            output_buffer.seek(0)
            end_time = datetime.now(); duration = end_time - start_time
            progress_bar.progress(1.0, text="Complete!")
            status_placeholder.success(f"✅ Success! Processed {len(processed_sheets)} tables in {duration.total_seconds():.1f} seconds.")

            download_filename = f"extracted_{os.path.splitext(uploaded_file.name)[0]}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            st.download_button( label=f"📥 Download Excel ({len(processed_sheets)} Sheets)", data=output_buffer, file_name=download_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_excel_button", use_container_width=True)

            try:
                user_email_for_update = st.session_state.get("user_email")
                if user_email_for_update:
                    if st.session_state.is_trial_user:
                        current_uses = st.session_state.get("trial_uses_today", 0) + 1; st.session_state.trial_uses_today = current_uses
                        trial_data = load_json(TRIAL_FILE); user_trial_info = trial_data.get(user_email_for_update, {})
                        user_trial_info.update({"uses": current_uses, "date": datetime.today().strftime("%Y-%m-%d")})
                        trial_data[user_email_for_update] = user_trial_info; save_json(TRIAL_FILE, trial_data)
                        logging.info(f"Trial credit used by {user_email_for_update}. Uses: {current_uses}")
                    else:
                        current_credits = st.session_state.get("user_credits", 0) - 1; st.session_state.user_credits = current_credits
                        credit_data = load_json(CREDIT_FILE); credit_data[user_email_for_update] = max(current_credits, 0); save_json(CREDIT_FILE, credit_data) # Prevent negative credits
                        logging.info(f"Credit used by {user_email_for_update}. Remaining: {current_credits}")
                    st.toast("1 credit deducted.", icon="🪙")
                else: logging.error("Could not deduct credit: user_email not found in session state.")
            except Exception as e:
                 st.error("⚠️ Error updating usage count. Download is ready.")
                 logging.error(f"Failed credit update for {user_email_for_update}: {e}", exc_info=True) # Log traceback

        except ImportError: status_placeholder.error("❌ Error: Required library missing."); logging.error("ImportError on process."); st.stop()
        # Catch specific Camelot error if needed, e.g., for PDF password
        # except camelot.handlers.PDFHandler.PasswordRequired:
        #    status_placeholder.error("❌ Error: PDF is password protected."); st.stop()
        except Exception as e:
            # Check for the specific error text from the traceback
            if "edge_tol,row_tol cannot be used with flavor='lattice'" in str(e):
                 status_placeholder.error("❌ Config Error: Tolerance settings cannot be used with 'lattice' flavor. Please use 'stream' or remove tolerances.")
                 logging.error(f"Configuration error: {e}")
            else:
                 status_placeholder.error(f"❌ An unexpected error occurred during processing.") # Generic message for user
                 logging.error(f"Processing failed: {e}", exc_info=True) # Detailed log for developer
            st.stop()


else:
    st.info("👋 Welcome! Please upload a PDF file to get started.") # Welcome message when no file uploaded

# --- Footer ---
st.divider()
st.caption("© {} PDF Table Extractor Pro | For support, contact lovinquesaba17@gmail.com.".format(datetime.now().year))
