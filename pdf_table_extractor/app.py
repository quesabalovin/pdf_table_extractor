# app.py
import streamlit as st
import pandas as pd
import camelot
from deep_translator import GoogleTranslator
from langdetect import detect
from io import BytesIO
from openpyxl.styles import Alignment
import json
import os
from datetime import datetime
from credentials import USERS

CREDIT_FILE = "credits.json"
TRIAL_FILE = "trial_users.json"
TRIAL_DAILY_LIMIT = 20

# === File Utilities ===
def load_json(filename):
    if not os.path.exists(filename):
        with open(filename, "w") as f:
            json.dump({}, f)
    with open(filename, "r") as f:
        return json.load(f)

def save_json(filename, data):
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

# === Login ===
def login():
    st.set_page_config(page_title="PDF Extractor Login", layout="centered")

    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    if not st.session_state.logged_in:
        st.title("🔐 Login to Access the App")
        with st.form("login_form"):
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Log In")

            if submitted:
                if email in USERS and USERS[email]["password"] == password:
                    st.session_state.logged_in = True
                    st.session_state.user_email = email

                    if email == "freetrial@example.com":
                        trial_data = load_json(TRIAL_FILE)
                        today = datetime.today().strftime("%Y-%m-%d")
                        user = trial_data.get(email, {"date": today, "uses": 0})

                        if user["date"] != today:
                            user["date"] = today
                            user["uses"] = 0

                        if user["uses"] >= TRIAL_DAILY_LIMIT:
                            st.error("❌ This free trial account has reached the daily limit (20 uses).")
                            return False

                        st.session_state.trial_user = user
                        st.session_state.trial_data = trial_data
                    else:
                        credit_data = load_json(CREDIT_FILE)
                        if email not in credit_data:
                            credit_data[email] = USERS[email]["credits"]
                            save_json(CREDIT_FILE, credit_data)
                        st.session_state.user_credits = credit_data[email]

                    st.success(f"✅ Welcome, {email}!")
                    st.rerun()
                else:
                    st.error("❌ Invalid email or password.")
        return False
    return True

# === Run Login First ===
if not login():
    st.stop()

# === Sidebar Info ===
if st.session_state.user_email == "freetrial@example.com":
    st.sidebar.markdown("### 🧪 Free Trial Account")
else:
    st.sidebar.markdown("### 💳 Credits")
    st.sidebar.write(f"Remaining credits: **{st.session_state.get('user_credits', 0)}**")
    if st.session_state.user_credits <= 0:
        st.sidebar.error("❌ You have no credits remaining.")
        st.stop()

# === Logout Button ===
st.sidebar.markdown("### 🔐 Session")
if st.sidebar.button("Log Out"):
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()

# === Main App ===
st.title("📄 PDF Table Extractor + Translator")

# === Language Map ===
full_language_names = {
    "en": "English", "es": "Spanish", "fr": "French", "de": "German", "zh-cn": "Chinese (Simplified)",
    "zh-tw": "Chinese (Traditional)", "ja": "Japanese", "ko": "Korean", "ru": "Russian", "ar": "Arabic",
    "pt": "Portuguese", "it": "Italian", "hi": "Hindi", "tr": "Turkish", "pl": "Polish", "uk": "Ukrainian",
    "vi": "Vietnamese", "id": "Indonesian", "nl": "Dutch", "sv": "Swedish", "no": "Norwegian", "fi": "Finnish"
}
sorted_lang_names = sorted(full_language_names.values())
lang_name_to_code = {v: k for k, v in full_language_names.items()}

# === Helpers ===
def translate_text(text, target_lang):
    try:
        text = str(text).strip()
        if not text:
            return text
        if detect(text) != target_lang:
            return GoogleTranslator(source='auto', target=target_lang).translate(text)
    except:
        return text
    return text

def translate_df(df, target_lang):
    return df.applymap(lambda x: translate_text(x, target_lang))

def split_merged_rows(df):
    new_rows = []
    for _, row in df.iterrows():
        if any('\n' in str(cell) for cell in row):
            parts = [str(cell).split('\n') for cell in row]
            max_len = max(len(p) for p in parts)
            for i in range(max_len):
                new_row = [p[i] if i < len(p) else '' for p in parts]
                new_rows.append(new_row)
        else:
            new_rows.append(row.tolist())
    return pd.DataFrame(new_rows, columns=df.columns)

# === Upload ===
new_file = st.file_uploader("📤 Upload your PDF", type=["pdf"])
if new_file:
    enable_translation = st.checkbox("🌍 Translate table content to another language?")
    if enable_translation:
        default_index = sorted_lang_names.index("English")
        selected_lang_name = st.selectbox("Choose target language:", sorted_lang_names, index=default_index)
        selected_lang_code = lang_name_to_code[selected_lang_name]
    else:
        selected_lang_code = None

    if st.button("✅ Process PDF"):
        with st.spinner("Processing..."):
            tables = camelot.read_pdf(new_file, pages='all', flavor='stream', strip_text='\n', edge_tol=200, row_tol=10)
            output = BytesIO()
            sheet_names = []

            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                for i, table in enumerate(tables):
                    df = table.df
                    if df.empty:
                        continue

                    df.columns = [str(col).strip() for col in df.columns]
                    df = split_merged_rows(df)
                    df = df.astype(str)

                    if selected_lang_code:
                        df = translate_df(df, selected_lang_code)

                    sheet_name = f"Page_{table.page}_Table_{i+1}"[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    sheet_names.append(sheet_name)

                workbook = writer.book
                for sheet in sheet_names:
                    ws = workbook[sheet]
                    for row in ws.iter_rows():
                        for cell in row:
                            cell.alignment = Alignment(wrap_text=True)
                    for col in ws.columns:
                        col_letter = col[0].column_letter
                        ws.column_dimensions[col_letter].width = 35

            output.seek(0)
            st.success("✅ Done! Download your Excel file:")
            st.download_button("📥 Download Excel", data=output, file_name="translated_tables.xlsx")

            # === Deduct Credit ===
            if st.session_state.user_email == "freetrial@example.com":
                st.session_state.trial_user["uses"] += 1
                st.session_state.trial_data["freetrial@example.com"] = st.session_state.trial_user
                save_json(TRIAL_FILE, st.session_state.trial_data)
            else:
                st.session_state.user_credits -= 1
                credit_data = load_json(CREDIT_FILE)
                credit_data[st.session_state.user_email] = st.session_state.user_credits
                save_json(CREDIT_FILE, credit_data)
