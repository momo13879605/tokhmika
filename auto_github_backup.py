#!/usr/bin/env python3
import json
import os
import subprocess
import time
import sys
from datetime import datetime

# ================================================================
# تنظیمات - این قسمت رو هر طور دلت میخواد تغییر بده
# ================================================================

REPO_PATH = "/home/a1274272/tokhmika"           # مسیر پروژه توی سرور
DB_FILE = "output/noboka_database.db"           # فایلی که میخوای push بشه
SLEEP_HOURS = 1                                 # هر چند ساعت یه بار (مثلاً ۱)
TOKEN_FILE = os.path.join(REPO_PATH, ".github_token.json")  # جای ذخیره توکن

# ================================================================
# کد اصلی - به این قسمت دست نزن
# ================================================================

def log(msg):
    """چاپ پیام با تاریخ و زمان"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)

def save_token(token):
    """ذخیره توکن در فایل JSON"""
    data = {"token": token, "saved_at": datetime.now().isoformat()}
    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f)
    os.chmod(TOKEN_FILE, 0o600)  # فقط خود کاربر بتونه بخونه
    log("✅ Token ذخیره شد.")

def load_token():
    """بارگذاری توکن از فایل JSON"""
    if not os.path.exists(TOKEN_FILE):
        return None
    with open(TOKEN_FILE, "r") as f:
        data = json.load(f)
    return data.get("token")

def get_token():
    """گرفتن توکن از کاربر (فقط اولین بار)"""
    token = load_token()
    if token:
        log("🔑 Token از فایل قبلی بارگذاری شد.")
        return token
    
    log("📝 برای اولین بار اجرا میشه...")
    log("🔐 لطفاً توکن گیت‌هات رو وارد کن:")
    log("   (توکن رو باید از Settings > Developer settings > Personal access tokens بسازی)")
    token = input("توکن: ").strip()
    
    if not token:
        log("❌ خطا: توکن نمیتونه خالی باشه.")
        sys.exit(1)
    
    save_token(token)
    log("✅ Token با موفقیت ذخیره شد.")
    return token

def setup_remote_with_token():
    """تنظیم remote با توکن (سازگار با git قدیمی)"""
    token = get_token()
    remote_url = f"https://momo13879605:{token}@github.com/momo13879605/tokhmika.git"
    
    # بررسی وجود remote origin با استفاده از git config (سازگار با همه نسخه‌ها)
    result = subprocess.run(
        ["git", "config", "--get", "remote.origin.url"],
        cwd=REPO_PATH,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        # origin وجود دارد، آدرس آن را به روز می‌کنیم
        subprocess.run(
            ["git", "remote", "set-url", "origin", remote_url],
            cwd=REPO_PATH,
            check=True
        )
        log("✅ Remote origin به‌روز شد.")
    else:
        # origin وجود ندارد، آن را اضافه می‌کنیم
        subprocess.run(
            ["git", "remote", "add", "origin", remote_url],
            cwd=REPO_PATH,
            check=True
        )
        log("✅ Remote origin اضافه شد.")

def push_database():
    """انجام commit و push"""
    full_path = os.path.join(REPO_PATH, DB_FILE)
    
    if not os.path.exists(full_path):
        log(f"⚠️ فایل {DB_FILE} وجود نداره!")
        return False
    
    # git add
    result = subprocess.run(["git", "add", DB_FILE], cwd=REPO_PATH, capture_output=True, text=True)
    if result.returncode != 0:
        log(f"❌ خطا در git add: {result.stderr}")
        return False
    log("✅ git add انجام شد.")
    
    # git commit
    commit_msg = f"Auto backup: {DB_FILE} at {datetime.now()}"
    result = subprocess.run(["git", "commit", "-m", commit_msg], cwd=REPO_PATH, capture_output=True, text=True)
    
    # ترکیب stdout و stderr برای بررسی
    output = (result.stdout + result.stderr).lower()
    if result.returncode != 0:
        if "nothing to commit" in output:
            log("📝 فایل تغییری نکرده بود. نیازی به push نیست.")
            return True
        else:
            log(f"❌ خطا در git commit: {result.stderr}")
            return False
    
    log("✅ git commit انجام شد.")
    
    # git push
    result = subprocess.run(["git", "push", "origin", "main"], cwd=REPO_PATH, capture_output=True, text=True)
    if result.returncode != 0:
        log(f"❌ خطا در git push: {result.stderr}")
        return False
    
    log("✅ git push انجام شد. فایل با موفقیت به گیت‌هاب رفت.")
    return True

def main():
    log("🚀 سرویس پشتیبان‌گیری خودکار از گیت‌هاب شروع شد.")
    log(f"📁 مسیر پروژه: {REPO_PATH}")
    log(f"📄 فایل مورد نظر: {DB_FILE}")
    log(f"⏱️ فاصله زمانی: هر {SLEEP_HOURS} ساعت یکبار")
    
    # چک کن که آیا پروژه git هست یا نه
    git_dir = os.path.join(REPO_PATH, ".git")
    if not os.path.exists(git_dir):
        log("❌ خطا: این مسیر یک مخزن گیت نیست!")
        log("   اول باید با دستور 'git init' پروژه رو git کنی.")
        sys.exit(1)
    
    # تنظیم remote با توکن (اولین بار توکن میگیره، دفعات بعد از فایل میخونه)
    setup_remote_with_token()
    
    log("🔄 سرویس در حال اجراست... (Ctrl+C برای توقف)")
    
    # حلقه بی‌نهایت
    while True:
        try:
            log("📤 شروع پوش کردن فایل...")
            push_database()
            
            # چند ثانیه صبر کن بعد دوباره چک کن
            wait_seconds = SLEEP_HOURS * 3600
            log(f"💤 {SLEEP_HOURS} ساعت صبر می‌کنم تا دوباره چک کنم...")
            time.sleep(wait_seconds)
            
        except KeyboardInterrupt:
            log("🛑 دریافت سیگنال توقف. سرویس متوقف شد.")
            break
        except Exception as e:
            log(f"❗ خطای غیرمنتظره: {e}")
            log(f"💤 {SLEEP_HOURS} ساعت صبر می‌کنم و دوباره تلاش می‌کنم...")
            time.sleep(wait_seconds)

if __name__ == "__main__":
    main()