import os
import re
import json
import time
import random
import logging
import sqlite3
import glob
import sys
import threading
import zipfile
import asyncio
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from typing import List, Dict, Set, Any, Tuple
from datetime import datetime, timedelta
import pytz

import aiohttp
from pyrubi import Client
from tqdm import tqdm
import numpy as np

# ═══════════════════ تنظیمات اصلی ═══════════════════
OUTPUT_DIR = "output"
STATE_DIR = os.path.join(OUTPUT_DIR, "states")
PHONE_MAP_FILE = os.path.join(OUTPUT_DIR, "phone_guid_map.json")
DB_FILE = os.path.join(OUTPUT_DIR, "noboka_database.db")
LOG_FILE = "rubika_unlimited.log"

# محدوده تأخیرهای انسانی (میانه به‌صورت log‑normal)
ADD_CONTACT_DELAY_RANGE = (1.0, 2.0)
OTHER_OPERATION_DELAY_RANGE = (1.0, 10.0)
DELETE_EXTRACT_DELAY_RANGE = (0.8, 3.0)   # جایگزین ۱.۵ ثانیه ثابت
GENERATE_DELAY_BETWEEN_BATCHES = (5, 15)

API_TIMEOUT = 30

RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX_PER_WINDOW = 15
RATE_BACKOFF_MAX = 10.0

# ═══════ پیش‌شماره‌های اپراتورها ═══════
OPERATORS_PREFIXES = {
    'mci':        ['0910','0911','0912','0913','0914','0915','0916','0917','0918','0919'],
    'irancell':   ['0901','0902','0903','0904','0905','0930','0933','0935','0936','0937','0938','0939'],
    'rightel':    ['0920','0921','0922'],
    'samantel':   ['0970'],
    'shatel':     ['09981','09982'],
    'aryatelecom':['0972'],
    'others':     ['0941','0951','0961','0981']
}

# ═══════ تنظیمات تلگرام (از طریق آرگومان یا متغیر محیطی) ═══════
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "7800540191:AAHZieM5HZY3VM11TekAxsMn9mLiil-a-bo")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "5914346958")

BATCH_SEND_INTERVAL = 3  # هر چند دسته، فایل زیپ دیتابیس ارسال شود

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ═══════════════ توابع تأخیر انسانی ═══════════════
def human_delay(median_seconds: float, sigma: float = 0.35, min_delay: float = 0.3, max_delay: float = 15.0) -> float:
    """تاخیر با توزیع log‑normal که الگوی انسانی را تقلید می‌کند."""
    while True:
        delay = np.random.lognormal(mean=np.log(median_seconds), sigma=sigma)
        if min_delay <= delay <= max_delay:
            time.sleep(delay)
            return delay

def human_delay_range(min_sec: float, max_sec: float) -> float:
    """نسخه‌ای که میانه را وسط بازه در نظر می‌گیرد."""
    median = (min_sec + max_sec) / 2.0
    sigma = 0.35
    while True:
        delay = np.random.lognormal(mean=np.log(median), sigma=sigma)
        if min_sec <= delay <= max_sec:
            time.sleep(delay)
            return delay

# ═══════════════ زمان‌بند هوشمند با ساعت تهران ═══════════════
class HumanScheduler:
    def __init__(self, tz_name='Asia/Tehran'):
        self.tz = pytz.timezone(tz_name)
        # پنجره‌های فعالیت (ساعت شروع, ساعت پایان) – مقادیر می‌توانند اعشاری باشند
        self.daily_patterns = [
            (8, 12.5),    # 08:00 تا 12:30
            (13.5, 17.5), # 13:30 تا 17:30
            (19, 23)      # 19:00 تا 23:00
        ]

    def _current_hour_float(self) -> float:
        """زمان کنونی تهران را به صورت عدد اعشاری (ساعت.دقیقه) برمی‌گرداند"""
        now = datetime.now(self.tz)
        return now.hour + now.minute / 60.0

    def _get_randomized_windows(self):
        """لیست پنجره‌های امروز را با تغییر تصادفی ±۳۰ دقیقه برمی‌گرداند"""
        windows = []
        for start_h, end_h in self.daily_patterns:
            s = start_h + random.uniform(-0.5, 0.5)
            e = end_h + random.uniform(-0.5, 0.5)
            if s < 0:
                s = 0
            if e > 24:
                e = 24
            windows.append((s, e))
        return windows

    def wait_until_active(self) -> float:
        while True:
            now_hour = self._current_hour_float()
            windows = self._get_randomized_windows()

            # ۱. بررسی کن آیا در یک پنجرهٔ فعال هستیم
            for s, e in windows:
                if s <= now_hour < e:
                    remaining = (e - now_hour) * 3600
                    logger.info(f"✅ در پنجرهٔ فعال {s:.2f} تا {e:.2f} – {remaining/60:.0f} دقیقه باقی‌مانده")
                    return remaining

            # ۲. اگر در هیچ پنجره‌ای نیستیم، نزدیک‌ترین پنجرهٔ آینده را پیدا کن
            future_windows = [(s, e) for (s, e) in windows if now_hour < s]
            if future_windows:
                # نزدیک‌ترین شروع
                next_s, next_e = min(future_windows, key=lambda w: w[0])
                wait_seconds = (next_s - now_hour) * 3600
                logger.info(f"⏰ خواب تا شروع پنجرهٔ {next_s:.2f} ({wait_seconds/60:.0f} دقیقه)")
                time.sleep(wait_seconds)
                # بعد از خواب داخل پنجره هستیم
                remaining = (next_e - next_s) * 3600
                return remaining

            # ۳. هیچ پنجره‌ای برای امروز باقی نمانده – بخواب تا اولین پنجرهٔ فردا
            # اولین پنجرهٔ فردا را با تغییر تصادفی محاسبه کن
            first_start = self.daily_patterns[0][0] + random.uniform(-0.5, 0.5)
            if first_start < 0:
                first_start = 0
            wait_hours = 24.0 - now_hour + first_start
            logger.info(f"🌙 امروز تمام شد – خواب تا فردا ساعت {first_start:.2f} ({wait_hours:.1f} ساعت)")
            time.sleep(wait_hours * 3600)
            # حلقه تکرار می‌شود

# ═══════════════ توابع کمکی ═══════════════
def normalize_phone(num: str) -> str:
    num = re.sub(r'[^\d]', '', num)
    if num.startswith('0'):
        return '98' + num[1:]
    if num.startswith('98'):
        return num
    if num.startswith('+'):
        return num[1:]
    return num

def atomic_json_dump(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

# ═══════════════ محدودکننده نرخ تطبیقی ═══════════════
class AdaptiveRateLimiter:
    def __init__(self, window_size=RATE_LIMIT_WINDOW, max_per_window=RATE_LIMIT_MAX_PER_WINDOW):
        self.window_size = window_size
        self.max_per_window = max_per_window
        self.sent_timestamps: List[float] = []
        self.backoff_multiplier = 1.0
        self.lock = threading.Lock()

    def can_send(self) -> bool:
        with self.lock:
            now = time.time()
            self.sent_timestamps = [t for t in self.sent_timestamps if now - t < self.window_size]
            return len(self.sent_timestamps) < (self.max_per_window / self.backoff_multiplier)

    def record_send(self):
        with self.lock:
            self.sent_timestamps.append(time.time())

    def increase_backoff(self, factor=1.5):
        with self.lock:
            self.backoff_multiplier = min(self.backoff_multiplier * factor, RATE_BACKOFF_MAX)
            logger.warning(f"📈 عقب‌نشینی نرخ به {self.backoff_multiplier:.2f}x")

# ═══════════════ دیتابیس ═══════════════
def init_db():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS contacts_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT,
                guid TEXT UNIQUE,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                chat_id TEXT,
                bio TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

def save_to_db(records: List[dict]):
    if not records:
        return
    with sqlite3.connect(DB_FILE) as conn:
        inserted = 0
        for rec in records:
            cur = conn.execute("""
                INSERT OR IGNORE INTO contacts_info 
                (phone, guid, first_name, last_name, username, chat_id, bio)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (rec.get('phone',''), rec.get('guid',''), rec.get('first_name',''),
                  rec.get('last_name',''), rec.get('username',''), rec.get('chat_id',''), rec.get('bio','')))
            if cur.rowcount > 0:
                inserted += 1
        conn.commit()
    logger.info(f"{inserted} رکورد جدید ذخیره شد، {len(records)-inserted} تکراری")

def get_existing_phones_from_db() -> Set[str]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            return {r[0] for r in conn.execute("SELECT DISTINCT phone FROM contacts_info") if r[0]}
    except:
        return set()

def get_existing_guids_from_db() -> Set[str]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            return {r[0] for r in conn.execute("SELECT guid FROM contacts_info WHERE guid IS NOT NULL AND guid != ''")}
    except:
        return set()

# ═══════════════ توابع ارسال تلگرام با aiohttp ═══════════════
async def async_send_telegram_message(token: str, chat_id: str, text: str) -> bool:
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    logger.info("✅ پیام تلگرام ارسال شد.")
                    return True
                else:
                    logger.error(f"⚠️ خطا در ارسال پیام: {resp.status} - {await resp.text()}")
                    return False
    except Exception as e:
        logger.error(f"❌ خطا در اتصال تلگرام (پیام): {e}")
        return False

async def async_send_telegram_document(token: str, chat_id: str, file_path: str) -> bool:
    if not token or not chat_id or not os.path.exists(file_path):
        return False
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    data = aiohttp.FormData()
    data.add_field("chat_id", chat_id)
    data.add_field("document", open(file_path, "rb"),
                   filename=os.path.basename(file_path))
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    logger.info(f"✅ فایل {os.path.basename(file_path)} به تلگرام ارسال شد.")
                    return True
                else:
                    logger.error(f"⚠️ خطا در ارسال فایل: {resp.status} - {await resp.text()}")
                    return False
    except Exception as e:
        logger.error(f"❌ خطا در اتصال تلگرام (فایل): {e}")
        return False

def sync_send_telegram_message(token: str, chat_id: str, text: str):
    return asyncio.run(async_send_telegram_message(token, chat_id, text))

def sync_send_telegram_document(token: str, chat_id: str, file_path: str):
    return asyncio.run(async_send_telegram_document(token, chat_id, file_path))

def zip_db_file() -> str:
    if not os.path.exists(DB_FILE):
        return ""
    zip_name = f"db_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    zip_path = os.path.join(OUTPUT_DIR, zip_name)
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(DB_FILE, arcname=os.path.basename(DB_FILE))
    logger.info(f"📦 فایل دیتابیس زیپ شد: {zip_path}")
    return zip_path

# ═══════════════ مدیر اکانت تکی ═══════════════
class ContactManager:
    def __init__(self, session_name: str):
        self.session_name = session_name
        self.client = Client(session_name)
        self.rate_limiter = AdaptiveRateLimiter()
        self.bot_guid = self._get_me_guid()
        logger.info(f"[{session_name}] Bot GUID: {self.bot_guid}")
        self.phone_map = self._load_phone_map()
        self.state_file = os.path.join(STATE_DIR, f"{session_name}_state.json")
        init_db()
        self.state = self._load_state()
        self._flush_completed_to_db()

    def _get_me_guid(self):
        try:
            me = self._safe_api_call(self.client.get_me, timeout=10)
            return me.get('user', {}).get('user_guid', '') if isinstance(me, dict) else ''
        except:
            return ''

    def _load_phone_map(self) -> dict:
        try:
            with open(PHONE_MAP_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}

    def _save_phone_map(self):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(PHONE_MAP_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.phone_map, f, ensure_ascii=False, indent=2)

    def _update_map_from_contacts(self, contacts: List[dict]):
        for c in contacts:
            guid = c.get('user_guid') or c.get('guid')
            phone = normalize_phone(c.get('phone', c.get('phone_number', '')))
            if guid and phone:
                self.phone_map[guid] = phone
        self._save_phone_map()

    def _load_state(self) -> dict:
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    state.setdefault("added_phones", [])
                    state.setdefault("completed", [])
                    return state
            except:
                pass
        return {"added_phones": [], "completed": []}

    def _save_state(self):
        atomic_json_dump(self.state, self.state_file)

    def _mark_phone_added(self, phone: str):
        if phone not in self.state["added_phones"]:
            self.state["added_phones"].append(phone)
            self._save_state()

    def _mark_phone_completed(self, record: dict):
        self.state["completed"].append(record)
        self._save_state()

    def _flush_completed_to_db(self):
        if not self.state["completed"]:
            return
        logger.info(f"[{self.session_name}] همگام‌سازی {len(self.state['completed'])} رکورد از state به دیتابیس...")
        save_to_db(self.state["completed"])
        self.state["completed"] = []
        self._save_state()

    def get_completed_phones(self) -> Set[str]:
        completed = {rec['phone'] for rec in self.state["completed"] if rec.get('phone')}
        completed.update(get_existing_phones_from_db())
        return completed

    def _safe_api_call(self, func, timeout=API_TIMEOUT, *args, **kwargs) -> Any:
        while not self.rate_limiter.can_send():
            wait = self.rate_limiter.window_size / self.rate_limiter.backoff_multiplier
            logger.warning(f"⏳ [{self.session_name}] محدودیت نرخ – انتظار {wait:.1f}s")
            time.sleep(wait)
        with ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(func, *args, **kwargs)
            try:
                result = future.result(timeout=timeout)
                if isinstance(result, dict):
                    st = str(result.get('status', '')).lower()
                    er = str(result.get('error', '')).lower()
                    if any(w in st or w in er for w in ['rate','limit','too many','spam']):
                        self.rate_limiter.increase_backoff(2.0)
                self.rate_limiter.record_send()
                return result
            except FutureTimeoutError:
                logger.error(f"⏱️ [{self.session_name}] تابع {func.__name__} تایم‌اوت")
                self.rate_limiter.increase_backoff(1.5)
                return {}
            except Exception as e:
                msg = str(e).lower()
                logger.error(f"❌ [{self.session_name}] خطا: {e}")
                if any(w in msg for w in ['rate','limit','too many','spam']):
                    self.rate_limiter.increase_backoff(2.0)
                elif any(w in msg for w in ['block','ban','forbidden']):
                    self.rate_limiter.increase_backoff(4.0)
                return {}

    def add_contact(self, phone: str, name: str = None) -> bool:
        if not name:
            name = f"tmp_{phone[-4:]}"
        human_delay_range(*ADD_CONTACT_DELAY_RANGE)
        try:
            self._safe_api_call(self.client.add_address_book, timeout=15,
                                phone=phone, first_name=name, last_name="")
            return True
        except Exception as e:
            logger.error(f"[{self.session_name}] افزودن ناموفق {phone}: {e}")
            return False

    def get_all_contacts(self) -> List[dict]:
        human_delay_range(*OTHER_OPERATION_DELAY_RANGE)
        resp = self._safe_api_call(self.client.get_contacts, timeout=API_TIMEOUT)
        contacts = []
        if isinstance(resp, dict):
            contacts = resp.get("users", []) or resp.get("contacts", [])
        elif isinstance(resp, list):
            contacts = resp
        self._update_map_from_contacts(contacts)
        return contacts

    def delete_contact(self, guid: str) -> bool:
        human_delay_range(*OTHER_OPERATION_DELAY_RANGE)
        try:
            self._safe_api_call(self.client.delete_contact, 15, object_guid=guid)
            return True
        except Exception as e:
            logger.error(f"[{self.session_name}] حذف ناموفق {guid}: {e}")
            return False

    def extract_real_info(self, guid: str) -> dict:
        info = self._safe_api_call(self.client.get_chat_info, API_TIMEOUT, object_guid=guid)
        user = info.get('user', {}) if isinstance(info, dict) else {}
        chat = info.get('chat', {}) if isinstance(info, dict) else {}
        return {
            "first_name": user.get("first_name", ""),
            "last_name": user.get("last_name", ""),
            "username": user.get("username", ""),
            "phone": user.get("phone", ""),
            "bio": user.get("bio", ""),
            "chat_id": chat.get("object_guid", ""),
        }

    def get_phone_from_map(self, guid: str) -> str:
        return self.phone_map.get(guid, "")

    def _build_phone_guid_map(self) -> Dict[str, str]:
        contacts = self.get_all_contacts()
        mapping = {}
        for c in contacts:
            guid = c.get('user_guid') or c.get('guid')
            phone = normalize_phone(c.get('phone', c.get('phone_number', '')))
            if guid and phone:
                mapping[phone] = guid
        return mapping

    def random_human_action(self):
        """یک عمل تصادفی و بی‌ضرر برای شکستن الگوی یکنواخت."""
        actions = [
            lambda: self._safe_api_call(self.client.get_me, timeout=10),
            lambda: self._safe_api_call(self.client.get_chat_info, 10, object_guid=self.bot_guid),
            lambda: self._safe_api_call(self.client.get_contacts, 10)
        ]
        action = random.choice(actions)
        try:
            action()
            human_delay_range(0.5, 2)
        except Exception as e:
            logger.debug(f"عمل تصادفی خطا داد: {e}")

    def process_batch(self, phone_list: List[str]) -> List[dict]:
        self._flush_completed_to_db()
        completed_phones = self.get_completed_phones()
        added_phones = set(self.state.get("added_phones", []))

        new_phones = [p for p in phone_list if p not in added_phones and p not in completed_phones]
        need_extraction = [p for p in phone_list if p in added_phones and p not in completed_phones]

        if not new_phones and not need_extraction:
            return []

        # مرحله ۱: افزودن (با احتمال ۱۰٪ یک عمل تصادفی قبل از شروع)
        if new_phones:
            if random.random() < 0.1:
                self.random_human_action()
            for phone in tqdm(new_phones, desc=f"➕ [{self.session_name}] افزودن", leave=False):
                if self.add_contact(phone):
                    self._mark_phone_added(phone)

        all_phones = new_phones + need_extraction
        if not all_phones:
            return []

        # مرحله ۲: نگاشت (با احتمال ۲۰٪ یک عمل تصادفی)
        if random.random() < 0.2:
            self.random_human_action()
        tqdm.write(f"   🔍 [{self.session_name}] دریافت نگاشت phone→guid...")
        phone_to_guid = self._build_phone_guid_map()
        rubika_phones = [p for p in all_phones if p in phone_to_guid]
        if not rubika_phones:
            return []

        for p in rubika_phones:
            g = phone_to_guid[p]
            if g:
                self.phone_map[g] = p
        self._save_phone_map()

        # مرحله ۳: حذف و استخراج با رفتار انسانی
        batch_results = []
        for phone in tqdm(rubika_phones, desc=f"🗑️🧠 [{self.session_name}] حذف و استخراج", leave=False):
            if phone in completed_phones:
                continue
            guid = phone_to_guid.get(phone)
            if not guid or guid == self.bot_guid:
                continue

            # ۸٪ احتمال عمل تصادفی قبل از حذف
            if random.random() < 0.08:
                self.random_human_action()

            if self.delete_contact(guid):
                # تأخیر تصادفی به‌جای ۱.۵ ثانیه
                human_delay_range(*DELETE_EXTRACT_DELAY_RANGE)
                # ۱۵٪ احتمال عمل بی‌ضرر قبل از استخراج
                if random.random() < 0.15:
                    self.random_human_action()
                real = self.extract_real_info(guid)
                final_phone = self.get_phone_from_map(guid) or phone
                record = {
                    "phone": final_phone,
                    "guid": guid,
                    "first_name": real.get("first_name", ""),
                    "last_name": real.get("last_name", ""),
                    "username": real.get("username", ""),
                    "chat_id": real.get("chat_id", ""),
                    "bio": real.get("bio", "")
                }
                self._mark_phone_completed(record)
                batch_results.append(record)
            else:
                logger.error(f"[{self.session_name}] حذف ناموفق برای {guid}")

        # مرحله ۴: ذخیره
        if batch_results:
            save_to_db(batch_results)
            self.state["completed"] = []
            self._save_state()

        return batch_results

# ═══════════════ مدیر چند اکانتی ═══════════════
class MultiAccountManager:
    def __init__(self, session_names: List[str]):
        self.managers: Dict[str, ContactManager] = {name: ContactManager(name) for name in session_names}

    def distribute_phones(self, phones: List[str]) -> Dict[str, List[str]]:
        dist = {name: [] for name in self.managers}
        for i, phone in enumerate(phones):
            names = list(self.managers.keys())
            dist[names[i % len(names)]].append(phone)
        return dist

    def process_batch(self, phones: List[str]) -> List[dict]:
        dist = self.distribute_phones(phones)
        all_results = []
        for name, mgr in self.managers.items():
            if not dist[name]:
                continue
            results = mgr.process_batch(dist[name])
            all_results.extend(results)
        return all_results

# ═══════════════ تولید شماره هوشمند ═══════════════
def generate_random_prefixes() -> List[str]:
    operators = list(OPERATORS_PREFIXES.keys())
    pattern = random.choices(['single','double','all'], weights=[0.3, 0.4, 0.3])[0]
    if pattern == 'single':
        return list(OPERATORS_PREFIXES[random.choice(operators)])
    elif pattern == 'double':
        chosen = random.sample(operators, 2)
        prefixes = []
        for op in chosen:
            prefixes.extend(OPERATORS_PREFIXES[op])
        return prefixes
    else:
        prefixes = []
        for op in operators:
            prefixes.extend(OPERATORS_PREFIXES[op])
        return prefixes

def generate_unique_numbers(count: int, used_phones: Set[str], max_attempts=2000) -> List[str]:
    numbers = set()
    attempts = 0
    while len(numbers) < count and attempts < max_attempts:
        prefixes = generate_random_prefixes()
        prefix = random.choice(prefixes)
        remaining = ''.join(random.choices('0123456789', k=7))
        raw = prefix + remaining
        phone = normalize_phone(raw)
        if phone not in used_phones and phone not in numbers:
            numbers.add(phone)
        attempts += 1
    return list(numbers)

# ═══════════════ شناسایی نشست‌ها ═══════════════
def auto_detect_sessions() -> List[str]:
    pyrubi_files = glob.glob("*.pyrubi")
    if not pyrubi_files:
        logger.error("هیچ فایل نشست .pyrubi یافت نشد.")
        sys.exit(1)
    sessions = sorted([os.path.splitext(f)[0] for f in pyrubi_files])
    print(f"📋 {len(sessions)} نشست شناسایی شد: {', '.join(sessions)}")
    return sessions

# ═══════════════ برنامه اصلی ═══════════════
def main():
    import argparse
    parser = argparse.ArgumentParser(description="استخراج‌کننده نامحدود روبیکا با تقلید رفتار انسانی (ساعت تهران)")
    parser.add_argument("--telegram-token", help="توکن ربات تلگرام")
    parser.add_argument("--telegram-chat-id", help="شناسه عددی چت تلگرام")
    args = parser.parse_args()

    token = args.telegram_token or TELEGRAM_TOKEN
    chat_id = args.telegram_chat_id or TELEGRAM_CHAT_ID
    if not token or not chat_id:
        logger.warning("⚠️ توکن یا chat_id تلگرام تنظیم نشده. ارسال غیرفعال است.")
    else:
        logger.info("✅ ارسال تلگرام فعال است.")

    print("""
╔══════════════════════════════════════════════════╗
║     استخراج‌کننده نامحدود شماره روبیکا          ║
║  (زمان‌بندی واقعی تهران، batch متغیر، تأخیر انسانی)║
╚══════════════════════════════════════════════════╝
    """)

    session_names = auto_detect_sessions()
    use_multi = len(session_names) > 1
    if use_multi:
        manager = MultiAccountManager(session_names)
    else:
        manager = ContactManager(session_names[0])

    used_phones = get_existing_phones_from_db()
    if use_multi:
        for m in manager.managers.values():
            m._flush_completed_to_db()
            used_phones.update(m.get_completed_phones())
    else:
        manager._flush_completed_to_db()
        used_phones.update(manager.get_completed_phones())

    # زمان‌بند تهران
    scheduler = HumanScheduler()

    iteration = 0
    total_new_guids = 0
    logger.info(f"شروع حلقه نامحدود. شماره‌های تکراری فعلی: {len(used_phones)}")

    try:
        while True:
            # منتظر پنجرهٔ فعال به وقت تهران بمان
            active_seconds = scheduler.wait_until_active()
            session_deadline = time.time() + active_seconds

            while time.time() < session_deadline:
                iteration += 1

                # تعیین تصادفی تعداد شماره در این دسته (بین ۱ تا ۲۰۰)
                batch_size = random.randint(1, 200)
                logger.info(f"⚡ چرخه {iteration}: انتخاب دسته {batch_size} تایی")

                new_batch = generate_unique_numbers(batch_size, used_phones)
                if len(new_batch) < batch_size:
                    logger.warning(f"فقط {len(new_batch)} شماره جدید تولید شد.")
                if not new_batch:
                    logger.error("امکان تولید شماره جدید نیست. توقف.")
                    return

                start_guids = len(get_existing_guids_from_db())

                if use_multi:
                    batch_results = manager.process_batch(new_batch)
                else:
                    batch_results = manager.process_batch(new_batch)

                used_phones.update(new_batch)
                current_guids = len(get_existing_guids_from_db())
                new_guids = current_guids - start_guids
                total_new_guids += new_guids

                report = (
                    f"📊 <b>چرخه {iteration}</b>\n"
                    f"🔹 شماره‌های جدید این دسته: {len(new_batch)}\n"
                    f"🔹 GUIDهای جدید این دسته: {new_guids}\n"
                    f"🔹 مجموع GUIDهای یکتا: {current_guids}\n"
                    f"🔹 زمان: {datetime.now().strftime('%H:%M:%S')}"
                )
                print(f"\n{report.replace('<b>','').replace('</b>','')}")

                if token and chat_id:
                    sync_send_telegram_message(token, chat_id, report)

                if iteration % BATCH_SEND_INTERVAL == 0:
                    zip_path = zip_db_file()
                    if zip_path and token and chat_id:
                        sync_send_telegram_document(token, chat_id, zip_path)
                        if os.path.exists(zip_path):
                            os.remove(zip_path)

                # احتمال استراحت در حین جلسه (۲۰٪)
                if random.random() < 0.2:
                    break_sec = random.uniform(300, 1200)  # ۵ تا ۲۰ دقیقه
                    logger.info(f"☕ استراحت {break_sec/60:.1f} دقیقه‌ای")
                    time.sleep(break_sec)
                    # بعد از استراحت ممکن است پنجره تمام شده باشد
                    if time.time() > session_deadline:
                        break

                # تأخیر بین دسته‌ها با توزیع انسانی
                human_delay_range(*GENERATE_DELAY_BETWEEN_BATCHES)

            logger.info("پنجرهٔ فعالیت تمام شد. انتظار برای پنجرهٔ بعدی...")

    except KeyboardInterrupt:
        print("\n⚠️ توقف دستی توسط کاربر. وضعیت ذخیره شد.")
        if token and chat_id:
            sync_send_telegram_message(token, chat_id, "⚠️ <b>عملیات با درخواست کاربر متوقف شد.</b>")
    finally:
        if use_multi:
            for m in manager.managers.values():
                m._flush_completed_to_db()
                m._save_state()
        else:
            manager._flush_completed_to_db()
            manager._save_state()
        if token and chat_id:
            zip_path = zip_db_file()
            if zip_path:
                sync_send_telegram_document(token, chat_id, zip_path)
                if os.path.exists(zip_path):
                    os.remove(zip_path)
        print("✅ اجرای برنامه پایان یافت.")

if __name__ == "__main__":
    main()