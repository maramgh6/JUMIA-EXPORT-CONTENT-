import os, json, io, time
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List
import requests
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import ConnectionError, ReadTimeout, ChunkedEncodingError, JSONDecodeError
from http.client import IncompleteRead

# Optional: Use orjson for faster JSON serialization
try:
    import orjson
    def dumps_fast(obj: Any) -> bytes:
        return orjson.dumps(obj, option=orjson.OPT_NON_STR_KEYS)
except Exception:
    def dumps_fast(obj: Any) -> bytes:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

# ========== SETTINGS ==========
API_KEY = "3185613-d3f4-4594-be6d-3b0e50164396"
PROJECT_ID_ENGLISH = "PUT_EN_PROJECT_ID_HERE"  # <-- Replace with actual ID if available
USE_ENGLISH_PROJECT = True
FALLBACK_PROJECT_ID = "511714237668"
FORCE_API_LANGUAGE_EN = True

ENDPOINT_URL = "https://prod.fodoole.com/products/list_products/"
DOMAIN = "www.jumia.com.eg"

# Save as plain JSON, not .gz
WRITE_GZIP = False

# New: JSON file every 50,000 items
PAGE_SIZE = 5000
ITEMS_PER_FILE = 50000
CONNECT_TIMEOUT = 8
READ_TIMEOUT = 60
MAX_ATTEMPTS = 5
LOG_EVERY_PAGES = 1

DEST_FOLDER_SPLIT = "/content/drive/MyDrive/جوميا انجلش محتوي"

# ========== Session with Retry ==========
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=6, connect=6, read=6, status=6,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=32))
    s.headers.update({
        "accept": "application/json",
        "X-API-Key": API_KEY,
        "Content-Type": "application/json"
    })
    return s

# ========== English Filter ==========
def is_english_item(item: Dict[str, Any]) -> bool:
    for k in item.keys():
        if k.endswith("_en"):
            return True
    lang = item.get("language") or item.get("lang") or item.get("locale")
    if isinstance(lang, str) and lang.strip().lower().startswith("en"):
        return True
    return True

# ========== English Cleaner ==========
def build_object_en(item: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    seen_bases = set()
    for k, v in item.items():
        if k.endswith("_en"):
            base = k[:-3]
            seen_bases.add(base)
            out[base] = v
        else:
            out.setdefault(k, v)

    for base in seen_bases:
        en_key = base + "_en"
        if en_key in item:
            out[base] = item[en_key]

    for base in ("title", "name", "description", "category", "brand"):
        en_key = base + "_en"
        if en_key in item:
            out[base] = item[en_key]
        elif base in item:
            out[base] = item[base]

    # Force language to 'en'
    if "language" in item and isinstance(item["language"], str):
        out["language"] = "en"
    elif "lang" in item and isinstance(item["lang"], str):
        out["lang"] = "en"
    elif "locale" in item and isinstance(item["locale"], str):
        out["locale"] = "en"

    return out

# ========== Fetch Single Page ==========
def fetch_page(session: requests.Session, start_key: Optional[str], page_num: int) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    payload = {
        "project_id": PROJECT_ID_ENGLISH if USE_ENGLISH_PROJECT and PROJECT_ID_ENGLISH != "PUT_EN_PROJECT_ID_HERE" else FALLBACK_PROJECT_ID,
        "domain": DOMAIN,
        "page_size": PAGE_SIZE,
        "start_key": start_key
    }
    if FORCE_API_LANGUAGE_EN:
        payload["language"] = "en"

    resp = session.post(ENDPOINT_URL, json=payload, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
    try:
        data = resp.json()
    except (JSONDecodeError, ValueError, IncompleteRead, ChunkedEncodingError) as e:
        raise RuntimeError(f"JSON parse error on page {page_num}: {e}") from e

    items = data.get("items", [])
    next_key = data.get("last_row_key")
    return items, next_key

# ========== Streaming Writer ==========
class StreamingJSONArrayWriter:
    def __init__(self, out_path: str, gzip_enabled: bool):
        self.out_path = out_path
        self.gzip_enabled = gzip_enabled
        Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)
        self._fh = open(out_path, "wb")
        self._first = True
        self._fh.write(b"[")

    @property
    def path(self) -> str:
        return self.out_path

    def write_many(self, items: List[Dict[str, Any]]) -> int:
        if not items:
            return 0
        buf = io.BytesIO()
        for it in items:
            if self._first:
                self._first = False
            else:
                buf.write(b",")
            buf.write(dumps_fast(it))
        self._fh.write(buf.getvalue())
        return len(items)

    def close(self):
        try:
            self._fh.write(b"]")
            self._fh.close()
        except Exception:
            pass

# ========== MAIN SCRIPT ==========
def fetch_all_products_english_only_split():
    session = make_session()
    Path(DEST_FOLDER_SPLIT).mkdir(parents=True, exist_ok=True)

    total_items = 0
    page_num = 1
    start_key = None
    file_index = 1
    buffer: List[Dict[str, Any]] = []

    effective_project = (PROJECT_ID_ENGLISH if USE_ENGLISH_PROJECT and PROJECT_ID_ENGLISH != "PUT_EN_PROJECT_ID_HERE"
                         else FALLBACK_PROJECT_ID)
    t0 = time.time()

    print(f"🚀 البدء | بروجيكت: {effective_project} | DOMAIN={DOMAIN} | PAGE_SIZE={PAGE_SIZE}")
    print(f"📂 الإخراج إلى: {DEST_FOLDER_SPLIT}")

    def write_buffered_items(buf: List[Dict[str, Any]], index: int):
        filename = f"jumia_en_content_{index}.json"
        out_path = os.path.join(DEST_FOLDER_SPLIT, filename)
        writer = StreamingJSONArrayWriter(out_path, WRITE_GZIP)
        writer.write_many(buf)
        writer.close()
        print(f"💾 ملف جديد: {filename} | عدد العناصر: {len(buf)}")
        return out_path

    while True:
        attempt = 0
        while True:
            attempt += 1
            try:
                items, next_key = fetch_page(session, start_key, page_num)
                break
            except (ConnectionError, ReadTimeout, RuntimeError, IncompleteRead, ChunkedEncodingError) as e:
                if attempt >= MAX_ATTEMPTS:
                    print(f"❌ الصفحة {page_num}: فشل بعد {attempt} محاولات — {e}")
                    raise
                sleep_s = min(30, 0.5 * (2 ** (attempt - 1)))
                print(f"⚠️ الصفحة {page_num}: {e} — إعادة المحاولة {attempt}/{MAX_ATTEMPTS} خلال {sleep_s:.1f}s")
                time.sleep(sleep_s)

        if not items:
            print("✅ نهاية البيانات.")
            break

        eng_raw = [it for it in items if is_english_item(it)]
        en_items = [build_object_en(it) for it in eng_raw]
        buffer.extend(en_items)

        while len(buffer) >= ITEMS_PER_FILE:
            to_write = buffer[:ITEMS_PER_FILE]
            buffer = buffer[ITEMS_PER_FILE:]
            write_buffered_items(to_write, file_index)
            file_index += 1
            total_items += len(to_write)

        print(f"📦 صفحة {page_num} | المستلم: {len(items)} | EN: {len(en_items)} | الإجمالي: {total_items + len(buffer):,}")
        page_num += 1
        start_key = next_key
        if not next_key:
            break

    # Save remaining buffer
    if buffer:
        write_buffered_items(buffer, file_index)
        total_items += len(buffer)

    dt = time.time() - t0
    print(f"🎉 تم! الإجمالي: {total_items:,} عنصر | الزمن: {dt:.1f} ثانية")
    print(f"📁 جميع الملفات موجودة في: {DEST_FOLDER_SPLIT}")

# ========== RUN ==========
fetch_all_products_english_only_split()
