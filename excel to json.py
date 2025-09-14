# 🚗 ربط Google Drive
from google.colab import drive
drive.mount('/content/drive')

# 📦 تثبيت ijson
!pip install -q ijson

# 📚 الاستيراد
import gzip, ijson, re, csv, os, glob, sys
from html import unescape
from pathlib import Path
from time import perf_counter

# =========================
# 🔧 الإعدادات العامة
# =========================
JSON_DIR = "/content/drive/MyDrive/جوميا انجلش محتوي"
JSON_PATH = None  # أو حددي مسار ملف واحد هنا
CONTENT_LANG = "en"
OUT_DIR = "/content/drive/MyDrive/--JUMIA CONTENT/English content"
CHUNK_ROWS = 50_000
PROGRESS_EVERY = 100_000

# =========================
# 🧽 تنظيف HTML
# =========================
TAG_RE = re.compile(r"<[^>]+>")
def strip_html(s):
    return "" if s is None else unescape(TAG_RE.sub("", str(s)))

# =========================
# 🧠 دوال مساعدة
# =========================
def get_lang_block(p, lang):
    return (p.get("contents") or {}).get(lang) or {}

def get_product_uid(p, lang):
    blk = get_lang_block(p, lang)
    return blk.get("product_uid") or p.get("uid") or ""

def get_all_image_urls_joined(p, lang, sep=" | "):
    blk = get_lang_block(p, lang)
    pd = (blk.get("product_details") or {})
    imgs = pd.get("image_url")
    urls = []
    if isinstance(imgs, list):
        urls = [img.strip() for img in imgs if isinstance(img, str) and img.strip()]
    elif isinstance(imgs, str) and imgs.strip():
        urls = [imgs.strip()]
    return sep.join(urls)

def get_variations(lang_block, uid_str):
    for el in (lang_block.get("elements") or []):
        if str(el.get("uid")) == str(uid_str):
            return [v.get("value") for v in (el.get("variations") or []) if v.get("value")]
    return []

def join(vals, nohtml=False):
    if not vals: return ""
    return " | ".join(strip_html(v) if nohtml else str(v) for v in vals)

# =========================
# 🧾 رؤوس الأعمدة
# =========================
HEADERS = [
    "product_uid", "image_url", "urls",
    "page_title", "title",
    "description_html", "description",
    "specifications_html", "specifications",
    "img_alt", "meta_title", "meta_description"
]

# =========================
# 📝 أدوات الكتابة للـ CSV
# =========================
Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

def new_writer(idx):
    path = os.path.join(OUT_DIR, f"{CONTENT_LANG}_part_{idx:05d}.csv")
    f = open(path, "w", encoding="utf-8-sig", newline="")  # لتوافق Excel
    w = csv.DictWriter(f, fieldnames=HEADERS)
    w.writeheader()
    return f, w, path

def process_stream(src_iter, writer_state):
    f, writer, current_path, totals = writer_state
    rows_in_chunk, total_rows, chunk_idx, start = totals

    for i, product in enumerate(src_iter, start=1):
        try:
            blk = get_lang_block(product, CONTENT_LANG)
            row = {
                "product_uid": get_product_uid(product, CONTENT_LANG),
                "image_url": get_all_image_urls_joined(product, CONTENT_LANG),
                "urls": " | ".join(product.get("urls", [])),
                "page_title":         join(get_variations(blk, "1")),
                "title":              join(get_variations(blk, "2")),
                "description_html":   join(get_variations(blk, "3")),
                "description":        join(get_variations(blk, "3"), nohtml=True),
                "specifications_html":join(get_variations(blk, "4")),
                "specifications":     join(get_variations(blk, "4"), nohtml=True),
                "img_alt":            join(get_variations(blk, "10")),
                "meta_title":         join(get_variations(blk, "100")),
                "meta_description":   join(get_variations(blk, "101")),
            }

            writer.writerow(row)
            rows_in_chunk += 1
            total_rows += 1

            if rows_in_chunk >= CHUNK_ROWS:
                f.close()
                print(f"💾 تم حفظ {rows_in_chunk} صف → {current_path}")
                chunk_idx += 1
                rows_in_chunk = 0
                f, writer, current_path = new_writer(chunk_idx)

            if total_rows % PROGRESS_EVERY == 0:
                elapsed = perf_counter() - start
                print(f"⏱️ تقدّم كلي: {total_rows:,} صف • زمن: {elapsed/60:.1f} دقيقة")

        except Exception as e:
            print(f"⚠️ تخطّي صف بسبب خطأ: {e}", file=sys.stderr)
            continue

    return (f, writer, current_path, (rows_in_chunk, total_rows, chunk_idx, start))

# =========================
# ▶️ التشغيل
# =========================
rows_in_chunk = 0
chunk_idx = 1
total_rows = 0
f, writer, current_path = new_writer(chunk_idx)
start = perf_counter()
print(f"🚀 التصدير إلى: {OUT_DIR}")

if JSON_PATH:
    cands = [JSON_PATH]
else:
    cands = glob.glob(os.path.join(JSON_DIR, "*.json")) + glob.glob(os.path.join(JSON_DIR, "*.json.gz"))
    cands = sorted(cands)

if not cands:
    raise FileNotFoundError(f"❌ لم يتم العثور على ملفات JSON في: {JSON_PATH or JSON_DIR}")

print(f"📦 عدد الملفات: {len(cands)}")
for p in cands: print(" •", p)

for file_idx, path in enumerate(cands, start=1):
    try:
        open_fn = gzip.open if str(path).lower().endswith(".gz") else open
        print(f"\n➡️ الملف [{file_idx}/{len(cands)}]: {path}")
        with open_fn(path, "rt", encoding="utf-8") as src:
            parser = ijson.items(src, "item")
            f, writer, current_path, (rows_in_chunk, total_rows, chunk_idx, start) = process_stream(
                parser,
                (f, writer, current_path, (rows_in_chunk, total_rows, chunk_idx, start))
            )
    except Exception as e:
        print(f"❌ خطأ بالملف {path}: {e}", file=sys.stderr)
        continue

f.close()
elapsed = perf_counter() - start
print(f"\n✅ تم الانتهاء. عدد الصفوف: {total_rows:,}")
print(f"📁 النتائج في: {OUT_DIR}")
print(f"⏳ الزمن الكلي: {elapsed/60:.2f} دقيقة")
