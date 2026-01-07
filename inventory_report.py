#!/usr/bin/env python3

import os
import asyncio
import random
import email.utils
from datetime import datetime, timedelta, timezone
import smtplib
from email.message import EmailMessage
from email.mime.image import MIMEImage
from collections import deque
import pickle

import aiohttp
import pandas as pd
from tqdm import tqdm


# =============================== API CONFIG ===============================
OPERATORS_API = "https://gds.ticketsimply.com/gds/api/operators.json?api_key=TSVMQUAPI06318478"
SCHEDULE_API_TEMPLATE = (
    "https://gds.ticketsimply.com/gds/api/operator_schedules/{operator_id}/{date}.json"
    "?api-key=TSVRTDAPI20684126"
)

# =============================== CONCURRENCY ==============================
MIN_CONC = 20
MAX_CONC = 60
START_CONC = 30

REQUEST_TIMEOUT = 25
MAX_RETRIES_PER_OP = 10
SLOW_MAX_RETRIES_PER_OP = 25
SLOW_REQUEST_TIMEOUT = 45

DAYS_TO_FETCH = 7  # Next 7 days

OUT_DIR = "output"
os.makedirs(OUT_DIR, exist_ok=True)
CHECKPOINT_FILE = os.path.join(OUT_DIR, "progress.pkl")

# Problematic IDs (slower path)
PROBLEM_OPERATOR_IDS = {
    136, 1084, 1728, 1785, 2314, 2436, 2610, 3196, 3197, 3291, 3297,
    3502, 3633, 3956, 4575, 4606, 4647, 4733, 4845, 4927, 5011, 5033,
    5054, 5109, 5350, 5362, 5372, 5399, 5401, 5415, 5424, 5427, 5439,
    5440, 5456, 5471, 5472, 5479, 5483, 5497, 5499, 5504, 5505, 5507,
    5512, 5515, 5516, 5517, 5525, 5526, 5528, 5532, 5535, 5536, 5538,
    5539, 5540, 5541, 5543, 5545, 5550, 5555, 5559, 5560, 5562, 5563,
    5565, 5566, 5568, 5569, 5571, 5572, 5573, 5574, 5575, 5577, 5578,
    5580, 5581, 5582, 5584, 5585, 5586, 5587, 5588, 5590, 5591, 5592,
    5593, 5598, 5602, 5603, 5604, 5607, 5615, 5624, 5630, 5634, 5639,
    5649, 5656, 5660, 5661, 5673, 5681, 5709, 5722, 5724, 5728, 5731,
    5732, 5739, 5891,
}

# =============================== ZOHO MAIL CONFIG =========================
SMTP_HOST = "smtp.zoho.in"
SMTP_PORT = 587
SMTP_USER = "madhu.l@hopzy.in"
SMTP_PASSWORD = "JqkGLkfkTf0n"
SENDER_EMAIL = SMTP_USER
MANAGER_EMAIL = ("madhu.l@hopzy.in","avinash.sk@hopzy.in")


# =============================== BACKOFF HELPERS ==========================
def compute_backoff(base_backoff, max_backoff, attempt, factor=1.0):
    exp = base_backoff * (2 ** (attempt - 1)) * factor
    cap = min(max_backoff, exp)
    return random.uniform(0, cap)


def retry_after_sleep_seconds(resp, base_backoff, max_backoff, attempt, verbose, tag):
    retry_after_raw = resp.headers.get("Retry-After")
    sleep_seconds = None

    if retry_after_raw is not None:
        retry_after_raw = retry_after_raw.strip()
        if retry_after_raw.isdigit():
            sleep_seconds = int(retry_after_raw)
        else:
            try:
                dt = email.utils.parsedate_to_datetime(retry_after_raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                diff = (dt - now).total_seconds()
                if diff > 0:
                    sleep_seconds = int(diff)
            except Exception as e:
                if verbose:
                    print(f"[{tag}] Could not parse Retry-After '{retry_after_raw}': {e}")

    if sleep_seconds is None:
        sleep_seconds = compute_backoff(base_backoff, max_backoff, attempt)

    if verbose:
        print(
            f"[{tag}] 429 rate limit, Retry-After='{retry_after_raw}', "
            f"sleeping {sleep_seconds:.2f}s"
        )
    return sleep_seconds


# =============================== RETRYING HTTP ============================
async def fetch_json_with_retry(
    session,
    url,
    tag,
    max_attempts,
    timeout_seconds,
    base_backoff=1.0,
    max_backoff=60.0,
    verbose=False,
):
    for attempt in range(1, max_attempts + 1):
        try:
            async with session.get(url, timeout=timeout_seconds) as resp:
                status = resp.status
                if verbose:
                    print(f"[{tag}] Attempt {attempt}: HTTP {status} for {url}")

                if status == 200:
                    try:
                        data = await resp.json()
                        return data, "success"
                    except Exception as e:
                        if verbose:
                            print(f"[{tag}] Attempt {attempt}: JSON parse error: {e}")
                        wait = compute_backoff(base_backoff, max_backoff, attempt)
                        await asyncio.sleep(wait)
                        continue

                if status in (404, 403):
                    return None, "not_found"

                if status == 429:
                    sleep_seconds = retry_after_sleep_seconds(
                        resp, base_backoff, max_backoff, attempt, verbose, tag
                    )
                    await asyncio.sleep(sleep_seconds)
                    continue

                if 500 <= status < 600:
                    wait = compute_backoff(base_backoff, max_backoff, attempt)
                    if verbose:
                        print(
                            f"[{tag}] Attempt {attempt}: {status} server error, "
                            f"sleeping {wait:.2f}s"
                        )
                    await asyncio.sleep(wait)
                    continue

                wait = compute_backoff(base_backoff, max_backoff, attempt, factor=0.7)
                if verbose:
                    print(
                        f"[{tag}] Attempt {attempt}: HTTP {status}, "
                        f"sleeping {wait:.2f}s"
                    )
                await asyncio.sleep(wait)
                continue

        except asyncio.TimeoutError:
            wait = compute_backoff(base_backoff, max_backoff, attempt)
            if verbose:
                print(f"[{tag}] Attempt {attempt}: timeout, sleeping {wait:.2f}s")
            await asyncio.sleep(wait)
            continue

        except Exception as e:
            wait = compute_backoff(base_backoff, max_backoff, attempt)
            if verbose:
                print(
                    f"[{tag}] Attempt {attempt}: exception {type(e).__name__} – {e}, "
                    f"sleeping {wait:.2f}s"
                )
            await asyncio.sleep(wait)
            continue

    if verbose:
        print(f"[{tag}] Reached max attempts ({max_attempts}) without success.")
    return None, "max_retries_failed"


# =============================== FETCH OPERATORS ==========================
async def fetch_operators(session):
    print("Fetching operators list...")
    data, status = await fetch_json_with_retry(
        session,
        OPERATORS_API,
        "operators_api",
        max_attempts=5,
        timeout_seconds=REQUEST_TIMEOUT,
        verbose=True,
    )

    if not data:
        print(f"Failed to fetch operators: {status}")
        return {}

    result = data.get("result", [])
    if not result or len(result) < 2:
        return {}

    headers = result[0]
    rows = result[1:]

    try:
        id_idx = headers.index("id")
        name_idx = headers.index("name")
    except ValueError:
        id_idx, name_idx = 0, 1

    operator_map = {}
    for row in rows:
        if len(row) > max(id_idx, name_idx):
            op_id = row[id_idx]
            op_name = row[name_idx]
            if op_id is not None:
                operator_map[int(op_id)] = str(op_name)

    return operator_map


# =============================== FETCH SCHEDULES ==========================
async def fetch_schedule_for_operator(session, operator_id, date_str):
    url = SCHEDULE_API_TEMPLATE.format(operator_id=operator_id, date=date_str)
    data, status = await fetch_json_with_retry(
        session,
        url,
        f"op-{operator_id}",
        max_attempts=MAX_RETRIES_PER_OP,
        timeout_seconds=REQUEST_TIMEOUT,
        verbose=False,
    )

    if data is None:
        return 0, 0, status

    result = data.get("result", [])
    if not result or len(result) < 2:
        return 0, 0, "no_data"

    headers = result[0]
    rows = result[1:]

    try:
        id_idx = headers.index("id")
    except ValueError:
        id_idx = 0
    try:
        route_id_idx = headers.index("route_id")
    except ValueError:
        route_id_idx = -1

    schedule_ids = set()
    route_ids = set()

    for row in rows:
        if id_idx < len(row) and row[id_idx]:
            schedule_ids.add(str(row[id_idx]))
        if route_id_idx >= 0 and route_id_idx < len(row) and row[route_id_idx]:
            route_ids.add(str(row[route_id_idx]))

    return len(schedule_ids), len(route_ids), "success"


async def fetch_schedule_for_operator_slow(session, operator_id, date_str):
    url = SCHEDULE_API_TEMPLATE.format(operator_id=operator_id, date=date_str)
    data, status = await fetch_json_with_retry(
        session,
        url,
        f"slow-op-{operator_id}",
        max_attempts=SLOW_MAX_RETRIES_PER_OP,
        timeout_seconds=SLOW_REQUEST_TIMEOUT,
        verbose=True,
    )

    if data is None:
        return 0, 0, status

    result = data.get("result", [])
    if not result or len(result) < 2:
        return 0, 0, "no_data"

    headers = result[0]
    rows = result[1:]

    try:
        id_idx = headers.index("id")
    except ValueError:
        id_idx = 0
    try:
        route_id_idx = headers.index("route_id")
    except ValueError:
        route_id_idx = -1

    schedule_ids = set()
    route_ids = set()

    for row in rows:
        if id_idx < len(row) and row[id_idx]:
            schedule_ids.add(str(row[id_idx]))
        if route_id_idx >= 0 and route_id_idx < len(row) and row[route_id_idx]:
            route_ids.add(str(row[route_id_idx]))

    return len(schedule_ids), len(route_ids), "success"


# =============================== CHECKPOINT ===============================
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "rb") as f:
                return pickle.load(f)
        except Exception:
            return {}
    return {}


def save_checkpoint(data):
    try:
        with open(CHECKPOINT_FILE, "wb") as f:
            pickle.dump(data, f)
    except Exception:
        pass


def clear_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)


# =============================== HTML EMAIL BUILDER =======================
def build_html_summary(operator_map, daily_stats, overall_stats):
    first_date_str = min(daily_stats.keys())
    last_date_str = max(daily_stats.keys())
    first_display = datetime.strptime(first_date_str, "%Y-%m-%d").strftime("%d-%m-%Y")
    last_display = datetime.strptime(last_date_str, "%Y-%m-%d").strftime("%d-%m-%Y")

    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; line-height: 1.5; color: #333; margin:0; padding:0; background:#f5f5f5;">
      <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5; padding:20px 0;">
        <tr>
         <td align="center">
           <table width="900" cellpadding="0" cellspacing="0" style="background:#ffffff; border:1px solid #d9d9d9; max-width:900px;">
             
             <!-- Header with logo and title -->
             <tr>
               <td align="left" style="padding:14px 20px 8px 20px; border-bottom:1px solid #e0e0e0;">
                 <table width="100%" cellpadding="0" cellspacing="0">
                   <tr>
                     <td align="left" valign="middle">
                       <img src="cid:bitla_logo" alt="Bitla" style="height:32px;">
                     </td>
                     <td align="right" valign="middle" style="font-size:18px; font-weight:bold; color:#0066cc;">
                       7-Day Bitla Inventory Report
                     </td>
                   </tr>
                 </table>
               </td>
             </tr>

             <!-- Period info -->
             <tr>
               <td style="padding:10px 20px; font-size:12px; color:#555; background:#f8f9fa; border-bottom:1px solid #e0e0e0;">
                 <div><strong>Period:</strong> {first_display} to {last_display}</div>
                 <div><strong>Generated:</strong> {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</div>
                 <div><strong>Total Operators:</strong> {len(operator_map):,}</div>
               </td>
             </tr>

             <!-- Overall Summary -->
             <tr>
               <td style="padding:16px 20px 8px 20px;">
                 <div style="font-size:14px; font-weight:bold; color:#333; margin-bottom:6px;">Overall Summary (Next 7 Days)</div>
                 <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; font-size:13px; margin-bottom:16px;">
                   <tr style="background:#0066cc; color:#ffffff;">
                     <th align="left" style="padding:8px 10px; border:1px solid #d9d9d9;">Metric</th>
                     <th align="right" style="padding:8px 10px; border:1px solid #d9d9d9;">Value</th>
                   </tr>
                   <tr>
                     <td style="padding:8px 10px; border:1px solid #d9d9d9;">Total Operators</td>
                     <td align="right" style="padding:8px 10px; border:1px solid #d9d9d9; font-weight:bold;">{len(operator_map):,}</td>
                   </tr>
                   <tr style="background:#f8f9fa;">
                     <td style="padding:8px 10px; border:1px solid #d9d9d9;">Operators With Data (Any Day)</td>
                     <td align="right" style="padding:8px 10px; border:1px solid #d9d9d9; color:#27ae60; font-weight:bold;">{overall_stats['with_data']:,}</td>
                   </tr>
                   <tr>
                     <td style="padding:8px 10px; border:1px solid #d9d9d9;">Operators Without Data (All Days)</td>
                     <td align="right" style="padding:8px 10px; border:1px solid #d9d9d9; color:#e74c3c; font-weight:bold;">{overall_stats['without_data']:,}</td>
                   </tr>
                   <tr style="background:#f8f9fa;">
                     <td style="padding:8px 10px; border:1px solid #d9d9d9;">Total Unique Schedules (7 Days)</td>
                     <td align="right" style="padding:8px 10px; border:1px solid #d9d9d9; font-weight:bold;">{overall_stats['total_schedules']:,}</td>
                   </tr>
                   <tr>
                     <td style="padding:8px 10px; border:1px solid #d9d9d9;">Total Unique Routes (7 Days)</td>
                     <td align="right" style="padding:8px 10px; border:1px solid #d9d9d9; font-weight:bold;">{overall_stats['total_routes']:,}</td>
                   </tr>
                   <tr style="background:#fff3cd;">
                     <td style="padding:8px 10px; border:1px solid #d9d9d9;">Overall Success Rate</td>
                     <td align="right" style="padding:8px 10px; border:1px solid #d9d9d9; font-weight:bold;">{overall_stats['success_rate']:.1f}%</td>
                   </tr>
                 </table>
               </td>
             </tr>

             <!-- Daily stats -->
             <tr>
               <td style="padding:0 20px 16px 20px;">
                 <div style="font-size:14px; font-weight:bold; color:#333; margin-bottom:6px;">Daily stats</div>
                 <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; font-size:12px;">
                   <tr style="background:#0066cc; color:#ffffff;">
                     <th style="padding:8px 10px; border:1px solid #d9d9d9;" align="left">Date</th>
                     <th style="padding:8px 10px; border:1px solid #d9d9d9;" align="right">With Data</th>
                     <th style="padding:8px 10px; border:1px solid #d9d9d9;" align="right">Without Data</th>
                     <th style="padding:8px 10px; border:1px solid #d9d9d9;" align="right">Total Schedules</th>
                     <th style="padding:8px 10px; border:1px solid #d9d9d9;" align="right">Total Routes</th>
                     <th style="padding:8px 10px; border:1px solid #d9d9d9;" align="right">Success Rate</th>
                   </tr>
    """

    for date_str, stats in daily_stats.items():
        date_display = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")
        row_bg = "#f8f9fa" if int(stats['day_num']) % 2 == 0 else "#ffffff"
        html_content += f"""
                   <tr style="background:{row_bg};">
                     <td style="padding:6px 8px; border:1px solid #d9d9d9;"><strong>{date_display}</strong></td>
                     <td style="padding:6px 8px; border:1px solid #d9d9d9; color:#27ae60;" align="right"><strong>{stats['with_data']:,}</strong></td>
                     <td style="padding:6px 8px; border:1px solid #d9d9d9; color:#e74c3c;" align="right"><strong>{stats['without_data']:,}</strong></td>
                     <td style="padding:6px 8px; border:1px solid #d9d9d9;" align="right"><strong>{stats['total_schedules']:,}</strong></td>
                     <td style="padding:6px 8px; border:1px solid #d9d9d9;" align="right"><strong>{stats['total_routes']:,}</strong></td>
                     <td style="padding:6px 8px; border:1px solid #d9d9d9;" align="right"><strong>{stats['success_rate']:.1f}%</strong></td>
                   </tr>
        """

    html_content += """
                 </table>
               </td>
             </tr>

             <!-- Footer -->
             <tr>
               <td style="padding:14px 20px; font-size:11px; color:#777; border-top:1px solid #e0e0e0; background:#f8f9fa;">
                 <strong>Attachments:</strong> 7-day Excel report (All 7 days data, Daily Summary, Overall Summary).<br>
                 <span>Automated 7-day Bitla inventory report.</span>
               </td>
             </tr>
           </table>
         </td>
       </tr>
      </table>
    </body>
    </html>
    """
    return html_content


# =============================== EMAIL SENDER =============================
def send_report_email(subject, html_body, attachment, cc_emails=None):
    msg = EmailMessage()
    msg["From"] = SENDER_EMAIL
    msg["To"] = ", ".join(MANAGER_EMAIL)
    if cc_emails:
        msg["Cc"] = ", ".join(cc_emails)
    msg["Subject"] = subject

    # Plain-text fallback + HTML
    msg.set_content("This email contains an HTML Bitla inventory report.")
    html_part = msg.add_alternative(html_body, subtype="html")

    # Attach inline Bitla logo (bitla_logo.jpg in current folder)
    try:
        with open("image.jpg", "rb") as img_file:
            logo_bytes = img_file.read()
        logo = MIMEImage(logo_bytes)
        logo.add_header("Content-ID", "<bitla_logo>")
        logo.add_header("Content-Disposition", "inline", filename="logo.jpg")
        html_part.attach(logo)
        print("Bitla logo attached inline.")
    except Exception as e:
        print(f"Logo not attached: {e}")

    # Excel attachment
    if os.path.exists(attachment):
        filename = os.path.basename(attachment)
        with open(attachment, "rb") as f:
            data = f.read()
        msg.add_attachment(
            data,
            maintype="application",
            subtype="octet-stream",
            filename=filename,
        )

    recipients = list(MANAGER_EMAIL)
    if cc_emails:
        recipients = recipients + list(cc_emails)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg, from_addr=SENDER_EMAIL, to_addrs=recipients)


# =============================== MAIN ASYNC ===============================
async def main_async():
    print("\n" + "=" * 80)
    print(f"7-DAY OPERATOR SCANNER – Next {DAYS_TO_FETCH} Days")
    print("=" * 80 + "\n")

    # Generate dates for next 7 days
    base_date = datetime.now().date() + timedelta(days=1)
    fetch_dates = [(base_date + timedelta(days=i)).isoformat() for i in range(DAYS_TO_FETCH)]

    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=30)
    connector = aiohttp.TCPConnector(limit=0, force_close=False, enable_cleanup_closed=True)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        operator_map = await fetch_operators(session)
        if not operator_map:
            print("Failed to fetch operators")
            return

        print(f"Loaded {len(operator_map):,} operators")
        print(f"ID Range: {min(operator_map.keys())} to {max(operator_map.keys())}\n")

        # Process each day
        all_results = {}
        daily_stats = {}
        overall_stats = {
            'with_data': 0, 'without_data': 0, 'total_schedules': 0, 'total_routes': 0
        }

        for day_num, fetch_date in enumerate(fetch_dates, 1):
            print(f"Processing Day {day_num}: {fetch_date}")

            checkpoint = load_checkpoint()
            processed_ids = set(checkpoint.get(fetch_date, {}).keys())

            operator_ids = sorted(operator_map.keys())
            pending_ids = [op_id for op_id in operator_ids if op_id not in processed_ids]

            if not pending_ids:
                pending_ids = operator_ids
                checkpoint[fetch_date] = {}
                processed_ids = set()

            print(f"Operators to process for {fetch_date}: {len(pending_ids):,}\n")

            day_results = checkpoint.get(fetch_date, {}).copy()
            completed = len(day_results)
            with_data_day = sum(1 for r in day_results.values() if r["schedules"] > 0)
            without_data_day = len(day_results) - with_data_day
            failed_after_retries = 0

            concurrency = START_CONC
            semaphore = asyncio.Semaphore(concurrency)
            recent_success = deque(maxlen=200)

            start_time = datetime.now()
            pbar = tqdm(total=len(pending_ids), desc=f"Day {day_num}", unit="op", ncols=120)

            async def worker(op_id):
                nonlocal concurrency, semaphore, completed, with_data_day, without_data_day, failed_after_retries

                try:
                    async with semaphore:
                        if op_id in PROBLEM_OPERATOR_IDS:
                            schedules, routes, status = await fetch_schedule_for_operator_slow(
                                session, op_id, fetch_date
                            )
                        else:
                            schedules, routes, status = await fetch_schedule_for_operator(
                                session, op_id, fetch_date
                            )

                        day_results[op_id] = {"schedules": schedules, "routes": routes, "status": status}

                        if schedules > 0:
                            with_data_day += 1
                        else:
                            without_data_day += 1

                        if status == "max_retries_failed":
                            failed_after_retries += 1

                        recent_success.append(1 if status != "max_retries_failed" else 0)

                except Exception as e:
                    day_results[op_id] = {
                        "schedules": 0,
                        "routes": 0,
                        "status": f"exception_{type(e).__name__}",
                    }
                    without_data_day += 1
                    failed_after_retries += 1
                    recent_success.append(0)

                completed += 1

                if completed % 50 == 0:
                    checkpoint[fetch_date] = day_results
                    save_checkpoint(checkpoint)

                elapsed = (datetime.now() - start_time).total_seconds()
                current_completed = completed - len(checkpoint.get(fetch_date, {}))
                speed = current_completed / elapsed if elapsed > 0 else 0.0
                remaining = len(pending_ids) - current_completed
                eta = remaining / speed if speed > 0 else 0.0

                if len(recent_success) >= 100:
                    success_rate_local = sum(recent_success) / len(recent_success)
                    if success_rate_local > 0.96 and speed > 30 and concurrency < MAX_CONC:
                        new_conc = min(MAX_CONC, concurrency + 5)
                        if new_conc != concurrency:
                            concurrency = new_conc
                            semaphore = asyncio.Semaphore(concurrency)
                    elif success_rate_local < 0.85 and concurrency > MIN_CONC:
                        new_conc = max(MIN_CONC, concurrency - 5)
                        if new_conc != concurrency:
                            concurrency = new_conc
                            semaphore = asyncio.Semaphore(concurrency)

                pbar.set_postfix({
                    "speed": f"{speed:.0f}/s",
                    "ETA": f"{int(eta/60)}m",
                    "conc": concurrency,
                    "with": with_data_day,
                    "without": without_data_day,
                    "fail": failed_after_retries,
                })
                pbar.update(1)

            active_tasks = set()
            idx = 0
            window_size = max(200, concurrency * 4)

            try:
                while idx < len(pending_ids) or active_tasks:
                    while len(active_tasks) < window_size and idx < len(pending_ids):
                        op_id = pending_ids[idx]
                        task = asyncio.create_task(worker(op_id))
                        active_tasks.add(task)
                        idx += 1

                    if active_tasks:
                        done, pending = await asyncio.wait(
                            active_tasks, return_when=asyncio.FIRST_COMPLETED
                        )
                        active_tasks = set(pending)
                        window_size = max(window_size, concurrency * 4)
            finally:
                pbar.close()

            checkpoint[fetch_date] = day_results
            save_checkpoint(checkpoint)
            all_results[fetch_date] = day_results

            day_total_schedules = sum(r["schedules"] for r in day_results.values())
            day_total_routes = sum(r["routes"] for r in day_results.values())
            daily_stats[fetch_date] = {
                'day_num': day_num,
                'with_data': with_data_day,
                'without_data': without_data_day,
                'total_schedules': day_total_schedules,
                'total_routes': day_total_routes,
                'success_rate': (with_data_day / len(operator_map) * 100) if operator_map else 0
            }

            overall_stats['total_schedules'] += day_total_schedules
            overall_stats['total_routes'] += day_total_routes

        print("\nBuilding comprehensive 7-day output files...")

        operators_with_data_any_day = set()
        for day_results in all_results.values():
            for op_id, data in day_results.items():
                if data["schedules"] > 0:
                    operators_with_data_any_day.add(op_id)

        overall_stats['with_data'] = len(operators_with_data_any_day)
        overall_stats['without_data'] = len(operator_map) - overall_stats['with_data']
        overall_stats['success_rate'] = (overall_stats['with_data'] / len(operator_map) * 100) if operator_map else 0

        all_rows = []
        for fetch_date in fetch_dates:
            inventory_label = datetime.strptime(fetch_date, "%Y-%m-%d").strftime("%d-%m-%Y")
            day_results = all_results[fetch_date]

            for op_id in sorted(operator_map.keys()):
                op_name = operator_map[op_id]
                data = day_results.get(op_id, {"schedules": 0, "routes": 0, "status": "missing"})

                all_rows.append({
                    "operator_id": op_id,
                    "operator_name": op_name,
                    "fetch_date": fetch_date,
                    "Inventory Date": inventory_label,
                    "status": "With Data" if data["schedules"] > 0 else "Without Data",
                    "unique_route_count": data["routes"],
                    "unique_schedule_count": data["schedules"],
                    "api_response": data["status"],
                })

        df_all = pd.DataFrame(all_rows)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        xlsx_file = os.path.join(
            OUT_DIR, f"Operator_Inventory_7Days_{len(operator_map)}_operators_{timestamp}.xlsx"
        )

        # ==================== NEW PIVOT SHEETS ====================
        pivot_data = []
        for row in all_rows:
            pivot_data.append({
                'operator_id': row['operator_id'],
                'operator_name': row['operator_name'],
                'inventory_date': row['Inventory Date'],
                'unique_routes': row['unique_route_count'],
                'unique_schedules': row['unique_schedule_count']
            })

        pivot_df = pd.DataFrame(pivot_data)

        # Pivot for Routes (SUM)
        pivot_routes = pivot_df.pivot_table(
            index=['operator_id', 'operator_name'],
            columns='inventory_date',
            values='unique_routes',
            aggfunc='sum',
            fill_value=0
        ).round(0).astype(int)

        # Pivot for Schedules (SUM)
        pivot_schedules = pivot_df.pivot_table(
            index=['operator_id', 'operator_name'],
            columns='inventory_date',
            values='unique_schedules',
            aggfunc='sum',
            fill_value=0
        ).round(0).astype(int)

        # ==================== EXCEL WRITER WITH ALL 5 SHEETS ====================
        with pd.ExcelWriter(xlsx_file, engine="openpyxl") as writer:
            # 1. All data sheet
            df_all.to_excel(writer, sheet_name="All_7Days_Data", index=False)

            # 2. Daily Summary sheet
            summary_rows = []
            for date_str, stats in daily_stats.items():
                date_display = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")
                summary_rows.append({
                    "Date": date_display,
                    "With_Data": stats['with_data'],
                    "Without_Data": stats['without_data'],
                    "Schedules": stats['total_schedules'],
                    "Routes": stats['total_routes'],
                    "Success_Rate": f"{stats['success_rate']:.1f}%",
                })

            summary_df = pd.DataFrame(summary_rows)
            summary_df.to_excel(writer, sheet_name="Daily_Summary", index=False)

            # 3. Overall Summary sheet
            overall_summary = {
                "Metric": [
                    "Total Operators",
                    "Operators With Data (Any Day)",
                    "Operators Without Data (All Days)",
                    "Total Unique Schedules (7 Days)",
                    "Total Unique Routes (7 Days)",
                    "Overall Success Rate",
                ],
                "Value": [
                    len(operator_map),
                    overall_stats['with_data'],
                    overall_stats['without_data'],
                    overall_stats['total_schedules'],
                    overall_stats['total_routes'],
                    f"{overall_stats['success_rate']:.1f}%",
                ],
            }
            pd.DataFrame(overall_summary).to_excel(writer, sheet_name="Overall_Summary", index=False)

            # 4. NEW Pivot Routes SUM sheet
            pivot_routes.to_excel(writer, sheet_name="Pivot_Routes_SUM", index=True)

            # 5. NEW Pivot Schedules SUM sheet
            pivot_schedules.to_excel(writer, sheet_name="Pivot_Schedules_SUM", index=True)

        clear_checkpoint()

        print("\n" + "=" * 80)
        print("7-DAY FETCH FINISHED!")
        print("=" * 80)
        print(f"Total Operators: {len(operator_map):,}")
        print(f"Operators With Data (Any Day): {overall_stats['with_data']:,} ({overall_stats['success_rate']:.1f}%)")
        print(f"Operators Without Data (All Days): {overall_stats['without_data']:,}")
        print(f"Total Schedules (7 Days): {overall_stats['total_schedules']:,}")
        print(f"Total Routes (7 Days): {overall_stats['total_routes']:,}")
        print(f"\nComprehensive Excel: {xlsx_file}")
        print("=" * 80 + "\n")

        html_body = build_html_summary(operator_map, daily_stats, overall_stats)

        first_display = datetime.strptime(fetch_dates[0], "%Y-%m-%d").strftime("%d-%m-%Y")
        last_display = datetime.strptime(fetch_dates[-1], "%Y-%m-%d").strftime("%d-%m-%Y")
        subject = f"Bitla Inventory report from {first_display} to {last_display}"

        cc_list = []
        send_report_email(subject, html_body, xlsx_file, cc_emails=cc_list)


# =============================== ENTRY POINT =============================
def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()

