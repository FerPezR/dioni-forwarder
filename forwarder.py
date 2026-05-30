"""Dioni signals forwarder + alert scheduler.

Monitor @dioniescalante en topic 92 del grupo RM (-1002087337771) y reenvía señales
nuevas al topic 101 del canal forense (-1003894658753). Además programa alertas
Telegram 30min pre-partido y exporta lista de Calendar pending.

Idempotente: `state.json` track fechas + msg_ids forwarded + alerts scheduled.

Reglas:
- Dioni postea 3-6 señales/día, en 1-3 lotes (a veces todas juntas, a veces partidas,
  a veces hasta la mañana siguiente).
- Hora en señales = Venezuela TZ (UTC-4). Para GT: restar 2h.
- Esperar >=30 min después de la 1ra señal del lote vigente antes del primer forward del día.
- Si caption del día YA posteado → solo forward señales nuevas sin caption.

Env vars requeridas:
- TELEGRAM_API_ID
- TELEGRAM_API_HASH
- TELEGRAM_SESSION_STRING
"""
import os
import sys
import io
import asyncio
import json
import random
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.messages import ForwardMessagesRequest

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION = os.environ["TELEGRAM_SESSION_STRING"]

SRC_GRP = -1002087337771       # RM apuetas inversas
SRC_TOPIC = 92                 # # Jugadas personales (Low risk)
DIONI_USERNAME = "dioniescalante"

DST_CHAN = -1003894658753      # canal forense
DST_TOPIC = 101                # 📈 RM Señales (manuales)

HERE = Path(__file__).resolve().parent
STATE_FILE = HERE / "state.json"
CAL_PENDING_FILE = HERE / "calendar-pending.json"

MIN_DELAY_FIRST_FWD = 30 * 60   # 30 min
ALERT_PREMATCH_MIN = 30          # alerta 30 min antes del partido

GT_TZ = timezone(timedelta(hours=-6))
VE_TZ = timezone(timedelta(hours=-4))

MESES_ES = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
            "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]


def parse_signal_time(text):
    m = re.match(r'(\d{1,2}):(\d{2})\s*(am|pm)', (text or "").strip(), re.I)
    if not m:
        return None
    h, mn, ampm = int(m.group(1)), int(m.group(2)), m.group(3).lower()
    if ampm == 'pm' and h != 12:
        h += 12
    if ampm == 'am' and h == 12:
        h = 0
    return h, mn


def signal_match_dt_utc(date_iso, h_ve, m_ve):
    y, mo, d = map(int, date_iso.split('-'))
    return datetime(y, mo, d, h_ve, m_ve, tzinfo=VE_TZ).astimezone(timezone.utc)


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {"forwarded": {}}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def target_date_for_now(now_utc):
    """Heurística: GT >= 18:00 → día siguiente, si no → día actual."""
    gt = now_utc - timedelta(hours=6)
    if gt.hour >= 18:
        return (gt + timedelta(days=1)).date()
    return gt.date()


async def main():
    target_date = None
    if len(sys.argv) > 1:
        target_date = datetime.fromisoformat(sys.argv[1]).date()
    now_utc = datetime.now(timezone.utc)
    if target_date is None:
        target_date = target_date_for_now(now_utc)
    date_key = target_date.isoformat()
    caption_text = f"Señales originales para el día {target_date.day} de {MESES_ES[target_date.month - 1]}"

    state = load_state()
    day_state = state["forwarded"].get(date_key, {"msg_ids": [], "caption_posted": False, "scheduled_alerts": {}})
    already_fwd = set(day_state["msg_ids"])
    scheduled_alerts = day_state.get("scheduled_alerts", {})
    print(f"[INFO] target_date={date_key} | ya fwd: {len(already_fwd)} | caption: {day_state['caption_posted']} | alerts: {len(scheduled_alerts)}")

    c = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
    await c.connect()
    src = await c.get_entity(SRC_GRP)
    dst = await c.get_entity(DST_CHAN)

    # Ventana desde 18:00 GT del día anterior hasta now
    window_start_gt = datetime(target_date.year, target_date.month, target_date.day, tzinfo=GT_TZ) - timedelta(hours=6)
    window_start_utc = window_start_gt.astimezone(timezone.utc)

    candidates = []
    async for m in c.iter_messages(src, limit=500, reply_to=SRC_TOPIC):
        if m.date < window_start_utc:
            break
        sender_username = None
        try:
            u = await c.get_entity(m.sender_id) if m.sender_id else None
            sender_username = getattr(u, "username", None)
        except Exception:
            pass
        if sender_username != DIONI_USERNAME:
            continue
        candidates.append(m)
    candidates.reverse()
    print(f"[INFO] candidatos Dioni: {len(candidates)}")

    nuevos = [m for m in candidates if m.id not in already_fwd]
    if nuevos and not day_state["caption_posted"]:
        delta_first = (now_utc - candidates[0].date).total_seconds()
        if delta_first < MIN_DELAY_FIRST_FWD:
            wait_min = (MIN_DELAY_FIRST_FWD - delta_first) / 60
            print(f"[HOLD] 1ra señal hace {delta_first/60:.1f}min, esperar {wait_min:.1f}min más.")
            await c.disconnect()
            return

    posted_caption_this_run = False
    cal_pending_to_append = []

    for m in candidates:
        # 1. Forward si nuevo
        if m.id not in already_fwd:
            await c(ForwardMessagesRequest(
                from_peer=src, id=[m.id], to_peer=dst,
                random_id=[random.randrange(2**63)], top_msg_id=DST_TOPIC,
            ))
            already_fwd.add(m.id)
            day_state["msg_ids"] = sorted(already_fwd)
            print(f"[FWD] msg {m.id} | {(m.message or '')[:50]}")
            if not day_state["caption_posted"] and not posted_caption_this_run:
                await c.send_message(dst, caption_text, reply_to=DST_TOPIC)
                day_state["caption_posted"] = True
                posted_caption_this_run = True
                print(f"[CAPTION] '{caption_text}'")

        # 2. Schedule alert si falta
        if str(m.id) in scheduled_alerts:
            continue
        parsed = parse_signal_time(m.message or "")
        if not parsed:
            scheduled_alerts[str(m.id)] = "skip"
            continue
        h_ve, mn_ve = parsed
        match_utc = signal_match_dt_utc(date_key, h_ve, mn_ve)
        alert_utc = match_utc - timedelta(minutes=ALERT_PREMATCH_MIN)
        match_gt = match_utc.astimezone(GT_TZ)
        alert_gt = alert_utc.astimezone(GT_TZ)
        if alert_utc <= now_utc + timedelta(seconds=30):
            scheduled_alerts[str(m.id)] = "past"
            continue
        alert_text = (
            f"⏰ **Próximo partido en {ALERT_PREMATCH_MIN} min**\n"
            f"🕐 Señal Dioni: **{m.message.strip()}**\n"
            f"🇻🇪 VE: {h_ve:02d}:{mn_ve:02d}  ·  🇬🇹 GT: {match_gt.strftime('%H:%M')}"
        )
        try:
            await c.send_message(
                dst, alert_text, reply_to=DST_TOPIC,
                parse_mode='markdown', schedule=alert_utc,
            )
            scheduled_alerts[str(m.id)] = alert_utc.isoformat()
            print(f"[ALERT] {alert_gt.strftime('%Y-%m-%d %H:%M GT')} · {m.message[:25]}")
        except Exception as e:
            print(f"[ALERT FAIL] {m.message[:30]}: {e}")
            continue
        cal_pending_to_append.append({
            "date": date_key,
            "summary": f"⚽ RM señal Dioni {m.message.strip()[:20]}",
            "match_iso_gt": match_gt.strftime('%Y-%m-%dT%H:%M:%S'),
            "alert_iso_gt": alert_gt.strftime('%Y-%m-%dT%H:%M:%S'),
            "signal_raw": m.message.strip(),
            "src_msg_id": m.id,
        })

    day_state["scheduled_alerts"] = scheduled_alerts
    state["forwarded"][date_key] = day_state
    save_state(state)

    if cal_pending_to_append:
        prev = []
        if CAL_PENDING_FILE.exists():
            try:
                prev = json.loads(CAL_PENDING_FILE.read_text(encoding="utf-8"))
                if not isinstance(prev, list):
                    prev = []
            except Exception:
                prev = []
        prev.extend(cal_pending_to_append)
        CAL_PENDING_FILE.write_text(json.dumps(prev, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[CAL] +{len(cal_pending_to_append)} pending events")

    print("[DONE]")
    await c.disconnect()


asyncio.run(main())
