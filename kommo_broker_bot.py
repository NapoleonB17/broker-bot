#!/usr/bin/env python3
"""
Бот-помощник брокера для Kommo CRM. Версия для GitHub Actions.

Что делает:
  1. Раз в запуск (workflow крутится каждые 5 минут) проверяет сделки,
     где ответственный — заданный пользователь (MY_USER_ID).
  2. Как только видит сделку, которую ещё не видел с тех пор, как она
     стала моей — СРАЗУ шлёт карточку в Телеграм (независимо от того,
     на каком этапе она в этот момент находится: «Новые заявки»,
     «Установление контакта» или любом другом — заявки у нас часто
     проскакивают первый этап раньше, чем успевает сработать бот).
  3. Через ~5 минут ПОСЛЕ ТОГО, КАК БОТ ЕЁ ВПЕРВЫЕ УВИДЕЛ (а не от даты
     создания сделки) — переводит её на этап «Установление контакта»,
     если она ещё туда не попала сама.
  4. Чужие сделки не трогает вообще: фильтр на стороне API + повторная
     проверка ответственного прямо перед изменением.

Режимы:
  python kommo_broker_bot.py --ids     → показать ID воронок и этапов
  python kommo_broker_bot.py --once    → один проход (для cron / GitHub Actions)
  python kommo_broker_bot.py           → бесконечный цикл (для VPS/локального запуска)

Зависимости: pip install requests

Состояние (какие сделки уже уведомлены/перемещены, и когда бот их впервые
увидел) хранится в JSON-файле STATE_FILE. В GitHub Actions это файл
коммитится обратно в репозиторий после каждого запуска — см.
.github/workflows/broker-bot.yml.
"""
import os
import sys
import json
import time
import requests

# ---------------------- НАСТРОЙКИ ----------------------
# Все реквизиты задаются снаружи (GitHub Secrets / Variables), в коде их нет —
# репозиторий публичный, поэтому ни поддомена CRM, ни ID пользователя тут не хранится.
SUBDOMAIN   = os.getenv("KOMMO_SUBDOMAIN", "")
KOMMO_TOKEN = os.getenv("KOMMO_TOKEN", "")
MY_USER_ID  = int(os.getenv("MY_USER_ID", "0") or "0")

TG_TOKEN   = os.getenv("TG_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

# Целевой этап. Ищется по названию; если задан TARGET_STATUS_ID — берётся он.
TARGET_STATUS_NAME = os.getenv("TARGET_STATUS_NAME", "установление контакта")
TARGET_STATUS_ID   = os.getenv("TARGET_STATUS_ID", "")

PIPELINE_ID = os.getenv("PIPELINE_ID", "")

DELAY_SECONDS = int(os.getenv("DELAY_SECONDS", "270"))   # 4:30 + шаг опроса = не больше 5 мин
POLL_SECONDS  = int(os.getenv("POLL_SECONDS", "30"))      # используется только в режиме бесконечного цикла
DRY_RUN       = os.getenv("DRY_RUN", "1") == "1"

STATE_FILE = os.getenv("STATE_FILE", "broker_bot_state.json")
BASE = f"https://{SUBDOMAIN}.kommo.com/api/v4"
KH = {"Authorization": f"Bearer {KOMMO_TOKEN}", "Content-Type": "application/json"}
WON_LOST = {142, 143}
RATE_SLEEP = 0.2

_pipe = {"chains": None, "names": None, "ts": 0}

# ---------------------- БАЗА ----------------------
def tg_send(text):
    if not (TG_TOKEN and TG_CHAT_ID):
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT_ID, "text": text,
                            "parse_mode": "HTML", "disable_web_page_preview": True},
                      timeout=10)
    except Exception as e:
        print("tg error:", e)

def log(msg, to_tg=False):
    print(time.strftime("%H:%M:%S"), msg, flush=True)
    if to_tg:
        tg_send(msg)

def k_get(path, params=None):
    time.sleep(RATE_SLEEP)
    r = requests.get(f"{BASE}{path}", headers=KH, params=params, timeout=30)
    if r.status_code == 204:
        return None
    if r.status_code == 401:
        raise SystemExit("401: токен Kommo невалиден. Перевыпусти долгосрочный токен.")
    r.raise_for_status()
    return r.json()

def k_patch(path, payload):
    time.sleep(RATE_SLEEP)
    r = requests.patch(f"{BASE}{path}", headers=KH, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

# ---------------------- СОСТОЯНИЕ ----------------------
def load_state():
    """Возвращает dict: initialized(bool), notified(set), moved(set), pending(dict id->ts)."""
    try:
        with open(STATE_FILE) as f:
            d = json.load(f)
    except Exception:
        d = {}
    return {
        "initialized": bool(d.get("initialized", False)),
        "notified": set(d.get("notified", [])),
        "moved": set(d.get("moved", [])),
        "pending": {str(k): int(v) for k, v in d.get("pending", {}).items()},
    }

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump({
            "initialized": True,
            "notified": sorted(state["notified"])[-5000:],
            "moved": sorted(state["moved"])[-5000:],
            "pending": state["pending"],
        }, f)

# ---------------------- ВОРОНКИ ----------------------
def get_pipelines():
    if _pipe["chains"] and time.time() - _pipe["ts"] < 600:
        return _pipe["chains"], _pipe["names"]
    d = k_get("/leads/pipelines")
    chains, names = {}, {}
    for p in d["_embedded"]["pipelines"]:
        ordered = sorted(p["_embedded"]["statuses"], key=lambda s: s["sort"])
        chains[p["id"]] = [s["id"] for s in ordered if s["id"] not in WON_LOST]
        for s in ordered:
            names[s["id"]] = s["name"]
    _pipe.update(chains=chains, names=names, ts=time.time())
    return chains, names

def print_ids():
    d = k_get("/leads/pipelines")
    print(f"\nАккаунт: {SUBDOMAIN}.kommo.com\n")
    for p in d["_embedded"]["pipelines"]:
        print(f"ВОРОНКА  {p['id']}  «{p['name']}»")
        for s in sorted(p["_embedded"]["statuses"], key=lambda x: x["sort"]):
            mark = "  <-- закрытый" if s["id"] in WON_LOST else ""
            print(f"   этап {s['id']:>10}  {s['name']}{mark}")
        print()

def resolve_target(pipeline_id):
    """ID этапа «Установление контакта» в нужной воронке."""
    if TARGET_STATUS_ID:
        return int(TARGET_STATUS_ID)
    chains, names = get_pipelines()
    for sid in chains.get(pipeline_id, []):
        if names.get(sid, "").strip().lower() == TARGET_STATUS_NAME.strip().lower():
            return sid
    return None

# ---------------------- СДЕЛКИ ----------------------
def fetch_my_leads():
    leads, page = [], 1
    while True:
        params = {"filter[responsible_user_id]": MY_USER_ID, "limit": 250, "page": page}
        if PIPELINE_ID:
            params["filter[pipeline_id]"] = PIPELINE_ID
        d = k_get("/leads", params)
        if not d:
            break
        leads.extend(d["_embedded"]["leads"])
        if not d.get("_links", {}).get("next"):
            break
        page += 1
    return leads

def check_unsorted():
    """Если заявки падают в «Неразобранное» — предупредим один раз."""
    try:
        d = k_get("/leads/unsorted", {"limit": 1})
        if d and d.get("_embedded", {}).get("unsorted"):
            return True
    except Exception:
        pass
    return False

def card(lead, names):
    price = lead.get("price") or 0
    return (f"🔔 <b>Новая заявка на мне</b>\n"
            f"<b>{lead.get('name') or 'Без названия'}</b>\n"
            f"Этап: {names.get(lead['status_id'], '?')}\n"
            f"Бюджет: {price}\n"
            f"https://{SUBDOMAIN}.kommo.com/leads/detail/{lead['id']}\n"
            f"<i>Перевод в «Установление контакта» через 5 мин (если ещё не там)</i>")

# ---------------------- ЦИКЛ ----------------------
def tick(state):
    """
    Логика больше не привязана к тому, на каком этапе воронки находится
    сделка в момент проверки (заявки у нас часто проскакивают «Новые
    заявки» раньше, чем успевает сработать бот). Вместо этого бот сам
    ведёт учёт: "видел ли я эту сделку с тех пор, как она стала моей".
    """
    _, names = get_pipelines()
    now = int(time.time())
    changed = False
    notified, moved, pending = state["notified"], state["moved"], state["pending"]

    for lead in fetch_my_leads():
        lid = lead["id"]
        key = str(lid)

        if lead["status_id"] in WON_LOST:
            if lid not in moved:
                moved.add(lid); changed = True
            pending.pop(key, None)
            continue

        if lid in moved:
            continue

        # 1) мгновенное уведомление о новой (ещё не виденной) сделке
        if lid not in notified:
            tg_send(card(lead, names))
            notified.add(lid)
            pending[key] = now
            changed = True
            log(f"Уведомил о #{lid}")
            continue  # таймер на перевод начнёт отсчёт со следующего прохода

        # 2) перевод по таймеру — считаем от момента, когда бот впервые её увидел
        first_seen = pending.get(key, now)
        age = now - first_seen
        if age < DELAY_SECONDS:
            continue

        target = resolve_target(lead["pipeline_id"])
        if not target:
            log(f"⚠️ Не нашёл этап «{TARGET_STATUS_NAME}» в воронке {lead['pipeline_id']}", True)
            continue

        # === защита: перечитываем сделку перед изменением ===
        fresh = k_get(f"/leads/{lid}")
        if not fresh:
            continue
        if fresh.get("responsible_user_id") != MY_USER_ID:
            log(f"Пропуск #{lid}: сделка ушла другому брокеру")
            moved.add(lid); pending.pop(key, None); changed = True
            continue
        if fresh["status_id"] == target:
            # уже на целевом этапе (сама доехала, или её передвинули руками) — просто закрываем трекинг
            moved.add(lid); pending.pop(key, None); changed = True
            continue

        # ВАЖНО: в лог НЕ пишем название сделки (там имя клиента) — логи GitHub
        # Actions в публичном репозитории видны всем. Имя уходит только в личный
        # Telegram. В логе остаётся лишь ID сделки.
        line = (f"#{lid} "
                f"{names.get(fresh['status_id'])} → {names.get(target)}")
        if DRY_RUN:
            log("[ТЕСТ] " + line)
        else:
            k_patch(f"/leads/{lid}", {"status_id": target, "pipeline_id": fresh["pipeline_id"]})
            log("✅ " + line)
            tg_send(f"✅ Переведено: <b>{fresh.get('name','')}</b> → Установление контакта")
        moved.add(lid)
        pending.pop(key, None)
        changed = True

    return changed

def run_once():
    check_config()
    state = load_state()
    if not state["initialized"]:
        # Разовая инициализация: сделки, которые УЖЕ висят на мне на момент
        # первого запуска бота, не уведомляем и не трогаем — работаем только
        # с тем, что появится (или что бот увидит впервые) после этого.
        ids = [l["id"] for l in fetch_my_leads()]
        state["notified"] = set(ids)
        state["moved"] = set(ids)
        state["pending"] = {}
        save_state(state)
        log(f"Первый запуск: пометил {len(ids)} текущих сделок, работаю только с новыми.", True)
        return
    if check_unsorted():
        log("⚠️ В аккаунте есть «Неразобранное» — проверь, не падают ли заявки туда.", True)
    if tick(state):
        save_state(state)
    log(f"Проход завершён. DRY_RUN={DRY_RUN}")

def run_forever():
    state = load_state()
    if not state["initialized"]:
        ids = [l["id"] for l in fetch_my_leads()]
        state["notified"] = set(ids)
        state["moved"] = set(ids)
        state["pending"] = {}
        save_state(state)
        log(f"Первый запуск: пометил {len(ids)} текущих сделок, работаю только с новыми.")
    if check_unsorted():
        log("⚠️ В аккаунте есть «Неразобранное» — проверь, не падают ли заявки туда.", True)
    log(f"Старт. user_id={MY_USER_ID}, задержка={DELAY_SECONDS}с, DRY_RUN={DRY_RUN}", True)
    while True:
        try:
            if tick(state):
                save_state(state)
        except Exception as e:
            log(f"Ошибка: {e}")
        time.sleep(POLL_SECONDS)

def check_config():
    missing = []
    if not SUBDOMAIN:
        missing.append("KOMMO_SUBDOMAIN")
    if not KOMMO_TOKEN:
        missing.append("KOMMO_TOKEN")
    if not MY_USER_ID:
        missing.append("MY_USER_ID")
    if missing:
        raise SystemExit("Не заданы настройки: " + ", ".join(missing))

def main():
    if "--ids" in sys.argv:
        check_config()
        print_ids()
        return
    check_config()
    if "--once" in sys.argv:
        run_once()
    else:
        run_forever()

if __name__ == "__main__":
    main()
