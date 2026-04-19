import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import httpx

if TYPE_CHECKING:
    from safeclaw.core.engine import SafeClaw

logger = logging.getLogger(__name__)

ASANA_BASE = "https://app.asana.com/api/1.0"
TELEGRAM_SEND = "https://api.telegram.org/bot{token}/sendMessage"
MAX_SEEN_IDS = 8000
REQUEST_PAUSE_SEC = 0.12


def _state_path(engine: "SafeClaw") -> Path:
    return engine.data_dir / "asana_feed_state.json"


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "version": 1,
            "event_sync_by_resource": {},
            "seen_story_gids": [],
            "seen_new_task_gids": [],
        }
    return json.loads(path.read_text(encoding="utf-8"))


def _save_state(path: Path, state: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _trim_seen(lst: list[str]) -> list[str]:
    if len(lst) > MAX_SEEN_IDS:
        return lst[-MAX_SEEN_IDS:]
    return lst


async def _asana_get(
    client: httpx.AsyncClient, token: str, path: str, params: dict[str, Any] | None = None
) -> tuple[int, dict[str, Any]]:
    url = f"{ASANA_BASE}{path}"
    headers = {"Authorization": f"Bearer {token}"}
    r = await client.get(url, headers=headers, params=params or {}, timeout=60.0)
    try:
        body = r.json()
    except json.JSONDecodeError:
        body = {}
    return r.status_code, body if isinstance(body, dict) else {}


async def _tg_send(
    client: httpx.AsyncClient, bot_token: str, chat_id: str, text: str
) -> None:
    url = TELEGRAM_SEND.format(token=bot_token)
    r = await client.post(
        url,
        json={"chat_id": chat_id, "text": text[:4090]},
        timeout=30.0,
    )
    if r.status_code != 200:
        logger.warning("Telegram send failed: %s %s", r.status_code, r.text[:300])


async def _list_projects(
    client: httpx.AsyncClient, token: str, workspace_gid: str
) -> list[str]:
    out: list[str] = []
    offset: str | None = None
    while True:
        params: dict[str, Any] = {
            "workspace": workspace_gid,
            "archived": "false",
            "limit": 100,
            "opt_fields": "gid,name",
        }
        if offset:
            params["offset"] = offset
        status, body = await _asana_get(client, token, "/projects", params)
        if status != 200:
            logger.warning("Asana projects list failed: %s", status)
            break
        for row in body.get("data") or []:
            gid = row.get("gid")
            if gid:
                out.append(str(gid))
        nxt = (body.get("next_page") or {}).get("offset")
        offset = str(nxt) if nxt else None
        if not offset:
            break
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    return out


async def _utl_gid(
    client: httpx.AsyncClient, token: str, user_gid: str, workspace_gid: str
) -> str | None:
    path = f"/users/{quote(user_gid, safe='')}/user_task_list"
    status, body = await _asana_get(
        client, token, path, {"workspace": workspace_gid, "opt_fields": "gid"}
    )
    if status != 200:
        return None
    data = body.get("data") or {}
    gid = data.get("gid")
    return str(gid) if gid else None


async def _drain_events(
    client: httpx.AsyncClient,
    token: str,
    resource_kind: str,
    resource_gid: str,
    sync_token: str | None,
) -> tuple[list[dict[str, Any]], str | None]:
    all_events: list[dict[str, Any]] = []
    sync: str | None = sync_token
    opt = (
        "action,type,created_at,user,user.gid,user.name,"
        "resource,resource.gid,resource.resource_type,resource.name,"
        "parent,parent.gid,parent.resource_type,parent.name,"
        "change,change.field,change.action,change.new_value,change.added_value"
    )
    path = f"/{resource_kind}/{resource_gid}/events"
    for _ in range(120):
        params: dict[str, Any] = {"opt_fields": opt}
        if sync:
            params["sync"] = sync
        status, body = await _asana_get(client, token, path, params)
        raw_sync = body.get("sync")
        if raw_sync is None:
            new_sync = None
        elif isinstance(raw_sync, str):
            new_sync = raw_sync
        else:
            new_sync = str(raw_sync)
        if status == 412:
            if new_sync:
                sync = new_sync
                continue
            return all_events, sync_token
        if status == 403:
            return all_events, sync_token
        if status != 200:
            if sync_token is None and sync is None:
                return all_events, "__skip_events__"
            return all_events, sync_token
        chunk = body.get("data") or []
        if isinstance(chunk, list):
            all_events.extend(chunk)
        if new_sync:
            sync = new_sync
        if body.get("has_more"):
            await asyncio.sleep(REQUEST_PAUSE_SEC)
            continue
        break
    return all_events, sync


async def _get_story(
    client: httpx.AsyncClient, token: str, story_gid: str
) -> dict[str, Any] | None:
    fields = (
        "gid,text,html_text,created_at,created_by,created_by.gid,created_by.name,"
        "type,resource_subtype,target,target.gid,target.name,permalink_url"
    )
    status, body = await _asana_get(
        client,
        token,
        f"/stories/{quote(story_gid, safe='')}",
        {"opt_fields": fields},
    )
    if status != 200:
        return None
    data = body.get("data")
    return data if isinstance(data, dict) else None


def _story_mentions_user(story: dict[str, Any], user_gid: str) -> bool:
    ug = str(user_gid)
    html = story.get("html_text") or ""
    text = story.get("text") or ""
    if ug in html or ug in text:
        return True
    if f'data-asana-gid="{ug}"' in html or f"data-asana-gid='{ug}'" in html:
        return True
    if re.search(rf"data-asana-gid\s*=\s*[\"']?{re.escape(ug)}[\"']?", html):
        return True
    if f"/0/{ug}/" in html or f"/0/{ug}/" in text:
        return True
    mentions = story.get("mentions")
    if isinstance(mentions, list):
        for m in mentions:
            if isinstance(m, dict) and str(m.get("gid") or "") == ug:
                return True
            if isinstance(m, str) and m == ug:
                return True
    return False


def _fmt_mention(story: dict[str, Any]) -> str:
    who = (story.get("created_by") or {}).get("name") or "Unknown"
    when = story.get("created_at") or ""
    body = (story.get("text") or "").strip() or "(empty)"
    task = (story.get("target") or {}).get("name") or "Task"
    link = story.get("permalink_url") or ""
    lines = ["Asana @mention", f"From: {who}", f"When: {when}", f"Task: {task}", "", body[:3500]]
    if link:
        lines.extend(["", link])
    return "\n".join(lines)


def _fmt_new_task(name: str, gid: str) -> str:
    return f"Asana: new task on you\n{name}\n(gid {gid})"


async def _poll_assignee_tasks(
    client: httpx.AsyncClient,
    token: str,
    workspace_gid: str,
    seen: list[str],
) -> tuple[list[str], list[str]]:
    seen_set = set(seen)
    new_ids: list[str] = []
    offset = None
    while True:
        params: dict[str, Any] = {
            "assignee": "me",
            "workspace": workspace_gid,
            "completed": "false",
            "limit": 100,
            "opt_fields": "gid,name,modified_at",
        }
        if offset:
            params["offset"] = offset
        status, body = await _asana_get(client, token, "/tasks", params)
        if status != 200:
            logger.warning("Asana tasks poll failed: %s", status)
            break
        for row in body.get("data") or []:
            gid = str(row.get("gid") or "")
            if gid and gid not in seen_set:
                new_ids.append(gid)
            if gid:
                seen_set.add(gid)
        nxt = (body.get("next_page") or {}).get("offset")
        offset = str(nxt) if nxt else None
        if not offset:
            break
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    return _trim_seen(list(seen_set)), new_ids


def _resource_key(kind: str, gid: str) -> str:
    return f"{kind}:{gid}"


async def run_asana_cycle(engine: "SafeClaw") -> None:
    cfg = engine.config.get("asana") or {}
    if not cfg.get("enabled"):
        return
    pat = (cfg.get("pat") or os.environ.get("ASANA_PAT", "")).strip()
    workspace_gid = str(cfg.get("workspace_gid") or "").strip()
    if not pat or not workspace_gid:
        logger.warning("Asana enabled but pat or workspace_gid missing")
        return

    tg_token, chat_id = _resolve_telegram_target(engine, cfg)
    if not tg_token or not chat_id:
        logger.warning("Asana enabled but Telegram token/chat_id unavailable")
        return

    state_path = _state_path(engine)
    state = _load_state(state_path)
    sync_map: dict[str, str | None] = {}
    raw = state.get("event_sync_by_resource") or {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            sync_map[str(k)] = v if v else None

    async with httpx.AsyncClient() as client:
        status_me, me_body = await _asana_get(
            client, pat, "/users/me", {"opt_fields": "gid,name,email"}
        )
        if status_me != 200:
            logger.warning("Asana /users/me failed: %s", status_me)
            return
        me = me_body.get("data") or {}
        user_gid = str(cfg.get("user_gid") or me.get("gid") or "").strip()
        if not user_gid:
            return

        projects = await _list_projects(client, pat, workspace_gid)
        utl = await _utl_gid(client, pat, user_gid, workspace_gid)
        resources: list[tuple[str, str]] = [("projects", p) for p in projects]
        if utl:
            resources.append(("user_task_lists", utl))

        seen_stories = list(state.get("seen_story_gids") or [])
        seen_story_set = set(seen_stories)
        seen_tasks = list(state.get("seen_new_task_gids") or [])
        bootstrap = not seen_tasks

        new_task_seen, new_task_ids = await _poll_assignee_tasks(
            client, pat, workspace_gid, seen_tasks
        )
        if bootstrap:
            new_task_ids = []

        for gid in new_task_ids:
            name = "?"
            st, tb = await _asana_get(
                client, pat, f"/tasks/{quote(gid, safe='')}", {"opt_fields": "name"}
            )
            if st == 200:
                row = tb.get("data") or {}
                name = row.get("name") or name
            await _tg_send(client, tg_token, chat_id, _fmt_new_task(name, gid))

        for kind, gid in resources:
            key = _resource_key(kind, gid)
            prev = sync_map.get(key)
            if prev == "__skip_events__":
                await asyncio.sleep(REQUEST_PAUSE_SEC)
                continue
            events, new_sync = await _drain_events(client, pat, kind, gid, prev)
            if new_sync is not None:
                sync_map[key] = new_sync
            for ev in events:
                res = ev.get("resource") or {}
                rtype = (res.get("resource_type") or "").lower()
                rgid = str(res.get("gid") or "")
                if rtype != "story" or not rgid:
                    continue
                if rgid in seen_story_set:
                    continue
                story = await _get_story(client, pat, rgid)
                if not story:
                    seen_story_set.add(rgid)
                    continue
                stype = (story.get("type") or "").lower()
                subtype = (story.get("resource_subtype") or "").lower()
                if stype != "comment" and "comment" not in subtype:
                    seen_story_set.add(rgid)
                    continue
                if not _story_mentions_user(story, user_gid):
                    seen_story_set.add(rgid)
                    continue
                seen_story_set.add(rgid)
                await _tg_send(client, tg_token, chat_id, _fmt_mention(story))
            await asyncio.sleep(REQUEST_PAUSE_SEC)

        state["event_sync_by_resource"] = sync_map
        state["seen_story_gids"] = _trim_seen(list(seen_story_set))
        state["seen_new_task_gids"] = new_task_seen
        _save_state(state_path, state)


def _resolve_telegram_target(engine: "SafeClaw", asana_cfg: dict[str, Any]) -> tuple[str, str]:
    chat = str(asana_cfg.get("notify_telegram_chat_id") or "").strip()
    ch_cfg = engine.config.get("channels", {}).get("telegram", {})
    root = engine.config.get("telegram", {})
    token = str(ch_cfg.get("token") or root.get("token") or "").strip()
    if not chat:
        au = ch_cfg.get("allowed_users") or root.get("allowed_users")
        if isinstance(au, list) and len(au) == 1:
            chat = str(au[0])
    return token, chat
