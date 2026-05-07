"""Webxdc app delivery + per-instance state pushing.

Owns:
  - app_msgids.json (atomic load/save):  chat_id -> {class_name -> msgid}
  - /apps onboarding flow                (send fresh xdc, retract withdrawn)
  - per-chat filtered status updates     (broadcast snapshots tailored
                                          to each chat's visible devices)

Not pure: writes files, calls bot.rpc. The engine and bot.py are the
only callers.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from pathlib import Path

log = logging.getLogger("mqtt_bot.webxdc")


class WebxdcIO:
    def __init__(self, state_dir: Path, devices_dir: Path) -> None:
        """state_dir: where app_msgids.json lives (per-bot config dir).
        devices_dir: components root (devices/<class>/<class>.xdc).
        """
        self._state_dir = state_dir
        self._devices_dir = devices_dir
        self._path = state_dir / "app_msgids.json"
        self._map: dict[int, dict[str, int]] = self._load()

    # --- persistence ------------------------------------------------------

    def _load(self) -> dict[int, dict[str, int]]:
        try:
            raw = json.loads(self._path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
        out: dict[int, dict[str, int]] = {}
        for chat, apps in raw.items():
            if not isinstance(apps, dict):
                continue
            out[int(chat)] = {str(c): int(m) for c, m in apps.items()
                              if isinstance(m, int)}
        return out

    def _save(self) -> None:
        self._state_dir.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        serialised = {
            str(chat): {str(c): int(m) for c, m in apps.items()}
            for chat, apps in self._map.items()
        }
        tmp.write_text(json.dumps(serialised))
        os.replace(tmp, self._path)

    # --- discovery --------------------------------------------------------

    def discover_xdcs(self) -> list[tuple[str, str]]:
        """Return [(class_name, full_path)] for every devices/<class>/<class>.xdc.

        The file's stem is taken as the class name; this matches the
        component layout where the build script writes
        devices/<class>/<class>.xdc next to the source `app/` dir.
        """
        out: list[tuple[str, str]] = []
        for class_dir in sorted(self._devices_dir.glob("*/")):
            cls = class_dir.name
            xdc = class_dir / f"{cls}.xdc"
            if xdc.is_file():
                out.append((cls, str(xdc)))
        return out

    def known_classes_for(self, chat_id: int) -> list[str]:
        return list(self._map.get(chat_id, {}).keys())

    def msgid_belongs_to_chat(self, chat_id: int, msgid: int) -> bool:
        return msgid in self._map.get(chat_id, {}).values()

    def class_for_msgid(self, chat_id: int, msgid: int) -> str | None:
        for cls, m in self._map.get(chat_id, {}).items():
            if m == msgid:
                return cls
        return None

    # --- send / retract ---------------------------------------------------

    def send_apps(
        self,
        bot,
        accid: int,
        chat_id: int,
        classes_visible: set[str],
    ) -> tuple[list[str], list[str]]:
        """Deliver xdcs for every class visible to this chat.

        For each apps/<class>.xdc whose class is in classes_visible, send
        the file fresh and delete the chat's prior msgid (always-resend so
        late joiners get a copy). Withdraw apps whose xdc no longer exists
        OR whose class is no longer visible to this chat.

        Returns (sent_classes, retracted_classes).
        """
        from deltachat2 import MsgData  # local import to keep webxdc_io light

        sent: list[str] = []
        retracted: list[str] = []
        chat_apps = self._map.setdefault(chat_id, {})
        available = {cls: path for cls, path in self.discover_xdcs()}

        # Retract anything tracked but no longer servable to this chat.
        for cls in list(chat_apps.keys()):
            if cls in available and cls in classes_visible:
                continue
            old = chat_apps.pop(cls)
            try:
                bot.rpc.delete_messages_for_all(accid, [old])
                retracted.append(cls)
            except Exception as ex:
                log.warning("retract %s msgid=%d chat=%d failed: %s", cls, old, chat_id, ex)

        for cls, path in available.items():
            if cls not in classes_visible:
                continue
            old = chat_apps.get(cls)
            try:
                new = int(bot.rpc.send_msg(accid, chat_id, MsgData(file=path)))
            except Exception as ex:
                log.error("send app %s to chat %d failed: %s", cls, chat_id, ex)
                continue
            chat_apps[cls] = new
            sent.append(cls)
            log.info("sent app %s to chat %d msgid=%d", cls, chat_id, new)
            if old is not None:
                try:
                    bot.rpc.delete_messages_for_all(accid, [old])
                except Exception as ex:
                    log.warning("delete prior %s msgid=%d chat=%d failed: %s",
                                cls, old, chat_id, ex)

        if sent or retracted:
            self._save()
        return sent, retracted

    # --- push -------------------------------------------------------------

    def push_to_msgid(self, bot, accid: int, msgid: int, payload: dict) -> bool:
        body = json.dumps({"payload": payload})
        try:
            bot.rpc.send_webxdc_status_update(accid, msgid, body, "")
            return True
        except Exception as ex:
            log.warning("push to msgid=%d failed: %s", msgid, ex)
            return False

    def push_filtered(
        self,
        bot,
        accid: int,
        snapshot_for: Callable[[int, str], dict | None],
    ) -> int:
        """For every (chat, class), build a snapshot via snapshot_for and
        push as a webxdc status update. Returns count of successful pushes.
        """
        pushed = 0
        for chat_id, apps in list(self._map.items()):
            for cls, msgid in apps.items():
                snap = snapshot_for(chat_id, cls)
                if snap is None:
                    continue
                dev_count = len(snap.get("devices", {})) if isinstance(snap, dict) else 0
                ok = self.push_to_msgid(bot, accid, msgid, snap)
                log.debug("push chat=%d class=%s msgid=%d devices=%d ok=%s",
                          chat_id, cls, msgid, dev_count, ok)
                if ok:
                    pushed += 1
        return pushed
