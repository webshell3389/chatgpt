import os
import sys
import json
import csv
import re
import time
import random
import threading
import asyncio
from typing import Dict, Optional, List, Tuple

from PyQt5 import QtWidgets, QtCore
from PyQt5.QtCore import pyqtSignal, QObject

from telethon import TelegramClient, events
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError, PeerFloodError,
    UserDeactivatedBanError, ChatWriteForbiddenError, ChatAdminRequiredError
)
from telethon.tl.types import InputPhoneContact
from telethon.tl.functions.contacts import ImportContactsRequest
from telethon.network.connection import ConnectionTcpMTProxyRandomizedIntermediate

# ========== 基本配置（请改成你的 API_ID / API_HASH） ==========
API_ID = 29256620
API_HASH = "bbbbe78c8a7306b17d0e7fed950ad393"

SESS_DIR = "sessions"
BLOCKED_FILE = "blocked.json"
PROGRESS_FILE = "broadcast_progress.json"

# 消息池文件（手动点击加载）
MSG_ADD_FILE = "messages_add.json"      # 添加后消息池（首条）
MSG_REPLY_FILE = "messages_reply.json"  # 回复/已读后消息池（二次）

# 代理池（手动点击加载）：每行一个代理
# 支持：
#   socks5://host:port 或 socks5://user:pass@host:port
#   http://host:port    或 http://user:pass@host:port
#   mtproxy://host:port:secret
PROXY_POOL_FILE = "proxy_pool.txt"

# 默认策略
DEFAULT_STRATEGY = "回复后再发"   # ["不重发", "已读后再发", "回复后再发", "定时再次发送"]
DEFAULT_SECOND_DELAY = 10        # 第二条延迟（秒）
DEFAULT_WAIT_REPLY_SEC = 600     # 等待回复的最长时间（秒）=10分钟
DEFAULT_SPACING_SEC = 30.0       # 号码间隔（秒）
DEFAULT_DELAY_JITTER = 10.0      # 延时抖动（秒）


# ========== 线程间信号 ==========
class Bridge(QObject):
    log = pyqtSignal(str)      # 日志区域
    chat = pyqtSignal(str)     # 聊天窗口
    status = pyqtSignal(str)   # 左下状态
    stats = pyqtSignal(dict)   # 统计信息
bridge = Bridge()


# ========== 异步事件循环线程 ==========
class AsyncLoopThread(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.loop = asyncio.new_event_loop()
    def run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

_async_thread = AsyncLoopThread()
_async_thread.start()
ALOOP = _async_thread.loop

def run_coro(coro):
    return asyncio.run_coroutine_threadsafe(coro, ALOOP)


# ========== 工具 ==========
def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def read_lines(path) -> List[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return [x.strip() for x in f if x.strip()]

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def now_ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")


# 在基础延时上添加抖动，确保结果不为负
def apply_delay_jitter(base: float, jitter: float) -> float:
    base = max(0.0, float(base))
    jitter = max(0.0, float(jitter))
    if jitter <= 0.0:
        return base
    return max(0.0, base + random.uniform(-jitter, jitter))


# ========== 群发进度与统计持久化 ==========
class BroadcastProgress:
    def __init__(self, path: str):
        self.path = path
        self.lock = threading.Lock()
        raw = load_json(path, {})
        if not isinstance(raw, dict):
            raw = {}
        raw.setdefault("sessions", {})
        raw.setdefault("global", {})
        self.data = raw
        self.session_sent_cache: Dict[str, set] = {}
        for name, info in self.data["sessions"].items():
            sent_numbers = info.get("sent_numbers") or []
            if not isinstance(sent_numbers, list):
                sent_numbers = list(sent_numbers)
                info["sent_numbers"] = sent_numbers
            self.session_sent_cache[name] = set(sent_numbers)
        with self.lock:
            self._ensure_global_daily_locked()

    def _today(self) -> str:
        return time.strftime("%Y-%m-%d")

    def _new_daily_dict(self, date: str) -> dict:
        return {
            "date": date,
            "first_sent": 0,
            "followup_sent": 0,
            "reads": 0,
            "partner_replies": 0,
            "self_replies": 0,
        }

    def _ensure_daily_keys(self, daily: dict, date: Optional[str] = None) -> dict:
        if date and daily.get("date") != date:
            daily.update(self._new_daily_dict(date))
        else:
            daily.setdefault("date", date or self._today())
            daily.setdefault("first_sent", 0)
            daily.setdefault("followup_sent", 0)
            daily.setdefault("reads", 0)
            daily.setdefault("partner_replies", 0)
            daily.setdefault("self_replies", 0)
        return daily

    def _ensure_global_daily_locked(self) -> dict:
        global_section = self.data.setdefault("global", {})
        today = self._today()
        daily = global_section.get("daily")
        if not isinstance(daily, dict) or daily.get("date") != today:
            global_section["daily"] = self._new_daily_dict(today)
        else:
            self._ensure_daily_keys(daily, today)
        return global_section["daily"]

    def _ensure_global_daily(self) -> dict:
        with self.lock:
            return dict(self._ensure_global_daily_locked())

    def _ensure_session(self, session: str) -> dict:
        sessions = self.data.setdefault("sessions", {})
        rec = sessions.get(session)
        if not isinstance(rec, dict):
            rec = {}
            sessions[session] = rec
        sent_numbers = rec.get("sent_numbers")
        if not isinstance(sent_numbers, list):
            sent_numbers = list(sent_numbers or [])
            rec["sent_numbers"] = sent_numbers
        cache = self.session_sent_cache.setdefault(session, set(sent_numbers))
        today = self._today()
        daily = rec.get("daily")
        if not isinstance(daily, dict) or daily.get("date") != today:
            rec["daily"] = self._new_daily_dict(today)
        else:
            self._ensure_daily_keys(daily, today)
        return rec

    def _save_locked(self):
        save_json(self.path, self.data)

    def touch_session(self, session: str):
        with self.lock:
            self._ensure_session(session)
            self._save_locked()

    def get_sent_numbers(self, session: str) -> set:
        with self.lock:
            self._ensure_session(session)
            return set(self.session_sent_cache.get(session, set()))

    def get_session_daily_counts(self, session: str) -> dict:
        with self.lock:
            rec = self._ensure_session(session)
            daily = rec.get("daily", {})
            return dict(daily)

    def get_global_daily_counts(self) -> dict:
        with self.lock:
            daily = self._ensure_global_daily_locked()
            return dict(daily)

    def register_first_send(self, session: str, phone: str) -> bool:
        with self.lock:
            rec = self._ensure_session(session)
            cache = self.session_sent_cache.setdefault(session, set(rec.get("sent_numbers", [])))
            is_new = phone not in cache
            if is_new:
                cache.add(phone)
                rec.setdefault("sent_numbers", []).append(phone)
                daily = rec["daily"]
                daily["first_sent"] += 1
                global_daily = self._ensure_global_daily_locked()
                global_daily["first_sent"] += 1
                self._save_locked()
                return True
            # 仍需保证全局日期已刷新
            self._ensure_global_daily_locked()
            return False

    def record_followup_send(self, session: str):
        with self.lock:
            rec = self._ensure_session(session)
            daily = rec["daily"]
            daily["followup_sent"] += 1
            daily["self_replies"] += 1
            global_daily = self._ensure_global_daily_locked()
            global_daily["followup_sent"] += 1
            global_daily["self_replies"] += 1
            self._save_locked()

    def record_partner_read(self, session: str):
        with self.lock:
            rec = self._ensure_session(session)
            daily = rec["daily"]
            daily["reads"] += 1
            global_daily = self._ensure_global_daily_locked()
            global_daily["reads"] += 1
            self._save_locked()

    def record_partner_reply(self, session: str):
        with self.lock:
            rec = self._ensure_session(session)
            daily = rec["daily"]
            daily["partner_replies"] += 1
            global_daily = self._ensure_global_daily_locked()
            global_daily["partner_replies"] += 1
            self._save_locked()

    def get_snapshot(self, logged_in: int, blocked: int) -> dict:
        with self.lock:
            daily = self._ensure_global_daily_locked()
            return {
                "logged_in": logged_in,
                "blocked": blocked,
                "today_sent": daily.get("first_sent", 0),
                "partner_reads": daily.get("reads", 0),
                "partner_replies": daily.get("partner_replies", 0),
                "self_replies": daily.get("self_replies", 0),
            }
# ========== 号码读取 ==========
PHONE_RE = re.compile(r"^\+?\d{6,}$")
def read_phone_file(path: str) -> List[str]:
    out = []
    if path.lower().endswith(".csv"):
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            for row in reader:
                for cell in row:
                    v = cell.strip()
                    if PHONE_RE.fullmatch(v):
                        out.append(v)
    else:
        for line in read_lines(path):
            if PHONE_RE.fullmatch(line):
                out.append(line)
    # 去重保持顺序
    seen=set(); ret=[]
    for p in out:
        if p not in seen:
            seen.add(p); ret.append(p)
    return ret


# ========== 代理解析 ==========
def parse_proxy_line(line: str) -> Optional[dict]:
    # socks5://user:pass@host:port 或 socks5://host:port
    # http://user:pass@host:port 或 http://host:port
    # mtproxy://host:port:secret
    try:
        if line.startswith("socks5://"):
            body = line[len("socks5://"):]
            if "@" in body:
                cred, hp = body.split("@", 1)
                user, pwd = cred.split(":", 1)
                host, port = hp.split(":", 1)
                return {"type":"socks5","host":host,"port":int(port),"user":user,"password":pwd}
            else:
                host, port = body.split(":", 1)
                return {"type":"socks5","host":host,"port":int(port)}
        if line.startswith("http://"):
            body = line[len("http://"):]
            if "@" in body:
                cred, hp = body.split("@", 1)
                user, pwd = cred.split(":", 1)
                host, port = hp.split(":", 1)
                return {"type":"http","host":host,"port":int(port),"user":user,"password":pwd}
            else:
                host, port = body.split(":", 1)
                return {"type":"http","host":host,"port":int(port)}
        if line.startswith("mtproxy://"):
            body = line[len("mtproxy://"):]
            host, port, secret = body.split(":")
            return {"type":"mtproxy","host":host,"port":int(port),"secret":secret}
    except Exception:
        return None
    return None

def make_telethon_proxy(cfg: Optional[dict]) -> Tuple[Optional[type], Optional[tuple]]:
    # 返回 (connection, proxy_tuple) 供 TelegramClient 使用
    # socks/http 需要 pysocks；mtproxy 使用 ConnectionTcpMTProxyRandomizedIntermediate
    if not cfg or (cfg.get("type") == "none"):
        return (None, None)
    t = cfg.get("type")
    if t in ("socks5","http"):
        try:
            import socks
            host = cfg.get("host","")
            port = int(cfg.get("port",0) or 0)
            user = cfg.get("user") or None
            pwd  = cfg.get("password") or None
            typ  = socks.SOCKS5 if t=="socks5" else socks.HTTP
            return (None, (typ, host, port, user, pwd))
        except Exception as e:
            bridge.log.emit(f"❌ 代理解析失败：{e}")
            return (None, None)
    if t == "mtproxy":
        return (ConnectionTcpMTProxyRandomizedIntermediate,
                (cfg.get("host",""), int(cfg.get("port",0) or 0), cfg.get("secret","")))
    return (None, None)

def assign_proxies_to_sessions(sessions: List[str], proxies: List[dict]) -> Dict[str, Optional[dict]]:
    if not proxies:
        return {s: None for s in sessions}
    mapping = {}
    for i, s in enumerate(sessions):
        mapping[s] = proxies[i % len(proxies)]
    return mapping


# ========== 客户端管理 ==========
class ClientManager:
    def __init__(self):
        self.clients: Dict[str, TelegramClient] = {}
        self.stop_flags: Dict[str, bool] = {}        # 风控/手动停止标记
        self.session_proxy_map: Dict[str, Optional[dict]] = {}  # 会话->代理分配
        self.blocked = load_json(BLOCKED_FILE, {})   # 被风控账号记录
        self.progress = BroadcastProgress(PROGRESS_FILE)

        # 运行时的消息池（手动加载）
        self.pool_add: List[str] = []
        self.pool_reply: List[str] = []
        self.pools_loaded = False

        # 代理池（手动加载）
        self.proxy_pool: List[dict] = []
        self.proxy_enabled = False

        # 回复监听任务，避免被GC回收
        self.pending_reply_tasks: Dict[str, List[asyncio.Task]] = {}

    def list_sessions(self) -> List[str]:
        ensure_dir(SESS_DIR)
        return [f for f in os.listdir(SESS_DIR) if f.endswith(".session")]

    def notify_stats(self):
        snapshot = self.progress.get_snapshot(len(self.clients), len(self.blocked))
        bridge.stats.emit(snapshot)

    # ------- 消息池 -------
    def load_message_pools(self):
        # 若不存在则创建示例
        if not os.path.exists(MSG_ADD_FILE):
            save_json(MSG_ADD_FILE, ["Hola 😊", "你好，很高兴认识你！", "👋 Hey, 你好吗？"])
        if not os.path.exists(MSG_REPLY_FILE):
            save_json(MSG_REPLY_FILE, ["太好了，我们继续聊聊。", "谢谢回复，我来详细说明一下～", "好的，我把细节发你。"])

        self.pool_add = load_json(MSG_ADD_FILE, [])
        self.pool_reply = load_json(MSG_REPLY_FILE, [])
        self.pools_loaded = True
        bridge.log.emit(f"🗃 消息池已加载：首条 {len(self.pool_add)} 条，二次 {len(self.pool_reply)} 条。")

    def get_random_add_msg(self, fallback: str = "Hola 😊") -> str:
        if self.pool_add:
            return random.choice(self.pool_add)
        return fallback

    def get_random_reply_msg(self, fallback: str = "👌") -> str:
        if self.pool_reply:
            return random.choice(self.pool_reply)
        return fallback

    # ------- 代理池 -------
    def load_proxy_pool(self):
        lines = read_lines(PROXY_POOL_FILE)
        proxies = []
        for ln in lines:
            cfg = parse_proxy_line(ln)
            if cfg: proxies.append(cfg)
        self.proxy_pool = proxies
        self.proxy_enabled = True if proxies else False
        msg = f"🌐 代理池加载完成：{len(proxies)} 个代理。" if proxies else "🌐 未加载到任何代理（将直连）。"
        bridge.log.emit(msg)

        # 分配给当前会话列表（平均分配）
        sessions = self.list_sessions()
        self.session_proxy_map = assign_proxies_to_sessions(sessions, proxies)
        if self.proxy_enabled:
            for s in sessions:
                assigned = self.session_proxy_map.get(s)
                if assigned:
                    bridge.log.emit(f"  • {s} ← {assigned}")
        else:
            bridge.log.emit("  • 当前所有账号将直连，不使用代理。")

    # ------- 登录/下线 -------
    async def login(self, session_name: str) -> str:
        if session_name in self.blocked:
            raise RuntimeError(f"该账号已在被风控列表中：{self.blocked[session_name]}")

        sess_path = os.path.join(SESS_DIR, session_name)
        # 为此 session 取代理（如果启用）
        assigned_proxy = self.session_proxy_map.get(session_name) if self.proxy_enabled else None
        conn, proxy_tuple = make_telethon_proxy(assigned_proxy)

        kw = {}
        if conn:
            kw["connection"] = conn
        if proxy_tuple:
            kw["proxy"] = proxy_tuple

        client = TelegramClient(sess_path, API_ID, API_HASH, **kw)
        await client.connect()
        if not await client.is_user_authorized():
            try:
                await client.start()
            except SessionPasswordNeededError:
                raise RuntimeError("该账号启用了两步验证：请先用其他方式完成一次登录生成 .session")

        me = await client.get_me()
        self.clients[session_name] = client
        self.stop_flags[session_name] = False

        # 收消息监听
        async def on_new_message(event):
            try:
                sender = await event.get_sender()
                name = sender.first_name or sender.username or "未知"
                text = event.raw_text
                bridge.chat.emit(f"📩 [{session_name}] {name}: {text}")
                try:
                    await client.send_read_acknowledge(event.chat_id, max_id=event.id)
                except Exception as ack_err:
                    bridge.log.emit(f"⚠️ [{session_name}] 标记已读失败：{ack_err}")
                try:
                    self.progress.record_partner_reply(session_name)
                    self.notify_stats()
                except Exception as stat_err:
                    bridge.log.emit(f"⚠️ [{session_name}] 更新回复统计失败：{stat_err}")
            except Exception as e:
                bridge.log.emit(f"⚠️ [{session_name}] 消息监听异常：{e}")

        async def on_message_read(event):
            try:
                self.progress.record_partner_read(session_name)
                self.notify_stats()
            except Exception as e:
                bridge.log.emit(f"⚠️ [{session_name}] 更新已读统计失败：{e}")

        client.add_event_handler(on_new_message, events.NewMessage(incoming=True))
        try:
            read_event = events.MessageRead(outgoing=True)
        except TypeError:
            read_event = events.MessageRead(outbox=True)
        client.add_event_handler(on_message_read, read_event)
        bridge.log.emit(f"✅ 登录成功：{session_name} - {me.first_name or ''} ({me.phone})")
        self.progress.touch_session(session_name)
        self.notify_stats()
        return f"{me.first_name or ''} ({me.phone})"

    async def logout(self, session_name: str):
        self.stop_flags[session_name] = True
        self._cancel_pending_reply_tasks(session_name)
        c = self.clients.get(session_name)
        if c:
            await c.disconnect()
            self.clients.pop(session_name, None)
            bridge.log.emit(f"🚪 已下线：{session_name}")
        self.notify_stats()

    # ------- 发送辅助 -------
    async def wait_until_read(self, client: TelegramClient, chat_id, message_id, timeout=600):
        start = time.time()
        while time.time() - start < timeout:
            msg = await client.get_messages(chat_id, ids=message_id)
            if msg and msg.read:
                return True
            await asyncio.sleep(5)
        return False

    async def wait_for_reply(self, client: TelegramClient, user_id, timeout: Optional[int] = 900):
        fut = asyncio.Future()

        @client.on(events.NewMessage(from_users=user_id))
        async def _handler(event):
            if not fut.done():
                fut.set_result(event)

        try:
            if timeout is None:
                return await fut
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        except asyncio.CancelledError:
            if not fut.done():
                fut.cancel()
            raise
        finally:
            # 尝试移除监听（Telethon目前不提供直接移除单个handler的简单API，这里交由GC）
            pass

    def _cancel_pending_reply_tasks(self, session_name: str):
        tasks = self.pending_reply_tasks.pop(session_name, [])
        for task in tasks:
            if not task.done():
                task.cancel()

    async def safe_send(self, client: TelegramClient, session_name: str, entity, text: str):
        try:
            return await client.send_message(entity, text)
        except (PeerFloodError, FloodWaitError) as e:
            # 触发风控，立即停止
            self.stop_flags[session_name] = True
            self._cancel_pending_reply_tasks(session_name)
            self.blocked[session_name] = f"{now_ts()} {type(e).__name__}: {e}"
            save_json(BLOCKED_FILE, self.blocked)
            bridge.log.emit(f"🛑 [{session_name}] 触发风控：{e}，已停止该账号任务并记录。")
            self.notify_stats()
            raise
        except UserDeactivatedBanError as e:
            self.stop_flags[session_name] = True
            self._cancel_pending_reply_tasks(session_name)
            self.blocked[session_name] = f"{now_ts()} {type(e).__name__}"
            save_json(BLOCKED_FILE, self.blocked)
            bridge.log.emit(f"💀 [{session_name}] 账号封禁。")
            self.notify_stats()
            raise
        except (ChatWriteForbiddenError, ChatAdminRequiredError) as e:
            bridge.log.emit(f"⚠️ [{session_name}] 无法发送（权限）：{e}")
            # 非致命，继续下一条
        except Exception as e:
            bridge.log.emit(f"❌ [{session_name}] 发送异常：{e}")
        return None

    # ------- 核心：导入通讯录 & 群发（含智能重发） -------
    async def import_and_broadcast(self,
                                   session_name: str,
                                   phones: List[str],
                                   base_text: str,
                                   spacing_sec: float,
                                   strategy: str,
                                   wait_reply_sec: int,
                                   second_delay_sec: int,
                                   schedule_second_sec: int = 3600,
                                   delay_jitter_sec: float = 0.0,
                                   max_first_messages: int = 0):
        client = self.clients.get(session_name)
        if not client:
            raise RuntimeError("该账号未登录。")

        already_sent = self.progress.get_sent_numbers(session_name)
        daily_counts = self.progress.get_session_daily_counts(session_name)
        already_today = daily_counts.get("first_sent", 0)

        remaining_quota = None
        if max_first_messages > 0:
            remaining_quota = max_first_messages - already_today
            if remaining_quota <= 0:
                bridge.log.emit(
                    f"⛔ [{session_name}] 今日已首发 {already_today} 条，已达到单账号上限 {max_first_messages}。")
                return

        numbers_to_send: List[str] = []
        skipped = 0
        for phone in phones:
            if phone in already_sent:
                skipped += 1
            else:
                numbers_to_send.append(phone)

        if skipped:
            bridge.log.emit(f"🔁 [{session_name}] 跳过 {skipped} 个已记录号码（断点续发）。")

        if not numbers_to_send:
            bridge.log.emit(f"⚠️ [{session_name}] 没有新的号码需要发送。")
            return

        if remaining_quota is not None and len(numbers_to_send) > remaining_quota:
            bridge.log.emit(
                f"ℹ️ [{session_name}] 受单账号上限限制，本轮仅发送 {remaining_quota} 个目标。")
            numbers_to_send = numbers_to_send[:remaining_quota]

        planned_first_total = len(numbers_to_send)
        bridge.log.emit(
            f"📊 [{session_name}] 今日已首发 {already_today} 条，本轮计划首发 {planned_first_total} 条。")

        ok = 0
        reply_tasks: List[asyncio.Task] = []

        for idx, phone in enumerate(numbers_to_send, 1):
            if self.stop_flags.get(session_name):
                bridge.log.emit(f"🧯 [{session_name}] 任务已被标记停止。")
                break

            try:
                contact = InputPhoneContact(client_id=idx, phone=phone, first_name=f"User{idx}", last_name="")
                result = await client(ImportContactsRequest([contact]))
                user = result.users[0] if result.users else None

                if not user:
                    bridge.log.emit(f"⚠️ [{session_name}] 无法添加：{phone}  ({idx}/{planned_first_total})")
                else:
                    first_msg = self.get_random_add_msg(base_text or "Hola 😊")
                    sent = await self.safe_send(client, session_name, user.id, first_msg)
                    if sent:
                        ok += 1
                        bridge.log.emit(f"📤 [{session_name}] 已发送首条 → {phone}  （{idx}/{planned_first_total}）")
                        if self.progress.register_first_send(session_name, phone):
                            already_sent.add(phone)
                            self.notify_stats()

                    if strategy == "已读后再发":
                        if sent and await self.wait_until_read(client, user.id, sent.id, timeout=wait_reply_sec):
                            await asyncio.sleep(apply_delay_jitter(second_delay_sec, delay_jitter_sec))
                            second_msg = self.get_random_reply_msg("👌")
                            follow = await self.safe_send(client, session_name, user.id, second_msg)
                            if follow:
                                bridge.log.emit(f"👀 [{session_name}] 已读后二次发送 → {phone}")
                                self.progress.record_followup_send(session_name)
                                self.notify_stats()

                    elif strategy == "回复后再发":
                        async def monitor_reply(target_id: int, phone_number: str):
                            try:
                                bridge.log.emit(f"👂 [{session_name}] 已启动回复监听 → {phone_number}")
                                reply_event = await self.wait_for_reply(client, target_id, timeout=None)
                                if reply_event is not None:
                                    try:
                                        await client.send_read_acknowledge(reply_event.chat_id, max_id=reply_event.id)
                                    except Exception as ack_err:
                                        bridge.log.emit(f"⚠️ [{session_name}] 标记回复已读失败（{phone_number}）：{ack_err}")
                                if self.stop_flags.get(session_name):
                                    bridge.log.emit(f"⏹️ [{session_name}] 任务已停止，跳过回复后二次发送 → {phone_number}")
                                    return
                                await asyncio.sleep(apply_delay_jitter(second_delay_sec, delay_jitter_sec))
                                if self.stop_flags.get(session_name):
                                    bridge.log.emit(f"⏹️ [{session_name}] 任务已停止，跳过回复后二次发送 → {phone_number}")
                                    return
                                second_msg = self.get_random_reply_msg("👌")
                                follow = await self.safe_send(client, session_name, target_id, second_msg)
                                if follow:
                                    bridge.log.emit(f"💬 [{session_name}] 对方回复后二次发送 → {phone_number}")
                                    self.progress.record_followup_send(session_name)
                                    self.notify_stats()
                            except asyncio.CancelledError:
                                if not self.stop_flags.get(session_name):
                                    bridge.log.emit(f"⏹️ [{session_name}] 已取消回复监听 → {phone_number}")
                                return
                            except Exception as e:
                                bridge.log.emit(f"❌ [{session_name}] 监听回复时出错（{phone_number}）：{e}")
                            finally:
                                tasks = self.pending_reply_tasks.get(session_name)
                                if tasks:
                                    try:
                                        tasks.remove(asyncio.current_task())
                                    except ValueError:
                                        pass

                        task = asyncio.create_task(monitor_reply(user.id, phone))
                        reply_tasks.append(task)
                        lst = self.pending_reply_tasks.setdefault(session_name, [])
                        lst.append(task)

                    elif strategy == "定时再次发送":
                        async def later_send(target_id: int, phone_number: str):
                            await asyncio.sleep(apply_delay_jitter(schedule_second_sec, delay_jitter_sec))
                            if self.stop_flags.get(session_name):
                                bridge.log.emit(f"⏹️ [{session_name}] 任务已停止，跳过定时二次发送 → {phone_number}")
                                return
                            second_msg = self.get_random_reply_msg("👌")
                            follow = await self.safe_send(client, session_name, target_id, second_msg)
                            if follow:
                                bridge.log.emit(f"⏰ [{session_name}] 定时二次发送 → {phone_number}")
                                self.progress.record_followup_send(session_name)
                                self.notify_stats()

                        asyncio.create_task(later_send(user.id, phone))

            except (PeerFloodError, FloodWaitError, UserDeactivatedBanError):
                break
            except Exception as e:
                bridge.log.emit(f"❌ [{session_name}] 处理 {phone} 出错：{e}")

            if not self.stop_flags.get(session_name) and idx < planned_first_total:
                await asyncio.sleep(apply_delay_jitter(spacing_sec, delay_jitter_sec))

        if strategy == "回复后再发" and reply_tasks:
            bridge.log.emit(
                f"📨 [{session_name}] 已完成首轮群发，持续监听回复以发送二次消息（{len(reply_tasks)} 个联系人）。")

        latest_daily = self.progress.get_session_daily_counts(session_name)
        bridge.log.emit(
            f"✅ [{session_name}] 群发完成：成功首发 {ok}/{planned_first_total}，今日累计 {latest_daily.get('first_sent', 0)} 条。")
        self.notify_stats()

CLIENTS = ClientManager()


# ========== GUI ==========
class StatsPanel(QtWidgets.QGroupBox):
    def __init__(self):
        super().__init__("统计（今日）")
        layout = QtWidgets.QFormLayout()
        self.lbl_logged_in = QtWidgets.QLabel("0")
        self.lbl_blocked = QtWidgets.QLabel("0")
        self.lbl_today_sent = QtWidgets.QLabel("0")
        self.lbl_reads = QtWidgets.QLabel("0")
        self.lbl_partner_replies = QtWidgets.QLabel("0")
        self.lbl_self_replies = QtWidgets.QLabel("0")

        layout.addRow("登录总账号", self.lbl_logged_in)
        layout.addRow("风控账号", self.lbl_blocked)
        layout.addRow("当天发送数据", self.lbl_today_sent)
        layout.addRow("对方已读消息数", self.lbl_reads)
        layout.addRow("对方回复消息数", self.lbl_partner_replies)
        layout.addRow("自己回复消息数", self.lbl_self_replies)

        self.setLayout(layout)
        self.setMinimumWidth(240)

    def update_stats(self, stats: dict):
        self.lbl_logged_in.setText(str(stats.get("logged_in", 0)))
        self.lbl_blocked.setText(str(stats.get("blocked", 0)))
        self.lbl_today_sent.setText(str(stats.get("today_sent", 0)))
        self.lbl_reads.setText(str(stats.get("partner_reads", 0)))
        self.lbl_partner_replies.setText(str(stats.get("partner_replies", 0)))
        self.lbl_self_replies.setText(str(stats.get("self_replies", 0)))


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Telegram 多功能控制面板（中文 GUI v5.0）")
        self.resize(1100, 720)
        self._setup_ui()
        self._bind()
        self.refresh_sessions()
        CLIENTS.notify_stats()

    # ----- UI -----
    def _setup_ui(self):
        central = QtWidgets.QWidget(); self.setCentralWidget(central)

        # 左侧：会话 & 控制
        self.list_sessions = QtWidgets.QListWidget()
        self.btn_refresh = QtWidgets.QPushButton("🔄 刷新会话列表")
        self.btn_login = QtWidgets.QPushButton("🔐 登录")
        self.btn_logout = QtWidgets.QPushButton("🚪 下线")
        self.btn_login_all = QtWidgets.QPushButton("🔐 登录全部")
        self.lab_status = QtWidgets.QLabel("状态：未登录")

        self.btn_load_msgpool = QtWidgets.QPushButton("🗃 加载消息池")
        self.btn_load_proxypool = QtWidgets.QPushButton("🌐 加载代理池（可不启用）")

        left = QtWidgets.QVBoxLayout()
        left.addWidget(QtWidgets.QLabel("会话（.session）"))
        left.addWidget(self.list_sessions, 5)
        left.addWidget(self.btn_refresh)
        left.addWidget(self.btn_login)
        left.addWidget(self.btn_logout)
        left.addWidget(self.btn_login_all)
        left.addSpacing(12)
        left.addWidget(self.btn_load_msgpool)
        left.addWidget(self.btn_load_proxypool)
        left.addStretch(1)
        left.addWidget(self.lab_status)

        # 右侧：Tabs
        self.tabs = QtWidgets.QTabWidget()

        # Tab1: 收发消息
        tab1 = QtWidgets.QWidget()
        self.view_chat = QtWidgets.QTextEdit(); self.view_chat.setReadOnly(True)
        self.ed_target = QtWidgets.QLineEdit(); self.ed_target.setPlaceholderText("输入目标用户名或ID，例如 @username 或 123456789")
        self.ed_text = QtWidgets.QLineEdit(); self.ed_text.setPlaceholderText("输入要发送的消息（留空则使用消息池随机）")
        self.btn_send = QtWidgets.QPushButton("发送消息")
        layout1 = QtWidgets.QHBoxLayout(tab1)
        chat_column = QtWidgets.QVBoxLayout()
        chat_column.addWidget(QtWidgets.QLabel("目标用户"))
        chat_column.addWidget(self.ed_target)
        chat_column.addWidget(QtWidgets.QLabel("消息窗口"))
        chat_column.addWidget(self.view_chat, 5)
        row1 = QtWidgets.QHBoxLayout()
        row1.addWidget(self.ed_text, 5)
        row1.addWidget(self.btn_send, 1)
        chat_column.addLayout(row1)
        layout1.addLayout(chat_column, 7)
        self.stats_panel = StatsPanel()
        layout1.addWidget(self.stats_panel, 3)
        self.tabs.addTab(tab1, "💬 收发消息")

        # Tab2: 导入通讯录并群发
        tab2 = QtWidgets.QWidget()
        self.ed_file = QtWidgets.QLineEdit(); self.ed_file.setPlaceholderText("选择 TXT/CSV 文件，每行一个手机号")
        self.btn_browse = QtWidgets.QPushButton("选择文件")
        self.txt_broadcast = QtWidgets.QTextEdit(); self.txt_broadcast.setPlaceholderText("首条消息备用文本（留空则使用添加后消息池随机）")
        self.spin_spacing = QtWidgets.QDoubleSpinBox(); self.spin_spacing.setDecimals(1); self.spin_spacing.setRange(0.0, 600.0); self.spin_spacing.setValue(DEFAULT_SPACING_SEC)
        self.spin_jitter = QtWidgets.QDoubleSpinBox(); self.spin_jitter.setDecimals(1); self.spin_jitter.setRange(0.0, 600.0); self.spin_jitter.setValue(DEFAULT_DELAY_JITTER); self.spin_jitter.setSingleStep(0.5)
        self.cmb_strategy = QtWidgets.QComboBox(); self.cmb_strategy.addItems(["不重发","已读后再发","回复后再发","定时再次发送"])
        self.cmb_strategy.setCurrentText(DEFAULT_STRATEGY)
        self.spin_waitreply = QtWidgets.QSpinBox(); self.spin_waitreply.setRange(30, 7200); self.spin_waitreply.setValue(DEFAULT_WAIT_REPLY_SEC)
        self.spin_second_delay = QtWidgets.QSpinBox(); self.spin_second_delay.setRange(0, 3600); self.spin_second_delay.setValue(DEFAULT_SECOND_DELAY)
        self.spin_schedule_second = QtWidgets.QSpinBox(); self.spin_schedule_second.setRange(60, 86400); self.spin_schedule_second.setValue(3600)
        self.spin_message_limit = QtWidgets.QSpinBox(); self.spin_message_limit.setRange(0, 100000); self.spin_message_limit.setValue(0); self.spin_message_limit.setSingleStep(10); self.spin_message_limit.setSpecialValueText("不限")
        self.btn_run_import = QtWidgets.QPushButton("📤 导入并发送")

        grid2 = QtWidgets.QGridLayout(tab2)
        grid2.addWidget(QtWidgets.QLabel("号码文件"), 0, 0)
        grid2.addWidget(self.ed_file, 0, 1)
        grid2.addWidget(self.btn_browse, 0, 2)
        grid2.addWidget(QtWidgets.QLabel("首条消息备用"), 1, 0)
        grid2.addWidget(self.txt_broadcast, 1, 1, 1, 2)
        grid2.addWidget(QtWidgets.QLabel("间隔(秒)"), 2, 0)
        grid2.addWidget(self.spin_spacing, 2, 1)
        grid2.addWidget(QtWidgets.QLabel("延时抖动±(秒)"), 3, 0)
        grid2.addWidget(self.spin_jitter, 3, 1)
        grid2.addWidget(QtWidgets.QLabel("二次策略"), 4, 0)
        grid2.addWidget(self.cmb_strategy, 4, 1)
        grid2.addWidget(QtWidgets.QLabel("等待回复/已读(秒)"), 5, 0)
        grid2.addWidget(self.spin_waitreply, 5, 1)
        grid2.addWidget(QtWidgets.QLabel("二次延迟(秒)"), 6, 0)
        grid2.addWidget(self.spin_second_delay, 6, 1)
        grid2.addWidget(QtWidgets.QLabel("定时再次(秒)"), 7, 0)
        grid2.addWidget(self.spin_schedule_second, 7, 1)
        grid2.addWidget(QtWidgets.QLabel("单账号首发数量"), 8, 0)
        grid2.addWidget(self.spin_message_limit, 8, 1)
        grid2.addWidget(self.btn_run_import, 9, 2)
        self.tabs.addTab(tab2, "📇 导入通讯录并群发")

        # Tab3: 消息池管理（左右两列）
        tab3 = QtWidgets.QWidget()
        self.list_add_pool = QtWidgets.QListWidget()
        self.list_reply_pool = QtWidgets.QListWidget()
        self.ed_add_msg = QtWidgets.QLineEdit(); self.ed_add_msg.setPlaceholderText("添加后消息：输入后点“添加到首条池”")
        self.ed_reply_msg = QtWidgets.QLineEdit(); self.ed_reply_msg.setPlaceholderText("回复后消息：输入后点“添加到二次池”")
        self.btn_add_add = QtWidgets.QPushButton("添加到首条池")
        self.btn_add_reply = QtWidgets.QPushButton("添加到二次池")
        self.btn_del_add = QtWidgets.QPushButton("删除选中（首条池）")
        self.btn_del_reply = QtWidgets.QPushButton("删除选中（二次池）")
        self.btn_save_pools = QtWidgets.QPushButton("💾 保存两个消息池")
        grid3 = QtWidgets.QGridLayout(tab3)
        grid3.addWidget(QtWidgets.QLabel("首条消息池 (messages_add.json)"), 0, 0)
        grid3.addWidget(QtWidgets.QLabel("二次消息池 (messages_reply.json)"), 0, 1)
        grid3.addWidget(self.list_add_pool, 1, 0)
        grid3.addWidget(self.list_reply_pool, 1, 1)
        grid3.addWidget(self.ed_add_msg, 2, 0)
        grid3.addWidget(self.ed_reply_msg, 2, 1)
        grid3.addWidget(self.btn_add_add, 3, 0)
        grid3.addWidget(self.btn_add_reply, 3, 1)
        grid3.addWidget(self.btn_del_add, 4, 0)
        grid3.addWidget(self.btn_del_reply, 4, 1)
        grid3.addWidget(self.btn_save_pools, 5, 0, 1, 2)
        self.tabs.addTab(tab3, "🗃 消息池管理")

        # Tab4: 日志
        tab4 = QtWidgets.QWidget()
        self.view_log = QtWidgets.QTextEdit(); self.view_log.setReadOnly(True)
        lay4 = QtWidgets.QVBoxLayout(tab4)
        lay4.addWidget(self.view_log)
        self.tabs.addTab(tab4, "🧾 日志")

        # 总布局
        root = QtWidgets.QHBoxLayout(central)
        left_wrap = QtWidgets.QWidget(); left_wrap.setLayout(left)
        root.addWidget(left_wrap, 3)
        root.addWidget(self.tabs, 7)

    # ----- 事件绑定 -----
    def _bind(self):
        self.btn_refresh.clicked.connect(self.refresh_sessions)
        self.btn_login.clicked.connect(self.on_login)
        self.btn_logout.clicked.connect(self.on_logout)
        self.btn_login_all.clicked.connect(self.on_login_all)

        self.btn_load_msgpool.clicked.connect(self.on_load_msgpool)
        self.btn_load_proxypool.clicked.connect(self.on_load_proxypool)

        self.btn_send.clicked.connect(self.on_send)

        self.btn_browse.clicked.connect(self.on_browse)
        self.btn_run_import.clicked.connect(self.on_run_import)

        self.btn_add_add.clicked.connect(self.on_add_to_addpool)
        self.btn_add_reply.clicked.connect(self.on_add_to_replypool)
        self.btn_del_add.clicked.connect(self.on_del_from_addpool)
        self.btn_del_reply.clicked.connect(self.on_del_from_replypool)
        self.btn_save_pools.clicked.connect(self.on_save_pools)

        bridge.log.connect(self.append_log)
        bridge.chat.connect(self.append_chat)
        bridge.status.connect(self.set_status)
        bridge.stats.connect(self.stats_panel.update_stats)

    # ----- UI辅助 -----
    def current_session(self) -> Optional[str]:
        it = self.list_sessions.currentItem()
        return it.text() if it else None

    def append_log(self, s: str):
        self.view_log.append(s)

    def append_chat(self, s: str):
        self.view_chat.append(s)

    def set_status(self, s: str):
        self.lab_status.setText(f"状态：{s}")

    def refresh_sessions(self):
        self.list_sessions.clear()
        ensure_dir(SESS_DIR)
        for s in CLIENTS.list_sessions():
            self.list_sessions.addItem(s)
        self.append_log("🔄 会话列表已刷新。")

    # ----- 登录/下线 -----
    def on_login(self):
        sess = self.current_session()
        if not sess:
            QtWidgets.QMessageBox.warning(self, "提示", "请选择一个 .session")
            return
        fut = run_coro(CLIENTS.login(sess))
        def done(f):
            try:
                me = f.result()
                bridge.status.emit(f"{sess} 已登录：{me}")
            except Exception as e:
                bridge.log.emit(f"❌ 登录失败：{e}")
        fut.add_done_callback(done)

    def on_login_all(self):
        sessions = CLIENTS.list_sessions()
        if not sessions:
            QtWidgets.QMessageBox.information(self, "提示", "没有 .session 文件")
            return
        for s in sessions:
            run_coro(CLIENTS.login(s))

    def on_logout(self):
        sess = self.current_session()
        if not sess:
            QtWidgets.QMessageBox.warning(self, "提示", "请选择要下线的会话")
            return
        run_coro(CLIENTS.logout(sess)).add_done_callback(lambda _ : bridge.status.emit(f"{sess} 已下线"))

    # ----- 消息池 -----
    def on_load_msgpool(self):
        CLIENTS.load_message_pools()
        # 同步展示
        self.list_add_pool.clear()
        self.list_reply_pool.clear()
        self.list_add_pool.addItems(CLIENTS.pool_add)
        self.list_reply_pool.addItems(CLIENTS.pool_reply)

    def on_add_to_addpool(self):
        txt = self.ed_add_msg.text().strip()
        if txt:
            self.list_add_pool.addItem(txt)
            self.ed_add_msg.clear()

    def on_add_to_replypool(self):
        txt = self.ed_reply_msg.text().strip()
        if txt:
            self.list_reply_pool.addItem(txt)
            self.ed_reply_msg.clear()

    def on_del_from_addpool(self):
        for it in self.list_add_pool.selectedItems():
            self.list_add_pool.takeItem(self.list_add_pool.row(it))

    def on_del_from_replypool(self):
        for it in self.list_reply_pool.selectedItems():
            self.list_reply_pool.takeItem(self.list_reply_pool.row(it))

    def on_save_pools(self):
        add_msgs = [self.list_add_pool.item(i).text() for i in range(self.list_add_pool.count())]
        reply_msgs = [self.list_reply_pool.item(i).text() for i in range(self.list_reply_pool.count())]
        save_json(MSG_ADD_FILE, add_msgs)
        save_json(MSG_REPLY_FILE, reply_msgs)
        CLIENTS.pool_add = add_msgs
        CLIENTS.pool_reply = reply_msgs
        CLIENTS.pools_loaded = True
        QtWidgets.QMessageBox.information(self, "提示", "两个消息池已保存。")
        bridge.log.emit(f"💾 消息池已保存：首条 {len(add_msgs)} 条，二次 {len(reply_msgs)} 条。")

    # ----- 代理池 -----
    def on_load_proxypool(self):
        ret = QtWidgets.QMessageBox.question(self, "代理选择", "是否启用代理池？\n选择“是”将从 proxy_pool.txt 加载并分配；\n选择“否”将不使用代理直连。")
        if ret == QtWidgets.QMessageBox.Yes:
            CLIENTS.load_proxy_pool()
        else:
            CLIENTS.proxy_enabled = False
            CLIENTS.session_proxy_map = {}
            bridge.log.emit("🌐 已选择不启用代理，所有账号将直连。")

    # ----- 单发 -----
    def on_send(self):
        sess = self.current_session()
        if not sess:
            QtWidgets.QMessageBox.warning(self, "提示", "请选择一个已登录账号")
            return
        target = self.ed_target.text().strip()
        if not target:
            QtWidgets.QMessageBox.warning(self, "提示", "请输入目标用户名或ID")
            return
        text = self.ed_text.text().strip() or CLIENTS.get_random_add_msg("Hola 😊")
        client = CLIENTS.clients.get(sess)
        if not client:
            QtWidgets.QMessageBox.warning(self, "提示", "该账号未登录")
            return
        async def _task():
            await CLIENTS.safe_send(client, sess, target, text)
            bridge.chat.emit(f"✅ [{sess}] 我 → {target}: {text}")
        run_coro(_task())

    # ----- 导入并群发 -----
    def on_browse(self):
        p, _ = QtWidgets.QFileDialog.getOpenFileName(self, "选择号码文件", "", "TXT/CSV (*.txt *.csv);;所有文件 (*)")
        if p:
            self.ed_file.setText(p)

    def on_run_import(self):
        sess = self.current_session()
        if not sess:
            QtWidgets.QMessageBox.warning(self, "提示", "请选择一个已登录账号")
            return
        if not CLIENTS.pools_loaded:
            ret = QtWidgets.QMessageBox.question(self, "消息池未加载", "尚未加载消息池，是否立即加载？")
            if ret == QtWidgets.QMessageBox.Yes:
                self.on_load_msgpool()
            else:
                bridge.log.emit("⚠️ 未加载消息池，将仅使用输入的首条备用文本。")

        path = self.ed_file.text().strip()
        if not path or not os.path.exists(path):
            QtWidgets.QMessageBox.warning(self, "提示", "请选择有效号码文件")
            return
        phones = read_phone_file(path)
        if not phones:
            QtWidgets.QMessageBox.warning(self, "提示", "未读取到有效手机号")
            return

        base_text = self.txt_broadcast.toPlainText().strip()
        spacing = float(self.spin_spacing.value())
        jitter = float(self.spin_jitter.value())
        strategy = self.cmb_strategy.currentText()
        wait_reply = int(self.spin_waitreply.value())
        second_delay = int(self.spin_second_delay.value())
        schedule_second = int(self.spin_schedule_second.value())
        max_first = int(self.spin_message_limit.value())

        plan_desc = f"计划首发 {min(len(phones), max_first) if max_first > 0 else len(phones)} 人" if phones else "无有效号码"
        bridge.log.emit(f"📱 读取号码 {len(phones)} 个，开始群发（策略：{strategy}，延时抖动±{jitter:.1f}s，{plan_desc}）...")
        run_coro(CLIENTS.import_and_broadcast(
            session_name=sess,
            phones=phones,
            base_text=base_text,
            spacing_sec=spacing,
            strategy=strategy,
            wait_reply_sec=wait_reply,
            second_delay_sec=second_delay,
            schedule_second_sec=schedule_second,
            delay_jitter_sec=jitter,
            max_first_messages=max_first
        ))


# ========== 入口 ==========
if __name__ == "__main__":
    QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling, True)
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle("Fusion")

    ensure_dir(SESS_DIR)
    win = MainWindow()
    win.show()
    sys.exit(app.exec_())
