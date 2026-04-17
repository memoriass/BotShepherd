"""
单个代理连接的实现
处理客户端与多个目标端点之间的消息转发
"""

import asyncio
import time as _time
import websockets
import json
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

from ..onebotv11.models import ApiResponse, Event
from ..onebotv11.message_segment import MessageSegmentParser
from ..commands import CommandHandler
from .message_processor import MessageProcessor
from ..utils.reboot import construct_reboot_message


class ProxyConnection:
    """单个代理连接"""

    def __init__(self, connection_id, config, client_ws, config_manager, database_manager, logger, backup_manager=None, status_callback=None, api_response_callback=None):
        self.connection_id = connection_id
        self.config = config
        self.client_ws = client_ws
        self.config_manager = config_manager
        self.database_manager = database_manager
        self.logger = logger
        self.backup_manager = backup_manager
        self.status_callback = status_callback
        self.api_response_callback = api_response_callback

        self.target_connections = []
        self.echo_cache = {}
        self.running = False
        self.client_alive = False  # 标记 NapCat client_ws 当前是否在线
        self.client_headers = None
        self.first_message = None
        self.self_id: int | None = None

        # 保活策略：只对自动注入的 manager target 生效，避免影响其他框架
        self.target_keepalive_enabled = bool(self.config.get("keep_target_alive", False))
        _target_eps = list(self.config.get("target_endpoints", []))
        # 识别哪些 target 是管理器接收端点（需要独立保活）
        self.manager_target_indexes: set = {
            idx for idx, ep in enumerate(_target_eps, start=1)
            if "/ws/napcat/" in ep or ep.endswith("/ws/onebot/v11/ws")
        }

        self.reconnect_locks = []  # 每个 target_index 一个 Lock
        for _ in self.config.get("target_endpoints", []):
            self.reconnect_locks.append(asyncio.Lock())

        # ── QQ 登录状态监控 ──
        self._pending_login_checks: dict = {}   # echo -> asyncio.Future
        self._qq_logged_in: bool | None = None  # BS 侧缓存的 QQ 登录态
        self._login_monitor_task: asyncio.Task | None = None
        _LOGIN_CHECK_INTERVAL = 60              # 秒

        # 初始化消息处理器
        self.message_processor = MessageProcessor(config_manager, database_manager, logger)

        # 自身指令处理
        self.command_handler = CommandHandler(config_manager, database_manager, logger, backup_manager)

    async def start_proxy(self):
        """启动代理"""
        self.running = True
        was_reconnect = not self.client_alive and self.target_keepalive_enabled
        self.client_alive = True  # client_ws 已连入

        # ★ 保活模式下 client 重连，向 manager target 推送合成 connect 事件
        if was_reconnect:
            await self._send_synthetic_lifecycle("connect")

        try:
            # 等待客户端第一个消息以获取请求头
            self.first_message = await self.client_ws.recv()

            try:
                # 尝试不同的方式获取请求头
                if hasattr(self.client_ws, 'request_headers'):
                    self.client_headers = self.client_ws.request_headers
                elif hasattr(self.client_ws, 'headers'):
                    self.client_headers = self.client_ws.headers
                elif hasattr(self.client_ws, 'request') and hasattr(self.client_ws.request, 'headers'):
                    self.client_headers = self.client_ws.request.headers
                else:
                    self.client_headers = {}
                    self.logger.ws.warning(f"[{self.connection_id}] 无法获取客户端请求头")
            except Exception as e:
                self.logger.ws.warning(f"[{self.connection_id}] 获取客户端请求头失败: {e}")
                self.client_headers = {}


            # 连接到目标端点
            await self._connect_to_targets()

            # 处理第一个消息，其中yunzai需要这个lifecycle消息来注册
            await self._process_client_message(self.first_message)

            await self.send_reboot_message()

            # 启动消息转发任务
            tasks = []

            # 客户端到目标的转发任务
            tasks.append(asyncio.create_task(self._forward_client_to_targets()))

            # 目标到客户端的转发任务
            for idx, target_ws in enumerate(self.target_connections):
                if target_ws:
                    tasks.append(asyncio.create_task(
                        self._forward_target_to_client(target_ws, self.list_index2target_index(idx))
                    ))

            # ★ 启动 QQ 登录状态后台监控（仅保活模式）
            if self.target_keepalive_enabled and self.manager_target_indexes:
                self._login_monitor_task = asyncio.create_task(self._login_monitor_loop())
                tasks.append(self._login_monitor_task)

            if tasks:
                # 等待任务，当任意任务结束时（如客户端断开）立即取消其他任务
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

                # 取消所有未完成的任务
                for task in pending:
                    task.cancel()

                # 等待所有任务真正结束
                if pending:
                    await asyncio.wait(pending, timeout=3.0)

                # 检查是否是客户端断开导致的
                for task in done:
                    if task.exception():
                        self.logger.ws.info(f"[{self.connection_id}] 任务异常退出: {task.exception()}")
            else:
                self.logger.ws.warning(f"[{self.connection_id}] 没有转发任务，连接将关闭")

        except Exception as e:
            self.logger.ws.error(f"[{self.connection_id}] 代理运行错误: {e}")
        finally:
            # client 已离线：标记状态，按保活策略决定是否同时关闭 target
            await self.on_client_disconnect()


    async def _connect_to_target(self, endpoint: str, target_index: int):
         try:

            if target_index > len(self.target_connections) + 1 or target_index == 0:
                raise Exception(f"[{self.connection_id}] 目标ID {target_index} 超出范围!")

            # 使用客户端请求头连接目标
            extra_headers = {}
            if self.client_headers:
                # 更新相关请求头，其中Nonebot必须x-self-id
                for header_name in ["authorization", "x-self-id", "x-client-role", "user-agent"]:
                    if header_name in self.client_headers:
                        extra_headers[header_name] = self.client_headers[header_name]

            # 尝试使用不同的参数名连接，同时配置连接参数
            target_ws = None
            connection_params = {
                'max_size': None,  # 移除消息大小限制
                'max_queue': None,  # 移除队列大小限制
                'ping_interval': 300,  # 心跳间隔
                'ping_timeout': 60,   # 心跳超时
                'close_timeout': None,   # 关闭超时
                'compression': 'deflate'
            }

            connection_attempts = [
                # 尝试 extra_headers 参数
                lambda: websockets.connect(endpoint, extra_headers=extra_headers, **connection_params),
                # 尝试 additional_headers 参数
                lambda: websockets.connect(endpoint, additional_headers=extra_headers, **connection_params),
                # 不使用额外头部，无法连接Nonebot2
                lambda: websockets.connect(endpoint, **connection_params)
            ]

            for attempt in connection_attempts:
                try:
                    target_ws = await attempt()
                    break
                except TypeError:
                    # 参数不支持，尝试下一种方式
                    continue
                except Exception as e:
                    # 其他错误，直接抛出
                    raise e

            if target_index == len(self.target_connections) + 1: # next one to append
                self.target_connections.append(target_ws) # 保证index正确，即使是None也添加
            else:
                self.target_connections[self.target_index2list_index(target_index)] = target_ws
            if target_ws is None:
                raise Exception(f"[{self.connection_id}] 所有连接方式都失败")

            self.logger.ws.info(f"[{self.connection_id}] 已连接到目标: {endpoint}")
            return target_ws

         except Exception as e:
            if target_index == len(self.target_connections) + 1:
                self.target_connections.append(None)
            else:
                self.target_connections[self.target_index2list_index(target_index)] = None
            self.logger.ws.error(f"[{self.connection_id}] 连接目标失败 {endpoint}: {e}")
            return None


    async def _connect_to_targets(self):
        """连接到目标端点。

        保活 runtime 复用场景：target 可能已经在重连循环中，跳过已连接的 target，
        只对尚未连接（None）的 target 尝试初始化，避免与后台重连任务竞态。
        """
        target_endpoints = self.config.get("target_endpoints", [])

        # 先标记哪些目标需要启动重连（避免在循环中启动任务）
        failed_targets = []
        for idx, endpoint in enumerate(target_endpoints):
            target_index = self.list_index2target_index(idx)
            list_idx = self.target_index2list_index(target_index)

            # 保活 runtime 复用：若该 target 已连接（非 None），跳过避免重复连接
            if list_idx < len(self.target_connections) and self.target_connections[list_idx] is not None:
                self.logger.ws.info(
                    f"[{self.connection_id}] target {target_index} 已连接，跳过重复初始化（保活复用）"
                )
                continue

            target_ws = await self._connect_to_target(endpoint, target_index)
            # 如果连接失败（返回 None），记录下来稍后启动重连
            if target_ws is None:
                failed_targets.append(target_index)

        # 在所有连接尝试完成后，启动失败目标的重连任务
        for target_index in failed_targets:
            self.logger.ws.warning(f"[{self.connection_id}] 目标 {target_index} 初始连接失败，启动后台重连")
            asyncio.create_task(self._start_reconnect_with_delay(target_index))

    async def _forward_client_to_targets(self):
        """转发客户端消息到目标"""
        try:
            async for message in self.client_ws:
                try:
                    await self._process_client_message(message)
                except websockets.exceptions.ConnectionClosed:
                    continue
        except Exception as e:
            self.logger.ws.error(f"[{self.connection_id}] 客户端消息转发错误: {e}")

    async def _forward_target_to_client(self, target_ws, target_index):
        """转发目标消息到客户端"""
        try:
            async for message in target_ws:
                # client 已离线时跳过回送，但不中止 target 接收循环
                if not self.client_alive:
                    self.logger.ws.debug(
                        f"[{self.connection_id}] client 离线，丢弃来自 target {target_index} 的回送消息"
                    )
                    continue
                try:
                    await self._process_target_message(message, target_index)
                except websockets.exceptions.ConnectionClosed:
                    # client 在此期间断开，标记并继续保持 target 活跃
                    self.client_alive = False
                    self.logger.ws.info(
                        f"[{self.connection_id}] target {target_index} 回送时 client 已断开，target 继续保活"
                    )
        except websockets.exceptions.ConnectionClosed:
            await self._reconnect_target(target_index)
        except TypeError:
            await self._reconnect_target(target_index)  # 如果是None，也挂一个后台重连
        except Exception as e:
            self.logger.ws.error(f"[{self.connection_id}] 目标消息转发错误 {target_index}: {e}")

    async def _start_reconnect_with_delay(self, target_index: int):
        """延迟启动重连任务，等待客户端完全初始化"""
        await asyncio.sleep(5)
        await self._reconnect_target(target_index)

    def _should_keep_target_alive(self, target_index: int) -> bool:
        """决定 target 重连是否应该继续。

        manager target（manager_target_indexes 内）在开启保活模式时，
        client 离线后仍继续重连，其他普通 target 跟随 client 生命周期。
        """
        if not self.running:
            return False
        if self.target_keepalive_enabled and target_index in self.manager_target_indexes:
            return True
        return self.client_alive

    async def _reconnect_target(self, target_index: int):
        self.logger.ws.info(f"[{self.connection_id}] 目标连接 {target_index} 已关闭。将在120秒内持续尝试重新连接。")

        lock = self.reconnect_locks[self.target_index2list_index(target_index)]
        if not lock.locked():
            async with lock:
                for _ in range(40):
                    if not self._should_keep_target_alive(target_index):
                        self.logger.ws.info(
                            f"[{self.connection_id}] 停止重连目标 {target_index}"
                            f"（running={self.running}, client_alive={self.client_alive}）"
                        )
                        return

                    await asyncio.sleep(3)
                    try:
                        target_ws = await self._connect_to_target(
                            self.config.get("target_endpoints", [])[self.target_index2list_index(target_index)],
                            target_index,
                        )
                        if target_ws is None:
                            continue
                        # 仅当 client 在线且有首包时才 replay，保证 Yunzai 等框架重新注册
                        if self.client_alive and self.first_message:
                            await self._process_client_message(self.first_message)
                        self.logger.ws.info(f"[{self.connection_id}] 目标连接 {target_index} 恢复成功，5秒后重新开始转发。")
                        await asyncio.sleep(5)
                        await self._forward_target_to_client(target_ws, target_index)
                    except Exception as e:
                        self.logger.ws.warning(f"[{self.connection_id}] 尝试重连目标 {target_index} 失败: {e}")

                # 长期重连循环（120s 快速重试结束后）
                while self._should_keep_target_alive(target_index):
                    await asyncio.sleep(600)  # 10分钟后再试
                    try:
                        target_ws = await self._connect_to_target(
                            self.config.get("target_endpoints", [])[self.target_index2list_index(target_index)],
                            target_index,
                        )
                        if target_ws:
                            if self.client_alive and self.first_message:
                                await self._process_client_message(self.first_message)
                            self.logger.ws.info(f"[{self.connection_id}] 目标连接 {target_index} 恢复成功，5秒后重新开始转发。")
                            await asyncio.sleep(5)
                            await self._forward_target_to_client(target_ws, target_index)
                    except Exception:
                        pass

                self.logger.ws.info(f"[{self.connection_id}] 终止目标 {target_index} 的重连循环")

    async def _process_client_message(self, message: str):
        """处理客户端消息"""
        try:
            # 解析JSON消息
            message_data = json.loads(message)
            if message_data.get("self_id"): # 每次更新，客户端可能会换账号
                if self.self_id and self.self_id != message_data["self_id"]:
                    # 但是，不论是通过头注册还是yunzai的方式都不能支持账号的热切换
                    self.logger.ws.warning("[{}] 客户端账号已切换到 {}，请重启该连接！".format(self.connection_id, message_data['self_id']))
                self.self_id = message_data["self_id"]
                # 通过回调更新状态中的self_id
                if self.status_callback:
                    self.status_callback('self_id', self.self_id)

            # 检查是否是API响应（有echo字段）
            if message_data.get("echo"):
                echo_val = str(message_data["echo"])
                # ★ 优先检查是否是 BS 内部登录检测的响应
                if echo_val in self._pending_login_checks:
                    future = self._pending_login_checks[echo_val]
                    if not future.done():
                        future.set_result(message_data)
                    return  # 内部 API 响应不转发给 target
                # 调用API响应回调（用于处理待处理的API请求，如在线状态检查）
                if self.api_response_callback:
                    if self.api_response_callback(echo_val, message_data):
                        return

            # ★ P1: 覆写心跳的 status.online 为 BS 侧真实 QQ 登录态
            if (message_data.get("meta_event_type") == "heartbeat"
                    and self._qq_logged_in is not None
                    and not self._qq_logged_in):
                if isinstance(message_data.get("status"), dict):
                    message_data["status"]["online"] = False

            # 消息预处理
            message_data = await self.command_handler.preprocesser(message_data)
            processed_message, parsed_event = await self._preprocess_message(message_data)

            if processed_message:
                if self._check_api_call_succ(parsed_event):
                    # 如果是发送成功
                    data_in_api = parsed_event.data
                    if isinstance(data_in_api, dict): # get list api 不可能是发送
                        message_id = message_data.get("data", {}).get("message_id")
                        await self.database_manager.save_message(
                            await self._construct_msg_from_echo(message_data["echo"], message_id=message_id), "SEND", self.connection_id
                        )
                else:
                    # 记录消息到数据库，注意记录的是处理后的消息，所以统计功能是无视别名的，只需要按key搜索即可
                    # 收到裸消息不会是api response
                    self._log_api_call_fail(parsed_event)
                    await self.database_manager.save_message(
                        processed_message, "RECV", self.connection_id
                    )

                # 本体指令集
                resp_api = await self.command_handler.handle_message(parsed_event)
                if resp_api:
                    processed_message = None # 自身返回时，阻止事件传递给框架 Preprocesser不受影响
                    await self._process_target_message(resp_api, 0) # 自身的index为0，其实并不是连接

                # 转发到所有目标
                processed_json = json.dumps(processed_message, ensure_ascii=False)

                if message_data.get("echo"):
                    # api请求内容，尽可能保证各框架的发送api都使用了echo。
                    # 尝试从各个 target_index 的缓存中查找
                    echo_val = str(message_data["echo"])
                    matched_target_index = None
                    for idx in range(1, len(self.target_connections) + 1):
                        echo_key = f"{idx}_{echo_val}"
                        if echo_key in self.echo_cache:
                            matched_target_index = self.echo_cache.pop(echo_key, {}).get("target_index")
                            break

                    if matched_target_index is not None and matched_target_index > 0 and self.target_connections[self.target_index2list_index(matched_target_index)]:
                        self.logger.ws.debug(f"[{self.connection_id}] 发送API请求到目标 {matched_target_index}: {processed_json[:1000]}")
                        try:
                            await self.target_connections[self.target_index2list_index(matched_target_index)].send(processed_json)
                        except websockets.exceptions.ConnectionClosed:
                            return
                        except Exception as e:
                            self.logger.ws.error(f"[{self.connection_id}] 发送到目标失败: {e}")
                            raise
                else:
                    for target_index, target_ws in enumerate(self.target_connections):
                        if target_ws:
                            try:
                                await target_ws.send(processed_json)
                            except websockets.exceptions.ConnectionClosed:
                                continue # 发送时不再捕捉
                            except Exception as e:
                                self.logger.ws.error(f"[{self.connection_id}] 发送到目标失败: {e}")
                                raise

        except json.JSONDecodeError:
            self.logger.ws.warning(f"[{self.connection_id}] 收到非JSON消息: {message[:1000]}")
        except websockets.exceptions.ConnectionClosed:
            raise
        except Exception as e:
            self.logger.ws.error(f"[{self.connection_id}] 处理客户端消息失败: {e}")

    async def _process_target_message(self, message: str | dict, target_index: int):
        """处理目标消息"""
        try:
            # 解析JSON消息
            if isinstance(message, str):
                message_data = json.loads(message)
            else:
                message_data = message

            self.logger.ws.debug(f"[{self.connection_id}] 来自连接 {target_index} 的API响应: {str(message_data)[:1000]}")

            if not self._construct_echo_info(message_data, target_index):
                # 兼容不使用echo回报的框架，不清楚有没有
                message_data_as_recv = await self._construct_data_as_msg(message_data)
                await self.database_manager.save_message(
                    message_data_as_recv, "SEND", self.connection_id
                )

            # 消息后处理
            processed_message = await self._postprocess_message(message_data, str(self.self_id))

            if processed_message:
                # 发送到客户端
                processed_json = json.dumps(processed_message, ensure_ascii=False)
                await self.client_ws.send(processed_json)

        except json.JSONDecodeError:
            self.logger.ws.warning(f"[{self.connection_id}] 目标 {target_index} 发送非JSON消息: {message[:1000]}")
        except websockets.exceptions.ConnectionClosed:
            raise
        except Exception as e:
            self.logger.ws.error(f"[{self.connection_id}] 处理目标消息 {message} 失败: {e}")

    async def _preprocess_message(self, message_data: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Event]]:
        """消息预处理"""
        return await self.message_processor.preprocess_client_message(message_data)

    async def _postprocess_message(self, message_data: Dict[str, Any], self_id: str) -> Optional[Dict[str, Any]]:
        """消息后处理"""
        return await self.message_processor.postprocess_target_message(message_data, self_id)

    async def send_reboot_message(self):
        api_resp = await construct_reboot_message(str(self.self_id))
        if api_resp:
            await self._process_target_message(api_resp, 0)

    def _construct_echo_info(self, message_data, target_index) -> str | None:
        echo = str(message_data.get("echo"))
        if not echo:
            return None

        # 将 target_index 加入键值，防止不同连接的 echo 混淆
        echo_key = f"{target_index}_{echo}"

        echo_info = {
            "data": message_data,
            "create_timestamp": int(datetime.now().timestamp()),
            "target_index": target_index,
            "original_echo": echo
        }

        if echo_key in self.echo_cache:
            self.logger.ws.warning(f"[{self.connection_id}] echo {echo_key} 已存在，将被覆盖")
        self.echo_cache[echo_key] = echo_info
        self.logger.ws.debug(f"[{self.connection_id}] 收到echo {echo_key}，缓存大小 {len(self.echo_cache)}")

        # 当缓存首次达到100个的时候。该函数阻塞。如网络正常不应该有这么多cache。
        if len(self.echo_cache) % 100 == 0:
            self.logger.ws.warning(f"[{self.connection_id}] echo 缓存达到 {len(self.echo_cache)} 个，强制清理过期的echo!")
            now_ts = int(datetime.now().timestamp())
            old_keys = [k for k, v in self.echo_cache.items() if now_ts - v.get("create_timestamp", 0) > 120]
            for k in old_keys:
                del self.echo_cache[k]

        return echo

    @staticmethod
    def _check_api_call_succ(event: Event):
        if isinstance(event, ApiResponse):
            return event.status == "ok" and event.retcode == 0
        return False

    def _log_api_call_fail(self, event: Event):
        if isinstance(event, ApiResponse):
            if event.status != "ok" or event.retcode != 0:
                # echo 现在包含 target_index 前缀，需要查找匹配的键
                echo_val = str(event.echo)
                echo_info = None
                for idx in range(1, len(self.target_connections) + 1):
                    echo_key = f"{idx}_{echo_val}"
                    if echo_key in self.echo_cache:
                        echo_info = self.echo_cache.get(echo_key, None)
                        break

                if echo_info:
                    # 截断过长的数据（如base64）避免日志爆炸
                    data_str = str(echo_info['data'])
                    if len(data_str) > 200:
                        data_str = data_str[:200] + f"...[total length: {len(data_str)}]"
                    self.logger.ws.warning("[{}] API调用失败: {} -> {}".format(self.connection_id, data_str, event))

    async def _construct_msg_from_echo(self, echo, **kwargs):
        """从api结果中构造模拟收到消息"""
        # echo 现在包含 target_index 前缀，需要查找匹配的键
        echo_val = str(echo)
        echo_info = None
        for idx in range(1, len(self.target_connections) + 1):
            echo_key = f"{idx}_{echo_val}"
            if echo_key in self.echo_cache:
                echo_info = self.echo_cache.get(echo_key, None)
                break

        if echo_info:
            return await self._construct_data_as_msg(echo_info["data"], **kwargs)
        return {}

    async def _construct_data_as_msg(self, message_data, **kwargs):
        """将发送api请求转换为消息事件"""

        if 'send' not in message_data.get('action'):
            return {}
        params = message_data.get("params", {})
        params.update({"self_id": self.self_id})
        if "sender" not in params:
            params.update({"sender": {"user_id": self.self_id, "nickname": "BS Bot Send"}})
        # 这个 message_sent 不是 napcat 那种修改的 Onebot，而是本框架数据库中的标识
        params.update({"post_type": "message_sent"})
        raw_message = MessageSegmentParser.message2raw_message(params.get("message", []))
        params.update({"raw_message": raw_message})

        params.update(kwargs)
        return params

    @staticmethod
    def target_index2list_index(target_index):
        return target_index - 1

    @staticmethod
    def list_index2target_index(list_index):
        return list_index + 1

    # ─────────────────────────────────────────────────────────
    #  QQ 登录状态后台监控 (P1 + P2)
    # ─────────────────────────────────────────────────────────

    async def _login_monitor_loop(self):
        """后台周期性检测 QQ 登录状态，状态翻转时推送合成事件。"""
        await asyncio.sleep(15)  # 初始延迟，等 NapCat 稳定
        interval = 60
        while self.running and self.client_alive:
            try:
                logged_in = await self._check_qq_login()
                old = self._qq_logged_in
                self._qq_logged_in = logged_in

                if old is not None and old != logged_in:
                    if not logged_in:
                        self.logger.ws.warning(
                            f"[{self.connection_id}] QQ 登录状态变化: 在线 → 离线"
                        )
                        await self._send_synthetic_bot_offline()
                    else:
                        self.logger.ws.info(
                            f"[{self.connection_id}] QQ 登录状态变化: 离线 → 在线"
                        )
                        await self._send_synthetic_lifecycle("connect")
            except asyncio.CancelledError:
                return
            except Exception as e:
                self.logger.ws.warning(
                    f"[{self.connection_id}] 登录状态监控异常: {e}"
                )
            await asyncio.sleep(interval)

    async def _check_qq_login(self) -> bool:
        """向 NapCat 发送 get_login_info，返回 QQ 是否登录。"""
        if not self.client_ws or not self.client_alive:
            return False

        echo = f"_bs_login_chk_{self.connection_id}_{int(_time.time())}"
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_login_checks[echo] = future

        try:
            req = json.dumps({
                "action": "get_login_info",
                "params": {},
                "echo": echo,
            }, ensure_ascii=False)
            await self.client_ws.send(req)

            response = await asyncio.wait_for(future, timeout=5.0)
            data = response.get("data", {})
            user_id = data.get("user_id", data.get("uin", ""))
            return bool(user_id) and str(user_id) != "0"
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            return False
        except Exception:
            return False
        finally:
            self._pending_login_checks.pop(echo, None)

    async def _send_synthetic_bot_offline(self):
        """向保活的 manager target 推送合成 bot_offline 通知。"""
        if not self.target_keepalive_enabled:
            return
        event = {
            "time": int(_time.time()),
            "self_id": self.self_id or 0,
            "post_type": "notice",
            "notice_type": "bot_offline",
            "tag": "qq_logout_detected",
            "message": "BS login monitor detected QQ logout",
        }
        event_json = json.dumps(event, ensure_ascii=False)
        for idx in self.manager_target_indexes:
            list_idx = self.target_index2list_index(idx)
            if list_idx < len(self.target_connections) and self.target_connections[list_idx]:
                try:
                    await self.target_connections[list_idx].send(event_json)
                    self.logger.ws.info(
                        f"[{self.connection_id}] 已向 target {idx} 推送合成 bot_offline"
                    )
                except Exception as e:
                    self.logger.ws.warning(
                        f"[{self.connection_id}] 推送合成 bot_offline 到 target {idx} 失败: {e}"
                    )

    # ─────────────────────────────────────────────────────────
    #  合成 lifecycle 事件
    # ─────────────────────────────────────────────────────────

    def _build_synthetic_lifecycle(self, sub_type: str) -> str:
        """构造合成 lifecycle 事件 JSON。

        当 NapCat client 断连/重连时，通过保活的 target WS 将此事件
        注入下游（如 Manager），使其能实时感知上游在线状态变化。
        """
        import time
        event = {
            "time": int(time.time()),
            "self_id": self.self_id or 0,
            "post_type": "meta_event",
            "meta_event_type": "lifecycle",
            "sub_type": sub_type,
        }
        return json.dumps(event, ensure_ascii=False)

    async def _send_synthetic_lifecycle(self, sub_type: str):
        """向所有保活中的 manager target 发送合成 lifecycle 事件。"""
        if not self.target_keepalive_enabled:
            return
        event_json = self._build_synthetic_lifecycle(sub_type)
        for idx in self.manager_target_indexes:
            list_idx = self.target_index2list_index(idx)
            if list_idx < len(self.target_connections) and self.target_connections[list_idx]:
                try:
                    await self.target_connections[list_idx].send(event_json)
                    self.logger.ws.info(
                        f"[{self.connection_id}] 已向 target {idx} 发送合成 lifecycle.{sub_type}"
                    )
                except Exception as e:
                    self.logger.ws.warning(
                        f"[{self.connection_id}] 发送合成 lifecycle.{sub_type} 到 target {idx} 失败: {e}"
                    )

    async def on_client_disconnect(self):
        """NapCat client 断线处理（由 start_proxy finally 调用）。

        保活模式下：只关闭 client_ws，target 连接由后台重连任务独立维护。
        非保活模式：等同于调用 stop()，client 和 target 全部关闭。
        """
        self.client_alive = False
        self._qq_logged_in = None  # 重置缓存

        # ★ 停止登录监控任务
        if self._login_monitor_task and not self._login_monitor_task.done():
            self._login_monitor_task.cancel()
            self._login_monitor_task = None

        # ★ 向保活的 manager target 推送合成 disconnect 事件
        await self._send_synthetic_lifecycle("disconnect")

        # 安全关闭 client 侧 ws
        if self.client_ws:
            await self._close_websocket(self.client_ws)
            self.client_ws = None

        if not self.target_keepalive_enabled:
            # 无保活需求：一并停止所有 target
            await self.stop()
        else:
            self.logger.ws.info(
                f"[{self.connection_id}] client 已断开，target 保活模式激活"
                f"（manager_targets={self.manager_target_indexes}）"
            )

    async def stop(self):
        """完全停止代理连接（关闭 client + 所有 target）。

        由外部（proxy_server.restart/stop）显式调用，或非保活模式的 on_client_disconnect 调用。
        """
        self.running = False
        self.client_alive = False

        # 关闭目标连接
        close_tasks = []
        for target_ws in self.target_connections:
            close_tasks.append(asyncio.create_task(self._close_websocket(target_ws)))

        # 关闭客户端连接（若仍存在）
        if self.client_ws:
            close_tasks.append(asyncio.create_task(self._close_websocket(self.client_ws)))
            self.client_ws = None

        if close_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*close_tasks, return_exceptions=True),
                    timeout=3.0
                )
            except asyncio.TimeoutError:
                self.logger.ws.warning(f"[{self.connection_id}] 关闭连接超时")
            except Exception as e:
                self.logger.ws.error(f"[{self.connection_id}] 关闭连接时出错: {e}")

        self.target_connections.clear()

    async def _close_websocket(self, ws):
        """安全关闭WebSocket连接"""
        try:
            if ws:
                await ws.close()
        except Exception as e:
            self.logger.ws.error(f"[{self.connection_id}] 关闭WebSocket连接时出错: {e}")
