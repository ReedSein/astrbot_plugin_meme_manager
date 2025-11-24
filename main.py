import re
import os
import io
import random
import logging
import time
import aiohttp
import ssl
import copy
from PIL import Image as PILImage
import asyncio
from multiprocessing import Process
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api.provider import LLMResponse
from astrbot.api.message_components import *
from astrbot.api.event.filter import EventMessageType
from astrbot.api.event import ResultContentType
from astrbot.core.message.components import Plain
from astrbot.api.all import *
from astrbot.core.message.message_event_result import MessageChain
from .webui import run_server, ServerState
from .utils import get_public_ip, generate_secret_key, dict_to_string, load_json
from .image_host.img_sync import ImageSync
from .config import MEMES_DIR, MEMES_DATA_PATH, DEFAULT_CATEGORY_DESCRIPTIONS
from .backend.category_manager import CategoryManager
from .init import init_plugin


@register(
    "meme_manager", "anka", "anka - 表情包管理器 - 支持表情包发送及表情包上传", "3.23"
)
class MemeSender(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.config = config or {}

        # 初始化插件环境
        if not init_plugin():
            raise RuntimeError("插件初始化失败")

        # 初始化核心组件
        self.category_manager = CategoryManager()
        
        # 初始化图床组件
        self.img_sync = None
        self._init_image_host()

        # 初始化 WebUI 进程管理
        self.webui_process = None
        self.server_key = None
        self.server_port = self.config.get("webui_port", 5000)

        # 初始化上传会话状态
        self.upload_states = {}  # {user_session: {"category": str, "expire_time": float}}

        # 初始化日志
        self.logger = logging.getLogger(__name__)

        # 记录 R2 初始化日志（如果已初始化）
        if hasattr(self, "_r2_bucket_name"):
            self.logger.info(f"Cloudflare R2 图床已初始化: {self._r2_bucket_name}")
            delattr(self, "_r2_bucket_name")

        # 加载 Prompt 配置
        self.prompt_head = self.config.get("prompt", {}).get("prompt_head", "")
        self.prompt_tail_1 = self.config.get("prompt", {}).get("prompt_tail_1", "")
        self.prompt_tail_2 = self.config.get("prompt", {}).get("prompt_tail_2", "")
        self.max_emotions_per_message = self.config.get("max_emotions_per_message", 1)
        self.emotions_probability = self.config.get("emotions_probability", 80)
        self.content_cleanup_rule = self.config.get("content_cleanup_rule", "&&[a-zA-Z]*&&")

        # --- 性能优化：预编译正则表达式 ---
        self.regex_hex = re.compile(r"&&([^&&]+)&&")
        self.regex_bracket = re.compile(r"\[([^\[\]]+)\]")
        self.regex_paren = re.compile(r"\(([^()]+)\)")
        
        # --- 性能优化：IO 缓存层 ---
        self.image_cache = {}  
        self.meme_queues = {}  
        self._refresh_image_cache()

        # 读取容错符
        self.fault_tolerant_symbols = self.config.get("fault_tolerant_symbols", ["⬡"])

        # 处理人格注入
        personas = self.context.provider_manager.personas
        self.persona_backup = copy.deepcopy(personas)
        self._reload_personas()

    def _init_image_host(self):
        """初始化图床配置"""
        image_host_type = self.config.get("image_host", "stardots")

        if image_host_type == "stardots":
            stardots_config = self.config.get("image_host_config", {}).get("stardots", {})
            if stardots_config.get("key") and stardots_config.get("secret"):
                self.img_sync = ImageSync(
                    config={
                        "key": stardots_config["key"],
                        "secret": stardots_config["secret"],
                        "space": stardots_config.get("space", "memes"),
                    },
                    local_dir=MEMES_DIR,
                    provider_type="stardots",
                )
        elif image_host_type == "cloudflare_r2":
            r2_config = self.config.get("image_host_config", {}).get("cloudflare_r2", {})
            required_fields = ["account_id", "access_key_id", "secret_access_key", "bucket_name"]
            if all(r2_config.get(field) for field in required_fields):
                if r2_config.get("public_url"):
                    r2_config["public_url"] = r2_config["public_url"].rstrip("/")
                self.img_sync = ImageSync(
                    config=r2_config, local_dir=MEMES_DIR, provider_type="cloudflare_r2"
                )
                self._r2_bucket_name = r2_config.get("bucket_name")

    def _refresh_image_cache(self):
        """性能优化：刷新图片文件索引缓存"""
        new_cache = {}
        self.meme_queues = {}
        
        if not os.path.exists(MEMES_DIR):
            self.logger.warning(f"表情包根目录不存在: {MEMES_DIR}")
            return

        for emotion_name in self.category_manager.get_descriptions().keys():
            emotion_path = os.path.join(MEMES_DIR, emotion_name)
            if os.path.exists(emotion_path):
                files = [
                    f for f in os.listdir(emotion_path) 
                    if f.lower().endswith(('.jpg', '.png', '.gif', '.jpeg', '.webp'))
                ]
                if files:
                    new_cache[emotion_name] = files
        
        self.image_cache = new_cache
        self.logger.info(f"表情包缓存已更新，共加载 {len(new_cache)} 个分类的图片索引")

    def _get_next_meme(self, category: str) -> str | None:
        """体验优化：使用洗牌算法获取下一张图片，防止重复"""
        if category not in self.image_cache:
            return None
            
        if not self.meme_queues.get(category):
            original_list = self.image_cache.get(category, [])
            if not original_list:
                return None
            shuffled = original_list.copy()
            random.shuffle(shuffled)
            self.meme_queues[category] = shuffled
            
        return self.meme_queues[category].pop()

    def _reload_personas(self):
        """重新注入人格"""
        self.category_mapping = load_json(MEMES_DATA_PATH, DEFAULT_CATEGORY_DESCRIPTIONS)
        self._refresh_image_cache()
        
        self.category_mapping_string = dict_to_string(self.category_mapping)
        self.sys_prompt_add = (
            self.prompt_head
            + self.category_mapping_string
            + self.prompt_tail_1
            + str(self.max_emotions_per_message)
            + self.prompt_tail_2
        )
        personas = self.context.provider_manager.personas
        for persona, persona_backup in zip(personas, self.persona_backup):
            persona["prompt"] = persona_backup["prompt"] + self.sys_prompt_add

    # ==================== WebUI 管理命令 ====================
    @filter.command_group("表情管理")
    def meme_manager(self):
        pass

    @filter.permission_type(filter.PermissionType.ADMIN)
    @meme_manager.command("开启管理后台")
    async def start_webui(self, event: AstrMessageEvent):
        yield event.plain_result("🚀 正在启动管理后台，请稍等片刻～")

        try:
            state = ServerState()
            state.ready.clear()
            self.server_key = generate_secret_key(8)
            self.server_port = self.config.get("webui_port", 5000)

            if await self._check_port_active():
                yield event.plain_result("🔧 检测到端口占用，正在尝试自动释放...")
                await self._shutdown()
                await asyncio.sleep(1)

            config_for_server = {
                "img_sync": self.img_sync,
                "category_manager": self.category_manager,
                "webui_port": self.server_port,
                "server_key": self.server_key,
            }
            self.webui_process = Process(target=run_server, args=(config_for_server,))
            self.webui_process.start()

            for i in range(10):
                if await self._check_port_active():
                    break
                await asyncio.sleep(1)
            else:
                raise RuntimeError("⌛ 启动超时，请检查防火墙设置")

            public_ip = await get_public_ip()
            yield event.plain_result(
                f"✨ 管理后台已就绪！\n"
                f"━━━━━━━━━━━━━━\n"
                f"🔑 临时密钥：{self.server_key}\n"
                f"🔗 访问地址：http://{public_ip}:{self.server_port}"
            )
        except Exception as e:
            self.logger.error(f"启动失败: {str(e)}")
            yield event.plain_result(f"⚠️ 后台启动失败: {str(e)}")
            await self._cleanup_resources()

    async def _check_port_active(self):
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", self.server_port), timeout=1
            )
            writer.close()
            return True
        except:
            return False

    @filter.permission_type(filter.PermissionType.ADMIN)
    @meme_manager.command("关闭管理后台")
    async def stop_server(self, event: AstrMessageEvent):
        yield event.plain_result("🚪 管理后台正在关闭...")
        try:
            await self._shutdown()
            yield event.plain_result("✅ 服务器已关闭")
        except Exception as e:
            yield event.plain_result(f"❌ 关闭失败: {str(e)}")
        finally:
            await self._cleanup_resources()

    async def _shutdown(self):
        if self.webui_process:
            self.webui_process.terminate()
            self.webui_process.join()

    async def _cleanup_resources(self):
        if self.img_sync:
            self.img_sync.stop_sync()
        self.server_key = None
        self.server_port = None
        if self.webui_process:
            if self.webui_process.is_alive():
                self.webui_process.terminate()
                self.webui_process.join()
        self.webui_process = None

    # ==================== 表情包操作命令 ====================
    @meme_manager.command("查看图库")
    async def list_emotions(self, event: AstrMessageEvent):
        descriptions = self.category_manager.get_descriptions()
        categories = "\n".join([f"- {tag}: {desc}" for tag, desc in descriptions.items()])
        yield event.plain_result(f"🖼️ 当前图库：\n{categories}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @meme_manager.command("添加表情")
    async def upload_meme(self, event: AstrMessageEvent, category: str = None):
        if not category:
            yield event.plain_result("📌 请输入: /表情管理 添加表情 [类别名称]")
            return

        if category not in self.category_manager.get_descriptions():
            yield event.plain_result(f"无效的类别「{category}」。")
            return

        user_key = f"{event.session_id}_{event.get_sender_id()}"
        self.upload_states[user_key] = {
            "category": category,
            "expire_time": time.time() + 30,
        }
        yield event.plain_result(f"请在30秒内发送要添加到【{category}】类别的图片。")

    @filter.event_message_type(EventMessageType.ALL)
    async def handle_upload_image(self, event: AstrMessageEvent):
        user_key = f"{event.session_id}_{event.get_sender_id()}"
        upload_state = self.upload_states.get(user_key)

        if not upload_state or time.time() > upload_state["expire_time"]:
            if user_key in self.upload_states: del self.upload_states[user_key]
            return

        images = [c for c in event.message_obj.message if isinstance(c, Image)]
        if not images:
            yield event.plain_result("请发送图片文件。")
            return

        category = upload_state["category"]
        save_dir = os.path.join(MEMES_DIR, category)
        os.makedirs(save_dir, exist_ok=True)
        
        saved_files = []
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        try:
            for idx, img in enumerate(images, 1):
                timestamp = int(time.time())
                try:
                    target_url = img.url
                    if "multimedia.nt.qq.com.cn" in target_url:
                        target_url = target_url.replace("https://", "http://", 1)

                    async with aiohttp.ClientSession(
                        connector=aiohttp.TCPConnector(ssl=ssl_context)
                    ) as session:
                        async with session.get(target_url) as resp:
                            content = await resp.read()

                    file_type = "unknown"
                    try:
                        with PILImage.open(io.BytesIO(content)) as pi:
                            file_type = pi.format.lower()
                    except Exception: pass

                    ext = {
                        "jpeg": ".jpg", "png": ".png", 
                        "gif": ".gif", "webp": ".webp"
                    }.get(file_type, ".bin")
                    
                    filename = f"{timestamp}_{idx}{ext}"
                    save_path = os.path.join(save_dir, filename)

                    with open(save_path, "wb") as f:
                        f.write(content)
                    saved_files.append(filename)

                except Exception as e:
                    self.logger.error(f"下载失败: {e}")
                    yield event.plain_result(f"文件下载失败: {e}")

            del self.upload_states[user_key]
            self._refresh_image_cache()

            msg = [Plain(f"✅ 已收录 {len(saved_files)} 张新表情到「{category}」！")]
            if self.img_sync:
                msg.append(Plain("\n☁️ 如需同步到云端请使用：同步到云端"))

            yield event.chain_result(msg)
            await self.reload_emotions()

        except Exception as e:
            yield event.plain_result(f"保存失败: {e}")

    async def reload_emotions(self):
        try:
            self.category_manager.sync_with_filesystem()
            self._refresh_image_cache()
        except Exception as e:
            self.logger.error(f"Reload失败: {e}")

    # ==================== 核心解析逻辑 ====================
    def _process_text_for_emotions(self, text: str) -> tuple[str, list[str]]:
        if not text: return text, []

        found_emotions = []
        valid_emoticons = set(self.category_mapping.keys())
        clean_text = text

        # 1. 严格匹配 &&emotion&&
        matches = list(self.regex_hex.finditer(clean_text))
        temp_replacements = []
        for match in matches:
            original = match.group(0)
            emotion = match.group(1).strip()
            if emotion in valid_emoticons:
                temp_replacements.append((original, emotion))
            else:
                temp_replacements.append((original, ""))

        for original, emotion in temp_replacements:
            clean_text = clean_text.replace(original, "", 1)
            if emotion: found_emotions.append(emotion)

        # 2. 标记处理
        if self.config.get("enable_alternative_markup", True):
            # [emotion]
            matches = self.regex_bracket.finditer(clean_text)
            bracket_replacements = []
            invalid_brackets = []
            for match in matches:
                original = match.group(0)
                emotion = match.group(1).strip()
                if emotion in valid_emoticons:
                    bracket_replacements.append((original, emotion))
                else:
                    invalid_brackets.append(original)
            for invalid in invalid_brackets: clean_text = clean_text.replace(invalid, "", 1)
            for original, emotion in bracket_replacements:
                clean_text = clean_text.replace(original, "", 1)
                found_emotions.append(emotion)

            # (emotion)
            matches = self.regex_paren.finditer(clean_text)
            paren_replacements = []
            invalid_parens = []
            for match in matches:
                original = match.group(0)
                emotion = match.group(1).strip()
                if emotion in valid_emoticons:
                    if self._is_likely_emotion_markup(original, clean_text, match.start()):
                        paren_replacements.append((original, emotion))
                else:
                    invalid_parens.append(original)
            for invalid in invalid_parens: clean_text = clean_text.replace(invalid, "", 1)
            for original, emotion in paren_replacements:
                clean_text = clean_text.replace(original, "", 1)
                found_emotions.append(emotion)

        # 3. 重复词
        if self.config.get("enable_repeated_emotion_detection", True):
            high_confidence = self.config.get("high_confidence_emotions", [])
            for emotion in valid_emoticons:
                if len(emotion) < 3: continue
                pattern = f"({re.escape(emotion)})\\1{{1,}}" if emotion in high_confidence else f"({re.escape(emotion)})\\1{{2,}}"
                if len(emotion) < 4 and emotion not in high_confidence: continue
                
                matches = list(re.finditer(pattern, clean_text))
                for match in matches:
                    original = match.group(0)
                    clean_text = clean_text.replace(original, "", 1)
                    found_emotions.append(emotion)

        # 4. 松散模式
        if self.config.get("enable_loose_emotion_matching", True):
            for emotion in valid_emoticons:
                pattern = r"\b(" + re.escape(emotion) + r")\b"
                for match in list(re.finditer(pattern, clean_text)):
                    word = match.group(1)
                    position = match.start()
                    if self._is_likely_emotion(word, clean_text, position, valid_emoticons):
                        found_emotions.append(word)
                        clean_text = clean_text[:position] + clean_text[position + len(word):]

        # 去重
        seen = set()
        filtered_emotions = []
        for emo in found_emotions:
            if emo not in seen:
                seen.add(emo)
                filtered_emotions.append(emo)
            if len(filtered_emotions) >= self.max_emotions_per_message: break

        clean_text = re.sub(r"&&+", "", clean_text).strip()
        return clean_text, filtered_emotions

    def _is_likely_emotion_markup(self, markup, text, position):
        before_text = text[:position].strip()
        after_text = text[position + len(markup) :].strip()
        if re.search(r"[\u4e00-\u9fff]", before_text[-1:] if before_text else "") or \
           re.search(r"[\u4e00-\u9fff]", after_text[:1] if after_text else ""): return True
        if re.match(r"\[\d+\]", markup) or " " in markup[1:-1]: return False
        if re.search(r"[a-zA-Z]\s+$", before_text) and re.search(r"^\s+[a-zA-Z]", after_text): return False
        return True

    def _is_likely_emotion(self, word, text, position, valid_emotions):
        before_text = text[:position].strip()
        after_text = text[position + len(word) :].strip()
        if re.search(r"[a-zA-Z]\s+$", before_text) or re.search(r"^\s+[a-zA-Z]", after_text): return False
        if re.search(r"[\u4e00-\u9fff]", before_text[-1:] if before_text else "") or \
           re.search(r"[\u4e00-\u9fff]", after_text[:1] if after_text else ""): return True
        if not before_text or before_text.endswith(("。", "，", "！", "？", ".", ",", ":", ";", "!", "?", "\n")): return True
        if word in self.config.get("high_confidence_emotions", []): return True
        return False

    # --- 关键架构修正：优先级调整 ---
    
    # 优先级设为 3 ( < Retry插件的 5 )
    # 确保 Retry 插件先执行重试逻辑，修正文本错误。
    # 然后 MemeSender 再执行，此时看到的已经是“修复后”的文本了。
    @filter.on_llm_response(priority=3)
    async def resp(self, event: AstrMessageEvent, response: LLMResponse):
        """处理 LLM 响应，识别表情"""
        if not response or not response.completion_text: return

        clean_text, emotions = self._process_text_for_emotions(response.completion_text)
        
        if not hasattr(event, "state_data"): event.state_data = {}
        event.state_data["found_emotions"] = emotions
        response.completion_text = clean_text

    # 优先级设为 10 ( > 默认值 0 )
    # 作为一个清理/格式化插件，它应该在大部分逻辑之后运行，但在最终发送之前。
    # 确保文本被彻底净化，没有残留标签。
    @filter.on_decorating_result(priority=10)
    async def on_decorating_result(self, event: AstrMessageEvent):
        """在消息发送前处理文本部分"""
        result = event.get_result()
        if not result: return

        state_data = getattr(event, "state_data", {})
        emotions = state_data.get("found_emotions", [])
        
        # 兜底扫描：即使优先级已经调整，作为防御性编程，依然保留对残留标签的扫描
        current_text = result.get_plain_text()
        if current_text and ("&&" in current_text or ("[" in current_text and "]" in current_text)):
             clean_text, new_emotions = self._process_text_for_emotions(current_text)
             # 如果发现了新表情或者清理了文本，说明有漏网之鱼
             if new_emotions or clean_text != current_text:
                 # 合并或覆盖？这里选择合并，因为 resp 可能已经识别了一部分
                 emotions.extend([e for e in new_emotions if e not in emotions])
                 if not hasattr(event, "state_data"): event.state_data = {}
                 event.state_data["found_emotions"] = emotions
                 
                 # 强制更新文本链
                 result.chain = [Plain(clean_text)]

        if not emotions: return

        try:
            chains = []
            original_chain = result.chain
            if original_chain:
                if isinstance(original_chain, str): chains.append(Plain(original_chain))
                elif isinstance(original_chain, MessageChain): chains.extend([c for c in original_chain if isinstance(c, Plain)])
                elif isinstance(original_chain, list): chains.extend([c for c in original_chain if isinstance(c, Plain)])

            cleaned_chains = []
            for component in chains:
                if isinstance(component, Plain):
                    text = component.text
                    if self.content_cleanup_rule: text = re.sub(self.content_cleanup_rule, "", text)
                    final_clean, _ = self._process_text_for_emotions(text)
                    if final_clean.strip(): cleaned_chains.append(Plain(final_clean))

            text_result = event.make_result().set_result_content_type(ResultContentType.LLM_RESULT)
            for component in cleaned_chains: text_result = text_result.message(component.text)

            if text_result.get_plain_text().strip():
                event.set_result(text_result)
            else:
                await self.after_message_sent(event)
                event.stop_event()
        except Exception as e:
            self.logger.error(f"处理文本失败: {e}")

    @filter.after_message_sent()
    async def after_message_sent(self, event: AstrMessageEvent):
        state_data = getattr(event, "state_data", {})
        emotions = state_data.get("found_emotions", [])

        if not emotions: return

        try:
            for emotion in emotions:
                if not emotion: continue
                meme_file = self._get_next_meme(emotion)
                if not meme_file: continue
                meme_path = os.path.join(MEMES_DIR, emotion, meme_file)
                if not os.path.exists(meme_path): continue

                if random.randint(0, 100) <= self.emotions_probability:
                    if event.get_platform_name() == "gewechat":
                        await event.send(MessageChain([Image.fromFileSystem(meme_path)]))
                    else:
                        await self.context.send_message(
                            event.unified_msg_origin,
                            MessageChain([Image.fromFileSystem(meme_path)]),
                        )
            state_data["found_emotions"] = []
        except Exception as e:
            self.logger.error(f"发送图片失败: {e}")

    @meme_manager.command("同步状态")
    async def check_sync_status(self, event: AstrMessageEvent):
        if not self.img_sync:
            yield event.plain_result("图床服务尚未配置。")
            return
        try:
            status = self.img_sync.check_status()
            to_upload = status.get("to_upload", [])
            to_download = status.get("to_download", [])
            result = ["同步状态检查结果："]
            if to_upload: result.append(f"\n需要上传({len(to_upload)}): " + ", ".join([f"{f['category']}/{f['filename']}" for f in to_upload[:3]]) + "...")
            if to_download: result.append(f"\n需要下载({len(to_download)}): " + ", ".join([f"{f['category']}/{f['filename']}" for f in to_download[:3]]) + "...")
            if not to_upload and not to_download: result.append("🌩️ 已完全同步！")
            yield event.plain_result("".join(result))
        except Exception as e:
            yield event.plain_result(f"检查失败: {e}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @meme_manager.command("同步到云端")
    async def sync_to_remote(self, event: AstrMessageEvent):
        if not self.img_sync:
            yield event.plain_result("图床未配置。")
            return
        try:
            yield event.plain_result("⚡ 开始云端同步...")
            if await self.img_sync.start_sync("upload"): yield event.plain_result("同步完成！")
            else: yield event.plain_result("同步失败，请看日志。")
        except Exception as e:
            self.logger.error(f"同步失败: {e}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @meme_manager.command("从云端同步")
    async def sync_from_remote(self, event: AstrMessageEvent):
        if not self.img_sync:
            yield event.plain_result("图床未配置。")
            return
        try:
            yield event.plain_result("⚡ 开始下载...")
            if await self.img_sync.start_sync("download"):
                yield event.plain_result("下载完成！")
                await self.reload_emotions()
            else: yield event.plain_result("下载失败，请看日志。")
        except Exception as e:
            self.logger.error(f"同步失败: {e}")

    async def terminate(self):
        personas = self.context.provider_manager.personas
        for persona, persona_backup in zip(personas, self.persona_backup):
            persona["prompt"] = persona_backup["prompt"]
        if self.img_sync: self.img_sync.stop_sync()
        await self._shutdown()
        await self._cleanup_resources()
