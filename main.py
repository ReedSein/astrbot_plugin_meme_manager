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
    "meme_manager", "anka", "anka - 表情包管理器 (Pro混排版)", "3.24"
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
        self.max_emotions_per_message = self.config.get("max_emotions_per_message", 5) # 混排模式建议调大此限制
        self.emotions_probability = self.config.get("emotions_probability", 80)
        self.content_cleanup_rule = self.config.get("content_cleanup_rule", "&&[a-zA-Z]*&&")

        # --- 性能优化：预编译正则表达式 (保留 v20 特性) ---
        self.regex_hex = re.compile(r"&&([^&&]+)&&")
        self.regex_bracket = re.compile(r"\[([^\[\]]+)\]")
        self.regex_paren = re.compile(r"\(([^()]+)\)")
        
        # --- 性能优化：IO 缓存层 (保留 v20 特性) ---
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
                # 兼容 v18 的 provider 字段
                stardots_config["provider"] = "stardots"
                self.img_sync = ImageSync(
                    config={
                        "key": stardots_config["key"],
                        "secret": stardots_config["secret"],
                        "space": stardots_config.get("space", "memes"),
                        "provider": "stardots",
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
                r2_config["provider"] = "cloudflare_r2"
                self.img_sync = ImageSync(
                    config=r2_config, local_dir=MEMES_DIR, provider_type="cloudflare_r2"
                )
                self._r2_bucket_name = r2_config.get("bucket_name")

    def _refresh_image_cache(self):
        """性能优化：刷新图片文件索引缓存 (保留 v20 特性)"""
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
        """体验优化：使用洗牌算法获取下一张图片，防止重复 (保留 v20 特性)"""
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

    # ==================== WebUI 管理命令 (v20) ====================
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

    # ==================== 管理命令扩展 (移植自 v18) ====================
    @meme_manager.command("同步状态")
    async def check_sync_status(self, event: AstrMessageEvent, detail: str = None):
        """[v18移植] 检查表情包与图床的同步状态，支持详细模式"""
        if not self.img_sync:
            yield event.plain_result("图床服务尚未配置，请先配置。")
            return

        try:
            # 获取图床信息
            provider_name = self.img_sync.provider.__class__.__name__
            if hasattr(self.img_sync.provider, "bucket_name"):
                storage_info = f"存储桶: {self.img_sync.provider.bucket_name}"
            else:
                storage_info = "未知存储类型"

            status = self.img_sync.check_status()
            to_upload = status.get("to_upload", [])
            to_download = status.get("to_download", [])

            result = [
                "📊 图床同步状态报告",
                "",
                f"🔧 服务: {provider_name}",
                f"📁 {storage_info}",
                "",
                "📈 统计:",
                f"  • 待上传: {len(to_upload)}",
                f"  • 待下载: {len(to_download)}",
                ""
            ]

            # 简略展示
            if to_upload:
                result.append("📤 待上传(前5):")
                for file in to_upload[:5]:
                    result.append(f"  • {file.get('category', '未分类')}/{file['filename']}")
                if len(to_upload) > 5: result.append("  ...")

            if to_download:
                result.append("\n📥 待下载(前5):")
                for file in to_download[:5]:
                    result.append(f"  • {file.get('category', '未分类')}/{file['filename']}")
                if len(to_download) > 5: result.append("  ...")

            if not to_upload and not to_download:
                result.append("✅ 云端与本地已完全同步！")

                # 详细模式逻辑
                if detail and detail.strip() == "详细":
                    result.append("\n📋 详细分类统计:")
                    try:
                        # 云端统计
                        if hasattr(self.img_sync.provider, "get_image_list"):
                            remote_images = self.img_sync.provider.get_image_list()
                            remote_stats = {}
                            for img in remote_images:
                                cat = img.get("category", "未分类")
                                remote_stats[cat] = remote_stats.get(cat, 0) + 1
                            
                            result.append("\n☁️ 云端分布:")
                            for cat, count in sorted(remote_stats.items(), key=lambda x: x[1], reverse=True):
                                result.append(f"  • {cat}: {count}")
                    except Exception as e:
                        result.append(f"  (获取云端详情失败: {e})")

                    # 本地统计
                    local_stats = {k: len(v) for k, v in self.image_cache.items()}
                    result.append("\n💻 本地分布:")
                    for cat, count in sorted(local_stats.items(), key=lambda x: x[1], reverse=True):
                        result.append(f"  • {cat}: {count}")

            yield event.plain_result("\n".join(result))
        except Exception as e:
            yield event.plain_result(f"检查失败: {e}")

    @meme_manager.command("图库统计")
    async def show_library_stats(self, event: AstrMessageEvent):
        """[v18移植] 显示图库详细统计信息"""
        try:
            result = ["📊 图库统计报告", "", "📁 本地:"]
            
            # 使用 v20 的缓存数据进行统计
            local_stats = {k: len(v) for k, v in self.image_cache.items()}
            local_total = sum(local_stats.values())

            if local_stats:
                result.append(f"  • 总文件: {local_total}")
                result.append(f"  • 分类数: {len(local_stats)}")
                result.append("\n📂 分类详情:")
                for cat, count in sorted(local_stats.items(), key=lambda x: x[1], reverse=True):
                    result.append(f"  • {cat}: {count}")
            else:
                result.append("  • (空)")

            # 云端统计
            if self.img_sync:
                result.append("\n☁️ 云端:")
                try:
                    remote_images = self.img_sync.provider.get_image_list()
                    remote_total = len(remote_images)
                    result.append(f"  • 总文件: {remote_total}")
                    
                    if local_total > remote_total:
                        result.append(f"  • 📉 本地比云端多 {local_total - remote_total}")
                    elif remote_total > local_total:
                        result.append(f"  • 📈 云端比本地多 {remote_total - local_total}")
                    else:
                        result.append("  • ✅ 数量一致")
                except Exception as e:
                    result.append(f"  (获取失败: {e})")
            
            # 空间估算
            if local_total > 0:
                size_mb = local_total * 0.5 # 假设平均500KB
                result.append(f"\n💾 预估占用空间: ~{size_mb:.1f} MB")

            yield event.plain_result("\n".join(result))

        except Exception as e:
            self.logger.error(f"统计失败: {e}")
            yield event.plain_result(f"统计失败: {e}")

    # ==================== 核心解析与分段算法 ====================

    def _split_text_by_tags(self, text: str, valid_emoticons: set) -> tuple[list, list]:
        """
        [v21核心] 根据标签将文本精准切分为 [Plain, Slot, Plain...]
        支持去除 LLM 对定界符的转义 (如 &\&tag&& -> &&tag&&) 以及标签内容的转义
        """
        if not text: return [], []

        # [关键修复] 预处理归一化
        # 1. 将 \& 替换为 &。这样 &\& 会变成 &&，\&\& 也会变成 &&
        # 2. 将 \[ 和 \] 替换为 [ 和 ]，防止中括号也被转义
        text = text.replace(r"\&", "&").replace(r"\[", "[").replace(r"\]", "]")

        # 模式解释：捕获 &&...&& 或 [xxx] 或 (xxx) 作为分隔符
        pattern = r"(&&[^&]+&&|\[[^\[\]]+\]|\([^()]+\))"
        parts = re.split(pattern, text)
        
        components = []
        found_emotions_in_order = []

        for part in parts:
            if not part: continue

            is_tag = False
            emotion = ""
            
            # --- 解析 Tag 内容 ---
            if part.startswith("&&") and part.endswith("&&"):
                # [二次清洗] 去除标签内容内部可能的转义符（防止 tag\_name 这种情况）
                emotion = part[2:-2].strip().replace("\\", "") 
                is_tag = True
            elif part.startswith("[") and part.endswith("]"):
                emotion = part[1:-1].strip()
                is_tag = True
            elif part.startswith("(") and part.endswith(")"):
                emotion = part[1:-1].strip()
                is_tag = True

            # --- 匹配逻辑 ---
            if is_tag and emotion in valid_emoticons:
                # 匹配成功 -> 转换为图片插槽
                found_emotions_in_order.append(emotion)
                components.append({"type": "image_slot", "emotion": emotion})
            else:
                # 匹配失败（如无效标签）-> 丢弃 &&...&& 格式的幻觉，保留其他文本
                if part.startswith("&&") and part.endswith("&&"):
                    self.logger.debug(f"丢弃无效标签: {part} (清洗后: {emotion})")
                    continue 
                
                components.append(Plain(part))
        
        return components, found_emotions_in_order

    # 优先级设为 3 ( < Retry插件的 5 )
    @filter.on_llm_response(priority=3)
    async def resp(self, event: AstrMessageEvent, response: LLMResponse):
        """
        [v21修改] LLM 响应处理
        注意：为了支持 on_decorating_result 的精准分段，这里只做检测记录，
        **不再** 对文本进行 strip 清理。保留标签给后续步骤使用。
        """
        if not response or not response.completion_text: return

        # 我们这里依然调用 parse 逻辑来更新 state_data (用于调试或其他插件消费)
        # 但我们不再回写 response.completion_text (或者只做最基础的清理)
        
        # 使用临时变量解析，不影响原始文本
        valid_emoticons = set(self.category_mapping.keys())
        _, emotions = self._split_text_by_tags(response.completion_text, valid_emoticons)
        
        if not hasattr(event, "state_data"): event.state_data = {}
        event.state_data["found_emotions"] = emotions
        
        # [CRITICAL] 不要在这里移除 &&tag&&，否则 decorating 阶段无法定位
        # response.completion_text = clean_text  <-- 注释掉这行

    # 优先级设为 10 ( > 默认值 0 )
    @filter.on_decorating_result(priority=10)
    async def on_decorating_result(self, event: AstrMessageEvent):
        """
        [v21核心] 消息组装
        使用 _split_text_by_tags 将文本标签替换为图片组件
        """
        result = event.get_result()
        if not result: return

        # 1. 获取 LLM 原始文本 (包含 &&tags&&)
        raw_text = result.get_plain_text()
        if not raw_text: return

        valid_emoticons = set(self.category_manager.get_descriptions().keys())
        
        # 2. 调用精准切割逻辑
        # mixed_components 包含 Plain 对象和 {"type": "image_slot"} 字典
        mixed_components, emotions = self._split_text_by_tags(raw_text, valid_emoticons)

        # 更新 state_data 供日志或后续使用
        if not hasattr(event, "state_data"): event.state_data = {}
        event.state_data["found_emotions"] = emotions

        if not emotions:
            # 如果没发现表情，就不做任何修改，直接返回
            return

        # 3. 实例化图片并替换插槽
        final_chain = []
        for comp in mixed_components:
            if isinstance(comp, Plain):
                final_chain.append(comp)
            elif isinstance(comp, dict) and comp.get("type") == "image_slot":
                # 这是一个插槽，尝试获取图片
                emotion = comp["emotion"]
                
                # 概率控制
                if random.randint(1, 100) <= self.emotions_probability:
                    # 使用 v20 的缓存+洗牌算法获取图片
                    meme_file = self._get_next_meme(emotion)
                    if meme_file:
                        meme_path = os.path.join(MEMES_DIR, emotion, meme_file)
                        try:
                            final_chain.append(Image.fromFileSystem(meme_path))
                        except Exception as e:
                            self.logger.error(f"图片加载失败 {meme_path}: {e}")
                
                # 如果没随机中，或者图片加载失败，这个 slot 就消失了
                # 对应的 &&tag&& 也就被移除，实现了"隐形"

        # 4. 更新消息链
        if final_chain:
            result.chain = final_chain

    @filter.after_message_sent()
    async def after_message_sent(self, event: AstrMessageEvent):
        """
        [v21修改] 已在 decorating 阶段处理混排，此处无需操作。
        """
        pass

    # ==================== 同步功能 (v20) ====================
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
