import os
import sys
from pathlib import Path

# 获取当前插件目录的绝对路径
PLUGIN_DIR = Path(__file__).parent.absolute()

# 定义表情包文件夹路径 - 避免使用相对路径如 ../..
# 直接使用绝对路径，或者相对于插件目录的路径
MEMES_DIR = Path(os.path.join(PLUGIN_DIR, "..", "..", "memes_data", "memes")).resolve()

# 确保目录存在
os.makedirs(MEMES_DIR, exist_ok=True)

# 添加日志输出帮助调试
print(f"插件目录: {PLUGIN_DIR}", file=sys.stderr)
print(f"表情包目录: {MEMES_DIR}", file=sys.stderr)

# 获取当前文件所在目录
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# 基础路径配置
BASE_DATA_DIR = os.path.join(CURRENT_DIR, "../../memes_data")
MEMES_DATA_PATH = os.path.join(BASE_DATA_DIR, "memes_data.json")  # 类别描述数据文件路径
TEMP_DIR = os.path.join(CURRENT_DIR, "../../temp")

# 默认的类别描述
DEFAULT_CATEGORY_DESCRIPTIONS = {
    "art": "发些不明觉厉的艺术图或故障风图片，用来展示逼格或打断无聊的对话。",
    "baka": "像看傻子一样看着对方，表达智商上的优越感和鄙视。",
    "bite": "表达“超级喜欢”或“占有欲”时使用，想把对方像猎物一样咬一口或吃掉。",
    "cheers": "优雅地举起酒杯或鼓掌，表示礼貌的庆祝、认可或“做得不错”。",
    "clown": "像看马戏团小丑一样无情嘲笑，用于回应群里的奇葩行为或烂梗。",
    "daze": "猫脑过载、双眼放空或正在发呆，表示“不想思考”或“听不懂人话”。",
    "deny": "冷酷且坚决地比叉或摇头，表示绝对的拒绝和否定。",
    "disgust": "像看到垃圾一样捏着鼻子或后退，表达强烈的生理性嫌弃和恶心。",
    "doubt": "歪头眯眼看着你，表示“你在说什么鬼话”或质疑对方的逻辑。",
    "feed": "敲着空碗要红包、要零食或要投喂，像流浪猫一样理直气壮地索取。",
    "gloom": "看着窗外的雨或缩在角落里emo，表达一种文艺范儿的低落和伤感。",
    "meow": "仅对主人使用的毫无保留的撒娇卖萌，软软地叫一声喵。",
    "morning": "刚睡醒伸懒腰或整理女仆装，用于早上的问候打卡。",
    "night": "蜷缩成一团睡觉或熄灯，表示结束一天的活动去休息。",
    "observe": "躲在暗处、墙角或透过玻璃偷看，暗中观察群友的一举一动。",
    "panic": "吓得炸毛、瞳孔地震或东西掉了，表示被突发状况吓到了。",
    "pleasure": "享受地抽烟、听歌或品酒，表现出一种高雅、惬意的个人状态。",
    "purr": "舒服得发出呼噜声或满地打滚，表示吃饱喝足或被哄得很开心。",
    "serve": "行女仆礼、端茶倒水或接受命令，展示作为女仆的职业素养。",
    "shy": "脸红、捂脸或不知所措，表示极其罕见的害羞或不好意思。",
    "silence": "无语到发省略号、黑屏或转身离开，单方面终结不想继续的对话。",
    "tempt": "做出撩人的姿势或眼神进行“钓鱼”，危险且带有目的性的调情。",
}
