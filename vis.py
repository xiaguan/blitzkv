import json
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

# 1. 读取 JSON 文件
with open("results.json", "r") as f:
    data = json.load(f)

# 2. 数据整理：以 (read_ratio, zipf) 作为键，对数据进行分组
#    每个键下分别有 baseline 和 optimized 两种变体的数据
groups = defaultdict(dict)
for entry in data:
    key = (entry["read_ratio"], entry["zipf"])
    groups[key][entry["variant"]] = entry

# 为了使图表中 x 轴顺序更合理，这里对 keys 进行排序
# 排序规则：先按 read_ratio 排序，再按 zipf 排序
keys = sorted(groups.keys(), key=lambda x: (x[0], x[1]))

# 生成 x 轴标签，例如 "read_ratio=0.7\nzipf=1.1"
labels = [f"read_ratio={r}\nzipf={z}" for r, z in keys]

# 分别获取 baseline 和 optimized 的 throughput 数据
baseline_throughput = []
optimized_throughput = []
for key in keys:
    baseline_throughput.append(groups[key]["baseline"]["throughput"])
    optimized_throughput.append(groups[key]["optimized"]["throughput"])

# 3. 绘制分组柱状图
x = np.arange(len(keys))  # x 轴坐标
width = 0.35  # 每个柱子的宽度

# 设置 seaborn 风格，使图表更美观
fig, ax = plt.subplots(figsize=(10, 6))

# 绘制两组柱状图
rects1 = ax.bar(
    x - width / 2, baseline_throughput, width, label="Baseline", color="skyblue"
)
rects2 = ax.bar(
    x + width / 2, optimized_throughput, width, label="Optimized", color="salmon"
)

# 添加标题和标签
ax.set_ylabel("Throughput")
ax.set_title("Different Throughput with Different Read Ratio and Zipf")
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()


# 在柱状图上添加数据标签，使图表更直观
def autolabel(rects):
    """为每个柱状图添加文本标签"""
    for rect in rects:
        height = rect.get_height()
        ax.annotate(
            f"{height:.0f}",
            xy=(rect.get_x() + rect.get_width() / 2, height),
            xytext=(0, 3),  # 偏移量
            textcoords="offset points",
            ha="center",
            va="bottom",
        )


autolabel(rects1)
autolabel(rects2)

plt.tight_layout()
# 保存图像到文件，而不显示图形窗口
plt.savefig("result.png", dpi=300)
