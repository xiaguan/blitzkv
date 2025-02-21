import json
import matplotlib.pyplot as plt
import numpy as np

# Read JSON file
with open("results.json", "r") as f:
    data = json.load(f)

# Create a figure with 2x2 subplots with adjusted size and spacing
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 10))
plt.subplots_adjust(wspace=0.3, hspace=0.4)

# Extract data
variants = ['baseline', 'optimized']
metrics = {
    'throughput': [],
    'hit_ratio': [],
    'read_ssd_ops': [],
    'write_ssd_ops': []
}

for variant in variants:
    variant_data = next(item for item in data if item['variant'] == variant)
    metrics['throughput'].append(variant_data['throughput'])
    metrics['hit_ratio'].append(variant_data['hit_ratio'])
    metrics['read_ssd_ops'].append(variant_data['read_ssd_ops'])
    metrics['write_ssd_ops'].append(variant_data['write_ssd_ops'])

def add_comparison_bars(ax, values, title, ylabel, format_str='.0f', scale_factor=1.05):
    width = 0.25  # Reduced bar width
    x = np.array([0])
    
    # Create thinner bars
    bars = ax.bar(x - width/2, [values[0]], width, label=variants[0], color='skyblue', alpha=0.8)
    bars2 = ax.bar(x + width/2, [values[1]], width, label=variants[1], color='salmon', alpha=0.8)
    
    # Calculate percentage change
    pct_change = ((values[1] - values[0]) / values[0]) * 100
    change_sign = '+' if pct_change >= 0 else ''
    color = 'green' if pct_change >= 0 else 'red'
    arrow = '▲' if pct_change >= 0 else '▼'
    
    # Add percentage change indicator with smaller font
    ax.text(0, max(values) * scale_factor,
           f'{arrow} {change_sign}{pct_change:.1f}%',
           ha='center', va='bottom', color=color, fontweight='bold',
           fontsize=9)
    
    # Add value labels on bars with smaller font
    for bar in [bars[0], bars2[0]]:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, height,
               f'{height:{format_str}}',
               ha='center', va='bottom',
               fontsize=8)
    
    ax.set_title(title, pad=10, fontsize=10)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.set_xticks([])
    ax.legend(fontsize=8, loc='upper right')
    
    # Adjust y-axis limits for better spacing
    ax.set_ylim(0, max(values) * 1.15)

# 1. Throughput Comparison
add_comparison_bars(ax1, metrics['throughput'],
                   'Throughput Comparison',
                   'Throughput (ops/sec)')

# 2. Hit Ratio Comparison
add_comparison_bars(ax2, metrics['hit_ratio'],
                   'Cache Hit Ratio Comparison',
                   'Hit Ratio', '.3f')

# 3. SSD Read Operations Comparison
add_comparison_bars(ax3, metrics['read_ssd_ops'],
                   'SSD Read Operations Comparison',
                   'Number of SSD Read Operations')

# 4. SSD Write Operations Comparison
add_comparison_bars(ax4, metrics['write_ssd_ops'],
                   'SSD Write Operations Comparison',
                   'Number of SSD Write Operations')

# Adjust layout
plt.tight_layout()
plt.savefig("result.png", dpi=300, bbox_inches='tight', pad_inches=0.2)
