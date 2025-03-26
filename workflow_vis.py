import json
import matplotlib.pyplot as plt
import numpy as np
import random
from matplotlib.patches import Rectangle, FancyArrowPatch
from matplotlib.colors import LinearSegmentedColormap

# Read JSON file
with open("results.json", "r") as f:
    data = json.load(f)

# Extract data for both variants
baseline_data = next(item for item in data if item['variant'] == 'baseline')
optimized_data = next(item for item in data if item['variant'] == 'optimized')

# Create a figure with adjusted size
plt.figure(figsize=(14, 10))

# Define colors
hot_color = '#ff7f0e'  # Orange for hot pages
cold_color = '#1f77b4'  # Blue for cold pages
arrow_color = '#2ca02c'  # Green for arrows
text_color = '#7f7f7f'  # Gray for text

# Create custom colormap for frequency heatmap
colors = [(0.8, 0.8, 0.8), (1, 0.5, 0)]  # Light gray to orange
cmap = LinearSegmentedColormap.from_list('custom_cmap', colors, N=100)

# Generate synthetic page data
# In a real scenario, this would come from the actual page allocation data
def generate_page_data(variant, count=100):
    # Use the frequency data from results.json to inform our synthetic data
    if variant == 'baseline':
        data_ref = baseline_data
        # In baseline, pages are allocated without considering hotness
        hot_threshold = 40000  # Very high threshold means almost all pages are "cold"
    else:
        data_ref = optimized_data
        hot_threshold = 2  # Low threshold means more pages are considered "hot"
    
    # Create synthetic page data
    pages = []
    
    # Use the frequency distribution from the results
    freq_p50 = data_ref['freq_p50']
    freq_p95 = data_ref['freq_p95']
    freq_p99 = data_ref['freq_p99']
    freq_max = data_ref['freq_max']
    
    # Generate frequencies following the distribution in the results
    for i in range(count):
        r = random.random()
        if r < 0.5:
            freq = random.uniform(1, freq_p50)
        elif r < 0.95:
            freq = random.uniform(freq_p50, freq_p95)
        elif r < 0.99:
            freq = random.uniform(freq_p95, freq_p99)
        else:
            freq = random.uniform(freq_p99, freq_max)
        
        # Determine if page is hot based on threshold
        is_hot = freq >= hot_threshold
        
        # Add some randomness to page size
        size = random.randint(1, 10)
        
        pages.append({
            'id': i,
            'frequency': freq,
            'is_hot': is_hot,
            'size': size
        })
    
    # Sort by frequency for visualization
    pages.sort(key=lambda x: x['frequency'], reverse=True)
    
    return pages

# Generate page data for both variants
baseline_pages = generate_page_data('baseline')
optimized_pages = generate_page_data('optimized')

# Get top 5 pages by frequency
top_pages = sorted(optimized_pages, key=lambda x: x['frequency'], reverse=True)[:5]
top_page_ids = [p['id'] for p in top_pages]

# Draw the workflow diagram
def draw_workflow():
    # Set up the axes
    ax = plt.subplot(1, 1, 1)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis('off')
    
    # Title
    plt.title('SSD Allocation Algorithm Workflow', fontsize=16, pad=20)
    
    # Draw the baseline allocation (left side)
    baseline_x = 15
    baseline_y = 80
    baseline_width = 25
    baseline_height = 60
    
    # Draw the optimized allocation (right side)
    optimized_x = 60
    optimized_y = 80
    optimized_width = 25
    optimized_height = 60
    
    # Draw containers
    ax.add_patch(Rectangle((baseline_x, baseline_y - baseline_height), baseline_width, baseline_height, 
                          fill=False, edgecolor='black', linewidth=2))
    ax.add_patch(Rectangle((optimized_x, optimized_y - baseline_height), optimized_width, optimized_height, 
                          fill=False, edgecolor='black', linewidth=2))
    
    # Add labels
    ax.text(baseline_x + baseline_width/2, baseline_y + 5, 'Baseline Allocation', 
            ha='center', va='center', fontsize=12, fontweight='bold')
    ax.text(optimized_x + optimized_width/2, optimized_y + 5, 'Optimized Allocation', 
            ha='center', va='center', fontsize=12, fontweight='bold')
    
    # Draw the baseline pages
    page_height = 2
    page_spacing = 0.5
    current_y = baseline_y - 5
    
    # Draw baseline pages (all cold)
    for i, page in enumerate(baseline_pages[:30]):  # Show only first 30 pages for clarity
        page_width = page['size'] * 0.5
        is_top = page['id'] in top_page_ids
        
        # In baseline, all pages are treated as cold regardless of frequency
        color = cold_color
        alpha = 0.7
        
        if is_top:
            # Highlight top pages
            edge_color = 'red'
            linewidth = 2
            alpha = 1.0
        else:
            edge_color = 'black'
            linewidth = 1
        
        # Draw the page
        ax.add_patch(Rectangle((baseline_x + 2, current_y - page_height), 
                              page_width, page_height, 
                              facecolor=color, edgecolor=edge_color, 
                              linewidth=linewidth, alpha=alpha))
        
        # Add frequency label for top pages
        if is_top:
            ax.text(baseline_x + page_width + 4, current_y - page_height/2, 
                   f"Freq: {page['frequency']:.1f}", 
                   va='center', fontsize=8, color='red')
        
        current_y -= (page_height + page_spacing)
    
    # Draw the optimized pages
    current_y = optimized_y - 5
    hot_section_height = 20
    
    # Draw a divider for hot/cold sections in optimized allocation
    divider_y = optimized_y - hot_section_height
    ax.axhline(y=divider_y, xmin=(optimized_x/100), xmax=((optimized_x + optimized_width)/100), 
              color='black', linestyle='--', linewidth=1)
    
    # Add section labels
    ax.text(optimized_x - 5, optimized_y - hot_section_height/2, 'Hot Pages', 
            ha='right', va='center', fontsize=10, rotation=90, fontweight='bold')
    ax.text(optimized_x - 5, optimized_y - hot_section_height - 20, 'Cold Pages', 
            ha='right', va='center', fontsize=10, rotation=90, fontweight='bold')
    
    # Draw optimized pages
    hot_y = optimized_y - 5
    cold_y = divider_y - 5
    
    for i, page in enumerate(optimized_pages[:30]):  # Show only first 30 pages for clarity
        page_width = page['size'] * 0.5
        is_top = page['id'] in top_page_ids
        
        if page['is_hot']:
            color = hot_color
            current_y = hot_y
            hot_y -= (page_height + page_spacing)
        else:
            color = cold_color
            current_y = cold_y
            cold_y -= (page_height + page_spacing)
        
        alpha = 0.7
        
        if is_top:
            # Highlight top pages
            edge_color = 'red'
            linewidth = 2
            alpha = 1.0
        else:
            edge_color = 'black'
            linewidth = 1
        
        # Draw the page
        ax.add_patch(Rectangle((optimized_x + 2, current_y - page_height), 
                              page_width, page_height, 
                              facecolor=color, edgecolor=edge_color, 
                              linewidth=linewidth, alpha=alpha))
        
        # Add frequency label for top pages
        if is_top:
            ax.text(optimized_x + page_width + 4, current_y - page_height/2, 
                   f"Freq: {page['frequency']:.1f}", 
                   va='center', fontsize=8, color='red')
    
    # Draw arrows connecting the workflow
    arrow = FancyArrowPatch((baseline_x + baseline_width + 5, baseline_y - baseline_height/2),
                           (optimized_x - 5, optimized_y - baseline_height/2),
                           connectionstyle="arc3,rad=.2",
                           arrowstyle="Simple,head_width=10,head_length=10",
                           color=arrow_color, linewidth=2)
    ax.add_patch(arrow)
    
    # Add explanation text
    ax.text((baseline_x + baseline_width + optimized_x)/2, 
            baseline_y - baseline_height/2 + 10,
            "Optimization separates\nhot and cold pages",
            ha='center', va='center', fontsize=10,
            bbox=dict(facecolor='white', alpha=0.7, boxstyle='round,pad=0.5'))
    
    # Add performance metrics
    metrics_x = 50
    metrics_y = 15
    
    # Create a metrics table
    metrics_text = (
        f"Performance Metrics:\n\n"
        f"Throughput: {baseline_data['throughput']:.1f} → {optimized_data['throughput']:.1f} ops/sec\n"
        f"Hit Ratio: {baseline_data['hit_ratio']:.3f} → {optimized_data['hit_ratio']:.3f}\n"
        f"SSD Reads: {baseline_data['read_ssd_ops']} → {optimized_data['read_ssd_ops']}\n"
        f"SSD Writes: {baseline_data['write_ssd_ops']} → {optimized_data['write_ssd_ops']}\n"
    )
    
    ax.text(metrics_x, metrics_y, metrics_text,
            ha='center', va='center', fontsize=10,
            bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=1.0'))
    
    # Add legend
    legend_x = 85
    legend_y = 80
    
    # Hot page
    ax.add_patch(Rectangle((legend_x, legend_y), 3, 3, facecolor=hot_color))
    ax.text(legend_x + 5, legend_y + 1.5, 'Hot Page', va='center', fontsize=9)
    
    # Cold page
    ax.add_patch(Rectangle((legend_x, legend_y - 5), 3, 3, facecolor=cold_color))
    ax.text(legend_x + 5, legend_y - 3.5, 'Cold Page', va='center', fontsize=9)
    
    # Top frequency page
    ax.add_patch(Rectangle((legend_x, legend_y - 10), 3, 3, facecolor='white', edgecolor='red', linewidth=2))
    ax.text(legend_x + 5, legend_y - 8.5, 'Top Frequency Page', va='center', fontsize=9)
    
    # Add key insights
    insights_text = (
        "Key Insights:\n\n"
        "1. Optimized allocation separates hot and cold pages\n"
        "2. Hot pages (frequently accessed) are grouped together\n"
        "3. This improves cache locality and reduces SSD operations\n"
        "4. Pages with highest frequency benefit the most\n"
        "5. Overall system throughput and hit ratio improve"
    )
    
    ax.text(legend_x - 5, legend_y - 25, insights_text,
            ha='left', va='top', fontsize=9,
            bbox=dict(facecolor='white', alpha=0.7, boxstyle='round,pad=0.5'))

# Draw the workflow diagram
draw_workflow()

# Save the figure
plt.tight_layout()
plt.savefig("workflow_diagram.png", dpi=300, bbox_inches='tight')
plt.close()

print("Workflow diagram created as 'workflow_diagram.png'")