import json
import os
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.cm as cm

# Check if the visualization JSON files exist
baseline_file = "baseline_vis.json"
optimized_file = "optimized_vis.json"

if not os.path.exists(baseline_file) or not os.path.exists(optimized_file):
    print(f"Error: Required files {baseline_file} and/or {optimized_file} not found.")
    print("Please run the benchmark first to generate these files.")
    exit(1)

# Load the data
with open(baseline_file, 'r') as f:
    baseline_data = json.load(f)

with open(optimized_file, 'r') as f:
    optimized_data = json.load(f)

# Create output directory for images
os.makedirs("visualization_output", exist_ok=True)

# Function to create a heatmap of page hotness
def create_page_heatmap(data, title, filename):
    # Extract page data
    pages = data["pages"]
    
    # Sort pages by hotness and access count
    pages_sorted = sorted(pages, key=lambda x: (x["is_hot"], x["access_count"]), reverse=True)
    
    # Prepare data for heatmap
    page_ids = [p["page_id"] for p in pages_sorted]
    access_counts = [p["access_count"] for p in pages_sorted]
    is_hot = [p["is_hot"] for p in pages_sorted]
    free_space = [p["free_space"] for p in pages_sorted]
    utilization = [1 - (p["free_space"] / 4096) for p in pages_sorted]  # Assuming 4KB page size
    
    # Create a custom colormap: cold (blue) to hot (red)
    colors = [(0, 0, 1), (1, 1, 1), (1, 0, 0)]  # Blue -> White -> Red
    cmap_name = 'hot_cold'
    cm_hot_cold = LinearSegmentedColormap.from_list(cmap_name, colors, N=100)
    
    # Normalize data for color mapping
    max_access = max(access_counts) if access_counts else 1
    normalized_access = [count / max_access for count in access_counts]
    
    # Create figure with custom size
    plt.figure(figsize=(12, 8))
    
    # Calculate grid dimensions
    n_pages = len(page_ids)
    grid_size = int(np.ceil(np.sqrt(n_pages)))
    
    # Create grid of squares
    for i, (page_id, access, hot, util) in enumerate(zip(page_ids, normalized_access, is_hot, utilization)):
        if i >= grid_size * grid_size:
            break
            
        row = i // grid_size
        col = i % grid_size
        
        # Square position and size (size represents utilization)
        size = 0.8 * util  # Scale size by utilization (min 10%, max 80% of cell)
        if size < 0.1:  # Ensure minimum visibility
            size = 0.1
            
        x = col + 0.5
        y = grid_size - row - 0.5  # Invert y-axis for better visualization
        
        # Color based on access frequency
        color = cm_hot_cold(access)
        
        # Draw square
        square = plt.Rectangle((x - size/2, y - size/2), size, size, 
                              color=color, alpha=0.8, 
                              linewidth=1, edgecolor='black')
        plt.gca().add_patch(square)
        
        # Add border for hot pages
        if hot:
            border = plt.Rectangle((x - 0.45, y - 0.45), 0.9, 0.9, 
                                  fill=False, linewidth=2, 
                                  edgecolor='gold', linestyle='--')
            plt.gca().add_patch(border)
    
    # Set plot limits and remove axes
    plt.xlim(0, grid_size)
    plt.ylim(0, grid_size)
    plt.axis('off')
    
    # Add title and legend
    plt.title(title, fontsize=16, pad=20)
    
    # Add custom legend
    legend_elements = [
        plt.Rectangle((0, 0), 1, 1, color='blue', alpha=0.8, label='Cold (Low Access)'),
        plt.Rectangle((0, 0), 1, 1, color='red', alpha=0.8, label='Hot (High Access)'),
        plt.Rectangle((0, 0), 1, 1, color='white', fill=False, edgecolor='gold', 
                     linestyle='--', linewidth=2, label='Hot Page'),
    ]
    plt.legend(handles=legend_elements, loc='upper center', 
              bbox_to_anchor=(0.5, -0.05), ncol=3, fontsize=12)
    
    # Add explanation text
    plt.figtext(0.5, -0.02, "Square size represents page utilization (larger = more utilized)", 
               ha='center', fontsize=10)
    
    # Save figure
    plt.tight_layout()
    plt.savefig(f"visualization_output/{filename}", dpi=300, bbox_inches='tight')
    plt.close()

# Create interactive dashboard with Plotly
def create_dashboard(baseline, optimized, filename):
    # Create figure with subplots
    fig = make_subplots(
        rows=2, cols=2,
        specs=[[{"type": "indicator"}, {"type": "indicator"}],
               [{"type": "bar"}, {"type": "scatter"}]],
        subplot_titles=("Cache Hit Ratio", "SSD Read Operations Reduction", 
                       "Page Temperature Distribution", "Access Frequency Distribution"),
        vertical_spacing=0.1,
        horizontal_spacing=0.1,
    )
    
    # 1. Hit Ratio Indicator
    baseline_hit = baseline["hit_ratio"]
    optimized_hit = optimized["hit_ratio"]
    hit_improvement = ((optimized_hit - baseline_hit) / baseline_hit) * 100
    
    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=optimized_hit * 100,  # Convert to percentage
            number={"suffix": "%", "font": {"size": 40}},
            delta={"reference": baseline_hit * 100, "relative": True, 
                  "valueformat": ".1f", "font": {"size": 20}},
            title={"text": "Cache Hit Ratio", "font": {"size": 20}},
            domain={"row": 0, "column": 0}
        ),
        row=1, col=1
    )
    
    # 2. SSD Read Operations Indicator
    baseline_reads = baseline["ssd_metrics"]["reads"]
    optimized_reads = optimized["ssd_metrics"]["reads"]
    read_reduction = ((baseline_reads - optimized_reads) / baseline_reads) * 100
    
    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=optimized_reads,
            number={"font": {"size": 40}},
            delta={"reference": baseline_reads, "relative": True, 
                  "valueformat": ".1f", "decreasing": {"color": "green"}, 
                  "increasing": {"color": "red"}, "font": {"size": 20}},
            title={"text": "SSD Read Operations", "font": {"size": 20}},
            domain={"row": 0, "column": 1}
        ),
        row=1, col=2
    )
    
    # 3. Page Temperature Distribution
    baseline_hot = sum(1 for p in baseline["pages"] if p["is_hot"])
    baseline_cold = len(baseline["pages"]) - baseline_hot
    optimized_hot = sum(1 for p in optimized["pages"] if p["is_hot"])
    optimized_cold = len(optimized["pages"]) - optimized_hot
    
    fig.add_trace(
        go.Bar(
            x=["Baseline", "Optimized"],
            y=[baseline_hot, optimized_hot],
            name="Hot Pages",
            marker_color="crimson"
        ),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Bar(
            x=["Baseline", "Optimized"],
            y=[baseline_cold, optimized_cold],
            name="Cold Pages",
            marker_color="royalblue"
        ),
        row=2, col=1
    )
    
    # 4. Access Frequency Distribution
    # Extract frequency data from objects
    baseline_freqs = [obj["freq"] for page in baseline["pages"] for obj in page["objects"]]
    optimized_freqs = [obj["freq"] for page in optimized["pages"] for obj in page["objects"]]
    
    # Create histograms
    baseline_hist, baseline_bins = np.histogram(baseline_freqs, bins=20, range=(0, 20))
    optimized_hist, optimized_bins = np.histogram(optimized_freqs, bins=20, range=(0, 20))
    
    # Plot frequency distributions
    fig.add_trace(
        go.Scatter(
            x=baseline_bins[:-1],
            y=baseline_hist,
            mode="lines",
            name="Baseline",
            line=dict(color="royalblue", width=2)
        ),
        row=2, col=2
    )
    
    fig.add_trace(
        go.Scatter(
            x=optimized_bins[:-1],
            y=optimized_hist,
            mode="lines",
            name="Optimized",
            line=dict(color="crimson", width=2)
        ),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(
        title_text="BlitzKV Optimization Dashboard",
        height=800,
        width=1200,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        barmode="stack"
    )
    
    # Update axes
    fig.update_xaxes(title_text="Algorithm", row=2, col=1)
    fig.update_yaxes(title_text="Number of Pages", row=2, col=1)
    fig.update_xaxes(title_text="Access Frequency", row=2, col=2)
    fig.update_yaxes(title_text="Number of Objects", row=2, col=2)
    
    # Save to HTML
    fig.write_html(f"visualization_output/{filename}")
    
    return fig

# Create 3D visualization of page access patterns
def create_3d_visualization(baseline, optimized, filename):
    # Create figure
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "scene"}, {"type": "scene"}]],
        subplot_titles=("Baseline Page Access Pattern", "Optimized Page Access Pattern"),
        horizontal_spacing=0.05
    )
    
    # Process data for both variants
    for i, (data, name, color) in enumerate([
        (baseline, "Baseline", "blue"), 
        (optimized, "Optimized", "red")
    ]):
        # Extract page data
        pages = data["pages"]
        
        # Create x, y, z data
        x = []  # Page IDs
        y = []  # Object frequencies
        z = []  # Access counts
        sizes = []  # Marker sizes based on utilization
        colors = []  # Colors based on hot/cold
        hover_texts = []  # Hover information
        
        for page in pages:
            page_id = page["page_id"]
            is_hot = page["is_hot"]
            access_count = page["access_count"]
            utilization = 1 - (page["free_space"] / 4096)  # Assuming 4KB page size
            
            # Add data for each object in the page
            for obj in page["objects"]:
                x.append(page_id)
                y.append(obj["freq"])
                z.append(access_count)
                sizes.append(utilization * 20 + 5)  # Scale for visibility
                colors.append("red" if is_hot else "blue")
                hover_texts.append(
                    f"Page ID: {page_id}<br>"
                    f"Object: {obj['key']}<br>"
                    f"Frequency: {obj['freq']:.2f}<br>"
                    f"Page Access Count: {access_count}<br>"
                    f"Utilization: {utilization:.2f}<br>"
                    f"Hot: {'Yes' if is_hot else 'No'}"
                )
        
        # Add 3D scatter plot
        fig.add_trace(
            go.Scatter3d(
                x=x,
                y=y,
                z=z,
                mode="markers",
                marker=dict(
                    size=sizes,
                    color=colors,
                    opacity=0.7,
                    line=dict(width=0.5, color="white")
                ),
                text=hover_texts,
                hoverinfo="text",
                name=name
            ),
            row=1, col=i+1
        )
    
    # Update layout
    fig.update_layout(
        title_text="3D Visualization of Page Access Patterns",
        height=800,
        width=1200,
        scene=dict(
            xaxis_title="Page ID",
            yaxis_title="Object Frequency",
            zaxis_title="Page Access Count",
            aspectmode="cube"
        ),
        scene2=dict(
            xaxis_title="Page ID",
            yaxis_title="Object Frequency",
            zaxis_title="Page Access Count",
            aspectmode="cube"
        )
    )
    
    # Save to HTML
    fig.write_html(f"visualization_output/{filename}")
    
    return fig

# Create page allocation visualization
def create_allocation_visualization(baseline, optimized, filename):
    # Create figure
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "xy"}, {"type": "xy"}]],
        subplot_titles=("Baseline Page Allocation", "Optimized Page Allocation"),
        horizontal_spacing=0.1
    )
    
    # Process data for both variants
    for i, (data, name) in enumerate([
        (baseline, "Baseline"), 
        (optimized, "Optimized")
    ]):
        # Extract page data
        pages = sorted(data["pages"], key=lambda x: x["page_id"])
        
        # Create data for visualization
        page_ids = [p["page_id"] for p in pages]
        utilizations = [1 - (p["free_space"] / 4096) for p in pages]  # Assuming 4KB page size
        is_hot = [p["is_hot"] for p in pages]
        
        # Create color array
        colors = ["red" if hot else "blue" for hot in is_hot]
        
        # Add bar chart
        fig.add_trace(
            go.Bar(
                x=page_ids,
                y=utilizations,
                marker_color=colors,
                name=name,
                text=[f"Hot: {'Yes' if hot else 'No'}" for hot in is_hot],
                hovertemplate="Page ID: %{x}<br>Utilization: %{y:.2f}<br>%{text}"
            ),
            row=1, col=i+1
        )
        
        # Add horizontal line at 0.5 utilization
        fig.add_shape(
            type="line",
            x0=min(page_ids),
            y0=0.5,
            x1=max(page_ids),
            y1=0.5,
            line=dict(color="green", width=2, dash="dash"),
            row=1, col=i+1
        )
    
    # Update layout
    fig.update_layout(
        title_text="Page Allocation and Utilization Comparison",
        height=600,
        width=1200,
        showlegend=False
    )
    
    # Update axes
    fig.update_xaxes(title_text="Page ID", row=1, col=1)
    fig.update_xaxes(title_text="Page ID", row=1, col=2)
    fig.update_yaxes(title_text="Page Utilization", row=1, col=1)
    fig.update_yaxes(title_text="Page Utilization", row=1, col=2)
    
    # Save to HTML
    fig.write_html(f"visualization_output/{filename}")
    
    return fig

# Generate all visualizations
print("Generating visualizations...")

# 1. Create heatmaps
print("Creating page heatmaps...")
create_page_heatmap(baseline_data, "Baseline Page Temperature Distribution", "baseline_heatmap.png")
create_page_heatmap(optimized_data, "Optimized Page Temperature Distribution", "optimized_heatmap.png")

# 2. Create dashboard
print("Creating performance dashboard...")
dashboard = create_dashboard(baseline_data, optimized_data, "performance_dashboard.html")

# 3. Create 3D visualization
print("Creating 3D access pattern visualization...")
viz_3d = create_3d_visualization(baseline_data, optimized_data, "access_patterns_3d.html")

# 4. Create allocation visualization
print("Creating page allocation visualization...")
alloc_viz = create_allocation_visualization(baseline_data, optimized_data, "page_allocation.html")

print("\nVisualization complete! Output files are in the 'visualization_output' directory.")
print("\nSummary of improvements:")
baseline_hit = baseline_data["hit_ratio"]
optimized_hit = optimized_data["hit_ratio"]
hit_improvement = ((optimized_hit - baseline_hit) / baseline_hit) * 100

baseline_reads = baseline_data["ssd_metrics"]["reads"]
optimized_reads = optimized_data["ssd_metrics"]["reads"]
read_reduction = ((baseline_reads - optimized_reads) / baseline_reads) * 100

print(f"- Cache Hit Ratio: {baseline_hit:.2%} → {optimized_hit:.2%} ({hit_improvement:+.2f}%)")
print(f"- SSD Read Operations: {baseline_reads:,} → {optimized_reads:,} ({read_reduction:+.2f}%)")

# Create a shell script to open the visualizations
with open("run_visualization.sh", "w") as f:
    f.write("#!/bin/bash\n\n")
    f.write("echo 'Opening visualizations...'\n")
    f.write("cd visualization_output\n")
    f.write("python -m http.server 8000 &\n")
    f.write("SERVER_PID=$!\n")
    f.write("sleep 1\n")
    f.write("xdg-open http://localhost:8000/performance_dashboard.html\n")
    f.write("echo 'Press Ctrl+C to stop the server'\n")
    f.write("trap \"kill $SERVER_PID; echo 'Server stopped'\" INT\n")
    f.write("wait\n")

# Make the script executable
os.chmod("run_visualization.sh", 0o755)
print("\nTo view interactive visualizations, run: ./run_visualization.sh")
