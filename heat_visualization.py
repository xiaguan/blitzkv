#!/usr/bin/env python3
import json
import math # For math.ceil
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Set the style for the plots
plt.style.use('ggplot')
# Use a brighter color palette
sns.set_palette("bright") # Changed from "colorblind"

def load_data(baseline_file, optimized_file):
    """Load data from baseline and optimized JSON files."""
    with open(baseline_file, 'r') as f:
        baseline_data = json.load(f)
    with open(optimized_file, 'r') as f:
        optimized_data = json.load(f)
    return baseline_data, optimized_data

def analyze_heat_distribution(data):
    """Analyze the heat distribution of pages."""
    pages = data.get('pages', [])
    if not pages:
        return {
            'total_pages': 0, 'total_accesses': 0,
            'gini_coefficient': 0, 'pages': []
        }

    all_access_counts = [page.get('access_count', 0) for page in pages]
    total_accesses = sum(all_access_counts)
    n = len(all_access_counts)

    gini = 0
    if n > 1 and total_accesses > 0:
        sorted_counts = np.sort(all_access_counts)
        index = np.arange(1, n + 1)
        gini = (np.sum((2 * index - n - 1) * sorted_counts)) / (n * total_accesses)

    return {
        'total_pages': n,
        'total_accesses': total_accesses,
        'gini_coefficient': gini,
        'pages': pages
    }

def calculate_concentration(pages_data, percentiles):
    """Calculates the percentage of total accesses concentrated in top N percentiles of pages."""
    access_counts = [page.get('access_count', 0) for page in pages_data]
    n = len(access_counts)
    concentrations = {}

    if n == 0:
        for p in percentiles:
            concentrations[f'Top {p}%'] = 0.0
        return concentrations, 0

    sorted_counts = np.sort(access_counts)[::-1] # Sort descending
    total_accesses = np.sum(sorted_counts)

    if total_accesses == 0:
        for p in percentiles:
            concentrations[f'Top {p}%'] = 0.0
        return concentrations, 0

    for p in percentiles:
        k = max(1, math.ceil(n * p / 100.0))
        k = min(k, n) # Ensure k does not exceed total pages
        top_k_accesses = np.sum(sorted_counts[:k])
        concentrations[f'Top {p}%'] = (top_k_accesses / total_accesses) * 100.0

    return concentrations, total_accesses

def visualize_heat_improvement(baseline_data, optimized_data):
    """Create visualizations comparing baseline and optimized heat distribution."""
    baseline_analysis = analyze_heat_distribution(baseline_data)
    optimized_analysis = analyze_heat_distribution(optimized_data)

    fig, axes = plt.subplots(1, 2, figsize=(18, 7))
    fig.suptitle('Heat Distribution Improvement Analysis - Optimized vs Baseline', fontsize=16, fontweight='bold')

    # Use the first two colors from the selected palette
    colors = sns.color_palette(n_colors=2)

    # --- Plot 1: Heat Concentration Bar Chart ---
    ax1 = axes[0]
    percentiles_to_plot = [1, 5, 10, 20, 30, 40]
    baseline_concentrations, base_total_access = calculate_concentration(baseline_analysis['pages'], percentiles_to_plot)
    optimized_concentrations, opt_total_access = calculate_concentration(optimized_analysis['pages'], percentiles_to_plot)

    labels = list(baseline_concentrations.keys())
    baseline_values = list(baseline_concentrations.values())
    optimized_values = list(optimized_concentrations.values())

    x = np.arange(len(labels))
    width = 0.35

    rects1 = ax1.bar(x - width/2, baseline_values, width, label='Baseline', color=colors[0], alpha=0.8)
    rects2 = ax1.bar(x + width/2, optimized_values, width, label='Optimized', color=colors[1], alpha=0.8)

    ax1.set_ylabel('Percentage of Total Accesses (%)')
    ax1.set_title('Access Concentration in Top Pages')
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    ax1.legend()
    ax1.grid(axis='y', linestyle='--', alpha=0.7)
    ax1.set_ylim(0, 105)

    # --- Plot 2: Access Count Frequency Distribution (Histogram) ---
    ax2 = axes[1]
    baseline_access_counts = [p.get('access_count', 0) for p in baseline_analysis['pages'] if p.get('access_count', 0) > 0] # Filter out 0 for log scale
    optimized_access_counts = [p.get('access_count', 0) for p in optimized_analysis['pages'] if p.get('access_count', 0) > 0] # Filter out 0 for log scale

    if not baseline_access_counts and not optimized_access_counts:
         ax2.text(0.5, 0.5, 'No positive access data for histogram', ha='center', va='center', transform=ax2.transAxes)
    else:
        all_counts = baseline_access_counts + optimized_access_counts
        if not all_counts:
             ax2.text(0.5, 0.5, 'No positive access data for histogram', ha='center', va='center', transform=ax2.transAxes)
        else:
            max_count = max(all_counts) if all_counts else 1 # Avoid log(0) or max error
            # Create bins suitable for log scale, ensuring the lowest bin starts slightly above 0
            bins = np.logspace(np.log10(0.5), np.log10(max_count + 1), 20) # Start bins near 0.5 to avoid issues with log(0)

            # Plot histograms only if data exists for them
            if baseline_access_counts:
                ax2.hist(baseline_access_counts, bins=bins, alpha=0.6, label='Baseline', color=colors[0])
            if optimized_access_counts:
                ax2.hist(optimized_access_counts, bins=bins, alpha=0.6, label='Optimized', color=colors[1])

            ax2.set_xscale('log')
            ax2.set_yscale('log') # Set Y-axis to log scale

            ax2.set_xlabel('Access Count (log scale)')
            ax2.set_ylabel('Number of Pages (log scale)') # Updated label
            ax2.set_title('Access Count Frequency Distribution')
            ax2.legend()
            ax2.grid(True, which='both', linestyle='--', alpha=0.6) # Grid lines for both axes


    # Adjust layout and save
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig('heat_improvement_concentration_bar_logy.png', dpi=300)
    plt.show()

    print("Visualization with concentration bar chart and log-y histogram saved as 'heat_improvement_concentration_bar_logy.png'")

    # --- Print Summary Statistics ---
    print("\nSummary Statistics:")
    print("Access Concentration (% of total accesses):")
    for label in labels:
        print(f"  {label}: Baseline={baseline_concentrations[label]:.2f}%, Optimized={optimized_concentrations[label]:.2f}%")
    print(f"\nBaseline - Total Pages: {baseline_analysis['total_pages']}, Total Accesses: {base_total_access}, Gini: {baseline_analysis['gini_coefficient']:.3f}")
    print(f"Optimized - Total Pages: {optimized_analysis['total_pages']}, Total Accesses: {opt_total_access}, Gini: {optimized_analysis['gini_coefficient']:.3f}")


if __name__ == "__main__":
    try:
        baseline_data, optimized_data = load_data('baseline_vis.json', 'optimized_vis.json')
        visualize_heat_improvement(baseline_data, optimized_data)
    except FileNotFoundError:
        print("Error: Ensure 'baseline_vis.json' and 'optimized_vis.json' are in the correct directory.")
    except KeyError as e:
        print(f"Error: Missing expected key in JSON data: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")