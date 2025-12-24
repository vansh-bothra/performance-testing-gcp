#!/usr/bin/env python3
"""
HTML Dashboard Viewer for Performance Test Results

Reads CSV results from api_flow.py and generates an interactive HTML dashboard.

Usage:
    python view_results.py results/2024-12-24_Baseline_Test.csv --open
    python view_results.py results/run.csv -o dashboard.html
"""

import argparse
import csv
import os
import statistics
import webbrowser
from datetime import datetime
from pathlib import Path


def load_csv(filepath: str) -> list[dict]:
    """Load CSV file and return list of row dicts."""
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        return list(reader)


def compute_stats(rows: list[dict]) -> dict:
    """Compute aggregate statistics from result rows."""
    # group by wave
    waves = {}
    for row in rows:
        wave_num = int(row.get('wave', 1))
        if wave_num not in waves:
            waves[wave_num] = []
        waves[wave_num].append(row)
    
    wave_stats = []
    all_latencies = []
    all_step1, all_step2, all_step3, all_step4 = [], [], [], []
    total_success = 0
    total_failed = 0
    
    for wave_num in sorted(waves.keys()):
        wave_rows = waves[wave_num]
        success = sum(1 for r in wave_rows if r.get('success') == 'True')
        failed = len(wave_rows) - success
        total_success += success
        total_failed += failed
        
        # collect latencies for this wave
        latencies = []
        step1_latencies = []
        step2_latencies = []
        step3_latencies = []
        step4_latencies = []
        
        for r in wave_rows:
            if r.get('success') == 'True':
                try:
                    total = float(r.get('total_ms', 0))
                    latencies.append(total)
                    all_latencies.append(total)
                    
                    s1 = float(r.get('step1_ms', 0))
                    s2 = float(r.get('step2_ms', 0))
                    s3 = float(r.get('step3_ms', 0))
                    s4 = float(r.get('step4_avg_ms', 0))
                    
                    step1_latencies.append(s1)
                    step2_latencies.append(s2)
                    step3_latencies.append(s3)
                    step4_latencies.append(s4)
                    
                    all_step1.append(s1)
                    all_step2.append(s2)
                    all_step3.append(s3)
                    all_step4.append(s4)
                except ValueError:
                    pass
        
        wave_stat = {
            'wave': wave_num,
            'threads': len(wave_rows),
            'success': success,
            'failed': failed,
            'rows': wave_rows,
        }
        
        if latencies:
            wave_stat['min'] = min(latencies)
            wave_stat['max'] = max(latencies)
            wave_stat['avg'] = statistics.mean(latencies)
            wave_stat['p95'] = sorted(latencies)[int(len(latencies) * 0.95)] if len(latencies) >= 2 else max(latencies)
            wave_stat['stdev'] = statistics.stdev(latencies) if len(latencies) >= 2 else 0
            wave_stat['step1_avg'] = statistics.mean(step1_latencies) if step1_latencies else 0
            wave_stat['step2_avg'] = statistics.mean(step2_latencies) if step2_latencies else 0
            wave_stat['step3_avg'] = statistics.mean(step3_latencies) if step3_latencies else 0
            wave_stat['step4_avg'] = statistics.mean(step4_latencies) if step4_latencies else 0
        
        wave_stats.append(wave_stat)
    
    # overall stats
    overall = {
        'total_threads': len(rows),
        'success': total_success,
        'failed': total_failed,
        'success_rate': (total_success / len(rows) * 100) if rows else 0,
    }
    
    if all_latencies:
        overall['min'] = min(all_latencies)
        overall['max'] = max(all_latencies)
        overall['avg'] = statistics.mean(all_latencies)
        overall['p50'] = statistics.median(all_latencies)
        overall['p95'] = sorted(all_latencies)[int(len(all_latencies) * 0.95)] if len(all_latencies) >= 2 else max(all_latencies)
        overall['stdev'] = statistics.stdev(all_latencies) if len(all_latencies) >= 2 else 0
        overall['step1_avg'] = statistics.mean(all_step1) if all_step1 else 0
        overall['step2_avg'] = statistics.mean(all_step2) if all_step2 else 0
        overall['step3_avg'] = statistics.mean(all_step3) if all_step3 else 0
        overall['step4_avg'] = statistics.mean(all_step4) if all_step4 else 0
    
    return {
        'waves': wave_stats,
        'overall': overall,
        'all_latencies': all_latencies,
    }


def find_outliers(latencies: list[float], stdev: float, avg: float) -> set[int]:
    """Find indices of outliers (> 2 standard deviations from mean)."""
    if stdev == 0:
        return set()
    threshold = avg + (2 * stdev)
    return {i for i, lat in enumerate(latencies) if lat > threshold}


def generate_html(csv_path: str, rows: list[dict], stats: dict) -> str:
    """Generate HTML dashboard string with charts."""
    import json
    
    title = Path(csv_path).stem
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    overall = stats['overall']
    
    # Prepare chart data
    all_latencies = stats['all_latencies']
    
    # Per-thread latencies for line chart (in order) with wave info
    thread_latencies = []
    thread_waves = []
    for row in rows:
        wave_num = int(row.get('wave', 1))
        if row.get('success') == 'True':
            try:
                thread_latencies.append(float(row.get('total_ms', 0)))
            except ValueError:
                thread_latencies.append(0)
        else:
            thread_latencies.append(0)
        thread_waves.append(wave_num)
    
    # Per-wave averages for line chart
    wave_avgs = [wave.get('avg', 0) for wave in stats['waves']]
    wave_labels = [f"Wave {wave['wave']}" for wave in stats['waves']]
    
    # Histogram bins
    if all_latencies:
        min_lat = min(all_latencies)
        max_lat = max(all_latencies)
        bin_count = min(20, len(all_latencies))
        bin_width = (max_lat - min_lat) / bin_count if bin_count > 0 else 1
        histogram_bins = [0] * bin_count
        histogram_labels = []
        for i in range(bin_count):
            bin_start = min_lat + i * bin_width
            bin_end = bin_start + bin_width
            histogram_labels.append(f"{bin_start:.0f}-{bin_end:.0f}")
            for lat in all_latencies:
                if bin_start <= lat < bin_end or (i == bin_count - 1 and lat == max_lat):
                    histogram_bins[i] += 1
    else:
        histogram_bins = []
        histogram_labels = []
    
    # Build wave table rows (show first 10, rest hidden)
    wave_table_rows = []
    for i, wave in enumerate(stats['waves']):
        success_str = f"{wave['success']}/{wave['threads']}"
        success_class = 'success' if wave['failed'] == 0 else 'warning' if wave['failed'] < wave['threads'] / 2 else 'danger'
        hidden_class = 'hidden-row' if i >= 10 else ''
        
        wave_table_rows.append(f"""
            <tr class="{hidden_class}" data-wave-row>
                <td>{wave['wave']}</td>
                <td class="{success_class}">{success_str}</td>
                <td>{wave.get('step1_avg', 0):.0f}</td>
                <td>{wave.get('step2_avg', 0):.0f}</td>
                <td>{wave.get('step3_avg', 0):.0f}</td>
                <td>{wave.get('step4_avg', 0):.0f}</td>
                <td>{wave.get('avg', 0):.0f}</td>
                <td>{wave.get('p95', 0):.0f}</td>
            </tr>
        """)
    
    show_more_waves = len(stats['waves']) > 10
    
    # Build thread detail sections (collapsible per wave)
    thread_sections = []
    for wave in stats['waves']:
        wave_rows = wave['rows']
        stdev = wave.get('stdev', 0)
        avg = wave.get('avg', 0)
        
        thread_rows_html = []
        for i, r in enumerate(wave_rows):
            is_success = r.get('success') == 'True'
            total_ms = float(r.get('total_ms', 0)) if is_success else 0
            is_outlier = is_success and stdev > 0 and total_ms > (avg + 2 * stdev)
            row_class = 'outlier' if is_outlier else ('failure' if not is_success else '')
            hidden_class = 'hidden-row' if i >= 10 else ''
            
            thread_rows_html.append(f"""
                <tr class="{row_class} {hidden_class}" data-thread-row-{wave['wave']}>
                    <td>{r.get('thread', i)}</td>
                    <td>{r.get('uid', '-')}</td>
                    <td>{'✓' if is_success else '✗'}</td>
                    <td>{r.get('step1_ms', '-')}</td>
                    <td>{r.get('step2_ms', '-')}</td>
                    <td>{r.get('step3_ms', '-')}</td>
                    <td>{r.get('step4_avg_ms', '-')}</td>
                    <td class="{'outlier-value' if is_outlier else ''}">{r.get('total_ms', '-')}</td>
                    <td class="error-cell">{r.get('error', '') if not is_success else ''}</td>
                </tr>
            """)
        
        show_more_threads = len(wave_rows) > 10
        toggle_btn = f"""<button class="toggle-btn" onclick="toggleThreadRows({wave['wave']}, this)">Show all ({len(wave_rows)})</button>""" if show_more_threads else ""
        
        thread_sections.append(f"""
            <details class="wave-details">
                <summary>Wave {wave['wave']} - {wave['success']}/{wave['threads']} success {toggle_btn}</summary>
                <table class="thread-table">
                    <thead>
                        <tr>
                            <th>Thread</th>
                            <th>UID</th>
                            <th>OK</th>
                            <th>Step1 (ms)</th>
                            <th>Step2 (ms)</th>
                            <th>Step3 (ms)</th>
                            <th>Step4 Avg (ms)</th>
                            <th>Total (ms)</th>
                            <th>Error</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(thread_rows_html)}
                    </tbody>
                </table>
            </details>
        """)
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - Performance Results</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{
            --bg-primary: #0d1117;
            --bg-secondary: #161b22;
            --bg-tertiary: #21262d;
            --text-primary: #c9d1d9;
            --text-secondary: #8b949e;
            --accent: #58a6ff;
            --success: #3fb950;
            --warning: #d29922;
            --danger: #f85149;
            --border: #30363d;
        }}
        
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            padding: 2rem;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        header {{
            margin-bottom: 2rem;
            padding-bottom: 1rem;
            border-bottom: 1px solid var(--border);
        }}
        
        h1 {{
            font-size: 1.8rem;
            margin-bottom: 0.5rem;
        }}
        
        .meta {{
            color: var(--text-secondary);
            font-size: 0.9rem;
        }}
        
        .cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }}
        
        .card {{
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 1rem;
        }}
        
        .card-label {{
            font-size: 0.8rem;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        
        .card-value {{
            font-size: 1.5rem;
            font-weight: 600;
            margin-top: 0.25rem;
        }}
        
        .card-value.success {{ color: var(--success); }}
        .card-value.warning {{ color: var(--warning); }}
        .card-value.danger {{ color: var(--danger); }}
        
        .section-header {{
            display: flex;
            align-items: center;
            gap: 1rem;
            margin: 2rem 0 1rem;
        }}
        
        h2 {{
            font-size: 1.2rem;
            color: var(--text-secondary);
            margin: 0;
        }}
        
        .toggle-btn {{
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            color: var(--accent);
            padding: 0.25rem 0.75rem;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.8rem;
        }}
        
        .toggle-btn:hover {{
            background: var(--accent);
            color: var(--bg-primary);
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            background: var(--bg-secondary);
            border-radius: 8px;
            overflow: hidden;
        }}
        
        th, td {{
            padding: 0.75rem 1rem;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }}
        
        th {{
            background: var(--bg-tertiary);
            font-weight: 600;
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-secondary);
        }}
        
        tr:last-child td {{
            border-bottom: none;
        }}
        
        .hidden-row {{
            display: none;
        }}
        
        .success {{ color: var(--success); }}
        .warning {{ color: var(--warning); }}
        .danger {{ color: var(--danger); }}
        
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 1.5rem;
            margin: 2rem 0;
        }}
        
        .chart-container {{
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 1.5rem;
        }}
        
        .chart-title {{
            font-size: 0.9rem;
            color: var(--text-secondary);
            margin-bottom: 1rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        
        .charts-toggle {{
            display: none;
        }}
        
        .charts-toggle.visible {{
            display: grid;
        }}
        
        .wave-details {{
            margin-bottom: 1rem;
        }}
        
        .wave-details summary {{
            cursor: pointer;
            padding: 0.75rem 1rem;
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 8px;
            font-weight: 500;
            display: flex;
            align-items: center;
            gap: 1rem;
        }}
        
        .wave-details summary:hover {{
            background: var(--bg-tertiary);
        }}
        
        .wave-details[open] summary {{
            border-radius: 8px 8px 0 0;
            border-bottom: none;
        }}
        
        .thread-table {{
            border-radius: 0 0 8px 8px;
            font-size: 0.9rem;
        }}
        
        .thread-table th {{
            font-size: 0.75rem;
        }}
        
        tr.outlier {{
            background: rgba(248, 81, 73, 0.1);
        }}
        
        tr.failure {{
            background: rgba(248, 81, 73, 0.15);
        }}
        
        .outlier-value {{
            color: var(--danger);
            font-weight: 600;
        }}
        
        .error-cell {{
            color: var(--danger);
            font-size: 0.8rem;
            max-width: 200px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        
        .legend {{
            margin-top: 2rem;
            padding: 1rem;
            background: var(--bg-secondary);
            border-radius: 8px;
            font-size: 0.85rem;
            color: var(--text-secondary);
        }}
        
        .legend-item {{
            display: inline-block;
            margin-right: 2rem;
        }}
        
        .legend-color {{
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 2px;
            margin-right: 0.5rem;
            vertical-align: middle;
        }}
        
        .legend-color.outlier {{ background: rgba(248, 81, 73, 0.3); }}
        .legend-color.failure {{ background: rgba(248, 81, 73, 0.5); }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>{title}</h1>
            <div class="meta">Generated: {timestamp} | Source: {Path(csv_path).name}</div>
        </header>
        
        <div class="cards">
            <div class="card">
                <div class="card-label">Total Threads</div>
                <div class="card-value">{overall.get('total_threads', 0)}</div>
            </div>
            <div class="card">
                <div class="card-label">Success Rate</div>
                <div class="card-value {'success' if overall.get('success_rate', 0) >= 95 else 'warning' if overall.get('success_rate', 0) >= 80 else 'danger'}">{overall.get('success_rate', 0):.1f}%</div>
            </div>
            <div class="card">
                <div class="card-label">Avg Latency</div>
                <div class="card-value">{overall.get('avg', 0):.0f} ms</div>
            </div>
            <div class="card">
                <div class="card-label">P95 Latency</div>
                <div class="card-value">{overall.get('p95', 0):.0f} ms</div>
            </div>
            <div class="card">
                <div class="card-label">Min / Max</div>
                <div class="card-value">{overall.get('min', 0):.0f} / {overall.get('max', 0):.0f}</div>
            </div>
        </div>
        
        <!-- Charts Section -->
        <div class="section-header">
            <h2>Charts</h2>
            <button class="toggle-btn" id="chartsToggleBtn" onclick="toggleCharts(this)">View charts</button>
        </div>
        <div class="charts-grid charts-toggle" id="chartsContainer">
            <div class="chart-container">
                <div class="chart-title">Latency Distribution (Histogram)</div>
                <canvas id="histogramChart"></canvas>
            </div>
            <div class="chart-container">
                <div class="chart-title">Per-Wave Average Latency</div>
                <canvas id="waveChart"></canvas>
            </div>
            <div class="chart-container" style="grid-column: span 2; height: 300px;">
                <div class="chart-title">Per-Thread Completion Time</div>
                <canvas id="threadChart"></canvas>
            </div>
        </div>
        
        <div class="section-header">
            <h2>Per-Wave Summary</h2>
            {'<button class="toggle-btn" onclick="toggleWaveRows(this)">Show all (' + str(len(stats['waves'])) + ')</button>' if show_more_waves else ''}
        </div>
        <table id="waveTable">
            <thead>
                <tr>
                    <th>Wave</th>
                    <th>Success</th>
                    <th>Step1 Avg</th>
                    <th>Step2 Avg</th>
                    <th>Step3 Avg</th>
                    <th>Step4 Avg</th>
                    <th>Total Avg</th>
                    <th>P95</th>
                </tr>
            </thead>
            <tbody>
                {''.join(wave_table_rows)}
            </tbody>
        </table>
        
        <div class="section-header">
            <h2>Thread Details (click to expand)</h2>
        </div>
        {''.join(thread_sections)}
        
        <div class="legend">
            <span class="legend-item"><span class="legend-color outlier"></span> Outlier (>2σ from mean)</span>
            <span class="legend-item"><span class="legend-color failure"></span> Failed request</span>
        </div>
    </div>
    
    <script>
        // Chart.js configuration
        Chart.defaults.color = '#8b949e';
        Chart.defaults.borderColor = '#30363d';
        
        // Histogram
        new Chart(document.getElementById('histogramChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(histogram_labels)},
                datasets: [{{
                    label: 'Threads',
                    data: {json.dumps(histogram_bins)},
                    backgroundColor: 'rgba(88, 166, 255, 0.6)',
                    borderColor: 'rgba(88, 166, 255, 1)',
                    borderWidth: 1
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    x: {{ 
                        title: {{ display: true, text: 'Latency (ms)' }},
                        grid: {{ display: false }}
                    }},
                    y: {{ 
                        title: {{ display: true, text: 'Count' }},
                        beginAtZero: true
                    }}
                }}
            }}
        }});
        
        // Wave line chart
        new Chart(document.getElementById('waveChart'), {{
            type: 'line',
            data: {{
                labels: {json.dumps(wave_labels)},
                datasets: [{{
                    label: 'Avg Latency (ms)',
                    data: {json.dumps(wave_avgs)},
                    borderColor: '#3fb950',
                    backgroundColor: 'rgba(63, 185, 80, 0.1)',
                    fill: true,
                    tension: 0.3
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    y: {{ 
                        beginAtZero: true,
                        title: {{ display: true, text: 'Latency (ms)' }}
                    }}
                }}
            }}
        }});
        
        // Thread completion time line chart
        const threadWaves = {json.dumps(thread_waves)};
        const threadData = {json.dumps(thread_latencies)};
        let activePoint = null;
        const threadChart = new Chart(document.getElementById('threadChart'), {{
            type: 'line',
            data: {{
                labels: {json.dumps(list(range(1, len(thread_latencies) + 1)))},
                datasets: [{{
                    label: 'Completion Time (ms)',
                    data: threadData,
                    borderColor: '#58a6ff',
                    backgroundColor: 'rgba(88, 166, 255, 0.1)',
                    fill: true,
                    pointRadius: 0,
                    pointHoverRadius: 6,
                    pointHoverBackgroundColor: '#ffffff',
                    pointHoverBorderColor: '#58a6ff',
                    pointHoverBorderWidth: 2,
                    tension: 0.1
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                interaction: {{
                    intersect: false,
                    mode: 'index'
                }},
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        enabled: false,
                        external: function(context) {{
                            if (activePoint === null) return;
                            
                            let tooltipEl = document.getElementById('chartjs-tooltip');
                            if (!tooltipEl) {{
                                tooltipEl = document.createElement('div');
                                tooltipEl.id = 'chartjs-tooltip';
                                tooltipEl.style.cssText = 'background: #21262d; border: 1px solid #30363d; border-radius: 6px; padding: 8px 12px; position: absolute; pointer-events: none; font-size: 13px; color: #c9d1d9; z-index: 100;';
                                document.body.appendChild(tooltipEl);
                            }}
                            
                            const idx = activePoint.index;
                            const wave = threadWaves[idx];
                            const time = threadData[idx].toFixed(0);
                            
                            tooltipEl.innerHTML = `<strong>Thread #${{idx + 1}}</strong><br>Completion: ${{time}} ms<br>Wave: ${{wave}}`;
                            tooltipEl.style.left = activePoint.x + 'px';
                            tooltipEl.style.top = (activePoint.y - 60) + 'px';
                            tooltipEl.style.display = 'block';
                        }}
                    }}
                }},
                scales: {{
                    x: {{ 
                        title: {{ display: false }},
                        grid: {{ display: false }}
                    }},
                    y: {{ 
                        beginAtZero: true,
                        title: {{ display: true, text: 'Latency (ms)' }}
                    }}
                }},
                onClick: function(evt, elements) {{
                    const tooltipEl = document.getElementById('chartjs-tooltip');
                    if (elements.length > 0) {{
                        const el = elements[0];
                        // Toggle off if clicking same point
                        if (activePoint && activePoint.index === el.index) {{
                            activePoint = null;
                            if (tooltipEl) tooltipEl.style.display = 'none';
                            return;
                        }}
                        const pos = el.element;
                        activePoint = {{ index: el.index, x: pos.x + this.canvas.offsetLeft, y: pos.y + this.canvas.offsetTop }};
                        this.options.plugins.tooltip.external({{ chart: this }});
                    }} else {{
                        activePoint = null;
                        if (tooltipEl) tooltipEl.style.display = 'none';
                    }}
                }}
            }}
        }});
        
        // Hide tooltip when clicking elsewhere
        document.addEventListener('click', function(e) {{
            if (!e.target.closest('#threadChart')) {{
                const tooltipEl = document.getElementById('chartjs-tooltip');
                if (tooltipEl) tooltipEl.style.display = 'none';
                activePoint = null;
            }}
        }});
        
        // Toggle functions
        function toggleWaveRows(btn) {{
            const rows = document.querySelectorAll('[data-wave-row]');
            const isHidden = rows[10]?.classList.contains('hidden-row');
            rows.forEach((row, i) => {{
                if (i >= 10) {{
                    row.classList.toggle('hidden-row', !isHidden);
                }}
            }});
            btn.textContent = isHidden ? 'Show less' : 'Show all ({len(stats['waves'])})';
        }}
        
        function toggleThreadRows(waveNum, btn) {{
            const rows = document.querySelectorAll(`[data-thread-row-${{waveNum}}]`);
            const hiddenRows = Array.from(rows).filter((_, i) => i >= 10);
            const isHidden = hiddenRows[0]?.classList.contains('hidden-row');
            hiddenRows.forEach(row => {{
                row.classList.toggle('hidden-row', !isHidden);
            }});
            btn.textContent = isHidden ? 'Show less' : `Show all (${{rows.length}})`;
        }}
        
        function toggleCharts(btn) {{
            const container = document.getElementById('chartsContainer');
            const isVisible = container.classList.contains('visible');
            container.classList.toggle('visible', !isVisible);
            btn.textContent = isVisible ? 'View charts' : 'Hide charts';
        }}
    </script>
</body>
</html>
"""
    return html


def main():
    parser = argparse.ArgumentParser(description="Generate HTML dashboard from CSV results")
    parser.add_argument("csv_file", help="Path to CSV results file")
    parser.add_argument("-o", "--output", help="Output HTML file path (default: same name as CSV with .html)")
    parser.add_argument("--open", action="store_true", help="Open the generated HTML in browser")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_file):
        print(f"Error: File not found: {args.csv_file}")
        return 1
    
    # load and process data
    rows = load_csv(args.csv_file)
    stats = compute_stats(rows)
    
    # generate HTML
    html = generate_html(args.csv_file, rows, stats)
    
    # determine output path
    if args.output:
        output_path = args.output
    else:
        output_path = Path(args.csv_file).with_suffix('.html')
    
    # write HTML
    with open(output_path, 'w') as f:
        f.write(html)
    
    print(f"Generated: {output_path}")
    print(f"  Threads: {stats['overall'].get('total_threads', 0)}")
    print(f"  Success: {stats['overall'].get('success', 0)} ({stats['overall'].get('success_rate', 0):.1f}%)")
    print(f"  Avg latency: {stats['overall'].get('avg', 0):.0f} ms")
    
    if args.open:
        webbrowser.open(f"file://{os.path.abspath(output_path)}")
    
    return 0


if __name__ == "__main__":
    exit(main())
