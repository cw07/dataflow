import json
import logging
from pathlib import Path
from typing import List
from dataflow.config.loaders.time_series import TimeSeriesConfig


logger = logging.getLogger(__name__)


def time_series_html(time_series: List[TimeSeriesConfig], output_path: Path = None) -> None:
    """
    Generate an interactive HTML dashboard for time series configurations.
    
    Args:
        time_series: List of TimeSeriesConfig objects
        output_path: Path to save the HTML file (default: ./time_series.html)
    """
    if output_path is None:
        output_path = Path("./time_series.html")
    
    # Prepare data for JavaScript
    ts_data = []
    for ts in time_series:
        ts_dict = {
            'service_id': ts.service_id,
            'series_id': ts.series_id,
            'series_type': ts.series_type,
            'root_id': ts.root_id,
            'venue': ts.venue,
            'data_schema': ts.data_schema.value if hasattr(ts.data_schema, 'value') else str(ts.data_schema),
            'data_source': ts.data_source,
            'destination': ', '.join(ts.destination) if ts.destination else '',
            'extractor': ts.extractor,
            'description': ts.description or '',
            'additional_params': json.dumps(ts.additional_params) if ts.additional_params else '{}',
            'symbol': ts.symbol or '',
            'active': ts.active
        }
        ts_data.append(ts_dict)
    
    # Group by venue
    venues = {}
    for ts in time_series:
        venue = ts.venue
        if venue not in venues:
            venues[venue] = []
        venues[venue].append(ts)
    
    # Group by data_source
    sources = {}
    for ts in time_series:
        source = ts.data_source
        if source not in sources:
            sources[source] = []
        sources[source].append(ts)
    
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Service Configuration Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: #f5f7fa;
            padding: 20px;
            color: #2c3e50;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .header p {{
            font-size: 1.2em;
            opacity: 0.9;
            margin-bottom: 20px;
        }}
        
        .header .stats {{
            display: flex;
            gap: 30px;
            margin-top: 20px;
            flex-wrap: wrap;
            justify-content: center;
        }}
        
        .stat-card {{
            background: rgba(255,255,255,0.2);
            padding: 15px 25px;
            border-radius: 8px;
            backdrop-filter: blur(10px);
        }}
        
        .stat-card .label {{
            font-size: 0.9em;
            opacity: 0.9;
        }}
        
        .stat-card .value {{
            font-size: 2em;
            font-weight: bold;
            margin-top: 5px;
        }}
        
        .controls {{
            background: white;
            padding: 25px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        .search-box {{
            width: 100%;
            padding: 12px 20px;
            font-size: 16px;
            border: 2px solid #e1e8ed;
            border-radius: 8px;
            margin-bottom: 20px;
            transition: border-color 0.3s;
        }}
        
        .search-box:focus {{
            outline: none;
            border-color: #667eea;
        }}
        
        .filters {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 15px;
        }}
        
        .filter-group {{
            display: flex;
            flex-direction: column;
        }}
        
        .filter-group label {{
            font-weight: 600;
            margin-bottom: 5px;
            font-size: 0.9em;
            color: #555;
        }}
        
        .filter-group select {{
            padding: 10px;
            border: 2px solid #e1e8ed;
            border-radius: 6px;
            font-size: 14px;
            background: white;
            cursor: pointer;
            transition: border-color 0.3s;
        }}
        
        .filter-group select:focus {{
            outline: none;
            border-color: #667eea;
        }}
        
        .reset-btn {{
            padding: 10px 20px;
            background: #667eea;
            color: white;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
            transition: background 0.3s;
            margin-top: 10px;
        }}
        
        .reset-btn:hover {{
            background: #5568d3;
        }}
        
        .section {{
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        .tabs-header {{
            display: flex;
            align-items: center;
            gap: 30px;
            margin-bottom: 20px;
            border-bottom: 2px solid #e1e8ed;
            padding-bottom: 10px;
        }}
        
        .tab-group-title {{
            font-size: 1.2em;
            font-weight: 600;
            color: #2c3e50;
            padding: 10px 0;
        }}
        
        .tab-group-title.active {{
            color: #667eea;
            border-bottom: 3px solid #667eea;
            margin-bottom: -12px;
        }}
        
        .tabs-container {{
            width: 100%;
        }}
        
        .tabs {{
            display: flex;
            flex-wrap: wrap;
            gap: 5px;
            margin-bottom: 20px;
        }}
        
        .tab {{
            padding: 12px 24px;
            background: #f8f9fa;
            border: 2px solid #e1e8ed;
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
            color: #666;
            border-radius: 8px;
            transition: all 0.3s;
            position: relative;
        }}
        
        .tab:hover {{
            background: #e9ecef;
            color: #333;
            border-color: #667eea;
        }}
        
        .tab.active {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-color: #667eea;
            box-shadow: 0 2px 8px rgba(102, 126, 234, 0.4);
        }}
        
        .tab .count {{
            display: inline-block;
            background: rgba(0,0,0,0.15);
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.85em;
            margin-left: 8px;
        }}
        
        .tab.active .count {{
            background: rgba(255,255,255,0.3);
        }}
        
        .tab-content {{
            display: none;
        }}
        
        .tab-content.active {{
            display: block;
            animation: fadeIn 0.3s;
        }}
        
        @keyframes fadeIn {{
            from {{
                opacity: 0;
                transform: translateY(10px);
            }}
            to {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}
        
        .table-wrapper {{
            max-height: 600px;
            overflow: auto;
            border: 1px solid #e1e8ed;
            border-radius: 8px;
            position: relative;
        }}
        
        .table-wrapper::-webkit-scrollbar {{
            width: 10px;
            height: 10px;
        }}
        
        .table-wrapper::-webkit-scrollbar-track {{
            background: #f1f1f1;
            border-radius: 10px;
        }}
        
        .table-wrapper::-webkit-scrollbar-thumb {{
            background: #888;
            border-radius: 10px;
        }}
        
        .table-wrapper::-webkit-scrollbar-thumb:hover {{
            background: #555;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            table-layout: auto;
        }}
        
        th {{
            background: #f8f9fa;
            padding: 12px 8px;
            text-align: left;
            font-weight: 600;
            color: #555;
            border-bottom: 2px solid #e1e8ed;
            position: sticky;
            top: 0;
            white-space: nowrap;
            overflow: hidden;
            user-select: none;
            z-index: 10;
            position: relative;
        }}
        
        th:hover {{
            background: #e9ecef;
        }}
        
        th .resize-handle {{
            position: absolute;
            right: 0;
            top: 0;
            bottom: 0;
            width: 8px;
            cursor: col-resize;
            user-select: none;
            z-index: 1;
        }}
        
        th .resize-handle:hover {{
            background: #667eea;
            opacity: 0.5;
        }}
        
        th.resizing {{
            background: #e9ecef;
        }}
        
        th.resizing .resize-handle {{
            background: #667eea;
            opacity: 0.8;
        }}
        
        td {{
            padding: 12px 8px;
            border-bottom: 1px solid #e1e8ed;
            overflow: hidden;
            text-overflow: ellipsis;
        }}
        
        tr:hover {{
            background: #f8f9fa;
        }}
        
        tr:last-child td {{
            border-bottom: none;
        }}
        
        .col-service-id {{ min-width: 80px; width: 80px; }}
        .col-series-id {{ min-width: 150px; width: 150px; }}
        .col-type {{ min-width: 100px; width: 100px; }}
        .col-root-id {{ min-width: 120px; width: 120px; }}
        .col-venue {{ min-width: 100px; width: 100px; }}
        .col-schema {{ min-width: 120px; width: 120px; }}
        .col-source {{ min-width: 120px; width: 120px; }}
        .col-destination {{ min-width: 150px; width: 150px; }}
        .col-extractor {{ min-width: 100px; width: 100px; }}
        .col-description {{ min-width: 250px; width: 250px; }}
        .col-symbol {{ min-width: 100px; width: 100px; }}
        .col-params {{ min-width: 150px; width: 150px; }}
        .col-status {{ min-width: 80px; width: 80px; }}
        
        .badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 0.85em;
            font-weight: 600;
            text-transform: uppercase;
        }}
        
        .badge-active {{
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }}
        
        .badge-inactive {{
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }}
        
        .json-preview {{
            font-family: 'Courier New', monospace;
            font-size: 0.85em;
            background: #f8f9fa;
            padding: 6px 10px;
            border-radius: 4px;
            border: 1px solid #e1e8ed;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            cursor: help;
            display: block;
        }}
        
        .json-preview:hover {{
            background: #e9ecef;
        }}
        
        .series-id {{
            font-weight: 600;
            color: #667eea;
        }}
        
        .service-id {{
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
            color: #666;
        }}
        
        .extractor-type {{
            padding: 3px 8px;
            border-radius: 3px;
            font-size: 0.85em;
            font-weight: 500;
        }}
        
        .extractor-realtime {{
            background: #e3f2fd;
            color: #1565c0;
            border: 1px solid #bbdefb;
        }}
        
        .extractor-historical {{
            background: #f3e5f5;
            color: #6a1b9a;
            border: 1px solid #e1bee7;
        }}
        
        .venue-badge {{
            padding: 3px 8px;
            border-radius: 3px;
            font-size: 0.85em;
            font-weight: 500;
            background: #fff3e0;
            color: #e65100;
            border: 1px solid #ffe0b2;
        }}
        
        .source-badge {{
            padding: 3px 8px;
            border-radius: 3px;
            font-size: 0.85em;
            font-weight: 500;
            background: #e8f5e9;
            color: #2e7d32;
            border: 1px solid #c8e6c9;
        }}
        
        .no-results {{
            text-align: center;
            padding: 40px;
            color: #888;
            font-size: 1.1em;
        }}
        
        .empty-tab {{
            text-align: center;
            padding: 60px 20px;
            color: #888;
        }}
        
        .empty-tab-icon {{
            font-size: 3em;
            margin-bottom: 10px;
        }}
        
        .group-switcher {{
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
            border-bottom: 2px solid #e1e8ed;
        }}
        
        .group-btn {{
            padding: 12px 24px;
            background: transparent;
            border: none;
            cursor: pointer;
            font-size: 1.1em;
            font-weight: 600;
            color: #666;
            border-bottom: 3px solid transparent;
            margin-bottom: -2px;
            transition: all 0.3s;
        }}
        
        .group-btn:hover {{
            color: #667eea;
        }}
        
        .group-btn.active {{
            color: #667eea;
            border-bottom-color: #667eea;
        }}
        
        @media (max-width: 768px) {{
            .header h1 {{
                font-size: 1.8em;
            }}
            
            .filters {{
                grid-template-columns: 1fr;
            }}
            
            .tabs {{
                overflow-x: auto;
                flex-wrap: nowrap;
            }}
            
            .tab {{
                padding: 10px 16px;
                font-size: 13px;
                white-space: nowrap;
            }}
            
            table {{
                font-size: 0.85em;
            }}
            
            .table-wrapper {{
                max-height: 400px;
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Time Series Configuration Dashboard</h1>
        <p>Realtime and Historical Data Service</p>
        <div class="stats">
            <div class="stat-card">
                <div class="label">Total Series</div>
                <div class="value" id="totalCount">{len(time_series)}</div>
            </div>
            <div class="stat-card">
                <div class="label">Venues</div>
                <div class="value">{len(venues)}</div>
            </div>
            <div class="stat-card">
                <div class="label">Data Sources</div>
                <div class="value">{len(sources)}</div>
            </div>
            <div class="stat-card">
                <div class="label">Active</div>
                <div class="value">{sum(1 for ts in time_series if ts.active)}</div>
            </div>
        </div>
    </div>
    
    <div class="controls">
        <input type="text" id="searchBox" class="search-box" placeholder="Search across all fields...">
        
        <div class="filters">
            <div class="filter-group">
                <label>Venue</label>
                <select id="filterVenue">
                    <option value="">All Venues</option>
                </select>
            </div>
            <div class="filter-group">
                <label>Data Source</label>
                <select id="filterSource">
                    <option value="">All Sources</option>
                </select>
            </div>
            <div class="filter-group">
                <label>Series Type</label>
                <select id="filterSeriesType">
                    <option value="">All Types</option>
                </select>
            </div>
            <div class="filter-group">
                <label>Extractor</label>
                <select id="filterExtractor">
                    <option value="">All Extractors</option>
                </select>
            </div>
            <div class="filter-group">
                <label>Data Schema</label>
                <select id="filterSchema">
                    <option value="">All Schemas</option>
                </select>
            </div>
            <div class="filter-group">
                <label>Status</label>
                <select id="filterActive">
                    <option value="">All Status</option>
                    <option value="true">Active</option>
                    <option value="false">Inactive</option>
                </select>
            </div>
        </div>
        
        <button class="reset-btn" onclick="resetFilters()">Reset All Filters</button>
    </div>
    
    <div class="section">
        <div class="group-switcher">
            <button class="group-btn active" id="btnVenue" onclick="switchGroup('venue')">Grouped by Venue</button>
            <button class="group-btn" id="btnSource" onclick="switchGroup('source')">Grouped by Data Source</button>
        </div>
        
        <div class="tabs-container">
            <div class="tabs" id="tabsBar"></div>
            <div id="tabsContent"></div>
        </div>
    </div>
    
    <script>
        const timeSeriesData = {json.dumps(ts_data, indent=2)};
        let currentGroup = 'venue';
        let currentTab = null;
        
        function populateFilters() {{
            const venues = new Set();
            const sources = new Set();
            const seriesTypes = new Set();
            const extractors = new Set();
            const schemas = new Set();
            
            timeSeriesData.forEach(ts => {{
                venues.add(ts.venue);
                sources.add(ts.data_source);
                seriesTypes.add(ts.series_type);
                extractors.add(ts.extractor);
                schemas.add(ts.data_schema);
            }});
            
            populateSelect('filterVenue', Array.from(venues).sort());
            populateSelect('filterSource', Array.from(sources).sort());
            populateSelect('filterSeriesType', Array.from(seriesTypes).sort());
            populateSelect('filterExtractor', Array.from(extractors).sort());
            populateSelect('filterSchema', Array.from(schemas).sort());
        }}
        
        function populateSelect(id, options) {{
            const select = document.getElementById(id);
            options.forEach(option => {{
                const opt = document.createElement('option');
                opt.value = option;
                opt.textContent = option;
                select.appendChild(opt);
            }});
        }}
        
        function filterData() {{
            const searchTerm = document.getElementById('searchBox').value.toLowerCase();
            const venue = document.getElementById('filterVenue').value;
            const source = document.getElementById('filterSource').value;
            const seriesType = document.getElementById('filterSeriesType').value;
            const extractor = document.getElementById('filterExtractor').value;
            const schema = document.getElementById('filterSchema').value;
            const active = document.getElementById('filterActive').value;
            
            let filtered = timeSeriesData.filter(ts => {{
                const searchMatch = !searchTerm || Object.values(ts).some(val => 
                    String(val).toLowerCase().includes(searchTerm)
                );
                
                const venueMatch = !venue || ts.venue === venue;
                const sourceMatch = !source || ts.data_source === source;
                const typeMatch = !seriesType || ts.series_type === seriesType;
                const extractorMatch = !extractor || ts.extractor === extractor;
                const schemaMatch = !schema || ts.data_schema === schema;
                const activeMatch = !active || String(ts.active) === active;
                
                return searchMatch && venueMatch && sourceMatch && typeMatch && 
                       extractorMatch && schemaMatch && activeMatch;
            }});
            
            document.getElementById('totalCount').textContent = filtered.length;
            renderTabs(filtered);
        }}
        
        function switchGroup(group) {{
            currentGroup = group;
            document.getElementById('btnVenue').classList.toggle('active', group === 'venue');
            document.getElementById('btnSource').classList.toggle('active', group === 'source');
            filterData();
        }}
        
        function renderTabs(data) {{
            const groupKey = currentGroup === 'venue' ? 'venue' : 'data_source';
            const grouped = {{}};
            
            data.forEach(ts => {{
                const key = ts[groupKey];
                if (!grouped[key]) grouped[key] = [];
                grouped[key].push(ts);
            }});
            
            const tabsBar = document.getElementById('tabsBar');
            const tabsContent = document.getElementById('tabsContent');
            
            tabsBar.innerHTML = '';
            tabsContent.innerHTML = '';
            
            if (Object.keys(grouped).length === 0) {{
                tabsContent.innerHTML = '<div class="no-results">No results found</div>';
                return;
            }}
            
            const sortedKeys = Object.keys(grouped).sort();
            
            sortedKeys.forEach((key, index) => {{
                const group = grouped[key];
                const tabButton = document.createElement('button');
                tabButton.className = 'tab' + (index === 0 ? ' active' : '');
                tabButton.innerHTML = `
                    ${{key}}
                    <span class="count">${{group.length}}</span>
                `;
                tabButton.onclick = () => switchTab(key);
                tabsBar.appendChild(tabButton);
                
                const tabContent = document.createElement('div');
                tabContent.className = 'tab-content' + (index === 0 ? ' active' : '');
                tabContent.id = `tab-${{key}}`;
                
                if (group.length === 0) {{
                    tabContent.innerHTML = `
                        <div class="empty-tab">
                            <div class="empty-tab-icon">📭</div>
                            <div>No time series found</div>
                        </div>
                    `;
                }} else {{
                    tabContent.innerHTML = `
                        <div class="table-wrapper">
                            <table>
                                <thead>
                                    <tr>
                                        <th class="col-service-id">Service ID<div class="resize-handle"></div></th>
                                        <th class="col-series-id">Series ID<div class="resize-handle"></div></th>
                                        <th class="col-type">Type<div class="resize-handle"></div></th>
                                        <th class="col-root-id">Root ID<div class="resize-handle"></div></th>
                                        <th class="col-venue">Venue<div class="resize-handle"></div></th>
                                        <th class="col-schema">Schema<div class="resize-handle"></div></th>
                                        <th class="col-source">Source<div class="resize-handle"></div></th>
                                        <th class="col-destination">Destination<div class="resize-handle"></div></th>
                                        <th class="col-extractor">Extractor<div class="resize-handle"></div></th>
                                        <th class="col-description">Description<div class="resize-handle"></div></th>
                                        <th class="col-symbol">Symbol<div class="resize-handle"></div></th>
                                        <th class="col-params">Additional Params<div class="resize-handle"></div></th>
                                        <th class="col-status">Status<div class="resize-handle"></div></th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${{group.map(ts => `
                                        <tr>
                                            <td class="service-id">${{ts.service_id}}</td>
                                            <td><span class="series-id">${{ts.series_id}}</span></td>
                                            <td>${{ts.series_type.toUpperCase()}}</td>
                                            <td>${{ts.root_id}}</td>
                                            <td><span class="venue-badge">${{ts.venue}}</span></td>
                                            <td>${{ts.data_schema}}</td>
                                            <td><span class="source-badge">${{ts.data_source}}</span></td>
                                            <td>${{ts.destination}}</td>
                                            <td><span class="extractor-type extractor-${{ts.extractor}}">${{ts.extractor}}</span></td>
                                            <td>${{ts.description}}</td>
                                            <td>${{ts.symbol || '-'}}</td>
                                            <td><span class="json-preview" title="${{ts.additional_params}}">${{ts.additional_params}}</span></td>
                                            <td><span class="badge ${{ts.active ? 'badge-active' : 'badge-inactive'}}">${{ts.active ? 'Active' : 'Inactive'}}</span></td>
                                        </tr>
                                    `).join('')}}
                                </tbody>
                            </table>
                        </div>
                    `;
                }}
                
                tabsContent.appendChild(tabContent);
            }});
            
            currentTab = sortedKeys[0];
            
            // Initialize column resizing after rendering
            initColumnResize();
        }}
        
        function initColumnResize() {{
            const tables = document.querySelectorAll('table');
            
            tables.forEach(table => {{
                const cols = table.querySelectorAll('th');
                
                cols.forEach((col, colIndex) => {{
                    const resizeHandle = col.querySelector('.resize-handle');
                    if (!resizeHandle) return;
                    
                    let startX, startWidth;
                    
                    const onMouseDown = (e) => {{
                        e.preventDefault();
                        e.stopPropagation();
                        
                        startX = e.pageX;
                        startWidth = col.offsetWidth;
                        
                        col.classList.add('resizing');
                        document.body.style.cursor = 'col-resize';
                        document.body.style.userSelect = 'none';
                        
                        document.addEventListener('mousemove', onMouseMove);
                        document.addEventListener('mouseup', onMouseUp);
                    }};
                    
                    const onMouseMove = (e) => {{
                        const diff = e.pageX - startX;
                        const newWidth = Math.max(50, startWidth + diff);
                        
                        col.style.width = newWidth + 'px';
                        col.style.minWidth = newWidth + 'px';
                        
                        // Apply same width to all cells in this column
                        const rows = table.querySelectorAll('tr');
                        rows.forEach(row => {{
                            const cell = row.children[colIndex];
                            if (cell) {{
                                cell.style.width = newWidth + 'px';
                                cell.style.minWidth = newWidth + 'px';
                            }}
                        }});
                    }};
                    
                    const onMouseUp = () => {{
                        col.classList.remove('resizing');
                        document.body.style.cursor = '';
                        document.body.style.userSelect = '';
                        
                        document.removeEventListener('mousemove', onMouseMove);
                        document.removeEventListener('mouseup', onMouseUp);
                    }};
                    
                    resizeHandle.addEventListener('mousedown', onMouseDown);
                }});
            }});
        }}
        
        function switchTab(tabKey) {{
            const tabs = document.getElementById('tabsBar').querySelectorAll('.tab');
            tabs.forEach(tab => {{
                if (tab.textContent.trim().startsWith(tabKey)) {{
                    tab.classList.add('active');
                }} else {{
                    tab.classList.remove('active');
                }}
            }});
            
            const contents = document.getElementById('tabsContent').querySelectorAll('.tab-content');
            contents.forEach(content => {{
                if (content.id === `tab-${{tabKey}}`) {{
                    content.classList.add('active');
                }} else {{
                    content.classList.remove('active');
                }}
            }});
            
            currentTab = tabKey;
        }}
        
        function resetFilters() {{
            document.getElementById('searchBox').value = '';
            document.getElementById('filterVenue').value = '';
            document.getElementById('filterSource').value = '';
            document.getElementById('filterSeriesType').value = '';
            document.getElementById('filterExtractor').value = '';
            document.getElementById('filterSchema').value = '';
            document.getElementById('filterActive').value = '';
            filterData();
        }}
        
        // Event listeners
        document.getElementById('searchBox').addEventListener('input', filterData);
        document.getElementById('filterVenue').addEventListener('change', filterData);
        document.getElementById('filterSource').addEventListener('change', filterData);
        document.getElementById('filterSeriesType').addEventListener('change', filterData);
        document.getElementById('filterExtractor').addEventListener('change', filterData);
        document.getElementById('filterSchema').addEventListener('change', filterData);
        document.getElementById('filterActive').addEventListener('change', filterData);
        
        // Initialize
        populateFilters();
        renderTabs(timeSeriesData);
    </script>
</body>
</html>
"""
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    logger.info(f"✅ Dashboard generated: {output_path.absolute()}")
