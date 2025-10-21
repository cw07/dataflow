import yaml
import json
from pathlib import Path
from datetime import datetime


class YamlToHtmlTable:
    def __init__(self, yaml_file_path: str, output_html_path: str):
        self.yaml_file_path = Path(yaml_file_path)
        self.output_html_path = Path(output_html_path)
        self.data = []

    def load_yaml_data(self):
        """Load YAML data from file."""
        if not self.yaml_file_path.exists():
            raise FileNotFoundError(f"YAML file not found: {self.yaml_file_path.absolute()}")

        with open(self.yaml_file_path, "r", encoding="utf-8") as f:
            self.data = yaml.safe_load(f) or []

    @staticmethod
    def format_value(value):
        """Pretty format a value for HTML display."""
        if isinstance(value, list):
            return ", ".join(str(item) for item in value)
        elif isinstance(value, dict):
            return f'<pre class="json">{json.dumps(value, indent=2)}</pre>'
        elif value is None:
            return '<em class="null">null</em>'
        else:
            return str(value)

    def generate_html(self):
        """Generate HTML page with filterable table from YAML data."""
        self.data.sort(key=lambda x: x.get("service_id", ""))

        # Generate table headers and filters
        headers = [
            "Service ID", "Series ID", "Type", "Root ID", "Venue",
            "Data Schema", "Data Source", "Destination", "Extractor",
            "Description", "Additional Params", "Symbol", "Active"
        ]

        # Mapping from header to data key (in order)
        keys = [
            "service_id", "series_id", "series_type", "root_id", "venue",
            "data_schema", "data_source", "destination", "extractor",
            "description", "additional_params", "symbol", "active"
        ]

        # Build filter inputs
        filter_inputs = ""
        for key in keys:
            filter_inputs += f'<th><input type="text" class="filter-input" data-column="{key}" placeholder="Filter {key.replace("_", " ").title()}..." /></th>'

        # Build table rows
        table_rows = ""
        for item in self.data:
            service_id = item.get("service_id", "")
            series_id = item.get("series_id", "")
            series_type = item.get("series_type", "")
            root_id = item.get("root_id", "")
            venue = item.get("venue", "")
            data_schema = item.get("data_schema", "")
            data_source = item.get("data_source", "")
            destination = self.format_value(item.get("destination", ""))
            extractor = item.get("extractor", "")
            description = item.get("description", "")
            additional_params = self.format_value(item.get("additional_params", {}))
            symbol = self.format_value(item.get("symbol", ""))
            active = "✅ Yes" if item.get("active", False) else "❌ No"

            table_rows += f"""
            <tr data-service-id="{service_id}" data-series-id="{series_id}" 
                data-series-type="{series_type}" data-root-id="{root_id}" data-venue="{venue}"
                data-data-schema="{data_schema}" data-data-source="{data_source}"
                data-extractor="{extractor}" data-description="{description}"
                data-symbol="{symbol}" data-active="{'yes' if item.get('active') else 'no'}">
                <td>{service_id}</td>
                <td>{series_id}</td>
                <td>{series_type}</td>
                <td>{root_id}</td>
                <td>{venue}</td>
                <td>{data_schema}</td>
                <td>{data_source}</td>
                <td>{destination}</td>
                <td>{extractor}</td>
                <td>{description}</td>
                <td>{additional_params}</td>
                <td>{symbol}</td>
                <td>{active}</td>
            </tr>
            """

        # Get last modified time of YAML file
        try:
            mtime = datetime.fromtimestamp(self.yaml_file_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        except:
            mtime = "Unknown"

        # JavaScript for filtering
        filter_script = """
        <script>
            document.addEventListener("DOMContentLoaded", function () {
                const filterInputs = document.querySelectorAll(".filter-input");
                const tableRows = document.querySelectorAll("tbody tr");

                filterInputs.forEach(input => {
                    input.addEventListener("keyup", function () {
                        const column = this.getAttribute("data-column");
                        const filterValue = this.value.toLowerCase();

                        tableRows.forEach(row => {
                            const cellValue = row.getAttribute(`data-${column.replace('_', '-')}`) || "";
                            if (cellValue.toLowerCase().includes(filterValue)) {
                                row.style.display = "";
                            } else {
                                row.style.display = "none";
                            }
                        });

                        // Optional: Re-check visibility based on *all* filters
                        // But for simplicity, we use per-column OR filtering above.
                    });
                });
            });
        </script>
        """

        # HTML template with inline CSS and JS
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
    <title>Time Series Configuration</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f9f9fc;
            color: #1a1a1a;
            padding: 20px;
            line-height: 1.6;
        }}
        h1 {{
            color: #2c3e50;
            text-align: center;
            margin-bottom: 10px;
        }}
        .subtitle {{
            text-align: center;
            color: #7f8c8d;
            font-style: italic;
            margin-bottom: 30px;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}
        th {{
            background-color: #34495e;
            color: white;
            padding: 12px 10px;
            text-align: left;
            position: sticky;
            top: 0;
            z-index: 10;
        }}
        /* Style for filter inputs */
        th input.filter-input {{
            width: 100%;
            padding: 6px;
            margin: 4px 0;
            border: 1px solid rgba(255,255,255,0.3);
            border-radius: 4px;
            background: rgba(255,255,255,0.2);
            color: white;
            box-sizing: border-box;
        }}
        th input.filter-input::placeholder {{
            color: rgba(255,255,255,0.7);
        }}
        td {{
            padding: 10px;
            border-bottom: 1px solid #ddd;
            vertical-align: top;
        }}
        tr:nth-child(even) {{
            background-color: #f2f5f8;
        }}
        tr:hover {{
            background-color: #e6f7ff;
            transition: background-color 0.2s;
        }}
        pre.json {{
            margin: 0;
            padding: 8px;
            background: #2c3e5e;
            color: #ecf0f1;
            border-radius: 4px;
            overflow-x: auto;
            font-family: 'Courier New', monospace;
            font-size: 12px;
        }}
        .null {{
            color: #bdc3c7;
            font-style: italic;
        }}
        footer {{
            text-align: center;
            margin-top: 30px;
            color: #95a5a6;
            font-size: 12px;
        }}
        @media (max-width: 768px) {{
            th, td {{
                padding: 6px;
                font-size: 12px;
            }}
            th input.filter-input {{
                padding: 4px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Time Series Configuration</h1>
        <p class="subtitle">Total Entries: {len(self.data)}</p>
        <table>
            <thead>
                <tr>
                    <th>Service ID</th>
                    <th>Series ID</th>
                    <th>Type</th>
                    <th>Root ID</th>
                    <th>Venue</th>
                    <th>Data Schema</th>
                    <th>Data Source</th>
                    <th>Destination</th>
                    <th>Extractor</th>
                    <th>Description</th>
                    <th>Additional Params</th>
                    <th>Symbol</th>
                    <th>Active</th>
                </tr>
                <tr>
                    {filter_inputs}
                </tr>
            </thead>
            <tbody>
                {table_rows}
            </tbody>
        </table>
    </div>
    <footer>Last generated on {mtime} (from YAML file modification time)</footer>
    {filter_script}
</body>
</html>
        """

        # Write HTML file
        with open(self.output_html_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"✅ HTML report generated: {self.output_html_path}")

    def generate(self):
        """Main method to run the full process."""
        print(f"📄 Loading YAML from {self.yaml_file_path}...")
        try:
            self.load_yaml_data()
            print(f"✔️  Loaded {len(self.data)} time series entries.")
        except Exception as e:
            print(f"❌ Failed to parse YAML: {e}")
            raise

        print(f"🎨 Generating HTML...")
        self.generate_html()
