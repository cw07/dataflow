from dataflow.utils.yaml_to_html import YamlToHtmlTable


if __name__ == "__main__":
    yaml_path = "./time_series.yaml"
    html_output_path = "./time_series_config.html"
    generator = YamlToHtmlTable(yaml_path, html_output_path)
    generator.generate()