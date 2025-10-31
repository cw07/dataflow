import argparse

import sys
import yaml
import logging
from pathlib import Path

from dataflow.utils.yaml_to_html import time_series_html
from dataflow.config.loaders.manager import gen_all_spec

logger = logging.getLogger(__name__)


def parse_arguments(args):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--serialization",
        default=None,
        choices=["yaml", "csv"],
    )
    parser.add_argument(
        "--generate-html",
        action="store_true"
    )
    args = parser.parse_args(args)
    return args


def main(args):
    args = parse_arguments(args)
    time_series = gen_all_spec()
    if args.serialization:
        serialization(time_series, output_type=args.serialization)
    if args.generate_html:
        time_series_html(time_series, output_path=Path("./time_series.html"))


def serialization(time_series, output_type: str):
    if output_type == "yaml":
        output_file = Path("./time_series.yaml")
        time_series_data = []
        for ts in time_series:
            d = ts.model_dump()
            d['data_schema'] = d['data_schema'].value

            if d['additional_params'] is not None:
                d['additional_params'] = dict(d['additional_params'])

            time_series_data.append(d)

        with open(output_file, "w", encoding="utf-8") as f:
            f.write("# This YAML file is auto-generated. Do not edit manually.\n\n")
            yaml.dump(time_series_data, f, indent=2, default_flow_style=False, sort_keys=False)
    elif output_type == "csv":
        pass
    else:
        raise NotImplementedError(f"Output type {output_type} is not implemented.")

def clmain():
    main(sys.argv[1:])


if __name__ == '__main__':
    gen_args = [
    ]

    main(gen_args)