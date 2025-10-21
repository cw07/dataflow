import argparse

import sys
import yaml
import logging
from pathlib import Path

from datacore.models.assets import AssetType

from dataflow.config.loaders.fx_spec import fx_specs
from dataflow.config.loaders.spread_spec import spread_specs
from dataflow.config.loaders.equity_spec import equity_specs
from dataflow.config.loaders.fut_spec import futures_specs
from dataflow.config.loaders.fwd_spec import fwd_specs
from dataflow.config.loaders.index_spec import index_specs

from dataflow.config.loaders.pipelines import pipeline_specs
from dataflow.config.loaders.time_series import TimeSeriesConfig
from dataflow.utils.yaml_to_html import time_series_html

logger = logging.getLogger(__name__)


def parse_arguments(args):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--serialization",
        choices=["yaml", "csv"],
    )
    parser.add_argument(
        "--generate-html",
        action="store_true",
        default=True,
    )
    args = parser.parse_args(args)
    return args


def gen_fut_spec(total_time_series: int, time_series: list[TimeSeriesConfig]):
    term_in_word = {1: "1st", 2: "2nd", 3: "3rd"}
    for root_id, fut_spec in futures_specs.specs.items():
        pipelines = pipeline_specs.get_spec(root_id)
        if not pipelines:
            continue
        for pipeline in pipelines:
            for i in range(1, fut_spec.terms+1):
                ts = TimeSeriesConfig(
                    service_id=total_time_series,
                    series_id=f"{root_id}.{i}",
                    series_type=AssetType.FUT,
                    root_id=root_id,
                    venue=root_id.split(".")[1],
                    data_schema=pipeline.schema,
                    data_source=pipeline.source,
                    destination=pipeline.output,
                    extractor=pipeline.extractor,
                    description=term_in_word.get(i, str(i)+"th") + " " + fut_spec.description,
                    additional_params=pipeline.params,
                    active=fut_spec.active
                )
                time_series.append(ts)
                total_time_series += 1
    return total_time_series, time_series


def gen_fx_spec(total_time_series: int, time_series: list[TimeSeriesConfig]):
    for root_id, fx_spec in fx_specs.specs.items():
        pipelines = pipeline_specs.get_spec(root_id)
        if not pipelines:
            continue
        for pipeline in pipelines:
            ts = TimeSeriesConfig(
                service_id=total_time_series,
                series_id=f"{root_id}",
                series_type=AssetType.FX,
                root_id=root_id,
                venue="GLOBAL",
                data_schema=pipeline.schema,
                data_source=pipeline.source,
                destination=pipeline.output,
                extractor=pipeline.extractor,
                description=fx_spec.description,
                additional_params=pipeline.params,
                active=fx_spec.active
            )
            time_series.append(ts)
            total_time_series += 1
    return total_time_series, time_series


def gen_index_spec(total_time_series: int, time_series: list[TimeSeriesConfig]):
    for root_id, index_spec in index_specs.specs.items():
        pipelines = pipeline_specs.get_spec(root_id)
        if not pipelines:
            continue
        for pipeline in pipelines:
            ts = TimeSeriesConfig(
                service_id=total_time_series,
                series_id=f"{root_id}",
                series_type=AssetType.INDEX,
                root_id=root_id,
                venue=root_id.split(".")[1],
                data_schema=pipeline.schema,
                data_source=pipeline.source,
                destination=pipeline.output,
                extractor=pipeline.extractor,
                description=index_spec.description,
                additional_params=pipeline.params,
                active=index_spec.active
            )
            time_series.append(ts)
            total_time_series += 1
    return total_time_series, time_series


def get_fwd_spec(total_time_series: int, time_series: list[TimeSeriesConfig]):
    for root_id, fwd_spec in fwd_specs.specs.items():
        pipelines = pipeline_specs.get_spec(root_id)
        if not pipelines:
            continue
        for pipeline in pipelines:
            ts = TimeSeriesConfig(
                service_id=total_time_series,
                series_id=f"{root_id}",
                series_type=AssetType.FWD,
                root_id=root_id,
                venue=root_id.split(".")[1],
                data_schema=pipeline.schema,
                data_source=pipeline.source,
                destination=pipeline.output,
                extractor=pipeline.extractor,
                description=fwd_spec.description,
                additional_params=pipeline.params,
                active=fwd_spec.active
            )
            time_series.append(ts)
            total_time_series += 1
    return total_time_series, time_series


def gen_spread_spec(total_time_series: int, time_series: list[TimeSeriesConfig]):
    for root_id, spread_spec in spread_specs.specs.items():
        pipelines = pipeline_specs.get_spec(root_id)
        if not pipelines:
            continue
        pass

    return total_time_series, time_series


def gen_equity_spec(total_time_series: int, time_series: list[TimeSeriesConfig]):
    for equity_spec in equity_specs.specs.items():
        pass

    return total_time_series, time_series


def main(args):
    args = parse_arguments(args)
    time_series = []
    total_time_series = 1
    total_time_series, time_series = gen_fut_spec(total_time_series, time_series)
    total_time_series, time_series = gen_index_spec(total_time_series, time_series)
    total_time_series, time_series = gen_fx_spec(total_time_series, time_series)
    total_time_series, time_series = get_fwd_spec(total_time_series, time_series)
    total_time_series, time_series = gen_equity_spec(total_time_series, time_series)
    total_time_series, time_series = gen_spread_spec(total_time_series, time_series)
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
        "--serialization", "yaml",
        "--generate-html"
    ]

    main(gen_args)