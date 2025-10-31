import argparse

import sys
import yaml
import logging
from pathlib import Path

from datacore.models.assets.asset_type import AssetType
from datacore.models.assets import Index, Forward, FXSpot, Futures, FuturesOptions, BaseFutures, TradingHours

from dataflow.config.loaders.fx_spec import fx_specs
from dataflow.config.loaders.spread_spec import spread_specs
from dataflow.config.loaders.equity_spec import equity_specs
from dataflow.config.loaders.fut_spec import futures_specs
from dataflow.config.loaders.futopt_spec import futures_opt_specs
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


def gen_fut_spec(num_time_series: int, time_series: list[TimeSeriesConfig]):
    for root_id, fut_spec in futures_specs.specs.items():
        base_future = BaseFutures(
            dflow_id=root_id,
            venue=fut_spec.venue,
            terms=fut_spec.terms,
            contract_size=fut_spec.contract_size,
            hours=TradingHours(time_zone=fut_spec.time_zone,
                               open_time_local=fut_spec.open_time_local,
                               close_time_local=fut_spec.close_time_local,
                               days=fut_spec.trading_days
                               ),
            contract_months=fut_spec.contract_months,
            description=fut_spec.description,
            category=fut_spec.category
        )
        pipelines = pipeline_specs.get_spec(root_id)
        if not pipelines:
            logger.warning(f"No pipeline for {root_id}")
            continue
        for pipeline in pipelines:
            for i in range(1, fut_spec.terms+1):
                futures = Futures(
                    dflow_id=f"{root_id}.{i}",
                    parent=base_future,
                    term=i
                )
                ts = TimeSeriesConfig(
                    service_id=num_time_series,
                    asset=futures,
                    data_schema=pipeline.schema,
                    data_source=pipeline.source,
                    destination=pipeline.output,
                    extractor=pipeline.extractor,
                    additional_params=pipeline.params,
                    active=fut_spec.active
                )
                time_series.append(ts)
                num_time_series += 1
    return num_time_series, time_series


def gen_fut_opt_spec(num_time_series: int, time_series: list[TimeSeriesConfig]):
    for root_id, fut_opt_spec in futures_opt_specs.specs.items():
        fut_spec = futures_specs.specs[fut_opt_spec.parent]
        base_future = BaseFutures(
            dflow_id=fut_spec.root_id,
            venue=fut_spec.venue,
            terms=fut_spec.terms,
            contract_size=fut_spec.contract_size,
            hours=TradingHours(time_zone=fut_spec.time_zone,
                               open_time_local=fut_spec.open_time_local,
                               close_time_local=fut_spec.close_time_local,
                               days=fut_spec.trading_days
                               ),
            contract_months=fut_spec.contract_months,
            description=fut_spec.description,
            category=fut_spec.category
        )
        pipelines = pipeline_specs.get_spec(root_id)
        if not pipelines:
            logger.warning(f"No pipeline for {root_id}")
            continue
        for pipeline in pipelines:
            for i in range(1, fut_opt_spec.terms + 1):
                futures = Futures(
                    dflow_id=f"{base_future.dflow_id}.{i}",
                    parent=base_future,
                    term=i
                )
                futures_option = FuturesOptions(
                    dflow_id=f"{root_id}.{i}",
                    term=i,
                    parent=futures
                )
                ts = TimeSeriesConfig(
                    service_id=num_time_series,
                    asset=futures_option,
                    data_schema=pipeline.schema,
                    data_source=pipeline.source,
                    destination=pipeline.output,
                    extractor=pipeline.extractor,
                    additional_params=pipeline.params,
                    active=fut_opt_spec.active
                )
                time_series.append(ts)
                num_time_series += 1
    return num_time_series, time_series


def gen_index_spec(total_time_series: int, time_series: list[TimeSeriesConfig]):
    for root_id, index_spec in index_specs.specs.items():
        pipelines = pipeline_specs.get_spec(root_id)
        if not pipelines:
            logger.warning(f"No pipeline for {root_id}")
            continue
        for pipeline in pipelines:
            index = Index(
                dflow_id=root_id,
                venue=index_spec.venue,
                hours=TradingHours(time_zone=index_spec.time_zone,
                                   open_time_local=index_spec.open_time_local,
                                   close_time_local=index_spec.close_time_local,
                                   days=index_spec.trading_days
                                   ),
                description=index_spec.description
            )
            ts = TimeSeriesConfig(
                service_id=total_time_series,
                asset=index,
                data_schema=pipeline.schema,
                data_source=pipeline.source,
                destination=pipeline.output,
                extractor=pipeline.extractor,
                additional_params=pipeline.params,
                active=index_spec.active
            )
            time_series.append(ts)
            total_time_series += 1
    return total_time_series, time_series


def gen_fx_spec(total_time_series: int, time_series: list[TimeSeriesConfig]):
    for root_id, fx_spec in fx_specs.specs.items():
        pipelines = pipeline_specs.get_spec(root_id)
        if not pipelines:
            logger.warning(f"No pipeline for {root_id}")
            continue
        for pipeline in pipelines:
            fx = FXSpot(
                dflow_id=root_id,
                hours=TradingHours(time_zone=fx_spec.time_zone,
                                   open_time_local=fx_spec.open_time_local,
                                   close_time_local=fx_spec.close_time_local,
                                   days=fx_spec.trading_days
                                   ),
                description=fx_spec.description
            )
            ts = TimeSeriesConfig(
                service_id=total_time_series,
                asset=fx,
                data_schema=pipeline.schema,
                data_source=pipeline.source,
                destination=pipeline.output,
                extractor=pipeline.extractor,
                additional_params=pipeline.params,
                active=fx_spec.active
            )
            time_series.append(ts)
            total_time_series += 1
    return total_time_series, time_series


def get_fwd_spec(total_time_series: int, time_series: list[TimeSeriesConfig]):
    for root_id, fwd_spec in fwd_specs.specs.items():
        pipelines = pipeline_specs.get_spec(root_id)
        if not pipelines:
            logger.warning(f"No pipeline for {root_id}")
            continue
        for pipeline in pipelines:
            fwd = Forward(
                dflow_id=root_id,
                hours=TradingHours(time_zone=fwd_spec.time_zone,
                                   open_time_local=fwd_spec.open_time_local,
                                   close_time_local=fwd_spec.close_time_local,
                                   days=fwd_spec.trading_days
                                   ),
                description=fwd_spec.description
            )
            ts = TimeSeriesConfig(
                service_id=total_time_series,
                asset=fwd,
                data_schema=pipeline.schema,
                data_source=pipeline.source,
                destination=pipeline.output,
                extractor=pipeline.extractor,
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
            logger.warning(f"No pipeline for {root_id}")
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
    total_time_series, time_series = gen_fut_opt_spec(total_time_series, time_series)
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