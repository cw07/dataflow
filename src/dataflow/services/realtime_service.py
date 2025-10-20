import os
import sys
import logging
import argparse
import datetime as dt
from functools import partial

from datacore.models.assets import AssetType
from datacore.models.mktdata.datasource import DataSource

from tradetools import DEFAULT_TIMEZONE
from tradetools.common import parse_time, print_args

from dataflow.utils.common import set_env_vars
from dataflow.config.loaders.time_series import time_series_config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def parse_arguments(args):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--mode",
        choices=["UAT", "PROD"],
        default="UAT",
        help="UAT or PROD, UAT mode will not trigger pd alert, and slack msg will start with 'TEST'"
    )

    parser.add_argument(
        "--start-time",
        type=parse_time,
        default=dt.datetime.now(DEFAULT_TIMEZONE),
        help="Start time in 'HH:MM:SS delta_day' format",
    )

    parser.add_argument(
        "--end-time",
        required=True,
        type=partial(parse_time, run_hours_limit=True),
        help="End time in 'HH:MM:SS delta_day' format"
    )

    parser.add_argument(
        "--data-source",
        type=DataSource,
        help="data source"
    )

    parser.add_argument(
        "--asset-type",
        type=AssetType,
        help="asset type"
    )

    parser.add_argument(
        "--extra-ids",
        nargs='*',
        default=[]
    )

    parser.add_argument(
        "--schema",
        type=str,
        required=True
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Log level"
    )

    args = parser.parse_args(args)
    return args


def main(args):
    args = parse_arguments(args)
    print_args(args)

    asset_ts = (
        time_series_config.get_realtime_ts()
        .get_ts_by_asset_type(args.asset_type)
        .get_ts_by_source(args.data_source)
        .get_ts_by_schema(args.schema)
    )

    service_config = {
        "schema": args.schema,
        "time_series": asset_ts,
    }

    set_env_vars({
        "EXTRACT_START_TIME": args.start_time.isoformat(),
        "EXTRACT_END_TIME": args.end_time.isoformat(),
    })

    from dataflow.services.orchestrator import ServiceOrchestrator
    with ServiceOrchestrator(service_type="realtime", service_config=service_config) as so:
        so.run_services()


def clmain():
    main(sys.argv[1:])


if __name__ == "__main__":
    onyx_args = [
        "--mode", "PROD",
        "--end-time", "05:30:00 1",
        "--data-source", "onyx",
        "--asset-type", "fut",
        "--schema", "mbp-1"
    ]

    databento_args = [
        "--mode", "PROD",
        "--end-time", "05:30:00 1",
        "--data-source", "databento",
        "--asset-type", "fut",
        "--schema", "mbp-1"
    ]

    main(onyx_args)




