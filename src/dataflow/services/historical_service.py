import os
import sys
import logging
import argparse
import datetime as dt
from functools import partial

from datacore.models.assets import AssetType
from datacore.models.mktdata.datasource import DataSource

from tradetools.bdate import BDate
from tradetools import DEFAULT_TIMEZONE
from tradetools.common import parse_time, print_args

from dataflow.utils.common import set_env_vars
from dataflow.config.loaders.time_series import time_series_config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


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
        "--start-range",
        type=str,
        required=False,
        help="Start range time in 'HH:MM:SS' format",
    )

    parser.add_argument(
        "--end-range",
        type=str,
        required=False,
        help="End range time in 'HH:MM:SS' format",
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

    asset_ts = (
        time_series_config.get_historical_ts()
        .get_ts_by_asset_type(args.asset_type)
        .get_ts_by_source(args.data_source)
        .get_ts_by_schema(args.schema)
    )

    if not asset_ts:
        logger.error(f"No historical time series found for asset type [{args.data_source}] [{args.asset_type}] [{args.schema}]")
        return

    set_env_vars({
        "EXTRACT_START_TIME": args.start_time.isoformat(),
        "EXTRACT_END_TIME": args.end_time.isoformat(),
    })

    start_range = args.start_range if args.start_ranage else BDate("T-1").date
    end_range = args.end_range if args.end_range else BDate("T-1").date

    service_config = {
        **vars(args),
        "time_series": asset_ts,
        "start_range": start_range,
        "end_range": end_range
    }

    print_args(args, extra_params=service_config)
    from dataflow.services.orchestrator import ServiceOrchestrator
    with ServiceOrchestrator(service_type="historical", service_config=service_config) as so:
        so.run_services()


def clmain():
    main(sys.argv[1:])


if __name__ == "__main__":
    bbg_args = [
        "--mode", "PROD",
        "--start-time", "01:00:00",
        "--end-time", "23:00:00",
        "--data-source", "bbg",
        "--asset-type", "futoption",
        "--schema", "ohlcv-1d"
    ]

    onyx_args = [
        "--mode", "PROD",
        "--start-time", "01:00:00",
        "--end-time", "12:00:00",
        "--data-source", "onyx",
        "--asset-type", "fut",
        "--schema", "ohlcv-1d"
    ]

    mkt_db_args = [
        "--mode", "PROD",
        "--start-time", "07:00:00",
        "--end-time", "12:00:00",
        "--data-source", "mkt_db",
        "--asset-type", "index",
        "--schema", "ohlcv-1d"
    ]

    main(bbg_args)
