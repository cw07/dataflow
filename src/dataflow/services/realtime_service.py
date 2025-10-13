import sys
import argparse
from functools import partial

from datacore.models.assets import AssetType
from datacore.models.mktdata.datasource import DataSource

from tradetools.common import parse_time, print_args

from .orchestrator import ServiceOrchestrator
from ..config.loaders.time_series_loader import time_series_config


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
        required=True,
        type=parse_time,
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

    asset_ts = time_series_config.get_ts_by_asset_type(args.asset_type).get_ts_by_source(args.data_source)
    with ServiceOrchestrator(service_type="realtime", time_series_config=asset_ts) as so:
        so.run_services()




def clmain():
    main(sys.argv[1:])


if __name__ == "__main__":
    realtime_args = [
        "--mode", "PROD",
        "--start-time", "09:30:00",
        "--end-time", "05:30:00 1",
        "--data-source", "databento",
        "--asset-type", "fut"
    ]
    main(realtime_args)




