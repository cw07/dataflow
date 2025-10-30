import os
import sys
import logging
import argparse
import datetime as dt
from functools import partial
from pyarrow import logging_memory_pool

from datacore.models.mktdata.venue import Venue
from datacore.models.assets.asset_type import AssetType
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
        "--venue",
        type=Venue,
        required=True,
        help="venue"
    )

    parser.add_argument(
        "--asset-type",
        type=AssetType,
        required=True,
        help="asset type"
    )

    parser.add_argument(
        "--data-source",
        type=DataSource,
        required=True,
        help="data source"
    )

    parser.add_argument(
        "--schema",
        type=str,
        required=True
    )

    parser.add_argument(
        "--root-ids",
        nargs="*",
        type=str,
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

    asset_ts = (
        time_series_config.get_realtime_ts()
        .get_ts_by_venue(args.venue)
        .get_ts_by_asset_type(args.asset_type)
        .get_ts_by_source(args.data_source)
        .get_ts_by_schema(args.schema)
        .get_ts_by_root_id(args.root_ids)
    )
    if not asset_ts:
        logger.error(f"No historical time series found for asset type [{args.data_source}] [{args.asset_type}] [{args.schema}]")
        return

    set_env_vars({
        "EXTRACT_START_TIME": args.start_time.isoformat(),
        "EXTRACT_END_TIME": args.end_time.isoformat(),
    })

    service_config = {
        "schema": args.schema,
        "time_series": asset_ts,
    }

    print_args(args, extra_params=service_config)
    from dataflow.services.orchestrator import ServiceOrchestrator
    with ServiceOrchestrator(service_type="realtime", service_config=service_config) as so:
        so.run_services()


def clmain():
    main(sys.argv[1:])


if __name__ == "__main__":

    onyx_fut_onyx = [
        "--mode", "PROD",
        "--end-time", "05:30:00 1",
        "--venue", "ONYX",
        "--asset-type", "fut",
        "--data-source", "onyx",
        "--schema", "mbp-1"
    ]

    lme_fut_bbg = [
        "--mode", "PROD",
        "--end-time", "05:30:00 1",
        "--venue", "LME",
        "--data-source", "bbg",
        "--asset-type", "fut",
        "--schema", "mbp-1"
    ]

    cme_fut_databento = [
        "--mode", "PROD",
        "--end-time", "23:59:00",
        "--venue", "CME",
        "--asset-type", "fut",
        "--data-source", "databento",
        "--schema", "mbp-1"
    ]

    main(cme_fut_databento)




