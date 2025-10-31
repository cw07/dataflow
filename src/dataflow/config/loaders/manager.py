import re
import logging
from typing import Optional

from datacore.models.mktdata.venue import Venue
from datacore.models.assets import Index, Forward, FXSpot, Futures, FuturesOptions, BaseFutures, TradingHours

from dataflow.config.loaders.pipelines import pipeline_specs
from dataflow.config.loaders.time_series import TimeSeriesConfig


logger = logging.getLogger(__name__)


def gen_fut_spec(num_time_series: int, time_series: list[TimeSeriesConfig]):
    from dataflow.config.loaders.fut_spec import futures_specs
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
    from dataflow.config.loaders.fut_spec import futures_specs
    from dataflow.config.loaders.futopt_spec import futures_opt_specs

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
    from dataflow.config.loaders.index_spec import index_specs

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
    from dataflow.config.loaders.fx_spec import fx_specs

    for root_id, fx_spec in fx_specs.specs.items():
        pipelines = pipeline_specs.get_spec(root_id)
        if not pipelines:
            logger.warning(f"No pipeline for {root_id}")
            continue
        for pipeline in pipelines:
            fx = FXSpot(
                dflow_id=root_id,
                venue=Venue.GLOBAL,
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
    from dataflow.config.loaders.fwd_spec import fwd_specs

    for root_id, fwd_spec in fwd_specs.specs.items():
        pipelines = pipeline_specs.get_spec(root_id)
        if not pipelines:
            logger.warning(f"No pipeline for {root_id}")
            continue
        for pipeline in pipelines:
            fwd = Forward(
                dflow_id=root_id,
                venue=fwd_spec.venue,
                contract_size=fwd_spec.contract_size,
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
    from dataflow.config.loaders.spread_spec import spread_specs

    for root_id, spread_spec in spread_specs.specs.items():
        pipelines = pipeline_specs.get_spec(root_id)
        if not pipelines:
            logger.warning(f"No pipeline for {root_id}")
            continue
        pass

    return total_time_series, time_series


def gen_equity_spec(total_time_series: int, time_series: list[TimeSeriesConfig]):
    from dataflow.config.loaders.equity_spec import equity_specs

    for equity_spec in equity_specs.specs.items():
        pass

    return total_time_series, time_series


def gen_all_spec() -> list[TimeSeriesConfig]:
    time_series = []
    num_time_series = 1
    num_time_series, time_series = gen_fut_spec(num_time_series, time_series)
    num_time_series, time_series = gen_fut_opt_spec(num_time_series, time_series)
    num_time_series, time_series = gen_index_spec(num_time_series, time_series)
    num_time_series, time_series = gen_fx_spec(num_time_series, time_series)
    num_time_series, time_series = get_fwd_spec(num_time_series, time_series)
    num_time_series, time_series = gen_equity_spec(num_time_series, time_series)
    num_time_series, time_series = gen_spread_spec(num_time_series, time_series)
    return time_series


class TimeSeriesFilterMixin:
    """Mixin class providing common filtering methods"""

    # Subclasses must implement this property
    @property
    def time_series(self) -> list[TimeSeriesConfig]:
        raise NotImplementedError("Subclass must provide time_series property")

    def get_extractors(self):
        return set(s.extractor for s in self.time_series)

    def get_active_ts(self):
        """Get all time series (no filtering)"""
        filtered = [s for s in self.time_series if s.active]
        return self._wrap_result(filtered)

    def get_ts_by_venue(self, venue: str):
        """Filter by venue"""
        filtered = [s for s in self.time_series if s.asset.venue == venue]
        return self._wrap_result(filtered)

    def get_ts_by_source(self, source: str):
        """Filter by data source"""
        filtered = [s for s in self.time_series if s.data_source == source]
        return self._wrap_result(filtered)

    def get_ts_by_asset_type(self, asset_type: str):
        """Filter by asset type"""
        filtered = [s for s in self.time_series if s.asset.asset_type == asset_type]
        return self._wrap_result(filtered)

    def get_ts_by_schema(self, schema: str):
        """Filter by schema"""
        filtered = [s for s in self.time_series if s.data_schema == schema]
        return self._wrap_result(filtered)

    def get_open_ts(self):
        filtered = [s for s in self.time_series if s.asset.is_open]
        return self._wrap_result(filtered)

    def get_ts_by_root_id(self, root_id: Optional[list[str]]):
        if root_id:
            filtered = []
            for s in self.time_series:
                for pattern in root_id:
                    if any(char in pattern for char in ['*', '.', '^', '$', '[', ']']):
                        if re.match(pattern, s.root_id):
                            filtered.append(s)
                            break
                    else:
                        if s.root_id == pattern:
                            filtered.append(s)
                            break
        else:
            filtered = self.time_series
        return self._wrap_result(filtered)

    def get_realtime_ts(self):
        """Filter for realtime series"""
        filtered = [s for s in self.time_series if s.extractor == "realtime"]
        return self._wrap_result(filtered)

    def get_historical_ts(self):
        """Filter for historical series"""
        filtered = [s for s in self.time_series if s.extractor == "historical"]
        return self._wrap_result(filtered)

    def _wrap_result(self, filtered: list[TimeSeriesConfig]):
        """Wrap filtered results - subclasses override this"""
        raise NotImplementedError("Subclass must implement _wrap_result")


class TimeSeriesQueryResult(TimeSeriesFilterMixin):
    """Wrapper class to enable method chaining on query results"""

    def __init__(self, time_series: list[TimeSeriesConfig]):
        self._time_series = time_series

    @property
    def time_series(self) -> list[TimeSeriesConfig]:
        return self._time_series

    def _wrap_result(self, filtered: list[TimeSeriesConfig]) -> 'TimeSeriesQueryResult':
        """Wrap filtered results in a new QueryResult for chaining"""
        return TimeSeriesQueryResult(filtered)

    def to_list(self) -> list[TimeSeriesConfig]:
        """Get the underlying list"""
        return self._time_series

    def __repr__(self) -> str:
        """String representation of the query result"""
        count = len(self._time_series)
        if count == 0:
            return "TimeSeriesQueryResult(0 time series)"
        configs_repr = '\n  '.join(repr(ts) for ts in self._time_series)
        if count == 1:
            return f"TimeSeriesQueryResult(1 time series):\n {configs_repr}"
        else:
            return f"TimeSeriesQueryResult({count} time series):\n{configs_repr}"

    def __iter__(self):
        """Make it iterable"""
        return iter(self._time_series)

    def __len__(self):
        """Get count"""
        return len(self._time_series)

    def __bool__(self):
        return len(self._time_series) > 0


class TimeSeriesManager(TimeSeriesFilterMixin):
    """Manages loading and querying service configurations"""

    def __init__(self, active_only: bool = True):
        self.active_only: bool = active_only
        self._time_series: list[TimeSeriesConfig] = []
        self.load_configurations()

    @property
    def time_series(self) -> TimeSeriesQueryResult:
        return self._wrap_result(self._time_series)

    def load_configurations(self):
         self._time_series = gen_all_spec()

    def _wrap_result(self, filtered: list[TimeSeriesConfig]) -> TimeSeriesQueryResult:
        """Wrap filtered results in a QueryResult for chaining"""
        return TimeSeriesQueryResult(filtered)


time_series_config = TimeSeriesManager(active_only=True).time_series


if __name__ == "__main__":
    ts_config = TimeSeriesManager(active_only=True).time_series
    result = ts_config.get_realtime_ts().get_ts_by_source("bbg")
    print(result)

