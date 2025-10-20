import copy
import logging
from collections import defaultdict

from dataflow.extractors.extractor_factory import ExtractorFactory

logger = logging.getLogger(__name__)


class ServiceOrchestrator:
    """Orchestrates data extraction services based on time series configurations"""

    def __init__(self, service_type: str, service_config):
        self.service_type = service_type
        self.service_config = service_config
        self.services = []
        self._initialized = False
        self.gate = None

    def initialize_services(self):
        """
        Initialize all services based on time series configurations.
        """
        if not self.service_config["time_series"]:
            logger.warning("No time series configurations found")
            return

        grouped_services = defaultdict(list)
        for time_series in self.service_config["time_series"]:
            extractor_type = time_series.extractor
            data_source = time_series.data_source
            group_key = (extractor_type, data_source)
            grouped_services[group_key].append(time_series)

        for group_key, all_time_series in grouped_services.items():
            extractor_type, data_source = group_key
            extractor_cls = ExtractorFactory.create_extractor(extractor_type, data_source)
            service_config = copy.deepcopy(self.service_config)
            service_config["time_series"] = all_time_series
            extractor = extractor_cls(config=service_config)
            self.services.append(extractor)

        logger.info(f"{len(self.services)} services initialized")
        self._initialized = True

    def start_services(self):
        for extract_service in self.services:
            logger.info(f"Starting service: {extract_service.__class__.__name__}")
            extract_service.start_extract()

    def stop_services(self):
        for extract_service in self.services:
            logger.info(f"Service {extract_service.__class__.__name__} stopped")

    def run_services(self):
        """Run a single service manually (for testing or one-off execution)"""
        self.start_services()

    def __enter__(self):
        """Context manager entry"""
        self.initialize_services()
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        """Context manager exit"""
        self.stop_services()