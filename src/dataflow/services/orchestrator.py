import logging
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
import threading
import time

from dataflow.config.loaders.time_series_loader import TimeSeriesConfigManager, TimeSeriesConfig
from dataflow.config.loaders.settings import settings
from ..outputs.router import OutputRouter
from ..outputs.database.db_manager import DatabaseManager
from ..outputs.file.file_manager import FileManager
from ..outputs.redis.redis_manager import RedisManager

# Import extractors
from ..extractors.realtime.databento import DatabentoRealtimeExtractor
from ..extractors.base import BaseExtractor

logger = logging.getLogger(__name__)


class ExtractorFactory:
    """Factory class to create extractor instances based on configuration"""
    
    _extractor_registry = {
        'databento': DatabentoRealtimeExtractor,
        # Add other extractors here as they are implemented
        # 'polygon': PolygonExtractor,
        # 'yfinance': YFinanceExtractor,
    }
    
    @classmethod
    def create_extractor(cls, extractor_type: str, config: Dict[str, Any]) -> BaseExtractor:
        """Create an extractor instance based on type and configuration"""
        if extractor_type not in cls._extractor_registry:
            raise ValueError(f"Unknown extractor type: {extractor_type}")
        
        extractor_class = cls._extractor_registry[extractor_type]
        
        # Merge with settings-based config for the extractor
        extractor_config = settings.get_extractor_config(extractor_type)
        extractor_config.update(config)
        
        return extractor_class(extractor_config)
    
    @classmethod
    def get_supported_extractors(cls) -> List[str]:
        """Get list of supported extractor types"""
        return list(cls._extractor_registry.keys())


class OutputFactory:
    """Factory class to create output manager instances"""
    
    @classmethod
    def create_output_allocator(cls) -> OutputRouter:
        """Create an OutputAllocator with all configured output managers"""
        # Initialize output managers
        db_manager = DatabaseManager()
        file_manager = FileManager()
        redis_manager = RedisManager()
        
        # Create and return allocator
        return OutputRouter(
            db_manager=db_manager,
            file_manager=file_manager,
            redis_manager=redis_manager
        )


class ServiceGroup:
    """Represents a group of time series configurations that share the same extractor"""
    
    def __init__(self, extractor_type: str, data_source: str, time_series_configs: List[TimeSeriesConfig]):
        self.extractor_type = extractor_type
        self.data_source = data_source
        self.time_series_configs = time_series_configs
        self.extractor: Optional[BaseExtractor] = None
        self.output_allocator: Optional[OutputRouter] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        
    @property
    def group_key(self) -> Tuple[str, str]:
        """Get the unique key for this service group"""
        return (self.extractor_type, self.data_source)
    
    def initialize(self):
        """Initialize the extractor and output allocator for this group"""
        try:
            # Create extractor configuration
            extractor_config = {
                'data_source': self.data_source,
                'time_series': [ts.model_dump() for ts in self.time_series_configs]
            }
            
            # Create extractor instance
            self.extractor = ExtractorFactory.create_extractor(
                self.extractor_type, 
                extractor_config
            )
            
            # Create output allocator
            self.output_allocator = OutputFactory.create_output_allocator()
            
            logger.info(f"Initialized service group {self.group_key} with {len(self.time_series_configs)} time series")
            
        except Exception as e:
            logger.error(f"Failed to initialize service group {self.group_key}: {e}")
            raise
    
    def start(self):
        """Start the service group in a separate thread"""
        if self._running:
            logger.warning(f"Service group {self.group_key} is already running")
            return
        
        if not self.extractor or not self.output_allocator:
            raise RuntimeError(f"Service group {self.group_key} not initialized")
        
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info(f"Started service group {self.group_key}")
    
    def stop(self):
        """Stop the service group"""
        if not self._running:
            return
        
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        
        if self.extractor:
            try:
                self.extractor.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting extractor for {self.group_key}: {e}")
        
        logger.info(f"Stopped service group {self.group_key}")
    
    def _run(self):
        """Main execution loop for the service group"""
        try:
            with self.extractor:
                logger.info(f"Service group {self.group_key} started processing")
                
                # For realtime extractors, this would typically be a streaming loop
                # For historical extractors, this would be a batch processing loop
                while self._running:
                    try:
                        # This is a simplified example - actual implementation would depend
                        # on the specific extractor type and its data retrieval methods
                        if hasattr(self.extractor, 'stream_data'):
                            # Realtime streaming
                            for data in self.extractor.stream_data():
                                if not self._running:
                                    break
                                self._process_data(data)
                        elif hasattr(self.extractor, 'extract_historical'):
                            # Historical data extraction
                            data = self.extractor.extract_historical()
                            self._process_data(data)
                            break  # Historical extraction is typically one-time
                        else:
                            logger.error(f"Extractor {self.extractor_type} doesn't support streaming or historical extraction")
                            break
                            
                    except Exception as e:
                        logger.error(f"Error in service group {self.group_key}: {e}")
                        if not self._running:
                            break
                        time.sleep(5)  # Wait before retrying
                        
        except Exception as e:
            logger.error(f"Fatal error in service group {self.group_key}: {e}")
        finally:
            logger.info(f"Service group {self.group_key} finished processing")
    
    def _process_data(self, data: Any):
        """Process and route data to configured outputs"""
        if not data:
            return
        
        try:
            # Route data to each configured destination for all time series in this group
            for ts_config in self.time_series_configs:
                if ts_config.enabled:
                    # Add metadata to data
                    enriched_data = {
                        'service_id': ts_config.service_id,
                        'series_id': ts_config.series_id,
                        'data': data,
                        'timestamp': time.time(),
                        'extractor_type': self.extractor_type,
                        'data_source': self.data_source
                    }
                    
                    # Route to configured destinations
                    destinations = ts_config.destination if isinstance(ts_config.destination, list) else [ts_config.destination]
                    for destination in destinations:
                        self.output_allocator.route(enriched_data, destination)
                        
        except Exception as e:
            logger.error(f"Error processing data in service group {self.group_key}: {e}")


class ServiceOrchestrator:
    """Orchestrates data extraction services based on time series configurations"""

    def __init__(self, service_type: str, time_series_config: list[TimeSeriesConfig]):
        self.service_type = service_type
        self.time_series_config = time_series_config
        self.service_groups: Dict[Tuple[str, str], ServiceGroup] = {}
        self._initialized = False
        self._running = False

    def initialize_services(self):
        """Initialize all services based on time series configurations"""
        try:
            # Load configurations
            configs = self.config_manager.load_configs()
            if not configs:
                logger.warning("No time series configurations found")
                return
            
            # Group configurations by (extractor_type, data_source)
            grouped_configs = defaultdict(list)
            for config in configs:
                if config.enabled:
                    # Extract extractor type and data source from the extractor field
                    extractor_parts = config.extractor.split('.')
                    if len(extractor_parts) >= 2:
                        extractor_type = extractor_parts[0]  # e.g., 'databento'
                        data_source = extractor_parts[1]     # e.g., 'equity_trades'
                    else:
                        extractor_type = config.extractor
                        data_source = 'default'
                    
                    group_key = (extractor_type, data_source)
                    grouped_configs[group_key].append(config)
            
            # Create service groups
            for group_key, ts_configs in grouped_configs.items():
                extractor_type, data_source = group_key
                
                service_group = ServiceGroup(
                    extractor_type=extractor_type,
                    data_source=data_source,
                    time_series_configs=ts_configs
                )
                
                # Initialize the service group
                service_group.initialize()
                self.service_groups[group_key] = service_group
                
                logger.info(f"Created service group {group_key} with {len(ts_configs)} time series")
            
            self._initialized = True
            logger.info(f"Initialized {len(self.service_groups)} service groups")
            
        except Exception as e:
            logger.error(f"Failed to initialize services: {e}")
            raise

    def start_services(self):
        """Start all initialized services"""
        if not self._initialized:
            raise RuntimeError("Services not initialized. Call initialize_services() first.")
        
        if self._running:
            logger.warning("Services are already running")
            return
        
        try:
            for group_key, service_group in self.service_groups.items():
                service_group.start()
            
            self._running = True
            logger.info(f"Started {len(self.service_groups)} service groups")
            
        except Exception as e:
            logger.error(f"Failed to start services: {e}")
            self.stop_services()  # Clean up on failure
            raise

    def stop_services(self):
        """Stop all running services"""
        if not self._running:
            return
        
        logger.info("Stopping all services...")
        
        for group_key, service_group in self.service_groups.items():
            try:
                service_group.stop()
            except Exception as e:
                logger.error(f"Error stopping service group {group_key}: {e}")
        
        self._running = False
        logger.info("All services stopped")

    def get_service_status(self) -> Dict[str, Any]:
        """Get status of all services"""
        status = {
            'initialized': self._initialized,
            'running': self._running,
            'service_groups': {}
        }
        
        for group_key, service_group in self.service_groups.items():
            group_status = {
                'extractor_type': service_group.extractor_type,
                'data_source': service_group.data_source,
                'time_series_count': len(service_group.time_series_configs),
                'running': service_group._running,
                'extractor_connected': service_group.extractor.is_connected if service_group.extractor else False
            }
            status['service_groups'][f"{group_key[0]}.{group_key[1]}"] = group_status
        
        return status

    def reload_configuration(self):
        """Reload configuration and restart services"""
        logger.info("Reloading configuration...")
        
        # Stop current services
        if self._running:
            self.stop_services()
        
        # Clear current service groups
        self.service_groups.clear()
        self._initialized = False
        
        # Reinitialize and restart
        self.initialize_services()
        if self._initialized:
            self.start_services()
        
        logger.info("Configuration reloaded successfully")

    def run_services(self) -> bool:
        """Run a single service manually (for testing or one-off execution)"""
        try:
            # Create a temporary time series config
            temp_config = TimeSeriesConfig(
                service_id=service_id,
                series_id=series_id,
                extractor=extractor_type,
                destination=destination,
                enabled=True,
                additional_params=kwargs
            )
            
            # Extract extractor type and data source
            extractor_parts = extractor_type.split('.')
            if len(extractor_parts) >= 2:
                ext_type = extractor_parts[0]
                data_source = extractor_parts[1]
            else:
                ext_type = extractor_type
                data_source = 'default'
            
            # Create temporary service group
            temp_group = ServiceGroup(
                extractor_type=ext_type,
                data_source=data_source,
                time_series_configs=[temp_config]
            )
            
            # Initialize and run
            temp_group.initialize()
            
            logger.info(f"Running single service: {service_id}.{series_id}")
            
            # For one-off execution, we'll run synchronously
            with temp_group.extractor:
                if hasattr(temp_group.extractor, 'extract_historical'):
                    data = temp_group.extractor.extract_historical()
                    temp_group._process_data(data)
                    logger.info(f"Successfully executed service: {service_id}.{series_id}")
                    return True
                else:
                    logger.error(f"Extractor {ext_type} doesn't support historical extraction")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to run service {service_id}.{series_id}: {e}")
            return False

    def __enter__(self):
        """Context manager entry"""
        self.initialize_services()
        self.start_services()
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        """Context manager exit"""
        self.stop_services()