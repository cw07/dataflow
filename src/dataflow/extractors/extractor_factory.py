import importlib


class ExtractorFactory:
    """Factory class to create extractor instances based on configuration"""

    _extractor_registry = {
        ("realtime", "databento"): ("dataflow.extractors.realtime.databento", "DatabentoRealtimeExtractor"),
        ("realtime", "bbg"): ("dataflow.extractors.realtime.bbg", "BBGRealtimeExtractor"),
        ("realtime", "onyx"): ("dataflow.extractors.realtime.onyx", "OnyxRealtimeExtractor"),
        ("historical", "databento"): ("dataflow.extractors.historical.databento", "DatabentoHistoricalExtractor"),
        ("historical", "bbg"): ("dataflow.extractors.historical.bbg", "BBGHistoricalExtractor"),
        ("historical", "onyx"): ("dataflow.extractors.historical.onyx", "OnyxHistoricalExtractor"),
        ("historical", "mkt_db"): ("dataflow.extractors.historical.mkt_db", "MktDBHistoricalExtractor"),
    }

    @classmethod
    def create_extractor(cls, extractor_type: str, data_source: str):
        """Create an extractor instance based on type and configuration"""
        key = (extractor_type, data_source)
        if key not in cls._extractor_registry:
            raise ValueError(f"Unknown extractor type: {extractor_type}")

        module_name, class_name = cls._extractor_registry[key]
        module = importlib.import_module(module_name)
        extractor_class = getattr(module, class_name)

        return extractor_class

    @classmethod
    def get_supported_extractors(cls) -> list:
        """Get list of supported extractor types"""
        return list(cls._extractor_registry.keys())
