from abc import abstractmethod
from typing import Callable, Dict, Any, Optional
from ..base import BaseExtractor


class BaseRealtimeExtractor(BaseExtractor):
    """Base class for realtime data extractors"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.handler: Optional[Callable] = None
        self.error_handler: Optional[Callable[[Exception], None]] = None

    def set_error_handler(self, handler: Callable[[Exception], None]):
        """Set handler for errors"""
        self.error_handler = handler

    @abstractmethod
    def subscribe(self, symbols: list):
        """Subscribe to realtime data for given symbols"""
        pass

    @abstractmethod
    def resubscribe(self, symbols: Optional[list] = None):
        """Resubscribe to realtime data for given symbols"""
        pass

    @abstractmethod
    def unsubscribe(self, symbols: Optional[list] = None):
        """Unsubscribe from symbols"""
        pass

    @abstractmethod
    def start_extract(self):
        """Start streaming data"""
        pass

    @abstractmethod
    def stop_extract(self):
        """Stop streaming data"""
        pass