from abc import abstractmethod
from typing import Callable, Any, Optional

from dataflow.extractors.base import BaseExtractor


class BaseHistoricalExtractor(BaseExtractor):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.handler: Optional[Callable] = None
        self.error_handler: Optional[Callable[[Exception], None]] = None

    @abstractmethod
    def start_extract(self) -> None:
        pass

    @abstractmethod
    def stop_extract(self) -> None:
        pass