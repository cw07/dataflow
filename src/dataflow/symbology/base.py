from abc import ABC, abstractmethod


class BaseSymbolResolver(ABC):

    @abstractmethod
    def resolve(self, input_symbols: list, symbol_type: str):
        pass
