from abc import ABC, abstractmethod


class BaseSymbolResolver(ABC):

    @abstractmethod
    def resolve(self, *args):
        pass
