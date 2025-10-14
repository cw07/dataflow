from abc import ABC, abstractmethod


class BaseOutputManager(ABC):

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def save(self, *args, **kwargs):
        pass