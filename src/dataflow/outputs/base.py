from abc import ABC, abstractmethod


class BaseOutputManager(ABC):

    def __init__(self, ):
        pass

    @abstractmethod
    def save(self, *args, **kwargs):
        pass