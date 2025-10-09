from ..base import BaseOutputManager


class FileManager(BaseOutputManager):
    def __init__(self):
        super().__init__()

    def save(self, message, orm: str, data_model):
        pass