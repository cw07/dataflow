from dataflow.outputs.base import BaseOutputManager


class FileManager(BaseOutputManager):
    def __init__(self, config: dict):
        super().__init__(config)

    def save(self, message, orm: str, data_model):
        pass