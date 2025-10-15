import logging

from dataflow.outputs.base import BaseOutputManager

logger = logging.getLogger(__name__)


class FileManager(BaseOutputManager):
    def __init__(self, config: dict):
        super().__init__(config)

    def save(self, message, orm: str, data_model):
        pass