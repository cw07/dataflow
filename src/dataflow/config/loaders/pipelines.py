import yaml
import json
from pathlib import Path
from typing import List, Dict, Any
from dataflow.config.loaders.base import BaseSpecReader


class Pipeline:
    """
    Represents a single time-series data pipeline configuration.
    """

    def __init__(
            self,
            extractor: str,
            source: str,
            schema: str,
            output: list[str],
            params: Dict[str, Any]
    ):
        self.extractor = extractor
        self.source = source
        self.schema = schema
        self.output: list[str] = output
        self.params = params

    def __repr__(self) -> str:
        return (
            f"Pipeline(extractor='{self.extractor}', source='{self.source}', "
            f"schema='{self.schema}', output='{self.output}', params={self.params})"
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Pipeline":
        """
        Create a Pipeline instance from a dictionary (e.g., parsed YAML).
        Parses 'params' if it's a JSON string.
        """
        params = data.get("params", {})
        if isinstance(params, str):
            try:
                params = json.loads(params)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in pipeline params: {params}") from e
        elif not isinstance(params, dict):
            raise TypeError(f"'params' must be a dict or JSON string, got {type(params)}")

        required = ["extractor", "source", "schema", "output"]
        for key in required:
            if key not in data:
                raise KeyError(f"Missing required pipeline field: '{key}'")

        if isinstance(data["output"], str):
            data["output"] = [data["output"]]
        elif isinstance(data["output"], list):
            pass
        else:
            raise TypeError(f"'output' must be a string or list of strings, got {type(data["output"])}")

        return cls(
            extractor=data["extractor"],
            source=data["source"],
            schema=data["schema"],
            output=data["output"],
            params=params
        )


class PipelineSpecReader(BaseSpecReader):
    """
    Manages time-series pipeline configurations for root IDs (e.g., 'NAPEW.ONYX').
    Loads from '../specs/pipelines.yaml' by default.
    """

    def __init__(self, config_path: str = None):
        super().__init__(config_path, default_filename="pipelines.yaml")

    def _load_config(self) -> Dict[str, List[Pipeline]]:
        """Load and parse the YAML configuration file."""
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.raw_data = yaml.safe_load(f)

        if "root_ids" not in self.raw_data:
            raise ValueError("YAML must contain a top-level 'root_ids' key")

        root_ids_config = self.raw_data["root_ids"]
        parsed_root_ids: Dict[str, List[Pipeline]] = {}

        for root_id, config_data in root_ids_config.items():
            if "pipelines" not in config_data:
                raise ValueError(f"Root ID '{root_id}' missing 'pipelines' key")

            pipelines: List[Pipeline] = []
            for idx, pipeline_data in enumerate(config_data["pipelines"]):
                try:
                    pipeline = Pipeline.from_dict(pipeline_data)
                    pipelines.append(pipeline)
                except (KeyError, ValueError, TypeError) as e:
                    raise ValueError(
                        f"Error in pipeline {idx} for root ID '{root_id}': {e}"
                    ) from e

            parsed_root_ids[root_id] = pipelines

        return parsed_root_ids


pipeline_specs = PipelineSpecReader()

