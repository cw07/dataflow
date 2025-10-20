import yaml
import json
from pathlib import Path
from typing import List, Dict, Any


class Pipeline:
    """
    Represents a single time-series data pipeline configuration.
    """

    def __init__(
            self,
            extractor: str,
            source: str,
            schema: str,
            output: str,
            params: Dict[str, Any]
    ):
        self.extractor = extractor
        self.source = source
        self.schema = schema
        self.output = output
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

        # Validate required fields
        required = ["extractor", "source", "schema", "output"]
        for key in required:
            if key not in data:
                raise KeyError(f"Missing required pipeline field: '{key}'")

        return cls(
            extractor=data["extractor"],
            source=data["source"],
            schema=data["schema"],
            output=data["output"],
            params=params
        )


class TimeSeriesPipelineConfig:
    """
    Manages time-series pipeline configurations for root IDs (e.g., 'NAPEW.ONYX').
    Loads from '../specs/pipelines.yaml' by default.
    """

    def __init__(self, config_path: str = "../specs/pipelines.yaml"):
        self.config_path = Path(config_path)
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path.resolve()}")
        self.root_ids: Dict[str, List[Pipeline]] = self._load_config()

    def _load_config(self) -> Dict[str, List[Pipeline]]:
        """Load and parse the YAML configuration file."""
        with open(self.config_path, "r", encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)

        if not isinstance(raw_config, dict) or "root_ids" not in raw_config:
            raise ValueError("YAML must contain a top-level 'root_ids' key")

        root_ids_config = raw_config["root_ids"]
        parsed_root_ids = {}

        for root_id, config_data in root_ids_config.items():
            if "pipelines" not in config_data:
                raise ValueError(f"Root ID '{root_id}' missing 'pipelines' key")

            pipelines = []
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

    def get_pipelines(self, root_id: str) -> List[Pipeline]:
        """Get all pipelines for a given root ID."""
        if root_id not in self.root_ids:
            raise KeyError(f"Root ID '{root_id}' not found in config")
        return self.root_ids[root_id]

    def list_root_ids(self) -> List[str]:
        """List all configured root IDs."""
        return list(self.root_ids.keys())


if __name__ == "__main__":
    try:
        config = TimeSeriesPipelineConfig("../specs/pipelines.yaml")

        print("Configured root IDs:", config.list_root_ids())
        print("\n" + "=" * 60)

        for root_id in config.list_root_ids():
            print(f"\nRoot ID: {root_id}")
            for i, pipeline in enumerate(config.get_pipelines(root_id), 1):
                print(f"  Pipeline {i}:")
                print(f"    Extractor: {pipeline.extractor}")
                print(f"    Source:    {pipeline.source}")
                print(f"    Schema:    {pipeline.schema}")
                print(f"    Output:    {pipeline.output}")
                print(f"    Params:    {pipeline.params}")

    except (FileNotFoundError, ValueError, KeyError) as e:
        print(f"Configuration error: {e}")