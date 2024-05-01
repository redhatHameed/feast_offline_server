import base64
import os
import tempfile
from pathlib import Path


class Config:
    FEATURE_STORE_YAML_ENV_NAME = "FEATURE_STORE_YAML_ENV_NAME"

    @classmethod
    def get_feature_store_yaml(cls):
        config_base64 = os.environ[cls.FEATURE_STORE_YAML_ENV_NAME]
        if config_base64 is None:
            raise ValueError(f"Environment variable '{cls.FEATURE_STORE_YAML_ENV_NAME}' is not set.")
        config_bytes = base64.b64decode(config_base64)
        repo_path = Path(tempfile.mkdtemp())
        with open(repo_path / "feature_store.yaml", "wb") as f:
            f.write(config_bytes)

        return str(repo_path.resolve())
