"""Write a dbt-duckdb model out as a Delta table.

Pure adapter plumbing. dbt-duckdb's bundled delta plugin reads Delta but cannot
write it; Starburst and Databricks write it natively, so this stands in for that.
Nothing here knows that ddi exists.

The conversion runs in a subprocess because importing pyarrow into a process that
has already loaded duckdb segfaults it (duckdb bundles its own Arrow, and the two
ABIs collide). Reading Delta in-process is fine — only pyarrow is the problem.
"""
import os
import subprocess
import sys

from dbt.adapters.duckdb.plugins import BasePlugin

_CONVERTER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_to_delta.py")


class Plugin(BasePlugin):
    def initialize(self, config):
        pass

    def store(self, target_config):
        delta_path = target_config.config.get("delta_path")
        if not delta_path:
            raise Exception("delta_path is required in the model config")
        parquet_path = os.path.abspath(target_config.location.path)
        subprocess.run(
            [sys.executable, _CONVERTER, parquet_path, delta_path],
            check=True,
        )
