#!/usr/bin/env python
# https://gist.github.com/yhoiseth/c80c1e44a7036307e424fce616eed25e
import subprocess
from typing import Any

import toml

with open("pyproject.toml", "r") as file:
    pyproject: dict[str, Any] = toml.load(file)
dependencies: list[str] = pyproject["project"]["dependencies"]
for dependency in dependencies:
    try:
        package, version = dependency.split(">=")
        subprocess.run(["uv", "remove", package])
        subprocess.run(["uv", "add", package])
    except ValueError:
        pass
