from __future__ import annotations

import json
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

REFERENCE = re.compile(r"\$\{([^{}]+)\}")
IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")


class Node(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    id: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
    type: Literal["text", "http", "inference"]
    depends_on: list[str] = Field(default_factory=list, max_length=31)
    config: dict[str, Any]

    @model_validator(mode="after")
    def check_config(self):
        fields = {"text": {"template", "timeout_seconds"},
                  "http": {"connector", "timeout_seconds"},
                  "inference": {"prompt", "mode", "timeout_seconds"}}[self.type]
        if set(self.config) - fields:
            raise ValueError("Unknown node configuration field")
        key = {"text": "template", "http": "connector", "inference": "prompt"}[self.type]
        if not isinstance(self.config.get(key), str) or not self.config[key] or len(self.config[key]) > 32_000:
            raise ValueError(f"{key} must be a nonempty string up to 32000 characters")
        timeout = self.config.get("timeout_seconds", 10)
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0.05 <= timeout <= 120:
            raise ValueError("timeout_seconds must be between 0.05 and 120")
        mode = self.config.get("mode", "mock")
        if self.type == "inference" and (not isinstance(mode, str) or mode not in {"mock", "live"}):
            raise ValueError("Inference mode must be mock or live")
        if len(self.depends_on) != len(set(self.depends_on)):
            raise ValueError("Duplicate dependency")
        return self


class Workflow(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    name: str = Field(min_length=1, max_length=80)
    nodes: list[Node] = Field(min_length=1, max_length=32)


class SubmitRun(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    workflow: Workflow
    inputs: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_dag(self):
        if len(self.inputs) > 32 or any(not IDENTIFIER.fullmatch(k) for k in self.inputs):
            raise ValueError("Inputs must contain at most 32 identifier keys")
        if any(not isinstance(value, (str, int, float, bool, type(None))) for value in self.inputs.values()):
            raise ValueError("Inputs must be JSON scalar values")
        # NaN/Infinity are not portable JSON and must not enter idempotency hashes.
        json.dumps(self.inputs, allow_nan=False)
        nodes = {n.id: n for n in self.workflow.nodes}
        if len(nodes) != len(self.workflow.nodes):
            raise ValueError("Duplicate node ID")
        if "inputs" in nodes:
            raise ValueError("The node ID inputs is reserved")
        ancestors: dict[str, set[str]] = {}
        visiting: set[str] = set()

        def visit(node_id):
            if node_id in ancestors:
                return ancestors[node_id]
            if node_id in visiting:
                raise ValueError("Workflow contains a cycle")
            visiting.add(node_id)
            found = set()
            for dependency in nodes[node_id].depends_on:
                if dependency not in nodes:
                    raise ValueError("Unknown dependency")
                found.add(dependency)
                found.update(visit(dependency))
            visiting.remove(node_id)
            ancestors[node_id] = found
            return found

        for node in self.workflow.nodes:
            allowed = visit(node.id)
            for value in node.config.values():
                if not isinstance(value, str):
                    continue
                for reference in REFERENCE.findall(value):
                    parts = reference.split(".")
                    if len(parts) != 2:
                        raise ValueError("Invalid template reference")
                    root, field = parts
                    if root == "inputs":
                        if field not in self.inputs:
                            raise ValueError("Missing workflow input")
                    elif field != "output" or root not in allowed:
                        raise ValueError("References must point to ancestor outputs")
                if "${" in REFERENCE.sub("", value):
                    raise ValueError("Malformed template reference")
        return self


def render(template: str, inputs: dict, outputs: dict[str, str], max_bytes: int = 262144) -> str:
    parts = []
    consumed = 0

    def append(value):
        nonlocal consumed
        # Check each bounded input segment before appending; never construct the expanded string first.
        consumed += len(value.encode("utf-8"))
        if consumed > max_bytes:
            raise ValueError("Rendered template exceeds the configured byte limit")
        parts.append(value)

    position = 0
    for match in REFERENCE.finditer(template):
        append(template[position:match.start()])
        root, field = match.group(1).split(".")
        value = inputs[field] if root == "inputs" else outputs[root]
        append(value if isinstance(value, str) else json.dumps(value, ensure_ascii=False))
        position = match.end()
    append(template[position:])
    # Replacement values are never interpreted a second time.
    return "".join(parts)
