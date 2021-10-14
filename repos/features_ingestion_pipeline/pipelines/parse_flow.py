import json
from pathlib import Path
from typing import Union


class FlowFile(object):
    def __init__(self, file_path: Union[str, Path]) -> None:
        if not isinstance(file_path, Path):
            file_path = Path(file_path)

        with file_path.open("r") as data:
            self._flow = json.load(data)

    @property
    def input_file_path(self) -> str:
        return self._flow["nodes"][0]["parameters"]["dataset_definition"][
            "s3ExecutionContext"
        ]["s3Uri"]

    @property
    def output_name(self) -> str:
        node_name = f"{self._flow['nodes'][-1]['node_id']}"
        output_name = f"{self._flow['nodes'][-1]['outputs'][0]['name']}"
        return f"{node_name}.{output_name}"

    @property
    def input_name(self) -> str:
        return self._flow["nodes"][0]["parameters"]["dataset_definition"]["name"]
