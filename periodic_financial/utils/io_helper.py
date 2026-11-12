from pathlib import Path

import json


def write_json(path: str, payload: list):
    path = Path(path)
    temp_path = path.with_name(path.name + ".tmp")

    with temp_path.open("w") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)

    # Renamed in one step, so a run stopped mid-write never leaves half a file
    temp_path.replace(path)


def open_json(path: str):
    path = Path(path)

    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)

    except json.JSONDecodeError:
        return []
