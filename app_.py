import os
import re
from typing import Iterator, Any

from flask import Flask, request, Response
from werkzeug.exceptions import BadRequest

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


def slise_limit(it: Iterator, value: int) -> Iterator:
    i = 0
    for item in it:
        if i < value:
            yield item
        else:
            break
        i += 1


def build_query(it: Iterator, cmd: str, value: str) -> Iterator:
    res = map(lambda v: v.strip(), it)
    if cmd == "filter":
        return filter(lambda v: value in v, res)
    if cmd == "sort":
        return iter(sorted(res, reverse=bool(value)))
    if cmd == "unique":
        return iter(set(res))
    if cmd == "limit":
        return slise_limit(res, int(value))
    if cmd == "map":
        res = map(lambda v: v.split(" ")[int(value)], res)
    if cmd == "regex":
        regex = re.compile(value)
        return filter(lambda x: regex.search(x), res)

    return res


@app.route("/perform_query")
def perform_query() -> Response:
    try:
        cmd_1 = request.args["cmd_1"]
        cmd_2 = request.args["cmd_2"]
        val_1 = request.args["val_1"]
        val_2 = request.args["val_2"]
        file_name = request.args["file_name"]
    except:
        raise BadRequest
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        raise BadRequest

    with open(file_path) as f:
        res = build_query(f, cmd_1, val_1)
        res = build_query(res, cmd_2, val_2)
        content = "\n".join(res)

    return app.response_class(content, content_type="text/plain")


app.run()
