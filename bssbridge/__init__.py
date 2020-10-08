# -*- coding: utf-8 -*-

__version__ = '0.1.6'

from typing import Any, Optional, Callable

import orjson
from clikit.api.io import flags as LogLevel

__all__ = ["LogLevel"]

from pydantic import BaseModel


def json_dumps(obj: Any, *, default: Optional[Callable[[Any], Any]] = None, option: Optional[int] = None, ) -> str:
    return orjson.dumps(
        obj, default=default,
        option=option or 0 | orjson.OPT_INDENT_2 | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_STRICT_INTEGER
    ).decode(encoding='utf-8')


BaseModel.Config.json_dumps = json_dumps
BaseModel.Config.json_loads = orjson.loads


def main() -> None:
    import warnings

    warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

    from cleo import Application
    from bssbridge.commands.dbf import ftp2odata as dbf_ftp2odata
    Application(name="BssBridge", version=__version__, complete=True).add(command=dbf_ftp2odata()).run()
