# -*- coding: utf-8 -*-

__version__ = '0.1.10'

import logging
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
    import cleo
    import clikit
    from clikit.api import event
    from clikit.api.args.format.option import Option
    from bssbridge.commands.dbf import ftp2odata as dbf_ftp2odata

    warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

    config = clikit.DefaultApplicationConfig(name='bb', version=__version__)
    config.set_catch_exceptions(False)
    config.set_terminate_after_run(True)
    config.set_display_name("BSS bridge")
    config.add_option(long_name='sentry', flags=Option.REQUIRED_VALUE | Option.STRING | Option.PREFER_LONG_NAME,
                      description='URL логера sentry.io (https://token@host.name/id)')
    config.add_option(long_name='bssapi', flags=Option.REQUIRED_VALUE | Option.STRING | Option.PREFER_LONG_NAME,
                      description='Базовый URL службы BssAPI', default='http://10.12.1.230:8000')
    config.add_option(long_name='odata', flags=Option.REQUIRED_VALUE | Option.STRING | Option.PREFER_LONG_NAME,
                      description='Базовый URL службы BssAPI', default='http://10.12.1.243:81/bss')

    def pre_handle(ev: event.PreHandleEvent, name: str, dispatcher: event.EventDispatcher):
        ev.command.application.config.debug(ev.io.is_debug())
        logging_level: Optional[int] = None
        if ev.io.is_debug():
            logging_level = logging.DEBUG
            ev.io.write_line(string="Включен режим отладки", flags=LogLevel.DEBUG)
        elif ev.io.is_very_verbose():
            logging_level = logging.INFO
        else:
            logging_level = logging.WARN
        logging.basicConfig(level=logging_level, format="%(asctime)s [%(name)s] %(message)s", datefmt="[%H:%M:%S]:")

    config.add_event_listener(event_name=event.PRE_HANDLE, listener=pre_handle, priority=0)

    cleo.Application(complete=True, config=config).add(command=dbf_ftp2odata()).run()
