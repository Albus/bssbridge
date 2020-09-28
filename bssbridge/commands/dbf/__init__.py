import asyncio
import decimal
import typing
from asyncio import run, CancelledError
from functools import partial
from pathlib import PurePosixPath
from typing import Optional

import aioftp
import aiohttp
import lz4.block
import sentry_sdk
from aiohttp.client import _RequestContextManager
from bssapi_schemas import exch
from bssapi_schemas.odata import oDataUrl
from bssapi_schemas.odata.InformationRegister import PacketsOfTabData, PacketsOfTabDataSources
from bssapi_schemas.odata.error import Model as oDataError
from cleo import Command
from pydantic import BaseModel, StrictBool, AnyHttpUrl, StrBytes, HttpUrl
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from bssbridge import LogLevel
from bssbridge.lib.ftp import FtpUrl, get_client


class ftp2odata(Command):
  """
  Трансфер файлов DBF с FTP на сервера 1C:oData

  dbf_ftp2odata
      {ftp        : URL FTP сервера (ftps://username:password@server:port/path)}
      {odata      : URL oData сервера (https://username:password@server:port/path)}
      {bssapi     : Базовый URL службв BssAPI (http://username:password@server:port)}
      {--del      : Удалять файлы после обработки}
      {--pause=15 : Пауза если на сервере нет файлов}
      {--sentry= : URL логера sentry.io (https://token@host/id)}
  """

  class Params:
    class Arguments(BaseModel):
      ftp: FtpUrl
      odata: oDataUrl
      bssapi: AnyHttpUrl

    class Options(BaseModel):
      delete: StrictBool = False
      sentry: Optional[HttpUrl]
      pause: decimal.Decimal = 15

  async def capture_message(self, message: str) -> None:
    self.line_error(message)
    if self.Params.Options.sentry:
      sentry_sdk.capture_message(message)

  async def capture_exception(self, message: Optional[str], exception: BaseException) -> None:
    if message:
      self.line_error(message)
    if self.Params.Options.sentry:
      sentry_sdk.capture_exception(exception)

  def repeat(fn):
    async def wrapper(self, func=fn, *args, **kwargs):
      import asyncio
      while True:
        try:
          task = asyncio.create_task(coro=func(self, *args, **kwargs))
        except Exception as exc:
          raise
        else:
          while True:
            try:
              repeat = await task.result()
            except asyncio.InvalidStateError:
              await asyncio.sleep(0.2)
              continue
            except:
              return
            else:
              self.line("Новая итерация")
              if repeat:
                break
              else:
                return

    return wrapper

  @repeat
  async def download(self):

    client1: aioftp.Client
    client2: aioftp.Client
    session: aiohttp.ClientSession
    stream: aioftp.DataConnectionThrottleStreamIO
    resp: aiohttp.ClientResponse
    path: PurePosixPath
    info: typing.Dict
    dbf_content: StrBytes

    async def delete() -> None:
      if self.Params.Options.delete:
        try:
          if await client2.exists(path=path):
            await client2.remove_file(path=path)
            self.line("Файл {filename} удален".format(filename=path))
          else:
            self.line("Не удалось удалить файл {filename} так как он не существует".format(filename=path))
        except BaseException as exc:
          await self.capture_exception(
            message="Ошибка при удалении файла {filename}".format(filename=path),
            exception=exc)

    async def get_packet_from_parser() -> _RequestContextManager:
      data = aiohttp.FormData()
      data.add_field(name="file", content_type="application/octet-stream;lz4;base64",
                     value=lz4.block.compress(mode='fast', source=dbf_content),
                     filename=path.name, content_transfer_encoding='base64')
      return session.post(
        url='{base_bssapi_url}/parser/dbf/source'.format(base_bssapi_url=self.Params.Arguments.bssapi),
        data=data, chunked=1000, compress=False, params={'url': self.Params.Arguments.ftp})

    async def get_format_from_parser() -> _RequestContextManager:
      data = aiohttp.FormData()
      data.add_field(name="file", content_type="application/octet-stream;lz4;base64",
                     value=lz4.block.compress(mode='fast', source=dbf_content),
                     filename=path.name, content_transfer_encoding='base64')
      return session.post(
        url='{base_bssapi_url}/parser/dbf/format'.format(base_bssapi_url=self.Params.Arguments.bssapi),
        data=data, chunked=1000, compress=False, params={'url': self.Params.Arguments.ftp})

    async def save_packet_to_odata() -> _RequestContextManager:
      return session.post(url=packet_of_tab_data.path(
        base_url=self.Params.Arguments.odata), data=packet_of_tab_data.json(),
        headers={'Content-type': 'application/json'})

    async def save_format_to_odata() -> _RequestContextManager:
      return session.post(url=format_of_tab_data.path(
        base_url=self.Params.Arguments.odata), data=format_of_tab_data.json(),
        headers={'Content-type': 'application/json'})

    async def mark_file_with_error() -> None:
      try:
        await client2.rename(source=path, destination=path.with_suffix('.error'))
        self.line("Файл переименован на сервере: {filename} -> {new_filename}".format(
          filename=path, new_filename=path.with_suffix('.error').name
        ))
      except BaseException as exc:
        await self.capture_exception(
          message="Не удалось переименовать файл на сервере: {filename} -> {new_filename}".format(
            filename=path, new_filename=path.with_suffix('.error')),
          exception=exc)

    async def return_repeat(pause: Optional[float] = float(self.Params.Options.pause), repeat: bool = False) -> bool:
      if repeat and pause:
        self.line("Пауза: {pause} сек.".format(pause=pause))
        await asyncio.sleep(pause)
      return repeat

    do_repeat = partial(return_repeat, repeat=True)
    dont_repeat = partial(return_repeat, repeat=False, pause=None)

    # Нужно 2 клиента FTP. Первый для листинга, второй для операций

    async with \
       get_client(self.Params.Arguments.ftp) as client1, get_client(self.Params.Arguments.ftp) as client2, \
       aiohttp.ClientSession(connector=aiohttp.TCPConnector(
         ssl=None, force_close=True, enable_cleanup_closed=True)) as session:

      try:  # Берем итератор файлов на сервере
        async for path, info in \
           client1.list(recursive=False, path=self.Params.Arguments.ftp.path):  # TODO: обработать рекурсивный режим

          if info["type"] == "file" and path.suffix == ".dbf" and info['size']:
            async with client2.download_stream(source=path) as stream:  # получаем поток загружаемого файла

              dbf_content = await stream.read()  # читаем поток в строку байт

              try:  # получаем формат от парсера
                async with await get_format_from_parser() as resp:

                  if resp.status == 200:  # код ответа при нормальной обработке пакета

                    try:  # читаем ответ парсера в объект формата пакета
                      format_of_tab_data = PacketsOfTabDataSources(
                        format=exch.FormatPacket.parse_raw(
                          b=await resp.text(), content_type=resp.content_type))

                    except BaseException as exc:  # не удалось всунуть ответ в ожидаемую модель
                      await self.capture_exception(message="Не удалось прочитать ответ паресера", exception=exc)

                    else:  # ответ прочитан, будем писать в odata
                      async with await save_format_to_odata() as resp:

                        if resp.status == 200:  # odata отвечает 200 если все прошло хорошо
                          self.line("Импортирован формат {filename}".format(filename=path))

                        else:  # odata сообщаяет об ошибке будем ее анализировать
                          try:  # пытаемся всунуть ответ в модель
                            error_msg = oDataError.parse_raw(await resp.text(), content_type=resp.content_type)

                          except BaseException as exc:  # ответ не соответствует модели, ничего не делаем, идем к следуюущему файлу
                            await self.capture_exception(
                              message="Не удалось получить описание ошибки oData", exception=exc)
                            continue

                          else:  # ответ прочитан в модель, посмотрим что случилось
                            if not error_msg.error.code == "15":  # Запись с такими полями уже существует
                              await self.capture_message("Ошибка при сохранении формата: {error}".format(
                                error=error_msg.error.message.value))

                  elif resp.status == 422:  # парсер не принял параметры запроса, такого в принципе не должно быть
                    continue

                  else:  # парсер не должен давать коды кроме 200 и 422
                    await self.capture_message("Не ожиданный ответ парсера")

              except CancelledError:
                self.line(text="Прерывание обращения к парсеру")
                return dont_repeat

              except BaseException as exc:  # какаято техническая ошибка парсера
                await self.capture_exception(message="Не удалось получить формат от парсера", exception=exc)

              else:  # обработка пакета данных

                try:  # получаем пакет данных от парсера
                  async with await get_packet_from_parser() as resp:

                    if resp.status == 200:  # код ответа при нормальной обработке пакета

                      try:  # читаем ответ парсера в объект пакета данных
                        packet_of_tab_data = PacketsOfTabData(
                          packet=exch.Packet.parse_raw(b=await resp.text(), content_type=resp.content_type))

                      except BaseException as exc:  # не удалось всунуть ответ в ожидаемую модель
                        await self.capture_exception(message="Не удалось прочитать ответ паресера", exception=exc)
                        continue  # оставим все как есть видимо чтото не так с моделью

                      else:  # ответ прочитан, будем писать в odata
                        async with await save_packet_to_odata() as resp:

                          if resp.status == 200:  # odata отвечает 200 если все прошло хорошо
                            self.line("Импортирован файл {filename}".format(filename=path))
                            await delete()  # файл импортирован, теперь его надо удалить с сервера
                            continue

                          else:  # odata сообщаяет об ошибке будем ее анализировать
                            try:  # пытаемся всунуть ответ в модель
                              error_msg = oDataError.parse_raw(await resp.text(), content_type=resp.content_type)

                            except BaseException as exc:  # ответ не соответствует модели, ничего не делаем, идем к следуюущему файлу
                              await self.capture_exception(
                                message="Не удалось получить описание ошибки oData", exception=exc)
                              continue

                            else:  # ответ прочитан в модель, посмотрим что случилось
                              await self.capture_message(
                                message="Не удалось импортировать: {hash}{filename}: {message}".format(
                                  filename=path, message=error_msg.error.message.value, hash=packet_of_tab_data.Hash))

                              if error_msg.error.code == "15":  # Запись с такими полями уже существует
                                await delete()  # файл уже импортирован, теперь его надо удалить с сервера
                                continue

                    elif resp.status == 422:  # парсер не принял параметры запроса, такого в принципе не должно быть
                      continue

                    else:  # парсер не должен давать коды кроме 200 и 422
                      await self.capture_message("Не ожиданный ответ парсера")
                      continue

                except CancelledError:
                  self.line(text="Прерывание обращения к парсеру")
                  return dont_repeat

                except BaseException as exc:  # какаято техническая ошибка парсера
                  await self.capture_exception(message="Не удалось обратиться к паресеру", exception=exc)

        else:
          try:
            return do_repeat(pause=0 if info else float(self.Params.Options.pause))
          except:
            pass

      except BaseException as exc:
        await self.capture_exception(message="Не удалось получить листинг FTP каталога", exception=exc)
    try:
      return dont_repeat
    except:
      await self.capture_exception(message=None, exception=exc)

  def handle(self):

    params = {self.Params.Arguments: self.Params.Arguments(**self.argument()).dict(),
              self.Params.Options: self.Params.Options(**self.option()).dict()}
    rows = []

    for obj in params:
      for key in params[obj]:
        setattr(obj, key, params[obj][key])
        if isinstance(params[obj][key], list):
          for val in params[obj][key]:
            rows.append([key, str(val)])
        else:
          rows.append([key, str(params[obj][key])])
    else:

      self.line(text="Аргументы приняты", verbosity=LogLevel.DEBUG)

      table = self.table()
      table.set_header_row(['Параметр', 'Значение'])
      table.set_rows(rows)
      table.render(io=self.io)

      try:
        del table, rows, obj, params, key
        del val
      except UnboundLocalError:
        pass

      if self.Params.Options.sentry:
        sentry_sdk.init(dsn=self.Params.Options.sentry, traces_sample_rate=1.0, integrations=[AioHttpIntegration()])

      try:
        run(self.download())
      except CancelledError:
        pass


