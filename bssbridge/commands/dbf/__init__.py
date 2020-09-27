from asyncio import run, CancelledError
from pathlib import PurePosixPath
from typing import Optional

import aioftp
import aiohttp
import lz4.block
import typing
from aiohttp.client import _RequestContextManager
from bssapi_schemas import exch
from bssapi_schemas.odata import oDataUrl
from bssapi_schemas.odata.InformationRegister import PacketsOfTabData, PacketsOfTabDataSources
from bssapi_schemas.odata.error import Model as oDataError
from cleo import Command
from pydantic import BaseModel, StrictBool, StrictStr

from bssbridge import LogLevel
from bssbridge.lib.ftp import FtpUrl, get_client


class ftp2odata(Command):
  """
  Трансфер файлов DBF с FTP на сервера 1C:oData


  dbf_ftp2odata
      {ftp        : URL FTP сервера (ftps://username:password@server:port/path)}
      {odata      : URL oData сервера (https://username:password@server:port/path)}
      {--d|del    : Удалять файлы после обработки}
      {--b|bot=?  : Токен бота телеграм (для отправки ошибок)}
      {--c|chat=? : Идентификатор чата телеграм (для отправки ошибок)}
  """

  class Params:
    class Arguments(BaseModel):
      ftp: FtpUrl
      odata: oDataUrl

    class Options(BaseModel):
      delete: StrictBool = False
      bot: Optional[StrictStr]
      chat: Optional[StrictStr]

  async def download(self, url: FtpUrl):

    client1: aioftp.Client
    client2: aioftp.Client
    session: aiohttp.ClientSession
    stream: aioftp.DataConnectionThrottleStreamIO
    resp: aiohttp.ClientResponse
    path: PurePosixPath
    info: typing.Dict

    async def delete() -> None:
      if self.Params.Options.delete:
        try:
          await client2.remove_file(path=path)
          self.line("Файл {filename} удален".format(filename=path))
        except:
          self.line_error("Ошибка при удалении файла {filename}".format(filename=path))

    async def get_packet_from_parser() -> _RequestContextManager:
      data = aiohttp.FormData()
      data.add_field(name="file", content_type="application/octet-stream;lz4;base64",
                     value=lz4.block.compress(mode='fast', source=dbf_content),
                     filename=path.name, content_transfer_encoding='base64')
      return session.post(
        url='http://10.12.1.230:8000/parser/dbf/source',
        data=data, chunked=1000, compress=False, params={'url': url})

    async def get_format_from_parser() -> _RequestContextManager:
      data = aiohttp.FormData()
      data.add_field(name="file", content_type="application/octet-stream;lz4;base64",
                     value=lz4.block.compress(mode='fast', source=dbf_content),
                     filename=path.name, content_transfer_encoding='base64')
      return session.post(
        url='http://10.12.1.230:8000/parser/dbf/format',
        data=data, chunked=1000, compress=False, params={'url': url})

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
      except:
        self.line_error("Не удалось переименовать файл на сервере: {filename} -> {new_filename}".format(
          filename=path, new_filename=path.with_suffix('.error')
        ))

    async with \
       get_client(url) as client1, get_client(url) as client2, \
       aiohttp.ClientSession(connector=aiohttp.TCPConnector(
         ssl=None, force_close=True, enable_cleanup_closed=True)) as session:

      try:
        async for path, info in \
           client1.list(recursive=False, path=url.path):  # TODO: обработать рекурсивный режим

          if info["type"] == "file" and path.suffix == ".dbf" and info['size']:
            async with client2.download_stream(source=path) as stream:

              dbf_content = await stream.read()

              try:
                async with await get_format_from_parser() as resp:
                  if resp.status == 200:
                    try:
                      format_of_tab_data = PacketsOfTabDataSources(
                        format=exch.FormatPacket.parse_raw(
                          b=await resp.text(), content_type=resp.content_type))
                    except:
                      self.line_error("Не удалось прочитать ответ паресера")
                    else:
                      async with await save_format_to_odata() as resp:
                        if resp.status == 200:
                          self.line("Импортирован формат {filename}".format(filename=path))
                        else:
                          try:
                            error_msg = oDataError.parse_raw(await resp.text(), content_type=resp.content_type)
                          except:
                            self.line_error("Не удалось получить описание ошибки oData")
                          else:
                            if not error_msg.error.code == "15":  # Запись с такими полями уже существует
                              self.line_error("Ошибка при сохранении формата: {error}".format(
                                error=error_msg.error.message.value))
                  elif resp.status == 422:
                    await mark_file_with_error()
                    continue
                  else:
                    self.line_error("Не ожиданный ответ парсера")
              except:
                self.line_error("Не удалось получить формат от парсера")

              try:
                async with await get_packet_from_parser() as resp:
                  if resp.status == 200:
                    try:
                      packet_of_tab_data = PacketsOfTabData(
                        packet=exch.Packet.parse_raw(
                          b=await resp.text(), content_type=resp.content_type))
                    except:
                      self.line_error("Не удалось прочитать ответ паресера")
                    else:
                      async with await save_packet_to_odata() as resp:
                        if resp.status == 200:
                          self.line("Импортирован файл {filename}".format(filename=path))
                        else:

                          try:
                            error_msg = oDataError.parse_raw(await resp.text(), content_type=resp.content_type)
                          except:
                            self.line_error("Не удалось получить описание ошибки oData")
                          else:
                            self.line_error("Не удалось импортировать: {hash}{filename}: {message}".format(
                              filename=path, message=error_msg.error.message.value,
                              hash=packet_of_tab_data.Hash))
                            if error_msg.error.code == "15":  # Запись с такими полями уже существует
                              await delete()
                              continue

                  elif resp.status == 422:
                    await mark_file_with_error()
                    continue
                  else:
                    self.line_error("Не ожиданный ответ парсера")
              except:
                self.line_error("Не удалось обратиться к паресеру")
      except:
        self.line_error("Не удалось получить листинг FTP каталога")

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

      try:
        run(self.download(url=self.Params.Arguments.ftp))
      except CancelledError:
        pass

  def default(self, default=True):
    pass
