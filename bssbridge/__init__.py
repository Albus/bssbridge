__version__ = '0.1.0'
from cleo import Application
from bssbridge.commands.dbf import ftp2odata as dbf_ftp2odata
import asyncio

APP_NAME="Перифирийный робот 1С:BSS"

app = Application()
app.add(command=dbf_ftp2odata())

def main()->None:

    async def aiomain():
        print('aiomain')
        await asyncio.sleep(5)
        pass

    asyncio.run(main=aiomain(),debug=True)


