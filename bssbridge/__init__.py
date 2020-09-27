__version__ = '0.1.0'

from clikit.api.io import flags as LogLevel

__all__ = ["LogLevel"]


def main() -> None:
    import tracemalloc
    import warnings

    tracemalloc.start()

    warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

    from cleo import Application
    from bssbridge.commands.dbf import ftp2odata as dbf_ftp2odata
    app = Application(name="bb", version=__version__, complete=True)
    app.add(command=dbf_ftp2odata())
    app.run()
