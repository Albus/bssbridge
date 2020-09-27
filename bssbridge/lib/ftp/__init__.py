import aioftp
import pydantic


class FtpUrl(pydantic.AnyUrl):
    user_required: bool = True
    allowed_schemes: frozenset = {"ftp", "ftps"}

    @property
    def is_secure(self) -> bool:
        return self.scheme == "ftps"

    def __init__(self, url: pydantic.StrictStr, **kwargs):
        super(FtpUrl, self).__init__(url=url, port=kwargs['port'] or 21,
                                     **{k: kwargs[k] for k in kwargs if k not in ["port"]})


def get_client(url: FtpUrl) -> aioftp.Client:
    return aioftp.Client.context(host=url.host, port=url.port, user=url.user,
                                 password=url.password, ssl=url.is_secure)
