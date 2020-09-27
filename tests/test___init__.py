import bssbridge.schemas.oData.InformationRegister as reg
from lib.odata import get_url, oDataUrl


def test_path():
    """Построение урла одата"""
    url = reg.PacketsOfTabData.path(get_url('http://ivan@santens.ru/bss/'))
    assert isinstance(url, oDataUrl)
