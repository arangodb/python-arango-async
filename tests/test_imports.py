import importlib
import pkgutil

import pytest

import arangoasync


def arangoasync_modules():
    yield arangoasync.__name__

    for module_info in pkgutil.walk_packages(
        arangoasync.__path__,
        prefix=f"{arangoasync.__name__}.",
    ):
        yield module_info.name


@pytest.mark.parametrize("module_name", sorted(arangoasync_modules()))
def test_import_module(module_name):
    importlib.import_module(module_name)
