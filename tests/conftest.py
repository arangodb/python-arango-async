from dataclasses import dataclass

import pytest


@dataclass
class GlobalData:
    url: str = None
    root: str = None
    password: str = None


global_data = GlobalData()


def pytest_addoption(parser):
    parser.addoption(
        "--host", action="store", default="127.0.0.1", help="ArangoDB host address"
    )
    parser.addoption(
        "--port", action="append", default=["8529"], help="ArangoDB coordinator ports"
    )
    parser.addoption(
        "--root", action="store", default="root", help="ArangoDB root user"
    )
    parser.addoption(
        "--password", action="store", default="passwd", help="ArangoDB password"
    )


def pytest_configure(config):
    ports = config.getoption("port")
    hosts = [f"http://{config.getoption('host')}:{p}" for p in ports]
    url = hosts[0]

    global_data.url = url
    global_data.root = config.getoption("root")
    global_data.password = config.getoption("password")


@pytest.fixture(autouse=False)
def url():
    return global_data.url


@pytest.fixture(autouse=False)
def root():
    return global_data.root


@pytest.fixture(autouse=False)
def password():
    return global_data.password
