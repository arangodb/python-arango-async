from dataclasses import dataclass

import pytest
import pytest_asyncio

from arangoasync.auth import JwtToken


@dataclass
class GlobalData:
    url: str = None
    root: str = None
    password: str = None
    secret: str = None
    token: str = None
    sys_db_name: str = "_system"


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
    parser.addoption(
        "--secret", action="store", default="secret", help="ArangoDB JWT secret"
    )


def pytest_configure(config):
    ports = config.getoption("port")
    hosts = [f"http://{config.getoption('host')}:{p}" for p in ports]
    url = hosts[0]

    global_data.url = url
    global_data.root = config.getoption("root")
    global_data.password = config.getoption("password")
    global_data.secret = config.getoption("secret")
    global_data.token = JwtToken.generate_token(global_data.secret)


@pytest.fixture(autouse=False)
def url():
    return global_data.url


@pytest.fixture(autouse=False)
def root():
    return global_data.root


@pytest.fixture(autouse=False)
def password():
    return global_data.password


@pytest.fixture(autouse=False)
def token():
    return global_data.token


@pytest.fixture(autouse=False)
def sys_db_name():
    return global_data.sys_db_name


@pytest_asyncio.fixture
async def client_session():
    """Make sure we close all sessions after the test is done."""
    sessions = []

    def get_client_session(client, url):
        s = client.create_session(url)
        sessions.append(s)
        return s

    yield get_client_session

    for session in sessions:
        await session.close()
