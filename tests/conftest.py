import asyncio
from dataclasses import dataclass

import pytest
import pytest_asyncio
from packaging import version

from arangoasync.auth import Auth, JwtToken
from arangoasync.client import ArangoClient
from arangoasync.typings import UserInfo
from tests.helpers import generate_col_name, generate_db_name, generate_username


@dataclass
class GlobalData:
    url: str = None
    root: str = None
    password: str = None
    secret: str = None
    token: JwtToken = None
    sys_db_name: str = "_system"
    username: str = generate_username()
    cluster: bool = False
    enterprise: bool = False
    db_version: version = version.parse("0.0.0")


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
    parser.addoption(
        "--cluster", action="store_true", help="Run tests in a cluster setup"
    )
    parser.addoption(
        "--enterprise", action="store_true", help="Run tests in an enterprise setup"
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
    global_data.cluster = config.getoption("cluster")
    global_data.enterprise = config.getoption("enterprise")

    async def get_db_version():
        async with ArangoClient(hosts=global_data.url) as client:
            sys_db = await client.db(
                global_data.sys_db_name,
                auth_method="basic",
                auth=Auth(global_data.root, global_data.password),
                verify=False,
            )
            db_version = (await sys_db.version())["version"]
            global_data.db_version = version.parse(db_version.split("-")[0])

    asyncio.run(get_db_version())


@pytest.fixture
def url():
    return global_data.url


@pytest.fixture
def root():
    return global_data.root


@pytest.fixture
def password():
    return global_data.password


@pytest.fixture
def basic_auth_root(root, password):
    return Auth(username=root, password=password)


@pytest.fixture
def cluster():
    return global_data.cluster


@pytest.fixture
def enterprise():
    return global_data.enterprise


@pytest.fixture
def username():
    return global_data.username


@pytest.fixture
def token():
    return global_data.token


@pytest.fixture
def sys_db_name():
    return global_data.sys_db_name


@pytest.fixture
def docs():
    return [
        {"_key": "1", "val": 1, "text": "foo", "loc": [1, 1]},
        {"_key": "2", "val": 2, "text": "foo", "loc": [2, 2]},
        {"_key": "3", "val": 3, "text": "foo", "loc": [3, 3]},
        {"_key": "4", "val": 4, "text": "bar", "loc": [4, 4]},
        {"_key": "5", "val": 5, "text": "bar", "loc": [5, 5]},
        {"_key": "6", "val": 6, "text": "bar", "loc": [5, 5]},
    ]


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


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


@pytest_asyncio.fixture
async def arango_client(url):
    async with ArangoClient(hosts=url) as client:
        yield client


@pytest_asyncio.fixture
async def sys_db(arango_client, sys_db_name, basic_auth_root):
    return await arango_client.db(
        sys_db_name, auth_method="basic", auth=basic_auth_root, verify=False
    )


@pytest_asyncio.fixture
async def superuser(arango_client, sys_db_name, basic_auth_root, token):
    return await arango_client.db(
        sys_db_name, auth_method="superuser", token=token, verify=False
    )


@pytest_asyncio.fixture
async def db(arango_client, sys_db, username, password, cluster):
    tst_db_name = generate_db_name()
    tst_user = UserInfo(
        user=username,
        password=password,
        active=True,
    )
    tst_db_kwargs = dict(name=tst_db_name, users=[tst_user])
    if cluster:
        tst_db_kwargs.update(
            dict(
                replication_factor=3,
                write_concern=2,
            )
        )
    await sys_db.create_database(**tst_db_kwargs)
    yield await arango_client.db(
        tst_db_name,
        auth_method="basic",
        auth=Auth(username=username, password=password),
        verify=False,
    )
    await sys_db.delete_database(tst_db_name)


@pytest_asyncio.fixture
async def bad_db(arango_client):
    return await arango_client.db(
        generate_db_name(),
        auth_method="basic",
        auth=Auth(username="bad_user", password="bad_password"),
        verify=False,
    )


@pytest_asyncio.fixture
async def doc_col(db):
    col_name = generate_col_name()
    yield await db.create_collection(col_name)
    await db.delete_collection(col_name)


@pytest.fixture
def bad_col(db):
    col_name = generate_col_name()
    return db.collection(col_name)


@pytest.fixture
def db_version():
    return global_data.db_version


@pytest_asyncio.fixture(scope="session", autouse=True)
async def teardown():
    yield
    async with ArangoClient(hosts=global_data.url) as client:
        sys_db = await client.db(
            global_data.sys_db_name,
            auth_method="basic",
            auth=Auth(username=global_data.root, password=global_data.password),
            verify=False,
        )

        # Remove all test users.
        tst_users = [
            user["user"]
            for user in await sys_db.users()
            if user["user"].startswith("test_user")
        ]
        await asyncio.gather(*(sys_db.delete_user(user) for user in tst_users))

        # Remove all test databases.
        tst_dbs = [
            db_name
            for db_name in await sys_db.databases()
            if db_name.startswith("test_database")
        ]
        await asyncio.gather(*(sys_db.delete_database(db_name) for db_name in tst_dbs))

        # Remove all test collections.
        tst_cols = [
            col_info.name
            for col_info in await sys_db.collections()
            if col_info.name.startswith("test_collection")
        ]
        await asyncio.gather(
            *(sys_db.delete_collection(col_name) for col_name in tst_cols)
        )
