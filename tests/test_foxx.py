import asyncio
import json

import aiofiles
import aiohttp
import pytest

from arangoasync.exceptions import (
    FoxxCommitError,
    FoxxConfigGetError,
    FoxxConfigReplaceError,
    FoxxConfigUpdateError,
    FoxxDependencyGetError,
    FoxxDependencyReplaceError,
    FoxxDependencyUpdateError,
    FoxxDevModeDisableError,
    FoxxDevModeEnableError,
    FoxxDownloadError,
    FoxxReadmeGetError,
    FoxxScriptListError,
    FoxxScriptRunError,
    FoxxServiceCreateError,
    FoxxServiceDeleteError,
    FoxxServiceGetError,
    FoxxServiceListError,
    FoxxServiceReplaceError,
    FoxxServiceUpdateError,
    FoxxSwaggerGetError,
    FoxxTestRunError,
)
from tests.helpers import generate_service_mount

service_name = "test"


@pytest.mark.asyncio
async def test_foxx(db, bad_db, skip_tests, foxx_path):
    if "foxx" in skip_tests:
        pytest.skip("Skipping Foxx tests")

    # Test errors
    with pytest.raises(FoxxServiceGetError):
        await bad_db.foxx.service(service_name)
    with pytest.raises(FoxxServiceListError):
        await bad_db.foxx.services()
    with pytest.raises(FoxxServiceCreateError):
        await bad_db.foxx.create_service(
            mount=generate_service_mount(),
            service={},
            headers={"content-type": "application/zip"},
        )
    with pytest.raises(FoxxServiceDeleteError):
        await bad_db.foxx.delete_service(service_name)
    with pytest.raises(FoxxServiceReplaceError):
        await bad_db.foxx.replace_service(
            mount=generate_service_mount(),
            service={},
        )
    with pytest.raises(FoxxServiceUpdateError):
        await bad_db.foxx.update_service(mount=generate_service_mount(), service={})
    with pytest.raises(FoxxConfigGetError):
        await bad_db.foxx.config("foo")
    with pytest.raises(FoxxConfigReplaceError):
        await bad_db.foxx.replace_config(mount="foo", options={})
    with pytest.raises(FoxxConfigUpdateError):
        await bad_db.foxx.update_config(mount="foo", options={})
    with pytest.raises(FoxxDependencyGetError):
        await bad_db.foxx.dependencies("foo")
    with pytest.raises(FoxxDependencyReplaceError):
        await bad_db.foxx.replace_dependencies(mount="foo", options={})
    with pytest.raises(FoxxDependencyUpdateError):
        await bad_db.foxx.update_dependencies(mount="foo", options={})
    with pytest.raises(FoxxDevModeEnableError):
        await bad_db.foxx.enable_development("foo")
    with pytest.raises(FoxxDevModeDisableError):
        await bad_db.foxx.disable_development("foo")
    with pytest.raises(FoxxReadmeGetError):
        await bad_db.foxx.readme("foo")
    with pytest.raises(FoxxSwaggerGetError):
        await bad_db.foxx.swagger("foo")
    with pytest.raises(FoxxDownloadError):
        await bad_db.foxx.download("foo")
    with pytest.raises(FoxxCommitError):
        await bad_db.foxx.commit()

    services = await db.foxx.services()
    assert isinstance(services, list)

    # Service as a path
    mount1 = generate_service_mount()
    service1 = {
        "source": foxx_path,
        "configuration": {"LOG_LEVEL": "info"},
        "dependencies": {},
    }
    service_info = await db.foxx.create_service(mount=mount1, service=service1)
    assert service_info["mount"] == mount1

    # Service as a FormData
    mount2 = generate_service_mount()
    service2 = aiohttp.FormData()
    service2.add_field(
        "source",
        open(f".{foxx_path}", "rb"),
        filename="service.zip",
        content_type="application/zip",
    )
    service2.add_field("configuration", json.dumps({"LOG_LEVEL": "info"}))
    service2.add_field("dependencies", json.dumps({}))
    service_info = await db.foxx.create_service(
        mount=mount2, service=service2, headers={"content-type": "multipart/form-data"}
    )
    assert service_info["mount"] == mount2

    # Service as raw data
    mount3 = generate_service_mount()
    async with aiofiles.open(f".{foxx_path}", mode="rb") as f:
        service3 = await f.read()
    service_info = await db.foxx.create_service(
        mount=mount3, service=service3, headers={"content-type": "application/zip"}
    )
    assert service_info["mount"] == mount3

    # Delete service
    await db.foxx.delete_service(mount3)

    # Replace service
    service4 = {
        "source": foxx_path,
        "configuration": {"LOG_LEVEL": "info"},
        "dependencies": {},
    }
    service_info = await db.foxx.replace_service(mount=mount2, service=service4)
    assert service_info["mount"] == mount2

    async with aiofiles.open(f".{foxx_path}", mode="rb") as f:
        service5 = await f.read()
    service_info = await db.foxx.replace_service(
        mount=mount1, service=service5, headers={"content-type": "application/zip"}
    )
    assert service_info["mount"] == mount1

    # Update service
    service6 = {
        "source": foxx_path,
        "configuration": {"LOG_LEVEL": "debug"},
        "dependencies": {},
    }
    service_info = await db.foxx.update_service(mount=mount1, service=service6)
    assert service_info["mount"] == mount1

    services = await db.foxx.services(exclude_system=True)
    assert len(services) == 2

    # Configuration
    config = await db.foxx.config(mount1)
    assert isinstance(config, dict)
    config = await db.foxx.replace_config(mount=mount1, options={})
    assert isinstance(config, dict)
    config = await db.foxx.replace_config(mount=mount1, options={})
    assert isinstance(config, dict)

    # Dependencies
    config = await db.foxx.dependencies(mount1)
    assert isinstance(config, dict)
    config = await db.foxx.replace_dependencies(mount=mount1, options={})
    assert isinstance(config, dict)
    config = await db.foxx.update_dependencies(mount=mount1, options={})
    assert isinstance(config, dict)

    # Scripts
    scripts = await db.foxx.scripts(mount1)
    assert "setup" in scripts
    assert "teardown" in scripts

    # List missing service scripts
    with pytest.raises(FoxxScriptListError):
        await db.foxx.scripts("invalid_mount")

    # Run service script
    assert await db.foxx.run_script(mount1, "setup", []) == {}
    assert await db.foxx.run_script(mount2, "teardown", []) == {}

    # Run missing service script
    with pytest.raises(FoxxScriptRunError):
        await db.foxx.run_script(mount1, "invalid", ())

    # Run tests on service
    result = await db.foxx.run_tests(
        mount=mount1, reporter="suite", idiomatic=True, filter="science"
    )
    result = json.loads(result)
    assert "stats" in result
    assert "tests" in result
    assert "suites" in result

    result = await db.foxx.run_tests(
        mount=mount2, reporter="stream", output_format="x-ldjson"
    )
    for result_part in result.split("\r\n"):
        if len(result_part) == 0:
            continue
        assert result_part.startswith("[")
        assert result_part.endswith("]")

    result = await db.foxx.run_tests(
        mount=mount1, reporter="stream", output_format="text"
    )
    assert result.startswith("[")
    assert result.endswith("]") or result.endswith("\r\n")

    result = await db.foxx.run_tests(
        mount=mount2, reporter="xunit", output_format="xml"
    )
    assert result.startswith("[")
    assert result.endswith("]") or result.endswith("\r\n")

    # Run tests on missing service
    with pytest.raises(FoxxTestRunError):
        await db.foxx.run_tests("foo")

    # Development mode
    result = await db.foxx.enable_development(mount1)
    assert result["mount"] == mount1
    result = await db.foxx.disable_development(mount1)
    assert result["mount"] == mount1

    # Readme
    result = await db.foxx.readme(mount1)
    assert isinstance(result, str)

    # Swagger
    result = await db.foxx.swagger(mount1)
    assert isinstance(result, dict)

    # Download service
    result = await db.foxx.download(mount1)
    assert isinstance(result, bytes)

    # Commit
    await db.foxx.commit(replace=True)

    # Delete remaining services
    await asyncio.gather(
        db.foxx.delete_service(mount1),
        db.foxx.delete_service(mount2),
    )
