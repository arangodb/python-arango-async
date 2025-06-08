import pytest

from arangoasync import errno
from arangoasync.exceptions import (
    ViewCreateError,
    ViewDeleteError,
    ViewGetError,
    ViewListError,
    ViewRenameError,
    ViewReplaceError,
    ViewUpdateError,
)
from tests.helpers import generate_view_name


@pytest.mark.asyncio
async def test_view_management(db, bad_db, doc_col, cluster):
    # Create a view
    view_name = generate_view_name()
    bad_view_name = generate_view_name()
    view_type = "arangosearch"

    result = await db.create_view(
        view_name,
        view_type,
        {"consolidationIntervalMsec": 50000, "links": {doc_col.name: {}}},
    )
    assert "id" in result
    assert result["name"] == view_name
    assert result["type"] == view_type
    assert result["consolidationIntervalMsec"] == 50000
    assert doc_col.name in result["links"]

    # Create view with bad database
    with pytest.raises(ViewCreateError):
        await bad_db.create_view(
            view_name,
            view_type,
            {"consolidationIntervalMsec": 50000, "links": {doc_col.name: {}}},
        )

    view_id = result["id"]

    # Test create duplicate view
    with pytest.raises(ViewCreateError) as err:
        await db.create_view(view_name, view_type, {"consolidationIntervalMsec": 50000})
    assert err.value.error_code == errno.DUPLICATE_NAME

    # Test get view (properties)
    view = await db.view(view_name)
    assert view["id"] == view_id
    assert view["name"] == view_name
    assert view["type"] == view_type
    assert view["consolidationIntervalMsec"] == 50000

    # Test get missing view
    with pytest.raises(ViewGetError) as err:
        await db.view(bad_view_name)
    assert err.value.error_code == errno.DATA_SOURCE_NOT_FOUND

    # Test get view info
    view_info = await db.view_info(view_name)
    assert view_info["id"] == view_id
    assert view_info["name"] == view_name
    assert view_info["type"] == view_type
    assert "consolidationIntervalMsec" not in view_info
    with pytest.raises(ViewGetError) as err:
        await db.view_info(bad_view_name)
    assert err.value.error_code == errno.DATA_SOURCE_NOT_FOUND

    # Test list views
    result = await db.views()
    assert len(result) == 1
    view = result[0]
    assert view["id"] == view_id
    assert view["name"] == view_name
    assert view["type"] == view_type

    # Test list views with bad database
    with pytest.raises(ViewListError) as err:
        await bad_db.views()
    assert err.value.error_code == errno.FORBIDDEN

    # Test replace view
    view = await db.replace_view(view_name, {"consolidationIntervalMsec": 40000})
    assert view["id"] == view_id
    assert view["name"] == view_name
    assert view["type"] == view_type
    assert view["consolidationIntervalMsec"] == 40000

    # Test replace view with bad database
    with pytest.raises(ViewReplaceError) as err:
        await bad_db.replace_view(view_name, {"consolidationIntervalMsec": 7000})
    assert err.value.error_code == errno.FORBIDDEN

    # Test update view
    view = await db.update_view(view_name, {"consolidationIntervalMsec": 70000})
    assert view["id"] == view_id
    assert view["name"] == view_name
    assert view["type"] == view_type
    assert view["consolidationIntervalMsec"] == 70000

    # Test update view with bad database
    with pytest.raises(ViewUpdateError) as err:
        await bad_db.update_view(view_name, {"consolidationIntervalMsec": 80000})
    assert err.value.error_code == errno.FORBIDDEN

    # Test rename view
    new_view_name = generate_view_name()
    if cluster:
        with pytest.raises(ViewRenameError):
            await db.rename_view(view_name, new_view_name)
        new_view_name = view_name
    else:
        await db.rename_view(view_name, new_view_name)
        result = await db.views()
        assert len(result) == 1
        view = result[0]
        assert view["id"] == view_id
        assert view["name"] == new_view_name

        # Test rename missing view
        with pytest.raises(ViewRenameError) as err:
            await db.rename_view(bad_view_name, view_name)
        assert err.value.error_code == errno.DATA_SOURCE_NOT_FOUND

    # Test delete view
    assert await db.delete_view(new_view_name) is True
    assert len(await db.views()) == 0

    # Test delete missing view
    with pytest.raises(ViewDeleteError) as err:
        await db.delete_view(new_view_name)
    assert err.value.error_code == errno.DATA_SOURCE_NOT_FOUND

    # Test delete missing view with ignore_missing set to True
    assert await db.delete_view(view_name, ignore_missing=True) is False
