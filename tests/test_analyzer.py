import pytest
from packaging import version

from arangoasync.exceptions import (
    AnalyzerCreateError,
    AnalyzerDeleteError,
    AnalyzerGetError,
    AnalyzerListError,
)
from tests.helpers import generate_analyzer_name


@pytest.mark.asyncio
async def test_analyzer_management(db, bad_db, enterprise, db_version):
    analyzer_name = generate_analyzer_name()
    full_analyzer_name = db.name + "::" + analyzer_name
    bad_analyzer_name = generate_analyzer_name()

    # Test create identity analyzer
    result = await db.create_analyzer(analyzer_name, "identity")
    assert result["name"] == full_analyzer_name
    assert result["type"] == "identity"
    assert result["properties"] == {}
    assert result["features"] == []

    # Test create delimiter analyzer
    result = await db.create_analyzer(
        name=generate_analyzer_name(),
        analyzer_type="delimiter",
        properties={"delimiter": ","},
    )
    assert result["type"] == "delimiter"
    assert result["properties"] == {"delimiter": ","}
    assert result["features"] == []

    # Test create duplicate with bad database
    with pytest.raises(AnalyzerCreateError):
        await bad_db.create_analyzer(analyzer_name, "identity")

    # Test get analyzer
    result = await db.analyzer(analyzer_name)
    assert result["name"] == full_analyzer_name
    assert result["type"] == "identity"
    assert result["properties"] == {}
    assert result["features"] == []

    # Test get missing analyzer
    with pytest.raises(AnalyzerGetError):
        await db.analyzer(bad_analyzer_name)

    # Test list analyzers
    result = await db.analyzers()
    assert full_analyzer_name in [a["name"] for a in result]

    # Test list analyzers with bad database
    with pytest.raises(AnalyzerListError):
        await bad_db.analyzers()

    # Test delete analyzer
    assert await db.delete_analyzer(analyzer_name, force=True) is True
    assert full_analyzer_name not in [a["name"] for a in await db.analyzers()]

    # Test delete missing analyzer
    with pytest.raises(AnalyzerDeleteError):
        await db.delete_analyzer(analyzer_name)

    # Test delete missing analyzer with ignore_missing set to True
    assert await db.delete_analyzer(analyzer_name, ignore_missing=True) is False

    # Test create geo_s2 analyzer
    if enterprise:
        analyzer_name = generate_analyzer_name()
        result = await db.create_analyzer(analyzer_name, "geo_s2", properties={})
        assert result["type"] == "geo_s2"
        assert await db.delete_analyzer(analyzer_name)

    if db_version >= version.parse("3.12.0"):
        # Test delimiter analyzer with multiple delimiters
        result = await db.create_analyzer(
            name=generate_analyzer_name(),
            analyzer_type="multi_delimiter",
            properties={"delimiters": [",", "."]},
        )
        assert result["type"] == "multi_delimiter"
        assert result["properties"] == {"delimiters": [",", "."]}

        # Test wildcard analyzer
        analyzer_name = generate_analyzer_name()
        result = await db.create_analyzer(analyzer_name, "wildcard", {"ngramSize": 4})
        assert result["type"] == "wildcard"
        assert result["properties"] == {"ngramSize": 4}
