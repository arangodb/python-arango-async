import pytest

from arangoasync.exceptions import (
    TaskCreateError,
    TaskDeleteError,
    TaskGetError,
    TaskListError,
)
from tests.helpers import generate_task_id, generate_task_name


@pytest.mark.asyncio
async def test_task_management(sys_db, bad_db, skip_tests):
    # This test intentionally uses the system database because cleaning up tasks is
    # easier there.

    if "task" in skip_tests:
        pytest.skip("Skipping task tests")

    test_command = 'require("@arangodb").print(params);'

    # Test errors
    with pytest.raises(TaskCreateError):
        await bad_db.create_task(command=test_command)
    with pytest.raises(TaskGetError):
        await bad_db.task("non_existent_task_id")
    with pytest.raises(TaskListError):
        await bad_db.tasks()
    with pytest.raises(TaskDeleteError):
        await bad_db.delete_task("non_existent_task_id")

    # Create a task with a random ID
    task_name = generate_task_name()
    new_task = await sys_db.create_task(
        name=task_name,
        command=test_command,
        params={"foo": 1, "bar": 2},
        offset=1,
    )
    assert new_task["name"] == task_name
    task_id = new_task["id"]
    assert await sys_db.task(task_id) == new_task

    # Delete task
    assert await sys_db.delete_task(task_id) is True

    # Create a task with a specific ID
    task_name = generate_task_name()
    task_id = generate_task_id()
    new_task = await sys_db.create_task(
        name=task_name,
        command=test_command,
        params={"foo": 1, "bar": 2},
        offset=1,
        period=10,
        task_id=task_id,
    )
    assert new_task["name"] == task_name
    assert new_task["id"] == task_id

    # Try to create a duplicate task
    with pytest.raises(TaskCreateError):
        await sys_db.create_task(
            name=task_name,
            command=test_command,
            params={"foo": 1, "bar": 2},
            task_id=task_id,
        )

    # Test get missing task
    with pytest.raises(TaskGetError):
        await sys_db.task(generate_task_id())

    # Test list tasks
    tasks = await sys_db.tasks()
    assert len(tasks) == 1

    # Delete tasks
    assert await sys_db.delete_task(task_id) is True
    assert await sys_db.delete_task(task_id, ignore_missing=True) is False
    with pytest.raises(TaskDeleteError):
        await sys_db.delete_task(task_id)
