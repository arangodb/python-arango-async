from uuid import uuid4


def generate_db_name():
    """Generate and return a random database name.

    Returns:
        str: Random database name.
    """
    return f"test_database_{uuid4().hex}"


def generate_col_name():
    """Generate and return a random collection name.

    Returns:
        str: Random collection name.
    """
    return f"test_collection_{uuid4().hex}"


def generate_graph_name():
    """Generate and return a random graph name.

    Returns:
        str: Random graph name.
    """
    return f"test_graph_{uuid4().hex}"


def generate_username():
    """Generate and return a random username.

    Returns:
        str: Random username.
    """
    return f"test_user_{uuid4().hex}"


def generate_string():
    """Generate and return a random unique string.

    Returns:
        str: Random unique string.
    """
    return uuid4().hex


def generate_view_name():
    """Generate and return a random view name.

    Returns:
        str: Random view name.
    """
    return f"test_view_{uuid4().hex}"


def generate_analyzer_name():
    """Generate and return a random analyzer name.

    Returns:
        str: Random analyzer name.
    """
    return f"test_analyzer_{uuid4().hex}"


def generate_task_name():
    """Generate and return a random task name.

    Returns:
        str: Random task name.
    """
    return f"test_task_{uuid4().hex}"


def generate_task_id():
    """Generate and return a random task ID.

    Returns:
        str: Random task ID
    """
    return f"test_task_id_{uuid4().hex}"
