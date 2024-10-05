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
