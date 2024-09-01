import time

import jwt


def generate_jwt(secret, exp=3600) -> str:
    """Generate and return a JWT token.

    Args:
        secret (str | bytes): JWT secret.
        exp (int): Time to expire in seconds.

    Returns:
        str: JWT token.
    """
    now = int(time.time())
    return jwt.encode(
        payload={
            "iat": now,
            "exp": now + exp,
            "iss": "arangodb",
            "server_id": "client",
        },
        key=secret,
    )
