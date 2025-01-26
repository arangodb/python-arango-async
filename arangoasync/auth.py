__all__ = [
    "Auth",
    "JwtToken",
]

import time
from dataclasses import dataclass
from typing import Optional

import jwt


@dataclass
class Auth:
    """Authentication details for the ArangoDB instance.

    Attributes:
        username (str): Username.
        password (str): Password.
        encoding (str): Encoding for the password (default: utf-8)
    """

    username: str
    password: str
    encoding: str = "utf-8"


class JwtToken:
    """JWT token.

    Args:
        token (str): JWT token.

    Raises:
        TypeError: If the token type is not str or bytes.
        jwt.exceptions.ExpiredSignatureError: If the token expired.
    """

    def __init__(self, token: str) -> None:
        self._token = token
        self._validate()

    @staticmethod
    def generate_token(
        secret: str | bytes,
        iat: Optional[int] = None,
        exp: int = 3600,
        iss: str = "arangodb",
        server_id: str = "client",
    ) -> "JwtToken":
        """Generate and return a JWT token.

        Args:
            secret (str | bytes): JWT secret.
            iat (int): Time the token was issued in seconds. Defaults to current time.
            exp (int): Time to expire in seconds.
            iss (str): Issuer.
            server_id (str): Server ID.

        Returns:
            str: JWT token.
        """
        iat = iat or int(time.time())
        token = jwt.encode(
            payload={
                "iat": iat,
                "exp": iat + exp,
                "iss": iss,
                "server_id": server_id,
            },
            key=secret,
        )
        return JwtToken(token)

    @property
    def token(self) -> str:
        """Get token."""
        return self._token

    @token.setter
    def token(self, token: str) -> None:
        """Set token.

        Raises:
            jwt.exceptions.ExpiredSignatureError: If the token expired.
        """
        self._token = token
        self._validate()

    def needs_refresh(self, leeway: int = 0) -> bool:
        """Check if the token needs to be refreshed.

        Args:
            leeway (int): Leeway in seconds, before official expiration,
                when to consider the token expired.

        Returns:
            bool: True if the token needs to be refreshed, False otherwise.
        """
        refresh: bool = int(time.time()) > self._token_exp - leeway
        return refresh

    def _validate(self) -> None:
        """Validate the token."""
        if type(self._token) is not str:
            raise TypeError("Token must be str")

        jwt_payload = jwt.decode(
            self._token,
            issuer="arangodb",
            algorithms=["HS256"],
            options={
                "require_exp": True,
                "require_iat": True,
                "verify_iat": True,
                "verify_exp": True,
                "verify_signature": False,
            },
        )

        self._token_exp = jwt_payload["exp"]
