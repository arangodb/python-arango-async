__all__ = [
    "Auth",
    "JwtToken",
]

from dataclasses import dataclass

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
        token (str | bytes): JWT token.

    Raises:
        TypeError: If the token type is not str or bytes.
        JWTExpiredError: If the token expired.
    """

    def __init__(self, token: str | bytes) -> None:
        self._token = token
        self._validate()

    @property
    def token(self) -> str | bytes:
        """Get token."""
        return self._token

    @token.setter
    def token(self, token: str | bytes) -> None:
        """Set token.

        Raises:
            jwt.ExpiredSignatureError: If the token expired.
        """
        self._token = token
        self._validate()

    def _validate(self) -> None:
        """Validate the token."""
        if type(self._token) not in (str, bytes):
            raise TypeError("Token must be str or bytes")

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