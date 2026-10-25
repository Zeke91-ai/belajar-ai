"""Password hashing for the local UI login. Standard library only.

Stored format: pbkdf2_sha256$<iterations>$<salt_hex>$<hash_hex>
The plain password is never written to disk or the database.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets

ITERATIONS = 600_000
DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "12345678"


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, ITERATIONS)
    return f"pbkdf2_sha256${ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        scheme, iterations_raw, salt_hex, digest_hex = stored.split("$")
    except ValueError:
        return False
    if scheme != "pbkdf2_sha256":
        return False
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), bytes.fromhex(salt_hex), int(iterations_raw)
    )
    return hmac.compare_digest(digest.hex(), digest_hex)
