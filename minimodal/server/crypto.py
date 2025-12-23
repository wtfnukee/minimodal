"""Encryption utilities for secrets management."""

import base64
import os
from pathlib import Path

from cryptography.fernet import Fernet


# Key file location
KEY_FILE = Path(os.environ.get("MINIMODAL_KEY_FILE", ".minimodal_key"))


def get_or_create_key() -> bytes:
    """
    Get encryption key from environment, file, or generate a new one.

    Priority:
    1. MINIMODAL_ENCRYPTION_KEY environment variable
    2. Key file at MINIMODAL_KEY_FILE location
    3. Generate new key and save to file

    Returns:
        Fernet-compatible encryption key
    """
    # Check environment variable first
    env_key = os.environ.get("MINIMODAL_ENCRYPTION_KEY")
    if env_key:
        return base64.urlsafe_b64decode(env_key)

    # Check key file
    if KEY_FILE.exists():
        with open(KEY_FILE, "rb") as f:
            return f.read()

    # Generate new key
    key = Fernet.generate_key()

    # Save to file
    with open(KEY_FILE, "wb") as f:
        f.write(key)

    # Set restrictive permissions
    os.chmod(KEY_FILE, 0o600)

    return key


def get_fernet() -> Fernet:
    """Get a Fernet instance with the current key."""
    key = get_or_create_key()
    return Fernet(key)


def encrypt_value(value: str) -> str:
    """
    Encrypt a string value.

    Args:
        value: Plain text value to encrypt

    Returns:
        Base64-encoded encrypted value
    """
    fernet = get_fernet()
    encrypted = fernet.encrypt(value.encode("utf-8"))
    return base64.b64encode(encrypted).decode("utf-8")


def decrypt_value(encrypted: str) -> str:
    """
    Decrypt an encrypted value.

    Args:
        encrypted: Base64-encoded encrypted value

    Returns:
        Decrypted plain text value
    """
    fernet = get_fernet()
    encrypted_bytes = base64.b64decode(encrypted)
    decrypted = fernet.decrypt(encrypted_bytes)
    return decrypted.decode("utf-8")
