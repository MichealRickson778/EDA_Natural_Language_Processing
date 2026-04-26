import os
import logging
import time
import re

from core.exceptions import SecretsError

SECRET_PATTERNS = [
    r'key=\S+',
    r'token=\S+', 
    r'password=\S+',
    r'AIzaSy\S+',
]


class SecretHandle:
    """wraps a screct value it appering in logs or repr()"""
    def __init__(self, name: str, value: str, audit_logger):
        self._name = name
        self._value = value
        self._audit_logger = audit_logger

    def get(self) -> str:
        # log the access then return self._value
        self._audit_logger.info(f"Secret accessed: {self._name}")
        return self._value

    def zero(self) -> None:
        # overwrite self._value with '\x00' * len(self._value)
        self._value = "\x00" * len(self._value)

    def __repr__(self):
        return f"SecretHandle(name={self._name}, value = REDACTED)"

    def __str__(self):
        return f"SecretHandle({self._name})"

    def __format__(self, spec):
        return f"[REDACTED:{self._name}]"


class SecretsManager:
    def __init__(self,
                 vault_addr: str,
                 vault_token: str,
                 cache_ttl_s: int,
                 enabled: bool):
        self.vault_addr = vault_addr
        self.vault_token = vault_token
        self.cache_ttl_s = cache_ttl_s
        self.enabled = enabled
        self._cache = {}  # stores {secret_name: (value, fetched_at)
        self._audit_logger = logging.getLogger("audit")

    def get(self, name: str) -> SecretHandle:
        # Step 1 — check cache
        if self._is_cache_valid(name):
            value, _ = self._cache[name]
            return SecretHandle(name, value, self._audit_logger)

        # Step 2 — fetch from Vault or env
        if self.enabled:
            value = self._fetch_from_vault(name)
        else:
            value = self._fetch_from_env(name)

        # Step 3 — store in cache
        self._cache[name] = (value, time.time())

        # Step 4 — return SecretHandle
        return SecretHandle(name, value, self._audit_logger)

    def _is_cache_valid(self, name: str) -> bool:
        if name not in self._cache:
            return False
        value, fetched_at = self._cache[name]
        return time.time() - fetched_at < self.cache_ttl_s
    
    def invalidate_cache(self, name: str) -> None:
        """Remove a secret from cache — called on 401 to force re-fetch."""
        if name in self._cache:
            del self._cache[name]

    def _fetch_from_env(self, name: str) -> str:
        value = os.getenv(name.upper())
        if value is None:
            raise SecretsError(f"Secret {name} not found in environment",
                         details={"name": name})
        return value
    
    def _fetch_from_vault(self, name: str) -> str:
        # TODO: implement when Vault is available
        # requires: pip install hvac
        # import hvac
        # client = hvac.Client(url=self.vault_addr, token=self.vault_token)
        # response = client.secrets.kv.read_secret_version(path=name)
        # return response['data']['data'][name]
        raise SecretsError(f"Vault not configured — use env vars in dev",
                     details={"name": name})
    
def sanitise_exception(exc: Exception) -> str:
    msg = str(exc)
    for pattern in SECRET_PATTERNS:
        # use re.sub to replace each pattern with '[REDACTED]'
        msg = re.sub(pattern, '[REDACTED]', msg)
    return msg



