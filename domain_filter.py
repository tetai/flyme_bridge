"""Domain filter for flyme sync."""

from __future__ import annotations

from typing import Iterable


class DomainFilter:
    """Filter entities by excluded domains."""

    def __init__(self) -> None:
        self._excluded_domains: set[str] = set()

    def update_excluded_domains(self, excluded_domains: Iterable[str]) -> None:
        """Update excluded domains."""
        self._excluded_domains = {domain for domain in excluded_domains if domain}

    def is_entity_included(self, entity_id: str | None) -> bool:
        """Check whether the entity should be included."""
        if not entity_id or "." not in entity_id:
            return False
        domain = entity_id.split(".", 1)[0]
        return domain not in self._excluded_domains
