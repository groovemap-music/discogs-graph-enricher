"""Configuration for the GrooveMap discogs-graph-enricher service."""

from dataclasses import dataclass, field
from os import getenv
from typing import cast

from common.config import _build_amqp_url, _build_neo4j_uri, get_secret


DEFAULT_HEALTH_PORT = 8001


def _health_port_from_env() -> int:
    """Read HEALTH_PORT, defaulting to the role port when unset; reject anything else invalid."""
    raw = getenv("HEALTH_PORT")
    if raw is None:
        return DEFAULT_HEALTH_PORT
    try:
        port = int(raw.strip())
    except ValueError:
        raise ValueError(f"Invalid HEALTH_PORT {raw!r}: must be an integer between 1 and 65535") from None
    if not 1 <= port <= 65535:
        raise ValueError(f"Invalid HEALTH_PORT {raw!r}: must be an integer between 1 and 65535")
    return port


@dataclass(frozen=True)
class GraphinatorConfig:
    """Configuration for the Discogs graph enricher."""

    amqp_connection: str = field(repr=False)
    neo4j_host: str
    neo4j_username: str
    neo4j_password: str = field(repr=False)
    health_port: int = DEFAULT_HEALTH_PORT

    @classmethod
    def from_env(cls) -> GraphinatorConfig:
        """Create configuration from environment variables."""
        neo4j_username = get_secret("NEO4J_USERNAME")
        neo4j_password = get_secret("NEO4J_PASSWORD")
        missing_vars = [
            name
            for name, value in (
                ("NEO4J_HOST", getenv("NEO4J_HOST")),
                ("NEO4J_USERNAME", neo4j_username),
                ("NEO4J_PASSWORD", neo4j_password),
            )
            if not value
        ]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        return cls(
            amqp_connection=_build_amqp_url(),
            neo4j_host=_build_neo4j_uri(),
            neo4j_username=cast("str", neo4j_username),
            neo4j_password=cast("str", neo4j_password),
            health_port=_health_port_from_env(),
        )
