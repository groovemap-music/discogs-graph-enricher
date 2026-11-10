"""Compare the pinned placeholder-label constants with catalog-api itself. Needs the network.

Two checks against https://github.com/groovemap-music/catalog-api (public, read anonymously):

1. At ``producer_commit``, the producer file hashes to ``producer_sha256`` and carries exactly
   the pinned constants: the pin says what that commit says.
2. On catalog-api's default branch the constants are still the pinned ones. If catalog-api has
   changed its rule, this fails: update graphinator/label_longevity_rank.py, re-pin
   contracts/label-placeholders/v1/source.json to the new commit, and bump STATS_VERSION so the
   ranks are rewritten.

Not part of ``just check``, which stays offline; run it with ``just contract-upstream``.
"""

import sys
from hashlib import sha256
from urllib.request import urlopen

from label_placeholders import constants_in, pin


RAW = "https://raw.githubusercontent.com/groovemap-music/catalog-api/{ref}/{path}"


def fetch(ref: str, path: str) -> bytes:
    """Return ``path`` at ``ref`` from catalog-api."""
    with urlopen(RAW.format(ref=ref, path=path), timeout=30) as response:  # noqa: S310 - fixed https URL
        body: bytes = response.read()
    return body


def main() -> int:
    record = pin()
    pinned = record["constants"]
    at_commit = fetch(record["producer_commit"], record["producer_path"])
    failures = []
    if sha256(at_commit).hexdigest() != record["producer_sha256"]:
        failures.append(f"{record['producer_path']} at {record['producer_commit']} does not hash to producer_sha256")
    if constants_in(at_commit.decode()) != pinned:
        failures.append(f"the pin does not match catalog-api at {record['producer_commit']}")
    on_main = constants_in(fetch("main", record["producer_path"]).decode())
    if on_main != pinned:
        failures.append(f"catalog-api main has changed its placeholder rule: {on_main} != {pinned}")
    for failure in failures:
        print(f"❌ {failure}", file=sys.stderr)
    if not failures:
        print(f"✅ placeholder-label constants match catalog-api at {record['producer_commit'][:12]} and on main")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
