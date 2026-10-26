"""Fails when a service throws an error code that nexus-frontend/API.md never mentions.

The front end compares error codes as plain strings (e.g. INSUFFICIENT_STOCK), so a code that is
thrown but not documented is one the front-end team cannot know about. Run from anywhere; the
service repos and nexus-frontend are expected as siblings of this repo (the layout of C:/FPT).

    python scripts/check_error_codes.py            # exit 1 and list the missing codes
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
SERVICES = [
    "user-service", "catalog-service", "auction-service",
    "commerce-service", "fulfillment-service", "notification-service", "api-gateway",
]
API_DOC = ROOT / "nexus-frontend" / "API.md"

# Never reaches a client: raised while reading a Kafka message, not while answering a request.
NOT_HTTP = {"MALFORMED_EVENT"}

# new ConflictException("CODE", ...) / super("CODE", ...) inside a DomainException subclass,
# possibly with the code on the next line.
THROWN = re.compile(r'(?:new\s+\w*Exception|super)\(\s*"([A-Z][A-Z0-9_]{2,})"')


def thrown_codes():
    found = {}
    for service in SERVICES:
        for java in (ROOT / service / "src" / "main").rglob("*.java"):
            for code in THROWN.findall(java.read_text(encoding="utf-8")):
                found.setdefault(code, set()).add(service)
    return found


def main():
    if not API_DOC.exists():
        sys.exit(f"{API_DOC} not found")
    documented = set(re.findall(r"[A-Z][A-Z0-9_]{2,}", API_DOC.read_text(encoding="utf-8")))
    codes = thrown_codes()
    missing = sorted(c for c in codes if c not in documented and c not in NOT_HTTP)

    print(f"{len(codes)} error codes thrown, {len(codes) - len(missing)} documented in API.md")
    for code in missing:
        print(f"  MISSING: {code}  ({', '.join(sorted(codes[code]))})")
    sys.exit(1 if missing else 0)


if __name__ == "__main__":
    main()
