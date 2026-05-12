"""Mint a fresh local-dev API token and print SQL to insert it.

Usage:
    uv run python scripts/seed_dev_token.py | psql "$(supabase status --output env | grep DB_URL | cut -d= -f2- | tr -d '\"')"

Or pipe to a file and run yourself.
"""

from __future__ import annotations

import sys
from uuid import UUID

from upstream_api.security.api_tokens import issue_token

DEV_ACCOUNT_ID = UUID("00000000-0000-0000-0000-000000000001")


def main() -> None:
    token = issue_token(environment="test")

    print("-- Save this plaintext token — it will not be retrievable again:", file=sys.stderr)
    print(f"-- {token.plaintext}", file=sys.stderr)
    print(f"-- (prefix: {token.prefix}, last_four: {token.last_four})", file=sys.stderr)
    print("", file=sys.stderr)

    # SQL on stdout, ready to pipe into psql.
    print(
        "insert into api_tokens "
        "(account_id, name, prefix, hash, last_four, scopes) values ("
        f"'{DEV_ACCOUNT_ID}'::uuid, "
        f"'local-dev', "
        f"'{token.prefix}', "
        f"'{token.hash}', "
        f"'{token.last_four}', "
        f"'{{agent}}'::text[]);"
    )


if __name__ == "__main__":
    main()
