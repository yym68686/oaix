import argparse
import os

from oaix_gateway.token_store import import_token_files_sync


def main() -> None:
    parser = argparse.ArgumentParser(description="Import token JSON files into the key pool database")
    parser.add_argument(
        "patterns",
        nargs="*",
        default=["token_*.json"],
        help="Glob patterns for token JSON files",
    )
    parser.add_argument(
        "--database-url",
        help="Override DATABASE_URL for this import run",
    )
    args = parser.parse_args()

    if args.database_url:
        os.environ["DATABASE_URL"] = args.database_url

    imported, failed = import_token_files_sync(args.patterns)
    print(f"Imported: {imported}")
    print(f"Failed: {failed}")


if __name__ == "__main__":
    main()
