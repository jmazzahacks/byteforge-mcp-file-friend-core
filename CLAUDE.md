# File Friend core — development guide

Independent core repository. Follow ByteForge python-lib-setup and postgres-setup conventions. Import package: `byteforge_mcp_file_friend_core`. Runtime dependencies: immutable GitHub models revision and psycopg2-binary; no Flask or client imports.

Keep transactions short; hash outside locks/transactions; persist deletion intent before filesystem operations. Never replay failed writes automatically. No startup migrations or JSON fallback. Tests use only disposable loopback PostgreSQL; the schema fixture is not a production migration.

Root virtual environment: `bin/activate`. Preserve `.gitignore` when recreating it. Test the installed wheel without source-path overrides. See README for commands and extraction status. Never modify another agent's code or infrastructure. License MIT, GitHub owner jmazzahacks, public visibility approved; current core checkpoint remains local.
