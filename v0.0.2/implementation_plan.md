# Upgrade and Fix TerSQL v0.0.2

This plan addresses the missing features and stubs that currently exist in the `v0.0.2` refactor of TerSQL compared to `v0.0.1`. Currently, `main.py` is empty, PostgreSQL and MongoDB plugins are stubbed out but not implemented, and several handy features from `v0.0.1` (bookmarks, auto-corrections, and extra aliases) were dropped during the transition to `v0.0.2`.

## User Review Required

> [!IMPORTANT]
> The new plugins require third-party libraries for PostgreSQL and MongoDB. 
> Please confirm if you are okay with me running `pip install psycopg2-binary pymongo` to install the required dependencies for these plugins.

## Proposed Changes

### Entry Point

#### [MODIFY] [main.py](file:///c:/Users/Sayan%20Dutta/Downloads/TerSQL/main.py)
* Fill this empty file to act as the true entry point, ensuring it imports and calls the `main()` function from `TerSQL.py`.

---

### Database Plugins

#### [NEW] [postgre.py](file:///c:/Users/Sayan%20Dutta/Downloads/TerSQL/plugins/postgre.py)
* Create the PostgreSQL backend plugin by inheriting from `BaseDB` present in `plugins/base.py`.
* Will utilize `psycopg2` for connection pooling, cursor handling, streaming, and database backup utilizing `pg_dump`.
* Add `PluginRegistry.register("postgresql")`.

#### [NEW] [mongodb.py](file:///c:/Users/Sayan%20Dutta/Downloads/TerSQL/plugins/mongodb.py)
* Create the MongoDB backend plugin by inheriting from `BaseDB`.
* Will utilize `pymongo` to bridge SQL-like interactions via the NLP engine, execute basic MongoDB queries, schemas based on collections.
* Add `PluginRegistry.register("mongodb")`.

---

### Core Enhancements (Restoring v0.0.1 features)

#### [MODIFY] [Core.py](file:///c:/Users/Sayan%20Dutta/Downloads/TerSQL/Core.py)
* **Bookmarks**: Add logic to load/save bookmarks from `~/.tersql_bookmarks.json`, and expose corresponding methods (`add_bookmark`, `list_bookmarks`, `run_bookmark`, `del_bookmark`).
* **Auto-Corrections**: Hook in the `SQL_FIXES` regex logic from v0.0.1 into the `run()` execution path to silently rewrite commands like `show table` to `SHOW TABLES;` prior to sending it to the new NLP engine.

#### [MODIFY] [TerSQL.py](file:///c:/Users/Sayan%20Dutta/Downloads/TerSQL/TerSQL.py)
* **Aliases**: Map `.q`, `.st`, `.bm`, `.fav`, etc. to their respective dot commands.
* **Dot Commands**: Register `.bookmark`, `.bookmarks`, `.run`, `.delbookmark` and route them to call the newly added logic in `TerSQLCore`. Update `DOT_COMMANDS` list for syntax hilighter completion. 

## Open Questions

1. Which distribution of `psycopg2` do you prefer? For portability, `psycopg2-binary` is typically chosen, but `psycopg` (v3) is the newer standard.
2. Are there any other specific features from `v0.0.1` (e.g. `pager`, `tee`) that you noticed missing and would like restored immediately as part of this upgrade?

## Verification Plan

### Automated Tests
* Run `python main.py -h` to verify the entry point.
* Enter the REPL to ensure `.bookmark`, `.bookmarks`, `.run`, and aliases operate correctly.
* Manually load the PostgreSQL and MongoDB plugins and run a basic liveness check via `.status`.
