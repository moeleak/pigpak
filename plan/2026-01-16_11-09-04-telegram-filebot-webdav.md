---
mode: plan
cwd: /Users/lolimaster/Projects/pigpak
task: Build a Go Telegram file bot using Telegram cache, with directory management, SQLite metadata, WebDAV, and Docker deployment.
complexity: complex
tool: mcp__sequential-thinking__sequentialthinking
total_thoughts: 10
created_at: 2026-01-16T11:09:04+08:00
---

# Plan: Telegram File Bot with WebDAV

Task Overview
- Build a Go Telegram bot that stores only Telegram file cache hashes and a virtual file tree in SQLite, with interactive keyboard-based UI.
- Provide Dockerfile and .env.example (bot token, bot API URL, mount dir, webdavEnable), plus optional WebDAV server mirroring the same file tree.
- Ensure downloads are streamed from Telegram back to users without server-side file persistence.

Execution Plan
1. Baseline the repo structure, decide module layout, and identify where to place bot, DB, and WebDAV components.
2. Define the data model (files, directories, relationships, Telegram file_id/cache hash, metadata) and design SQLite schema and migrations.
3. Implement core bot commands and keyboard flows (browse tree, upload, share links, retention actions).
4. Wire Telegram file ingest: save metadata, link to directories, persist file_id/hash only, and handle size/type limits.
5. Implement download streaming: fetch file via Telegram API and stream to user without storing on disk.
6. Build directory management actions (create/move/rename/delete), with consistent pagination and keyboard navigation.
7. Add WebDAV server mode that maps the virtual tree to Telegram-backed files, with streaming for GET/PUT and shared metadata paths.
8. Add configuration handling (.env.example, config loader), Dockerfile, and runtime volume and mount settings.
9. Add logging, error handling, and basic tests for DB and tree operations; validate manual flows.

Risks and Notes
- Telegram API limits and file size constraints may require chunking or deferred retries.
- WebDAV semantics (PUT/PROPFIND/MKCOL) must map correctly to the virtual tree and Telegram cache IDs.
- Streaming without temp files must handle retries and partial reads reliably.
- Access control for WebDAV and bot share links needs explicit scope.
- The UI screenshot implies UX expectations that may require custom keyboard layouts.

References
- codex-clipboard-xfsvD4.png (UI reference)
