# internal/

Private, pre-release work that must never reach the public Diploid repo:
unfinished features, notes, and (eventually) any commercial/enterprise-only
code that won't be released under AGPL-3.0.

This directory only exists on `main` (and branches based on it) in the
private `MediaBridge` repo. `scripts/publish-to-diploid.sh` strips it out
of every sync to the `public-release` branch, and `public-release`'s own
`.gitignore` also excludes it as a second layer of defense. Do not
reference paths under `internal/` from any file that does get published
(README.md, CLAUDE.md, code comments, etc.).
