---
'@workflow/next': patch
---

Fix dev HMR dropping file edits that land while a full rediscovery rebuild is in flight; the post-rebuild baseline refresh absorbed such edits so their watcher events classified as no-ops and the change never reached the manifest.
