---
reth-engine-tree: patch
---

Fixed fatal error propagation from payload validation in `on_downloaded_block` by properly handling `InsertPayloadError::Payload` variants, treating `NewPayloadError::Other` errors as fatal instead of silently ignoring them.
