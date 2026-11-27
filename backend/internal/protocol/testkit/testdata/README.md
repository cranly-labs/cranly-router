# Protocol testkit data layout

This directory stores protocol-independent scenarios, hand-written wire fixtures, sanitized traffic recordings, and golden snapshots used by `internal/protocol/testkit`.

Directory layout:

- `scenarios/`: protocol-neutral IR scenario definitions in JSON.
- `wire/<protocol>/request/`: hand-written client request JSON fixtures.
- `wire/<protocol>/stream/`: hand-written upstream SSE stream fixtures.
- `wire/<protocol>/response/`: hand-written non-streaming response JSON fixtures.
- `corpus/clients/<client>/<version>/<scenario>/<n>.request.json`: sanitized client requests captured from `clientlab`.
- `corpus/upstreams/<provider>/<model>/<scenario>/<n>.stream.sse`: sanitized upstream SSE streams captured from `clientlab`.
- `golden/`: IR, Loss, field-census, and canonical wire snapshots.

Do not place `.go` source files in this directory. Raw unredacted captures go to `corpus/_incoming/` (gitignored) and enter version control through `go run ./internal/protocol/testkit/cmd/corpus import`.
