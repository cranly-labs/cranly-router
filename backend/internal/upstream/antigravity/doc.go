// Package antigravity forwards Claude and Gemini generation through
// project-scoped v1internal requests. Forwarding keeps provider retry decisions
// separate from client-protocol conversion, while response observation can retain
// usage from the upstream tail after client output fails.
//
// Reading entry points:
//   - execute.go: Selects one exchange, client output, and attempt observations.
//   - payload.go: Wraps v1internal envelopes, injects identity instructions, and cleans schemas.
//   - retry_loop.go: Selects rate-limit waits, capacity retries, and provider switching.
//
// File groups:
//   - oauth.go and client.go: OAuth credentials, endpoint availability, and account queries.
//   - payload.go and gemini_types.go: Request preparation and shared wire aliases.
//   - response_stream.go and response_converted.go: Native and IR-converted response adaptation.
//   - retry*.go, quota_errors.go, and recovery*.go: Provider-local recovery decisions.
//   - probe.go: Connection probes using the forwarding retry adapter.
package antigravity
