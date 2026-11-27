// Package testkit provides embedded fixture indexing, golden assertions,
// lossless JSON diffing, independent protocol validators, client turn
// simulators, and loss-to-essence rules for protocol conversion tests.
//
// Start here:
//   - corpus.go: loads embedded wire fixtures, recorded traffic, and source files.
//   - request_rules.go: dispatches strict wire-request validators across protocols.
//   - essence.go: applies reported ir.Loss records to semantic EssenceDoc summaries.
//
// File groups:
//   - corpus.go and corpus_import.go: fixture queries, capture sanitization, and corpus verification.
//   - golden.go and jsondiff.go: golden snapshot management and arbitrary-precision JSON path diffs.
//   - grammar.go, grammar_messages.go, grammar_responses.go, grammar_chat.go, and grammar_gemini.go: SSE stream state-machine validators.
//   - request_rules.go, request_rules_messages.go, request_rules_responses.go, request_rules_chat.go, and request_rules_gemini.go: strict upstream request validators.
//   - clientsim.go, clientsim_messages.go, clientsim_responses.go, clientsim_chat.go, and clientsim_gemini.go: multi-turn client SDK replay simulators.
//   - essence.go and script.go: loss-to-essence reconciliation and scripted upstream turn matching types.
package testkit
