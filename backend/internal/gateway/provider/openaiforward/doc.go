// Package openaiforward executes a selected provider attempt for Responses,
// Chat Completions, passthrough, and Anthropic-compatible requests. Preparation
// keeps request identities, client-tool restoration data, and provider-local
// recovery budgets with that attempt; synchronous output readers expose usage
// and timing to the caller before the outer gateway decides completion or
// another provider attempt.
//
// Reading entry points:
//   - prelude.go: request preparation and protocol and transport selection.
//   - responses_converted.go: merged IR Responses conversion for Messages and Chat clients.
//   - http.go: HTTP exchanges and retries on the same provider.
//
// File groups:
//   - requests.go, transform*.go: upstream request construction and body transformation.
//   - passthrough*.go, responses_converted*.go: Responses passthrough and merged IR Responses execution.
//   - raw_chat*.go, raw_fallback_converted.go: native Chat forwarding and merged IR Chat conversion.
//   - anthropic*.go: native and converted Anthropic requests, stream reading, and line pumping.
//   - result_projection.go: conversion between gateway, compatibility, and package result and usage types.
package openaiforward
