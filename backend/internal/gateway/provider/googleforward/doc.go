// Package googleforward executes selected-provider Gemini and Antigravity attempts.
// Each request retains its own tool and image observations while injected provider
// services supply credentials, transport, and health feedback for platform retry
// and recovery. HTTP presentation remains synchronous with the calling request.
//
// Start with:
//   - gemini_attempts.go: native GenerateContent forwarding and shared retry helpers.
//   - gemini_converted.go: IR-based Gemini conversion, recovery, and response reading.
//   - antigravity_converted.go: IR-based Antigravity conversion, recovery, and response reading.
//
// File groups:
//   - gemini_*.go: Gemini native forwarding, IR conversion, and responses.
//   - antigravity_*.go: Antigravity native forwarding, IR conversion, signature recovery, and responses.
//   - model_attributes.go: native sampling and modality parameters from resolved model attributes.
package googleforward
