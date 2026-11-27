// Package forward prepares one provider attempt, coordinates IR protocol
// exchanges, and returns usage or failure observations to the surrounding
// gateway loop. The loop owns provider selection and remaining retry budgets;
// forwarding retains logical model input separately from selected upstream names
// and raw response declarations. HTTP, credential, health, and output operations
// are supplied by request adapters so these flows run with isolated attempt state.
//
// Response reading can retain usage alongside a failure, and a selected failover
// signal changes whether that partial result is usable. Protocol conversion
// algorithms live in protocol/convert, while this package orders provider
// preparation, signature-rejection retries, and health-before-display decisions.
//
// Reading entrypoints:
//   - messages.go orders Messages preparation, HTTP decisions, and stream outcomes.
//   - conversion.go prepares OpenAI requests for Messages targets through IR exchanges.
//   - failure.go defines classifications and retry instructions consumed by gateway/failover.
//
// File groups:
//   - messages*.go, gemini*.go, and count*.go pair execution flows with adapter operations.
//   - anthropic*.go handles API-key passthrough and health-before-display errors.
//   - conversion.go, conversion_contract.go, and mimic.go handle IR request conversion,
//     adapter contracts, and OAuth client preparation.
//   - failure.go, grok_credential_failure.go, invalid_json.go, qoder_error.go, and
//     upstream_warning.go carry classified failures or captured warning evidence.
//   - result.go, openai_result.go, messages_result.go, and response_observer.go keep
//     usage, replay values, and raw model/tier observations for completion.
package forward
