package architecture

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// policy_test.go checks architecture dependency boundaries and scanner coverage.
// Positive fixtures permit reviewed collaborations, while neighboring-file
// fixtures keep narrow allowances from widening module or standard-library
// access. Consumer checks require every configured exception to retain a real import.

// TestDependencyBoundaries checks permitted imports and forbidden dependencies
// for each directory role and consolidated technical library boundary.
// File-specific allowances must not authorize nearby files, production counterparts, or broader target subpackages.
func TestDependencyBoundaries(t *testing.T) {
	cases := []struct {
		file     string // importing source file path
		imported string // candidate dependency import path
		allowed  bool   // expected dependency admission result
	}{
		{"migrations/platform_independent_pricing_integration_test.go", modulePath + "/internal/billing/pricing", false},
		{"migrations/helpers_test.go", modulePath + "/internal/billing/pricing", false},
		{"migrations/pricing_merge_test.go", modulePath + "/internal/billing/pricing", false},
		{"migrations/new_test.go", modulePath + "/internal/billing/pricing", false},
		{"migrations/platform_independent_pricing_integration_test.go", modulePath + "/internal/routing/postgres", false},
		{"internal/provider/new.go", modulePath + "/internal/billing", true},
		{"internal/provider/helpers/new.go", modulePath + "/internal/billing", true},
		{"internal/provider/helpers/store.go", "database/sql", false},
		{"internal/provider/new.go", "github.com/gin-gonic/gin", false},
		{"internal/provider/new.go", modulePath + "/internal/provider/provider", false},
		{"internal/provider/new.go", modulePath + "/internal/server/httpx", false},
		{"internal/provider/httpapi/new.go", modulePath + "/internal/provider/postgres", false},
		{"internal/provider/rediscache/new.go", modulePath + "/ent", false},
		{"internal/provider/postgres/new.go", modulePath + "/internal/provider/rediscache", false},
		{"internal/provider/postgres/new.go", "database/sql", true},
		{"internal/search/postgres/new.go", "database/sql", true},
		{"internal/search/new.go", modulePath + "/internal/search/postgres", false},
		{"internal/plugin/httpapi/helpers_test.go", modulePath + "/internal/identity", true},
		{"internal/plugin/httpapi/proxy.go", modulePath + "/internal/identity", false},
		{"internal/plugin/runtime.go", modulePath + "/plugin-sdk/go/pluginpb/v1", true},
		{"internal/plugin/catalog.go", modulePath + "/plugin-sdk/go/index", true},
		{"internal/plugin/service.go", modulePath + "/plugin-sdk/go/sdk", false},
		{"internal/plugin/service.go", modulePath + "/plugin-sdk/go/pkgfmt", false},
		{"internal/plugin/service.go", "github.com/hashicorp/go-plugin", false},
		{"internal/plugin/provider/process.go", "github.com/hashicorp/go-plugin", true},
		{"internal/plugin/provider/process.go", modulePath + "/plugin-sdk/go/sdk", true},
		{"internal/search/helper/postgres/new.go", "database/sql", false},
		{"internal/protocol/openai/helpers/new.go", "os", false},
		{"internal/protocol/openai/new_test.go", "net/http", false},
		{"internal/protocol/openai/new.go", "bytes", true},
		{"internal/protocol/systemone/payload.go", "mime", true},
		{"internal/protocol/systemone/payload.go", "mime/multipart", true},
		{"internal/protocol/systemone/payload_test.go", "mime/multipart", true},
		{"internal/protocol/systemone/new.go", "mime/multipart", false},
		{"internal/protocol/systemone/new_test.go", "mime/multipart", false},
		{"internal/protocol/systemone/payload.go", "os", false},
		{"internal/protocol/systemone/references_test.go", "math/rand/v2", true},
		{"internal/protocol/systemone/references_test.go", "runtime", true},
		{"internal/protocol/systemone/references.go", "math/rand/v2", false},
		{"internal/protocol/systemone/new_test.go", "runtime", false},
		{"internal/server/middleware/api_key_model_scenario_test.go", modulePath + "/internal/protocol/systemone", true},
		{"internal/server/middleware/new_test.go", modulePath + "/internal/protocol/systemone", false},
		{"internal/server/httpx/new_test.go", modulePath + "/internal/protocol/systemone", false},
		{"internal/server/middleware/api_key_model_scenario.go", modulePath + "/internal/protocol/systemone", false},
		{"internal/protocol/openai/new.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/bridge/new.go", modulePath + "/internal/protocol/openai", false},
		{"internal/protocol/bridge/new_external_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/bridge/new.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/bridge/new.go", modulePath + "/internal/protocol/ir", false},
		{"internal/protocol/bridge/conversion_matrix_scenario_test.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/bridge/conversion_matrix_scenario_test.go", "os", false},
		{"internal/protocol/bridge/matrix_scenario_integration_test.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/bridge/matrix_scenario_integration_test.go", "os", false},
		{"internal/protocol/openai/new_test.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/openai/new_integration_test.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/sse/reader_external_test.go", modulePath + "/internal/protocol/sse", true},
		{"internal/protocol/sse/reader.go", modulePath + "/internal/protocol", false},
		{"internal/protocol/sse/reader.go", modulePath + "/internal/protocol/ir", false},
		{"internal/protocol/sse/reader.go", "os", false},
		{"internal/protocol/sse/reader_test.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/ir/types.go", "encoding/json/v2", true},
		{"internal/protocol/ir/types.go", "encoding/json/jsontext", true},
		{"internal/protocol/ir/types.go", "encoding/gob", false},
		{"internal/protocol/ir/types.go", "os", false},
		{"internal/protocol/ir/types.go", modulePath + "/internal/protocol", true},
		{"internal/protocol/ir/types_external_test.go", modulePath + "/internal/protocol/ir", true},
		{"internal/protocol/ir/types.go", modulePath + "/internal/protocol/sse", false},
		{"internal/protocol/ir/types.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/ir/types.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/ir/essence_external_test.go", modulePath + "/internal/protocol/ir", true},
		{"internal/protocol/ir/essence_external_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/ir/essence_external_integration_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/ir/essence_external_test.go", "os", false},
		{"internal/protocol/ir/types_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/ir/types_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/ir/types_test.go", modulePath + "/internal/protocol/convert", false},
		{"internal/protocol/codec/messages/request_decode.go", modulePath + "/internal/protocol", true},
		{"internal/protocol/codec/messages/request_decode.go", modulePath + "/internal/protocol/ir", true},
		{"internal/protocol/codec/messages/request_decode.go", modulePath + "/internal/protocol/sse", true},
		{"internal/protocol/codec/messages/request_decode.go", modulePath + "/internal/protocol/anthropic", true},
		{"internal/protocol/codec/messages/request_decode.go", modulePath + "/internal/protocol/wirejson", true},
		{"internal/protocol/codec/messages/request_decode_external_test.go", modulePath + "/internal/protocol/codec/messages", true},
		{"internal/protocol/codec/messages/conformance_scenario_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/codec/messages/conformance_scenario_integration_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/codec/messages/conformance_scenario_test.go", "os", false},
		{"internal/protocol/codec/messages/request_decode.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/codec/messages/request_decode.go", modulePath + "/internal/protocol/openai", false},
		{"internal/protocol/codec/messages/request_decode.go", modulePath + "/internal/protocol/codec/responses", false},
		{"internal/protocol/codec/messages/request_decode.go", modulePath + "/internal/protocol/convert", false},
		{"internal/protocol/codec/messages/request_decode.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/codec/messages/conformance_scenario_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/codec/responses/request_decode.go", modulePath + "/internal/protocol", true},
		{"internal/protocol/codec/responses/request_decode.go", modulePath + "/internal/protocol/ir", true},
		{"internal/protocol/codec/responses/request_decode.go", modulePath + "/internal/protocol/sse", true},
		{"internal/protocol/codec/responses/request_decode.go", modulePath + "/internal/protocol/openai", true},
		{"internal/protocol/codec/responses/request_decode.go", modulePath + "/internal/protocol/wirejson", true},
		{"internal/protocol/codec/responses/request_decode_external_test.go", modulePath + "/internal/protocol/codec/responses", true},
		{"internal/protocol/codec/responses/conformance_scenario_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/codec/responses/request_decode.go", modulePath + "/internal/protocol/anthropic", false},
		{"internal/protocol/codec/responses/request_decode.go", modulePath + "/internal/protocol/codec/chat", false},
		{"internal/protocol/codec/responses/request_decode.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/codec/chat/request_decode.go", modulePath + "/internal/protocol", true},
		{"internal/protocol/codec/chat/request_decode.go", modulePath + "/internal/protocol/ir", true},
		{"internal/protocol/codec/chat/request_decode.go", modulePath + "/internal/protocol/sse", true},
		{"internal/protocol/codec/chat/request_decode.go", modulePath + "/internal/protocol/openai", true},
		{"internal/protocol/codec/chat/request_decode.go", modulePath + "/internal/protocol/wirejson", true},
		{"internal/protocol/codec/chat/request_decode_external_test.go", modulePath + "/internal/protocol/codec/chat", true},
		{"internal/protocol/codec/chat/conformance_scenario_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/codec/chat/request_decode.go", modulePath + "/internal/protocol/anthropic", false},
		{"internal/protocol/codec/chat/request_decode.go", modulePath + "/internal/protocol/codec/messages", false},
		{"internal/protocol/codec/chat/request_decode.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/codec/generatecontent/request_decode.go", modulePath + "/internal/protocol", true},
		{"internal/protocol/codec/generatecontent/request_decode.go", modulePath + "/internal/protocol/ir", true},
		{"internal/protocol/codec/generatecontent/request_decode.go", modulePath + "/internal/protocol/sse", true},
		{"internal/protocol/codec/generatecontent/request_decode.go", modulePath + "/internal/protocol/gemini", true},
		{"internal/protocol/codec/generatecontent/request_decode.go", modulePath + "/internal/protocol/wirejson", true},
		{"internal/protocol/codec/generatecontent/request_decode_external_test.go", modulePath + "/internal/protocol/codec/generatecontent", true},
		{"internal/protocol/codec/generatecontent/conformance_scenario_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/codec/generatecontent/conformance_scenario_integration_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/codec/generatecontent/conformance_scenario_test.go", "os", false},
		{"internal/protocol/codec/generatecontent/request_decode.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/codec/generatecontent/request_decode.go", modulePath + "/internal/protocol/openai", false},
		{"internal/protocol/codec/generatecontent/request_decode.go", modulePath + "/internal/protocol/anthropic", false},
		{"internal/protocol/codec/generatecontent/request_decode.go", modulePath + "/internal/protocol/codec/messages", false},
		{"internal/protocol/codec/generatecontent/request_decode.go", modulePath + "/internal/protocol/convert", false},
		{"internal/protocol/codec/generatecontent/request_decode.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/codec/generatecontent/conformance_scenario_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/gemini/observation.go", modulePath + "/internal/protocol", true},
		{"internal/protocol/gemini/observation_external_test.go", modulePath + "/internal/protocol/gemini", true},
		{"internal/protocol/gemini/observation.go", modulePath + "/internal/protocol/openai", false},
		{"internal/protocol/gemini/observation.go", modulePath + "/internal/protocol/ir", false},
		{"internal/protocol/gemini/observation.go", "os", false},
		{"internal/protocol/qoder/payload.go", "encoding/json/v2", true},
		{"internal/protocol/qoder/payload.go", "encoding/json/jsontext", true},
		{"internal/protocol/qoder/payload.go", modulePath + "/internal/protocol", true},
		{"internal/protocol/qoder/payload_external_test.go", modulePath + "/internal/protocol/qoder", true},
		{"internal/protocol/qoder/payload_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/qoder/payload_integration_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/qoder/payload.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/qoder/payload.go", modulePath + "/internal/protocol/ir", false},
		{"internal/protocol/qoder/payload.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/qoder/payload.go", "os", false},
		{"internal/protocol/codec/qoderchat/request_encode.go", modulePath + "/internal/protocol", true},
		{"internal/protocol/codec/qoderchat/request_encode.go", modulePath + "/internal/protocol/ir", true},
		{"internal/protocol/codec/qoderchat/request_encode.go", modulePath + "/internal/protocol/qoder", true},
		{"internal/protocol/codec/qoderchat/request_encode.go", modulePath + "/internal/protocol/sse", true},
		{"internal/protocol/codec/qoderchat/request_encode.go", modulePath + "/internal/protocol/wirejson", true},
		{"internal/protocol/codec/qoderchat/request_encode_external_test.go", modulePath + "/internal/protocol/codec/qoderchat", true},
		{"internal/protocol/codec/qoderchat/conformance_scenario_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/codec/qoderchat/conformance_scenario_integration_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/codec/qoderchat/conformance_scenario_test.go", "os", false},
		{"internal/protocol/codec/qoderchat/request_encode.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/codec/qoderchat/request_encode.go", modulePath + "/internal/protocol/openai", false},
		{"internal/protocol/codec/qoderchat/request_encode.go", modulePath + "/internal/protocol/anthropic", false},
		{"internal/protocol/codec/qoderchat/request_encode.go", modulePath + "/internal/protocol/codec/messages", false},
		{"internal/protocol/codec/qoderchat/request_encode.go", modulePath + "/internal/protocol/convert", false},
		{"internal/protocol/codec/qoderchat/request_encode.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/codec/qoderchat/conformance_scenario_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/codec/unregistered/new.go", modulePath + "/internal/protocol/ir", false},
		{"internal/protocol/codec/unregistered/new_external_test.go", modulePath + "/internal/protocol/codec/unregistered", false},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol", true},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/ir", true},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/sse", true},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/wirejson", true},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/qoder", true},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/codec/messages", true},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/codec/responses", true},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/codec/chat", true},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/codec/generatecontent", true},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/codec/qoderchat", true},
		{"internal/protocol/convert/exchange_external_test.go", modulePath + "/internal/protocol/convert", true},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/openai", false},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/anthropic", false},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/gemini", false},
		{"internal/protocol/convert/exchange.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/protocol/convert/exchange.go", "os", false},
		{"internal/protocol/convert/conformance_scenario_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/convert/conformance_scenario_integration_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/convert/conformance_scenario_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/convert/conformance_scenario_test.go", modulePath + "/internal/protocol/openai", false},
		{"internal/protocol/convert/conformance_scenario_test.go", modulePath + "/internal/protocol/anthropic", false},
		{"internal/protocol/convert/conformance_scenario_test.go", modulePath + "/internal/protocol/gemini", false},
		{"internal/protocol/convert/conformance_scenario_integration_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/convert/conformance_scenario_integration_test.go", modulePath + "/internal/protocol/openai", false},
		{"internal/protocol/convert/conformance_scenario_test.go", "os", false},
		{"internal/protocol/convert/conformance_scenario_test.go", modulePath + "/internal/gateway", false},
		{"internal/protocol/testkit/corpus.go", "embed", true},
		{"internal/protocol/testkit/golden.go", "os", true},
		{"internal/protocol/testkit/corpus.go", "encoding/json/v2", true},
		{"internal/protocol/testkit/corpus.go", "github.com/stretchr/testify/require", true},
		{"internal/protocol/testkit/corpus.go", modulePath + "/internal/protocol", true},
		{"internal/protocol/testkit/corpus.go", modulePath + "/internal/protocol/ir", true},
		{"internal/protocol/testkit/corpus.go", modulePath + "/internal/protocol/sse", true},
		{"internal/protocol/testkit/corpus_external_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/testkit/corpus.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/testkit/corpus.go", modulePath + "/internal/protocol/convert", false},
		{"internal/protocol/testkit/corpus.go", modulePath + "/internal/protocol/openai", false},
		{"internal/protocol/testkit/corpus.go", modulePath + "/internal/gateway", false},
		{"internal/protocol/testkit/corpus.go", "log", false},
		{"internal/protocol/testkit/corpus_integration_test.go", "os", true},
		{"internal/protocol/testkit/corpus_integration_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/testkit/helper/new.go", "os", true},
		{"internal/protocol/testkit/helper/new.go", modulePath + "/internal/protocol/ir", true},
		{"internal/protocol/testkit/helper/new.go", modulePath + "/internal/protocol/convert", false},
		{"internal/protocol/testkit/mockupstream/handler.go", "net/http", true},
		{"internal/protocol/testkit/mockupstream/handler.go", modulePath + "/internal/protocol", true},
		{"internal/protocol/testkit/mockupstream/handler.go", modulePath + "/internal/protocol/ir", true},
		{"internal/protocol/testkit/mockupstream/handler.go", modulePath + "/internal/protocol/sse", true},
		{"internal/protocol/testkit/mockupstream/handler.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/testkit/mockupstream/handler.go", modulePath + "/internal/protocol/convert", true},
		{"internal/protocol/testkit/mockupstream/handler_external_test.go", modulePath + "/internal/protocol/testkit/mockupstream", true},
		{"internal/protocol/testkit/mockupstream/handler.go", modulePath + "/internal/protocol/codec/messages", false},
		{"internal/protocol/testkit/mockupstream/handler.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/testkit/mockupstream/handler.go", modulePath + "/internal/gateway", false},
		{"internal/protocol/testkit/mockupstream/handler.go", "log", false},
		{"internal/protocol/testkit/cmd/corpus/main.go", "os", true},
		{"internal/protocol/testkit/cmd/corpus/main.go", "log/slog", true},
		{"internal/protocol/testkit/cmd/corpus/main.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/protocol/testkit/cmd/corpus/main_external_test.go", modulePath + "/internal/protocol/testkit/cmd/corpus", true},
		{"internal/protocol/testkit/cmd/corpus/main.go", "log", false},
		{"internal/protocol/testkit/cmd/corpus/main.go", modulePath + "/internal/protocol/convert", false},
		{"internal/protocol/testkit/cmd/corpus/main.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/protocol/unregistered/new.go", "os", false},
		{"internal/protocol/unregistered/new.go", modulePath + "/internal/protocol/ir", false},
		{"internal/protocol/unregistered/new_external_test.go", modulePath + "/internal/protocol/unregistered", false},
		{"internal/app/protocol_lab_scenario_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/app/protocol_lab_scenario_test.go", modulePath + "/internal/protocol/testkit/mockupstream", true},
		{"internal/gateway/forward/conversion.go", modulePath + "/internal/protocol/convert", true},
		{"internal/gateway/forward/conversion.go", modulePath + "/internal/protocol/ir", true},
		{"internal/gateway/forward/conversion.go", modulePath + "/internal/protocol/sse", true},
		{"internal/gateway/forward/conversion.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/gateway/forward/conversion_integration_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/gateway/httpapi/openai_responses_execution.go", modulePath + "/internal/protocol/convert", true},
		{"internal/gateway/httpapi/openai_responses_execution.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/gateway/provider/openaiforward/responses_converted.go", modulePath + "/internal/protocol/convert", true},
		{"internal/gateway/provider/openaiforward/responses_converted.go", modulePath + "/internal/protocol/ir", true},
		{"internal/gateway/provider/openaiforward/responses_converted.go", modulePath + "/internal/protocol/sse", true},
		{"internal/gateway/provider/openaiforward/responses_converted.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/gateway/provider/openaiforward/responses_converted_integration_test.go", modulePath + "/internal/protocol/convert", true},
		{"internal/gateway/provider/openaiforward/responses_converted_integration_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/gateway/ws/passthrough.go", modulePath + "/internal/protocol/convert", true},
		{"internal/gateway/ws/httpapi/openai_ws_http_bridge.go", modulePath + "/internal/protocol/convert", true},
		{"internal/gateway/provider/openaiforward/responses_converted.go", modulePath + "/internal/protocol/codec/responses", false},
		{"internal/gateway/execution/contracts.go", modulePath + "/internal/protocol/convert", false},
		{"internal/gateway/tokenestimate/estimate_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/gateway/tokenestimate/estimate_integration_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/gateway/tokenestimate/estimate.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/gateway/provider/input_tokens_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/gateway/provider/input_tokens_integration_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/gateway/provider/input_tokens.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/upstream/openai/response_converted.go", modulePath + "/internal/protocol/convert", true},
		{"internal/upstream/openai/response_converted.go", modulePath + "/internal/protocol/ir", true},
		{"internal/upstream/openai/response_converted.go", modulePath + "/internal/protocol/sse", true},
		{"internal/upstream/openai/response_converted.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/upstream/openai/response_converted_integration_test.go", modulePath + "/internal/protocol/convert", true},
		{"internal/upstream/openai/response_converted_integration_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/upstream/openai/response_raw_chat_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/upstream/openai/response_raw_chat_integration_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/upstream/openai/response_raw_chat.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/upstream/anthropic/request_test.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/upstream/gemini/response_converted_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/upstream/gemini/response_converted_integration_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/upstream/gemini/response_converted.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/upstream/gemini/response_converted.go", modulePath + "/internal/protocol/codec/generatecontent", false},
		{"internal/upstream/gemini/response_converted.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/upstream/gemini/response_converted_integration_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/upstream/grok/request_patch_test.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/upstream/gemini/response_converted.go", modulePath + "/internal/protocol/convert", true},
		{"internal/upstream/gemini/response_converted.go", modulePath + "/internal/protocol/ir", true},
		{"internal/upstream/gemini/response_converted.go", modulePath + "/internal/protocol/sse", true},
		{"internal/upstream/antigravity/response_converted.go", modulePath + "/internal/protocol/convert", true},
		{"internal/upstream/antigravity/response_converted.go", modulePath + "/internal/protocol/ir", true},
		{"internal/upstream/antigravity/response_converted.go", modulePath + "/internal/protocol/sse", true},
		{"internal/upstream/antigravity/response_converted.go", modulePath + "/internal/protocol/wirejson", true},
		{"internal/upstream/antigravity/response_converted.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/upstream/antigravity/response_converted_integration_test.go", modulePath + "/internal/protocol/wirejson", true},
		{"internal/upstream/antigravity/response_converted_integration_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/upstream/antigravity/response_converted_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/upstream/antigravity/response_converted_integration_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/upstream/antigravity/response_converted.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/upstream/antigravity/response_converted_test.go", modulePath + "/internal/protocol/codec/generatecontent", false},
		{"internal/protocol/convert/conformance_scenario_test.go", modulePath + "/internal/upstream/antigravity", false},
		{"internal/upstream/grok/request_patch.go", modulePath + "/internal/protocol/convert", true},
		{"internal/upstream/grok/request_patch.go", modulePath + "/internal/protocol/ir", true},
		{"internal/upstream/grok/request_patch.go", modulePath + "/internal/protocol/sse", true},
		{"internal/upstream/grok/request_patch.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/upstream/grok/request_patch_integration_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/upstream/qoder/execute.go", modulePath + "/internal/protocol/convert", true},
		{"internal/upstream/qoder/execute.go", modulePath + "/internal/protocol/ir", true},
		{"internal/upstream/qoder/execute.go", modulePath + "/internal/protocol/sse", true},
		{"internal/upstream/qoder/execute.go", modulePath + "/internal/protocol/qoder", true},
		{"internal/upstream/qoder/execute.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/upstream/qoder/execute_integration_test.go", modulePath + "/internal/protocol/qoder", true},
		{"internal/upstream/qoder/execute_integration_test.go", modulePath + "/internal/protocol/bridge", false},
		{"internal/upstream/qoder/execute_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/upstream/qoder/execute_integration_test.go", modulePath + "/internal/protocol/testkit", true},
		{"internal/upstream/qoder/execute.go", modulePath + "/internal/protocol/testkit", false},
		{"internal/upstream/qoder/execute.go", modulePath + "/internal/protocol/codec/qoderchat", false},
		{"internal/upstream/openai/response_converted.go", modulePath + "/internal/protocol/codec/responses", false},
		{"internal/upstream/openai/response_converted.go", modulePath + "/internal/protocol/qoder", false},
		{"internal/protocol/convert/conformance_scenario_test.go", modulePath + "/internal/upstream/qoder", false},
		{"internal/gateway/provider/qoder_error.go", modulePath + "/internal/protocol/qoder", true},
		{"internal/gateway/provider/qoder_error.go", modulePath + "/internal/protocol/codec/qoderchat", false},
		{"internal/upstream/anthropic/request.go", modulePath + "/internal/protocol/convert", false},
		{"internal/upstream/bedrock/request.go", modulePath + "/internal/protocol/convert", false},
		{"internal/upstream/deepseek/usage.go", modulePath + "/internal/protocol/convert", false},
		{"internal/provider/provider/qoder_test_execution.go", modulePath + "/internal/protocol/convert", true},
		{"internal/provider/provider/qoder_test_execution.go", modulePath + "/internal/protocol/ir", true},
		{"internal/provider/provider/qoder_test_execution.go", modulePath + "/internal/protocol/qoder", true},
		{"internal/provider/provider/qoder_test_execution.go", modulePath + "/internal/protocol/sse", true},
		{"internal/provider/provider/qoder_test_execution_integration_test.go", modulePath + "/internal/protocol/convert", true},
		{"internal/provider/provider/qoder_test_execution.go", modulePath + "/internal/protocol/codec/qoderchat", false},
		{"internal/routing/new.go", modulePath + "/internal/protocol/qoder", false},
		{"internal/billing/pricing/new.go", modulePath + "/internal/billing", false},
		{"internal/upstream/openai/helpers/new.go", modulePath + "/internal/upstream/gemini", false},
		{"internal/upstream/internal/googleauth/new.go", modulePath + "/internal/upstream/openai", false},
		{"internal/upstream/newplatform/new.go", "net/http", false},
		{"internal/upstream/anthropic/rediscache/new.go", "database/sql", false},
		{"internal/upstream/anthropic/rediscache/new_test.go", "database/sql", false},
		{"internal/infra/httpclient/new.go", modulePath + "/internal/billing", false},
		{"internal/app/new.go", modulePath + "/internal/service", false},
		{"internal/app/new.go", modulePath + "/internal/service/restored", false},
		{"internal/app/new.go", modulePath + "/internal/provider/postgres", true},
		{"internal/setup/setup.go", modulePath + "/internal/app/bootstrap", true},
		{"internal/setup/new.go", modulePath + "/internal/app/bootstrap", false},
		{"internal/apikey/postgres/key_store.go", modulePath + "/internal/billing/postgres", true},
		{"internal/apikey/postgres/key_store.go", modulePath + "/internal/billing/postgres/extra", false},
		{"internal/apikey/postgres/new.go", modulePath + "/internal/billing/postgres", false},
		{"internal/apikey/postgres/helpers/new_test.go", modulePath + "/internal/billing/postgres", false},
		{"internal/provider/health_spark.go", "net/http", true},
		{"internal/provider/health_spark.go", "net/http/httptest", false},
		{"internal/provider/new.go", "net/http", false},
		{"internal/gateway/clientmeta/claude_validator_test.go", "os", true},
		{"internal/gateway/clientmeta/new_test.go", "os", false},
		{"internal/pkg/ipmatch/helpers/new.go", "net", true},
		{"internal/pkg/ipmatch_extra/new.go", "net", false},
		{"internal/backup/helpers/new.go", "os/exec", false},
		{"internal/ops/maintenance/helpers/new.go", "os/exec", false},
		{"internal/billing/new.go", "golang.org/x/sync/singleflight", true},
		{"internal/billing/new.go", "golang.org/x/net/proxy", false},
		{"internal/billing/new.go", "nonstandard/client", false},
		{"internal/identity/provider/email_oauth.go", "github.com/tidwall/gjson", false},
		{"internal/billing/new.go", "github.com/tidwall/gjson", false},
		{"internal/moderation/input.go", "github.com/tidwall/gjson", true},
		{"internal/moderation/service.go", "github.com/tidwall/gjson", false},
		{"internal/protocol/openai/usage.go", "github.com/tidwall/sjson", true},
		{"internal/provider/provider/upstream_health.go", "github.com/tidwall/sjson", false},
		{"internal/identity/provider/oidc_oauth.go", "github.com/imroc/req/v3", false},
		{"internal/upstream/grok/oauth_client.go", "github.com/imroc/req/v3", false},
		{"internal/upstream/openai/oauth_client.go", "github.com/imroc/req/v3", false},
		{"internal/upstream/openai/privacy_client.go", "github.com/imroc/req/v3", true},
		{"internal/upstream/anthropic/oauth_client.go", "github.com/imroc/req/v3", true},
		{"internal/config/config.go", "github.com/spf13/viper", false},
		{"internal/ops/httpapi/ops_ws_handler.go", "github.com/gorilla/websocket", false},
		{"internal/ops/httpapi/ops_ws_handler.go", "github.com/coder/websocket", true},
		{"internal/infra/timingwheel/wheel.go", "github.com/zeromicro/go-zero/core/collection", false},
		{"internal/billing/group_rate_resolver.go", "github.com/patrickmn/go-cache", false},
		{"internal/ops/service.go", "log", false},
		{"internal/ops/service.go", "log/syslog", false},
		{"internal/ops/service.go", "log/slog", true},
		{"internal/ops/service_test.go", "log", true},
		{"internal/infra/telemetry/logging/logger.go", "log", true},
		{"cmd/server/main.go", "log", true},
		{"internal/newmodule/new.go", modulePath + "/internal/provider", false},
		{"internal/provider/new.go", modulePath + "-other/internal/billing", false},
	}
	var sources []source
	for _, c := range cases {
		sources = append(sources, source{Path: c.file, Imports: []importRef{{Path: c.imported, Line: 7}}})
	}
	violations, err := check(sources)
	if err != nil {
		t.Fatal(err)
	}
	denied := map[string]bool{}
	for _, v := range violations {
		if v.Line != 7 {
			t.Errorf("诊断没有保留 import 行号：%+v", v)
		}
		denied[v.File+"|"+v.Import] = true
	}
	for _, c := range cases {
		if denied[c.file+"|"+c.imported] == c.allowed {
			t.Errorf("%s 导入 %s：预期允许=%v", c.file, c.imported, c.allowed)
		}
	}
}

func TestScanIncludesBuildVariants(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"normal.go":           "package fixture\nimport _ \"context\"\n",
		"normal_test.go":      "package fixture_test\nimport _ \"os\"\n",
		"unit.go":             "//go:build unit\n\npackage fixture\nimport _ \"net/http\"\n",
		"integration_test.go": "//go:build integration\n\npackage fixture\nimport _ \"database/sql\"\n",
		"wire.go":             "//go:build wireinject\n\npackage fixture\nimport _ \"fmt\"\n",
		"embed.go":            "//go:build embed\n\npackage fixture\nimport _ \"embed\"\n",
		"file_windows.go":     "package fixture\nimport _ \"os/exec\"\n",
		"file_linux.go":       "package fixture\nimport _ \"os\"\n",
		"generated.go":        "// Code generated by fixture. DO NOT EDIT.\npackage fixture\nimport _ \"os\"\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	sources, err := scan(root)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, s := range sources {
		seen[s.Path] = true
		if len(s.Imports) != 1 || s.Imports[0].Line < 2 {
			t.Errorf("导入解析不完整：%+v", s)
		}
	}
	for name := range files {
		if seen[name] == (name == "generated.go") {
			t.Errorf("文件入选不符合预期：%s", name)
		}
	}
}

func TestScanFailsClosed(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "bad.go"), []byte("package fixture\nimport (\"unfinished"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := scan(root); err == nil {
		t.Fatal("无效 import 不应被忽略")
	}
	if _, err := check(nil); err == nil {
		t.Fatal("空扫描结果不应通过")
	}
}

// 保留例外需要真实消费者；删除或迁移文件后，陈旧许可应及时退出。
func TestFilePermissionsHaveConsumers(t *testing.T) {
	sources, err := scan(filepath.Join("..", "..", "backend"))
	if err != nil {
		t.Fatal(err)
	}
	imports := map[string]map[string]bool{}
	for _, s := range sources {
		imports[s.Path] = map[string]bool{}
		for _, imp := range s.Imports {
			imports[s.Path][imp.Path] = true
		}
	}
	for _, permission := range filePermissions {
		for _, file := range strings.Fields(permission.Files) {
			for _, target := range strings.Fields(permission.Imports) {
				if !imports[permission.Scope+"/"+file][importName(target)] {
					t.Errorf("文件权限缺少实际消费者：%s/%s -> %s", permission.Scope, file, target)
				}
			}
		}
	}
	for _, permissions := range []map[string]string{pureFileStandard, ioFileExceptions} {
		for file, targets := range permissions {
			for _, target := range strings.Fields(targets) {
				if !imports[file][target] {
					t.Errorf("标准库例外缺少实际消费者：%s -> %s", file, target)
				}
			}
		}
	}
}
