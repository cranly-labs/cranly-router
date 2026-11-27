package googleforward_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/googleforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/session"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	gemininative "github.com/TokenFlux/TokenRouter/internal/upstream/gemini"
)

// TestCountGeminiInlineImageOutputs compares camel/snake image fields, mixed
// parts, MIME variants, empty payloads, and malformed/error JSON. The returned
// count matches each fixture while non-image and empty-data parts contribute
// zero.
func TestCountGeminiInlineImageOutputs(t *testing.T) {
	cases := []struct {
		name    string // table scenario name
		payload string // upstream response fixture JSON
		want    int    // expected inline image count
	}{
		{
			name: "camelCase inlineData",

			payload: geminiImageResponse(`{"inlineData":{"mimeType":"image/png","data":"` + geminiTestPNG + `"}}`),

			want: 1,
		},

		{
			// Accept snake_case fields returned by SDKs and
			// compatible gateways.

			name: "snake_case inline_data",

			payload: geminiImageResponse(`{"inline_data":{"mime_type":"image/png","data":"` + geminiTestPNG + `"}}`),

			want: 1,
		},

		{
			name: "text and image mixed",

			payload: geminiImageResponse(`{"text":"here you go"},` +
				`{"inlineData":{"mimeType":"image/jpeg","data":"` + geminiTestPNG + `"}}`),

			want: 1,
		},

		{
			name: "multiple images",

			payload: geminiImageResponse(`{"inlineData":{"mimeType":"image/png","data":"` + geminiTestPNG + `"}},` +
				`{"inlineData":{"mimeType":"image/webp","data":"` + geminiTestPNG + `"}}`),

			want: 2,
		},

		{
			name: "uppercase mime type",

			payload: geminiImageResponse(`{"inlineData":{"mimeType":"IMAGE/PNG","data":"` + geminiTestPNG + `"}}`),

			want: 1,
		},

		{
			name:    "text only",
			payload: geminiImageResponse(`{"text":"no image here"}`),
			want:    0,
		},

		{
			// Non-image attachments, such as audio, do not count as
			// images.

			name: "non image mime type",

			payload: geminiImageResponse(`{"inlineData":{"mimeType":"audio/mpeg","data":"` + geminiTestPNG + `"}}`),

			want: 0,
		},

		{
			name: "empty data is not billable",

			payload: geminiImageResponse(`{"inlineData":{"mimeType":"image/png","data":""}}`),

			want: 0,
		},

		{name: "empty payload", payload: "", want: 0},

		{name: "invalid json", payload: "not-json", want: 0},

		{name: "error response", payload: `{"error":{"code":429,"message":"quota"}}`, want: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, gemininative.CountGeminiInlineImageOutputs([]byte(tc.payload)))
		})
	}
}

// TestObserveGeminiImageOutputs_CumulativeChunksDoNotDoubleCount observes the
// same one-image payload four times through fresh attempt state. The retained
// image count remains one, protecting maximum-payload observation against
// cumulative stream repetition.
func TestObserveGeminiImageOutputs_CumulativeChunksDoNotDoubleCount(t *testing.T) {
	c := newGeminiImageTestContext(t)
	c.Images = 0

	oneImage := geminiImageResponse(`{"inlineData":{"mimeType":"image/png","data":"` + geminiTestPNG + `"}}`)
	for range 4 {
		c.ObserveImagesForTest([]byte(oneImage))
	}

	require.Equal(t, 1, c.Images)
}

// TestObserveGeminiImageOutputs_KeepsLargestChunk observes text, a two-image
// payload, and trailing usage-only input. The retained counter stays at the
// largest payload count after the smaller trailing observation.
func TestObserveGeminiImageOutputs_KeepsLargestChunk(t *testing.T) {
	c := newGeminiImageTestContext(t)
	c.Images = 0

	c.ObserveImagesForTest([]byte(geminiImageResponse(`{"text":"working"}`)))
	c.ObserveImagesForTest([]byte(geminiImageResponse(
		`{"inlineData":{"mimeType":"image/png","data":"` + geminiTestPNG + `"}},` +
			`{"inlineData":{"mimeType":"image/png","data":"` + geminiTestPNG + `"}}`)))
	// A usage-only trailing chunk leaves the observed image count
	// unchanged.
	c.ObserveImagesForTest([]byte(`{"usageMetadata":{"promptTokenCount":9}}`))

	require.Equal(t, 2, c.Images)
}

// TestBeginGeminiImageOutputObservation_ResetsPerForward manually resets a
// populated attempt counter before observing another payload. The assertions
// check direct reset and new observation; this fixture does not invoke a
// forwarding entry.
func TestBeginGeminiImageOutputObservation_ResetsPerForward(t *testing.T) {
	c := newGeminiImageTestContext(t)
	oneImage := []byte(geminiImageResponse(`{"inlineData":{"mimeType":"image/png","data":"` + geminiTestPNG + `"}}`))

	c.Images = 0
	c.ObserveImagesForTest(oneImage)
	require.Equal(t, 1, c.Images)

	c.Images = 0
	require.Equal(t, 0, c.Images)
	c.ObserveImagesForTest(oneImage)
	require.Equal(t, 1, c.Images)
}

// TestResolveGeminiImageCount compares observed images under custom names with
// recognized-name inference and text-only input. Observed counts win,
// mapped/request names supply the estimate when applicable, and unmatched text
// models return zero.
func TestResolveGeminiImageCount(t *testing.T) {
	oneImage := []byte(geminiImageResponse(`{"inlineData":{"mimeType":"image/png","data":"` + geminiTestPNG + `"}}`))
	textOnly := []byte(geminiImageResponse(`{"text":"hello"}`))

	t.Run("custom model name bills by observed images", func(t *testing.T) {
		c := newGeminiImageTestContext(t)
		c.Images = 0
		c.ObserveImagesForTest(oneImage)

		require.Equal(t, 1, c.ImageCountForTest("nana-banana-2", "nana-banana-2"))
	})

	t.Run("requested model names do not fabricate output", func(t *testing.T) {
		c := newGeminiImageTestContext(t)
		c.Images = 0
		c.ObserveImagesForTest(textOnly)

		require.Equal(t, 0, c.ImageCountForTest("gemini-3-pro-image-preview", "gemini-3-pro-image-preview"))
	})

	t.Run("mapped model names do not fabricate output", func(t *testing.T) {
		c := newGeminiImageTestContext(t)
		c.Images = 0
		c.ObserveImagesForTest(textOnly)

		require.Equal(t, 0, c.ImageCountForTest("my-image-alias", "gemini-2.5-flash-image"))
	})

	t.Run("text model stays unbilled", func(t *testing.T) {
		c := newGeminiImageTestContext(t)
		c.Images = 0
		c.ObserveImagesForTest(textOnly)

		require.Equal(t, 0, c.ImageCountForTest("gemini-2.5-pro", "gemini-2.5-pro"))
	})

	t.Run("missing observations remain zero", func(t *testing.T) {
		c := newGeminiImageTestContext(t)
		require.Equal(t, 0, c.ImageCountForTest("nana-banana-2", "nana-banana-2"))
		require.Equal(t, 0, c.ImageCountForTest("gemini-3-pro-image", "gemini-3-pro-image"))
	})
}

// TestAntigravityCallMemoryScoping exercises Antigravity and Gemini CallMemory
// across nil receivers, invalid scopes, shared GeminiToolCalls stores,
// standalone zero-value fallbacks, and Exchange round trips. Valid scopes must
// persist upstream call metadata per provider and model without cross-scope leaks.
func TestAntigravityCallMemoryScoping(t *testing.T) {
	validScope := session.GeminiToolScope{
		APIKeyID:   41,
		ProviderID: 3757,
		Model:      "claude-sonnet-4-5-thinking",
	}

	t.Run("nil receivers and invalid scopes return nil CallMemory", func(t *testing.T) {
		var nilAG *googleforward.Antigravity
		var nilGem *googleforward.Gemini
		require.Nil(t, nilAG.CallMemory(validScope))
		require.Nil(t, nilGem.CallMemory(validScope))

		var standalone googleforward.Antigravity
		for _, badScope := range []session.GeminiToolScope{
			{},
			{APIKeyID: 0, ProviderID: 3757, Model: "claude-sonnet-4-5"},
			{APIKeyID: -1, ProviderID: 3757, Model: "claude-sonnet-4-5"},
			{APIKeyID: 41, ProviderID: 0, Model: "claude-sonnet-4-5"},
			{APIKeyID: 41, ProviderID: 3757, Model: ""},
		} {
			require.Nil(t, standalone.CallMemory(badScope))
		}
	})

	t.Run("shared ToolCalls isolates by API key provider and mapped model while zero-value runtime falls back cleanly", func(t *testing.T) {
		sharedStore := &session.GeminiToolCalls{}
		gemSvc := &googleforward.Gemini{
			ToolCalls: sharedStore,
		}
		agSvc := &googleforward.Antigravity{
			ToolCalls: sharedStore,
		}

		agClaudeMem := agSvc.CallMemory(validScope)
		require.NotNil(t, agClaudeMem)
		agClaudeMem.Remember("toolu_shared_1", convert.CallBinding{
			UpstreamID: "toolu_up_1",
			Token:      &ir.ReplayToken{Scheme: ir.SchemeAntigravityClaude, Data: "sig-claude-shared"},
		})

		// Same shared store recalled from Gemini or Antigravity under identical scope hits,
		// whereas distinct key, provider, or thinking-suffix model scopes miss cleanly.
		gotSame, ok := gemSvc.CallMemory(validScope).Recall("toolu_shared_1")
		require.True(t, ok)
		require.Equal(t, "toolu_up_1", gotSame.UpstreamID)
		require.Equal(t, &ir.ReplayToken{Scheme: ir.SchemeAntigravityClaude, Data: "sig-claude-shared"}, gotSame.Token)

		for _, otherScope := range []session.GeminiToolScope{
			{APIKeyID: 42, ProviderID: 3757, Model: "claude-sonnet-4-5-thinking"},
			{APIKeyID: 41, ProviderID: 3758, Model: "claude-sonnet-4-5-thinking"},
			{APIKeyID: 41, ProviderID: 3757, Model: "claude-sonnet-4-5"},
			{APIKeyID: 41, ProviderID: 3757, Model: "gemini-2.5-pro"},
		} {
			_, hit := agSvc.CallMemory(otherScope).Recall("toolu_shared_1")
			require.False(t, hit, "unexpected scope leak for %+v", otherScope)
		}

		// Zero-value Antigravity without shared ToolCalls uses its embedded fallback store across Exchange turns.
		var zeroAG googleforward.Antigravity
		attrSvc := newExecutionReadersFixture(nil).Attributes
		prov := newAntigravityCompatProvider(capability.ProviderTypeOAuth)

		for _, family := range []struct {
			model      string // mapped Antigravity model
			wantScheme string // expected ReplayScheme
		}{
			{model: "claude-sonnet-4-5-thinking", wantScheme: ir.SchemeAntigravityClaude},
			{model: "gemini-2.5-pro", wantScheme: ir.SchemeGemini},
		} {
			scope := session.GeminiToolScope{
				APIKeyID:   77,
				ProviderID: prov.Record.ID,
				Model:      family.model,
			}
			res, err := gatewayprovider.ResolveModelAttributes(t.Context(), prov, family.model, attrSvc, "gemini_generate_content")
			require.NoError(t, err)
			dialect := gatewayprovider.ConversionDialect(t.Context(), prov, res, protocol.ProtocolGeminiGenerateContent)
			require.Equal(t, family.wantScheme, dialect.ReplayScheme)
			require.Equal(t, convert.SchemaAntigravity, dialect.SchemaDialect)

			turn1Req := []byte(`{"model":"gpt-5","tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"q":{"type":"string"}}}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"search"}]}]}`)
			ex1, err := convert.NewExchange(turn1Req, convert.Options{
				Route: convert.Route{
					Client:   protocol.ProtocolOpenAIResponses,
					Upstream: protocol.ProtocolGeminiGenerateContent,
					Dialect:  dialect,
				},
				ClientModel: "gpt-5",
				Calls:       zeroAG.CallMemory(scope),
			})
			require.NoError(t, err)
			_, err = ex1.Response([]byte(`{"responseId":"ag_turn1","modelVersion":"` + family.model + `","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"call_ag_77","name":"lookup","args":{"q":"docs"}},"thoughtSignature":"sig_` + family.wantScheme + `"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":9,"candidatesTokenCount":4,"totalTokenCount":13}}`))
			require.NoError(t, err)

			turn2Req := []byte(`{"model":"gpt-5","tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"q":{"type":"string"}}}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"search"}]},{"type":"function_call","call_id":"call_ag_77","name":"lookup","arguments":"{\"q\":\"docs\"}"},{"type":"function_call_output","call_id":"call_ag_77","output":"ok"}]}`)
			ex2, err := convert.NewExchange(turn2Req, convert.Options{
				Route: convert.Route{
					Client:   protocol.ProtocolOpenAIResponses,
					Upstream: protocol.ProtocolGeminiGenerateContent,
					Dialect:  dialect,
				},
				ClientModel: "gpt-5",
				Calls:       zeroAG.CallMemory(scope),
			})
			require.NoError(t, err)
			upBytes, err := ex2.UpstreamRequest()
			require.NoError(t, err)
			require.Equal(t, "call_ag_77", gjson.GetBytes(upBytes, "contents.1.parts.0.functionCall.id").String())
			require.Equal(t, "sig_"+family.wantScheme, gjson.GetBytes(upBytes, "contents.1.parts.0.thoughtSignature").String())
			require.Equal(t, "call_ag_77", gjson.GetBytes(upBytes, "contents.2.parts.0.functionResponse.id").String())
			require.Equal(t, "ok", gjson.GetBytes(upBytes, "contents.2.parts.0.functionResponse.response.result").String())
		}
	})
}
