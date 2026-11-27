package app

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
)

// protocol_routes_scenario_test.go snapshots the gateway protocol conversion
// inventory across gateway_openai_text_execution.go,
// gateway_messages_execution.go, gateway_google.go, gateway_grok_execution.go,
// qoder_runtime.go, gateway_count_tokens.go, and gateway_openai_tokens.go into
// testdata/golden/routes.txt. Route-table entries derive from
// capability.ProtocolFallbackTargets and capability.SupportsProtocolConversion,
// while off-table entries record the five conversion mechanisms invoked outside
// group fallback resolution.
//
// Keeping both sections in one golden inventory makes route-table edits visible
// in review and anchors line-by-line T3, T5, and T6 route coverage checks. Each
// route-table entry also binds to its request and response upstream families so
// tests verify alignment with the merged gateway execution architecture.

const (
	// Request-side upstream conversion family identifiers classify route-table
	// conversions by their merged request entry point.
	requestFamilyResponses         = "responses"          // OpenAI-compatible Responses request conversion
	requestFamilyChat              = "chat"               // OpenAI-compatible raw Chat conversion
	requestFamilyMessagesAnthropic = "messages_anthropic" // Anthropic platform Messages conversion
	requestFamilyMessagesCompat    = "messages_compat"    // compatible Anthropic-endpoint Messages conversion
	requestFamilyGemini            = "gemini"             // Gemini GenerateContent request conversion
	requestFamilyAntigravity       = "antigravity"        // Antigravity wrapped Gemini conversion
	requestFamilyQoder             = "qoder"              // Qoder normalized payload conversion

	// Response-side upstream conversion family identifiers classify route-table
	// conversions by their merged streaming and buffered response readers.
	responseFamilyResponses   = "responses"   // OpenAI-compatible Responses reader pair
	responseFamilyChat        = "chat"        // OpenAI-compatible raw Chat reader pair
	responseFamilyMessages    = "messages"    // shared Anthropic Messages reader pair
	responseFamilyGemini      = "gemini"      // Gemini GenerateContent reader pair
	responseFamilyAntigravity = "antigravity" // Antigravity response reader pair
	responseFamilyQoder       = "qoder"       // Qoder stream and buffered reader pair

	// Off-table conversion category identifiers name the five conversion uses
	// executed outside capability.ResolveRoute.
	offTableResponsesClientTools    = "responses_client_tool_lowering"    // same-protocol Responses client-tool lowering
	offTableCursorChatInput         = "cursor_chat_responses_input"       // Responses-shaped input on Chat endpoint
	offTableMessagesCountTokens     = "messages_count_tokens"             // Messages count_tokens to Gemini or OpenAI
	offTableLocalTokenEstimation    = "local_input_token_estimation"      // local tiktoken count from Messages input
	offTableResponsesLegacyMessages = "responses_legacy_messages_ingress" // legacy Chat messages in Responses body

	// Section header markers separate capability-table conversion rows from
	// helper-driven off-table conversion rows inside testdata/golden/routes.txt.
	routesSectionRouteTable = "[route_table]" // capability route-table conversion section
	routesSectionOffTable   = "[off_table]"   // helper and adapter off-table section
)

// conversionRouteEntry records one single-step text conversion route derived
// from the capability catalogue together with its upstream execution families.
type conversionRouteEntry struct {
	ClientProtocol   protocol.ProtocolID // inbound client protocol identifier
	Platform         string              // selected provider platform identifier
	ProviderType     string              // selected provider authentication type
	UpstreamProtocol protocol.ProtocolID // resolved target upstream protocol
	RequestFamily    string              // merged request conversion family name
	ResponseFamily   string              // merged response reader family name
}

// offTableConversionEntry records one protocol conversion path invoked by
// gateway helpers or same-protocol adapters outside capability.ResolveRoute.
type offTableConversionEntry struct {
	Category           string              // off-table conversion category name
	ClientProtocol     protocol.ProtocolID // inbound client protocol identifier
	Scope              string              // semicolon-separated platform:types scope
	TargetProtocol     protocol.ProtocolID // target wire or projection protocol
	RequestConversion  string              // request-side conversion mechanism name
	ResponseConversion string              // response-side restoration or output mode
}

// nonTextConversionEntry records a transport or media adaptation entry in
// capability.SupportsProtocolConversion that remains outside IR text conversion.
type nonTextConversionEntry struct {
	ClientProtocol   protocol.ProtocolID // non-text client protocol identifier
	Platform         string              // selected provider platform identifier
	ProviderType     string              // selected provider authentication type
	AuthMode         string              // optional OpenAI OAuth mode selector
	UpstreamProtocol protocol.ProtocolID // target transport or execution protocol
}

// TestProtocolConversionRoutesGolden builds the capability route-table
// conversion list alongside the five off-table conversion categories and checks
// the rendered snapshot against testdata/golden/routes.txt. It also parses the
// rendered lines back into structured records so route-coverage checks can
// rely on unique, non-empty six-column entries.
func TestProtocolConversionRoutesGolden(t *testing.T) {
	t.Parallel()

	routeTable, err := collectTextConversionRoutes()
	require.NoError(t, err)
	require.NotEmpty(t, routeTable)

	offTable, err := collectOffTableConversionRoutes()
	require.NoError(t, err)
	require.NotEmpty(t, offTable)

	rendered := formatConversionRoutesGolden(routeTable, offTable)
	testkit.Golden(t, "routes.txt", []byte(rendered))

	parsedRoutes, parsedOffTable, err := parseConversionRoutesGolden(rendered)
	require.NoError(t, err)
	require.Equal(t, routeTable, parsedRoutes)
	require.Equal(t, offTable, parsedOffTable)
}

// TestProtocolConversionRoutesMatchUpstreamFamilies verifies that every public
// client protocol belongs to either the text conversion inventory or the known
// transport and media exclusions. It asserts exact route counts across the
// seven request families and six response families, including Anthropic Bedrock
// conversions and the five off-table conversion categories.
func TestProtocolConversionRoutesMatchUpstreamFamilies(t *testing.T) {
	t.Parallel()

	assertClientProtocolScopePartition(t)
	assertOpenAIAuthModesPreserveTextConversions(t)
	assertOutOfScopeNonTextConversions(t)

	routeTable, err := collectTextConversionRoutes()
	require.NoError(t, err)

	requestCounts := map[string]int{}
	responseCounts := map[string]int{}
	for _, route := range routeTable {
		reqFamily, respFamily, classifyErr := classifyUpstreamFamilies(route.Platform, route.UpstreamProtocol)
		require.NoError(t, classifyErr, "route %+v", route)
		require.Equal(t, reqFamily, route.RequestFamily)
		require.Equal(t, respFamily, route.ResponseFamily)
		require.NotEqual(t, route.ClientProtocol, route.UpstreamProtocol)
		requestCounts[route.RequestFamily]++
		responseCounts[route.ResponseFamily]++

		// Both Anthropic platform conversions and compatible Anthropic-endpoint
		// conversions share the single merged Messages response reader pair.
		if route.RequestFamily == requestFamilyMessagesAnthropic || route.RequestFamily == requestFamilyMessagesCompat {
			require.Equal(t, responseFamilyMessages, route.ResponseFamily)
		}
	}

	expectedRequestFamilies := map[string]int{
		requestFamilyResponses:         14,
		requestFamilyChat:              14,
		requestFamilyMessagesAnthropic: 10,
		requestFamilyMessagesCompat:    8,
		requestFamilyGemini:            12,
		requestFamilyAntigravity:       3,
		requestFamilyQoder:             3,
	}
	require.Equal(t, expectedRequestFamilies, requestCounts)

	expectedResponseFamilies := map[string]int{
		responseFamilyResponses:   14,
		responseFamilyChat:        14,
		responseFamilyMessages:    18,
		responseFamilyGemini:      12,
		responseFamilyAntigravity: 3,
		responseFamilyQoder:       3,
	}
	require.Equal(t, expectedResponseFamilies, responseCounts)

	// Verify that Anthropic Bedrock conversion routes remain present in the
	// inventory and bind to the shared Messages conversion request and response
	// families alongside standard Anthropic transports.
	require.True(t, containsConversionRoute(
		routeTable,
		capability.ProtocolOpenAIResponses,
		capability.PlatformAnthropic,
		capability.ProviderTypeBedrock,
		capability.ProtocolAnthropicMessages,
	))
	require.True(t, containsConversionRoute(
		routeTable,
		capability.ProtocolOpenAIChatCompletions,
		capability.PlatformAnthropic,
		capability.ProviderTypeBedrock,
		capability.ProtocolAnthropicMessages,
	))

	// Verify that OpenCode Gemini routes share the Gemini conversion family
	// while Antigravity OAuth routes stay in the dedicated Antigravity family.
	require.True(t, containsConversionRouteWithFamilies(
		routeTable,
		capability.ProtocolAnthropicMessages,
		capability.PlatformOpenCode,
		capability.ProviderTypeAPIKey,
		capability.ProtocolGeminiGenerateContent,
		requestFamilyGemini,
		responseFamilyGemini,
	))
	require.True(t, containsConversionRouteWithFamilies(
		routeTable,
		capability.ProtocolAnthropicMessages,
		capability.PlatformAntigravity,
		capability.ProviderTypeOAuth,
		capability.ProtocolGeminiGenerateContent,
		requestFamilyAntigravity,
		responseFamilyAntigravity,
	))

	offTable, err := collectOffTableConversionRoutes()
	require.NoError(t, err)

	categories := map[string]int{}
	for _, entry := range offTable {
		require.NotEmpty(t, entry.Scope, "off-table entry %+v must resolve a non-empty provider scope", entry)
		categories[entry.Category]++
	}
	expectedCategories := map[string]int{
		offTableResponsesClientTools:    5,
		offTableCursorChatInput:         3,
		offTableMessagesCountTokens:     2,
		offTableLocalTokenEstimation:    1,
		offTableResponsesLegacyMessages: 2,
	}
	require.Equal(t, expectedCategories, categories)
}

// collectTextConversionRoutes traverses platforms, provider types, text client
// protocols, and fallback targets in deterministic catalogue order to collect
// every supported single-step text conversion route. Any route whose platform
// and upstream protocol cannot map to a merged execution family returns an
// error immediately.
func collectTextConversionRoutes() ([]conversionRouteEntry, error) {
	textSources := inScopeTextClientProtocols()
	providerTypes := registeredProviderTypes()
	var routes []conversionRouteEntry

	for _, platform := range capability.ProviderPlatforms() {
		for _, providerType := range providerTypes {
			for _, source := range textSources {
				targets := capability.ProtocolFallbackTargets(platform, source)
				for _, target := range targets {
					if !capability.SupportsProtocolConversion(platform, providerType, "", source, target) {
						continue
					}
					reqFamily, respFamily, err := classifyUpstreamFamilies(platform, target)
					if err != nil {
						return nil, fmt.Errorf("classify %s -> %s/%s/%s: %w", source, platform, providerType, target, err)
					}
					routes = append(routes, conversionRouteEntry{
						ClientProtocol:   source,
						Platform:         platform,
						ProviderType:     providerType,
						UpstreamProtocol: target,
						RequestFamily:    reqFamily,
						ResponseFamily:   respFamily,
					})
				}
			}
		}
	}
	return routes, nil
}

// collectOffTableConversionRoutes constructs the five conversion use categories
// that execute outside capability.ResolveRoute. Each entry derives its eligible
// platform and provider-type scope from capability declarations and returns an
// error if a required scope resolves to empty.
func collectOffTableConversionRoutes() ([]offTableConversionEntry, error) {
	openAIAPIKeyScope, err := resolveNativeProtocolScope([]string{capability.PlatformOpenAI}, capability.ProtocolOpenAIResponses, capability.ProviderTypeAPIKey)
	if err != nil {
		return nil, err
	}
	deepseekAPIKeyScope, err := resolveNativeProtocolScope([]string{capability.PlatformDeepseek}, capability.ProtocolOpenAIResponses, capability.ProviderTypeAPIKey)
	if err != nil {
		return nil, err
	}
	grokResponsesScope, err := resolveNativeProtocolScope([]string{capability.PlatformGrok}, capability.ProtocolOpenAIResponses)
	if err != nil {
		return nil, err
	}
	wsOpenAIAPIKeyScope, err := resolveWSFunctionToolBridgeScope(capability.PlatformOpenAI)
	if err != nil {
		return nil, err
	}
	wsGrokScope, err := resolveWSFunctionToolBridgeScope(capability.PlatformGrok)
	if err != nil {
		return nil, err
	}

	// OpenAI-compatible RunConvertedResponses handles Responses-shaped Cursor input directly on
	// OpenAI, Kimi, DeepSeek, and OpenCode Responses routes, while Grok rejects
	// top-level input in GrokChatResponsesBridgeEligibility and only converts
	// input on explicit non-Responses routes before platform dispatch.
	cursorResponsesPlatforms := []string{
		capability.PlatformOpenAI,
		capability.PlatformKimi,
		capability.PlatformDeepseek,
		capability.PlatformOpenCode,
	}
	cursorResponsesScope, err := resolveNativeProtocolScope(cursorResponsesPlatforms, capability.ProtocolOpenAIResponses)
	if err != nil {
		return nil, err
	}
	openAICompatPlatforms := []string{
		capability.PlatformOpenAI,
		capability.PlatformGrok,
		capability.PlatformKimi,
		capability.PlatformZhipu,
		capability.PlatformDeepseek,
		capability.PlatformOpenCode,
	}
	cursorRawChatScope, err := resolveNativeProtocolScope(openAICompatPlatforms, capability.ProtocolOpenAIChatCompletions)
	if err != nil {
		return nil, err
	}
	cursorCompatMessagesScope, err := resolveNativeProtocolScope(openAICompatPlatforms, capability.ProtocolAnthropicMessages)
	if err != nil {
		return nil, err
	}

	geminiCountScope, err := resolveNativeProtocolScope([]string{capability.PlatformGemini}, capability.ProtocolGeminiGenerateContent)
	if err != nil {
		return nil, err
	}
	openAICountScope, err := resolveNativeProtocolScope([]string{capability.PlatformOpenAI}, capability.ProtocolOpenAIResponses)
	if err != nil {
		return nil, err
	}

	openAIOAuthScope, err := resolveNativeProtocolScope([]string{capability.PlatformOpenAI}, capability.ProtocolOpenAIResponses, capability.ProviderTypeOAuth)
	if err != nil {
		return nil, err
	}
	localEstimateCompatScope, err := resolveAnyTextNativeScope([]string{
		capability.PlatformGrok,
		capability.PlatformKimi,
		capability.PlatformZhipu,
		capability.PlatformDeepseek,
		capability.PlatformOpenCode,
	})
	if err != nil {
		return nil, err
	}
	localEstimateScope := joinScopes(openAIOAuthScope, localEstimateCompatScope)

	openAIOAuthWSScope, err := resolveNativeProtocolScope([]string{capability.PlatformOpenAI}, capability.ProtocolResponsesWebSocket, capability.ProviderTypeOAuth)
	if err != nil {
		return nil, err
	}
	anthropicConvertedResponsesScope, err := resolveConversionTargetScope(
		capability.PlatformAnthropic,
		capability.ProtocolOpenAIResponses,
		capability.ProtocolAnthropicMessages,
	)
	if err != nil {
		return nil, err
	}
	legacyMessagesIngressScope := joinScopes(openAIOAuthScope, anthropicConvertedResponsesScope)

	return []offTableConversionEntry{
		{
			Category:           offTableResponsesClientTools,
			ClientProtocol:     capability.ProtocolOpenAIResponses,
			Scope:              openAIAPIKeyScope,
			TargetProtocol:     capability.ProtocolOpenAIResponses,
			RequestConversion:  "adapt_responses_tools",
			ResponseConversion: "restore_responses_tools",
		},
		{
			Category:           offTableResponsesClientTools,
			ClientProtocol:     capability.ProtocolOpenAIResponses,
			Scope:              deepseekAPIKeyScope,
			TargetProtocol:     capability.ProtocolOpenAIResponses,
			RequestConversion:  "adapt_responses_tools",
			ResponseConversion: "restore_responses_tools",
		},
		{
			Category:           offTableResponsesClientTools,
			ClientProtocol:     capability.ProtocolOpenAIResponses,
			Scope:              grokResponsesScope,
			TargetProtocol:     capability.ProtocolOpenAIResponses,
			RequestConversion:  "adapt_responses_tools",
			ResponseConversion: "restore_responses_tools",
		},
		{
			Category:           offTableResponsesClientTools,
			ClientProtocol:     capability.ProtocolResponsesWebSocket,
			Scope:              wsOpenAIAPIKeyScope,
			TargetProtocol:     capability.ProtocolOpenAIResponses,
			RequestConversion:  "adapt_responses_tools_ws",
			ResponseConversion: "restore_responses_tools",
		},
		{
			Category:           offTableResponsesClientTools,
			ClientProtocol:     capability.ProtocolResponsesWebSocket,
			Scope:              wsGrokScope,
			TargetProtocol:     capability.ProtocolOpenAIResponses,
			RequestConversion:  "adapt_responses_tools_ws",
			ResponseConversion: "restore_responses_tools",
		},
		{
			Category:           offTableCursorChatInput,
			ClientProtocol:     capability.ProtocolOpenAIChatCompletions,
			Scope:              cursorResponsesScope,
			TargetProtocol:     capability.ProtocolOpenAIResponses,
			RequestConversion:  "responses_input_passthrough",
			ResponseConversion: responseFamilyResponses,
		},
		{
			Category:           offTableCursorChatInput,
			ClientProtocol:     capability.ProtocolOpenAIChatCompletions,
			Scope:              cursorRawChatScope,
			TargetProtocol:     capability.ProtocolOpenAIChatCompletions,
			RequestConversion:  requestFamilyChat,
			ResponseConversion: "chat_passthrough",
		},
		{
			Category:           offTableCursorChatInput,
			ClientProtocol:     capability.ProtocolOpenAIChatCompletions,
			Scope:              cursorCompatMessagesScope,
			TargetProtocol:     capability.ProtocolAnthropicMessages,
			RequestConversion:  requestFamilyMessagesCompat,
			ResponseConversion: responseFamilyMessages,
		},
		{
			Category:           offTableMessagesCountTokens,
			ClientProtocol:     capability.ProtocolAnthropicMessages,
			Scope:              geminiCountScope,
			TargetProtocol:     capability.ProtocolGeminiGenerateContent,
			RequestConversion:  "gemini_count_tokens_request",
			ResponseConversion: "anthropic_count_tokens_response",
		},
		{
			Category:           offTableMessagesCountTokens,
			ClientProtocol:     capability.ProtocolAnthropicMessages,
			Scope:              openAICountScope,
			TargetProtocol:     capability.ProtocolOpenAIResponses,
			RequestConversion:  "openai_input_tokens_projection",
			ResponseConversion: "anthropic_count_tokens_response",
		},
		{
			Category:           offTableLocalTokenEstimation,
			ClientProtocol:     capability.ProtocolAnthropicMessages,
			Scope:              localEstimateScope,
			TargetProtocol:     capability.ProtocolOpenAIResponses,
			RequestConversion:  "tokenestimate_responses_projection",
			ResponseConversion: "local_tiktoken_count",
		},
		{
			Category:           offTableResponsesLegacyMessages,
			ClientProtocol:     capability.ProtocolOpenAIResponses,
			Scope:              legacyMessagesIngressScope,
			TargetProtocol:     capability.ProtocolOpenAIResponses,
			RequestConversion:  "legacy_chat_messages_to_responses_input",
			ResponseConversion: "none",
		},
		{
			Category:           offTableResponsesLegacyMessages,
			ClientProtocol:     capability.ProtocolResponsesWebSocket,
			Scope:              openAIOAuthWSScope,
			TargetProtocol:     capability.ProtocolOpenAIResponses,
			RequestConversion:  "legacy_chat_messages_to_responses_input",
			ResponseConversion: "none",
		},
	}, nil
}

// formatConversionRoutesGolden renders route-table and off-table entries into
// deterministic tab-separated text with header comments and section markers.
// Every record occupies one six-column line terminated by a trailing newline.
func formatConversionRoutesGolden(routes []conversionRouteEntry, offTable []offTableConversionEntry) string {
	var b strings.Builder
	_, _ = b.WriteString("# Protocol conversion route inventory (generated by TestProtocolConversionRoutesGolden).\n")
	_, _ = b.WriteString("# Update with: (cd backend && TOKENROUTER_UPDATE_GOLDEN=1 go test ./internal/app -run '^TestProtocolConversionRoutesGolden$')\n")
	_, _ = b.WriteString("#\n")
	_, _ = b.WriteString("# [route_table] columns:\n")
	_, _ = b.WriteString("# client_protocol\tplatform\tprovider_type\tupstream_protocol\trequest_family\tresponse_family\n")
	_, _ = b.WriteString(routesSectionRouteTable + "\n")
	for _, r := range routes {
		_, _ = fmt.Fprintf(
			&b,
			"%s\t%s\t%s\t%s\t%s\t%s\n",
			r.ClientProtocol,
			r.Platform,
			r.ProviderType,
			r.UpstreamProtocol,
			r.RequestFamily,
			r.ResponseFamily,
		)
	}
	_, _ = b.WriteString("\n")
	_, _ = b.WriteString("# [off_table] columns:\n")
	_, _ = b.WriteString("# category\tclient_protocol\tscope\ttarget_protocol\trequest_conversion\tresponse_conversion\n")
	_, _ = b.WriteString(routesSectionOffTable + "\n")
	for _, entry := range offTable {
		_, _ = fmt.Fprintf(
			&b,
			"%s\t%s\t%s\t%s\t%s\t%s\n",
			entry.Category,
			entry.ClientProtocol,
			entry.Scope,
			entry.TargetProtocol,
			entry.RequestConversion,
			entry.ResponseConversion,
		)
	}
	return b.String()
}

// parseConversionRoutesGolden parses the tab-separated routes.txt text back
// into structured route-table and off-table slices. Blank columns, unexpected
// column counts, duplicate lines within a section, or records appearing before
// a section header return an error.
func parseConversionRoutesGolden(text string) ([]conversionRouteEntry, []offTableConversionEntry, error) {
	var routes []conversionRouteEntry
	var offTable []offTableConversionEntry
	seenLines := map[string]int{}
	section := ""

	lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	for idx, line := range lines {
		lineNo := idx + 1
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if line == routesSectionRouteTable || line == routesSectionOffTable {
			section = line
			continue
		}
		if prev, exists := seenLines[section+"\x00"+line]; exists {
			return nil, nil, fmt.Errorf("line %d duplicates line %d: %q", lineNo, prev, line)
		}
		seenLines[section+"\x00"+line] = lineNo

		fields := strings.Split(line, "\t")
		if len(fields) != 6 {
			return nil, nil, fmt.Errorf("line %d: expected 6 tab-separated fields, got %d", lineNo, len(fields))
		}
		for col, value := range fields {
			if strings.TrimSpace(value) == "" {
				return nil, nil, fmt.Errorf("line %d column %d is blank", lineNo, col+1)
			}
		}

		switch section {
		case routesSectionRouteTable:
			routes = append(routes, conversionRouteEntry{
				ClientProtocol:   protocol.ProtocolID(fields[0]),
				Platform:         fields[1],
				ProviderType:     fields[2],
				UpstreamProtocol: protocol.ProtocolID(fields[3]),
				RequestFamily:    fields[4],
				ResponseFamily:   fields[5],
			})
		case routesSectionOffTable:
			offTable = append(offTable, offTableConversionEntry{
				Category:           fields[0],
				ClientProtocol:     protocol.ProtocolID(fields[1]),
				Scope:              fields[2],
				TargetProtocol:     protocol.ProtocolID(fields[3]),
				RequestConversion:  fields[4],
				ResponseConversion: fields[5],
			})
		default:
			return nil, nil, fmt.Errorf("line %d appears before any section header", lineNo)
		}
	}
	return routes, offTable, nil
}

// assertClientProtocolScopePartition verifies that every public client protocol
// in capability.SupportedGroupClientProtocols belongs to either the four
// in-scope text protocols or the explicit non-text exclusion list. Any newly
// added public protocol fails this check until its conversion scope is decided.
func assertClientProtocolScopePartition(t *testing.T) {
	t.Helper()

	inScope := inScopeTextClientProtocols()
	outOfScope := outOfScopeClientProtocols()
	var combined []protocol.ProtocolID
	combined = append(combined, inScope...)
	combined = append(combined, outOfScope...)

	publicProtocols := capability.SupportedGroupClientProtocols("")
	for _, id := range publicProtocols {
		inText := slices.Contains(inScope, id)
		inNonText := slices.Contains(outOfScope, id)
		require.NotEqual(t, inText, inNonText, "client protocol %q must belong to exactly one scope partition", id)
	}
	require.ElementsMatch(t, publicProtocols, combined)
}

// assertOpenAIAuthModesPreserveTextConversions confirms that OpenAI OAuth mode
// variants do not alter SupportsProtocolConversion for any text client source.
// This ensures the four-column route key omits no mode-specific text route.
func assertOpenAIAuthModesPreserveTextConversions(t *testing.T) {
	t.Helper()

	modes := []string{
		capability.OpenAIAuthModePersonalAccessToken,
		capability.OpenAIAuthModeAgentIdentity,
	}
	for _, source := range inScopeTextClientProtocols() {
		for _, target := range capability.ProtocolFallbackTargets(capability.PlatformOpenAI, source) {
			want := capability.SupportsProtocolConversion(
				capability.PlatformOpenAI,
				capability.ProviderTypeOAuth,
				"",
				source,
				target,
			)
			for _, mode := range modes {
				got := capability.SupportsProtocolConversion(
					capability.PlatformOpenAI,
					capability.ProviderTypeOAuth,
					mode,
					source,
					target,
				)
				require.Equal(t, want, got, "OpenAI OAuth mode %q changed text conversion %s -> %s", mode, source, target)
			}
		}
	}
}

// assertOutOfScopeNonTextConversions scans every non-text public protocol across
// platforms, provider types, and OpenAI OAuth modes. It fails when any
// non-text conversion appears or disappears outside the known transport and
// media adaptation list.
func assertOutOfScopeNonTextConversions(t *testing.T) {
	t.Helper()

	var actual []nonTextConversionEntry
	for _, platform := range capability.ProviderPlatforms() {
		for _, providerType := range registeredProviderTypes() {
			modes := []string{""}
			if platform == capability.PlatformOpenAI && providerType == capability.ProviderTypeOAuth {
				modes = append(
					modes,
					capability.OpenAIAuthModePersonalAccessToken,
					capability.OpenAIAuthModeAgentIdentity,
				)
			}
			for _, mode := range modes {
				for _, source := range outOfScopeClientProtocols() {
					for _, target := range capability.ProtocolFallbackTargets(platform, source) {
						if !capability.SupportsProtocolConversion(platform, providerType, mode, source, target) {
							continue
						}
						// Record mode-independent OAuth conversions once under the
						// default empty mode and retain explicit modes only when
						// SupportsProtocolConversion requires them.
						if mode != "" && capability.SupportsProtocolConversion(platform, providerType, "", source, target) {
							continue
						}
						actual = append(actual, nonTextConversionEntry{
							ClientProtocol:   source,
							Platform:         platform,
							ProviderType:     providerType,
							AuthMode:         mode,
							UpstreamProtocol: target,
						})
					}
				}
			}
		}
	}

	expected := []nonTextConversionEntry{
		{capability.ProtocolImagesGenerations, capability.PlatformOpenAI, capability.ProviderTypeOAuth, "", capability.ProtocolOpenAIResponses},
		{capability.ProtocolImagesEdits, capability.PlatformOpenAI, capability.ProviderTypeOAuth, "", capability.ProtocolOpenAIResponses},
		{capability.ProtocolResponsesWebSocket, capability.PlatformOpenAI, capability.ProviderTypeOAuth, "", capability.ProtocolOpenAIResponses},
		{capability.ProtocolAlphaSearch, capability.PlatformOpenAI, capability.ProviderTypeOAuth, capability.OpenAIAuthModePersonalAccessToken, capability.ProtocolOpenAIResponses},
		{capability.ProtocolResponsesWebSocket, capability.PlatformOpenAI, capability.ProviderTypeAPIKey, "", capability.ProtocolOpenAIResponses},
		{capability.ProtocolResponsesWebSocket, capability.PlatformGrok, capability.ProviderTypeOAuth, "", capability.ProtocolOpenAIResponses},
		{capability.ProtocolResponsesCompact, capability.PlatformGrok, capability.ProviderTypeOAuth, "", capability.ProtocolOpenAIResponses},
		{capability.ProtocolWebSearch, capability.PlatformGrok, capability.ProviderTypeOAuth, "", capability.ProtocolOpenAIResponses},
		{capability.ProtocolXSearch, capability.PlatformGrok, capability.ProviderTypeOAuth, "", capability.ProtocolOpenAIResponses},
		{capability.ProtocolResponsesWebSocket, capability.PlatformGrok, capability.ProviderTypeAPIKey, "", capability.ProtocolOpenAIResponses},
		{capability.ProtocolResponsesCompact, capability.PlatformGrok, capability.ProviderTypeAPIKey, "", capability.ProtocolOpenAIResponses},
		{capability.ProtocolWebSearch, capability.PlatformGrok, capability.ProviderTypeAPIKey, "", capability.ProtocolOpenAIResponses},
		{capability.ProtocolXSearch, capability.PlatformGrok, capability.ProviderTypeAPIKey, "", capability.ProtocolOpenAIResponses},
	}
	require.Equal(t, expected, actual)
}

// classifyUpstreamFamilies maps a provider platform and target upstream
// protocol to its merged request conversion family and response reader family.
// Unrecognized platform and target combinations return an error so newly added
// route-table targets cannot silently bypass family verification.
func classifyUpstreamFamilies(platform string, target protocol.ProtocolID) (string, string, error) {
	openAICompat := isOpenAICompatibleTextPlatform(platform)
	switch target {
	case capability.ProtocolOpenAIResponses:
		if openAICompat {
			return requestFamilyResponses, responseFamilyResponses, nil
		}
	case capability.ProtocolOpenAIChatCompletions:
		if openAICompat {
			return requestFamilyChat, responseFamilyChat, nil
		}
	case capability.ProtocolAnthropicMessages:
		if platform == capability.PlatformAnthropic {
			return requestFamilyMessagesAnthropic, responseFamilyMessages, nil
		}
		if openAICompat {
			return requestFamilyMessagesCompat, responseFamilyMessages, nil
		}
	case capability.ProtocolGeminiGenerateContent:
		if platform == capability.PlatformAntigravity {
			return requestFamilyAntigravity, responseFamilyAntigravity, nil
		}
		if platform == capability.PlatformGemini || platform == capability.PlatformOpenCode {
			return requestFamilyGemini, responseFamilyGemini, nil
		}
	case capability.ProtocolQoderChat:
		if platform == capability.PlatformQoder {
			return requestFamilyQoder, responseFamilyQoder, nil
		}
	}
	return "", "", fmt.Errorf("unclassified upstream route platform=%q target=%q", platform, target)
}

// isOpenAICompatibleTextPlatform reports whether platform executes text
// conversions through the OpenAI-compatible executor family when not targeting
// Gemini GenerateContent.
func isOpenAICompatibleTextPlatform(platform string) bool {
	switch platform {
	case capability.PlatformOpenAI,
		capability.PlatformGrok,
		capability.PlatformKimi,
		capability.PlatformZhipu,
		capability.PlatformDeepseek,
		capability.PlatformOpenCode:
		return true
	default:
		return false
	}
}

// inScopeTextClientProtocols returns the four text client protocols in
// capability catalogue order.
func inScopeTextClientProtocols() []protocol.ProtocolID {
	want := []protocol.ProtocolID{
		capability.ProtocolAnthropicMessages,
		capability.ProtocolOpenAIResponses,
		capability.ProtocolOpenAIChatCompletions,
		capability.ProtocolGeminiGenerateContent,
	}
	var ordered []protocol.ProtocolID
	for _, id := range capability.SupportedGroupClientProtocols("") {
		if slices.Contains(want, id) {
			ordered = append(ordered, id)
		}
	}
	return ordered
}

// outOfScopeClientProtocols returns public client protocols that Section 2.2
// excludes from IR text protocol conversion.
func outOfScopeClientProtocols() []protocol.ProtocolID {
	return []protocol.ProtocolID{
		capability.ProtocolSystemOne,
		capability.ProtocolEmbeddings,
		capability.ProtocolImagesGenerations,
		capability.ProtocolImagesEdits,
		capability.ProtocolImageBatches,
		capability.ProtocolVideosGenerations,
		capability.ProtocolVideosEdits,
		capability.ProtocolVideosExtensions,
		capability.ProtocolTTS,
		capability.ProtocolSTT,
		capability.ProtocolCustomVoices,
		capability.ProtocolVoiceRealtime,
		capability.ProtocolResponsesWebSocket,
		capability.ProtocolLive,
		capability.ProtocolResponsesCompact,
		capability.ProtocolAlphaSearch,
		capability.ProtocolWebSearch,
		capability.ProtocolXSearch,
	}
}

// registeredProviderTypes returns the seven provider authentication types
// declared by routing/capability in administrative catalogue traversal order.
func registeredProviderTypes() []string {
	return []string{
		capability.ProviderTypeOAuth,
		capability.ProviderTypeSetupToken,
		capability.ProviderTypeAPIKey,
		capability.ProviderTypeUpstream,
		capability.ProviderTypeBedrock,
		capability.ProviderTypeServiceAccount,
		capability.ProviderTypeCosy,
	}
}

// resolveNativeProtocolScope formats semicolon-separated platform:types entries
// for platforms and optional allowedTypes that natively support targetProtocol.
// Empty resolution returns an error so stale off-table rules fail immediately.
func resolveNativeProtocolScope(platforms []string, targetProtocol protocol.ProtocolID, allowedTypes ...string) (string, error) {
	var parts []string
	for _, platform := range platforms {
		var matchedTypes []string
		for _, providerType := range registeredProviderTypes() {
			if len(allowedTypes) > 0 && !slices.Contains(allowedTypes, providerType) {
				continue
			}
			if slices.Contains(capability.NativeProtocolOptions(platform, providerType, ""), targetProtocol) {
				matchedTypes = append(matchedTypes, providerType)
			}
		}
		if len(matchedTypes) > 0 {
			parts = append(parts, platform+":"+strings.Join(matchedTypes, ","))
		}
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("no native provider types resolved for %v on %s", platforms, targetProtocol)
	}
	return strings.Join(parts, ";"), nil
}

// resolveWSFunctionToolBridgeScope formats platform:types for provider types on
// platform that bridge Responses WebSocket turns to HTTP Responses with
// function-only client-tool lowering. OpenAI OAuth is excluded because it uses
// native WebSocket or Codex OAuth normalization, and an empty match returns an
// error.
func resolveWSFunctionToolBridgeScope(platform string) (string, error) {
	var matchedTypes []string
	for _, providerType := range registeredProviderTypes() {
		if platform == capability.PlatformOpenAI && providerType == capability.ProviderTypeOAuth {
			continue
		}
		if capability.SupportsProtocolConversion(
			platform,
			providerType,
			"",
			capability.ProtocolResponsesWebSocket,
			capability.ProtocolOpenAIResponses,
		) {
			matchedTypes = append(matchedTypes, providerType)
		}
	}
	if len(matchedTypes) == 0 {
		return "", fmt.Errorf("no WS HTTP function-tool bridge provider types resolved for %s", platform)
	}
	return platform + ":" + strings.Join(matchedTypes, ","), nil
}

// resolveAnyTextNativeScope formats semicolon-separated platform:types entries
// for platforms whose provider types natively support at least one text
// protocol. Empty resolution returns an error so removed platform support fails
// immediately.
func resolveAnyTextNativeScope(platforms []string) (string, error) {
	textProtocols := inScopeTextClientProtocols()
	var parts []string
	for _, platform := range platforms {
		var matchedTypes []string
		for _, providerType := range registeredProviderTypes() {
			native := capability.NativeProtocolOptions(platform, providerType, "")
			hasText := false
			for _, candidate := range textProtocols {
				if slices.Contains(native, candidate) {
					hasText = true
					break
				}
			}
			if hasText {
				matchedTypes = append(matchedTypes, providerType)
			}
		}
		if len(matchedTypes) > 0 {
			parts = append(parts, platform+":"+strings.Join(matchedTypes, ","))
		}
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("no text-capable provider types resolved for %v", platforms)
	}
	return strings.Join(parts, ";"), nil
}

// resolveConversionTargetScope formats platform:types for provider types on
// platform that support converting source into target. Empty resolution returns
// an error so stale conversion assumptions fail immediately.
func resolveConversionTargetScope(platform string, source, target protocol.ProtocolID) (string, error) {
	var matchedTypes []string
	for _, providerType := range registeredProviderTypes() {
		if capability.SupportsProtocolConversion(platform, providerType, "", source, target) {
			matchedTypes = append(matchedTypes, providerType)
		}
	}
	if len(matchedTypes) == 0 {
		return "", fmt.Errorf("no conversion provider types resolved for %s %s -> %s", platform, source, target)
	}
	return platform + ":" + strings.Join(matchedTypes, ","), nil
}

// joinScopes concatenates non-empty scope segments with semicolons while
// skipping blank inputs.
func joinScopes(scopes ...string) string {
	var nonEmpty []string
	for _, s := range scopes {
		if s != "" {
			nonEmpty = append(nonEmpty, s)
		}
	}
	return strings.Join(nonEmpty, ";")
}

// containsConversionRoute reports whether routes contains an entry matching the
// supplied four-part conversion key.
func containsConversionRoute(routes []conversionRouteEntry, source protocol.ProtocolID, platform, providerType string, target protocol.ProtocolID) bool {
	for _, r := range routes {
		if r.ClientProtocol == source && r.Platform == platform && r.ProviderType == providerType && r.UpstreamProtocol == target {
			return true
		}
	}
	return false
}

// containsConversionRouteWithFamilies reports whether routes contains an entry
// matching both the four-part conversion key and the supplied upstream families.
func containsConversionRouteWithFamilies(
	routes []conversionRouteEntry,
	source protocol.ProtocolID,
	platform, providerType string,
	target protocol.ProtocolID,
	reqFamily, respFamily string,
) bool {
	for _, r := range routes {
		if r.ClientProtocol == source &&
			r.Platform == platform &&
			r.ProviderType == providerType &&
			r.UpstreamProtocol == target &&
			r.RequestFamily == reqFamily &&
			r.ResponseFamily == respFamily {
			return true
		}
	}
	return false
}
