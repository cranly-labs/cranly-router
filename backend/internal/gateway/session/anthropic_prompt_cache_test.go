package session

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	protocolanthropic "github.com/TokenFlux/TokenRouter/internal/protocol/anthropic"
)

// TestAnthropicPromptCacheScopePrefixAndExpiry binds and extends a Messages digest
// with an injected clock. It checks provider/key isolation, trimmed keys, longest
// prefix reuse, old-chain removal, and rejection at the expiration instant.
func TestAnthropicPromptCacheScopePrefixAndExpiry(t *testing.T) {
	now := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)
	store := NewAnthropicPromptCache(func() time.Time { return now })
	store.Bind(1, 2, "s:a-u:b", " key ", "", time.Hour)
	key, chain := store.Find(1, 2, "s:a-u:b-a:c-u:d")
	require.Equal(t, "key", key)
	require.Equal(t, "s:a-u:b", chain)
	key, _ = store.Find(2, 2, "s:a-u:b")
	require.Empty(t, key)
	key, _ = store.Find(1, 3, "s:a-u:b")
	require.Empty(t, key)
	store.Bind(1, 2, "s:a-u:b-a:c", "new", "s:a-u:b", time.Hour)
	key, _ = store.Find(1, 2, "s:a-u:b")
	require.Empty(t, key)
	key, chain = store.Find(1, 2, "s:a-u:b-a:c-u:d")
	require.Equal(t, "new", key)
	require.Equal(t, "s:a-u:b-a:c", chain)
	now = now.Add(time.Hour)
	key, chain = store.Find(1, 2, "s:a-u:b-a:c-u:d")
	require.Empty(t, key)
	require.Empty(t, chain)
}

// TestAnthropicPromptCacheConcurrentBindings writes one digest under twenty providers
// concurrently. After joining the writers, each provider returns its own prompt
// key and matching chain under the shared API Key.
func TestAnthropicPromptCacheConcurrentBindings(t *testing.T) {
	store := NewAnthropicPromptCache(time.Now)
	var pending sync.WaitGroup
	// Exercise writes in overlapping goroutines, then join every writer before
	// asserting per-provider lookup so assertions observe completed bindings.
	for id := int64(1); id <= 20; id++ {
		pending.Go(func() {
			store.Bind(id, 5, "s:a-u:b", fmt.Sprint(id), "", time.Hour)
		})
	}
	pending.Wait()
	for id := int64(1); id <= 20; id++ {
		key, chain := store.Find(id, 5, "s:a-u:b-a:c")
		require.Equal(t, fmt.Sprint(id), key)
		require.Equal(t, "s:a-u:b", chain)
	}
}

// TestBuildAnthropicDigestChainBytesAndMetadataKeyParity feeds spaced JSON,
// null system fields, 64-bit integers, and malformed payloads into the raw-byte
// prompt-cache helpers. Valid inputs match the legacy struct helpers exactly,
// while nil, invalid JSON, and session-free metadata return empty strings.
func TestBuildAnthropicDigestChainBytesAndMetadataKeyParity(t *testing.T) {
	body := []byte(`{
		"model": "claude-sonnet-4-5",
		"extra_int64": 9007199254740993,
		"system": [
			{"type": "text", "text": "Follow repo rules."}
		],
		"metadata": {
			"user_id": "user_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef_account_11111111-2222-3333-4444-555555555555_session_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
			"custom_tag": "preserve"
		},
		"messages": [
			{"role": "user", "content": "Open main.go"},
			{"role": "assistant", "content": [{"type": "text", "text": "Reading main.go"}]},
			{"role": "user", "content": [{"type": "text", "text": "Run tests"}]}
		]
	}`)

	var req protocolanthropic.AnthropicRequest
	require.NoError(t, json.Unmarshal(body, &req))

	require.Equal(t, BuildAnthropicDigestChain(&req), BuildAnthropicDigestChainBytes(body))
	require.NotEmpty(t, BuildAnthropicDigestChainBytes(body))
	require.Equal(t, AnthropicMetadataPromptCacheKey(&req), AnthropicMetadataPromptCacheKeyBytes(body))
	require.NotEmpty(t, AnthropicMetadataPromptCacheKeyBytes(body))

	nullSysBody := []byte(`{"model":"claude-sonnet-4-5","system":null,"messages":[{"role":"user","content":"hi"}]}`)
	var nullSysReq protocolanthropic.AnthropicRequest
	require.NoError(t, json.Unmarshal(nullSysBody, &nullSysReq))
	require.Equal(t, BuildAnthropicDigestChain(&nullSysReq), BuildAnthropicDigestChainBytes(nullSysBody))

	require.Empty(t, BuildAnthropicDigestChainBytes(nil))
	require.Empty(t, BuildAnthropicDigestChainBytes([]byte(`{invalid`)))
	require.Empty(t, AnthropicMetadataPromptCacheKeyBytes([]byte(`{"metadata":{"user_id":"plain"}}`)))
}
