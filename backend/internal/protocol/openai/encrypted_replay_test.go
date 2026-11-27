package openai

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Replay cleanup keeps a reasoning summary when encrypted_content and content
// are nil. The fixtures also cover removal of empty reasoning items and the
// encrypted versus unencrypted compaction distinction through change flags and
// retained input items.

// TestTrimOpenAIEncryptedReasoningItems_ContentNull surrounds a reasoning summary
// with user and assistant messages. Cleanup retains all three entries while
// removing both nil content and encrypted_content from the reasoning map.
func TestTrimOpenAIEncryptedReasoningItems_ContentNull(t *testing.T) {
	reqBody := map[string]any{
		"model": "grok-4.5",
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": "hi"},
			map[string]any{
				"type":              "reasoning",
				"summary":           []any{map[string]any{"type": "summary_text", "text": "thinking..."}},
				"content":           nil,
				"encrypted_content": nil,
			},
			map[string]any{"type": "message", "role": "assistant", "content": "Hello!"},
		},
	}

	changed := TrimEncryptedReasoningItems(reqBody)
	require.True(t, changed)

	input, ok := reqBody["input"].([]any)
	require.True(t, ok)
	require.Len(t, input, 3)

	reasoning, ok := input[1].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "reasoning", reasoning["type"])
	assert.NotNil(t, reasoning["summary"])
	_, hasContent := reasoning["content"]
	assert.False(t, hasContent, "content: null should be stripped")
	_, hasEncrypted := reasoning["encrypted_content"]
	assert.False(t, hasEncrypted, "encrypted_content should be stripped")
}

// TestTrimOpenAIEncryptedReasoningItems_ContentNullOnly supplies a summary with
// nil content and no encrypted_content field. Cleanup reports a change, retains
// the one input item, and removes its content key.
func TestTrimOpenAIEncryptedReasoningItems_ContentNullOnly(t *testing.T) {
	reqBody := map[string]any{
		"model": "grok-4.5",
		"input": []any{
			map[string]any{
				"type":    "reasoning",
				"summary": []any{map[string]any{"type": "summary_text", "text": "ok"}},
				"content": nil,
			},
		},
	}

	changed := TrimEncryptedReasoningItems(reqBody)
	require.True(t, changed)

	input, ok := reqBody["input"].([]any)
	require.True(t, ok)
	require.Len(t, input, 1)

	reasoning, ok := input[0].(map[string]any)
	require.True(t, ok)
	_, hasContent := reasoning["content"]
	assert.False(t, hasContent, "content: null should be stripped even without encrypted_content")
}

// TestTrimOpenAIEncryptedReasoningItems_ContentNonNull supplies plaintext
// reasoning content without encrypted_content. Cleanup reports no change and
// retains the exact content string available through the input map.
func TestTrimOpenAIEncryptedReasoningItems_ContentNonNull(t *testing.T) {
	reqBody := map[string]any{
		"model": "grok-4.5",
		"input": []any{
			map[string]any{
				"type":    "reasoning",
				"summary": []any{map[string]any{"type": "summary_text", "text": "ok"}},
				"content": "some actual content",
			},
		},
	}

	changed := TrimEncryptedReasoningItems(reqBody)
	assert.False(t, changed, "non-null content should not be stripped")

	input, ok := reqBody["input"].([]any)
	require.True(t, ok)
	reasoning, ok := input[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "some actual content", reasoning["content"])
}

// TestTrimOpenAIEncryptedReasoningItems_NoReasoningItems supplies one ordinary
// user message. The cleanup change flag stays false for this request, which has
// no reasoning or encrypted compaction item.
func TestTrimOpenAIEncryptedReasoningItems_NoReasoningItems(t *testing.T) {
	reqBody := map[string]any{
		"model": "grok-4.5",
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": "hi"},
		},
	}

	changed := TrimEncryptedReasoningItems(reqBody)
	assert.False(t, changed)
}

// TestTrimOpenAIEncryptedReasoningItems_Compaction compares both encrypted
// compaction labels with an unencrypted compaction object. Encrypted items are
// removed ahead of the remaining message; the unencrypted item and two-item
// input retain their type and length.
func TestTrimOpenAIEncryptedReasoningItems_Compaction(t *testing.T) {
	tests := []struct {
		name      string // subtest display name
		itemType  string // supplied compaction label
		encrypted bool   // include ciphertext field
		changed   bool   // expected cleanup flag
	}{
		{name: "compaction", itemType: "compaction", encrypted: true, changed: true},
		{name: "compaction summary", itemType: "compaction_summary", encrypted: true, changed: true},
		{name: "unencrypted compaction", itemType: "compaction", encrypted: false, changed: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := map[string]any{"type": tt.itemType, "id": "cmp_stale"}
			if tt.encrypted {
				item["encrypted_content"] = "gAAA"
			}
			reqBody := map[string]any{"input": []any{
				item,
				map[string]any{"type": "message", "content": "hi"},
			}}

			changed := TrimEncryptedReasoningItems(reqBody)
			assert.Equal(t, tt.changed, changed)
			input, ok := reqBody["input"].([]any)
			require.True(t, ok)
			require.NotEmpty(t, input)
			first, ok := input[0].(map[string]any)
			require.True(t, ok)
			if tt.changed {
				require.Len(t, input, 1)
				assert.Equal(t, "message", first["type"])
				return
			}
			require.Len(t, input, 2)
			assert.Equal(t, tt.itemType, first["type"])
		})
	}
}

// TestTrimOpenAIEncryptedReasoningItems_ContentNullDropsBareSkeleton starts with
// a reasoning item containing only type and nil content. Cleanup reports a
// change and removes the input key after dropping the emptied item.
func TestTrimOpenAIEncryptedReasoningItems_ContentNullDropsBareSkeleton(t *testing.T) {
	reqBody := map[string]any{
		"input": []any{
			map[string]any{"type": "reasoning", "content": nil},
		},
	}

	changed := TrimEncryptedReasoningItems(reqBody)
	require.True(t, changed)
	_, hasInput := reqBody["input"]
	assert.False(t, hasInput, "bare reasoning skeleton should be dropped, emptying input")
}

// TestStripResponsesEncryptedReasoningBytes_LosslessCleanup exercises raw-byte
// encrypted reasoning and compaction cleanup across summary-less skeletons,
// visible summaries, and unencrypted transcripts. It verifies that 64-bit
// integer literals survive verbatim, invalid UTF-8 strings do not fail decoding,
// and unencrypted payloads return their original slice unchanged.
func TestStripResponsesEncryptedReasoningBytes_LosslessCleanup(t *testing.T) {
	t.Run("drops summary-less reasoning skeleton while preserving 64-bit integers and accepting invalid UTF-8", func(t *testing.T) {
		raw := []byte("{\"model\":\"grok-4.5\",\"stream\":true,\"custom_int\":9007199254740993,\"input\":[" +
			"{\"type\":\"message\",\"role\":\"user\",\"content\":\"hi \xff\"}," +
			"{\"type\":\"reasoning\",\"id\":\"rs_empty\",\"summary\":[],\"encrypted_content\":\"xai_cipher_1\"}," +
			"{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup\",\"arguments\":\"{\\\"id\\\":9007199254740993}\"}," +
			"{\"type\":\"function_call_output\",\"call_id\":\"call_1\",\"output\":\"ok\"}" +
			"]}")

		stripped, changed := StripResponsesEncryptedReasoningBytes(raw)
		require.True(t, changed)
		assert.NotContains(t, string(stripped), "xai_cipher_1")
		assert.NotContains(t, string(stripped), "rs_empty")
		assert.Contains(t, string(stripped), `"custom_int":9007199254740993`)
		assert.Contains(t, string(stripped), `\"id\":9007199254740993`)
		assert.Contains(t, string(stripped), "hi \ufffd")
	})

	t.Run("retains reasoning item with visible summary and strips id when store is false", func(t *testing.T) {
		raw := []byte(`{` +
			`"model":"grok-4.5",` +
			`"store":false,` +
			`"input":[` +
			`{"type":"reasoning","id":"rs_vis","summary":[{"type":"summary_text","text":"kept summary"}],"content":null,"encrypted_content":"xai_cipher_2"},` +
			`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}` +
			`]` +
			`}`)

		stripped, changed := StripResponsesEncryptedReasoningBytes(raw)
		require.True(t, changed)
		assert.NotContains(t, string(stripped), "xai_cipher_2")
		assert.NotContains(t, string(stripped), `"rs_vis"`)
		assert.NotContains(t, string(stripped), `"content":null`)
		assert.Contains(t, string(stripped), `"kept summary"`)
	})

	t.Run("drops encrypted compaction and keeps empty input array when only skeleton existed", func(t *testing.T) {
		raw := []byte(`{"model":"grok-4.5","input":[{"type":"compaction","id":"cmp_1","encrypted_content":"enc_cmp"},{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"enc_r"}]}`)
		stripped, changed := StripResponsesEncryptedReasoningBytes(raw)
		require.True(t, changed)
		assert.Equal(t, `{"model":"grok-4.5","input":[]}`, string(stripped))
	})

	t.Run("returns original slice unchanged when no encrypted reasoning exists", func(t *testing.T) {
		raw := []byte(`{"model":"grok-4.5","input":[{"type":"message","role":"user","content":"literal encrypted_content mention"}]}`)
		stripped, changed := StripResponsesEncryptedReasoningBytes(raw)
		assert.False(t, changed)
		assert.Equal(t, string(raw), string(stripped))
	})
}
