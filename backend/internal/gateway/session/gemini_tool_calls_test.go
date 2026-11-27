package session

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// geminiWireContentsDoc decodes top-level Gemini GenerateContent contents turns in round-trip tests.
type geminiWireContentsDoc struct {
	Contents []struct {
		Role  string `json:"role"` // Gemini turn role ("user" or "model")
		Parts []struct {
			ThoughtSignature string `json:"thoughtSignature"` // wire thoughtSignature field
			FunctionCall     struct {
				ID   string `json:"id"`   // wire functionCall.id
				Name string `json:"name"` // wire functionCall.name
			} `json:"functionCall"` // wire model functionCall payload
			FunctionResponse struct {
				ID   string `json:"id"`   // wire functionResponse.id
				Name string `json:"name"` // wire functionResponse.name
			} `json:"functionResponse"` // wire user functionResponse payload
		} `json:"parts"` // ordered Gemini content parts
	} `json:"contents"` // ordered conversation turns
}

// TestGeminiToolCallsScopeAndExpiry verifies that replay can be repeated by
// the same caller and destination. Changing scope or expiring the original
// record must make the lookup miss.
func TestGeminiToolCallsScopeAndExpiry(t *testing.T) {
	var calls GeminiToolCalls
	scope := GeminiToolScope{APIKeyID: 1, ProviderID: 2, Model: "gemini"}
	calls.Remember(scope, "client-call", "original-id", "signature")
	for range 2 {
		id, signature, ok := calls.Lookup(scope, "client-call")
		require.True(t, ok)
		require.Equal(t, "original-id", id)
		require.Equal(t, "signature", signature)
	}
	other := scope
	other.APIKeyID++
	_, _, ok := calls.Lookup(other, "client-call")
	require.False(t, ok)

	entry := calls.order.Front()
	expired, ok := entry.Value.(geminiToolEntry)
	require.True(t, ok)
	expired.expires = time.Now().Add(-time.Second)
	entry.Value = expired
	_, _, ok = calls.Lookup(scope, "client-call")
	require.False(t, ok)
	require.Zero(t, calls.bytes)
	require.Empty(t, calls.entries)
}

// TestGeminiToolCallsEvictsOldestWithinBudget fills the metadata budget with
// two large signatures. The newer call stays available, and an unsigned
// call is recorded as a hit even when its original fields are empty.
func TestGeminiToolCallsEvictsOldestWithinBudget(t *testing.T) {
	var calls GeminiToolCalls
	scope := GeminiToolScope{APIKeyID: 1, ProviderID: 2, Model: "gemini"}
	// A shared fixture string still counts for each entry because two
	// distinct upstream responses would hold separate copies.
	signature := strings.Repeat("x", geminiToolCallMaxBytes/2)
	calls.Remember(scope, "first", "first", signature)
	calls.Remember(scope, "second", "second", signature)
	_, _, ok := calls.Lookup(scope, "first")
	require.False(t, ok)
	id, _, ok := calls.Lookup(scope, "second")
	require.True(t, ok)
	require.Equal(t, "second", id)

	calls.Remember(scope, "unsigned", "", "")
	id, opaque, ok := calls.Lookup(scope, "unsigned")
	require.True(t, ok)
	require.Empty(t, id)
	require.Empty(t, opaque)
}

// TestGeminiCallMemoryNilAndInvalidScopeSafety verifies nil-receiver safety on
// GeminiToolCalls and geminiCallMemory alongside invalid scope and empty ID guards.
// Invalid inputs must return nil or miss cleanly without panicking or allocating entries.
func TestGeminiCallMemoryNilAndInvalidScopeSafety(t *testing.T) {
	validScope := GeminiToolScope{APIKeyID: 10, ProviderID: 20, Model: "gemini-2.5-pro"}

	var nilStore *GeminiToolCalls
	require.Nil(t, nilStore.CallMemory(validScope))
	nilStore.Remember(validScope, "call-1", "up-1", "sig-1")
	id, sig, ok := nilStore.Lookup(validScope, "call-1")
	require.False(t, ok)
	require.Empty(t, id)
	require.Empty(t, sig)

	var nilMem *geminiCallMemory
	nilMem.Remember("call-1", convert.CallBinding{
		UpstreamID: "up-1",
		Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-1"},
	})
	binding, found := nilMem.Recall("call-1")
	require.False(t, found)
	require.Equal(t, convert.CallBinding{}, binding)

	var calls GeminiToolCalls
	invalidScopes := []GeminiToolScope{
		{},
		{APIKeyID: 0, ProviderID: 20, Model: "gemini-2.5-pro"},
		{APIKeyID: -1, ProviderID: 20, Model: "gemini-2.5-pro"},
		{APIKeyID: 10, ProviderID: 0, Model: "gemini-2.5-pro"},
		{APIKeyID: 10, ProviderID: -3, Model: "gemini-2.5-pro"},
		{APIKeyID: 10, ProviderID: 20, Model: ""},
	}
	for _, bad := range invalidScopes {
		require.Nil(t, calls.CallMemory(bad), "invalid scope %+v must return nil CallMemory", bad)
	}

	mem := calls.CallMemory(validScope)
	require.NotNil(t, mem)
	mem.Remember("", convert.CallBinding{
		UpstreamID: "up-empty",
		Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig"},
	})
	_, found = mem.Recall("")
	require.False(t, found)
	require.Empty(t, calls.entries)
	require.Zero(t, calls.bytes)
}

// TestGeminiCallMemoryThreeDimensionalScopeIsolation verifies independent key
// spaces across APIKeyID, ProviderID, and mapped upstream Model under identical
// client call IDs. Changing any single dimension must miss without affecting sibling scopes.
func TestGeminiCallMemoryThreeDimensionalScopeIsolation(t *testing.T) {
	var calls GeminiToolCalls
	base := GeminiToolScope{APIKeyID: 101, ProviderID: 202, Model: "gemini-2.5-pro"}
	diffKey := GeminiToolScope{APIKeyID: 102, ProviderID: 202, Model: "gemini-2.5-pro"}
	diffProvider := GeminiToolScope{APIKeyID: 101, ProviderID: 203, Model: "gemini-2.5-pro"}
	diffModel := GeminiToolScope{APIKeyID: 101, ProviderID: 202, Model: "gemini-2.5-flash"}

	memBase := calls.CallMemory(base)
	memKey := calls.CallMemory(diffKey)
	memProvider := calls.CallMemory(diffProvider)
	memModel := calls.CallMemory(diffModel)

	memBase.Remember("shared_call_id", convert.CallBinding{
		UpstreamID: "up-base",
		Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-base"},
	})
	memKey.Remember("shared_call_id", convert.CallBinding{
		UpstreamID: "up-key",
		Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-key"},
	})

	// Scopes that never recorded shared_call_id must miss even when two dimensions match.
	_, ok := memProvider.Recall("shared_call_id")
	require.False(t, ok)
	_, ok = memModel.Recall("shared_call_id")
	require.False(t, ok)

	memProvider.Remember("shared_call_id", convert.CallBinding{
		UpstreamID: "up-provider",
		Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-provider"},
	})
	memModel.Remember("shared_call_id", convert.CallBinding{
		UpstreamID: "up-model",
		Token:      &ir.ReplayToken{Scheme: ir.SchemeAntigravityClaude, Data: "sig-model"},
	})

	// Repeated Recall calls must keep each scope's own binding available for multi-turn replay.
	for range 2 {
		gotBase, ok := memBase.Recall("shared_call_id")
		require.True(t, ok)
		require.Equal(t, "up-base", gotBase.UpstreamID)
		require.Equal(t, &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-base"}, gotBase.Token)

		gotKey, ok := memKey.Recall("shared_call_id")
		require.True(t, ok)
		require.Equal(t, "up-key", gotKey.UpstreamID)
		require.Equal(t, &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-key"}, gotKey.Token)

		gotProvider, ok := memProvider.Recall("shared_call_id")
		require.True(t, ok)
		require.Equal(t, "up-provider", gotProvider.UpstreamID)
		require.Equal(t, &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-provider"}, gotProvider.Token)

		gotModel, ok := memModel.Recall("shared_call_id")
		require.True(t, ok)
		require.Equal(t, "up-model", gotModel.UpstreamID)
		require.Equal(t, &ir.ReplayToken{Scheme: ir.SchemeAntigravityClaude, Data: "sig-model"}, gotModel.Token)
	}
}

// TestGeminiCallMemorySignedUnsignedNamelessAndSchemes verifies lossless storage
// of gemini and antigravity_claude schemes, account-bound ProviderID, nameless
// calls, and observed unsigned calls. Recall must return detached ReplayToken
// pointers and distinguish nil-token hits from cache misses.
func TestGeminiCallMemorySignedUnsignedNamelessAndSchemes(t *testing.T) {
	var calls GeminiToolCalls
	scope := GeminiToolScope{APIKeyID: 7, ProviderID: 19, Model: "claude-sonnet-4-5-thinking"}
	mem := calls.CallMemory(scope)

	// 1. Signed Gemini call with explicit UpstreamID and detached pointer ownership.
	inputTok := &ir.ReplayToken{
		Scheme: ir.SchemeGemini,
		Data:   "gemini-thought-sig-1",
	}
	mem.Remember("call_gem_1", convert.CallBinding{
		UpstreamID: "gem-explicit-1",
		Token:      inputTok,
	})
	inputTok.Scheme = "mutated"
	inputTok.Data = "mutated"

	gotGem, ok := mem.Recall("call_gem_1")
	require.True(t, ok)
	require.Equal(t, "gem-explicit-1", gotGem.UpstreamID)
	require.Equal(t, &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "gemini-thought-sig-1"}, gotGem.Token)
	gotGem.Token.Data = "mutated-after-recall"

	gotGemAgain, ok := mem.Recall("call_gem_1")
	require.True(t, ok)
	require.Equal(t, "gemini-thought-sig-1", gotGemAgain.Token.Data)

	// 2. Stage 4 Antigravity Claude scheme and account-bound ProviderID preservation.
	mem.Remember("call_ag_1", convert.CallBinding{
		UpstreamID: "toolu_ag_explicit",
		Token: &ir.ReplayToken{
			Scheme: ir.SchemeAntigravityClaude,
			Data:   "ag-claude-sig-1",
		},
	})
	mem.Remember("call_acct_1", convert.CallBinding{
		UpstreamID: "call_openai_1",
		Token: &ir.ReplayToken{
			Scheme:     ir.SchemeOpenAI,
			ProviderID: 19,
			Data:       "acct-bound-token",
		},
	})

	gotAG, ok := mem.Recall("call_ag_1")
	require.True(t, ok)
	require.Equal(t, "toolu_ag_explicit", gotAG.UpstreamID)
	require.Equal(t, &ir.ReplayToken{
		Scheme: ir.SchemeAntigravityClaude,
		Data:   "ag-claude-sig-1",
	}, gotAG.Token)

	gotAcct, ok := mem.Recall("call_acct_1")
	require.True(t, ok)
	require.Equal(t, &ir.ReplayToken{
		Scheme:     ir.SchemeOpenAI,
		ProviderID: 19,
		Data:       "acct-bound-token",
	}, gotAcct.Token)

	// 3. Observed unsigned calls (nil Token and empty-Data Token) vs cache miss.
	mem.Remember("call_unsigned_explicit", convert.CallBinding{
		UpstreamID: "gem-unsigned-id",
		Token:      nil,
	})
	mem.Remember("call_unsigned_empty_data", convert.CallBinding{
		UpstreamID: "gem-unsigned-empty",
		Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: ""},
	})

	gotUnsigned, ok := mem.Recall("call_unsigned_explicit")
	require.True(t, ok)
	require.Equal(t, "gem-unsigned-id", gotUnsigned.UpstreamID)
	require.Nil(t, gotUnsigned.Token)

	gotEmptyData, ok := mem.Recall("call_unsigned_empty_data")
	require.True(t, ok)
	require.Equal(t, "gem-unsigned-empty", gotEmptyData.UpstreamID)
	require.Nil(t, gotEmptyData.Token)

	_, ok = mem.Recall("call_never_seen")
	require.False(t, ok)

	// 4. Nameless upstream calls (UpstreamID == "") for both signed and unsigned variants.
	mem.Remember("toolu_synth_signed", convert.CallBinding{
		UpstreamID: "",
		Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "nameless-sig"},
	})
	mem.Remember("toolu_synth_unsigned", convert.CallBinding{
		UpstreamID: "",
		Token:      nil,
	})

	gotNamelessSigned, ok := mem.Recall("toolu_synth_signed")
	require.True(t, ok)
	require.Empty(t, gotNamelessSigned.UpstreamID)
	require.Equal(t, &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "nameless-sig"}, gotNamelessSigned.Token)

	gotNamelessUnsigned, ok := mem.Recall("toolu_synth_unsigned")
	require.True(t, ok)
	require.Empty(t, gotNamelessUnsigned.UpstreamID)
	require.Nil(t, gotNamelessUnsigned.Token)
}

// TestGeminiCallMemoryLateSignatureUpdatePreservesUpstreamID reproduces late
// thoughtSignature revisions arriving after an explicit-ID call already closed.
// Secondary Remember calls must update the token without dropping live UpstreamID
// values, leaking expired IDs, or double-counting retained bytes.
func TestGeminiCallMemoryLateSignatureUpdatePreservesUpstreamID(t *testing.T) {
	now := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	calls := GeminiToolCalls{
		now: func() time.Time { return now },
	}
	scope := GeminiToolScope{APIKeyID: 5, ProviderID: 9, Model: "gemini-2.5-pro"}
	mem := calls.CallMemory(scope)

	// 1. applyLateToolSignatures style: second Remember repeats UpstreamID and adds signature.
	mem.Remember("gemini-paris", convert.CallBinding{
		UpstreamID: "gemini-paris",
		Token:      nil,
	})
	initialBytes := calls.bytes
	require.Positive(t, initialBytes)

	mem.Remember("gemini-paris", convert.CallBinding{
		UpstreamID: "gemini-paris",
		Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-late-paris"},
	})
	gotParis, ok := mem.Recall("gemini-paris")
	require.True(t, ok)
	require.Equal(t, "gemini-paris", gotParis.UpstreamID)
	require.Equal(t, &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-late-paris"}, gotParis.Token)
	require.Len(t, calls.entries, 1)
	require.Equal(t, initialBytes+len("sig-late-paris")+len(ir.SchemeGemini), calls.bytes)

	// 2. Direct late-signature update with empty UpstreamID retains live explicit UpstreamID.
	mem.Remember("toolu_client_2", convert.CallBinding{
		UpstreamID: "gemini-osaka-explicit",
		Token:      nil,
	})
	mem.Remember("toolu_client_2", convert.CallBinding{
		UpstreamID: "",
		Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-late-osaka"},
	})
	gotOsaka, ok := mem.Recall("toolu_client_2")
	require.True(t, ok)
	require.Equal(t, "gemini-osaka-explicit", gotOsaka.UpstreamID)
	require.Equal(t, &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-late-osaka"}, gotOsaka.Token)

	// 3. Oversized late update after inheriting UpstreamID leaves existing live entry intact.
	beforeOversizedBytes := calls.bytes
	nearMaxSig := strings.Repeat("s", geminiToolCallMaxBytes-len(scope.Model)-len("toolu_client_2")-len(ir.SchemeGemini)-4)
	mem.Remember("toolu_client_2", convert.CallBinding{
		UpstreamID: "",
		Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: nearMaxSig},
	})
	gotStillOsaka, ok := mem.Recall("toolu_client_2")
	require.True(t, ok)
	require.Equal(t, "gemini-osaka-explicit", gotStillOsaka.UpstreamID)
	require.Equal(t, "sig-late-osaka", gotStillOsaka.Token.Data)
	require.Equal(t, beforeOversizedBytes, calls.bytes)

	// 4. Expired entry must not leak stale UpstreamID into a new nameless signed call.
	now = now.Add(geminiToolCallTTL)
	mem.Remember("toolu_client_2", convert.CallBinding{
		UpstreamID: "",
		Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-after-expiry"},
	})
	gotFresh, ok := mem.Recall("toolu_client_2")
	require.True(t, ok)
	require.Empty(t, gotFresh.UpstreamID)
	require.Equal(t, "sig-after-expiry", gotFresh.Token.Data)
}

// TestGeminiCallMemoryTTLExpirationAndEvictionLimits verifies the exact one-hour
// TTL boundary, 16,384-entry capacity cap, and 32 MiB byte-budget FIFO eviction.
// Expired and oldest entries must release both map slots and byte counters.
func TestGeminiCallMemoryTTLExpirationAndEvictionLimits(t *testing.T) {
	t.Run("exact one-hour TTL boundary and lazy sweep", func(t *testing.T) {
		now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
		calls := GeminiToolCalls{
			now: func() time.Time { return now },
		}
		scope := GeminiToolScope{APIKeyID: 1, ProviderID: 2, Model: "gemini-2.5-pro"}
		mem := calls.CallMemory(scope)

		mem.Remember("call-ttl-1", convert.CallBinding{
			UpstreamID: "up-ttl-1",
			Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-ttl-1"},
		})
		mem.Remember("call-ttl-2", convert.CallBinding{
			UpstreamID: "up-ttl-2",
			Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-ttl-2"},
		})

		// One nanosecond before one hour: both entries remain live.
		now = now.Add(time.Hour - time.Nanosecond)
		got, ok := mem.Recall("call-ttl-1")
		require.True(t, ok)
		require.Equal(t, "up-ttl-1", got.UpstreamID)

		// Exactly at one hour: Recall misses and removes call-ttl-1; next Remember sweeps call-ttl-2.
		now = now.Add(time.Nanosecond)
		_, ok = mem.Recall("call-ttl-1")
		require.False(t, ok)
		require.Len(t, calls.entries, 1)

		mem.Remember("call-ttl-3", convert.CallBinding{
			UpstreamID: "up-ttl-3",
			Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-ttl-3"},
		})
		require.Len(t, calls.entries, 1)
		_, ok = mem.Recall("call-ttl-2")
		require.False(t, ok)
		got3, ok := mem.Recall("call-ttl-3")
		require.True(t, ok)
		require.Equal(t, "up-ttl-3", got3.UpstreamID)
	})

	t.Run("entry count cap evicts oldest across scopes", func(t *testing.T) {
		var calls GeminiToolCalls
		scopeA := GeminiToolScope{APIKeyID: 1, ProviderID: 1, Model: "gemini-2.5-pro"}
		scopeB := GeminiToolScope{APIKeyID: 2, ProviderID: 1, Model: "gemini-2.5-flash"}
		memA := calls.CallMemory(scopeA)
		memB := calls.CallMemory(scopeB)

		const overflow = 20
		total := geminiToolCallMaxEntries + overflow
		for i := range total {
			id := fmt.Sprintf("call-%d", i)
			target := memA
			if i%2 == 1 {
				target = memB
			}
			target.Remember(id, convert.CallBinding{
				UpstreamID: id,
				Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig"},
			})
		}

		require.Len(t, calls.entries, geminiToolCallMaxEntries)
		require.Equal(t, geminiToolCallMaxEntries, calls.order.Len())

		for i := range overflow {
			id := fmt.Sprintf("call-%d", i)
			target := memA
			if i%2 == 1 {
				target = memB
			}
			_, ok := target.Recall(id)
			require.False(t, ok, "oldest entry %s must be evicted past geminiToolCallMaxEntries", id)
		}
		for i := overflow; i < overflow+10; i++ {
			id := fmt.Sprintf("call-%d", i)
			target := memA
			if i%2 == 1 {
				target = memB
			}
			_, ok := target.Recall(id)
			require.True(t, ok, "retained entry %s must remain live", id)
		}
	})

	t.Run("byte budget cap evicts oldest and ignores single oversized entry", func(t *testing.T) {
		var calls GeminiToolCalls
		scope := GeminiToolScope{APIKeyID: 3, ProviderID: 4, Model: "gemini-2.5-pro"}
		mem := calls.CallMemory(scope)

		chunk := strings.Repeat("b", geminiToolCallMaxBytes/2)
		mem.Remember("first-large", convert.CallBinding{
			UpstreamID: "up-first",
			Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: chunk},
		})
		mem.Remember("second-large", convert.CallBinding{
			UpstreamID: "up-second",
			Token:      &ir.ReplayToken{Scheme: ir.SchemeAntigravityClaude, Data: chunk},
		})

		_, ok := mem.Recall("first-large")
		require.False(t, ok)
		second, ok := mem.Recall("second-large")
		require.True(t, ok)
		require.Equal(t, ir.SchemeAntigravityClaude, second.Token.Scheme)

		// Oversized single entry (> 32 MiB) must be rejected without evicting second-large.
		oversized := strings.Repeat("z", geminiToolCallMaxBytes)
		mem.Remember("too-big", convert.CallBinding{
			UpstreamID: "up-big",
			Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: oversized},
		})
		_, ok = mem.Recall("too-big")
		require.False(t, ok)
		_, ok = mem.Recall("second-large")
		require.True(t, ok)
		require.Equal(t, sumAccountedBytes(&calls), calls.bytes)
	})
}

// TestGeminiCallMemoryConcurrentReadWrite stresses concurrent CallMemory
// Remember/Recall and direct Remember/Lookup operations across overlapping
// and distinct scopes. Running with -race verifies mutex synchronization and
// exact byte accounting.
func TestGeminiCallMemoryConcurrentReadWrite(t *testing.T) {
	var calls GeminiToolCalls
	const workers = 12
	const iterations = 200

	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			scope := GeminiToolScope{
				APIKeyID:   int64((w % 3) + 1),
				ProviderID: int64(((w / 3) % 2) + 1),
				Model:      fmt.Sprintf("gemini-2.5-model-%d", w%2),
			}
			mem := calls.CallMemory(scope)
			for i := range iterations {
				callID := fmt.Sprintf("shared-call-%d", i%25)
				switch i % 4 {
				case 0:
					mem.Remember(callID, convert.CallBinding{
						UpstreamID: fmt.Sprintf("up-%d-%d", w, i),
						Token:      nil,
					})
					mem.Remember(callID, convert.CallBinding{
						UpstreamID: "",
						Token: &ir.ReplayToken{
							Scheme: ir.SchemeGemini,
							Data:   fmt.Sprintf("late-sig-%d-%d", w, i),
						},
					})
				case 1:
					mem.Remember(callID, convert.CallBinding{
						UpstreamID: "",
						Token: &ir.ReplayToken{
							Scheme: ir.SchemeAntigravityClaude,
							Data:   fmt.Sprintf("ag-sig-%d-%d", w, i),
						},
					})
				case 2:
					mem.Remember(callID, convert.CallBinding{
						UpstreamID: fmt.Sprintf("unsigned-%d", i),
						Token:      nil,
					})
				case 3:
					calls.Remember(scope, callID, fmt.Sprintf("legacy-%d", i), "legacy-sig")
					_, _, _ = calls.Lookup(scope, callID)
				}

				if got, ok := mem.Recall(callID); ok && got.Token != nil {
					// Mutate returned copy concurrently to verify no shared pointer aliasing.
					got.Token.Data = "caller-local-mutation"
				}
			}
		})
	}
	wg.Wait()

	require.Equal(t, calls.order.Len(), len(calls.entries))
	require.Equal(t, sumAccountedBytes(&calls), calls.bytes)
}

// TestGeminiCallMemoryExchangeRoundTrip exercises GeminiToolCalls.CallMemory
// end to end through convert.NewExchange on both standard gemini and Stage 4
// antigravity_claude routes. Turn 2 must replay explicit IDs, late signatures,
// nameless omissions, and known-unsigned calls only within the matching scope.
func TestGeminiCallMemoryExchangeRoundTrip(t *testing.T) {
	t.Run("standard gemini stream late signature, nameless call, unsigned parallel, and scope isolation", func(t *testing.T) {
		var calls GeminiToolCalls
		scope := GeminiToolScope{APIKeyID: 42, ProviderID: 77, Model: "gemini-2.5-pro"}
		otherScope := GeminiToolScope{APIKeyID: 42, ProviderID: 78, Model: "gemini-2.5-pro"}

		route := convert.Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolGeminiGenerateContent,
			Dialect: convert.Dialect{
				Name:             "gemini",
				Tools:            convert.ToolSupportFunction,
				ReplayScheme:     ir.SchemeGemini,
				SchemaDialect:    convert.SchemaGemini,
				PlaceholderToken: "skip_thought_signature_validator",
			},
		}

		req1 := []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"tools":[{"name":"get_weather","description":"Weather","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],"messages":[{"role":"user","content":"Check cities"}]}`)
		ex1, err := convert.NewExchange(req1, convert.Options{
			Route:       route,
			ClientModel: "claude-sonnet-4-5",
			Runtime:     deterministicTestRuntime(),
			Calls:       calls.CallMemory(scope),
		})
		require.NoError(t, err)

		// Frame 1 emits: explicit unsigned call (gets late sig on Frame 2), nameless signed call, and two unsigned parallel calls.
		frames := []sse.Frame{
			{Data: []byte(`{"responseId":"gem_rt_1","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"gem-explicit-paris","name":"get_weather","args":{"city":"Paris"}}},{"functionCall":{"name":"get_weather","args":{"city":"London"}},"thoughtSignature":"sig-london-nameless"},{"functionCall":{"id":"gem-unsigned-1","name":"get_weather","args":{"city":"Rome"}}},{"functionCall":{"id":"gem-unsigned-2","name":"get_weather","args":{"city":"Madrid"}}}]}}]}`)},
			{Data: []byte(`{"responseId":"gem_rt_1","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"gem-explicit-paris","name":"get_weather","args":{"city":"Paris"}},"thoughtSignature":"sig-paris-late"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":15,"candidatesTokenCount":12,"totalTokenCount":27}}`)},
		}
		st := ex1.Stream()
		for _, f := range frames {
			_, wErr := st.Write(f)
			require.NoError(t, wErr)
		}
		_, err = st.Close(nil)
		require.NoError(t, err)

		// Turn 2 history splits into two assistant steps: Step A (Paris + London) and Step B (Rome + Madrid unsigned parallel).
		req2 := []byte(`{
			"model":"claude-sonnet-4-5",
			"max_tokens":256,
			"tools":[{"name":"get_weather","description":"Weather","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],
			"messages":[
				{"role":"user","content":"Check cities"},
				{"role":"assistant","content":[
					{"type":"tool_use","id":"gem-explicit-paris","name":"get_weather","input":{"city":"Paris"}},
					{"type":"tool_use","id":"toolu_1","name":"get_weather","input":{"city":"London"}}
				]},
				{"role":"user","content":[
					{"type":"tool_result","tool_use_id":"gem-explicit-paris","content":"Sunny"},
					{"type":"tool_result","tool_use_id":"toolu_1","content":"Cloudy"}
				]},
				{"role":"assistant","content":[
					{"type":"tool_use","id":"gem-unsigned-1","name":"get_weather","input":{"city":"Rome"}},
					{"type":"tool_use","id":"gem-unsigned-2","name":"get_weather","input":{"city":"Madrid"}}
				]},
				{"role":"user","content":[
					{"type":"tool_result","tool_use_id":"gem-unsigned-1","content":"Warm"},
					{"type":"tool_result","tool_use_id":"gem-unsigned-2","content":"Dry"}
				]}
			]
		}`)

		// Same scope: restores gem-explicit-paris + late signature, omits London wire ID + restores signature,
		// and keeps second known-unsigned parallel call free of placeholder signature.
		ex2Same, err := convert.NewExchange(req2, convert.Options{
			Route:       route,
			ClientModel: "claude-sonnet-4-5",
			Runtime:     deterministicTestRuntime(),
			Calls:       calls.CallMemory(scope),
		})
		require.NoError(t, err)
		upSame, err := ex2Same.UpstreamRequest()
		require.NoError(t, err)

		sameDoc := parseGeminiContentsForTest(t, upSame)
		require.Len(t, sameDoc.Contents, 5)
		stepA := sameDoc.Contents[1].Parts
		require.Equal(t, "gem-explicit-paris", stepA[0].FunctionCall.ID)
		require.Equal(t, "sig-paris-late", stepA[0].ThoughtSignature)
		require.Empty(t, stepA[1].FunctionCall.ID)
		require.Equal(t, "sig-london-nameless", stepA[1].ThoughtSignature)
		require.Equal(t, "gem-explicit-paris", sameDoc.Contents[2].Parts[0].FunctionResponse.ID)
		require.Empty(t, sameDoc.Contents[2].Parts[1].FunctionResponse.ID)

		stepB := sameDoc.Contents[3].Parts
		require.Equal(t, "gem-unsigned-1", stepB[0].FunctionCall.ID)
		require.Equal(t, "skip_thought_signature_validator", stepB[0].ThoughtSignature)
		require.Equal(t, "gem-unsigned-2", stepB[1].FunctionCall.ID)
		require.Empty(t, stepB[1].ThoughtSignature)

		// Different provider scope: misses CallMemory, omits explicit Gemini IDs, and injects placeholder on all calls.
		ex2Other, err := convert.NewExchange(req2, convert.Options{
			Route:       route,
			ClientModel: "claude-sonnet-4-5",
			Runtime:     deterministicTestRuntime(),
			Calls:       calls.CallMemory(otherScope),
		})
		require.NoError(t, err)
		upOther, err := ex2Other.UpstreamRequest()
		require.NoError(t, err)

		otherDoc := parseGeminiContentsForTest(t, upOther)
		require.Empty(t, otherDoc.Contents[1].Parts[0].FunctionCall.ID)
		require.Equal(t, "skip_thought_signature_validator", otherDoc.Contents[1].Parts[0].ThoughtSignature)
		require.Equal(t, "skip_thought_signature_validator", otherDoc.Contents[1].Parts[1].ThoughtSignature)
		require.Equal(t, "skip_thought_signature_validator", otherDoc.Contents[3].Parts[0].ThoughtSignature)
		require.Equal(t, "skip_thought_signature_validator", otherDoc.Contents[3].Parts[1].ThoughtSignature)

		// Exchange.Buffer path (SSE folded into buffered JSON) also refreshes late signatures in CallMemory.
		bufScope := GeminiToolScope{APIKeyID: 42, ProviderID: 79, Model: "gemini-2.5-pro"}
		exBuf, err := convert.NewExchange(req1, convert.Options{
			Route:       route,
			ClientModel: "claude-sonnet-4-5",
			Runtime:     deterministicTestRuntime(),
			Calls:       calls.CallMemory(bufScope),
		})
		require.NoError(t, err)
		buf := exBuf.Buffer()
		for _, f := range frames {
			require.NoError(t, buf.Write(f))
		}
		_, err = buf.Close(nil)
		require.NoError(t, err)

		bufRecalled, ok := calls.CallMemory(bufScope).Recall("gem-explicit-paris")
		require.True(t, ok)
		require.Equal(t, "gem-explicit-paris", bufRecalled.UpstreamID)
		require.Equal(t, &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig-paris-late"}, bufRecalled.Token)
	})

	t.Run("antigravity_claude scheme round-trips and stays isolated from gemini scheme", func(t *testing.T) {
		var calls GeminiToolCalls
		agScope := GeminiToolScope{APIKeyID: 42, ProviderID: 88, Model: "claude-sonnet-4-5-thinking"}

		agRoute := convert.Route{
			Client:   protocol.ProtocolOpenAIResponses,
			Upstream: protocol.ProtocolGeminiGenerateContent,
			Dialect: convert.Dialect{
				Name:          "antigravity",
				Tools:         convert.ToolSupportFunction,
				ReplayScheme:  ir.SchemeAntigravityClaude,
				SchemaDialect: convert.SchemaAntigravity,
			},
		}

		req1 := []byte(`{"model":"gpt-5","tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"q":{"type":"string"}}}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Find docs"}]}]}`)
		ex1, err := convert.NewExchange(req1, convert.Options{
			Route:       agRoute,
			ClientModel: "gpt-5",
			Runtime:     deterministicTestRuntime(),
			Calls:       calls.CallMemory(agScope),
		})
		require.NoError(t, err)
		_, err = ex1.Response([]byte(`{"responseId":"ag_1","modelVersion":"claude-sonnet-4-5-thinking","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"toolu_ag_99","name":"lookup","args":{"q":"ir"}},"thoughtSignature":"sig-ag-claude-99"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":6,"totalTokenCount":16}}`))
		require.NoError(t, err)

		req2 := []byte(`{"model":"gpt-5","tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"q":{"type":"string"}}}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Find docs"}]},{"type":"function_call","call_id":"toolu_ag_99","name":"lookup","arguments":"{\"q\":\"ir\"}"},{"type":"function_call_output","call_id":"toolu_ag_99","output":"Found"}]}`)
		ex2, err := convert.NewExchange(req2, convert.Options{
			Route:       agRoute,
			ClientModel: "gpt-5",
			Runtime:     deterministicTestRuntime(),
			Calls:       calls.CallMemory(agScope),
		})
		require.NoError(t, err)
		upAG, err := ex2.UpstreamRequest()
		require.NoError(t, err)

		agDoc := parseGeminiContentsForTest(t, upAG)
		require.Equal(t, "toolu_ag_99", agDoc.Contents[1].Parts[0].FunctionCall.ID)
		require.Equal(t, "sig-ag-claude-99", agDoc.Contents[1].Parts[0].ThoughtSignature)

		// Replaying the same memory entry against a gemini scheme drops the antigravity_claude token cleanly.
		geminiRoute := agRoute
		geminiRoute.Dialect.ReplayScheme = ir.SchemeGemini
		geminiRoute.Dialect.SchemaDialect = convert.SchemaGemini
		exMismatch, err := convert.NewExchange(req2, convert.Options{
			Route:       geminiRoute,
			ClientModel: "gpt-5",
			Runtime:     deterministicTestRuntime(),
			Calls:       calls.CallMemory(agScope),
		})
		require.NoError(t, err)
		upMismatch, err := exMismatch.UpstreamRequest()
		require.NoError(t, err)

		mismatchDoc := parseGeminiContentsForTest(t, upMismatch)
		require.Empty(t, mismatchDoc.Contents[1].Parts[0].ThoughtSignature)
		for _, loss := range exMismatch.Losses() {
			require.NotEqual(t, ir.LossReplayTokenDropped, loss.Kind)
		}
	})
}

// sumAccountedBytes walks calls.order and sums each geminiToolEntry.size to
// verify that calls.bytes matches live FIFO elements. Callers invoke it after
// synchronous or completed concurrent test mutations.
func sumAccountedBytes(calls *GeminiToolCalls) int {
	total := 0
	for el := calls.order.Front(); el != nil; el = el.Next() {
		entry, _ := el.Value.(geminiToolEntry)
		total += entry.size
	}
	return total
}

// deterministicTestRuntime constructs a convert.Runtime with fixed timestamps
// and sequential IDs for repeatable Exchange round-trip tests. Each invocation
// starts an independent counter at zero so turn assertions stay deterministic.
func deterministicTestRuntime() convert.Runtime {
	seq := 0
	return convert.Runtime{
		Now: func() time.Time {
			return time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
		},
		NewID: func(prefix string) string {
			seq++
			return fmt.Sprintf("%s%d", prefix, seq)
		},
	}
}

// parseGeminiContentsForTest unmarshals raw Gemini request JSON into
// geminiWireContentsDoc for turn-level wire assertions. Malformed JSON payloads
// fail t immediately so callers can inspect decoded turns directly.
func parseGeminiContentsForTest(t *testing.T, raw []byte) geminiWireContentsDoc {
	t.Helper()
	var doc geminiWireContentsDoc
	require.NoError(t, json.Unmarshal(raw, &doc))
	return doc
}
