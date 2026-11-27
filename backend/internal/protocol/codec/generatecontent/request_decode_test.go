package generatecontent

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestDecodeRequestRichShapesAndStrictRejections exercises Gemini request
// decoding across systemInstruction extras, nameless parallel functionCalls,
// plain-text thoughtSignature preservation, executableCode OpaqueItems, VALIDATED
// toolConfig, and thinkingLevel controls. It also checks that malformed bounds,
// conflicting thinking fields, and duplicate JSON keys fail decoding cleanly.
func TestDecodeRequestRichShapesAndStrictRejections(t *testing.T) {
	body := []byte(`{
		"systemInstruction": {
			"role": "user",
			"parts": [{"text": "System prompt"}]
		},
		"tools": [
			{
				"functionDeclarations": [
					{
						"name": "exec_sql",
						"description": "Run read-only query",
						"parametersJsonSchema": {"type": "object", "properties": {"sql": {"type": "string"}}, "additionalProperties": false}
					}
				]
			},
			{"codeExecution": {}},
			{"urlContext": {}}
		],
		"toolConfig": {
			"includeServerSideToolInvocations": true,
			"functionCallingConfig": {
				"mode": "VALIDATED",
				"allowedFunctionNames": ["exec_sql"]
			}
		},
		"contents": [
			{
				"role": "user",
				"parts": [
					{"fileData": {"mimeType": "application/pdf", "fileUri": "files/abc123"}},
					{"text": "Inspect schema", "thoughtSignature": "txt_sig_u1"}
				]
			},
			{
				"role": "model",
				"parts": [
					{"executableCode": {"language": "PYTHON", "code": "print(1)"}},
					{"codeExecutionResult": {"outcome": "OUTCOME_OK", "output": "1\n"}},
					{"functionCall": {"name": "exec_sql", "args": {"sql": "SELECT 1"}}},
					{"functionCall": {"name": "exec_sql", "args": {"sql": "SELECT 2"}}}
				]
			},
			{
				"role": "user",
				"parts": [
					{"functionResponse": {"name": "exec_sql", "response": {"rows": [1]}}},
					{"functionResponse": {"name": "exec_sql", "response": {"rows": [2]}}}
				]
			}
		],
		"generationConfig": {
			"maxOutputTokens": 2048,
			"temperature": 0.2,
			"topP": 0.9,
			"topK": 40,
			"candidateCount": 1,
			"seed": 42,
			"thinkingConfig": {
				"thinkingLevel": "high",
				"includeThoughts": false
			}
		},
		"safetySettings": [{"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"}],
		"cachedContent": "cachedContents/ctx-99",
		"labels": {"env": "prod", "seq": 9007199254740993}
	}`)

	dec1, err := DecodeRequest(body)
	require.NoError(t, err)
	require.NoError(t, ir.Validate(dec1))

	require.Len(t, dec1.System, 1)
	require.Len(t, dec1.Tools, 3)
	require.Equal(t, ir.ToolFunction, dec1.Tools[0].Kind)
	require.Equal(t, ir.ToolHosted, dec1.Tools[1].Kind)
	require.Equal(t, "codeExecution", dec1.Tools[1].Hosted.Type)
	require.Equal(t, "urlContext", dec1.Tools[2].Hosted.Type)

	require.NotNil(t, dec1.ToolChoice)
	require.Equal(t, ir.ToolChoiceAuto, dec1.ToolChoice.Mode)
	require.Equal(t, []ir.ToolRef{{Kind: ir.ToolFunction, Name: "exec_sql"}}, dec1.ToolChoice.AllowedTools)

	require.NotNil(t, dec1.Reasoning)
	require.Equal(t, "high", dec1.Reasoning.Effort)

	// Verify file_id classification, executableCode opaque preservation, and distinct nameless CallIDs.
	require.Equal(t, ir.MediaSourceFileID, dec1.Messages[0].Parts[0].Media.Source)
	require.False(t, dec1.Messages[0].Parts[1].Extra.IsZero())
	require.Equal(t, ir.PartOpaque, dec1.Messages[1].Parts[0].Kind)
	require.Equal(t, "executableCode", dec1.Messages[1].Parts[0].Opaque.Type)
	callID1 := dec1.Messages[1].Parts[2].Call.CallID
	callID2 := dec1.Messages[1].Parts[3].Call.CallID
	require.NotEqual(t, callID1, callID2)
	require.Equal(t, callID1, dec1.Messages[2].Parts[0].Result.CallID)
	require.Equal(t, callID2, dec1.Messages[2].Parts[1].Result.CallID)

	// Verify full Law Q2 round-trip fixed point on rich shape.
	enc1, losses1, err := EncodeRequest(dec1, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, losses1)
	require.Empty(t, testkit.ValidateGeminiRequest(enc1, testkit.RequestValidationOptions{}))

	dec2, err := DecodeRequest(enc1)
	require.NoError(t, err)
	require.Equal(t, dec1, dec2)

	enc2, losses2, err := EncodeRequest(dec2, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, losses2)
	require.True(t, bytes.Equal(enc1, enc2))

	// Invalid request shapes must fail DecodeRequest cleanly.
	for _, bad := range [][]byte{
		[]byte(`{}`),
		[]byte(`{"contents":[]}`),
		[]byte(`{"contents":[{"role":"system","parts":[{"text":"bad"}]}]}`),
		[]byte(`{"systemInstruction":{"parts":[{"text":"bad","thought":true}]},"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
		[]byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"maxOutputTokens":0}}`),
		[]byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"topK":-1}}`),
		[]byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"candidateCount":0}}`),
		[]byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"thinkingConfig":{"thinkingBudget":-2}}}`),
		[]byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"thinkingConfig":{"thinkingBudget":512,"thinkingLevel":"low"}}}`),
		[]byte(`{"contents":[{"role":"user","parts":[{"text":"hi"},{"text":"dup","text":"x"}]}]}`),
	} {
		_, decErr := DecodeRequest(bad)
		require.Error(t, decErr, "expected DecodeRequest error on %s", string(bad))
	}
}

// FuzzRequestDecode feeds arbitrary and corpus-seeded JSON byte slices into
// DecodeRequest without panicking. Accepted requests must pass ir.Validate,
// encode deterministically into valid Gemini JSON, reconcile under VerifyEssence,
// and reach a byte-for-byte fixed point when encoding incurs zero Loss.
func FuzzRequestDecode(f *testing.F) {
	wireReqs, err := testkit.WireFixtures(protocol.ProtocolGeminiGenerateContent, testkit.FixtureRequest)
	require.NoError(f, err)
	for _, fix := range wireReqs {
		f.Add(fix.Data)
	}
	corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolGeminiGenerateContent)
	require.NoError(f, err)
	for _, fix := range corpusReqs {
		f.Add(fix.Data)
	}
	f.Add([]byte(`{"systemInstruction":{"parts":[{"text":"sys"}],"role":"user"},"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{},"toolConfig":{}}`))
	f.Add([]byte(`{"tools":[{"functionDeclarations":[]}],"contents":[{"role":"user","parts":[{"text":"hello"}]}]}`))
	f.Add([]byte(`{"tools":[{"google_search":{}}],"contents":[{"role":"user","parts":[{"text":"hi"}]},{"role":"user","parts":[{"text":"again"}]}]}`))
	f.Add([]byte(`{"contents":[{"role":"model","parts":[{"functionCall":{"name":"fn","args":{}}}]},{"role":"user","parts":[{"functionResponse":{"name":"fn","response":{"result":"ok"}}}]}]}`))
	f.Add([]byte(`{"contents":[{"role":"user","parts":[{"0":""}]},{"role":"user","parts":[{"0":""}],"":""}]}`))
	f.Add([]byte(`{"contents":[{"role":"model","parts":[{"functionCall":{"id":"c1","name":"0","args":{}}}]},{"role":"user","parts":[{"functionResponse":{"id":"c1","name":"0","response":{"0":0}}}]}]}`))
	f.Add([]byte(`{"toolConfig":{"functionCallingConfig":{"mode":"ANY","allowedFunctionNames":["0"]}},"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`))
	f.Add([]byte(`{"contents":[{"role":"user","parts":[{"":1}]}]}`))
	f.Add([]byte(`{"contents":[{"role":"model","parts":[{"thought":true}]},{"parts":[{"0":{}}]}]}`))
	f.Add([]byte(`{"contents":[{"role":"model","parts":[{"functionCall":{"name":"0","args":{"0":"0"}}}]},{"role":"user","parts":[{"functionResponse":{"name":"0","response":{}}}]}]}`))
	f.Add([]byte("{\"contents\":[{\"role\":\"user\",\"parts\":[{\"text\":\"\xff\xfe\"}]}]}"))

	f.Fuzz(func(t *testing.T, data []byte) {
		dec1, err := DecodeRequest(data)
		if err != nil {
			return
		}
		if err := ir.Validate(dec1); err != nil {
			t.Fatalf("DecodeRequest succeeded but ir.Validate failed: %v", err)
		}

		enc1, losses1, err := EncodeRequest(dec1, ir.Dialect{})
		if err != nil {
			return
		}
		enc2, _, err := EncodeRequest(dec1, ir.Dialect{})
		if err != nil {
			t.Fatalf("second EncodeRequest failed: %v", err)
		}
		if !bytesEqual(enc1, enc2) {
			t.Fatalf("non-deterministic EncodeRequest output")
		}

		if v := testkit.ValidateGeminiRequest(enc1, testkit.RequestValidationOptions{}); len(v) > 0 {
			t.Fatalf("ValidateGeminiRequest failed on encoded output: %v\nencoded=%s", v, string(enc1))
		}

		decAfter, err := DecodeRequest(enc1)
		if err != nil {
			t.Fatalf("DecodeRequest(enc1) failed: %v\nencoded=%s", err, string(enc1))
		}
		if err := ir.Validate(decAfter); err != nil {
			t.Fatalf("ir.Validate(DecodeRequest(enc1)) failed: %v", err)
		}

		diffs, vErr := testkit.VerifyEssence(ir.Essence(dec1), ir.Essence(decAfter), losses1)
		if vErr != nil {
			t.Fatalf("VerifyEssence error: %v (losses=%v)", vErr, losses1)
		}
		if len(diffs) > 0 {
			t.Fatalf("VerifyEssence diffs:\n%s\nlosses=%v\nenc1=%s", testkit.FormatJSONDiff(diffs), losses1, string(enc1))
		}

		if len(losses1) == 0 {
			encAfter, lossesAfter, err := EncodeRequest(decAfter, ir.Dialect{})
			if err != nil {
				t.Fatalf("EncodeRequest(decAfter) failed: %v", err)
			}
			if len(lossesAfter) != 0 || !bytesEqual(enc1, encAfter) {
				t.Fatalf("Enc(Dec(enc1)) differed from enc1:\nenc1=%s\nencAfter=%s", string(enc1), string(encAfter))
			}
			if len(testkit.ValidateGeminiRequest(data, testkit.RequestValidationOptions{})) == 0 && len(dec1.Messages) == len(decAfter.Messages) {
				if !reflect.DeepEqual(dec1, decAfter) {
					t.Fatalf("Law Q2 violated on canonical input:\ndec1=%+v\ndecAfter=%+v", dec1, decAfter)
				}
			}
		}
	})
}
