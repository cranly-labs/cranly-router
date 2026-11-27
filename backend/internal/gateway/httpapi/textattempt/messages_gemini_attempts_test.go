package textattempt

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/apikey"
	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewaycapture "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	textflow "github.com/TokenFlux/TokenRouter/internal/gateway/text"
	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
)

// messages_gemini_attempts_test.go verifies admission timing and schedule
// feedback gating for Messages requests converted to Gemini execution. Its
// shared HTTP and scheduler scenarios preserve the distinction between
// successful acquisition, retry waiting and refusal, and confirm that local
// model-attribute rejections skip scheduler penalties.

// TestGeminiMessagesAcquireAdmissionTiming exercises selected capacity, waiting
// and failure through the Gemini Messages adapter. Retry acquisition must retain
// the initial admission endpoint without extending preprocessing.
func TestGeminiMessagesAcquireAdmissionTiming(t *testing.T) {
	testProviderAdmissionTiming(t, "/v1/messages", func(base messageAttemptBridge) admissionTimingAttempt {
		bridge := &geminiMessageAttemptBridge{messageAttemptBridge: base}
		return admissionTimingAttempt{acquire: bridge.Acquire, release: &bridge.providerReleaseFunc}
	})
}

// TestGeminiMessagesForwardSkipsScheduleReportOnModelAttributeError dispatches
// a forced-Gemini Messages attempt returning MODEL_CAPABILITY_UNSUPPORTED.
// ReportSchedule must stay uninvoked so client capability rejections do not
// penalize healthy Gemini accounts.
func TestGeminiMessagesForwardSkipsScheduleReportOnModelAttributeError(t *testing.T) {
	reported := 0
	runtime := New(Bindings{
		Forward: ForwardPorts{
			ForwardGemini: func(context.Context, *gin.Context, *gatewaycapture.ExecutionProvider, []byte) (*forwardcore.MessagesResult, error) {
				return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", "ultra effort unsupported")
			},
		},
		Selection: SelectionPorts{
			ReportSchedule: func(*gatewaycapture.SelectionResult, int64, bool, *forwardcore.MessagesResult) {
				reported++
			},
		},
	})

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/gemini/v1/messages", nil)

	execProv := &gatewaycapture.ExecutionProvider{
		Record: provider.Record{ID: 77, Platform: capability.PlatformGemini},
	}
	bridge := &geminiMessageAttemptBridge{
		messageAttemptBridge: messageAttemptBridge{
			fixed:     runtime.dependencies,
			c:         c,
			apiKey:    &apikey.APIKey{ID: 5},
			provider:  execProv,
			selection: &gatewaycapture.SelectionResult{Provider: execProv},
		},
		forwardModel: "gemini-2.5-pro",
		forwardBody:  []byte(`{"model":"gemini-2.5-pro"}`),
	}

	out := bridge.Forward(textflow.AttemptState{})
	require.Error(t, out.Err)
	require.Zero(t, reported)
}
