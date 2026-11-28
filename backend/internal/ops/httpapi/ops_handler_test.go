package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/ops"
)

// TestParseOpsViewParam supplies excluded, all, and unknown selector text
// through Gin requests, then a nil context. It checks retained named views,
// unknown-to-errors selection, and the distinct empty result for nil input.
func TestParseOpsViewParam(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/?view=excluded", nil)
	require.Equal(t, opsListViewExcluded, parseOpsViewParam(c))

	c2, _ := gin.CreateTestContext(w)
	c2.Request = httptest.NewRequest(http.MethodGet, "/?view=all", nil)
	require.Equal(t, opsListViewAll, parseOpsViewParam(c2))

	c3, _ := gin.CreateTestContext(w)
	c3.Request = httptest.NewRequest(http.MethodGet, "/?view=unknown", nil)
	require.Equal(t, opsListViewErrors, parseOpsViewParam(c3))

	require.Equal(t, "", parseOpsViewParam(nil))
}

// TestParseOpsDuration supplies the recognized one-hour preset and an
// unsupported string to the shared duration parser. It checks the hour
// value/success flag and rejection flag for unknown text.
func TestParseOpsDuration(t *testing.T) {
	dur, ok := parseOpsDuration("1h")
	require.True(t, ok)
	require.Equal(t, time.Hour, dur)

	_, ok = parseOpsDuration("invalid")
	require.False(t, ok)
}

// TestParseOpsTimeRange supplies two valid RFC3339 endpoints one hour apart and
// then malformed start text through Gin requests. It checks ordered successful
// endpoints and an error for the malformed timestamp.
func TestParseOpsTimeRange(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	now := time.Now().UTC()
	startStr := now.Add(-time.Hour).Format(time.RFC3339)
	endStr := now.Format(time.RFC3339)
	c.Request = httptest.NewRequest(http.MethodGet, "/?start_time="+startStr+"&end_time="+endStr, nil)
	start, end, err := parseOpsTimeRange(c, "1h")
	require.NoError(t, err)
	require.True(t, start.Before(end))

	c2, _ := gin.CreateTestContext(w)
	c2.Request = httptest.NewRequest(http.MethodGet, "/?start_time=bad", nil)
	_, _, err = parseOpsTimeRange(c2, "1h")
	require.Error(t, err)
}

// TestApplyOpsErrorStatusParams supplies valid client and upstream status lists,
// Other flags, and malformed status values through Gin requests. It checks
// parsed integer slices, truthy Other flags, and HTTP 400 responses on negative
// or non-numeric query values.
func TestApplyOpsErrorStatusParams(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(
		http.MethodGet,
		"/?status_codes=200,502&status_codes_other=true&upstream_status_codes=0,429&upstream_status_codes_other=1",
		nil,
	)
	filter := &ops.OpsErrorLogFilter{}
	require.True(t, applyOpsErrorStatusParams(c, filter))
	require.Equal(t, []int{200, 502}, filter.StatusCodes)
	require.True(t, filter.StatusCodesOther)
	require.Equal(t, []int{0, 429}, filter.UpstreamStatusCodes)
	require.True(t, filter.UpstreamStatusCodesOther)

	wBadClient := httptest.NewRecorder()
	cBadClient, _ := gin.CreateTestContext(wBadClient)
	cBadClient.Request = httptest.NewRequest(http.MethodGet, "/?status_codes=-1", nil)
	require.False(t, applyOpsErrorStatusParams(cBadClient, &ops.OpsErrorLogFilter{}))
	require.Equal(t, http.StatusBadRequest, wBadClient.Code)

	wBadUpstream := httptest.NewRecorder()
	cBadUpstream, _ := gin.CreateTestContext(wBadUpstream)
	cBadUpstream.Request = httptest.NewRequest(http.MethodGet, "/?upstream_status_codes=bad", nil)
	require.False(t, applyOpsErrorStatusParams(cBadUpstream, &ops.OpsErrorLogFilter{}))
	require.Equal(t, http.StatusBadRequest, wBadUpstream.Code)

	wOverflowClient := httptest.NewRecorder()
	cOverflowClient, _ := gin.CreateTestContext(wOverflowClient)
	cOverflowClient.Request = httptest.NewRequest(http.MethodGet, "/?status_codes=2147483648", nil)
	require.False(t, applyOpsErrorStatusParams(cOverflowClient, &ops.OpsErrorLogFilter{}))
	require.Equal(t, http.StatusBadRequest, wOverflowClient.Code)

	wOverflowUpstream := httptest.NewRecorder()
	cOverflowUpstream, _ := gin.CreateTestContext(wOverflowUpstream)
	cOverflowUpstream.Request = httptest.NewRequest(http.MethodGet, "/?upstream_status_codes=2147483648", nil)
	require.False(t, applyOpsErrorStatusParams(cOverflowUpstream, &ops.OpsErrorLogFilter{}))
	require.Equal(t, http.StatusBadRequest, wOverflowUpstream.Code)
}
