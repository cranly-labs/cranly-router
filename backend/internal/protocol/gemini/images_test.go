package gemini

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNativeIsGeminiInlineImageMIMEType_RecognizesSupportedRasterTypes tests
// lowercase raster MIME strings alongside uppercase, vector, document, and
// blank inputs. Only exact lowercase GIF, JPEG, PNG, and WebP labels must
// report true.
func TestNativeIsGeminiInlineImageMIMEType_RecognizesSupportedRasterTypes(t *testing.T) {
	for _, allowed := range []string{"image/gif", "image/jpeg", "image/png", "image/webp"} {
		require.True(t, NativeIsGeminiInlineImageMIMEType(allowed), allowed)
	}
	for _, rejected := range []string{"", "IMAGE/PNG", "image/svg+xml", "application/pdf", "text/plain"} {
		require.False(t, NativeIsGeminiInlineImageMIMEType(rejected), rejected)
	}
}
