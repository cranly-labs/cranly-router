package gemini

// images.go defines Gemini creative image generation request types and validates
// inline image MIME labels observed in Gemini response parts.
// Creative image requests carry prompt and reference-image parts alongside
// generateContent configuration, keeping imageSize and aspectRatio nested in
// imageConfig and reasoning controls nested in thinkingConfig, while inline
// image observation accepts the four raster MIME types emitted in inlineData.

// ImageGenerationConfig contains creative generateContent output and image-generation settings.
type ImageGenerationConfig struct {
	ResponseModalities []string             `json:"responseModalities"`       // requested output modalities
	ImageConfig        *ImageConfig         `json:"imageConfig,omitempty"`    // optional image settings
	ThinkingConfig     *ImageThinkingConfig `json:"thinkingConfig,omitempty"` // optional reasoning settings
}

// ImageConfig contains image settings; imageSize belongs inside this object.
type ImageConfig struct {
	ImageSize   string `json:"imageSize,omitempty"`   // requested size label
	AspectRatio string `json:"aspectRatio,omitempty"` // requested aspect ratio
}

// ImageThinkingConfig controls image-model reasoning effort and whether thoughts are returned.
type ImageThinkingConfig struct {
	ThinkingLevel   string `json:"thinkingLevel"`   // requested reasoning level
	IncludeThoughts bool   `json:"includeThoughts"` // reasoning output selection
}

// ImageGenerateRequest is the creative workspace generateContent request body.
type ImageGenerateRequest struct {
	Contents         []BatchContent        `json:"contents"`         // prompt and image parts
	GenerationConfig ImageGenerationConfig `json:"generationConfig"` // creative generation settings
}

// NativeIsGeminiInlineImageMIMEType reports whether mimeType exactly matches
// one of the four lowercase raster image MIME types ("image/gif", "image/jpeg",
// "image/png", or "image/webp") recognized for Gemini inline image parts.
// Uppercase variants, media-type parameters, and other MIME types return false.
func NativeIsGeminiInlineImageMIMEType(mimeType string) bool {
	switch mimeType {
	case "image/gif", "image/jpeg", "image/png", "image/webp":
		return true
	default:
		return false
	}
}
