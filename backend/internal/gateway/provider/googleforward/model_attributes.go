package googleforward

import (
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/routing"
)

// model_attributes.go applies resolved image-modality and sampling parameter
// policy to native Gemini GenerateContent request bodies. Native Gemini input
// can spell generation configuration and nucleus sampling in camelCase or
// snake_case; attribute application checks both spellings after evaluating
// request reasoning state, validates declared image output when IMAGE modality
// is requested, and removes only explicitly unsupported sampling fields.

// applyGeminiModelParameters enforces declared image-modality permissions and
// removes explicitly unsupported sampling controls on native Gemini requests.
// Unknown and supported sampling flags retain supplied fields, whereas known
// false values delete present temperature, topP, and top_p fields across both
// generationConfig and generation_config spellings.
func applyGeminiModelParameters(body []byte, resolution modelrules.Resolution) ([]byte, error) {
	result := resolution.ForRequest(gatewayprovider.ModelRequestState(body))
	var err error
	// Check both accepted config and topP spellings so a client alternate
	// spelling cannot retain a sampling control that the final selected
	// route marks unsupported.
	for _, configName := range []string{"generationConfig", "generation_config"} {
		config := gjson.GetBytes(body, configName)
		for _, field := range []string{"responseModalities", "response_modalities"} {
			for _, modality := range config.Get(field).Array() {
				if strings.EqualFold(modality.String(), "image") {
					if err := routing.RequireImageOutput(result.Attributes); err != nil {
						return nil, err
					}
				}
			}
		}
		for key, support := range map[string]*bool{"temperature": result.Attributes.Temperature, "topP": result.Attributes.TopP, "top_p": result.Attributes.TopP} {
			if support == nil || *support || !config.Get(key).Exists() {
				continue
			}
			body, err = sjson.DeleteBytes(body, configName+"."+key)
			if err != nil {
				return nil, err
			}
		}
	}
	return body, nil
}
