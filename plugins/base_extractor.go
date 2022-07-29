package plugins

type BaseExtractor struct {
	BasePlugin
}

func NewBaseExtractor(info Info, configRef interface{}) BaseExtractor {
	return BaseExtractor{
		BasePlugin: NewBasePlugin(info, configRef),
	}
}

// Validate checks if the given options is valid for the plugin.
func (p *BaseExtractor) Validate(config Config) error {
	if config.URNScope == "" {
		return ErrEmptyURNScope
	}

	return buildConfig(config.RawConfig, p.configRef)
}
