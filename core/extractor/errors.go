package extractor

type InvalidConfigError struct {
}

func (err InvalidConfigError) Error() string {
	return "invalid extractor config"
}
