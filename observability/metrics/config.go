package metrics

type Config struct {
	Enable bool `yaml:"enable"`
	Port   int  `yaml:"port"`
}

func (c Config) GetPort() int {
	if c.Port == 0 {
		return 2112
	}
	return c.Port
}
