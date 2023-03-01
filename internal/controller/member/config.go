package member

type Config struct {
	LeaseDuration int64    `yaml:"lease_duration"`
	Name          string   `yaml:"name"`
	EtcdEndpoints []string `yaml:"etcd"`
}

const (
	ResourceLockKeyPrefixInKVStore = "/vanus/internal/resource/resourcelock"
	LeaderInfoKeyPrefixInKVStore   = "/vanus/internal/resource/leaderinfo"
)
