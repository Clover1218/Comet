package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Network NetworkConfig `mapstructure:"network"`
	Storage StorageConfig `mapstructure:"storage"`
	Log     LogConfig     `mapstructure:"log"`
	Node    NodeConfig    `mapstructure:"node"`
}
type NodeConfig struct {
	ID   string `mapstructure:"id"`
	Name string `mapstructure:"name"`
}

type NetworkConfig struct {
	ListenAddr        string `mapstructure:"server"`
	Port              int    `mapstructure:"port"`
	DiscoveryEnabled  bool   `mapstructure:"discovery_enabled"`
	DiscoveryInterval int    `mapstructure:"discovery_interval"`
	InterfaceName     string `mapstructure:"interface_name"`
}
type StorageConfig struct {
	DataDir        string `mapstructure:"data_dir"`
	DownloadsDir   string `mapstructure:"downloads_dir"`
	CheckpointsDir string `mapstructure:"checkpoints_dir"`
	QueueDir       string `mapstructure:"queue_dir"`
	PeersCacheDir  string `mapstructure:"peers_cache_dir"`
}
type LogConfig struct {
	Path string `mapstructure:"path"`
}

var GlobalConfig *Config

// InitConfig 初始化 Viper 并读取配置
func InitConfig(configPath, configType string) error {
	v := viper.GetViper()
	v.SetConfigFile(configPath)                        // 直接指定配置文件全路径
	v.SetConfigType(configType)                        // 如 "yaml", "json", "toml"
	v.AutomaticEnv()                                   // 允许环境变量覆盖
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // 如 server.port → SERVER_PORT

	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("读取配置文件失败: %w", err)
	}

	if err := v.Unmarshal(&GlobalConfig); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}

	return nil
}
