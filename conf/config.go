package config

type Config struct {
	Mysql Mysql `yaml:"Mysql"`
	Slave Slave `yaml:"Slave"`
}

type Mysql struct {
	Host string `yaml:"Host"`
	Port uint16 `yaml:"Port"`
	User string `yaml:"User"`
	Pwd  string `yaml:"Pwd"`
}

type Slave struct {
	ServerID uint32 `yaml:"ServerID"`
	Flavor   string `yaml:"Flavor"`
	Binlog   string `yaml:"Binlog"`
	Pos      uint32 `yaml:"Pos"`
	LoadNew  bool   `yaml:"LoadNew"`
	LoadPath string `yaml:"LoadPath"`
}
