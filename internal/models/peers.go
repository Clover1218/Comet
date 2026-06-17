package models

import (
	"net"
	"time"
)

type Peer struct {
	ID       string    `json:"id"`        // 唯一标识：hostname 或随机生成
	Addr     string    `json:"addr"`      // IPv6地址（含端口），如 "[2001:db8::2]:9000"
	IP       net.IP    `json:"ip"`        // 解析出的 IP
	Port     int       `json:"port"`      // 端口
	Hostname string    `json:"hostname"`  // 友好名称
	Online   bool      `json:"online"`    // 是否在线（由心跳/发现维护）
	LastSeen time.Time `json:"last_seen"` // 最后见到时间
}

// PeerKey 生成用于 Map 的 Key（避免重复）
func (p *Peer) Key() string {
	return p.ID // 或者 p.Addr，但 ID 更稳定
}
