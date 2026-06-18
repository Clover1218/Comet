package peer

import (
	"comet/internal/logger"
	"comet/internal/models"
	"comet/internal/storage"
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/eball/zeroconf"
)

// EventType 事件类型
type EventType string

const (
	EventPeerJoined EventType = "joined" // 新节点上线
	EventPeerLeft   EventType = "left"   // 节点离开
	EventPeerUpdate EventType = "update" // 节点信息更新（如 IP 变化）
)

// Event 发现事件
type Event struct {
	Type EventType
	Peer *models.Peer
}

// Manager 节点发现与管理接口
type Manager interface {
	// Start 启动发现服务（注册自身 + 开始浏览）
	Start(ctx context.Context) error

	// Stop 停止发现服务（优雅退出，发送 goodbye）
	Stop() error

	// Register 注册本节点服务（告诉别人“我在这里”）
	Register(port int, meta map[string]string) error

	// Unregister 注销服务
	Unregister() error

	// ListPeers 获取所有在线节点列表
	ListPeers() ([]*models.Peer, error)

	// GetPeer 根据 ID 获取节点
	GetPeer(id string) (*models.Peer, error)

	// GetPeerByAddr 根据地址获取节点
	GetPeerByAddr(addr string) (*models.Peer, error)

	// Events 返回事件通道（上层订阅节点变化）
	Events() <-chan Event

	AddManualPeer(name string, addr string) error
	RemovePeer(id string) error
}

type peerManager struct {
	logger   *logger.Logger
	store    storage.Store      // 存储层（持久化缓存）
	service  *zeroconf.Server   // mDNS 服务端（注册用）
	resolver *zeroconf.Resolver // mDNS 解析器（浏览用）

	peers map[string]*models.Peer // 内存中的在线节点表
	mu    sync.RWMutex            // 保护 peers map

	eventCh chan Event // 事件通道
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	selfID     string // 自己的 ID（用于过滤自己）
	port       int    // 监听端口
	registered bool   // 是否已注册
}

// NewPeerManager 创建节点管理器
func NewPeerManager(log *logger.Logger, store storage.Store, selfID string, port int) Manager {
	ctx, cancel := context.WithCancel(context.Background())
	return &peerManager{
		logger:  log,
		store:   store,
		selfID:  selfID,
		port:    port,
		peers:   make(map[string]*models.Peer),
		eventCh: make(chan Event, 100), // 缓冲100个事件
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start 启动发现
func (m *peerManager) Start(ctx context.Context) error {
	m.logger.Infof("[Discovery] 启动节点发现, 自身ID: %s, 端口: %d", m.selfID, m.port)

	// 1. 从存储加载缓存的节点列表（离线节点也加载，便于快速显示）
	if err := m.loadCachedPeers(); err != nil {
		m.logger.Warnf("[Discovery] 加载缓存节点失败: %v", err)
	}

	// 2. 启动 mDNS 浏览（发现别人）
	if err := m.startBrowsing(); err != nil {
		return fmt.Errorf("启动mDNS浏览失败: %w", err)
	}

	// 3. 注册自身服务（让别人发现自己）
	if err := m.Register(m.port, nil); err != nil {
		return fmt.Errorf("注册mDNS服务失败: %w", err)
	}

	// 4. 启动后台清理协程（超时节点自动离线）
	m.wg.Add(1)
	go m.cleanupLoop()

	return nil
}

// Stop 停止发现
func (m *peerManager) Stop() error {
	m.logger.Info("[Discovery] 停止节点发现")

	m.cancel() // 取消所有协程

	// 注销 mDNS 服务（发送 goodbye）
	if err := m.Unregister(); err != nil {
		m.logger.Warnf("[Discovery] 注销失败: %v", err)
	}

	m.wg.Wait()
	close(m.eventCh)

	// 保存在线节点到缓存
	m.saveCachedPeers()

	return nil
}

// Register 注册服务
func (m *peerManager) Register(port int, meta map[string]string) error {
	if m.registered {
		return nil
	}

	// 构建 TXT 记录（可放版本、能力等元数据）
	txtRecords := []string{
		"id=" + m.selfID,
		"ver=1.0.0",
	}
	for k, v := range meta {
		txtRecords = append(txtRecords, k+"="+v)
	}

	server, err := zeroconf.Register(
		m.selfID,      // 服务实例名
		"_comet._tcp", // 服务类型（p2p file transfer）
		"local.",      // 域
		"",
		port,       // 端口
		txtRecords, // TXT记录
		nil,        // 回调（可留空）
	)
	if err != nil {
		return err
	}

	m.service = server
	m.registered = true
	m.logger.Infof("[Discovery] mDNS注册成功: %s, 端口: %d", m.selfID, port)
	return nil
}

// Unregister 注销服务
func (m *peerManager) Unregister() error {
	if m.service != nil && m.registered {
		m.service.Shutdown()
		m.registered = false
		m.logger.Info("[Discovery] mDNS注销成功")
	}
	return nil
}

// startBrowsing 开始浏览局域网节点
func (m *peerManager) startBrowsing() error {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return err
	}
	m.resolver = resolver

	entries := make(chan *zeroconf.ServiceEntry)

	// 启动浏览（直接使用 m.ctx，确保只有 Stop 时才会取消）
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		// 关键：使用 m.ctx，不派生新的
		if err := resolver.Browse(m.ctx, "_comet._tcp", "local.", entries); err != nil {
			m.logger.Errorf("[Discovery] 浏览失败: %v", err)
		}
		// Browse 返回后，entries 通道会被关闭
	}()

	// 处理发现的条目
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for entry := range entries {
			m.handleServiceEntry(entry)
		}
		m.logger.Info("[Discovery] 浏览通道已关闭")
	}()

	m.logger.Info("[Discovery] mDNS浏览已启动")
	return nil
}

// handleServiceEntry 处理发现的节点
func (m *peerManager) handleServiceEntry(entry *zeroconf.ServiceEntry) {
	// 1. 解析 TXT 记录获取 ID
	var peerID string
	for _, txt := range entry.Text {
		if len(txt) > 3 && txt[:3] == "id=" {
			peerID = txt[3:]
			break
		}
	}
	if peerID == "" {
		// 如果没有 ID，用实例名
		peerID = entry.Instance
	}

	// 2. 过滤自己（避免自己发现自己）
	if peerID == m.selfID {
		return
	}

	// 3. 提取地址（优先取 IPv6）
	var ip net.IP
	if len(entry.AddrIPv6) > 0 {
		ip = entry.AddrIPv6[0]
	} else if len(entry.AddrIPv4) > 0 {
		ip = entry.AddrIPv4[0]
	} else {
		return // 没有有效 IP
	}

	// 4. 构建 Peer 对象
	peer := &models.Peer{
		ID:       peerID,
		IP:       ip,
		Port:     entry.Port,
		Addr:     net.JoinHostPort(ip.String(), fmt.Sprintf("%d", entry.Port)),
		Hostname: entry.HostName,
		Online:   true,
		LastSeen: time.Now(),
	}

	// 5. 更新内存表
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.peers[peerID]
	if !ok {
		// 新节点加入
		m.peers[peerID] = peer
		m.logger.Infof("[Discovery] ✅ 新节点加入: %s (%s)", peer.Hostname, peer.Addr)
		m.eventCh <- Event{Type: EventPeerJoined, Peer: peer}
		m.saveCachedPeersLocked() // 持久化
	} else {
		// 已存在，更新 LastSeen 和 IP（可能变了）
		existing.LastSeen = time.Now()
		existing.Online = true
		if existing.Addr != peer.Addr {
			existing.Addr = peer.Addr
			existing.IP = peer.IP
			m.logger.Infof("[Discovery] 🔄 节点更新: %s 地址变更为 %s", peerID, peer.Addr)
			m.eventCh <- Event{Type: EventPeerUpdate, Peer: existing}
		}
	}
}

// ListPeers 获取在线节点列表
func (m *peerManager) ListPeers() ([]*models.Peer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]*models.Peer, 0, len(m.peers))
	for _, p := range m.peers {
		// 只返回在线的（30秒内有响应）
		if p.Online && time.Since(p.LastSeen) < 30*time.Second {
			peers = append(peers, p)
		}
	}
	return peers, nil
}

// GetPeer 根据 ID 获取节点
func (m *peerManager) GetPeer(id string) (*models.Peer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	p, ok := m.peers[id]
	if !ok {
		return nil, fmt.Errorf("节点 %s 不存在", id)
	}
	if !p.Online {
		return nil, fmt.Errorf("节点 %s 已离线", id)
	}
	return p, nil
}

// GetPeerByAddr 根据地址获取节点
func (m *peerManager) GetPeerByAddr(addr string) (*models.Peer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, p := range m.peers {
		if p.Addr == addr && p.Online {
			return p, nil
		}
	}
	return nil, fmt.Errorf("地址 %s 不在线", addr)
}

// Events 返回事件通道
func (m *peerManager) Events() <-chan Event {
	return m.eventCh
}

// cleanupLoop 后台清理离线节点
func (m *peerManager) cleanupLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.cleanupOfflinePeers()
		}
	}
}

// cleanupOfflinePeers 标记超时节点为离线
func (m *peerManager) cleanupOfflinePeers() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for id, p := range m.peers {
		if p.Online && now.Sub(p.LastSeen) > 45*time.Second {
			p.Online = false
			m.logger.Infof("[Discovery] ❌ 节点离线: %s (%s)", id, p.Addr)
			m.eventCh <- Event{Type: EventPeerLeft, Peer: p}
		}
	}
	m.saveCachedPeersLocked()
}

// --- 持久化缓存（与存储层交互）---

func (m *peerManager) loadCachedPeers() error {
	peers, err := m.store.ListPeers()
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, p := range peers {
		// 加载时默认标记为离线（等待mDNS确认）
		p.Online = false
		m.peers[p.ID] = p
	}
	m.logger.Infof("[Discovery] 从缓存加载 %d 个节点", len(peers))
	return nil
}

func (m *peerManager) saveCachedPeersLocked() {
	// 复制当前所有 peer 到一个切片，避免在锁内启动 goroutine 再读 map
	peers := make([]*models.Peer, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, p)
	}
	// 异步保存
	go func() {
		for _, p := range peers {
			if err := m.store.SavePeer(p); err != nil {
				m.logger.Warnf("[Discovery] 保存节点缓存失败: %v", err)
			}
		}
	}()
}

func (m *peerManager) saveCachedPeers() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.saveCachedPeersLocked()
}
func (m *peerManager) AddManualPeer(name string, addr string) error {
	// 解析地址
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("地址格式错误（需包含端口）: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("端口无效: %v", err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// 尝试解析主机名
		ips, err := net.LookupIP(host)
		if err != nil || len(ips) == 0 {
			return fmt.Errorf("无法解析主机名: %s", host)
		}
		ip = ips[0]
	}

	// 用 name 作为 ID（如果为空则用地址）
	id := name
	if id == "" {
		id = addr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 如果已存在则更新
	existing, ok := m.peers[id]
	if ok {
		existing.Addr = addr
		existing.IP = ip
		existing.Port = port
		existing.Hostname = name
		existing.Online = true
		existing.LastSeen = time.Now()
		m.saveCachedPeersLocked()
		m.eventCh <- Event{Type: EventPeerUpdate, Peer: existing}
		m.logger.Infof("[Discovery] 更新手动节点: %s -> %s", id, addr)
		return nil
	}

	peer := &models.Peer{
		ID:       id,
		Addr:     addr,
		IP:       ip,
		Port:     port,
		Hostname: name,
		Online:   true,
		LastSeen: time.Now(),
	}
	m.peers[id] = peer
	m.eventCh <- Event{Type: EventPeerJoined, Peer: peer}
	m.saveCachedPeersLocked()
	m.logger.Infof("[Discovery] 手动添加节点: %s (%s)", name, addr)
	return nil
}

func (m *peerManager) RemovePeer(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.peers[id]; !ok {
		return fmt.Errorf("节点 %s 不存在", id)
	}
	delete(m.peers, id)
	m.saveCachedPeersLocked()
	m.logger.Infof("[Discovery] 移除节点: %s", id)
	return nil
}
