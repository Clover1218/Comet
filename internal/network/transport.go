package network

import (
	"context"
	"io"
	"net"
	"sync"
	"time"
)

type Conn interface {
	SendPacket(cmd byte, payload []byte) error
	ReadPacket() (byte, []byte, error)
	RawConn() net.Conn
	Close() error
}

type Transport interface {
	Listen(ctx context.Context, addr string) (Listener, error)
	Dial(ctx context.Context, addr string) (Conn, error)
}

type Listener interface {
	Accept() (Conn, error)
	Close() error
	Addr() net.Addr
}

type tcpTransport struct {
}

func NewTCPTransport() Transport {
	return &tcpTransport{}
}

func (t *tcpTransport) Listen(ctx context.Context, addr string) (Listener, error) {
	ln, err := net.Listen("tcp6", addr)
	if err != nil {
		return nil, err
	}
	return &tcpListener{ln: ln}, nil
}

func (t *tcpTransport) Dial(ctx context.Context, addr string) (Conn, error) {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp6", addr)
	if err != nil {
		return nil, err
	}
	return &tcpConn{conn: conn}, nil
}

type tcpListener struct {
	ln net.Listener
}

func (l *tcpListener) Accept() (Conn, error) {
	conn, err := l.ln.Accept()
	if err != nil {
		return nil, err
	}
	return &tcpConn{conn: conn}, nil
}

func (l *tcpListener) Close() error {
	return l.ln.Close()
}

func (l *tcpListener) Addr() net.Addr {
	return l.ln.Addr()
}

type tcpConn struct {
	conn net.Conn
	mu   sync.Mutex
	buf  []byte
}

func (c *tcpConn) SendPacket(cmd byte, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := Encode(cmd, payload)
	_, err := c.conn.Write(data)
	if err != nil {
		return err
	}
	// logger.Debugf("[Network] 发送包: cmd=0x%02X, len=%d", cmd, len(payload))
	return nil
}

func (c *tcpConn) ReadPacket() (byte, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 累积读取，直到完整包
	for {
		if len(c.buf) < 9 {
			// 至少需要头部
			tmp := make([]byte, 1024)
			n, err := c.conn.Read(tmp)
			if err != nil {
				return 0, nil, err
			}
			c.buf = append(c.buf, tmp[:n]...)
			continue
		}

		cmd, payload, consumed, err := Decode(c.buf)
		if err != nil {
			if err.Error() == "数据不足" || err.Error() == "数据不完整" {
				// 继续读取更多数据
				tmp := make([]byte, 1024)
				n, err := c.conn.Read(tmp)
				if err != nil {
					if err == io.EOF && len(c.buf) > 0 {
						// 连接关闭但有残留数据，尝试解析
						cmd, payload, consumed, err = Decode(c.buf)
						if err == nil {
							c.buf = c.buf[consumed:]
							// logger.Debugf("[Network] 收到包(EOF): cmd=0x%02X, len=%d", cmd, len(payload))
							return cmd, payload, nil
						}
					}
					return 0, nil, err
				}
				c.buf = append(c.buf, tmp[:n]...)
				continue
			}
			return 0, nil, err
		}

		c.buf = c.buf[consumed:]
		// logger.Debugf("[Network] 收到包: cmd=0x%02X, len=%d", cmd, len(payload))
		return cmd, payload, nil
	}
}

func (c *tcpConn) RawConn() net.Conn {
	return c.conn
}

func (c *tcpConn) Close() error {
	return c.conn.Close()
}

// WithTimeout 为连接设置超时
func (c *tcpConn) SetTimeout(timeout time.Duration) {
	c.conn.SetDeadline(time.Now().Add(timeout))
}
