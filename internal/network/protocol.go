package network

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	Magic = 0xFEEDBEAF

	CmdAuth      byte = 0x01
	CmdAuthOK    byte = 0x02
	CmdAuthFail  byte = 0x03
	CmdMeta      byte = 0x10
	CmdMetaAck   byte = 0x11
	CmdChunk     byte = 0x20
	CmdChunkAck  byte = 0x21
	CmdQuery     byte = 0x30
	CmdQueryResp byte = 0x31
	CmdComplete  byte = 0x40
	CmdError     byte = 0xFF
)

type Packet struct {
	Cmd     byte
	Payload []byte
}

func Encode(cmd byte, payload []byte) []byte {
	// [Magic 4B][Cmd 1B][PayloadLen 4B][Payload]
	data := make([]byte, 9+len(payload))
	binary.BigEndian.PutUint32(data[0:4], Magic)
	data[4] = cmd
	binary.BigEndian.PutUint32(data[5:9], uint32(len(payload)))
	copy(data[9:], payload)
	return data
}

func Decode(data []byte) (cmd byte, payload []byte, consumed int, err error) {
	if len(data) < 9 {
		return 0, nil, 0, errors.New("数据不足")
	}

	magic := binary.BigEndian.Uint32(data[0:4])
	if magic != Magic {
		return 0, nil, 0, fmt.Errorf("魔数错误: 0x%X", magic)
	}

	cmd = data[4]
	payloadLen := binary.BigEndian.Uint32(data[5:9])

	if len(data) < 9+int(payloadLen) {
		return 0, nil, 0, errors.New("数据不完整")
	}

	payload = data[9 : 9+payloadLen]
	consumed = 9 + int(payloadLen)
	return cmd, payload, consumed, nil
}

// Helper functions
func EncodeString(s string) []byte {
	return []byte(s)
}

func DecodeString(b []byte) string {
	return string(b)
}
