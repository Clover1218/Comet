package engine

import "encoding/binary"

// boolsToBytes 将 []bool 按位打包为 []byte，前4字节为长度（大端）
func boolsToBytes(bits []bool) []byte {
	n := (len(bits) + 7) / 8
	data := make([]byte, 4+n)
	binary.BigEndian.PutUint32(data[:4], uint32(len(bits)))
	for i, v := range bits {
		if v {
			data[4+i/8] |= 1 << (i % 8)
		}
	}
	return data
}

// bytesToBools 将 boolsToBytes 打包的数据还原为 []bool
func bytesToBools(data []byte) []bool {
	if len(data) < 4 {
		return nil
	}
	n := int(binary.BigEndian.Uint32(data[:4]))
	bits := make([]bool, n)
	for i := 0; i < n; i++ {
		if data[4+i/8]&(1<<(i%8)) != 0 {
			bits[i] = true
		}
	}
	return bits
}

// mergeCompleted 合并两个 completed 位图（取并集）
// 返回一个新的切片，长度为 max(len(a), len(b))
func mergeCompleted(a, b []bool) []bool {
	n := len(a)
	if len(b) > n {
		n = len(b)
	}
	result := make([]bool, n)
	for i := range result {
		if i < len(a) && a[i] {
			result[i] = true
		}
		if i < len(b) && b[i] {
			result[i] = true
		}
	}
	return result
}
