// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package record

func Pack(entry []byte, firstSize, otherSize int) ([]Record, int) {
	num := calPacketNum(entry, firstSize, otherSize)
	if num == 1 {
		packet := makePacket(Full, entry)
		padding := firstSize - packet.Size()
		if padding >= HeaderSize {
			padding = 0
		}
		return []Record{packet}, padding
	}

	packets := make([]Record, 0, num)

	// first packet
	packets = append(packets, makePacket(First, entry[:firstSize-HeaderSize]))

	// middle packet(s)
	fo := firstSize - HeaderSize
	for i := 0; i < num-2; i++ {
		eo := fo + otherSize - HeaderSize
		packets = append(packets, makePacket(Middle, entry[fo:eo]))
		fo = eo
	}

	// last packet
	last := makePacket(Last, entry[fo:])
	packets = append(packets, last)

	padding := otherSize - last.Size()
	if padding >= HeaderSize {
		padding = 0
	}

	return packets, padding
}

func calPacketNum(entry []byte, firstSize, otherSize int) int {
	payload := len(entry)
	if payload <= firstSize-HeaderSize {
		return 1
	}
	// 1 + ((payload-(firstSize-HeaderSize))+((otherSize-HeaderSize)-1))/(otherSize-HeaderSize)
	return 1 + (payload-firstSize+otherSize-1)/(otherSize-HeaderSize)
}

func makePacket(t Type, payload []byte) Record {
	return Record{
		CRC:    0,
		Length: uint16(len(payload)),
		Type:   t,
		Data:   payload,
	}
}
