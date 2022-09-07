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

// vbs package implements a file format names Vanus Block Format.
// It is designed to store variable length CloudEvent data.
//
// The layout of `File` is:
//
//	┌──────────┬───────────────┬─────────────────┬───────────────────┐
//	│  Header  │  Entries ...  │  [ End Entry ]  │  [ Index Entry ]  │
//	└──────────┴───────────────┴─────────────────┴───────────────────┘
//
// The layout of `Header` is:
//
//	┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
//	│     Magic(4)    │      CRC(4)     │     Flags(4)    │  Break Flags(4) │
//	├─────────────────┼───┬───┬─────────┼─────────────────┴─────────────────┤
//	│  Data Offset(4) │(1)│(1)│ Size(2) │            Capacity(8)            │
//	├─────────────────┴───┴───┴─────────┼─────────────────┬─────────┬───────┤
//	│          Entry Length(8)          │   Entry Num(4)  │Offset(2)│       ;
//	└───────────────────────────────────┴─────────────────┴─────────┴-------'
//
// All values little-endian
//
//	+00 4B Magic number (0x00627376, "vsb" in ASCII)
//	+04 4B CRC-32c of header block
//	+08 4B Flags
//	+0C 4B Break Flags
//	+10 4B Data Offset (in bytes, currently 4096)
//	+14 1B State (0: working, 1: archived)
//	+15 1B Reserved
//	+16 2B Index Size (in bytes, currently 24)
//	+18 8B Capacity (in bytes)
//	+20 8B Entry Length (in bytes)
//	+28 4B Entry Num (number of entries)
//	+2C 2B Index Offset (in bytes)
//
// The layout of `Packet` is:
//
//	┌───────────────────────────────────┬───────────────────────────────────┐
//	│          Packet Length(4)         │              Payload ...          │
//	├───────────────────────────────────┼───────────────────────────────────┤
//	│          Packet Length(4)         │               CRC(4)              │
//	└───────────────────────────────────┴───────────────────────────────────┘
//
// All values little-endian
//
//	+00 4B Packet Length (in bytes)
//	+04    Payload
//	    4B Packet Length (in bytes)
//	    4B CRC-32c of payload
//
// The layout of `Record` is:
//
//	;-----------------------------------┌─────────────────┌─────────────────┐
//	;                                   │     Type(2)     │    Offset(2)    │
//	├───────────────────────────────────┴─────────────────┴─────────────────┤
//	│                               Payload ...                             │
//	└───────────────────────────────────────────────────────────────────────┘
//
// All values little-endian
//
//	+00 2B Type
//	+02 2B Offset (offset of payload, 18 for v1)
//	+04    Payload (CloudEvent, End, etc.)
//
// The layout of `Entry` is:
//
//	┌─────────────────┬─────────────────────────────────────────────────────┐
//	│   Ext Count(2)  │           Non-Null Attributes Bitmap(6)             │
//	├─────────────────┴─────────────────────────────────────────────────────┤
//	│                    Optional Attributes Vector ...                     │
//	├───────────────────────────────────────────────────────────────────────┤
//	│                    Extension Attributes Vector ...                    │
//	├───────────────────────────────────────────────────────────────────────┤
//	│                   Variable Length Values Region ...                   │
//	└───────────────────────────────────────────────────────────────────────┘
//
// All values little-endian
//
//	+00 2B: Extension Attribute Count
//	+02 6B: Non-Null Attributes Bitmap
//
// The layout of `Optional Attributes Vectors` is:
//
//	┌─────────────────┬─────────────────┬───────────────────────────────────┐
//	│ Value Length(4) │ Value Offset(4) │                ...                │
//	└─────────────────┴─────────────────┴───────────────────────────────────┘
//
// The layout of `Extension Attributes Vectors` is:
//
//	┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
//	│  Key Length(4)  │  Key Offset(4)  │ Value Length(4) │ Value Offset(4) │
//	├─────────────────┴─────────────────┴─────────────────┴─────────────────┤
//	│                                  ...                                  │
//	└───────────────────────────────────────────────────────────────────────┘
//
// The layout of `Index` is:
//
//	┌───────────────────────────────────────────────────────────────────────┐
//	│                               Offset(8)                               │
//	├───────────────────────────────────┬───────────────────────────────────┤
//	│               Length(4)           │            Reserved(4)            │
//	├───────────────────────────────────┴───────────────────────────────────┤
//	│                                Stime(8)                               │
//	└───────────────────────────────────────────────────────────────────────┘
//
// All values little-endian
//
//	+00 8B Offset
//	+08 4B Length (in bytes)
//	+0C 4B Reserved (all 0)
//	+10 8B Stime
package vsb
