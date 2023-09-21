// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package snowflake

const (
	// NodeName space: [0, 65535], DON'T CHANGE THEM!!!
	controllerNodeIDStart           = uint16(16)
	reservedControlPanelNodeIDStart = uint16(32)
	storeNodeIDStart                = uint16(1024)
	reservedNodeIDStart             = uint16(8192)
)

type Service int

const (
	ControllerService Service = iota
	StoreService
	UnknownService
)

func (s Service) Name() string {
	switch s {
	case ControllerService:
		return "ControllerService"
	case StoreService:
		return "StoreService"
	default:
		return "UnknownService"
	}
}

type node struct {
	start uint16
	end   uint16
	id    uint16
	svc   Service
}

func NewNode(svc Service, id uint16) *node { //nolint: revive // it's ok
	switch svc {
	case ControllerService:
		return &node{
			start: controllerNodeIDStart,
			end:   reservedControlPanelNodeIDStart,
			svc:   svc,
			id:    id,
		}
	case StoreService:
		return &node{
			start: storeNodeIDStart,
			end:   reservedNodeIDStart,
			svc:   svc,
			id:    id,
		}
	}
	return &node{
		start: reservedNodeIDStart,
		end:   reservedNodeIDStart,
		svc:   UnknownService,
		id:    id,
	}
}

func (n *node) logicID() uint16 {
	return n.start + n.id
}

func (n *node) valid() bool {
	return n.logicID() < n.end && n.logicID() >= n.start
}
