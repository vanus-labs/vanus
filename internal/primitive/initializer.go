package primitive

import "context"

type Initializer interface {
	Initialize(context.Context) error
}

type Closer interface {
	Close() error
}
