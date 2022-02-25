package primitive

type Initializer interface {
	Initialize() error
}

type Closer interface {
	Close() error
}
