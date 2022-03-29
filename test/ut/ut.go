package ut

import "fmt"

//go:generate mockgen --source=test/ut/ut.go --destination=test/ut/mock_ut.go --package=ut
type Foo interface {
	Bar(x int) int
}

func SUT(f Foo) {
	fmt.Println(f.Bar(99))
}
