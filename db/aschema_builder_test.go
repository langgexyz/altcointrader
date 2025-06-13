package db

import (
	"fmt"
	"github.com/xpwu/go-db-mongo/mongodb/field"
	"reflect"
)

func Example_Build_Schema() {
	builder := field.New()

	builder.Build(reflect.TypeOf(&KlineDocument{}))
	fmt.Print(true)
	// Output:
	// true
}
