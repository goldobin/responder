package responder

import "errors"

var Closed = errors.New("closed")
var Panicked = errors.New("panicked")
var NoTarget = errors.New("no target")
