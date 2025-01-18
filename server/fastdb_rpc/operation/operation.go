package operation

type Operation struct {
	RequestBody any
	Type        OperationType
}

type OperationType uint32

const (
	SET OperationType = iota + 1
	DELETE
)
