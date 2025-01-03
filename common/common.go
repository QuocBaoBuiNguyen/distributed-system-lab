package common

type SetArgs struct {
	Bucket string
	Key    int
	Value  interface{}
}

type GetArgs struct {
	Bucket string
	Key    int
}

type GetAllArgs struct {
	Bucket string
}

type DeleteArgs struct {
	Bucket string
	Key    int
}
