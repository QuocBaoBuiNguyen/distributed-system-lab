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

type NodeInfoArgs struct {
	Port    string
	ShardID string
}

type GetNodesByShardIDArgs struct {
	ShardID string
}

type GetNodesByShardIDRes struct {
	Ports []string
}

type NotifyNodesInShardArgs struct {
	Ports []string
}

type HashRange struct {
	Start uint64
	End   uint64
}

type MigrationRequestArgs struct {
	TargetShard   string
	TargetAddress string
	HashRange     HashRange
}
