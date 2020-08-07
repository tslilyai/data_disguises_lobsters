package gdpr


/* Removal Policies */
type EffectsPolicy int

const (
	Retain          EffectsPolicy = 1
	Revoke                        = 2
	RevokeDelete               = 3
)

type EffectsPolicies struct {
	VotePolicy EffectsPolicy
	TextPolicy EffectsPolicy
}

/* Data Representations */
type VoteCounts struct {
	Upvotes   int
	Downvotes int
}

type InvisText struct {
	// invisible ids
	InvisId       string
	InvisChildren []string

	// necessary to revive a dead text
    // will be empty if entry actually deleted
    TextCopy Text
}

type Text struct {
	User    int
	Id      int64
	Version int64

	Parent   int64
	Children []int64
	Content  string
}

type Vote struct {
	User    int
	Id      int64
	Version int64

	IsPos bool
}

/* RPC */

type Err int

const (
	OK              Err = 1
	ErrNotFound         = 2
	ErrUnauthorized     = 3
	ErrDisconnect       = 4
	ErrDB               = 5
)
type UpdateArgs struct {
	User     int
	Texts    []Text
	Votes    []Vote
	IsDelete bool
	// uploads accept 'orphaned' content, unless policy is "RevokeAndDelete"
	IsUpload bool
}

type UpdateReply struct {
	KnownVersion int64
	Err          Err
}

type UnsubscribeArgs struct {
	User int
}

type UnsubscribeReply struct {
	Err Err
}

type ReadArgs struct {
	User int
	Id   int64
}

type ReadReply struct {
	KnownVersion int64
	Text         Text
	VoteCounts   VoteCounts
	Err          Err
}
