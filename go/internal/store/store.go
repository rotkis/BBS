package store

import (
	"context"
	"errors"

	"bbs/internal/proto"
)

var ErrGap = errors.New("falta alguma operação antes desta")

type Store interface {
	LastSeq() int64
	Append(ctx context.Context, op proto.Op) (proto.Op, error)
	ApplyReplicated(ctx context.Context, op proto.Op) (bool, error)
	OpsAfter(ctx context.Context, after int64, limit int) ([]proto.Op, error)
	ChannelExists(ctx context.Context, name string) (bool, error)
	ListChannels(ctx context.Context) ([]string, error)
	History(ctx context.Context, channel string, limit int) (int64, []proto.Entry, error)
}
