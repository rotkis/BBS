package store

import "context"

type Store interface {
	SaveLogin(ctx context.Context, user string, timestamp int64) error
	CreateChannel(ctx context.Context, name, user string, timestamp int64) (bool, error)
	ListChannels(ctx context.Context) ([]string, error)
	SavePublication(ctx context.Context, channel, user, text string, timestamp, publishedAt int64) error
}
