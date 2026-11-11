package store

import (
	"context"
	"errors"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Mongo struct {
	logins       *mongo.Collection
	channels     *mongo.Collection
	publications *mongo.Collection
}

// connect tenta conectar no MongoDB até ele responder, já que o container pode demorar a subir.
func connect(ctx context.Context, uri string) (*mongo.Client, error) {
	const attempts = 60
	for attempt := 1; attempt <= attempts; attempt++ {
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = client.Ping(pingCtx, readpref.Primary())
			cancel()
			if err == nil {
				return client, nil
			}
			_ = client.Disconnect(ctx)
		}
		log.Printf("mongo indisponível (%d/%d): %v", attempt, attempts, err)
		time.Sleep(time.Second)
	}
	return nil, errors.New("não consegui conectar no MongoDB")
}

// NewMongo abre a conexão e garante que o nome do canal seja único no banco.
func NewMongo(ctx context.Context, uri, dbName string) (*Mongo, error) {
	client, err := connect(ctx, uri)
	if err != nil {
		return nil, err
	}
	db := client.Database(dbName)
	channels := db.Collection("channels")
	index := mongo.IndexModel{
		Keys:    bson.D{{Key: "name", Value: 1}},
		Options: options.Index().SetUnique(true),
	}
	if _, err := channels.Indexes().CreateOne(ctx, index); err != nil {
		return nil, err
	}
	return &Mongo{
		logins:       db.Collection("logins"),
		channels:     channels,
		publications: db.Collection("publications"),
	}, nil
}

// SaveLogin guarda o login com o timestamp enviado pelo bot e o horário em que o servidor recebeu.
func (m *Mongo) SaveLogin(ctx context.Context, user string, timestamp int64) error {
	_, err := m.logins.InsertOne(ctx, bson.M{
		"user":        user,
		"timestamp":   timestamp,
		"received_at": time.Now().UnixMilli(),
	})
	return err
}

// CreateChannel cria o canal e devolve false se o nome já existia.
func (m *Mongo) CreateChannel(ctx context.Context, name, user string, timestamp int64) (bool, error) {
	_, err := m.channels.InsertOne(ctx, bson.M{
		"name":       name,
		"created_by": user,
		"timestamp":  timestamp,
	})
	if mongo.IsDuplicateKeyError(err) {
		return false, nil
	}
	return err == nil, err
}

// ListChannels devolve os nomes de todos os canais em ordem alfabética.
func (m *Mongo) ListChannels(ctx context.Context) ([]string, error) {
	opts := options.Find().
		SetSort(bson.D{{Key: "name", Value: 1}}).
		SetProjection(bson.D{{Key: "name", Value: 1}})
	cursor, err := m.channels.Find(ctx, bson.D{}, opts)
	if err != nil {
		return nil, err
	}
	var docs []struct {
		Name string `bson:"name"`
	}
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(docs))
	for _, doc := range docs {
		names = append(names, doc.Name)
	}
	return names, nil
}

// SavePublication guarda a publicação com o timestamp do pedido, o da publicação e o horário de gravação.
func (m *Mongo) SavePublication(ctx context.Context, channel, user, text string, timestamp, publishedAt int64) error {
	_, err := m.publications.InsertOne(ctx, bson.M{
		"channel":      channel,
		"user":         user,
		"text":         text,
		"timestamp":    timestamp,
		"published_at": publishedAt,
		"received_at":  time.Now().UnixMilli(),
	})
	return err
}
