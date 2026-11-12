package store

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"bbs/internal/proto"
)

type Mongo struct {
	mu           sync.Mutex
	last         int64
	logins       *mongo.Collection
	channels     *mongo.Collection
	publications *mongo.Collection
	oplog        *mongo.Collection
}

type opDoc struct {
	Seq       int64  `bson:"_id"`
	Type      string `bson:"type"`
	User      string `bson:"user"`
	Channel   string `bson:"channel"`
	Text      string `bson:"text"`
	Timestamp int64  `bson:"timestamp"`
	At        int64  `bson:"at"`
}

type publicationDoc struct {
	Seq         int64  `bson:"_id"`
	User        string `bson:"user"`
	Text        string `bson:"text"`
	PublishedAt int64  `bson:"published_at"`
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

// NewMongo abre a conexão, garante o índice único do nome do canal e descobre o número da última operação gravada.
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
	m := &Mongo{
		logins:       db.Collection("logins"),
		channels:     channels,
		publications: db.Collection("publications"),
		oplog:        db.Collection("oplog"),
	}
	var last opDoc
	err = m.oplog.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}})).Decode(&last)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, err
	}
	m.last = last.Seq
	return m, nil
}

// LastSeq devolve o número da última operação aplicada neste servidor.
func (m *Mongo) LastSeq() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.last
}

// apply grava a operação na coleção do tipo e no log; só pode rodar com o lock adquirido.
func (m *Mongo) apply(ctx context.Context, op proto.Op) error {
	var err error
	now := time.Now().UnixMilli()
	switch op.Type {
	case "login":
		_, err = m.logins.InsertOne(ctx, bson.M{"_id": op.Seq, "user": op.User, "timestamp": op.Timestamp, "received_at": now})
	case "channel":
		_, err = m.channels.InsertOne(ctx, bson.M{"_id": op.Seq, "name": op.Channel, "created_by": op.User, "timestamp": op.Timestamp})
	case "publish":
		_, err = m.publications.InsertOne(ctx, bson.M{
			"_id": op.Seq, "channel": op.Channel, "user": op.User, "text": op.Text,
			"timestamp": op.Timestamp, "published_at": op.At, "received_at": now,
		})
	}
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return err
	}
	_, err = m.oplog.InsertOne(ctx, bson.M{
		"_id": op.Seq, "type": op.Type, "user": op.User, "channel": op.Channel,
		"text": op.Text, "timestamp": op.Timestamp, "at": op.At,
	})
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return err
	}
	m.last = op.Seq
	return nil
}

// Append numera a operação com o próximo número da sequência, grava e devolve a operação numerada; é o que o coordenador usa.
func (m *Mongo) Append(ctx context.Context, op proto.Op) (proto.Op, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	op.Seq = m.last + 1
	return op, m.apply(ctx, op)
}

// ApplyReplicated aplica uma operação vinda do coordenador; devolve false se já tinha e ErrGap se falta alguma antes.
func (m *Mongo) ApplyReplicated(ctx context.Context, op proto.Op) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if op.Seq <= m.last {
		return false, nil
	}
	if op.Seq != m.last+1 {
		return false, ErrGap
	}
	return true, m.apply(ctx, op)
}

// OpsAfter devolve até limit operações com número maior que after, em ordem.
func (m *Mongo) OpsAfter(ctx context.Context, after int64, limit int) ([]proto.Op, error) {
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(limit))
	cursor, err := m.oplog.Find(ctx, bson.M{"_id": bson.M{"$gt": after}}, opts)
	if err != nil {
		return nil, err
	}
	var docs []opDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	ops := make([]proto.Op, 0, len(docs))
	for _, doc := range docs {
		ops = append(ops, proto.Op{
			Seq: doc.Seq, Type: doc.Type, User: doc.User, Channel: doc.Channel,
			Text: doc.Text, Timestamp: doc.Timestamp, At: doc.At,
		})
	}
	return ops, nil
}

// ChannelExists diz se já existe um canal com esse nome.
func (m *Mongo) ChannelExists(ctx context.Context, name string) (bool, error) {
	count, err := m.channels.CountDocuments(ctx, bson.M{"name": name}, options.Count().SetLimit(1))
	return count > 0, err
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

// History devolve o total de publicações do canal e as últimas limit delas, da mais antiga para a mais nova.
func (m *Mongo) History(ctx context.Context, channel string, limit int) (int64, []proto.Entry, error) {
	total, err := m.publications.CountDocuments(ctx, bson.M{"channel": channel})
	if err != nil {
		return 0, nil, err
	}
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(int64(limit))
	cursor, err := m.publications.Find(ctx, bson.M{"channel": channel}, opts)
	if err != nil {
		return 0, nil, err
	}
	var docs []publicationDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return 0, nil, err
	}
	entries := make([]proto.Entry, 0, len(docs))
	for i := len(docs) - 1; i >= 0; i-- {
		entries = append(entries, proto.Entry{Seq: docs[i].Seq, User: docs[i].User, Text: docs[i].Text, Timestamp: docs[i].PublishedAt})
	}
	return total, entries, nil
}
