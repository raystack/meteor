package mongodb

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var posts = []interface{}{
	bson.D{{"title", "World"}, {"body", "Hello World"}},
	bson.D{{"title", "Mars"}, {"body", "Hello Mars"}},
	bson.D{{"title", "Pluto"}, {"body", "Hello Pluto"}},
}

var connections = []interface{}{
	bson.D{{"name", "Albert"}, {"relation", "mutual"}},
	bson.D{{"name", "Josh"}, {"relation", "following"}},
	bson.D{{"name", "Abish"}, {"relation", "follower"}},
}

var reach = []interface{}{
	bson.D{{"views", "500"}, {"likes", "200"}, {"comments", "50"}},
	bson.D{{"views", "400"}, {"likes", "100"}, {"comments", "5"}},
	bson.D{{"views", "800"}, {"likes", "300"}, {"comments", "80"}},
}

type Post struct {
	Title string `bson:"title,omitempty"`
	Body  string `bson:"body,omitempty"`
}

type Connection struct {
	Name     string `bson:"name,omitempty"`
	Relation string `bson:"relation,omitempty"`
}

type Reach struct {
	Views    string `bson:"views,omitempty"`
	Likes    string `bson:"likes,omitempty"`
	Comments string `bson:"comments,omitempty"`
}

func MockDataGenerator(clientOptions *options.ClientOptions) (err error) {
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		log.Fatal(err)
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		return
	}
	err = InsertBlogs(ctx, client)
	if err != nil {
		return
	}
	err = InsertConnections(ctx, client)
	if err != nil {
		return
	}
	err = InsertReach(ctx, client)
	if err != nil {
		return
	}
	client.Disconnect(ctx)
	return
}

func InsertBlogs(ctx context.Context, client *mongo.Client) (err error) {
	collection := client.Database("blog").Collection("posts")
	_, insertErr := collection.InsertMany(ctx, posts)
	if insertErr != nil {
		return insertErr
	}
	cur, currErr := collection.Find(ctx, bson.D{})
	if currErr != nil {
		return currErr
	}
	defer cur.Close(ctx)
	var postsDB []Post
	if err = cur.All(ctx, &postsDB); err != nil {
		return err
	}
	return
}

func InsertConnections(ctx context.Context, client *mongo.Client) (err error) {
	collection := client.Database("blog").Collection("connection")
	_, insertErr := collection.InsertMany(ctx, connections)
	if insertErr != nil {
		return insertErr
	}
	cur, currErr := collection.Find(ctx, bson.D{})
	if currErr != nil {
		return currErr
	}
	defer cur.Close(ctx)
	var connectionsDB []Connection
	if err = cur.All(ctx, &connectionsDB); err != nil {
		return err
	}
	return
}

func InsertReach(ctx context.Context, client *mongo.Client) (err error) {
	collection := client.Database("blog").Collection("reach")
	_, insertErr := collection.InsertMany(ctx, reach)
	if insertErr != nil {
		return insertErr
	}
	cur, currErr := collection.Find(ctx, bson.D{})
	if currErr != nil {
		return currErr
	}
	defer cur.Close(ctx)
	var reachDB []Reach
	if err = cur.All(ctx, &reachDB); err != nil {
		return err
	}
	return
}
