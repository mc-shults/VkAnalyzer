package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Config - program config
type Config struct {
	MongoURL string
}

// Photo - vk photo
type Photo struct {
	Photo130 string `bson:"photo_130"`
}

// Attachment - vk post attachement
type Attachment struct {
	Type  string
	Photo Photo
}

// Post - vk wall post
type Post struct {
	ID          interface{} `bson:"_id"`
	Attachments []Attachment
}

var dbPosts *mongo.Collection

func (post Post) getPhotos() []Photo {
	var photos []Photo
	for _, attachment := range post.Attachments {
		if attachment.Type == "photo" {
			photos = append(photos, attachment.Photo)
		}
	}
	return photos
}

func check(e error) {
	if e != nil {
		log.Fatal("dial error:", e)
	}
}

func initConfig(filename string) Config {
	dat, err := ioutil.ReadFile(filename)
	check(err)

	var conf Config
	json.Unmarshal([]byte(dat), &conf)
	return conf
}

func getUnprocessedPost(dbPosts *mongo.Collection) Post {
	for {
		exists := bson.D{{Key: "$exists", Value: true}}
		notExists := bson.D{{Key: "$exists", Value: false}}
		filter := bson.D{{Key: "attachments.0", Value: exists}, {Key: "is_bike", Value: notExists}}
		var post Post
		err := dbPosts.FindOne(context.TODO(), filter).Decode(&post)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		return post
	}
}

func setIsBike(id interface{}, dbPosts *mongo.Collection, isBike bool) {
	filter := bson.D{{Key: "_id", Value: id}}
	transportType := bson.D{{Key: "is_bike", Value: isBike}}
	update := bson.D{{Key: "$set", Value: transportType}}
	dbPosts.UpdateOne(context.TODO(), filter, update)
}

func analysePhoto(connection *websocket.Conn, url string) (result bool, err bool) {
	result = false
	err = true
	errInfo := connection.WriteMessage(1, []byte(url))
	if errInfo != nil {
		log.Println("analyse url write error:", errInfo)
		return
	}

	_, message, errInfo := connection.ReadMessage()
	if errInfo != nil {
		log.Println("Analyse result read error:", errInfo)
		return
	}
	result = string(message) == "1"
	err = false
	return
}

func analyzePhotos(connection *websocket.Conn, photos []Photo) (result bool, err bool) {

	for _, photo := range photos {
		result, err = analysePhoto(connection, photo.Photo130)
		if err {
			return
		} else if !result {
			return
		}
	}
	return true, false
}

func processPost(connection *websocket.Conn, post Post) (err bool) {
	photos := post.getPhotos()
	if len(photos) == 0 {
		setIsBike(post.ID, dbPosts, false)
		return false
	}
	isBike, connErr := analyzePhotos(connection, photos)
	if connErr {
		return true
	}
	setIsBike(post.ID, dbPosts, isBike)
	return false
}

func waitForRequest(connection *websocket.Conn) (err bool) {
	_, message, errInfo := connection.ReadMessage()
	if errInfo != nil {
		log.Println("Read error:", errInfo)
		return true
	} else if string(message) != "get" {
		connection.WriteMessage(1, []byte("Invalid request"))
		return true
	}
	return false
}

func endProcessPost(connection *websocket.Conn, postID interface{}) (err bool) {
	fmt.Println("End:", postID)
	errInfo := connection.WriteMessage(1, []byte("end"))

	if errInfo != nil {
		log.Println("write:", err)
		return true
	}
	return false
}

func work(connection *websocket.Conn) {
	for {
		if waitForRequest(connection) {
			break
		}
		post := getUnprocessedPost(dbPosts)
		if processPost(connection, post) {
			break
		}
		if endProcessPost(connection, post.ID) {
			break
		}
	}
}

func server(w http.ResponseWriter, r *http.Request) {
	var upgrader = websocket.Upgrader{}
	upgrader.CheckOrigin = func(r *http.Request) bool {
		return true
	}
	connection, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer connection.Close()
	work(connection)
}

func connectMongo(mongoURL string) *mongo.Client {
	db, err := mongo.NewClient(options.Client().ApplyURI(mongoURL))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MongoDB!")
	return db
}

func main() {
	conf := initConfig("config.json")
	db := connectMongo(conf.MongoURL)
	dbPosts = db.Database("big_data").Collection("posts")
	defer db.Disconnect(context.TODO())
	http.HandleFunc("/", server)
	log.Fatal(http.ListenAndServe("0.0.0.0:8080", nil))
}
