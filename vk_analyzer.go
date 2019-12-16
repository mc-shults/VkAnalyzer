package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"time"
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

func downloadFile(url string, filepath string) error {
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	return nil
}

func readResult(filename string) bool {
	dat, err := ioutil.ReadFile(filename)
	check(err)
	return string(dat) == "1"
}

func analysePhoto(url string) bool {
	err := downloadFile(url, "image.jpg")
	if err != nil {
		return false
	}
	cmd := exec.Command(`c:\Users\dmitr\AppData\Local\Programs\Python\Python36\python.exe`, "predict.py")
	cmd.Run()
	return readResult("result")
}

func work(done *chan struct{}, dbPosts *mongo.Collection) {
	for {
		post := getUnprocessedPost(dbPosts)
		photos := post.getPhotos()
		if len(photos) == 0 {
			setIsBike(post.ID, dbPosts, false)
			continue
		}
		isBike := false
		for _, photo := range photos {
			isBike = isBike || analysePhoto(photo.Photo130)
		}
		setIsBike(post.ID, dbPosts, isBike)
	}
}

func waitEnd(done *chan struct{}) {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	select {
	case <-interrupt:
		fmt.Println("interrupt")
		select {
		case <-*done:
		case <-time.After(time.Second):
		}
	case <-*done:
	}
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
	dbPosts := db.Database("big_data").Collection("posts")
	defer db.Disconnect(context.TODO())

	done := make(chan struct{})
	defer close(done)
	go work(&done, dbPosts)
	waitEnd(&done)
}
