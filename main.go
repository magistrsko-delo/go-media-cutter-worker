package main

import (
	"crypto/tls"
	"go-media-cutter-worker/Models"
	"go-media-cutter-worker/Worker"
	"github.com/joho/godotenv"
	"log"
	"net/http"
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}
	Models.InitEnv()
}

func main()  {
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	worker := Worker.InitWorker()
	defer worker.RabbitMQ.Conn.Close()
	defer worker.RabbitMQ.Ch.Close()
	worker.Work()
}