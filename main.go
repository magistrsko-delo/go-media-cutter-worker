package main

import (
	"crypto/tls"
	"github.com/heptiolabs/healthcheck"
	"go-media-cutter-worker/Models"
	"go-media-cutter-worker/Worker"
	"github.com/joho/godotenv"
	"log"
	"net/http"
	"time"
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}
	Models.InitEnv()
}

func main()  {
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	env := Models.GetEnvStruct()
	health := healthcheck.NewHandler()

	health.AddLivenessCheck("aws-storage: " + env.AwsStorageGrpcServer + ":" + env.AwsStorageGrpcPort, healthcheck.TCPDialCheck(env.AwsStorageGrpcServer + ":" + env.AwsStorageGrpcPort, 5*time.Second))
	health.AddLivenessCheck("aws-service-check-url: " + Models.GetEnvStruct().AwsStorageUrl + "health", healthcheck.HTTPGetCheck(Models.GetEnvStruct().AwsStorageUrl + "health", 5*time.Second))
	health.AddLivenessCheck("media-metadata: " + env.MediaMetadataGrpcServer + ":" + env.MediaMetadataGrpcPort, healthcheck.TCPDialCheck(env.MediaMetadataGrpcServer + ":" + env.MediaMetadataGrpcPort, 5*time.Second))
	health.AddLivenessCheck("chunk-metadata: " + env.ChunkMetadataGrpcServer + ":" + env.ChunkMetadataGrpcPort, healthcheck.TCPDialCheck(env.ChunkMetadataGrpcServer + ":" + env.ChunkMetadataGrpcPort, 5*time.Second))
	health.AddLivenessCheck("rabbit-mq: " + env.RabbitHost + ":" + env.RabbitPort, healthcheck.TCPDialCheck(env.RabbitHost + ":" + env.RabbitPort, 5*time.Second))

	go http.ListenAndServe("0.0.0.0:8888", health)
	worker := Worker.InitWorker()
	defer worker.RabbitMQ.Conn.Close()
	defer worker.RabbitMQ.Ch.Close()
	worker.Work()
}