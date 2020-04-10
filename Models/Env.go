package Models

import (
	"fmt"
	"os"
)

var envStruct *Env

type Env struct {
	RabbitUser string
	RabbitPassword string
	RabbitQueue string
	RabbitHost string
	AwsStorageUrl string
	Env string
	MediaMetadataGrpcServer string
	MediaMetadataGrpcPort string
	ChunkMetadataGrpcServer string
	ChunkMetadataGrpcPort string
	AwsStorageGrpcServer string
	AwsStorageGrpcPort string
}

func InitEnv()  {
	envStruct = &Env{
		RabbitUser:       			os.Getenv("RABBIT_USER"),
		RabbitPassword:   			os.Getenv("RABBIT_PASSWORD"),
		RabbitQueue:      			os.Getenv("RABBIT_QUEUE"),
		RabbitHost:       			os.Getenv("RABBIT_HOST"),
		AwsStorageUrl:   			os.Getenv("AWS_STORAGE_URL"),
		Env: 			  			os.Getenv("ENV"),
		MediaMetadataGrpcServer: 	os.Getenv("MEDIA_METADATA_GRPC_SERVER"),
		MediaMetadataGrpcPort:   	os.Getenv("MEDIA_METADATA_GRPC_PORT"),
		ChunkMetadataGrpcServer:  	os.Getenv("CHUNK_METADATA_GRPC_SERVER"),
		ChunkMetadataGrpcPort:		os.Getenv("CHUNK_METADATA_GRPC_PORT"),
		AwsStorageGrpcServer: 		os.Getenv("AWS_STORAGE_GRPC_SERVER"),
		AwsStorageGrpcPort:			os.Getenv("AWS_STORAGE_GRPC_PORT"),
	}
	fmt.Println(envStruct)
}

func GetEnvStruct() *Env  {
	return  envStruct
}