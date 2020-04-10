package Worker

import (
	"encoding/json"
	"fmt"
	"go-media-cutter-worker/Http"
	"go-media-cutter-worker/Models"
	"go-media-cutter-worker/grpc_client"
	"log"
)

type Worker struct {
	RabbitMQ *RabbitMqConnection
	mediaMetadataClient *grpc_client.MediaMetadataClient
	mediaChunksClient *grpc_client.MediaChunksClient
	awsStorageClient *grpc_client.AwsStorageClient
	mediaDowLoader *Http.MediaDownloader
	env *Models.Env
}

func (worker *Worker) Work()  {
	forever := make(chan bool)

	go func() {
		for d := range worker.RabbitMQ.msgs {
			log.Printf("Received a message: %s", d.Body)

			var mediaCutMessages []*Models.MediaCutRabbitMQMessage
			err := json.Unmarshal(d.Body, &mediaCutMessages)
			if err != nil{
				log.Println(err)
			}

			// cut each media chunk and save it as new chunk on aws and metadata
			for i := 0; i < len(mediaCutMessages); i++ {
				fmt.Println(mediaCutMessages[i])
			}

			log.Printf("Done")
			_ = d.Ack(false)
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}


func InitWorker() *Worker  {
	return &Worker{
		RabbitMQ: 				initRabbitMqConnection(Models.GetEnvStruct()),
		mediaMetadataClient: 	grpc_client.InitMediaMetadataGrpcClient(),
		mediaChunksClient:		grpc_client.InitChunkMetadataClient(),
		awsStorageClient:		grpc_client.InitAwsStorageGrpcClient(),
		mediaDowLoader: 		&Http.MediaDownloader{},
		env:      				Models.GetEnvStruct(),
	}

}