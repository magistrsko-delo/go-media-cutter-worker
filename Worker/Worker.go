package Worker

import (
	"encoding/json"
	"fmt"
	"go-media-cutter-worker/Http"
	"go-media-cutter-worker/Models"
	"go-media-cutter-worker/ffmpeg"
	"go-media-cutter-worker/grpc_client"
	pbMediaChunks "go-media-cutter-worker/proto/media_chunks_metadata"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Worker struct {
	RabbitMQ *RabbitMqConnection
	mediaMetadataClient *grpc_client.MediaMetadataClient
	mediaChunksClient *grpc_client.MediaChunksClient
	awsStorageClient *grpc_client.AwsStorageClient
	mediaDowLoader *Http.MediaDownloader
	ffmpeg *ffmpeg.FFmpeg
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

			if (len(mediaCutMessages) == 0) {
				log.Println("no chunk data for cut available")
			}

			mediaInfo, err := worker.mediaMetadataClient.GetMediaMetadata(mediaCutMessages[0].MediaId)

			// cut each media chunk and save it as new chunk on aws and metadata
			for i := 0; i < len(mediaCutMessages); i++ {
				fmt.Println(mediaCutMessages[i].ChunksId)
				chunksInfo, err := worker.mediaChunksClient.GetMediaChunkInfo(mediaCutMessages[i].ChunksId)

				if err != nil {
					log.Println(err)
				}

				fileUrl := worker.env.AwsStorageUrl + "v1/awsStorage/media/" + chunksInfo.GetAwsBucketName() + "/" + chunksInfo.GetAwsStorageName()
				err = worker.mediaDowLoader.DownloadFile("./assets/" + chunksInfo.GetAwsStorageName() , fileUrl)

				if err != nil {
					log.Println(err)
				}

				err = worker.cutVideo(mediaCutMessages[i], chunksInfo.GetAwsStorageName(),  "./assets/")
				if err != nil {
					log.Println(err)
				}

				files, err := worker.getFilesPathsInDirectory()
				if err != nil {
					log.Println(err)
				}
				err = worker.handleMediaChunks(files, chunksInfo, mediaCutMessages[i])
				if err != nil {
					log.Println(err)
				}

				worker.removeFile("./assets/" + chunksInfo.GetAwsStorageName())
			}

			_, err = worker.mediaMetadataClient.UpdateMediaMetadata(mediaInfo)
			if err != nil {
				log.Println(err)
			}

			log.Printf("Done")
			_ = d.Ack(false)
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func (worker *Worker) cutVideo(mediaCutMessage *Models.MediaCutRabbitMQMessage, fileName string, directory string) error  {
	startHour := strconv.Itoa(int(math.Floor(mediaCutMessage.From / 3600)))
	startMin := strconv.Itoa(int(math.Floor( (math.Mod(mediaCutMessage.From, 3600)) / 60   )))
	startSec := strconv.Itoa(int(math.Floor( math.Mod( math.Mod(mediaCutMessage.From, 3600), 60 )  )))
	startMiliSec := strconv.Itoa(int( math.Mod(mediaCutMessage.From * 1000, 1000)  ))

	ss := startHour + ":" + startMin + ":" + startSec + "." + startMiliSec

	cutDuration := mediaCutMessage.To - mediaCutMessage.From
	cutDurationHour := strconv.Itoa(int(math.Floor(cutDuration / 3600)))
	cutDurationMinute := strconv.Itoa(int(math.Floor( (math.Mod(cutDuration, 3600)) / 60   )))
	cutDurationSec := strconv.Itoa(int(math.Floor( math.Mod( math.Mod(cutDuration, 3600), 60 )  )))
	cutDurationMisiSec := strconv.Itoa(int( math.Mod(cutDuration * 1000, 1000)  ))

	to := cutDurationHour + ":" + cutDurationMinute + ":" + cutDurationSec + "." + cutDurationMisiSec

	cmdArgs := []string{"-i", directory + fileName,  "-ss", ss, "-t", to, "-async", "1", directory + "chunks/" + strconv.Itoa(int(mediaCutMessage.ChunksId)) + "_" + strconv.Itoa(rand.Intn(1000000000000)) + "_" + fileName}

	err := worker.ffmpeg.ExecFFmpegCommand(cmdArgs)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (worker *Worker) handleMediaChunks(files []string, chunkInfo  *pbMediaChunks.ChunkInfo, mediaCutMessage *Models.MediaCutRabbitMQMessage)  error {
	position := 0
	for index, file := range files {
		if index == 0 || strings.Contains(file, ".gitkeep") {
			continue
		}

		log.Println(file)
		filePathArray := []string{}
		if worker.env.Env == "live" {
			filePathArray = strings.Split(file, "/")
		} else {
			filePathArray = strings.Split(file, "\\")
		}

		_, err := worker.awsStorageClient.UploadMedia(file, chunkInfo.GetAwsBucketName(), filePathArray[len(filePathArray) - 1])
		if err != nil {
			log.Println(err)
			return err
		}

		_, err = worker.mediaChunksClient.NewMediaChunk(
			chunkInfo.GetAwsBucketName(),
			filePathArray[len(filePathArray) - 1],
			mediaCutMessage.To - mediaCutMessage.From,
			mediaCutMessage.MediaId,
			mediaCutMessage.Resolution,
			mediaCutMessage.Position)
		if err != nil {
			log.Println(err)
			return err
		}
		position++
		worker.removeFile(file)
	}
	return nil
}

func (worker *Worker) getFilesPathsInDirectory() ([]string, error) {
	var files []string
	root := "./assets/chunks"

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)
		return err
	})

	if err != nil {
		log.Println(err)
		return nil, err
	}

	return files, nil
}

func (worker *Worker) removeFile(path string)  {
	err := os.Remove(path)
	if err != nil {
		fmt.Println(err)
	}
}


func InitWorker() *Worker  {
	return &Worker{
		RabbitMQ: 				initRabbitMqConnection(Models.GetEnvStruct()),
		mediaMetadataClient: 	grpc_client.InitMediaMetadataGrpcClient(),
		mediaChunksClient:		grpc_client.InitChunkMetadataClient(),
		awsStorageClient:		grpc_client.InitAwsStorageGrpcClient(),
		mediaDowLoader: 		&Http.MediaDownloader{},
		ffmpeg: 				&ffmpeg.FFmpeg{},
		env:      				Models.GetEnvStruct(),
	}

}