package grpc_client

import (
	"fmt"
	"go-media-cutter-worker/Models"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"

	pbMediaChunks "go-media-cutter-worker/proto/media_chunks_metadata"
)

type MediaChunksClient struct {
	Conn *grpc.ClientConn
	client pbMediaChunks.MediaMetadataClient
}

func (mediaChunksClient *MediaChunksClient) GetMediaChunkInfo(chunkId int32) (*pbMediaChunks.ChunkInfo, error)  {

	response, err := mediaChunksClient.client.GetMediaChunkInfo(context.Background(), &pbMediaChunks.GetMediaChunkInfoRequest{
		ChunkId:              chunkId,
	})

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (mediaChunksClient *MediaChunksClient) NewMediaChunk(bucketName string, storageName string, length float64, mediaId int32, resolution string, position int32) (*pbMediaChunks.MediaChunkInfoResponseRepeated, error)  {

	response, err := mediaChunksClient.client.NewMediaChunk(context.Background(), &pbMediaChunks.NewMediaChunkRequest{
		AwsBucketName:        bucketName,
		AwsStorageName:       storageName,
		Length:               length,
		MediaId:              mediaId,
		Resolution:           resolution,
		Position:             position,
	})

	if err != nil {
		return nil, err
	}

	return response, nil
}


func InitChunkMetadataClient() *MediaChunksClient  {
	env := Models.GetEnvStruct()
	fmt.Println("CONNECTING chunks metadata")

	conn, err := grpc.Dial(env.ChunkMetadataGrpcServer + ":" + env.ChunkMetadataGrpcPort, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	fmt.Println("END CONNECTION chunk metadata")

	client := pbMediaChunks.NewMediaMetadataClient(conn)
	return &MediaChunksClient{
		Conn:    conn,
		client:  client,
	}

}
