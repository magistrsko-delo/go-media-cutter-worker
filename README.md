# go-media-cutter-worker
go media  cutter worker

## RabbitMQ 
* Sample message  
```[{"chunkId":15,"from":2.418667,"to":5.072,"position":0,"resolution":"1920x1080","mediaId":16},{"chunkId":19,"from":0,"to":2.0153329999999983,"position":4,"resolution":"1920x1080","mediaId":16}]```

##PROTOCOL BUFFER

```.env
protoc proto\helloworld.proto --go_out=plugins=grpc:.
```