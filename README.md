# go-media-cutter-worker
go media  cutter worker

## RabbitMQ 
* Sample message  
```[{"chunkId":10,"from":1.3466669999999983,"to":5.08,"position":0,"resolution":"1920x1080","mediaId":19},{"chunkId":12,"from":0,"to":3.089999999999998,"position":2,"resolution":"1920x1080","mediaId":19}]```

## PROTOCOL BUFFER

```.env
protoc proto\helloworld.proto --go_out=plugins=grpc:.
```