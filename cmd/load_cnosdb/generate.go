package main

//go:generate flatc -o models --go --go-namespace models --gen-onefile ./models/models.fbs
//go:generate protoc --go_out=. --go-grpc_out=. proto/kv_service.proto
