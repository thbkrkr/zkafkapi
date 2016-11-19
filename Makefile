
build: build-go
	docker build -t krkr/zkafkapi .

build-go:
	docker run --rm \
		-v $$(pwd):/go/src/zkafkapi \
		-e GOBIN=/go/bin/ \
		-e CGO_ENABLED=0 \
		-e GOPATH=/go \
		-w /go/src/zkafkapi \
			golang:1.7.1-alpine \
				go build

push:
	docker push krkr/zkafkapi
