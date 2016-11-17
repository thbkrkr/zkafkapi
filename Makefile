
build: build-go
	docker build -t krkr/kafka-topics .

build-go:
	docker run --rm \
		-v $$(pwd):/go/src/kafka-topics \
		-e GOBIN=/go/bin/ \
		-e CGO_ENABLED=0 \
		-e GOPATH=/go \
		-w /go/src/kafka-topics \
			golang:1.7.1-alpine \
				go build

push:
	docker push krkr/kafka-topics
