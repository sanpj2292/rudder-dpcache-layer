FROM golang:1.17-alpine
RUN apk add --no-cache build-base
RUN mkdir /app
WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go install
ENTRYPOINT [ "rudder-dpcache-layer" ]
EXPOSE 9800