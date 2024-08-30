FROM golang:alpine
LABEL authors="Dissan Uddin Ahmed"

ADD dkv /app/dkv

WORKDIR /app/dkv
RUN go mod download
RUN go build main.go



