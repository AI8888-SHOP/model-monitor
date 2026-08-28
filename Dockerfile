FROM golang:1.22-alpine AS build

WORKDIR /src

ARG TARGETOS
ARG TARGETARCH

COPY go.mod go.sum ./
RUN go mod download

COPY main.go main_test.go ./
COPY web ./web

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags='-s -w' -o /out/model-monitor .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=build /out/model-monitor /app/model-monitor

ENV DATA_DIR=/app/data \
    LISTEN_HOST=0.0.0.0 \
    LISTEN_PORT=8020

VOLUME ["/app/data"]
EXPOSE 8020

ENTRYPOINT ["/app/model-monitor"]
