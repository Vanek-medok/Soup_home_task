
ARG GO_VERSION=1.27.1

FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" \
    -o /out/service ./cmd/service

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" \
    -o /out/harness ./cmd/harness


# Образ API и worker
FROM alpine:3.22 AS service

RUN apk add --no-cache ca-certificates

COPY --from=build /out/service /service

ENTRYPOINT ["/service"]


# Образ chaos harness
FROM alpine:3.22 AS harness

RUN apk add --no-cache ca-certificates docker-cli

COPY --from=build /out/harness /harness

ENTRYPOINT ["/harness"]
