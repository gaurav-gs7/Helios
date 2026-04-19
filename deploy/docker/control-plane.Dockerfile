FROM golang:1.26 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/control-plane ./cmd/control-plane

FROM gcr.io/distroless/base-debian12
COPY --from=build /out/control-plane /control-plane
ENTRYPOINT ["/control-plane"]
