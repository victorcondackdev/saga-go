# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/sagad ./cmd/sagad \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/sagactl ./cmd/sagactl

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/sagad /out/sagactl /usr/local/bin/
VOLUME /data
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/sagad"]
CMD ["-addr", ":8080", "-store", "wal", "-dir", "/data"]
