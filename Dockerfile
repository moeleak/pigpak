FROM golang:1.22-alpine AS build
WORKDIR /app
RUN apk add --no-cache ca-certificates
ENV GOFLAGS=-mod=mod
COPY go.mod ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 go build -o /bin/pigpak ./cmd/pigpak

FROM gcr.io/distroless/base-debian12
WORKDIR /app
ENV DATA_DIR=/data
COPY --from=build /bin/pigpak /usr/local/bin/pigpak
VOLUME ["/data"]
EXPOSE 8081
CMD ["pigpak"]
