# One image, every Herald server: the subcommand picks the role.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /herald ./cmd/herald

FROM alpine:3.22
COPY --from=build /herald /usr/local/bin/herald
COPY deploy/partitions.yaml /etc/herald/partitions.yaml
ENTRYPOINT ["herald"]
