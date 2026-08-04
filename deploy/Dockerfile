# Multi-stage: build a static binary, then ship it on a minimal base.
# CMD selects which binary to build, so node and proxy share one Dockerfile.
ARG CMD=node

FROM golang:1.24-alpine AS build
WORKDIR /src

# No external dependencies, so go.mod alone is the whole dependency graph and
# this layer stays cached across source changes.
COPY go.mod ./
RUN go mod download

COPY . .
ARG CMD
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

# Alpine rather than distroless or scratch: a shell is worth more than the 5 MB
# during a live demo, when being able to exec in and look around beats image
# size. Production would use distroless/static.
FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/app /app
USER app
EXPOSE 8081
ENTRYPOINT ["/app"]
