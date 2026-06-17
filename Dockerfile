FROM golang:1.24-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY oaix_gateway/web ./oaix_gateway/web

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/oaix-gateway ./cmd/oaix-gateway && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/oaix-worker ./cmd/oaix-worker && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/oaix-migrate ./cmd/oaix-migrate

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
ENV OAIX_AUTO_MIGRATE_ON_STARTUP=true

COPY --from=build /out/oaix-gateway /usr/local/bin/oaix-gateway
COPY --from=build /out/oaix-worker /usr/local/bin/oaix-worker
COPY --from=build /out/oaix-migrate /usr/local/bin/oaix-migrate
COPY --from=build /src/oaix_gateway/web /app/oaix_gateway/web

EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/oaix-gateway"]
