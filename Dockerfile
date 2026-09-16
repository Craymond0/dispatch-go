# The dashboard is built first and copied into the Go build context so the
# binary embeds it; the final image is a single static binary on distroless.
FROM node:22-alpine AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
# vite.config.ts writes to ../internal/web/dist, i.e. /internal/web/dist here.
RUN mkdir -p /internal/web/dist && npm run build && ls -l /internal/web/dist

FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=web /internal/web/dist ./internal/web/dist
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /dispatch ./cmd/dispatch

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /dispatch /dispatch
EXPOSE 8080
ENTRYPOINT ["/dispatch"]
