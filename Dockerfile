# Two build stages: the dashboard, then the Go binary that embeds it. The
# final image is a single static binary on distroless.
FROM node:22-alpine AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
# Build into ./dist inside this stage rather than the repo-relative outDir
# vite.config.ts uses for local development, so nothing writes outside the
# project root and this stage stands alone.
RUN npx tsc --noEmit \
 && npx vite build --outDir dist --emptyOutDir \
 && test -f dist/index.html \
 && ls -l dist

FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# internal/web/dist is .dockerignore'd, so this is the only thing that fills
# it and //go:embed always sees a freshly built dashboard.
COPY --from=web /web/dist ./internal/web/dist
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /dispatch ./cmd/dispatch

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /dispatch /dispatch
EXPOSE 8080
ENTRYPOINT ["/dispatch"]
