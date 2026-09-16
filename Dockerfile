FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -o /dispatch ./cmd/dispatch
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /dispatch /dispatch
ENTRYPOINT ["/dispatch"]
