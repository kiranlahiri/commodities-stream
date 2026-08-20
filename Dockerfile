FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o collector ./example.go


FROM gcr.io/distroless/static-debian12

WORKDIR /app

COPY --from=builder /app/collector /app/collector

ENTRYPOINT ["/app/collector"]