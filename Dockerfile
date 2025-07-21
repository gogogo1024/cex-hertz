# --- 构建阶段 ---
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o app

# --- 运行阶段 ---
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/app .
COPY output output
EXPOSE 8080 8081
CMD ["./app"]

