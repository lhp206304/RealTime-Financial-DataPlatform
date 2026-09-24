# 功能：多阶段构建（对齐 api/Dockerfile 的角色）
# 阶段1：编译（alpine 自带 go 工具链，go.sum 缓存依赖）
# 版本必须 >= go.mod 声明的 go 版本（当前 1.27），否则 go mod download 报 requires go >= 1.27
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download          # 先拷依赖清单再下载：源码不变时这层走缓存，构建快
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/server ./cmd/server
# CGO_ENABLED=0：纯静态二进制（mysql 驱动无 cgo 依赖），distroless 才能跑

# 阶段2：运行（distroless 无 shell 无包管理器，攻击面最小）
FROM gcr.io/distroless/static-debian12
COPY --from=build /bin/server /bin/server
EXPOSE 8001
ENTRYPOINT ["/bin/server"]