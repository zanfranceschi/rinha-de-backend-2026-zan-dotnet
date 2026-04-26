FROM mcr.microsoft.com/dotnet/sdk:11.0-preview-alpine AS builder
RUN apk add --no-cache clang zlib-dev musl-dev
WORKDIR /app
COPY src/rinha2026.csproj src/
RUN dotnet restore src/rinha2026.csproj -r linux-musl-x64
COPY src/ src/
RUN dotnet publish src/rinha2026.csproj -c Release -r linux-musl-x64 -o /app/publish

FROM alpine:3.21
WORKDIR /app
COPY --from=builder /app/publish/rinha2026 .
COPY resources/references.bin resources/mcc_risk.json resources/normalization.json /resources/
ENV RESOURCES_PATH=/resources
CMD ["./rinha2026"]