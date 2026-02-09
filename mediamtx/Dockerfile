FROM alpine:3.20

# Instala curl e utilitários básicos
RUN apk --no-cache add curl tar

# Define argumentos de build que indicam arquitetura
ARG TARGETARCH

# Define versão do MediaMTX (ajuste conforme desejado)
ENV MEDIAMTX_VERSION=1.13.1

# Define URL base do repositório
ENV BASE_URL=https://github.com/bluenviron/mediamtx/releases/download

# Cria diretório de trabalho
WORKDIR /app

# Baixa o binário correto com base na arquitetura e extrai
RUN curl -L "${BASE_URL}/v${MEDIAMTX_VERSION}/mediamtx_v${MEDIAMTX_VERSION}_linux_${TARGETARCH}.tar.gz" \
    -o mediamtx.tar.gz && \
    tar -xzf mediamtx.tar.gz && \
    mv mediamtx /usr/local/bin/mediamtx && \
    chmod +x /usr/local/bin/mediamtx && \
    rm mediamtx.tar.gz

# Copie o arquivo de configuração se estiver local
COPY mediamtx.yml /app/mediamtx.yml

# Expõe as portas típicas
EXPOSE 8554
EXPOSE 1935
EXPOSE 8888
EXPOSE 8889
EXPOSE 9997
EXPOSE 8000-9000/udp

# Comando padrão
CMD ["mediamtx", "/app/mediamtx.yml"]