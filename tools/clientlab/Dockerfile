# syntax=docker/dockerfile:1.7
# Dockerfile builds an isolated disposable container for T6 live-mode clientlab runs.
# Live mode lets real upstream models choose shell commands, so client_lab_scenario_test.go
# refuses live execution unless TOKENROUTER_CLIENTLAB_CONTAINER=1 is set by ENTRYPOINT here.

ARG GOLANG_IMAGE=golang:1.27.4-bookworm
ARG NODE_IMAGE=node:24-bookworm-slim

FROM ${GOLANG_IMAGE} AS go-toolchain

FROM ${NODE_IMAGE}

RUN apt-get update && \
    apt-get install -y --no-install-recommends bash ca-certificates git procps && \
    rm -rf /var/lib/apt/lists/*

COPY --from=go-toolchain /usr/local/go /usr/local/go
ENV PATH="/usr/local/go/bin:${PATH}"

# Run as an unprivileged user because Claude Code rejects bypassPermissions under UID 0.
RUN useradd --create-home --shell /bin/bash clientlab

WORKDIR /workspace
COPY --chown=clientlab:clientlab tools/clientlab/package.json tools/clientlab/package-lock.json ./tools/clientlab/

USER clientlab
RUN cd /workspace/tools/clientlab && npm ci --no-audit --no-fund

COPY --chown=clientlab:clientlab plugin-sdk/go/go.mod plugin-sdk/go/go.sum ./plugin-sdk/go/
COPY --chown=clientlab:clientlab backend/go.mod backend/go.sum ./backend/
RUN go -C /workspace/backend mod download

COPY --chown=clientlab:clientlab .dockerignore ./.dockerignore
COPY --chown=clientlab:clientlab tools/clientlab/Dockerfile ./tools/clientlab/Dockerfile
COPY --chown=clientlab:clientlab tools/clientlab/templates ./tools/clientlab/templates
COPY --chown=clientlab:clientlab plugin-sdk/go ./plugin-sdk/go
COPY --chown=clientlab:clientlab backend ./backend
RUN go -C /workspace/backend test -c ./internal/app -o /tmp/clientlab-prewarm.test && rm -f /tmp/clientlab-prewarm.test

ENV TOKENROUTER_CLIENTLAB=1

ENTRYPOINT ["/usr/bin/env", "TOKENROUTER_CLIENTLAB_CONTAINER=1"]
CMD ["go", "-C", "backend", "test", "./internal/app", "-run", "^TestClientLab", "-count=1", "-timeout", "30m", "-v"]
