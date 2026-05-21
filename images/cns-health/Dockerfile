# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

################################################################################
##                               BUILD ARGS                                   ##
################################################################################
ARG GOLANG_IMAGE=golang:1.26.2

# Distroless gives us a 2-MB final image with no shell/package manager. The
# cns-health binary is a single static Go binary embedding its own templates,
# so there is nothing else to ship.
ARG BASE_IMAGE=gcr.io/distroless/static:nonroot

################################################################################
##                              BUILD STAGE                                   ##
################################################################################
FROM ${GOLANG_IMAGE} AS builder

ARG VERSION=unknown
ARG GOPROXY

WORKDIR /build

COPY go.mod go.sum ./
COPY pkg/    pkg/
COPY cmd/    cmd/

ENV CGO_ENABLED=0
ENV GOPROXY=${GOPROXY:-https://proxy.golang.org}

RUN go build \
      -a \
      -ldflags="-w -s -extldflags=static -X main.version=${VERSION}" \
      -o cns-health \
      ./cmd/cns-health

################################################################################
##                               MAIN STAGE                                   ##
################################################################################
FROM ${BASE_IMAGE}

ARG GIT_COMMIT
LABEL git_commit=$GIT_COMMIT
LABEL org.opencontainers.image.title="cns-health"
LABEL org.opencontainers.image.description="Agentic observability container for vSphere CSI driver"
LABEL org.opencontainers.image.source="https://github.com/kubernetes-sigs/vsphere-csi-driver"
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /build/cns-health /bin/cns-health

USER 65532:65532

EXPOSE 8080 2114
ENTRYPOINT ["/bin/cns-health"]
