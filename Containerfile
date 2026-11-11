# Container image for the RDF Fusion CLI.
#
# Packages a prebuilt release binary (see misc/ci/build-container.sh), so the build context must
# contain the `rdf-fusion` binary and an empty `rootfs/data` directory with mode 0775. The image is
# not built from source to ship exactly the binary that was tested and released.

# Must provide glibc >= 2.39, libssl3, and libstdc++ (check with `ldd` when changing it).
FROM gcr.io/distroless/cc-debian13:nonroot

ARG RDF_FUSION_VERSION
LABEL org.opencontainers.image.title="rdf-fusion" \
      org.opencontainers.image.version="${RDF_FUSION_VERSION}" \
      org.opencontainers.image.source="https://codeberg.org/tschwarzinger/rdf-fusion" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY rdf-fusion /usr/local/bin/rdf-fusion
# /data is owned by the `nonroot` user (65532) of the distroless image and group 0 with group-write,
# so it stays writable when the image runs as an arbitrary UID in group 0 (e.g., OpenShift). It is
# copied as part of `rootfs` to keep its mode from the build context.
COPY --chown=65532:0 rootfs/ /

WORKDIR /data
EXPOSE 7878
ENTRYPOINT ["/usr/local/bin/rdf-fusion"]
# By default, serve a read-write Delta Lake store in /data. Users can override the arguments.
CMD ["--storage-type", "delta-quads", "--location", "file:///data/", "serve", "--bind", "0.0.0.0:7878"]
