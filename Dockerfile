FROM scratch
COPY LICENSE /LICENSE
COPY --chmod=0755 bin/node-executor /node-executor
ENTRYPOINT ["/node-executor"]
