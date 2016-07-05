FROM scratch
COPY kafka-topics /kafka-topics
ENTRYPOINT ["/kafka-topics"]