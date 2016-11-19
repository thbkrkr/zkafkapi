FROM scratch

COPY zkafkapi /zkafkapi

ENTRYPOINT ["/zkafkapi"]