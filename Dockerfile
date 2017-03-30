FROM python:2.7-alpine

RUN apk add --no-cache py-requests
ADD . /app
VOLUME /app/html
WORKDIR /app
ENTRYPOINT ["/app/search_metrics.py"]
