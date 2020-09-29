FROM python:3.8-buster

ENV TZ=Europe/Moscow \
    PYTHONPATH=${PYTHONPATH}:/app

WORKDIR /app
RUN pip3 install --no-cache-dir --quiet --upgrade pip bssbridge

STOPSIGNAL SIGINT
ENTRYPOINT ["bb"]
CMD ["help"]
