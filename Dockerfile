FROM python:3.10-alpine

RUN mkdir /app
RUN apk add redis
RUN pip install flask redis

WORKDIR /app

CMD ["flask", "run", "--host", "0.0.0.0", "--port", "8080"]

