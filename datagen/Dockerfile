FROM python:3

WORKDIR /usr/src/app

COPY . .

RUN pip install -r requirements.txt

CMD [ "python", "./kafka_push.py" ]

EXPOSE 9101








