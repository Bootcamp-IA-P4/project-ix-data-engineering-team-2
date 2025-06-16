FROM python:3

WORKDIR /usr/src/app

COPY . .

RUN pip install -r requirements.txt
RUN chmod +x wait-for-it.sh

CMD [ "python", "./kafka_push.py" ]








