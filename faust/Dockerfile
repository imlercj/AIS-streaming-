FROM python:3.8

WORKDIR /code
COPY . .
RUN pip install -r requirements.txt

#COPY ../ais_receiver/ais_receiver.py ais_receiver.py
#COPY ../config/ships.json ships.json

RUN chown 1001:1001 -R /code

USER 1001

CMD ["python", "faust.py"]


