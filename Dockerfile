FROM python:3.8.12

USER root

ADD ./task2.py  /usr/local/source/

WORKDIR /usr/local/source

# RUN pip3 install json
RUN python -m pip install paho-mqtt --upgrade
RUN python -m pip install pika --upgrade
RUN python -m pip install pandas --upgrade

CMD python3 ./task2.py