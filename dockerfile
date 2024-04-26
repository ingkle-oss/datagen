FROM python:3.11

ENV APPNAME datagen

COPY requirements.txt /opt/${APPNAME}/requirements.txt
RUN pip3 install -r /opt/${APPNAME}/requirements.txt

COPY src /opt/${APPNAME}/

WORKDIR /opt/${APPNAME}/

CMD ["python3"]
