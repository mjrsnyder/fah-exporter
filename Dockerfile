FROM python:3.8-alpine

RUN mkdir /app

ADD requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

ADD main.py /app/main.py

EXPOSE 9361

CMD ["python", "/app/main.py"]