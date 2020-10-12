FROM python:3.7-alpine

RUN mkdir /app
WORKDIR /app
ADD . /app/
RUN pip install -r requirements.txt

CMD ["python", "/app/main.py"]