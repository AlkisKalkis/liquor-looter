FROM python:3.11-alpine

ADD app .
ADD requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "./main.py"]