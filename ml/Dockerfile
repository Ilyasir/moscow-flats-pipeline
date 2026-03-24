FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY ml/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ml/ .

CMD ["python", "train.py"]