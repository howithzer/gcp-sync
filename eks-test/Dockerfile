FROM python:3.9-slim

WORKDIR /app

RUN pip install watchdog

COPY app.py .

CMD ["python", "-u", "app.py"]
