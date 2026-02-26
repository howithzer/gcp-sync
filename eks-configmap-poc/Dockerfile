FROM --platform=linux/amd64 python:3.9-slim

WORKDIR /app

# The POC script only relies on standard libraries
# (os, time, json, signal, sys), so we don't need a requirements.txt

COPY app.py .

CMD ["python", "-u", "app.py"]
