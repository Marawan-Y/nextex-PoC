FROM python:3.12-slim

WORKDIR /app

COPY cloud_common/requirements.txt /app/cloud_common/requirements.txt
RUN pip install --no-cache-dir -r /app/cloud_common/requirements.txt

COPY cloud_common /app/cloud_common
COPY cloud_consumer /app/cloud_consumer

ENV PYTHONUNBUFFERED=1

CMD ["python", "cloud_consumer/worker.py"]
