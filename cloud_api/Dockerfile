FROM python:3.12-slim

WORKDIR /app

COPY cloud_common/requirements.txt /app/cloud_common/requirements.txt
RUN pip install --no-cache-dir -r /app/cloud_common/requirements.txt

COPY cloud_common /app/cloud_common
COPY cloud_api /app/cloud_api

ENV PYTHONUNBUFFERED=1
EXPOSE 8001

CMD ["uvicorn", "cloud_api.main:app", "--host", "0.0.0.0", "--port", "8001"]
