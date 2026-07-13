FROM python:3.12-slim

WORKDIR /app

COPY edge_simulator/requirements.txt /app/edge_simulator/requirements.txt
RUN pip install --no-cache-dir -r /app/edge_simulator/requirements.txt

COPY edge_simulator /app/edge_simulator

ENV PYTHONUNBUFFERED=1
EXPOSE 8002

CMD ["uvicorn", "edge_simulator.main:app", "--host", "0.0.0.0", "--port", "8002"]
