FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY extender.py gate_controller.py metrics.py scoring.py signal_loader.py temporal.py workload_classifier.py ./
CMD ["uvicorn", "extender:app", "--host", "0.0.0.0", "--port", "8080"]
