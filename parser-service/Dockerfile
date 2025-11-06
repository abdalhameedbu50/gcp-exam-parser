FROM python:3.11-slim
RUN pip install --upgrade pip==24.0
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
COPY gunicorn.conf.py .
ENV PORT=8080
# Increased timeout and graceful shutdown for metadata server access
CMD ["gunicorn", "-c", "gunicorn.conf.py", "--timeout", "300", "--graceful-timeout", "60", "main:app"]

