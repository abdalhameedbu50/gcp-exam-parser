
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Run the application
CMD exec gunicorn --bind :$PORT --workers 1 --threads 4 --timeout 3600 main:app
