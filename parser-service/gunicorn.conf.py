import multiprocessing
import os

# Server socket
bind = f"0.0.0.0:{os.environ.get('PORT', '8080')}"
backlog = 2048

# Worker processes
workers = 1  # Single worker to avoid metadata service issues
worker_class = "sync"
worker_connections = 1000
timeout = 300  # 5 minutes - enough for file processing
keepalive = 5

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"
capture_output = True

# Process naming
proc_name = "parser-service"

# Server mechanics
daemon = False
pidfile = None
umask = 0
user = None
group = None
tmp_upload_dir = None

# Increase graceful timeout for Cloud Run
graceful_timeout = 30
