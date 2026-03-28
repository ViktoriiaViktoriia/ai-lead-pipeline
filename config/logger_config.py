import logging
import os
from logging.handlers import RotatingFileHandler

# Create logs directory if it doesn't exist
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = "logs/app.log"

# Set up rotating log handler (max size of 10MB, 3 backups)
# RotatingFileHandler prevents log file from growing indefinitely
handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=3)

# Read log level from environment variable, default to INFO
log_level = os.getenv("LOG_LEVEL", "INFO").upper()

# Configure logging
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),  # Default to INFO if invalid level
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        handler,
        logging.StreamHandler()
    ]
)

# Export the shared logger
logger = logging.getLogger(__name__)
