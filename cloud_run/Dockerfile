FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy required files
COPY requirements.txt .
COPY main.py .
COPY utils.py .
COPY keys/service-account-key.json /secrets/service-account-key.json

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV GOOGLE_APPLICATION_CREDENTIALS=/secrets/service-account-key.json

# Run the application
CMD ["python", "main.py"] 