FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy required files
COPY requirements.txt .
COPY main.py .
COPY utils.py .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the application
CMD ["python", "main.py"] 