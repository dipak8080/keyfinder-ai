# Official Python slim image - lightweight and fast
FROM python:3.11-slim

# Install ffmpeg (includes ffprobe) system-wide
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all your code
COPY . .

# Tell Railway which port to use (dynamic)
ENV PORT=8000

# Run the app
CMD uvicorn main:app --host 0.0.0.0 --port $PORT