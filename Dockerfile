# Official Python slim image - lightweight and fast
FROM python:3.11-slim

# Install ffmpeg (includes ffprobe) AND git (needed for pip to install yt-dlp from git)
RUN apt-get update && apt-get install -y ffmpeg git && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all your code
COPY . .

# Run the app with dynamic port
CMD uvicorn main:app --host 0.0.0.0 --port $PORT