# --- Base image ---
FROM python:3.11-slim

# --- Set working directory ---
WORKDIR /app

# --- Install system dependencies for Playwright ---
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    curl \
    unzip \
    fonts-liberation \
    libasound2 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libgtk-3-0 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libx11-xcb1 \
    libnss3 \
    libxss1 \
    libxtst6 \
    libglu1-mesa \
    && rm -rf /var/lib/apt/lists/*

# --- Install Python dependencies ---
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Install Playwright Browsers ---
RUN pip install playwright && playwright install --with-deps chromium

# --- Copy project files ---
COPY . .

# --- Expose port for FastAPI (Railway sets $PORT) ---
EXPOSE 8000

# --- Start FastAPI with Uvicorn ---
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
