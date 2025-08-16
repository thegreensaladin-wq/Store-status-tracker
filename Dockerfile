# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive

# System deps for Chromium + fonts + certificates
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    ca-certificates \
    fonts-liberation \
    fonts-noto-color-emoji \
    fonts-freefont-ttf \
    libnss3 \
    libasound2 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libdrm2 \
    libgbm1 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libxshmfence1 \
    libatk1.0-0 \
    libpango-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    libx11-6 \
    libxext6 \
    libxfixes3 \
    libxcb1 \
    libxrender1 \
    libxi6 \
    libxcursor1 \
    libxss1 \
  && rm -rf /var/lib/apt/lists/*

# Help some tools find Chrome
ENV CHROME_BIN=/usr/bin/chromium \
    CHROMIUM_BIN=/usr/bin/chromium
RUN ln -sf /usr/bin/chromium /usr/bin/google-chrome || true

WORKDIR /app

# Install Python deps first for layer caching
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy application code (including service_account.json if present)
COPY . .

# Default to the 15-minute loop; for Render cron we'll override the command
CMD ["python", "run_every_15m.py"] 