FROM python:3.9-slim-buster

RUN sed -i \
      -e 's/deb.debian.org/archive.debian.org/g' \
      -e 's/security.debian.org/archive.debian.org/g' \
      -e 's/buster\/updates/buster-updates/g' \
      /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 3000

CMD ["python", "server.py"]

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD [ "curl", "-f", "http://localhost:3000/" ]