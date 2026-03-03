FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN crawl4ai-setup
COPY . .
# Railway volumes are configured in the dashboard (mount path /data).
# Set DB_PATH=/data/leads.db in Railway variables so the app uses the volume.
ENV DB_PATH=/data/leads.db
ENV STREAMLIT_SERVER_HEADLESS=true
# Railway injects PORT at runtime; default 8080 for local Docker
EXPOSE 8080
CMD ["sh", "-c", "streamlit run app.py --server.port ${PORT:-8080} --server.address 0.0.0.0 --server.headless true"]
