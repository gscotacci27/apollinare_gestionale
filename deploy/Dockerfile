FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml requirements.txt ./
RUN uv pip install --system --no-cache -r requirements.txt

COPY src/ src/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=src

EXPOSE 8080

CMD ["uvicorn", "gateway.main:app", "--host", "0.0.0.0", "--port", "8080"]
