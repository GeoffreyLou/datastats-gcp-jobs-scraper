FROM python:3.11-slim

COPY . /app
WORKDIR /app

# Install uv
RUN pip install uv
RUN uv venv && uv sync
ENV PATH="app/.venv/bin:$PATH"

# Run Python with uv
ENTRYPOINT ["uv", "run", "main.py"]
