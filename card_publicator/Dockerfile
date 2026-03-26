# Define base container image (platform has to be defined if using with ARM64 architecture)
ARG PYTHON_BASE_TAG=0.0.1
FROM balticrcc/opcoord-base:${PYTHON_BASE_TAG}
#FROM --platform=linux/amd64 python:3.11-slim

# Set working directory
WORKDIR /app

# Copy files to container
COPY config/ ./config/
COPY integrations/ ./integrations/
COPY card_publicator/ ./card_publicator/

# Change ownership to non-root user
RUN chown -R appuser:appuser /app
USER appuser

# Start process
ENV PYTHONPATH="${PYTHONPATH}:/app"
ENTRYPOINT ["python"]
CMD ["card_publicator/worker.py"]
#ENTRYPOINT ["/bin/bash"]