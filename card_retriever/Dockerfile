# Define base container image (platform has to be defined if using with ARM64 architecture)
# Global argument for base image for workers, can be overridden when building with --build-arg
ARG BASE_IMAGE=balticrcc/opcoord-base:latest
FROM ${BASE_IMAGE}
#FROM --platform=linux/amd64 python:3.11-slim

# Set working directory
WORKDIR /app

# Copy files to container
COPY config/ ./config/
COPY integrations/ ./integrations/
COPY card_retriever/ ./card_retriever/

# Change ownership to non-root user
RUN chown -R appuser:appuser /app
USER appuser

# Start process
ENV PYTHONPATH="${PYTHONPATH}:/app"
ENTRYPOINT ["python"]
CMD ["card_retriever/worker.py"]
#ENTRYPOINT ["/bin/bash"]