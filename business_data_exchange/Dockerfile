# Define base container image (platform has to be defined if using with ARM64 architecture)
# Global argument for base image for workers, can be overridden when building with --build-arg
ARG BASE_IMAGE=balticrcc/opcoord-base:latest
FROM ${BASE_IMAGE}

# Set working directory
WORKDIR /app

# Copy files to container
COPY config/ ./config/
COPY integrations/ ./integrations/
COPY business_data_exchange/ ./business_data_exchange/

# Change ownership to non-root user
RUN chown -R appuser:appuser /app
USER appuser

# Start process (run-once: schedule via Kubernetes CronJob, once per day
# before 1D CROSA and after consistency check)
ENV PYTHONPATH="${PYTHONPATH}:/app"
ENTRYPOINT ["python"]
CMD ["business_data_exchange/worker.py"]
