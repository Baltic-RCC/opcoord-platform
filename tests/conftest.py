"""Shared test setup.

Settings objects are created at import time (e.g. ``conf = ElasticSettings()``),
so dummy environment variables must exist before any project module is imported.
Tests never connect to real services.
"""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(__file__).resolve().parent / "data"
SERVICES = ("card_publicator", "card_retriever", "business_data_exchange")

# Dummy settings (env vars take precedence over config/.env)
os.environ.update({
    "ELASTIC_HOST": "http://localhost:9200",
    "ELASTIC_API_KEY": "test",
    "RMQ_HOST": "localhost",
    "RMQ_USERNAME": "test",
    "RMQ_PASSWORD": "test",
    "OPFAB_HOST": "http://localhost",
    "OPFAB_USERNAME": "test",
    "OPFAB_PASSWORD": "test",
    "MINIO_HOST": "localhost:9000",
    "MINIO_USERNAME": "test",
    "MINIO_API_KEY": "test",
    "LOGS_ELASTIC_HANDLER": "false",
})


def _activate_service(service: str) -> None:
    """Make a service's flat imports (``import settings``, ``import builders``) resolve to it.

    Every service has its own ``settings.py``/``handlers.py``, so modules loaded
    from another service are dropped from the import cache first.
    """
    service_dir = ROOT / service
    for name, module in list(sys.modules.items()):
        module_file = getattr(module, "__file__", None) or ""
        if any(module_file.startswith(str(ROOT / other) + os.sep) for other in SERVICES if other != service):
            del sys.modules[name]
    if str(service_dir) in sys.path:
        sys.path.remove(str(service_dir))
    sys.path.insert(0, str(service_dir))


def pytest_collectstart(collector):
    if isinstance(collector, pytest.Module):
        service = collector.path.parent.name
        if service in SERVICES:
            _activate_service(service)


@pytest.fixture
def data_dir() -> Path:
    return DATA_DIR


@pytest.fixture
def nc_sar_xml() -> str:
    return (DATA_DIR / "nc_sar.xml").read_text(encoding="utf-8")


@pytest.fixture
def nc_ras_xml() -> str:
    return (DATA_DIR / "nc_ras.xml").read_text(encoding="utf-8")
