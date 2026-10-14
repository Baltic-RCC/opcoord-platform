import handlers
import settings


def test_settings_load():
    conf = settings.get_settings()
    assert conf.exchange.retention_days >= 0


def test_handlers_import():
    assert handlers.BusinessDataExchangeHandler
