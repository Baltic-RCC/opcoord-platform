import handlers
import settings


def test_settings_load():
    conf = settings.get_settings()
    assert conf.publicator.cards_index


def test_handlers_import():
    assert handlers.RootPublicationHandler
