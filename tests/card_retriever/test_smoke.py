import handlers
import settings


def test_settings_load():
    conf = settings.get_settings()
    assert conf.retriever.cards_index


def test_handlers_import():
    assert handlers.RootRetrievingHandler
