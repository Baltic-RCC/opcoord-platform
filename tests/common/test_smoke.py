from config.integrations import ElasticSettings, MinioSettings


def test_integration_settings_load():
    assert ElasticSettings().host
    assert MinioSettings().username


def test_sample_data_present(nc_sar_xml, nc_ras_xml):
    assert nc_sar_xml.strip()
    assert nc_ras_xml.strip()
