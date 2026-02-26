"""Tests for configuration module."""

from pfm.config import Settings, get_settings


def test_settings_defaults():
    settings = Settings()
    assert settings.log_level == "INFO"
    assert str(settings.database_path) == "data/pfm.db"
    assert settings.okx_api_key.get_secret_value() == ""


def test_settings_secret_repr():
    settings = Settings(okx_api_key="super-secret-key")  # type: ignore[arg-type]
    repr_str = repr(settings)
    assert "super-secret-key" not in repr_str


def test_get_settings_cached():
    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2
