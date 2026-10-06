import os
import urllib.parse

import pytest
from sqlalchemy.engine import make_url

from cert_orchestrator.config import Settings
from cert_orchestrator.messaging import rabbitmq as rabbitmq_module
from cert_orchestrator.messaging.rabbitmq import RabbitMQClient


@pytest.fixture
def clean_env(monkeypatch):
    """Remove all CERT_ORCH_* variables for isolated testing."""
    for key in list(os.environ.keys()):
        if key.startswith("CERT_ORCH_"):
            monkeypatch.delenv(key, raising=False)
    return monkeypatch


class TestDatabaseUrlDefaults:
    """Test database URL defaults."""

    def test_defaults_produce_correct_url(self, clean_env):
        """Defaults produce postgresql+psycopg://dcos:changeme@localhost:5432/cert_orchestrator."""
        for key in (
            "CERT_ORCH_POSTGRES_HOST",
            "CERT_ORCH_POSTGRES_PORT",
            "CERT_ORCH_POSTGRES_DB",
            "CERT_ORCH_POSTGRES_USER",
            "CERT_ORCH_POSTGRES_PASSWORD",
            "CERT_ORCH_DATABASE_URL",
        ):
            clean_env.delenv(key, raising=False)

        settings = Settings()
        assert settings.resolved_database_url == "postgresql+psycopg://dcos:changeme@localhost:5432/cert_orchestrator"

    def test_postgres_host_override(self, clean_env):
        """CERT_ORCH_POSTGRES_HOST changes the host."""
        clean_env.setenv("CERT_ORCH_POSTGRES_HOST", "db.example.com")
        settings = Settings()
        url = make_url(settings.resolved_database_url)
        assert url.host == "db.example.com"

    def test_postgres_port_override(self, clean_env):
        """CERT_ORCH_POSTGRES_PORT changes the port."""
        clean_env.setenv("CERT_ORCH_POSTGRES_PORT", "5433")
        settings = Settings()
        url = make_url(settings.resolved_database_url)
        assert url.port == 5433

    def test_postgres_db_override(self, clean_env):
        """CERT_ORCH_POSTGRES_DB changes the database."""
        clean_env.setenv("CERT_ORCH_POSTGRES_DB", "mydb")
        settings = Settings()
        url = make_url(settings.resolved_database_url)
        assert url.database == "mydb"

    def test_postgres_user_override(self, clean_env):
        """CERT_ORCH_POSTGRES_USER changes the user."""
        clean_env.setenv("CERT_ORCH_POSTGRES_USER", "admin")
        settings = Settings()
        url = make_url(settings.resolved_database_url)
        assert url.username == "admin"

    def test_postgres_password_override(self, clean_env):
        """CERT_ORCH_POSTGRES_PASSWORD changes the password."""
        clean_env.setenv("CERT_ORCH_POSTGRES_PASSWORD", "secret123")
        settings = Settings()
        url = make_url(settings.resolved_database_url)
        assert url.password == "secret123"

    def test_database_url_override_wins(self, clean_env):
        """CERT_ORCH_DATABASE_URL wins over components."""
        clean_env.setenv("CERT_ORCH_DATABASE_URL", "postgresql+psycopg://user:pass@host:9999/db")
        clean_env.setenv("CERT_ORCH_POSTGRES_HOST", "ignored")
        settings = Settings()
        assert settings.resolved_database_url == "postgresql+psycopg://user:pass@host:9999/db"

    def test_database_url_empty_string_treated_as_unset(self, clean_env):
        """Empty CERT_ORCH_DATABASE_URL is treated as unset."""
        clean_env.setenv("CERT_ORCH_DATABASE_URL", "")
        clean_env.setenv("CERT_ORCH_POSTGRES_HOST", "myhost")
        settings = Settings()
        url = make_url(settings.resolved_database_url)
        assert url.host == "myhost"

    def test_password_with_special_chars_roundtrips(self, clean_env):
        """Password with @, :, / and % round-trips correctly."""
        password = "p@ss:w/rd%special"
        clean_env.setenv("CERT_ORCH_POSTGRES_PASSWORD", password)
        settings = Settings()
        url = make_url(settings.resolved_database_url)
        assert url.password == password


class TestRabbitmqUrlDefaults:
    """Test RabbitMQ URL defaults."""

    def test_defaults_produce_correct_url(self, clean_env):
        """Defaults produce amqp://dcos:changeme@localhost:5672/%2F."""
        for key in (
            "CERT_ORCH_RABBITMQ_HOST",
            "CERT_ORCH_RABBITMQ_PORT",
            "CERT_ORCH_RABBITMQ_USER",
            "CERT_ORCH_RABBITMQ_PASSWORD",
            "CERT_ORCH_RABBITMQ_VHOST",
            "CERT_ORCH_RABBITMQ_URL",
        ):
            clean_env.delenv(key, raising=False)

        settings = Settings()
        assert settings.resolved_rabbitmq_url == "amqp://dcos:changeme@localhost:5672/%2F"

    def test_rabbitmq_host_override(self, clean_env):
        """CERT_ORCH_RABBITMQ_HOST changes the host."""
        clean_env.setenv("CERT_ORCH_RABBITMQ_HOST", "mq.example.com")
        settings = Settings()
        url_obj = urllib.parse.urlparse(settings.resolved_rabbitmq_url)
        assert url_obj.hostname == "mq.example.com"

    def test_rabbitmq_port_override(self, clean_env):
        """CERT_ORCH_RABBITMQ_PORT changes the port."""
        clean_env.setenv("CERT_ORCH_RABBITMQ_PORT", "5673")
        settings = Settings()
        url_obj = urllib.parse.urlparse(settings.resolved_rabbitmq_url)
        assert url_obj.port == 5673

    def test_rabbitmq_user_override(self, clean_env):
        """CERT_ORCH_RABBITMQ_USER changes the user."""
        clean_env.setenv("CERT_ORCH_RABBITMQ_USER", "admin")
        settings = Settings()
        url_obj = urllib.parse.urlparse(settings.resolved_rabbitmq_url)
        decoded_user = urllib.parse.unquote(url_obj.username)
        assert decoded_user == "admin"

    def test_rabbitmq_password_override(self, clean_env):
        """CERT_ORCH_RABBITMQ_PASSWORD changes the password."""
        clean_env.setenv("CERT_ORCH_RABBITMQ_PASSWORD", "secret123")
        settings = Settings()
        url_obj = urllib.parse.urlparse(settings.resolved_rabbitmq_url)
        decoded_password = urllib.parse.unquote(url_obj.password)
        assert decoded_password == "secret123"

    def test_rabbitmq_vhost_override_default_slash(self, clean_env):
        """Default vhost / renders as %2F."""
        for key in (
            "CERT_ORCH_RABBITMQ_HOST",
            "CERT_ORCH_RABBITMQ_PORT",
            "CERT_ORCH_RABBITMQ_USER",
            "CERT_ORCH_RABBITMQ_PASSWORD",
            "CERT_ORCH_RABBITMQ_VHOST",
            "CERT_ORCH_RABBITMQ_URL",
        ):
            clean_env.delenv(key, raising=False)

        settings = Settings()
        assert settings.resolved_rabbitmq_url == "amqp://dcos:changeme@localhost:5672/%2F"

    def test_rabbitmq_vhost_override_custom(self, clean_env):
        """Custom vhost renders correctly."""
        for key in (
            "CERT_ORCH_RABBITMQ_HOST",
            "CERT_ORCH_RABBITMQ_PORT",
            "CERT_ORCH_RABBITMQ_USER",
            "CERT_ORCH_RABBITMQ_PASSWORD",
            "CERT_ORCH_RABBITMQ_VHOST",
            "CERT_ORCH_RABBITMQ_URL",
        ):
            clean_env.delenv(key, raising=False)
        clean_env.setenv("CERT_ORCH_RABBITMQ_VHOST", "dcos")
        settings = Settings()
        assert settings.resolved_rabbitmq_url == "amqp://dcos:changeme@localhost:5672/dcos"

    def test_rabbitmq_url_override_wins(self, clean_env):
        """CERT_ORCH_RABBITMQ_URL wins over components."""
        clean_env.setenv("CERT_ORCH_RABBITMQ_URL", "amqp://user:pass@host:9999/vhost")
        clean_env.setenv("CERT_ORCH_RABBITMQ_HOST", "ignored")
        settings = Settings()
        assert settings.resolved_rabbitmq_url == "amqp://user:pass@host:9999/vhost"

    def test_rabbitmq_url_empty_string_treated_as_unset(self, clean_env):
        """Empty CERT_ORCH_RABBITMQ_URL is treated as unset."""
        clean_env.setenv("CERT_ORCH_RABBITMQ_URL", "")
        clean_env.setenv("CERT_ORCH_RABBITMQ_HOST", "myhost")
        settings = Settings()
        url_obj = urllib.parse.urlparse(settings.resolved_rabbitmq_url)
        assert url_obj.hostname == "myhost"

    def test_password_with_special_chars_roundtrips(self, clean_env):
        """Password with @, :, /, and % round-trips correctly."""
        password = "p@ss:w/rd%special"
        clean_env.setenv("CERT_ORCH_RABBITMQ_PASSWORD", password)
        settings = Settings()
        url_obj = urllib.parse.urlparse(settings.resolved_rabbitmq_url)
        decoded_password = urllib.parse.unquote(url_obj.password)
        assert decoded_password == password


class TestSecretHandling:
    """Test that passwords are not leaked."""

    def test_repr_does_not_contain_password(self, clean_env):
        """repr(settings) does not contain the password."""
        clean_env.setenv("CERT_ORCH_POSTGRES_PASSWORD", "mysecret")
        clean_env.setenv("CERT_ORCH_RABBITMQ_PASSWORD", "mysecret")
        settings = Settings()
        repr_str = repr(settings)
        assert "mysecret" not in repr_str

    def test_str_does_not_contain_password(self, clean_env):
        """str(settings) does not contain the password."""
        clean_env.setenv("CERT_ORCH_POSTGRES_PASSWORD", "mysecret")
        clean_env.setenv("CERT_ORCH_RABBITMQ_PASSWORD", "mysecret")
        settings = Settings()
        str_str = str(settings)
        assert "mysecret" not in str_str


class TestRabbitMQClient:
    """Test RabbitMQClient integration."""

    def test_rabbitmq_client_uses_resolved_url(self, clean_env, monkeypatch):
        """RabbitMQClient() with no argument uses settings.resolved_rabbitmq_url."""
        clean_env.setenv("CERT_ORCH_RABBITMQ_HOST", "myhost")
        clean_env.setenv("CERT_ORCH_RABBITMQ_PORT", "5673")
        test_settings = Settings()
        monkeypatch.setattr(rabbitmq_module, "settings", test_settings)
        client = RabbitMQClient()
        assert client.url == test_settings.resolved_rabbitmq_url

    def test_rabbitmq_client_uses_explicit_url(self, clean_env):
        """RabbitMQClient(url="...") uses the provided URL."""
        explicit_url = "amqp://custom:pass@custom.host:9999/vhost"
        client = RabbitMQClient(url=explicit_url)
        assert client.url == explicit_url
