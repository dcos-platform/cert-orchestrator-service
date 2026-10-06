import urllib.parse

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL as SQLAlchemyURL

DB_SCHEME = "postgresql+psycopg"
MQ_SCHEME = "amqp"


class Settings(BaseSettings):
    app_name: str = "cert-orchestrator-service"
    database_url: str | None = None
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "cert_orchestrator"
    postgres_user: str = "dcos"
    postgres_password: SecretStr = SecretStr("changeme")
    rabbitmq_url: str | None = None
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "dcos"
    rabbitmq_password: SecretStr = SecretStr("changeme")
    rabbitmq_vhost: str = "/"
    incoming_queue: str = "certificate.lifecycle.events"
    completion_queue: str = "certificate.lifecycle.completions"
    dead_letter_exchange: str = "cert.orchestrator.dlx"
    dead_letter_queue: str = "cert.orchestrator.dlq"
    wait_queue_prefix: str = "cert.orchestrator.wait"
    max_retries: int = 3
    max_delivery_attempts: int = 3
    backoff_seconds: int = 5

    model_config = SettingsConfigDict(env_prefix="CERT_ORCH_", extra="ignore")

    @property
    def resolved_database_url(self) -> str:
        """Return database URL from override or construct from components."""
        if self.database_url and self.database_url.strip():
            return self.database_url
        url = SQLAlchemyURL.create(
            drivername=DB_SCHEME,
            username=self.postgres_user,
            password=self.postgres_password.get_secret_value(),
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        )
        return url.render_as_string(hide_password=False)

    @property
    def resolved_rabbitmq_url(self) -> str:
        """Return RabbitMQ URL from override or construct from components."""
        if self.rabbitmq_url and self.rabbitmq_url.strip():
            return self.rabbitmq_url
        user = urllib.parse.quote(self.rabbitmq_user, safe="")
        password = urllib.parse.quote(self.rabbitmq_password.get_secret_value(), safe="")
        vhost = urllib.parse.quote(self.rabbitmq_vhost, safe="")
        return f"{MQ_SCHEME}://{user}:{password}@{self.rabbitmq_host}:{self.rabbitmq_port}/{vhost}"


settings = Settings()
