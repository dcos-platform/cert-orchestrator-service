from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "cert-orchestrator-service"
    database_url: str = "postgresql+psycopg://localhost:5432/cert_orchestrator"
    rabbitmq_url: str = "amqp://localhost/"
    incoming_queue: str = "certificate.lifecycle.events"
    completion_queue: str = "certificate.lifecycle.completions"
    max_retries: int = 3
    backoff_seconds: int = 5

    model_config = SettingsConfigDict(env_prefix="CERT_ORCH_", extra="ignore")


settings = Settings()
