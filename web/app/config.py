from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg://mediabridge:mediabridge@db:5432/mediabridge"
    movies_root: str = "/mediafiles/Movies"
    media_root: str = "/mediafiles"
    secret_key: str = "dev-only-insecure-secret-key"
    rabbitmq_default_user: str = "mediabridge"
    rabbitmq_default_pass: str = "mediabridge"
    rabbitmq_host: str = "rabbitmq"
    rabbitmq_port: int = 5672
    terminator_api_key: str = ""

    class Config:
        env_file = ".env"

    @property
    def broker_url(self) -> str:
        return f"amqp://{self.rabbitmq_default_user}:{self.rabbitmq_default_pass}@{self.rabbitmq_host}:{self.rabbitmq_port}//"


settings = Settings()

VIDEO_EXTENSIONS = {"mp4", "m4v", "mkv", "avi", "mov", "wmv"}
