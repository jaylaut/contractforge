class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__")

    app_env: Literal["dev", "test", "prod"] = "dev"
    debug: bool = True
    database_url: PostgresDsn          # postgresql+asyncpg://...
    redis_url: RedisDsn
    minio_endpoint: str
    minio_access_key: SecretStr
    llm_api_key: SecretStr | None      # M1 前允许为空
    log_level: str = "INFO"

    @field_validator("database_url")
    def must_use_asyncpg(cls, v):      # 生产坑：同步驱动混入异步栈
        if "asyncpg" not in str(v): raise ValueError("必须使用 asyncpg 驱动")
        return v

settings = Settings()   # 导入即校验，配置错在启动第一秒炸