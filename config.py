import os

class Config:
    """Базовая конфигурация"""
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    DATABASE = 'auto_parts.db'
    UPLOAD_FOLDER = 'uploads'
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024
    REQUIRE_AUTH = False  # По умолчанию без авторизации

class DevelopmentConfig(Config):
    """Конфигурация для разработки"""
    DEBUG = True
    REQUIRE_AUTH = False  # Без авторизации на локальной машине

class ProductionConfig(Config):
    """Конфигурация для продакшена"""
    DEBUG = False
    REQUIRE_AUTH = True   # С авторизацией на сервере
    SECRET_KEY = os.environ.get('SECRET_KEY')  # Обязательно из переменных окружения

config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}