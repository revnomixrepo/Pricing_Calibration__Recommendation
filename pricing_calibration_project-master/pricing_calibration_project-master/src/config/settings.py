import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    DEBUG = False
    TESTING = False
    CSRF_ENABLED = True
    SECRET_KEY = 'xxxx-xxx-xxxx'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = True
    PROPAGATE_EXCEPTIONS = True
    JWT_BLACKLIST_ENABLED = True
    JWT_BLACKLIST_TOKEN_CHECKS = [
        "access",
        "refresh",
    ]
    JWT_SECRET_KEY="jose"


class ProductionConfig(Config):
    FLASK_ENV = 'production'
    DEBUG = False
    PORT = 5000


class StagingConfig(Config):
    FLASK_ENV = 'stage'
    DEVELOPMENT = True
    DEBUG = True
    PORT = 5000


class DevelopmentConfig(Config):
    FLASK_ENV = 'development'
    DEVELOPMENT = True
    DEBUG = True
    PORT = 5000


class TestingConfig(Config):
    FLASK_ENV = 'test'
    TESTING = True
    DEBUG = True
    PORT = 5000
