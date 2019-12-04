from os import getenv
from os.path import abspath, dirname, join


basedir = abspath(dirname(__file__))


class Config(object):
    SECRET_KEY = getenv('SECRET_KEY') or 'secret_key'
    CONFIRMATION_KEY = getenv('CONFIRMATION_KEY') or 'confirmation_key'
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    @staticmethod
    def init_app(app):
        pass


class DevelopmentConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = getenv('DEV_DATABASE_URL') or \
        'sqlite:///' + join(basedir, 'data_dev.sqlite')


class TestingConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = getenv('TEST_DATABASE_URL') or 'sqlite://'


class ProductionConfig(Config):
    SQLALCHEMY_DATABASE_URI = getenv('DATABASE_URL') or \
        'sqlite:///' + join(basedir, 'data.sqlite')


config = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,

    'default': DevelopmentConfig
}
