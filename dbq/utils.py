import os
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler


def date_filter(value):
    accepted_formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S']

    parsed_value = None
    for format_ in accepted_formats:
        try:
            parsed_value = datetime.strptime(value, format_)
        except ValueError:
            pass

    if parsed_value is None:
        raise ValueError('Date \'{}\' does not match any of {} format'.format(
            value, accepted_formats))

    return parsed_value


def create_logger(name, log_path):
    if not os.path.exists(log_path):
        try:
            os.mkdir(log_path)
        except Exception:
            raise AssertionError('Permission Denied: Please create logging folder {} \
                and provide read/write access to it'.format(log_path))

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    rotatingHandler = RotatingFileHandler(
        os.path.join(log_path, name + '.log'),
        maxBytes=1024,
        backupCount=5
    )
    rotatingHandler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s :: %(module)s:%(lineno)d \nMessage:: %(message)s'))

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s :: %(message)s'))

    logger.addHandler(rotatingHandler)
    logger.addHandler(consoleHandler)

    return logger
