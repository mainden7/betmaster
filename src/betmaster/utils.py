import os
import yaml

import yaml.scanner
import argparse


def check_positive(value):
    value = int(value)
    if value <= 0:
        raise argparse.ArgumentTypeError(
            f"{value} is an invalid positive int value")
    return value


def check_conf(path):
    actions_conf = os.path.abspath(path)
    try:
        with open(actions_conf, 'r') as f:
            yaml.load(f.read())
    except (yaml.parser.ParserError,
            yaml.scanner.ScannerError) as e:
        raise argparse.ArgumentTypeError(f'Invalid configuration file {e}')
    except FileNotFoundError:
        raise argparse.ArgumentTypeError(f'No such file: {actions_conf}')
    return actions_conf


def get_configuration(conf_path, service):
    with open(conf_path, 'r') as f:
        db_conf = yaml.load(f.read())
    return db_conf[service]
