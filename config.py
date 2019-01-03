#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import configparser
import sys

def get_google_project(config_filename):
    config = configparser.ConfigParser()
    try:
        config.read(config_filename)
    except Exception:
        print("Oops! Wrong config file input:" + config_filename + ".")
        sys.exit(1)

    return config['google']['project']


def get_db_config(config_filename):
    config = configparser.ConfigParser()
    try:
        config.read(config_filename)
    except Exception:
        print("Oops! Wrong config file input:" + config_filename + ".")
        sys.exit(1)

    if ('postgresql' in config) is False:
        print("Oops! Config file should have valid postgresql configuration!")
        sys.exit(1)
    if ('dbname' in config['postgresql']) is False:
        print("Oops! Config file should have valid postgresql configuration!")
        sys.exit(1)
    if ('user' in config['postgresql']) is False:
        print("Oops! Config file should have valid postgresql configuration!")
        sys.exit(1)
    if ('password' in config['postgresql']) is False:
        print("Oops! Config file should have valid postgresql configuration!")
        sys.exit(1)
    if ('host' in config['postgresql']) is False:
        print("Oops! Config file should have valid postgresql configuration!")
        sys.exit(1)
    if ('port' in config['postgresql']) is False:
        print("Oops! Config file should have valid postgresql configuration!")
        sys.exit(1)

    return config['postgresql']


def get_redis_config(config_filename):
    config = configparser.ConfigParser()
    try:
        config.read(config_filename)
    except Exception:
        print("Oops! Wrong config file input:" + config_filename + ".")
        sys.exit(1)

    if ('redis' in config) is False:
        print("Oops! Config file should have valid redis configuration!")
        sys.exit(1)
    if ('host' in config['redis']) is False:
        print("Oops! Config file should have valid redis configuration!")
        sys.exit(1)
    if ('password' in config['redis']) is False:
        print("Oops! Config file should have valid redis configuration!")
        sys.exit(1)
    if ('port' in config['redis']) is False:
        print("Oops! Config file should have valid redis configuration!")
        sys.exit(1)
    return config['redis']

def get_follow_twitter_user(config_filename):
    config = configparser.ConfigParser()
    try:
        config.read(config_filename)
    except Exception:
        print("Oops! Wrong config file input:" + config_filename + ".")
        sys.exit(1)
    if ('facebook' in config) is False:
        print("Oops! Config file should have facebook section which contains username/password!")
        sys.exit(1)
    if ('twuser' in config['facebook']) is False:
        print("Oops! Config file should have facebook section which contains valid twitter user to follow")
        sys.exit(1)
    return config['facebook']['twuser']

def get_facebook_auth_config(config_filename):
    config = configparser.ConfigParser()
    try:
        config.read(config_filename)
    except Exception:
        print("Oops! Wrong config file input:" + config_filename + ".")
        sys.exit(1)

    if ('facebook' in config) is False:
        print("Oops! Config file should have facebook section which contains username/password!")
        sys.exit(1)
    if ('username' in config['facebook']) is False:
        print("Oops! Config file should have facebook section which contains valid username/password!")
        sys.exit(1)
    if ('password' in config['facebook']) is False:
        print("Oops! Config file should have facebook section which contains valid username/password!")
        sys.exit(1)
    auth = (config['facebook']['username'], config['facebook']['password'])
    return auth


def get_celery_broker_config(config_filename):
    config = configparser.ConfigParser()
    try:
        config.read(config_filename)
    except Exception:
        print("Oops! Wrong config file input:" + config_filename + ".")
        sys.exit(1)

    if ('celery' in config) is False:
        print("Oops! Config file should have valid celery section!")
        sys.exit(1)

    if ('broker' in config['celery']) is False:
        print("Oops! Config file should have valid celery section!")
        sys.exit(1)

    broker_str = config['celery']['broker']
    return broker_str

def get_ss_config(config_filename):
    config = configparser.ConfigParser()
    try:
        config.read(config_filename)
    except Exception:
        print("Oops! Wrong config file input:" + config_filename + ".")
        sys.exit(1)

    if ('shadowsocks' in config) is False:
        print("Oops! Config file should have valid shadowsocks section!")
        sys.exit(1)

    return config['shadowsocks']

def get_kafka_config(config_filename):
    config = configparser.ConfigParser()
    try:
        config.read(config_filename)
    except Exception:
        print("Oops! Wrong config file input:" + config_filename + ".")
        sys.exit(1)

    if ('kafka' in config) is False:
        print("Oops! Config file should have valid kafka section!")
        sys.exit(1)
    return config['kafka']
