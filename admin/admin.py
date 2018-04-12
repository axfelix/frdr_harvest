#!/usr/bin/python3

from flask import Flask
from flask_basicauth import BasicAuth
from flask_admin import Admin
from flask_sqlalchemy import SQLAlchemy
from flask_admin.contrib.sqla import ModelView
from sqlalchemy import Table
import configparser
import argparse
import sys
import os
import daemon
from lockfile.pidlockfile import PIDLockFile
import logging
import logging.handlers

LOGGER = logging.getLogger(__name__)
log_format = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')

def get_config_ini(config_file="../conf/harvester.conf"):
    '''
    Read ini-formatted config file from disk
    :param config_file: Filename of config file
    :return: configparser-style config file
    '''

    config = configparser.ConfigParser()
    config.read(config_file)
    return config


def run_admin_server(with_tls=False):
    """
    Set up flask-admin based CRUD server for repositories table.
    :param with_tls: Boolean on whether to run with TLS security or not.
    """

    config = get_config_ini()
    # Initial setup magic
    app = Flask(__name__)
    basic_auth = BasicAuth(app)
    db_connection_str = '%s%s%s%s%s%s%s%s' % (
        'postgresql://', str(config['db'].get('user')), ':', str(config['db'].get('pass')),
        '@', str(config['db'].get('host')), '/', str(config['db'].get('dbname')))
    app.config['SQLALCHEMY_DATABASE_URI'] = db_connection_str
    app.config['SECRET_KEY'] = os.urandom(24)
    app.config['BASIC_AUTH_USERNAME'] = config['db'].get('user')
    app.config['BASIC_AUTH_PASSWORD'] = config['db'].get('pass')
    app.config['BASIC_AUTH_FORCE'] = True
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db = SQLAlchemy(app)

    # Load existing database into flask-admin
    db.reflect()
    repositories_table = Table("repositories", db.metadata, autoload=True)

    class Repositories(db.Model):
        # Magic line to have a model mirror existing schema in flask-sqladmin
        __table__ = repositories_table

    # Some attributes can only be set by subclassing ModelView, so set all
    # table/form editing configuration here

    class RepositoryModelView(ModelView):
        page_size = 10
        # Columns not to show
        column_list = ['repository_name', 'repository_url', 'repository_type', 'repository_set']
        # Dropdown boxes on create/edit
        form_choices = {
            'repository_type': [
                ('oai', 'oai'),
                ('ckan', 'ckan'),
                ('marklogic', 'marklogic'),
                ('csw','csw'),
            ],
            'enabled': [
                ('true', 'Yes'),
                ('false', 'No'),
            ]
        }
        # Which columns support searching
        column_searchable_list = ['repository_set', 'repository_url', 'repository_name', 'repository_set']
        edit_modal = True
        export_types = ['csv', 'json']


    # Initialize interface and bind to ports.
    admin = Admin(app, name='FRDR Harvester', template_mode='bootstrap3')
    repositories_view = RepositoryModelView(Repositories, db.session)
    admin.add_view(repositories_view)
    # Set up SSL.
    pid = '/tmp/hadmin.pid'
    if (with_tls):
        print("TLS")
        cert_path = config['admin'].get('cert_path')
        key_path = config['admin'].get('key_path')
        with daemon.DaemonContext(pidfile=PIDLockFile(pid)):
            app.run(port=8100, host='0.0.0.0', ssl_context=(cert_path, key_path))
    else:
        with daemon.DaemonContext(pidfile=PIDLockFile(pid)):
            app.run(port=8100, host='0.0.0.0')


if __name__ == '__main__':

    cl_parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    cl_parser.add_argument('-s', '--tls', help='Run with TLS', action='store_true')
    args = cl_parser.parse_args()
    with_tls = False
    if (args.tls):
        with_tls = True

    syslog_handler = logging.handlers.SysLogHandler(address='/dev/log')
    logging.basicConfig(level=logging.INFO, format=log_format, handlers=[syslog_handler])
    run_admin_server(with_tls)
