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
    '''
    Read ini-formatted config file from disk
    :param with_tls: Boolean on whether to run with TLS security or not.
    '''

    # Initial setup magic
    config = get_config_ini()
    app = Flask(__name__)
    basic_auth = BasicAuth(app)
    db_connection_str = "postgresql://" + str(config['db'].get('user')) + ":" + str(config['db'].get('pass')) + "@" \
                        + str(config['db'].get('host')) + "/" + str(config['db'].get('dbname'))
    app.config['SQLALCHEMY_DATABASE_URI'] = db_connection_str
    app.config['SECRET_KEY'] = os.urandom(24)
    app.config['BASIC_AUTH_USERNAME'] = config['db'].get('user')
    app.config['BASIC_AUTH_PASSWORD'] = config['db'].get('pass')
    app.config['BASIC_AUTH_FORCE'] = True
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
        column_exclude_list = ['last_crawl_timestamp', 'abort_after_numerrors',
                               'max_records_updated_per_run', 'update_log_after_numitems',
                               'record_refresh_days', 'repo_refresh_days', ]
        form_choices = {
            'repository_type': [
                ('oai', 'OAI'),
                ('ckan', 'CKAN'),
                ('marklogic', 'Marklogic'),
            ],
            'enabled': [
                ('true', 'Yes'),
                ('false', 'No'),
            ]
        }
        edit_modal = True

    # Initialize interface and bind to ports.
    admin = Admin(app, name='FRDR Harvester', template_mode='bootstrap3')
    repositories_view = RepositoryModelView(Repositories, db.session)
    admin.add_view(repositories_view)
    # Set up SSL.
    if (with_tls):
        cert_path = config['admin'].get('cert_path')
        key_path = config['admin'].get('key_path')
        app.run(port=8000, host='0.0.0.0', ssl_context=(cert_path, key_path))
    else:
        app.run(port=8000, host='0.0.0.0')


if __name__ == '__main__':

    cl_parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    cl_parser.add_argument('-s', '--tls', help='Run with TLS', action='store_true')
    args = cl_parser.parse_args()
    with_tls = False
    if (args.tls):
        with_tls = True
    run_admin_server(with_tls)
