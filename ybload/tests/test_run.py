#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mock
import unittest
import os
import sys
import copy
import json

import adsputils
from ybload import app, models
from ybload.models import Base, MetricsBase
from adsputils import get_date
import testing.postgresql
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
import tempfile
import run
import mock

class TestRun(unittest.TestCase):
    """
    Tests the appliction's methods
    """
    
    @classmethod
    def setUpClass(cls):
        cls.postgresql = \
            testing.postgresql.Postgresql(host='127.0.0.1', port=15678, user='postgres', 
                                          database='test')

    @classmethod
    def tearDownClass(cls):
        cls.postgresql.stop()
        
    def setUp(self):
        unittest.TestCase.setUp(self)
        
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        self.app = app.YBLoader('test', local_config=\
            {
            'SQLALCHEMY_URL': 'postgresql://postgres@127.0.0.1:15678/test',
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME' : proj_home,
            'TEST_DIR' : os.path.join(proj_home, 'ybload/tests'),
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
        
        
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()


    
    def test_ingest_binary(self):
        with mock.patch.object(run, 'app', self.app):
            fname = None
            try:
                fd, fname = tempfile.mkstemp()
                with open(fname,'w') as fo:
                    fo.write('{}\t{}\n'.format(fname, self.app.conf.get('TEST_DIR') + '/data/foo.txt'))
                    fo.write('{}\t{}\n'.format(fname + 'second', self.app.conf.get('TEST_DIR') + '/data/foo.txt'))
                

                run.ingest_binary_files(fname, report = 1)
                with self.app.session_scope() as s:
                    r = s.query(models.BigTable).filter_by(key=fname).first()
                    assert r.value == b'bar baz'

            finally:
                if fname and os.path.exists(fname):
                    os.remove(fname)




if __name__ == '__main__':
    unittest.main()        