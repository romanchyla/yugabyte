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


class TestApp(unittest.TestCase):
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
            'SQLALCHEMY_ECHO': True,
            'PROJ_HOME' : proj_home,
            'TEST_DIR' : os.path.join(proj_home, 'ybload/tests'),
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()
        
        
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    def test_app(self):
        assert self.app._config.get('SQLALCHEMY_URL') == 'postgresql://postgres@127.0.0.1:15678/test'

    def test_update_get(self):
        r = self.app.get_record('abc')
        self.assertEqual(r, None)
        
        self.app.update_storage('abc', 'bib_data', {'bibcode': 'abc', 'hey': 1})
        self.app.mark_processed(['abc'], 'solr', checksums=['jkl'], status='success')
        r = self.app.get_record('abc')
        
        self.assertTrue(r['solr_processed'])
        self.assertTrue(r['status'])

        self.app.mark_processed(['abc'], 'solr', checksums=['jkl'], status='solr-failed')
        r = self.app.get_record('abc')
        self.assertTrue(r['solr_processed'])
        self.assertTrue(r['processed'])
        self.assertEqual(r['status'], 'solr-failed')

    def test_insert_binary_data(self):
        pass


if __name__ == '__main__':
    unittest.main()        