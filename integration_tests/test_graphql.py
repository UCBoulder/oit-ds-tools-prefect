"""Integration tests for the graphql module"""

import unittest

from prefect import Flow, task, Parameter

from oit_ds_prefect_tools import graphql

class QueryTests(unittest.TestCase):
    """Tests for the query task"""

    def setUp(self):

        self.connection_info = {'endpoint': 'https://api.spacex.land/graphql/'}

    def test_singleton(self):
        """Tests query using just one request"""

        @task
        def validate(result):
            self.assertEqual(result, {'launch': {'launch_year':'2006'}})

        with Flow('test') as flow:
            flow.add_task(Parameter('env', default='dev'))
            result = graphql.query(
                query_str='query test($launch_id: ID!) { launch(id: $launch_id) { launch_year } }',
                variables={'launch_id':'1'},
                connection_info=self.connection_info)
            validate(result)
        flow.run()

    def test_chunking(self):
        """Tests query with a chunked variable"""

    def test_pagination(self):
        """Tests query using the next_variables_getter to implement pagination"""
