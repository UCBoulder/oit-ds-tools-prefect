"""Integration tests for the rest module"""

import unittest

from prefect import Flow, task, Parameter

from oit_ds_prefect_tools import rest

class GetTests(unittest.TestCase):
    """Tests for the rest.get task"""

    def setUp(self):

        self.connection_info = {'domain': 'https://pokeapi.co'}

    def test_pagination(self):
        """Tests the get task with paginated results"""

        @task
        def validate(results):
            data = [i for result in results for i in result['results']]
            self.assertGreater(len(data), 500)

        def next_page(response):
            return response.json()['next']

        with Flow('test') as flow:
            flow.add_task(Parameter('env', default='dev'))
            results = rest.get(
                endpoint='/api/v2/pokemon/?limit=300',
                next_page_getter=next_page,
                connection_info=self.connection_info)
            validate(results)
        state = flow.run()
        self.assertFalse(state.is_failed())
