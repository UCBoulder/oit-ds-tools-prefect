"""Integration tests for the graphql module"""

import unittest

from prefect import Flow, task, Parameter, unmapped

from oit_ds_prefect_tools import graphql

class QueryTests(unittest.TestCase):
    """Tests for the query task"""

    def setUp(self):

        self.connection_info = {'endpoint': 'https://rickandmortyapi.com/graphql'}

    def test_singleton(self):
        """Tests query using just one request"""

        @task
        def validate(result):
            self.assertEqual(result, {'character': {'name':'Rick Sanchez'}})

        with Flow('test') as flow:
            flow.add_task(Parameter('env', default='dev'))
            result = graphql.query(
                query_str='query test($characterId: ID!) { character(id: $characterId) { name } }',
                variables={'characterId':'1'},
                connection_info=self.connection_info)
            validate(result)
        state = flow.run()
        self.assertFalse(state.is_failed())

    def test_chunking(self):
        """Tests query with chunked variables"""

        @task
        def validate(results):
            full_list = [i['name'] for result in results for i in result['charactersByIds']]
            self.assertEqual(len(full_list), 10)

        with Flow('test') as flow:
            flow.add_task(Parameter('env', default='dev'))
            chunks = [["1", "2", "3"], ["4", "5", "6"], ["7", "8", "9"], ["10"]]
            variables = [{'ids':i} for i in chunks]
            results = graphql.query.map(
                query_str=unmapped(
                    'query ChunkQuery($ids: [ID!]!) { charactersByIds(ids: $ids) { name } }'),
                variables=variables,
                connection_info=unmapped(self.connection_info))
            validate(results)
        state = flow.run()
        self.assertFalse(state.is_failed())

    def test_pagination(self):
        """Tests query using the next_variables_getter to implement pagination"""

        @task
        def validate(results):
            full_list = [i['name'] for result in results for i in result['episodes']['results']]
            self.assertGreater(len(full_list), 20)

        def get_next_page(result):
            if result["episodes"]["info"]["next"]:
                return {'page': result["episodes"]["info"]["next"]}
            return None

        with Flow('test') as flow:
            flow.add_task(Parameter('env', default='dev'))
            results = graphql.query(
                query_str='query PaginatedQuery($page: Int) '
                          '{ episodes(page: $page) { info { next } results { name } } }',
                variables={'page':1},
                next_variables_getter=get_next_page,
                connection_info=self.connection_info)
            validate(results)
        state = flow.run()
        self.assertFalse(state.is_failed())
