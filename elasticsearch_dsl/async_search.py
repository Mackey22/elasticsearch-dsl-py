# Asynchronous searching module enhancing Search operations
from .connections import connections
from .search import Search, MultiSearch


class AsyncSearch(Search):
    def __init__(self, **kwargs):
        super(AsyncSearch, self).__init__(**kwargs)

    async def count(self):
        """
        Return the number of hits matching the query and filters. Note that
        only the actual number is returned
        """
        if hasattr(self, '_response'):
            return self._response.hits.total

        es = connections.get_connection(self._using)

        d = self.to_dict(count=True)
        # TODO: failed shards detection
        c = await es.count(
            index=self._index,
            doc_type=self._get_doc_type(),
            body=d,
            **self._params)
        return c['count']

    async def execute(self, ignore_cache=False):
        """
        Execute the search and return an instance of ``Response`` wrapping all
        the data.

        :arg response_class: optional subclass of ``Response`` to use instead.
        """
        if ignore_cache or not hasattr(self, '_response'):
            es = connections.get_connection(self._using)

            s = await es.search(
                index=self._index,
                doc_type=self._get_doc_type(),
                body=self.to_dict(),
                **self._params)
            self._response = self._response_class(self, s)
        return self._response

    async def delete(self):
        """
        delete() executes the query by delegating to delete_by_query()
        """

        es = connections.get_connection(self._using)

        d = await es.delete_by_query(
            index=self._index,
            body=self.to_dict(),
            doc_type=self._get_doc_type(),
            **self._params)
        return AttrDict(d)


class AsyncMultiSearch(MultiSearch):
    """
    Combine multiple :class:`~elasticsearch_dsl.Search` objects into a single
    request.
    """

    def __init__(self, **kwargs):
        super(AsyncMultiSearch, self).__init__(**kwargs)

    async def execute(self, ignore_cache=False, raise_on_error=True):
        """
        Execute the multi search request and return a list of search results.
        """
        if ignore_cache or not hasattr(self, '_response'):
            es = connections.get_connection(self._using)

            responses = await es.msearch(
                index=self._index,
                doc_type=self._get_doc_type(),
                body=self.to_dict(),
                **self._params)

            out = []
            for s, r in zip(self._searches, responses['responses']):
                if r.get('error', False):
                    if raise_on_error:
                        raise TransportError('N/A', r['error']['type'],
                                             r['error'])
                    r = None
                else:
                    r = Response(s, r)
                out.append(r)

            self._response = out

        return self._response
