from datetime import timedelta, datetime
from six import iteritems, itervalues, string_types

from .async_search import AsyncSearch
from .aggs import A
from .utils import AttrDict
from .response import Response
from .query import Q
from .faceted_search import FacetedSearch, FacetedResponse


class AsyncFacetedSearch(FacetedSearch):
    """
    Abstraction for creating faceted navigation searches that takes care of
    composing the queries, aggregations and filters as needed as well as
    presenting the results in an easy-to-consume fashion::

        class BlogSearch(FacetedSearch):
            index = 'blogs'
            doc_types = [Blog, Post]
            fields = ['title^5', 'category', 'description', 'body']

            facets = {
                'type': TermsFacet(field='_type'),
                'category': TermsFacet(field='category'),
                'weekly_posts': DateHistogramFacet(field='published_from', interval='week')
            }

            def search(self):
                ' Override search to add your own filters '
                s = super(BlogSearch, self).search()
                return s.filter('term', published=True)

        # when using:
        blog_search = BlogSearch("web framework", filters={"category": "python"})

        # supports pagination
        blog_search[10:20]

        response = blog_search.execute()

        # easy access to aggregation results:
        for category, hit_count, is_selected in response.facets.category:
            print(
                "Category %s has %d hits%s." % (
                    category,
                    hit_count,
                    ' and is chosen' if is_selected else ''
                )
            )

    """

    def __init__(self, query=None, filters={}, sort=()):
        """
        :arg query: the text to search for
        :arg filters: facet values to filter
        :arg sort: sort information to be passed to :class:`~elasticsearch_dsl.Search`
        """
        super(AsyncFacetedSearch, self).__init__(
            query=query, filters=filters, sort=sort)

    def search(self):
        """
        Returns the base Search object to which the facets are added.

        You can customize the query by overriding this method and returning a
        modified search object.
        """
        s = AsyncSearch(
            doc_type=self.doc_types, index=self.index, using=self.using)
        return s.response_class(FacetedResponse)

    async def execute(self):
        """
        Execute the search and return the response.
        """
        r = await self._s.execute()
        r._faceted_search = self
        return r
