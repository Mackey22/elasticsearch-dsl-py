from elasticsearch_dsl import document, field, InnerDoc
from elasticsearch_dsl.exceptions import ValidationException, IllegalOperation

from pytest import raises, mark


class MyInner(InnerDoc):
    old_field = field.Text()


class MyDoc(document.DocType):
    title = field.Keyword()
    name = field.Text()
    created_at = field.Date()
    inner = field.Object(MyInner)


@mark.asyncio
async def test_save_no_index(mock_client):
    md = MyDoc()
    with raises(ValidationException):
        await md.async_save(using='mock')


@mark.asyncio
async def test_delete_no_index(mock_client):
    md = MyDoc()
    with raises(ValidationException):
        await md.async_delete(using='mock')


@mark.asyncio
async def test_update_no_fields():
    md = MyDoc()
    with raises(IllegalOperation):
        await md.async_update(refresh=True)
