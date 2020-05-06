from operator import itemgetter
import pytest
import pyspark.sql.types as pst
from pysparkme.functions import *


def test_as_list_converts_rows(spark):
    """Given a dataframe, 
       When as_list is called,
       Then result is a list of dictionaries"""
    input = [ (1, 'John') ]
    input_df = spark.createDataFrame(input, ['id', 'first_name'])

    actual = as_list(input_df, False)
    expect = [ dict(id=1, first_name='John')]

    assert actual == expect


def test_as_list_shallow_doesnt_convert_fields(spark):
    """Given a dataframe with nested structure fields, 
       When as_list with shallow=True is called,
       Then result is a list of dictionaries with structure field not converted"""
    input = [ (1, pst.Row(first_name='John')) ]
    input_df = spark.createDataFrame(input, ['id', 'person'])

    actual = as_list(input_df, False)
    expect = [ dict(id=1, person=pst.Row(first_name='John'))]

    assert actual == expect


def test_as_list_deep_convert_fields(spark):
    """Given a dataframe with nested structure fields, 
       When as_list with shallow=True is called,
       Then result is a list of dictionaries with structure field not converted"""
    input = [ (1, pst.Row(first_name='John')) ]
    input_df = spark.createDataFrame(input, ['id', 'person'])

    actual = as_list(input_df, False)
    expect = [ dict(id=1, person=pst.Row(first_name='John'))]

    assert actual == expect


def test_as_list_with_order_by_sorts_rows(spark):
    """Given unsorted dataframe with two rows,
       When as_list with order_by is called,
       Then result list is ordered"""
    
    input = [(9, 'Zztop'),
             (1, 'John')]
    input_df = spark.createDataFrame(input, ['id', 'name'])
    
    expect = [dict(id=1, name='John'),
              dict(id=9, name='Zztop')]
    
    actual = as_list(input_df, order_by=itemgetter('id'))
    assert actual == expect
