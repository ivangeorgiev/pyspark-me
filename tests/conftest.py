import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    import os
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    spark_session = (SparkSession
        .builder
        .appName('TestingPysparkMe')
        .master('local[*]')
        .enableHiveSupport()
        .getOrCreate())
    spark_session.sparkContext.setLogLevel('ERROR')
    yield spark_session
    try:
        spark_session.stop()
    except:
        pass

