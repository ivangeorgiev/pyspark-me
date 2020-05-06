from typing import List, Callable
import pyspark.sql as ps
import pyspark.sql.types as pst
import pyspark



def as_list(input_df:ps.DataFrame, deep:bool=False, 
            order_by:Callable[[object, object],object]=None) -> List[dict]:
    """Convert Spark DataFrame to Python list of dictionaries.
    
    The DataFrame is collected and each row is converted into
    a dictionary. Optionally fields of pyspark Row type are 
    also recursively converted to dictionary.
    
    The resulting list could be sorted, using a supplied key function.
    Sorted list is useful, for example, to compare against expcted 
    fixture.
    
    Parameters:
      input_df (pyspark.sql.DataFrame): Spark DataFrame to be converted.
      
      deep (bool): (Default=False) Whether Row to dictionary conversion should be deep.
      
      order_by (callable): (Default=None) Key function to order by. If provided, the result 
                           list will be sorted using the provided key function.
    Returns:
      list: A list of records, converted to dictionary.
    """
    def row_as_dict(r):
      d = r.asDict()
      if deep:
        for k in d:
          if isinstance(d[k], pst.Row):
            d[k] = row_as_dict(d[k])
      return d
    
    if order_by:
      assert callable(order_by), "order_by must be callable"
    if isinstance(input_df, ps.DataFrame):
        result = [row_as_dict(r) for r in input_df.collect()]
    else:
        result = input_df
    if order_by and isinstance(result, list):
        result.sort(key=order_by)
    return result

