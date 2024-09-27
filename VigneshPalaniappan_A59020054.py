import json
import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd
from dask.distributed import Client
import ctypes
import dask.multiprocessing

def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)
    
def PA0(user_reviews_csv_path):
    client = Client()
    # Helps fix any memory leaks. We noticed Dask can slow down when same code is run again.
    client.run(trim_memory)
    client = client.restart()
    
########################## WRITE YOUR CODE HERE ##########################

    # 1. Read in the large dataset
    user_reviews_ddf = dd.read_csv(user_reviews_csv_path)

    # 2. Extract data to new columns
    #(a) Year
    user_reviews_ddf = user_reviews_ddf.map_partitions(lambda df: df.assign(reviewing_since=df.reviewTime.str.extract(r'([0-9]{4})') , meta=pd.Series(dtype=float)))

    #(b) Helpful votes
    user_reviews_ddf = user_reviews_ddf.map_partitions(lambda df: df.assign(helpful_votes=df.helpful.str.extract(r'^\D*(\d*)') , meta=pd.Series(dtype=float)))

    #(c) Total votes
    user_reviews_ddf = user_reviews_ddf.map_partitions(lambda df: df.assign(total_votes=df.helpful.str.extract(r'(\d*)\D*$') , meta=pd.Series(dtype=float)))

    # change datatype of new columns so we can do operations on them
    user_reviews_ddf = user_reviews_ddf.astype({"reviewing_since": int,"helpful_votes": int,"total_votes": int })

    #3. Create users table with aggregated columns of interest
    ddf = user_reviews_ddf.groupby("reviewerID")
    users = ddf.agg({"asin":'count','overall':'mean','reviewing_since':'min','helpful_votes':'sum','total_votes':'sum'},split_out=4)
    users = users.rename(columns={'asin':'number_products_rated','overall':'avg_ratings'})


##########################################################################
    
    submit = users.describe().compute().round(2)    
    with open('results_PA0.json', 'w') as outfile: json.dump(json.loads(submit.to_json()), outfile)

#user_reviews_csv_path = './PA0/user_reviews_Release.csv'
#if __name__ == '__main__':
#    PA0(user_reviews_csv_path)