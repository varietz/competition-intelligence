from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import DocType, Text, Date,Search
from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch
from .models import *

# from .models import DateHierarchy, ProductHierarchy, SalesHistory1,BlogPost
# import models 

connections.create_connection()

class BlogPostIndex(DocType):
    author = Text()
    posted_date = Date()
    title = Text()
    text = Text()

    class Meta:
        index = 'blogpost-index'

def bulk_indexing():

    BlogPostIndex.init()
    es = Elasticsearch()
    bulk(client=es, actions=(b.indexing() for b in models.BlogPost.objects.all().iterator()))

def search():
    es1 = Elasticsearch()
    s = Search(using=es1,index="products").query('match', sku="s/ac")
    response = s.execute()
    # return response
    cnt=0
    for hit in s:
        print(hit.sku)
        skuid=hit.sku_id
        print(skuid)

        s1 = Search(using=es1,index="salesh").query('match', sku_id=skuid)
        for hit in s1:
            print(hit.sku_id,hit.sale_price,hit.units_sold,hit.sales_date)
        cnt+=1
    print(cnt)    
