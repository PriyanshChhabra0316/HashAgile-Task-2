# THIS WORK IS DONE BY PRIYANSH CHHABRA 
# COLLEGE : GLAGOTIAS COLLEGE OF ENGINEERING AND TECHNOLGY
# DOMAIN : FULL STACK DEVELOPER 
# CODE IS WRITTEN BY PRIYANSH CHHABRA 




from elasticsearch import Elasticsearch, helpers
import pandas as pd

# In this way i am connecting it to elasticsearch 

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])


def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Collection {p_collection_name} created.")
    else:
        print(f"Collection {p_collection_name} already exists.")

def indexData(p_collection_name, p_exclude_column):

    df = pd.read_csv('/Users/priyansh/Downloads/Employee Sample Data 1.csv')  
    df = df.drop(columns=[p_exclude_column])  


    actions = [
        {
            "_index": p_collection_name,
            "_id": str(row['Employee_id']),  
            "_source": row.to_dict()
        }
        for _, row in df.iterrows()
    ]


    helpers.bulk(es, actions)
    print(f"Data indexed into {p_collection_name}, excluding {p_exclude_column}.")

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    body = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    res = es.search(index=p_collection_name, body=body)
    return res['hits']['hits']


def getEmpCount(p_collection_name):
    res = es.count(index=p_collection_name)
    return res['count']


def delEmpById(p_collection_name, p_employee_id):
    es.delete(index=p_collection_name, id=p_employee_id)
    print(f"Employee {p_employee_id} deleted from {p_collection_name}.")

def getDepFacet(p_collection_name):
    body = {
        "aggs": {
            "by_department": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    res = es.search(index=p_collection_name, body=body)
    return res['aggregations']['by_department']['buckets']


v_nameCollection = 'Hash_Priyansh'
v_phoneCollection = 'Hash_2234'


createCollection(v_nameCollection)
createCollection(v_phoneCollection)

print(f"Initial employee count: {getEmpCount(v_nameCollection)}")

indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')

delEmpById(v_nameCollection, 'E02003')


print(f"Employee count after deletion: {getEmpCount(v_nameCollection)}")


print(searchByColumn(v_nameCollection, 'Department', 'IT'))
print(searchByColumn(v_nameCollection, 'Gender', 'Male'))
print(searchByColumn(v_phoneCollection, 'Department', 'IT'))


print(getDepFacet(v_nameCollection))
print(getDepFacet(v_phoneCollection))
