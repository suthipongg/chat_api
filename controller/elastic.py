from elasticsearch.helpers import bulk
from datetime import datetime
import json

from configs.db import client

class Elastic():
    def __init__(self) -> None:
        self.client = client

    def create_indices(self, alias_name: str, mapping: dict):
        index_name = alias_name + '_' + datetime.now().strftime("%Y%m%d.%H%M")
        # check indicse is exists
        _isAlias = self.client.indices.exists_alias(index='*', name=alias_name)
        if not _isAlias:
            # first create index and alias
            created_indices = self.client.indices.create(index=index_name, body=mapping, ignore=[400, 404])
            print(created_indices)
            # create alias
            set_alias = self.client.indices.put_alias(index=index_name, name=alias_name)
            print(set_alias)
            return True
        return False
            

    
    def migrate_data(self, index_name: str, datas: list):
        actions = []
        for data in datas:
            action = {
                "_index": index_name,
                "_op_type": "index",  # Use "index" for indexing documents
                "_id": data['id'] if data.get('id', False) else data['customer_id'],  # Set the _id field
                "_source": data  # Your document data
            }
            actions.append(action)

        success, failed = bulk(
            self.client,
            actions,
        )
        print(f"Indexed {success} documents successfully. Failed to index {failed} documents.")
        return success, failed
    
    def es_update(self, index_name: str, data_dict: dict):
        updated = self.client.update(index=index_name, id=data_dict['id'], body={'doc': data_dict})
        return updated
    
    def es_update_by_query(self, index_name: str, query: dict, data_dict: dict):
        _inline = ''
        
        for key, value in data_dict.items():
            _inline += f"ctx._source.{key}='{str(value)}'; "
        q = {
            "query": query,
            "script": {
                "source": _inline,
                "lang"   : "painless"
            }
        }
        print(json.dumps(q))
        updated = self.client.update_by_query(index=index_name, body=json.dumps(q))
        return updated
    
    def reindex(self, source: str, dest: str):
        _reindex = self.client.reindex(
            {
                "source": {
                    "index": source,
                },
                "dest": {
                    "index": dest
                }
            }
        )
        print(_reindex)
        return True
    
    def _backup_indices(self, alias_name: str):
        isAlias = self.client.indices.exists_alias(index='*', name=alias_name)
        isAliasTemp = self.client.indices.exists_alias(index='*', name=alias_name + '_temp')
        if isAlias:
            _indicesAlias = self.client.indices.get_alias(index='*', name=alias_name)
            _indices = list(_indicesAlias.keys())[0]
            print(_indicesAlias)
            if isAliasTemp:
                _indicesAliasTemp = self.client.indices.get_alias(index='*', name=alias_name + '_temp')
                _indicesTemp = list(_indicesAliasTemp.keys())[0]
                #  delete old index temp
                self.client.indices.delete(index=_indicesTemp, ignore=[404, 400])
                # create reindex new temp
                self.reindex(source=_indices, dest=_indices + '_temp')
                # set alias temp
                self.client.indices.put_alias(index=_indices + '_temp', name=alias_name + '_temp')
                return True
            # create reindex new temp
            self.reindex(source=_indices, dest=_indices + '_temp')
            # set alias temp
            self.client.indices.put_alias(index=_indices + '_temp', name=alias_name + '_temp')
            return True
        return False
    
    def backup_indices(self, alias_name: str):
        isAlias = self.client.indices.exists_alias(index='*', name=alias_name)
        isAliasTemp = self.client.indices.exists_alias(index='*', name=alias_name + '_temp')
        if isAlias:
            _indicesAlias = self.client.indices.get_alias(index='*', name=alias_name)
            _indices = list(_indicesAlias.keys())[0]
            self.reindex(source=_indices, dest=_indices + '_temp')
            print(_indices)
            if isAliasTemp:
                # get index temp
                _indicesAliasTemp = self.client.indices.get_alias(index='*', name=alias_name + '_temp')
                _indicesTemp = list(_indicesAliasTemp.keys())[0]
                _setAlias = self.client.indices.update_aliases(
                    {
                        "actions": [
                            { "remove": { "index": "*", "alias": alias_name + '_temp'  }}, 
                            { "add":    { "index": _indices + '_temp', "alias": alias_name + '_temp'  }}  
                        ]
                    }
                )
                # remove index old _temp
                _removed = self.client.indices.delete(index=_indicesTemp)
                print(_removed)

            _setAlias = self.client.indices.put_alias(index=_indices + '_temp', name=alias_name + '_temp')
            print(_setAlias)
            return True
        return False
    
    def restructure_indices(self, alias_name: str, mapping: dict, datas: list):
        index_name = alias_name + '_' + datetime.now().strftime("%Y%m%d.%H%M")
        ## 1. create new index
        self.client.indices.create(index=index_name, body=mapping, ignore=[400, 404])
        # self.create_indices(alias_name=alias_name, index_name=index_name, mapping=mapping)
        ## 2. migrate data to new index
        self.migrate_data(index_name=index_name, datas=datas)
        ## 3. backup data old index
        self._backup_indices(alias_name=alias_name)
        ## 4. update alias to new index
        _setAlias = self.client.indices.update_aliases(
                    {
                        "actions": [
                            { "remove": { "index": "*", "alias": alias_name }}, 
                            { "add":    { "index": index_name, "alias": alias_name }}  
                        ]
                    }
                )
        ## 5. delete index old
        self.delete_indices_update(alias_name=alias_name, backup_size=3)
        return True

    def delete_indices_update(self, alias_name: str, backup_size: int = 3):
        getIndices = self.client.indices.get(index=alias_name + '*')
        listIndices = [index for index in list(getIndices.keys())[:-1] if 'temp' not in index]
        if len(listIndices) > backup_size:
            return self.client.indices.delete(index=listIndices[0])
        



    



if __name__ == "__main__":
    es = Elastic(host='http://localhost', port='9200')
    alias_name = 'my_index'
    index_name = alias_name + '_' + datetime.now().strftime('%Y%m%d.%H%M')

    mapping_start = {
            "mappings": {
                "properties": {
                    "title": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "content": {
                        "type": "text"
                    },
                    "timestamp": {
                        "type": "date"
                    },
                    "id": {
                        "type": "keyword"
                    }
                }
            }
        }
    
    print(json.dumps(mapping_start))

    # # create index
    # # es.create_indices(alias_name=alias_name, index_name=index_name, mapping=mapping_start)

    # # Sample data with the _id field
    # sample_data = [
    #     {
    #         "_id": "1",  # Specify the _id field using the 'id' value
    #         "title": "Sample Document 1",
    #         "content": "This is the content1 of Sample Document 1.",
    #         "timestamp": "3033-10-01T10:00:00"
    #     },
    #     {
    #         "_id": "2",
    #         "title": "Sample Document 2",
    #         "content": "This is the content2 of Sample Document 2.",
    #         "timestamp": "3033-10-01T11:00:00"
    #     },
    #     {
    #         "id":"3",
    #         "title": "Sample Document 3",
    #         "content": "This is the content3 of Sample Document 3.",
    #         "timestamp": "3033-10-01T13:00:00"
    #     },
    #     {
    #         "id":"4",
    #         "title": "Sample Document 4",
    #         "content": "This is the content4 of Sample Document 4.",
    #         "timestamp": "3033-10-01T13:00:00"
    #     },
    #     # Add more documents with '_id' specified
    # ]

    # # migrate data
    # # es.migrate_data(index_name=index_name, datas=sample_data)

    # # backup index
    # # es.backup_indices(alias_name=alias_name)

    
    # # ---------------------
    # # update index (structure index)
    # mapping_new = {
    #         "mappings": {
    #             "properties": {
    #                 "title10": {
    #                     "type": "text",
    #                     "fields": {
    #                         "keyword": {
    #                             "type": "keyword"
    #                         }
    #                     }
    #                 },
    #                 "content10": {
    #                     "type": "text"
    #                 },
    #                 "timestamp10": {
    #                     "type": "date"
    #                 },
    #                 "id10": {
    #                     "type": "keyword"
    #                 }
    #             }
    #         }
    #     }
    # sample_data_new = [
    #     {
    #         "id10": "1",  # Specify the _id field using the 'id' value
    #         "title10": "Sample Document 1",
    #         "content10": "This is the content1 of Sample Document 1.",
    #         "timestamp10": "3033-10-01T10:00:00"
    #     },
    #     {
    #         "id10": "2",
    #         "title10": "Sample Document 2",
    #         "content10": "This is the content2 of Sample Document 2.",
    #         "timestamp10": "3033-10-01T11:00:00"
    #     },
    #     {
    #         "id10":"3",
    #         "title10": "Sample Document 3",
    #         "content10": "This is the content3 of Sample Document 3.",
    #         "timestamp10": "3033-10-01T13:00:00"
    #     },
    #     {
    #         "id10":"10",
    #         "title10": "Sample Document 10",
    #         "content10": "This is the content10 of Sample Document 10.",
    #         "timestamp10": "3033-10-01T13:00:00"
    #     },
    #     # Add more documents with '_id' specified
    # ]
    # es.update_indices(alias_name=alias_name,index_name=index_name,mapping=mapping_new,datas=sample_data_new)

    # # delet indices temp
    # # es.delete_indices_update(alias_name=alias_name)
    




