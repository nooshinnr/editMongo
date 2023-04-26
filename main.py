import pymongo


def del_redundant(collection):
    pipeline = [
        {
            "$group": {
                "_id": {
                    "field1": "$field1",
                    "field2": "$field2",
                    "field3": "$field3",
                    # add more fields as needed
                },
                "ids": {
                    "$push": "$_id"
                },
                "count": {
                    "$sum": 1
                }
            }
        },
        {
            "$match": {
                "count": {
                    "$gt": 1
                }
            }
        },
        {
            "$sort": {
                "count": -1
            }
        },
        {
            "$skip": 1
        },
        {
            "$unwind": "$ids"
        },
        {
            "$group": {
                "_id": "$ids",
                "count": {
                    "$sum": 1
                }
            }
        },
        {
            "$match": {
                "count": {
                    "$gt": 1
                }
            }
        },
        {
            "$replaceRoot": {
                "newRoot": {
                    "_id": "$_id"
                }
            }
        }
    ]

    result = collection.aggregate(pipeline)

    for doc in result:
        collection.delete_one({"_id": doc["_id"]})


def edit_array(collection):
    query = {"co_active_industries": {"$regex": r'^\["'}}
    documents = collection.find(query)

    # Update the format of co_active_industries in each document
    for doc in documents:
        old_value = doc["co_active_industries"]
        new_value = [s.strip('"') for s in old_value[1:-1].split(',')]
        collection.update_one({"_id": doc["_id"]}, {"$set": {"co_active_industries": new_value}})
        print(new_value)


if __name__ == '__main__':

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["Industries"]
    collection = db["Companies"]
    edit_array(collection)
    del_redundant(collection)

    # Find all documents with the old format of co_active_industries

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
