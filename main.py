import pymongo


def del_redundant(collection):
    from pymongo import MongoClient

    pipeline = [
        {
            "$group": {
                "_id": {
                    "co_name": "$co_name",
                    "city": "$city",
                    "co_active_industries": "$co_active_industries",
                    "Link": "$Link",
                    "Revenue": "$Revenue"
                },
                "ids": {"$push": "$_id"},
                "count": {"$sum": 1}
            }
        },
        {
            "$match": {
                "count": {"$gt": 1}
            }
        }
    ]

    # Identify duplicate documents
    duplicate_groups = collection.aggregate(pipeline)

    # Delete duplicate documents
    for group in duplicate_groups:
        for id in group["ids"][1:]:
            collection.delete_one({"_id": id})


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
    collection = db["companies"]
    edit_array(collection)
    #del_redundant(collection)

    # Find all documents with the old format of co_active_industries

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
