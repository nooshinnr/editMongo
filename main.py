# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import pymongo

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["Industries"]
    collection = db["Companies"]

    # Find all documents with the old format of co_active_industries
    query = {"co_active_industries": {"$regex": r'^\["'}}
    documents = collection.find(query)

    # Update the format of co_active_industries in each document
    for doc in documents:
        old_value = doc["co_active_industries"]
        new_value = [s.strip('"') for s in old_value[1:-1].split(',')]
        collection.update_one({"_id": doc["_id"]}, {"$set": {"co_active_industries": new_value}})
        print(new_value)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
