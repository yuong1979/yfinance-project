import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

#setting up firebase connection
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)

db=firestore.client()


# Based on https://youtu.be/N0j6Fe2vAK4

#### Adding and updating data ########

# #adding new collections to the database
# db.collection('persons').add({"name":"John", "Age": 40})

## add documents with auto ids
# data={'name': 'John Smith', 'age': 40, 'employed': True}
# db.collection('people').add(data)
# data={'name': 'Jane Smith', 'age': 35, 'employed': False}
# db.collection('people').add(data)

# ## merging - should be similar to update - document reference is the id - merge true is impt if you dont want destructive writes to project
# db.collection('people').document('hN1AQVbzhWRemlKPkjut').set({'Age':20}, merge=True)

## add documnets with your own ids - discouraged
# data={'name': 'John Smith', 'age': 40, 'employed': True}
# db.collection('people').document('yourownuniqueid').add(data)

# ## adding subcollections - you dont need this
# db.collection('Persons').document('janedoe').collection('movies').add({'name':'Avengers'})



######## Reading data ########

# # Getting the document of a known ID
# result = db.collection('people').document('7kl68GFNdX8uq4K009kL').get()
# if result.exists:
#     print (result.to_dict())

# # Getting all the documents in a collection
# docs = db.collection('people').get()
# for doc in docs:
#     print (doc.to_dict())

# # Getting all the documents in a collection where certain criteria is required
# docs = db.collection('people').where("age", "==", 40).get()
# for doc in docs:
#     print (doc.to_dict())

# # Getting all the documents in a collection where certain criteria is required
# docs = db.collection('people').where("name", "==", "John").get()
# for doc in docs:
#     print (doc.to_dict())


# # Getting all the documents in a collection where certain criteria is required
# docs = db.collection('people').where("socials", "array_contains", "linkedin").get()
# for doc in docs:
#     print (doc.to_dict())


# # Getting all the documents in a collection where certain criterias is required
# docs = db.collection('people').where("address", "in", ["Singapore", "Bangkok"]).get()
# for doc in docs:
#     print (doc.to_dict())




######## update data ########

# #with known key
# #note that if the field does not exist it will be created - just like added a new field/data
# db.collection('people').document('hN1AQVbzhWRemlKPkjut').update({'age':'25'})
# db.collection('people').document('hN1AQVbzhWRemlKPkjut').update({'address':'London'})

# ## does not work
# # db.collection('people').document('hN1AQVbzhWRemlKPkjut').update({'socials': firestore.ArrayRemove(['instagram'])})
# # db.collection('people').document('hN1AQVbzhWRemlKPkjut').update({'socials': firestore.ArrayUnion(['instagram'])})

# #with unknown key
# docs = db.collection('people').get()
# for doc in docs:
#     if doc.to_dict()['age'] >= 39:
#         key = doc.id
#         print (key)
#         db.collection('people').document(key).update({"agegroup": "middle aged"})

# #second way much more recommended because less writes to the db - only getting docs related to the query
# docs = db.collection('people').where('age', ">=", 50).get()
# for doc in docs:
#     key = doc.id
#     db.collection('people').document(key).update({"agegroup": "older than 50"})


######## delete data ########
# #for known ids
# db.collection('people').document('5dDYonUBnOatTXmfeblb').delete()

# #for certain fields of known ids
# db.collection('people').document('5dDYonUBnOatTXmfeblb').update({"socials":firestore.DELETE_FIELD})

# # delete docs of unknown id
# docs = db.collection('people').where("age", ">=", 100).get()
# for doc in docs:
#     key = doc.id
#     db.collection('people').document(key).delete()

# # delete multiple fields in selected
# docs = db.collection('people').where("age", ">=", 120).get()
# for doc in docs:
#     key = doc.id
#     db.collection('people').document(key).update({"age": firestore.DELETE_FIELD, "name": firestore.DELETE_FIELD})



######## Reading data on firestore ########

# https://youtu.be/Ofux_4c94FI
