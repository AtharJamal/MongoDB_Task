import pymongo
import data_logging

class DB_Operations:
    def __init__(self):
        self.logger_object=data_logging.App_Logger()
        self.file_object=open("DB_Operations_log.txt",'a+')
    def DB_Connection(self,path):
        self.logger_object.log(self.file_object,"About to start Mongo DB connection")
        self.path=path
        try:
            Client=pymongo.MongoClient(self.path)
            self.logger_object.log(self.file_object,"Connection established of Python IDE with MongoDB")
            return Client
        except Exception as e:
            self.logger_object.log("Error while connecting to database")
            return e
    def DB_createdatabase(self,Databasename):
        self.logger_object.log(self.file_object,"About to create database inside MongoDB")
        self.Databasename=Databasename
        try:
            db=self.DB_Connection[self.Databasename]
            self.logger_object.log(self.file_object,"Connection established with database")
            return db
        except Exception as e:
            self.logger_object.log("Error while connecting to database")
            return e
    def DB_createcollection(self,collectionname):
        self.Collectionname=Collectionname
        self.logger_object.log(self.file_object,"About to create collection inside database")
        try:
            coll=self.DB_cretedatabase[self.Collectionname]
            self.logger_object.log(self.file_object,"Connection established with database")
            return coll
        except Exception as e:
            self.logger_object.log("Error while connecting to database")
            return e
    def DB_insertion_one(self,doc):
        self.doc=doc
        self.logger_object.log(self.file_object,"About to start Mongo DB connection")
        try:
            self.DB_cretecollection.insert_one(self.doc)
            self.logger_object.log(self.file_object,"Inserted one record into database")
        except Exception as e:
            self.logger_object.log("Error while connecting to database")
            return e
    def DB_insertion_many(self,doc1):
        self.doc1=doc1
        self.logger_object.log(self.file_object,"About to start Mongo DB connection")
        try:
            self.DB_cretecollection.insert_many(self.doc1)
            self.logger_object.log(self.file_object,"Inserted All records into database")
        except Exception as e:
            self.logger_object.log("Error while connecting to database")
            return e        
            
            
