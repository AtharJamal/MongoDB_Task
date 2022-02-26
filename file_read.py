import data_logging
import pandas as pd
class fileread():
    def __init__(self,path):
        self.path=path
        self.logger_object=data_logging.App_Logger()
        self.file_write=open("log_file.txt",'a+')
    def csv_reader(self):
        self.logger_object.log(self.file_write,"About to start the file reading operation")
        try:
            self.data=pd.read_csv(self.path,sep=";")
            self.logger_object.log(self.file_write,"Data Load Successful.Exited the csv_reader method of the fileread class")
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_write,"Exception occured at csv_reader method of the fileread class")
            self.logger_object.log(self.file_write,"Data Load Unsuccessful.Exited the csv_reader method of the fileread class")
            raise e
