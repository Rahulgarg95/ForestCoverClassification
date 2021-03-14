from datetime import datetime
from os import listdir
from application_logging.logger import App_Logger
import pandas as pd
from AzureBlobStorage.azureBlobStorage import AzureBlobStorage

class dataTransform:

     """
               This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

               Written By: iNeuron Intelligence
               Version: 1.0
               Revisions: None

               """

     def __init__(self):
          self.goodDataPath = 'Training_Good_Raw_Files_Validated'
          self.logger = App_Logger()
          self.azureObj = AzureBlobStorage()

     def addQuotesToStringValuesInColumn(self):
          """
                            Method Name: addQuotesToStringValuesInColumn
                            Description: This method converts all the columns with string datatype such that
                                        each value for that column is enclosed in quotes. This is done
                                        to avoid the error while inserting string values in table as varchar.

          """

          #log_file = open("Training_Logs/addQuotesToStringValuesInColumn.txt", 'a+')
          log_file = 'addQuotesToStringValuesInColumn'
          try:
               #onlyfiles = [f for f in listdir(self.goodDataPath)]
               onlyfiles = self.azureObj.listDirFiles(self.goodDataPath)
               for file in onlyfiles:
                    #data = pd.read_csv(self.goodDataPath+"/" + file)
                    data = self.azureObj.csvToDataframe(self.goodDataPath, file)
                    data['class'] = data['class'].apply(lambda x: "'" + str(x) + "'")
                    self.azureObj.saveDataframeToCsv(self.goodDataPath,file,data)
                    #data.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                    self.logger.log(log_file," %s: Quotes added successfully!!" % file)
          except Exception as e:
               self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
