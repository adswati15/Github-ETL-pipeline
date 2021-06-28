from DataSources import Extract
from DataLoad import MongoDB
import urllib
import pandas as pd
import numpy as np

class Transformation:
    
    def __init__(self, dataSource, dataSet):
      
        # creating Extract class object here, to fetch data using its generic methods for API
        extractObj = Extract()
        
        if dataSource == 'api':
            self.data = extractObj.getAPISData(dataSet)
            funcName = dataSource+dataSet
            
            # getattr function takes in function name of class and calls it.
            getattr(self, funcName)()
        else:
            print('Unkown Data Source!!! Please try again...')
            

        
    # Github Data Transformation
    def apiGithub(self):
        github_data = self.data['items']
        df = pd.DataFrame(columns = ["RepositoryName", "CreatedDate","Language","stars","forks","watcher"])
        for repository in github_data:
            df["RepositoryName"] = repository["full_name"]
            df["CreatedDate"] = repository["created_at"]
            df["language"] = repository["language"]
            df["stars"] = repository["stargazers_count"]
            df["forks"] = repository["forks_count"]
            df["watcher"] = repository["watchers_count"]
                
        
        
        # connection to mongo db
        mongoDB_obj = MongoDB(urllib.parse.quote_plus('root'), urllib.parse.quote_plus('password'), 'host', 'GithubAPI')
        # Insert Data into MongoDB
        mongoDB_obj.insert_into_db(df, 'Github_API_Repos')

