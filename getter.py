class Getter:
    '''Responsible for getting information from dataframe 
        like column names and partial dataframe'''
    def get_column_names(self, df, depth):
        pattern = str(depth) + str(' ')
        columns = [c for c in df.columns if pattern in c]
        for c in columns:
            if 'name' in c.lower():
                name = c
            if 'url' in c.lower():
                url = c
            if 'id' in c.lower():
                id = c
        return name, id, url

    def get_first_node(self, df, name, id, url, depth):
        return df.select(name, id, url)\
            .filter(df['depth'] == depth)\
            .withColumnRenamed(name, 'label')\
            .withColumnRenamed(id, 'ID')\
            .withColumnRenamed(url, 'link')

    def get_subsequent_node(self, df, parent, name, id, url, depth):
        return df.select(parent, name, id, url)\
            .filter(df['depth'] == depth)\
            .withColumnRenamed(parent, 'pid')\
            .withColumnRenamed(name, 'label')\
            .withColumnRenamed(id, 'ID')\
            .withColumnRenamed(url, 'link')