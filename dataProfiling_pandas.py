import pandas as pd

def dataprofile(data_all_df,data_cols,database_name,table_name):

    data_df = data_all_df
    columns2Bprofiled = data_df.columns
    database_name = database_name
    table_name = table_name

    dprof_df = pd.DataFrame({'database_name':[database_name] * len(data_df.columns),\
                             'table_name':[table_name] * len(data_df.columns),\
                             'column_names':data_df.columns,\
                             'data_types':[x for x in data_df.dtypes]})
    dprof_df = dprof_df[['database_name','table_name','column_names', 'data_types']]
    dprof_df.set_index('column_names', inplace=True, drop=False)

    # ======== Step 1 ==============
    num_rows = data_df.count()
    dprof_df['num_rows'] = num_rows

    # ======== Step 2 ==============
    # number of rows with nulls and nans
    df_nacounts = data_df.isnull().sum().transpose()
    df_nacounts = df_nacounts.reset_index()
    df_nacounts.columns = ['column_names','num_null']
    dprof_df = pd.merge(dprof_df, df_nacounts, on = ['column_names'], how = 'left')

    # ============ Step 3 ============
    # number of rows with white spaces (one or more space) or blanks
    dprof_df['num_spaces'] = dprof_df['num_null']
    dprof_df['num_blank'] = dprof_df['num_null']

    # ========== Step 4 ===============
    # using the in built describe() function
    desc_df = data_df.describe().transpose()
    desc_df=desc_df[['count', 'mean', 'std', 'min', 'max']]
    desc_df = desc_df.iloc[:, :]
    desc_df = desc_df.reset_index()
    desc_df.columns.values[0] = 'column_names'
    desc_df = desc_df[['column_names','count', 'mean', 'std']]
    dprof_df = pd.merge(dprof_df, desc_df , on = ['column_names'], how = 'left')


    # =============== Step 5 ============================

    allminvalues = data_df[columns2Bprofiled].min(axis=0,skipna=False)
    allmaxvalues = data_df[columns2Bprofiled].max(axis=0, skipna=False)
    
    x = [(x,data_df.where(data_df[x] == y, axis=0)[x].count()) for x,y in zip(columns2Bprofiled, allminvalues)]
    allmincounts = pd.DataFrame(x, columns=['column_names','counts_min'])

    y = [(x, data_df.where(data_df[x] == y, axis=0)[x].count()) for x, y in zip(columns2Bprofiled, allmaxvalues)]
    allmaxcounts = pd.DataFrame(y, columns=['column_names', 'counts_max'])


    df_counts = dprof_df[['column_names']]
    df_counts.loc[:,'min'] = allminvalues.values
    df_counts.loc[:, 'max'] = allmaxvalues.values

    df_counts = pd.merge(df_counts, allmincounts,on='column_names')
    df_counts = pd.merge(df_counts, allmaxcounts, on='column_names')

    df_counts = df_counts[['column_names','min','counts_min','max','counts_max']]
    dprof_df = pd.merge(dprof_df, df_counts , on = ['column_names'], how = 'left')



    # ============= Step 6 =============================
    # number of distinct values in each column
    dprof_df['num_distinct'] = [len(data_df[x].unique().tolist()) for x in columns2Bprofiled]

    # ================ Step 7 ============================
    # most frequently occuring value in a column and its count
    dprof_df['most_freq_valwcount'] = [data_df[x].value_counts().sort_values(ascending=False).head(1).reset_index().values.tolist()[0] for x in columns2Bprofiled]
    dprof_df['most_freq_value'] = [x[0] for x in dprof_df['most_freq_valwcount']]
    dprof_df['most_freq_value_count'] = [x[1] for x in dprof_df['most_freq_valwcount']]
    dprof_df = dprof_df.drop(['most_freq_valwcount'],axis=1)


    # least frequently occuring value in a column and its count
    dprof_df['least_freq_valwcount'] = [data_df[x].value_counts().sort_values(ascending=True).head(1).reset_index().values.tolist()[0] for x in columns2Bprofiled]
    dprof_df['least_freq_value'] = [x[0] for x in dprof_df['least_freq_valwcount']]
    dprof_df['least_freq_value_count'] = [x[1] for x in dprof_df['least_freq_valwcount']]
    dprof_df = dprof_df.drop(['least_freq_valwcount'],axis=1)

    return dprof_df

# Set the spark session and conf parameters

df = pd.read_csv("dataprofiling_file1.csv", header =0)
cols2profile = df.columns
dprofile = dataprofile(df,cols2profile,'test_db','test_table')
print(dprofile)

