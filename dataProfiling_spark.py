#spark2-submit --master yarn --deploy-mode client --driver-memory 2g --num-executors 50 --executor-memory 30g --executor-cores 5 --conf spark.key1=$1 --conf spark.key2=$2 dataProfiling_spark.py
import pandas as pd
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col
import time

def dataprofile(data_all_df,data_cols,database_name,table_name):
    print("Starting data profiling for "+ database_name + " " + table_name + " and for "+ str(len(data_cols))+" columns")
    data_df = data_all_df.select(data_cols)

    columns2Bprofiled = data_cols
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

    print("Step 1: Num rows calculation: completed....")

    # ======== Step 2 ==============
    # number of rows with nulls and nans
    df_nacounts = data_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in data_df.columns if data_df.select(c).dtypes[0][1]!='timestamp']).toPandas().transpose()
    df_nacounts = df_nacounts.reset_index()
    df_nacounts.columns = ['column_names','num_null']
    dprof_df = pd.merge(dprof_df, df_nacounts, on = ['column_names'], how = 'left')

    print("Step 2: Num rows with NAN/Null: completed....")

    # ============ Step 3 ============
    # number of rows with white spaces (one or more space) or blanks
    dprof_df['num_spaces'] = [data_df.where(F.col(c).rlike('^\\s+$')).count() for c in data_df.columns]
    dprof_df['num_blank'] = [data_df.where(F.col(c)=='').count() for c in data_df.columns]

    print("Step 3: White spaces/blanks: completed....")

    # ========== Step 4 ===============
    # using the in built describe() function
    desc_df = data_df.describe().toPandas().transpose()
    desc_df.columns = desc_df.iloc[0]
    desc_df=desc_df[['count', 'mean', 'stddev', 'min', 'max']]
    desc_df = desc_df.iloc[1:,:]
    desc_df = desc_df.reset_index()
    desc_df.columns.values[0] = 'column_names'
    desc_df = desc_df[['column_names','count', 'mean', 'stddev']]
    dprof_df = pd.merge(dprof_df, desc_df , on = ['column_names'], how = 'left')

    print("Step 4:Statistic metrics: completed....")

    # =============== Step 5 ============================
    # max / min values and counts
    allminvalues = [data_df.select(F.min(x)).limit(1).toPandas().iloc[0][0] for x in columns2Bprofiled]
    allmaxvalues = [data_df.select(F.max(x)).limit(1).toPandas().iloc[0][0] for x in columns2Bprofiled]

    df_counts = dprof_df[['column_names']]
    df_counts.insert(loc=0, column='min', value=allminvalues)
    df_counts.insert(loc=0, column='max', value=allmaxvalues)

    allmincounts = [data_df.where(col(x) == str(y)).count() for x, y in zip(columns2Bprofiled, allminvalues)]
    allmaxcounts = [data_df.where(col(x) == str(y)).count() for x, y in zip(columns2Bprofiled, allmaxvalues)]

    df_counts.insert(loc=0, column='counts_min', value=allmincounts)
    df_counts.insert(loc=0, column='counts_max', value=allmaxcounts)

    df_counts = df_counts[['column_names','min','counts_min','max','counts_max']]
    dprof_df = pd.merge(dprof_df, df_counts , on = ['column_names'], how = 'left')

    print("Step 5: Max/Min calculation: completed....")

    # ============= Step 6 =============================
    # number of distinct values in each column
    dprof_df['num_distinct'] = [data_df.select(x).distinct().count() for x in columns2Bprofiled]

    print("Step 6: Distinct values: completed....")

    # ================ Step 7 ============================
    # most frequently occuring value in a column and its count
    dprof_df['most_freq_valwcount'] = [data_df.groupBy(x).count().sort("count",ascending=False).limit(1).toPandas().iloc[0].values.tolist() for x in columns2Bprofiled]
    dprof_df['most_freq_value'] = [x[0] for x in dprof_df['most_freq_valwcount']]
    dprof_df['most_freq_value_count'] = [x[1] for x in dprof_df['most_freq_valwcount']]
    dprof_df = dprof_df.drop(['most_freq_valwcount'],axis=1)


    # least frequently occuring value in a column and its count
    dprof_df['least_freq_valwcount'] = [data_df.groupBy(x).count().sort("count",ascending=True).limit(1).toPandas().iloc[0].values.tolist() for x in columns2Bprofiled]
    dprof_df['least_freq_value'] = [x[0] for x in dprof_df['least_freq_valwcount']]
    dprof_df['least_freq_value_count'] = [x[1] for x in dprof_df['least_freq_valwcount']]
    dprof_df = dprof_df.drop(['least_freq_valwcount'],axis=1)

    print("Step 7: Frequency calculation: completed....")

    return dprof_df

# Set the spark session and conf parameters
spark = SparkSession.builder.appName("Data Profiling").getOrCreate()
spark.conf.set("spark.debug.maxToStringFields", 100)
spark.sparkContext.setLogLevel("ERROR")

start_time = time.time()

database_name = spark.conf.get("spark.key1")
table_name = spark.conf.get("spark.key2")
sql = 'select * from '+database_name+'.'+table_name
df = spark.sql(sql)

cols2profile = df.columns
dprofile = dataprofile(df,cols2profile,database_name,table_name)
print(dprofile)

print("\nOverall time taken..",time.time()- start_time," seconds\n")
spark.stop()
