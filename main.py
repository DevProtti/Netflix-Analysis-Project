# %%
import os
import findspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.functions import col, round, mean, sum


# %%
# Initialize the spark environment
os.environ["JAVA_HOME"] = 'C:\Gabriel\Programming\Java\jdk'
os.environ["SPARK_HOME"] = 'C:\Gabriel\Programming\Spark\spark-3.5.0-bin-hadoop3'

# Initialize findspark lib
findspark.init()

spark = SparkSession.builder\
        .master('local')\
        .appName("NFLX_Report")\
        .getOrCreate()
spark.sparkContext.setLogLevel("DEBUG")

# %%
# Transforming excel sheets in csv and splitting in 3 different files

def transform_excel_to_csv(source_path, source_file_name, sheet_name, new_file_name):
    source_file_path = os.path.join(source_path, source_file_name)
    pandas_df = pd.read_excel(source_file_path, sheet_name=sheet_name)
    pandas_df.to_csv(f'{source_path}/{new_file_name}.csv', header=True, index=False)
    

source_path = 'data'
source_file_name = "NFLX_DS_10_23_Data.xlsx"

# Creating a csv file for NFLX Top 10 sheet
excel_sheet = 'NFLX Top 10'
transform_excel_to_csv(source_path, source_file_name, excel_sheet, 'NFLX_Top_10')

# Creating a csv file for IMDB Rating sheet
excel_sheet = 'IMDB Rating'
transform_excel_to_csv(source_path, source_file_name, excel_sheet, 'IMDB_Rating')

# Creating a csv file for Runtime sheet
excel_sheet = 'Runtime'
transform_excel_to_csv(source_path, source_file_name, excel_sheet, 'Runtime')

# %% [markdown]
# ## 1st Question:
#     > Topics:
#         - Read the dataset NFLX Top 10
#         - Filter the data (ignoring the week of May 22nd and getting only TV (English) category)
#         - Count how many times each show title appeared in the dataset
#         - Select the title with the most appearances

# %%

# Reading Top 10 csv files with spark
file_name = 'NFLX_Top_10'
df_top10 = spark.read.option("header", 'true').csv(f'./data/{file_name}.csv', inferSchema=True)

outage_week = '2022-05-22'
category = 'TV (English)'

# Filtering the dataset to exclude the week of May 22nd and retain only entries from the TV (English) category
df_top10_filtered = df_top10.filter((df_top10['week'] != outage_week) 
                                                & (df_top10['category'] == category))

# Calculating the amout of times each show title appeared in top 10 dataset
result_set_df = df_top10_filtered.groupBy('show_title').count()
result_set_df.orderBy(result_set_df['count'].desc()).show(5)
most_appeared_tv_show = result_set_df.orderBy(result_set_df['count'].desc()).first()    
print(f"The most frequent English-language TV show in the NFLX Top 10 dataset was "\
      f"\"{most_appeared_tv_show['show_title']}\" which appeared {most_appeared_tv_show['count']} times.")



# %% [markdown]
# ## 2nd Question
# 
#     > Topics:
#         - Read the datasets (NFTL_Top_10 and IMDB_Rating)
#         - Filter the data (ignoring the week of May 22nd and getting only Films (English) category)
#         - Relate the datasets by show title name
#         - Get the title with the lowest rating
#         - Get the average from weekly_hours_viewed from each week

# %% [markdown]
# Lowest rating

# %%
# Filtering the the NFLX Top 10 dataset by excluding the week of May 22nd and
# retaining only entries from the TV (English) category
category = 'Films (English)'
df_top10_filtered = df_top10.filter((df_top10['week'] != outage_week) 
                                                & (df_top10['category'] == category))
# Reading the IMDB Ratings dataset
file_name = 'IMDB_Rating'
df_imdb_rating = spark.read.option("header", 'true').csv(f'./data/{file_name}.csv', inferSchema=True)

# Left joining Netflix Top 10 (filtered) dataset with IMDB ratings dataset
result_set = df_top10_filtered.join(df_imdb_rating, 
                                    df_top10_filtered['show_title']==df_imdb_rating['title'],
                                    how='left')\
                              .select(["week", "show_title", "weekly_rank", "weekly_hours_viewed", "rating"])

# Calculating rating average to handle different ratings for the same title
df_title_rating = result_set.groupBy(["week","show_title"])\
                            .mean("rating")\
                            .withColumnRenamed("avg(rating)", "rating (avg)")
                            
# Filtering where the average is not equals to zero
df_title_rating = df_title_rating.filter(df_title_rating["rating (avg)"] != 0)

# Getting the title with the lowest rating
df_title_rating.orderBy("rating (avg)").show(1)
title_with_lowest_rating = df_title_rating.orderBy("rating (avg)").first()
print(f"The title with the lowest rating was "\
      f"\"{title_with_lowest_rating['show_title']}\", with a rating of {title_with_lowest_rating['rating (avg)']}")


## OBS: I could do this without calculating the average to handle different ratings for the same title,
## and the result would be the same, but in this particular case I think it is more coherent calculating the average


# %% [markdown]
# Average hour viewed

# %%
# Filtering the the NFTL Top 10 dataset by excluding the week of May 22nd and
# retaining only entries from the Films (English) category
category = 'Films (English)'
df_top10_filtered = df_top10.filter((df_top10['week'] != outage_week) 
                                                & (df_top10['category'] == category))
# Reading the IMDB Ratings dataset
file_name = 'IMDB_Rating'
df_imdb_rating = spark.read.option("header", 'true').csv(f'./data/{file_name}.csv', inferSchema=True)

# Left joining Netflix Top 10 (filtered) dataset with IMDB ratings dataset
result_set = df_top10_filtered.join(df_imdb_rating, 
                                    df_top10_filtered['show_title']==df_imdb_rating['title'],
                                    how='left')\
                              .select(["week", "category" ,"show_title", "weekly_rank", "weekly_hours_viewed", "rating"])

result_set.select("category", "show_title",'rating').orderBy(result_set['rating']).filter(result_set['rating'] != 0)

# Investigating the amout of weeks the show title appeared
weeks_show_appeared = result_set.filter(result_set["show_title"] == '365 Days: This Day')

# Calculating the average hours viewed
average_hours_viewed = weeks_show_appeared.groupBy("show_title").agg(mean("weekly_hours_viewed").cast(LongType()).alias("average_hours_viewed"))
average_hours_viewed.show()                           



# %% [markdown]
# ## Question 3
#     > Topics:
#         - Filter the top 10 Dataset to get Films (Non-English) only
#         - Order by de the cumulative_weeks_in_top_10 de
#         - Estimate the total viewers

# %% [markdown]
# Determining the title that has spent the most time in the top 10 week

# %%
outage_week = '2022-05-22'
category = 'Films (Non-English)'

# Filtering the dataset by the outage week and category
file_name = 'NFLX_Top_10'
df_top10 = spark.read.option("header", 'true').csv(f'./data/{file_name}.csv', inferSchema=True)
df_non_english_films = df_top10.filter((df_top10["week"] != outage_week) & (df_top10["category"] == category))

# Displaying only the title that has spent the most week in top 10
df_non_english_films.orderBy(df_non_english_films["cumulative_weeks_in_top_10"].desc()).show(1)
non_english_title_most_weeks_top_10 = df_non_english_films.orderBy(df_non_english_films["cumulative_weeks_in_top_10"].desc()).first()

print(
f"""The non English language title that has spent the most weeks in top 10 \
was \"{non_english_title_most_weeks_top_10['show_title']}\" with \
{non_english_title_most_weeks_top_10['cumulative_weeks_in_top_10']} weeks""")

# %% [markdown]
# Estimating the amount of viewers for this title

# %%
# Filtering df_non_english_films Data Frame by the title "Through My Window"
show_title = "Through My Window"
df_filtered_by_show_title = df_non_english_films.filter(df_non_english_films["show_title"] == show_title)

# Reading the Runtime Dataset
df_runtime = spark.read.option("header", 'true').csv("data/Runtime.csv")

# Filtering the df_runtime by the title
df_runtime_filtered_by_title = df_runtime.filter(df_runtime["title"] == show_title)

# Calculating the aproximate number of viewers for each week
df_weekly_viewers = df_filtered_by_show_title.crossJoin(df_runtime_filtered_by_title) \
                .withColumn("weekly_viewers", round(col("weekly_hours_viewed") / (col("runtime") / 60), 0).cast("int")) \
                .select("week","show_title", "weekly_viewers")

df_weekly_viewers.show()

# Estimating the number of users who watched 'Through My Window' during the period from 2022-04-03 to 2022-06-26
amount_of_users = df_weekly_viewers.groupBy("show_title").agg(sum("weekly_viewers").alias("Total_viewers"))
amount_of_users.show()



