#!/usr/bin/env python
# coding: utf-8

# ## 02_Silver_to_Gold
# 
# null

# In[7]:


from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window


# Dimensional modeling


# Define the reporting window
start_year, end_year = 2019, 2023

# 1. dim_year (Hardcoded for the focus window)
data_years = [(y,) for y in range(start_year, end_year +1)]
print(data_years)

dim_year = spark.createDataFrame(data_years, ["Years"])
display(dim_year)

dim_year.write.format("delta").mode("overwrite").saveAsTable("AMR_Gold.gold.dim_year")
print("Gold table 'dim_year created successfully")


# In[27]:


# Helper function to create dimensions with an ID column
def create_dimension(source_table, column_name, dim_table_name, id_col_name):
    df = spark.read.table(f"AMR_Silver.silver.{source_table}")
    # Filter out nulls and get distinct values
    df_distinct = df.filter(col(column_name).isNotNull()).select(col(column_name)).distinct()
    # Add a surrogate key (ID)
    w = Window.orderBy(column_name)
    df_dim = df_distinct.withColumn(id_col_name, row_number().over(w)).select(id_col_name, column_name)
    df_dim.write.format("delta").mode("overwrite").saveAsTable(f"AMR_Gold.gold.{dim_table_name}")
    return df_dim


# 2. dim_geography (from coverage maps)
create_dimension("coverage_maps", "CountryTerritoryArea", "dim_geography", "Geography_ID" )
print("Gold table 'dim_geography' created successfully")

# 3. dim_infection (from coverage maps)
create_dimension("coverage_maps", "Raw_Infection_Type", "dim_infection", "Infection_ID")
print("Gold table 'dim_infection' created successfully")

# 4. dim_pathogen (from AMR signal table)
create_dimension("pathogen_antibiotic_coverage", "PathogenName", "dim_pathogen", "Pathogen_ID")
print("Gold table 'dim_pathogen' created successfully")

# 5. dim_antibiotic (from AMR signal table)
# Filter out empty string antibiotics first
df_abx_raw = spark.read.table("AMR_Silver.silver.pathogen_antibiotic_coverage")
df_abx_clean = df_abx_raw.filter((col("AntibioticName").isNotNull()) & (col("AntibioticName") != ""))
w_abx = Window.orderBy("AntibioticName")
dim_antibiotic = df_abx_clean.select("AntibioticName").distinct().withColumn("Antibiotic_ID", row_number().over(w_abx))
dim_antibiotic.write.format("delta").mode("overwrite").saveAsTable("AMR_Gold.gold.dim_antibiotic")
print("Gold table 'dim_antibiotic' created successfully")


# In[43]:


# Building Fact tables
'''
filter data to the 2019-2023 window and 
swap the raw text names for the integer IDs  created. 
This ensures your Power BI DirectLake connection runs fast.
'''

# Load the dimension tables into memory for join
dim_geo = spark.read.table("AMR_Gold.gold.dim_geography")
dim_inf = spark.read.table("AMR_Gold.gold.dim_infection")
dim_path = spark.read.table("AMR_GOLD.gold.dim_pathogen")
dim_abx = spark.read.table("AMR_Gold.gold.dim_antibiotic")

# 1. fact_country_coverage
# Answers: "Where is data missing?"
df_cov = spark.read.table("AMR_Silver.silver.coverage_maps") \
    .filter(col("Year").between(start_year, end_year))
display(df_cov)

fact_coverage = df_cov \
    .join(dim_geo, df_cov.CountryTerritoryArea == dim_geo.CountryTerritoryArea, "left") \
    .join(dim_inf, df_cov.Raw_Infection_Type == dim_inf.Raw_Infection_Type, "left") \
    .select("Year", "Geography_ID", "Infection_ID","BCIsPerMillion", "TotalSpecimenIsolates", "IsolsPerMillion", "TotalSpecimenIsolatesWithAST", "ASTResult")

display(fact_coverage)

fact_coverage.write.format("delta").mode("overwrite").saveAsTable("AMR_Gold.gold.fact_country_coverage")
print("Gold table 'fact_country_coverage' created successfully")

# 2. fact_global_pathogen
# Answers: "which pathogens are well monitored?"
df_path_inf = spark.read.table("AMR_Silver.silver.pathogen_infection_coverage") \
    .filter(col("Year"). between(start_year, end_year))

display(df_path_inf)

fact_pathogen = df_path_inf \
    .join(dim_inf, df_path_inf.Specimen == dim_inf.Raw_Infection_Type, "left") \
    .join(dim_path, df_path_inf.PathogenName == dim_path.PathogenName, "left") \
    .select("Year", "Infection_ID", "Pathogen_ID", "CTAs_Reported_BCIs", "CTAs_Reported_BCIs_AST_80", "Total_BCIs", "Total_BCIs_AST", "Median")

display(fact_pathogen)

fact_pathogen.write.format("delta").mode("overwrite").saveAsTable("AMR_Gold.gold.fact_global_pathogen")
print("Gold table 'fact_global_pathogen' created successfully.")


# 3. fact_global_amr
# Answers: "what are the resistance trends?"
df_amr = spark.read.table("AMR_Silver.silver.pathogen_antibiotic_coverage") \
    .filter(col("Year").between(start_year, end_year))

display(df_amr)

fact_amr = df_amr \
    .join(dim_inf, df_amr.Specimen == dim_inf.Raw_Infection_Type, "left") \
    .join(dim_path, df_amr.PathogenName == dim_path.PathogenName, "left") \
    .join(dim_abx, df_amr.AntibioticName == dim_abx.AntibioticName, "left") \
    .filter(col("Antibiotic_ID").isNotNull()) \
    .select("Year", "Infection_ID", "Pathogen_ID", "Antibiotic_ID", "Total_BCIs", "Total_BCIs_AST", "Min", "Q1", "Median", "Q3", "Max")

display(fact_amr)

fact_amr.write.format("delta").mode("overwrite").saveAsTable("AMR_Gold.gold.fact_global_amr")
print("Gold table 'fact_global_amr' created successfully.")


# 4. fact_system_capacity
# Answers: "Are national surveillance systems improving?"
df_cta = spark.read.table("AMR_Silver.silver.cta_reporting_status") \
    .filter(col("Year").between(start_year, end_year))

display(df_cta)

df_cta.write.format("delta").mode("overwrite").saveAsTable("AMR_Gold.gold.fact_system_capacity")
print("Gold table 'fact_system_capacity' created successfully")


# 5. fact_current_indicators

# Note: The 'system_indicators' table represents the current state (no year column),
# so pass it straight to Gold without the year filter

spark.read.table("AMR_Silver.silver.system_indicators").write.format("delta").mode("overwrite").saveAsTable("AMR_Gold.gold.fact_current_indicators")
print("Gold table 'fact_current_indicators' created successfully.")

