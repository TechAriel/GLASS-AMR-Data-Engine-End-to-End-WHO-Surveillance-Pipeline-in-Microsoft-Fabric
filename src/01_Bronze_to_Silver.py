#!/usr/bin/env python
# coding: utf-8

# ## 01_Bronze_to_Silver_Notebook
# 
# null

# In[3]:


# Standardising the "Global Maps files"-Coverage Layer
'''
This block reads all the 20 files in this category at once,
extracts the year and infection type from the filenames,
cleans the "NA" strings, and saves it as a single table
'''

from pyspark.sql.functions import input_file_name, regexp_extract, col, when, regexp_replace, monotonically_increasing_id, split
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# 1. Read all the 20 raw text files from the Bronze Coverage Layer simultaneously
bronze_maps_path = "abfss://6005e045-448a-48b4-9ec6-e08b9f150edd@onelake.dfs.fabric.microsoft.com/672258a3-02a8-4727-8aa8-6839bafb76aa/Files/Bronze_Landing/Coverage_Layer/Global maps *"
df_raw_text = spark.read.text(bronze_maps_path).withColumn("filename", input_file_name())

# data validation display step
display(df_raw_text)

# 2. Extract Year and Infection Type from the filename
# Filename pattern: .../Global maps of testing covergage by infection type_2023-URINE.csv
df_enriched = df_raw_text \
    .withColumn("Year", regexp_extract(col("filename"), r"_(\d{4})-", 1 ).cast("int")) \
    .withColumn("Raw_Infection_Type", regexp_extract(col("filename"), r"-([A-Z]+)\.csv", 1))

display(df_enriched)

# 3. Standardise Infection Types 
df_enriched = df_enriched \
    .withColumn("Infection_Type",
        when(col("Raw_Infection_Type") == "URINE", "Urinary tract")
        .when(col("Raw_Infection_Type") == "UROGENITAL", "Gonorrhoea")
        .when(col("Raw_Infection_Type") == "STOOL", "Gastrointestinal")
        .when(col("Raw_Infection_Type") == "BLOOD", "Bloodstream")
        .otherwise("Unknown")) 

display(df_enriched)

# 4. Find the header row and filter out the metadata above it
# The true data starts immediately after the row containing "CountryTerritoryArea"
df_filtered = df_enriched.filter(
    ~col("value").contains("Filters") &
    ~col("value").contains("filter:") &
    ~col("value").rlike(r"^\s*$") & # Remove empty lines
    ~col("value").contains("CountryTerritoryArea") # Remove the header string
    )

display(df_filtered)

# 5. Split the CSV string into actual columns
split_col = split(col("value"), '","|,"|,') # handle both quoted and unquoted CSV formats
df_split = df_filtered \
    .withColumn("CountryTerritoryArea", regexp_replace(split_col.getItem(0), '"', '')) \
    .withColumn("BCIsPerMillion", regexp_replace(split_col.getItem(1), '"', '').cast("float")) \
    .withColumn("TotalSpecimenIsolates", regexp_replace(split_col.getItem(2), '"', '').cast("int")) \
    .withColumn("IsolsPerMillion", regexp_replace(split_col.getItem(3), '"','').cast("float")) \
    .withColumn("TotalSpecimenIsolatesWithAST", regexp_replace(split_col.getItem(4), '"', '').cast("int")) \
    .withColumn("ASTResult", regexp_replace(split_col.getItem(5), '"','').cast("float"))

display(df_split)

# 6. Final Clean: Fix Encoding and Select Columns
df_maps_final = df_split \
    .withColumn("CountryTerritoryArea", regexp_replace(col("CountryTerritoryArea"), "C<U\+00F4>te d<U\+2019>Ivoire", "Côte dIvoire" )) \
    .select("Year", "Infection_Type", "Raw_Infection_Type", "CountryTerritoryArea","BCIsPerMillion", "TotalSpecimenIsolates", "IsolsPerMillion","TotalSpecimenIsolatesWithAST", "ASTResult" )

display(df_maps_final)

# 7. Write to Silver
df_maps_final.write.format("delta").mode("overwrite").saveAsTable("AMR_Silver.silver.coverage_maps")
print("Silver table 'coverage_maps' created successfully")


# In[9]:


# Standardising the high-level testing volume dataset

# 1. Read the raw text of the macro coverage file
bronze_macro_path = "abfss://6005e045-448a-48b4-9ec6-e08b9f150edd@onelake.dfs.fabric.microsoft.com/672258a3-02a8-4727-8aa8-6839bafb76aa/Files/Bronze_Landing/Coverage_Layer/Testing coverage by infection type_All.csv"
df_macro_text = spark.read.text(bronze_macro_path)

display(df_macro_text)

# 2. Filter out the WHO metadata and the header string
df_macro_filtered = df_macro_text.filter(
        ~col("value").contains("Filters") & 
        ~col("value").contains("filter:") &
        ~col("value").rlike(r"^\s*$") &
        ~col("value").contains("Year")
)

display(df_macro_filtered)

# 3. Split the CSV string
split_macro = split(col("value"), '","|,"|,')
df_macro_split = df_macro_filtered \
    .withColumn("Year", regexp_replace(split_macro.getItem(0), '"', '').cast("int")) \
    .withColumn("Specimen", regexp_replace(split_macro.getItem(1),'"', '' )) \
    .withColumn("CTAs_Reported_BCIs", regexp_replace(split_macro.getItem(2), '"', '')) \
    .withColumn("CTAs_Reported_BCIs_AST_80", regexp_replace(split_macro.getItem(3), '"', '')) \
    .withColumn("Total_BCIs", regexp_replace(split_macro.getItem(4), '"', '')) \
    .withColumn("Total_BCIs_AST", regexp_replace(split_macro.getItem(5), '"', '')) \
    .withColumn("Min", regexp_replace(split_macro.getItem(6), '"', '')) \
    .withColumn("Q1", regexp_replace(split_macro.getItem(7), '"', '')) \
    .withColumn("Median", regexp_replace(split_macro.getItem(8), '"', '')) \
    .withColumn("Q3", regexp_replace(split_macro.getItem(9), '"', '')) \
    .withColumn("Max", regexp_replace(split_macro.getItem(10), '"', ''))

display(df_macro_split)

# 4. Standardise Specimen names to match the coverage maps
df_macro_clean = df_macro_split.withColumn("Infection_Type",
    when(col("Specimen") == "URINE", "Urinary tract")
    .when(col("Specimen") == "UROGENITAL", "Gonorrhoea")
    .when(col("Specimen") == "STOOL", "Gastrointestinal")
    .when(col("Specimen") == "BLOOD", "Bloodstream")
    .otherwise(col("Specimen")))

display(df_macro_clean)

# 5. Handle "NA" strings for Power BI math compatibility
cols_to_clean_macro = ["CTAs_Reported_BCIs", "CTAs_Reported_BCIs_AST_80", "Total_BCIs", "Total_BCIs_AST", "Min", "Q1", "Median", "Q3", "Max"]

for c in cols_to_clean_macro:
    df_macro_clean = df_macro_clean.withColumn(c, when(col(c) == "NA", None).otherwise(col(c)))

display(df_macro_clean)

# 6. Cast numeric metrics
df_macro_final = df_macro_clean \
    .withColumn("CTAs_Reported_BCIs", col("CTAs_Reported_BCIs").cast("int")) \
    .withColumn("CTAs_Reported_BCIs_AST_80", col("CTAs_Reported_BCIs_AST_80").cast("int")) \
    .withColumn("Total_BCIs", col("Total_BCIs").cast("int")) \
    .withColumn("Total_BCIs_AST", col("Total_BCIs_AST").cast("int")) \
    .withColumn("Min", col("Min").cast("float")) \
    .withColumn("Q1", col("Q1").cast("float")) \
    .withColumn("Median", col("Median").cast("float")) \
    .withColumn("Q3", col("Q3").cast("float")) \
    .withColumn("Max", col("Max").cast("float")) \
    .select("Year", "Infection_Type", "Specimen", "CTAs_Reported_BCIs", "CTAs_Reported_BCIs_AST_80", "Total_BCIs", "Total_BCIs_AST", "Min", "Q1", "Median", "Q3", "Max")

display(df_macro_final)

# 7. Write to Silver
df_macro_final.write.format("delta").mode("overwrite").saveAsTable("AMR_Silver.silver.infection_type_macro_coverage")
print("Silver table 'infection_type_macro_coverage' created successfully")


# In[8]:


# Cleaning the AMR Signal Layer (Pathogens & Antibiotics)
# Handles the granular pathogen files, specifically tackling the "NA" trap so Power BI can do math on the results.

# 1. Read raw text
bronze_pathogen_path = "abfss://6005e045-448a-48b4-9ec6-e08b9f150edd@onelake.dfs.fabric.microsoft.com/672258a3-02a8-4727-8aa8-6839bafb76aa/Files/Bronze_Landing/AMR_Signal_Layer/Testing coverage by bacterial *"
df_path_text = spark.read.text(bronze_pathogen_path)

display(df_path_text)

# 2. Filter out metadata and the header string
df_path_filtered = df_path_text.filter(
    ~col("value").contains("Filters") &
    ~col("value").contains("filter:") &
    ~col("value").rlike(r"^\s*$") &
    ~col("value").contains("Year")
)

display(df_path_filtered)

# 3. Split the CSV string
split_path = split(col("value"), '","|,"|,')
df_path_split = df_path_filtered \
    .withColumn("Year", regexp_replace(split_path.getItem(0), '"', '').cast("int")) \
    .withColumn("Specimen", regexp_replace(split_path.getItem(1), '"', '')) \
    .withColumn("PathogenName", regexp_replace(split_path.getItem(2), '"', '')) \
    .withColumn("AntibioticName", regexp_replace(split_path.getItem(3), '"', '')) \
    .withColumn("CTAs_Reported_BCIs", regexp_replace(split_path.getItem(4), '"', '')) \
    .withColumn("CTAs_Reported_BCIs_AST_80", regexp_replace(split_path.getItem(5), '"', '')) \
    .withColumn("Total_BCIs", regexp_replace(split_path.getItem(6), '"', '')) \
    .withColumn("Total_BCIs_AST", regexp_replace(split_path.getItem(7), '"', '')) \
    .withColumn("Min", regexp_replace(split_path.getItem(8), '"', '')) \
    .withColumn("Q1", regexp_replace(split_path.getItem(9), '"', '')) \
    .withColumn("Median", regexp_replace(split_path.getItem(10), '"', '')) \
    .withColumn("Q3", regexp_replace(split_path.getItem(11), '"', '')) \
    .withColumn("Max", regexp_replace(split_path.getItem(12), '"', ''))

display(df_path_split)

4. # Handle "NA" strings and cast metrics
cols_to_clean = ["CTAs_Reported_BCIs", "CTAs_Reported_BCIs_AST_80", "Total_BCIs", "Total_BCIs_AST", "Min", "Q1", "Median", "Q3", "Max"]
df_path_clean = df_path_split

for c in cols_to_clean:
    df_path_clean = df_path_clean.withColumn(c, when(col(c) == "NA", None).otherwise(col(c)))

display(df_path_clean)

df_pathogen_final = df_path_clean \
    .withColumn("CTAs_Reported_BCIs", col("CTAs_Reported_BCIs").cast("int")) \
    .withColumn("CTAs_Reported_BCIs_AST_80", col("CTAs_Reported_BCIs_AST_80").cast("int")) \
    .withColumn("Total_BCIs", col("Total_BCIs").cast("int")) \
    .withColumn("Total_BCIs_AST", col("Total_BCIs_AST").cast("int")) \
    .withColumn("Min", col("Min").cast("float")) \
    .withColumn("Q1", col("Q1").cast("float")) \
    .withColumn("Median", col("Median").cast("float")) \
    .withColumn("Q3", col("Q3").cast("float")) \
    .withColumn("Max", col("Max").cast("float")) \
    .select("Year", "Specimen", "PathogenName", "AntibioticName","CTAs_Reported_BCIs", "CTAs_Reported_BCIs_AST_80", "Total_BCIs", "Total_BCIs_AST", "Min", "Q1", "Median", "Q3", "Max")

display(df_pathogen_final)

# 5. Write to Silver
df_pathogen_final.write.format("delta").mode("overwrite").saveAsTable("AMR_Silver.silver.pathogen_antibiotic_coverage")
print("Silver table 'pathogen_antibiotic_coverage' created successfully.")


# In[11]:


# Cleaning the AMR Signal Layer (Pathogens & Infection Data)
# Cleans, and unifies the granular " Pathogen & Infection" datasets

# 1. Read the raw text files
bronze_path_inf_path = "abfss://6005e045-448a-48b4-9ec6-e08b9f150edd@onelake.dfs.fabric.microsoft.com/672258a3-02a8-4727-8aa8-6839bafb76aa/Files/Bronze_Landing/AMR_Signal_Layer/Testing coverage by infection type and bacterial pathogen*"
df_pi_text = spark.read.text(bronze_path_inf_path)

display(df_pi_text)

# 2. Filter out metadata and the header string
df_pi_filtered = df_pi_text.filter(
    ~col("value").contains("Filters") &
    ~col("value").contains("filter:") &
    ~col("value").rlike(r"^\s*$") &
    ~col("value").contains("Year")
)

display(df_pi_filtered)

# 3. Split the CSV string into columns
split_pi = split(col("value"), '","|,"|,')
df_pi_split = df_pi_filtered \
    .withColumn("Year", regexp_replace(split_pi.getItem(0), '"', '').cast("int")) \
    .withColumn("Specimen", regexp_replace(split_pi.getItem(1), '"', '')) \
    .withColumn("PathogenName", regexp_replace(split_pi.getItem(2), '"', '')) \
    .withColumn("CTAs_Reported_BCIs", regexp_replace(split_pi.getItem(3), '"', '')) \
    .withColumn("CTAs_Reported_BCIs_AST_80", regexp_replace(split_pi.getItem(4), '"', '')) \
    .withColumn("Total_BCIs", regexp_replace(split_pi.getItem(5), '"', '')) \
    .withColumn("Total_BCIs_AST", regexp_replace(split_pi.getItem(6), '"', '')) \
    .withColumn("Min", regexp_replace(split_pi.getItem(7), '"', '')) \
    .withColumn("Q1", regexp_replace(split_pi.getItem(8), '"', '')) \
    .withColumn("Median", regexp_replace(split_pi.getItem(9), '"', '')) \
    .withColumn("Q3", regexp_replace(split_pi.getItem(10), '"', '')) \
    .withColumn("Max", regexp_replace(split_pi.getItem(11), '"', ''))

display(df_pi_split)

# 4. Handle "NA" strings for Power BI math compatibility
cols_to_clean_pi = ["CTAs_Reported_BCIs", "CTAs_Reported_BCIs_AST_80", "Total_BCIs", "Total_BCIs_AST", "Min", "Q1", "Median", "Q3", "Max"]
df_pi_clean = df_pi_split

for c in cols_to_clean_pi:
    df_pi_clean = df_pi_clean.withColumn(c, when(col(c) == "NA", None).otherwise(col(c)))

display(df_pi_clean)

# 5. Cast numeric metrics
df_pi_final = df_pi_clean \
    .withColumn("CTAs_Reported_BCIs", col("CTAs_Reported_BCIs").cast("int")) \
    .withColumn("CTAs_Reported_BCIs_AST_80", col("CTAs_Reported_BCIs_AST_80").cast("int")) \
    .withColumn( "Total_BCIs", col( "Total_BCIs").cast("int")) \
    .withColumn("Total_BCIs_AST", col("Total_BCIs_AST").cast("int")) \
    .withColumn("Min", col("Min").cast("float")) \
    .withColumn("Q1", col("Q1").cast("float")) \
    .withColumn("Median", col("Median").cast("float")) \
    .withColumn("Q3", col("Q3").cast("float")) \
    .withColumn("Max", col("Max").cast("float")) \
    .select("Year", "Specimen", "PathogenName", "CTAs_Reported_BCIs", "CTAs_Reported_BCIs_AST_80", "Total_BCIs", "Total_BCIs_AST", "Min", "Q1", "Median", "Q3", "Max")

display(df_pi_final)

# 6. Write to Silver
df_pi_final.write.format("delta").mode("overwrite").saveAsTable("AMR_Silver.silver.pathogen_infection_coverage")
print("Silver table 'pathogen_infection_coverage' created successfully.")


# In[1]:


# Unpivoting the System Layer
from pyspark.sql.functions import col, split, regexp_replace, when, sum as _sum, first, row_number, monotonically_increasing_id
from pyspark.sql.window import Window


# Unpivot the Indicators File

# 1. Read as text and force into a single partition to guarantee sequntial  order
bronze_indicators_path = "abfss://6005e045-448a-48b4-9ec6-e08b9f150edd@onelake.dfs.fabric.microsoft.com/672258a3-02a8-4727-8aa8-6839bafb76aa/Files/Bronze_Landing/System_Layer/GLASS-AMR implementation indicators_All.csv"
df_ind_text = spark.read.text(bronze_indicators_path).coalesce(1)

display(df_ind_text)

# 2. Add strict row numbers to maintain physical file order
w_order = Window.orderBy(monotonically_increasing_id())
df_ordered = df_ind_text.withColumn("row_id", row_number().over(w_order))

display(df_ordered)

# 3. Filter out metadata, empty rows, and redundant colum headers
df_clean_text = df_ordered.filter(
    ~col("value").contains("Filters") &
    ~col("value").contains("filter:") &
    ~col("value").rlike(r"^\s*$") &
    ~col("value").contains("Names")
)

display(df_clean_text)

# 4. Identify Indicator Name rows vs Data rows
#Data rows contain commas. The stacked Indicator headers do not
df_flagged = df_clean_text.withColumn("is_indicator", when(~col("value").contains(","), 1).otherwise(0))

display (df_flagged)

# 5. Create groups: increment group ID every time we hit a new indicator header
w_group = Window.orderBy("row_id")
df_grouped = df_flagged.withColumn("indicator_group", _sum("is_indicator").over(w_group))

display(df_grouped)

# 6. Assign the Indicator Name to all rows in that group (Forward Fill)
w_fill = Window.partitionBy("indicator_group").orderBy("row_id")
df_filled = df_grouped.withColumn("Indicator_Name", first("value").over(w_fill))

display(df_filled)

# 7. Keep only the data rows (drop the standalone indicator header rows)
df_data_only = df_filled.filter(col("is_indicator") == 0)

display(df_data_only)

# 8. Split the data rows into Status, Count, and Percentage
split_ind = split(col("value"), ",")
df_indicators_final = df_data_only \
    .withColumn("Status", regexp_replace(split_ind.getItem(0), '"', '' )) \
    .withColumn("Count", split_ind.getItem(1).cast("int")) \
    .withColumn("Percentage", split_ind.getItem(2).cast("float")) \
    .select("Indicator_Name", "Status", "Count", "Percentage")

display(df_indicators_final)

# 9. Write Indicators to Silver
df_indicators_final.write.format("delta").mode("overwrite").saveAsTable("AMR_Silver.silver.system_indicators")
print("Silver table 'system_indicators' created successfully.")


# In[23]:


# Clean CTA reporting file

bronze_cta_path = "abfss://6005e045-448a-48b4-9ec6-e08b9f150edd@onelake.dfs.fabric.microsoft.com/672258a3-02a8-4727-8aa8-6839bafb76aa/Files/Bronze_Landing/System_Layer/Numbers of CTAs reporting data to GLASS-AMR_All.csv"
df_cta_text = spark.read.text(bronze_cta_path)

display(df_cta_text)

# 1. Filter out the WHO portal metadata
df_cta_filtered = df_cta_text.filter(
        ~col("value").contains("Filters") &
        ~col("value").contains("filter:") &
        ~col("value").rlike(r"^\s*$") &
        ~col("value").contains("Year")
)

display(df_cta_filtered)

# 2. Split the data rows into columns
split_cta = split(col("value"), ",")
df_cta_final = df_cta_filtered \
    .withColumn("Year", split_cta.getItem(0).cast("int")) \
    .withColumn("Total_CTAs", split_cta.getItem(1).cast("int")) \
    .withColumn("Enrolled_CTAs", split_cta.getItem(2).cast("int")) \
    .withColumn("Reporting_Any_BCI", split_cta.getItem(3).cast("int")) \
    .withColumn("Reporting_80_AST", split_cta.getItem(4).cast("int")) \
    .select("Year", "Total_CTAs", "Enrolled_CTAs", "Reporting_Any_BCI", "Reporting_80_AST")

display(df_cta_final)

# 3. Write CTA data to Silver
df_cta_final.write.format("delta").mode("overwrite").saveAsTable("AMR_Silver.silver.cta_reporting_status")
print("Silver table 'cta_reporting_status' created successfully.")

