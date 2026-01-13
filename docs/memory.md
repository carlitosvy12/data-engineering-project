## Project Objective

The objective of this project is to design and implement a complete data pipeline to analyze the affordability of housing in Spain.

The project processes two public datasets: average salaries and housing prices by autonomous community. These datasets are cleaned, validated, integrated, and transformed to create meaningful indicators related to housing affordability.

The final goal is to generate a structured dataset and a data warehouse that allow the analysis of the relationship between income and housing costs over time. The results are prepared to be easily consumed by analytical tools and are also published to the cloud for external access.

This project follows a structured data engineering approach, covering all main stages of a data pipeline: ingestion, validation, transformation, storage, and data serving.


## Selected Datasets and Justification

For this project, two public datasets have been selected. Both datasets are related to the economic situation and housing market in Spain and are compatible for joint analysis.

The first dataset contains information about average salaries by autonomous community and date. Its main fields include the autonomous community name, the date, and the average salary expressed in euros. This dataset allows the analysis of income levels across different regions and over time. Some records contain missing values in the salary field, which makes it necessary to apply data cleaning and imputation techniques.

The second dataset contains information about housing prices by autonomous community and date. It includes fields such as the autonomous community name, the date, the average housing price per square meter, and the average rent price per square meter. This dataset also presents missing values and some extreme values, especially in housing prices, which require validation and normalization during the transformation phase.

Both datasets are related through two common attributes: the autonomous community and the date. This shared structure makes it possible to integrate them using a join operation and to compare salaries with housing costs for the same region and time period.

The choice of these datasets is justified because together they provide a clear view of housing affordability. Salaries represent the income side, while housing prices represent the cost side. By combining both sources, it is possible to calculate indicators that help understand how accessible housing is for citizens in different regions of Spain.


## Data Lifecycle Overview

This project follows a complete data lifecycle approach, from raw data acquisition to data consumption. The objective of this lifecycle is to ensure that data is reliable, well-structured, and ready for analysis at the end of the process.

The first stage of the lifecycle is data ingestion. In this phase, raw data files are loaded from their original source into the system without modifying their content. This step ensures that the original data is preserved and available for further processing.

The second stage is data validation. Here, basic quality checks are applied to detect common data issues such as missing values, duplicated records, or incorrect schemas. This step helps identify problems early and prevents incorrect data from propagating through the pipeline.

The third stage is data transformation. During this phase, the data is cleaned, standardized, and enriched. This includes normalizing text fields, handling missing values, converting data types, and preparing the datasets so they can be correctly integrated.

The fourth stage is data integration and storage. In this project, the processed data is integrated into a single dataset and then loaded into a data warehouse. This structured storage allows efficient analysis and supports analytical queries.

The final stage is data serving. In this phase, the results of the pipeline are prepared for consumption by external systems or users. The data is exported in a usable format and published to the cloud, making it accessible for analysis and visualization.

In the following sections, each stage of the data lifecycle is explained in detail, describing how it has been implemented in this project.
data-engineering-project\docs\evidence\Architecture


## Data Ingestion

The ingestion phase is responsible for loading the raw data into the system. In this project, the raw datasets are stored as CSV files and are ingested without any modification to preserve the original information.

During this stage, the system reads the salary and housing datasets from the raw data directory. The ingestion process only verifies that the files exist and can be correctly loaded, but it does not apply any cleaning or transformation operations.

This approach ensures that the original datasets remain unchanged and can always be used as a reference. Keeping raw data intact is important for traceability and for detecting data quality issues in later stages of the data lifecycle.


## Data Validation

The validation phase is used to check the quality of the raw data before applying any transformation. The goal of this step is not to modify the data, but to detect possible problems early in the pipeline.

In this project, validation is implemented using two Python files: one that defines the quality checks and another that executes them on the raw datasets.

The first validation file defines the expected structure and the main quality checks. For each dataset, an expected schema is defined, listing the columns that must be present. The check_schema function compares the real columns of the dataset with the expected ones and reports missing or extra columns. This ensures that the data structure matches what the pipeline expects.

The check_nulls function is used to detect missing values in the datasets. It counts null values per column and logs a warning if any are found. This helps identify which fields may need cleaning or imputation during the transformation phase.

The check_duplicates function checks for duplicated rows in each dataset. Duplicates can affect analysis results, so detecting them early is important even if they are handled later.

The second validation file is responsible for running these checks. It loads the raw salary and housing datasets and applies the schema, null, and duplicate checks to each one. All results are written to the logs, providing a clear overview of the data quality before transformation.

This validation step ensures that data issues are known in advance and that the transformation phase can focus on fixing and standardizing the data in a controlled way.


## Data Transformation

In this project, transformation is implemented in two scripts. The first one cleans the raw datasets and saves them in the staging area. The second one integrates both cleaned datasets and creates the final metrics.

### 1 Cleaning to Staging (clean_to_staging.py)

This script reads the two raw CSV files (salaries and housing) and prepares them for a reliable join later.

First, it standardizes the key fields used for matching:
- date is converted to a clean string (trimmed).
- autonomous_community is kept as a raw version (autonomous_community_raw) for traceability.
- A normalized key called autonomous_community_key is created. It removes extra spaces, converts to lowercase, and removes accents. This avoids problems like different spelling formats (for example, accents or uppercase letters).

Then, it removes duplicated rows. The script logs how many duplicates are dropped, so the cleaning actions are transparent.

After that, it converts important columns to numeric values:
- For salaries: avg_salary_eur and employment_rate
- For housing: avg_price_m2_eur and avg_rent_m2_eur
If a value cannot be converted, it becomes null (NaN), which is a safe way to detect bad values.

Missing values are handled in a simple and stable way:
- avg_salary_eur and avg_price_m2_eur are filled using the median value of the same autonomous community (grouped by autonomous_community_key). This keeps the dataset complete without using extreme values.

Finally, the housing price per m² is capped at the 99 percentile (p99) inside each community. This reduces the impact of extreme outliers and makes the later indicators more robust.

At the end, the script saves the cleaned outputs into:
- data/staging/salaries/salaries_clean.csv
- data/staging/housing/housing_clean.csv

### 2 Integration and Feature Creation (integrate_data.py)

This second script reads the two cleaned staging files and integrates them into one dataset.

The integration is done with an inner join using:
- date
- autonomous_community_key

Using an inner join means that only records that exist in both datasets (same community and date) are kept. This avoids comparing salary data with missing housing data, or the opposite.

After the join, two derived metrics are created:
- affordability_index = avg_salary_eur / avg_price_m2_eur
- salary_to_rent_ratio = avg_salary_eur / (avg_rent_m2_eur * 80)

The idea is to compare income with housing costs using simple ratios that are easy to interpret.

Finally, the integrated dataset is saved as:
- data/staging/integrated/affordability_integrated.csv



## Data Serving (Azure Blob Storage)

In the serving phase, I publish the final integrated dataset so it can be accessed outside my local machine. In my case, I upload the file affordability_integrated.csv to Azure Blob Storage.

The script connects to my Azure Storage account, selects a container called integrated, and uploads the CSV as affordability_integrated.csv. I use overwrite=True so the file is replaced when I run the pipeline again. This makes the output easy to share and easy to refresh.

For this part, I had to use an Azure Storage account from my sister, because in my own account I could not continue with the free trial (it was already finished or not available for my subscription). With her account, I was able to complete the cloud publication and provide evidence of the serving step.

Important, in a real project, I would not store credentials inside the code. The correct approach is to use environment variables (or Azure Key Vault) and keep secrets outside the repository. The version included here is only for you, Victor.


## Data Warehouse (SQLite)

Once the integrated dataset is generated, I store the results in a data warehouse implemented in two different ways. First, I create a dimensional model stored as CSV files, This version represents the data warehouse structure in a simple and clear way and helps to understand the dimensions and the fact table. I only have made this yo give a other perspective.

After that, I implement a physical data warehouse using SQLite. I decided to use SQLite because it is easy to use and does not require a server installation. The SQLite data warehouse is stored in the file dw.sqlite and follows a star schema structure. It includes a date dimension, a community dimension, and a fact table that contains the main measures related to salaries, housing prices, and affordability indicators. This structure reduces duplicated information and makes analytical queries easier and clearer.

The loading process for the SQLite data warehouse is implemented in the script load_to_dw_sqlite.py. This script reads the integrated dataset and first creates the necessary tables if they do not already exist. Then, it loads the dimension tables by inserting unique dates and autonomous communities. For the community dimension, a representative and readable name is selected from the dataset to keep the dimension understandable.

After loading the dimensions, the script fills the fact table. The integrated data is linked to the corresponding dimension keys, and all relevant numeric measures are inserted, including salaries, housing prices, and the derived affordability indicators. Before inserting new data, the fact table is cleared to avoid duplicated records when the pipeline is executed more than once.

Finally, a small verification script is used to check that the data warehouse has been correctly populated. This script connects to the SQLite database and prints the number of rows in each table, allowing a quick validation that the loading process was successful.


## Pipeline Orchestration

To run the full process in a controlled way, I created an orchestration script that executes the pipeline step by step. The idea is to avoid running each script manually and to ensure that the workflow always follows the same order.

The orchestration script defines a list of pipeline steps and runs each one as a Python module using subproces. For every step, it writes clear log messages showing when the step starts and when it ends. It also captures standard output and errors, so I can see what happened during execution without opening each script separately.

The pipeline is executed in this order: first, data quality checks on the raw datasets; then cleaning to staging; next, dataset integration and metric creation; after that, loading the integrated data into the SQLite data warehouse; and finally, publishing the final CSV to Azure Blob Storage.



## Pipeline Scheduling

All processes in the project are designed to be orchestrated and executed at least once per day. The pipeline is fully automated through the orchestration script, which allows all steps to run sequentially without manual intervention.

Since this project is developed in an academic context and not deployed in a real production environment, the pipeline is not scheduled on a server. Running it automatically every day on a local machine is not always feasible, as the computer may be turned off or not available.

However, the configuration is prepared so that daily execution would be easy to implement in a real scenario. For example, the orchestration script can be scheduled using system tools such as a cron job on Linux or the Task Scheduler on Windows. In that case, the pipeline would run once per day at a fixed time and automatically generate logs, success files, or alert files depending on the result.


## Monitoring: Logging and Alerting


Logging is used in all main steps of the pipeline. Each script writes informative messages that indicate what the process is doing, such as reading files, cleaning data, integrating datasets, loading the data warehouse, or publishing results to the cloud. These logs are written both to the console and to a log file (pipeline.log)

Alerting is handled at the orchestration level. If any step of the pipeline fails, the orchestration script stops the execution and creates an alert file called ALERT_PIPELINE_FAILED.tx. This file includes a timestamp and a short error message, making it clear that the pipeline did not finish correctly. When the pipeline runs successfully, a confirmation file called PIPELINE_SUCCESS.txt is created instead.


## Data Insights and Visualization (Looker Studio)

### Evidence image 1  
 docs/evidence/dashboard_01_overview.png

In this image, the upper chart shows the affordability index by autonomous community. Each bar represents a different community, and higher values mean better housing affordability. From this chart, it can be seen that communities such as Castilla y León and Galicia have higher affordability, while Cataluña, País Vasco, and Comunidad de Madrid show lower values. This makes sense because housing prices in large urban areas are higher, which reduces affordability even if salaries are higher.

In the same image, the lower chart compares the average salary and the average housing price per square meter over time. The visualization shows that housing prices remain high during the year and often grow faster than salaries. This explains why affordability is limited in some regions, as income growth does not fully compensate for housing costs.

---

### Evidence image 2  
Path: docs/evidence/dashboard_02_filtered_community.png.png

In this image, the dashboard is filtered to a single autonomous community, Comunidad de Madrid. The upper chart shows the affordability index only for this region. The value is clearly lower compared to many other communities seen in the global overview, confirming that housing affordability in Madrid is more limited.

The lower chart in this image compares average salary and housing prices over time for Comunidad de Madrid. It shows that housing prices are consistently very high, while salary growth is not enough to offset these costs. This directly explains the low affordability index shown in the upper chart.

Overall, the insights extracted from these visualizations are consistent and realistic. Regions with lower housing prices tend to have better affordability, while large metropolitan areas show lower affordability due to high housing costs. These results confirm that the processed data and calculated indicators are meaningful and reliable.



## Scalability, Performance and Cloud Cost Estimation


Cloud cost estimations were calculated using the Azure Pricing Calculator. The configuration and estimated monthly costs for different data sizes can be seen in the screenshots included as evidence.

The current integrated dataset generated by the pipeline is relatively small and occupies less than 1 GB. For this reason, storage cost at the current scale is almost zero and can be considered negligible. To analyze scalability, I assumed a minimum starting point of 1 GB and then estimated the impact of increasing the data volume by x10, x100, x1000, and x10^6.

To estimate cloud costs, I used Microsoft Azure Blob Storage as the cloud provider. The cost estimation was obtained using the Azure pricing configuration for standard Blob Storage, hot access tier, LRS redundancy, and the Spain Central region. The screenshots included as evidence show the estimated monthly storage cost for different data sizes.

For the x10 scenario (around 10 GB), the estimated monthly cost is still very low, slightly above 1 USD per month. At this scale, performance would remain acceptable, and the current pipeline design could still work without major changes.

For the x100 scenario (around 100 GB), storage cost increases but remains affordable (around a few dollars per month). However, processing performance would start to degrade if the data is handled fully in memory using pandas. At this point, execution time would noticeably increase.

For the x1000 scenario (around 1 TB), storage cost rises to several tens of dollars per month. At this scale, the current approach using local execution, pandas, and SQLite would no longer be appropriate. Memory limitations and long execution times would become a serious issue.

For the x10^6 scenario (very large data volumes), storage cost becomes very high and could reach thousands of dollars per month. Processing such volumes would require a completely different architecture, including distributed storage and distributed data processing.

To address these scalability and performance problems, a clear proposal is to migrate the pipeline to a cloud-native architecture. Data ingestion and transformation could be handled using distributed frameworks such as Apache Spark. Storage could be moved from local CSV files and SQLite to cloud-based data lakes and analytical databases. This would allow the system to scale efficiently while controlling performance and cost.

In conclusion, the current solution is suitable and cost-effective for small datasets, while the proposed cloud-based approach would allow the project to scale to much larger data volumes if needed.



## Data Consumers and Delivery Solution

The data produced by this project can be consumed by different types of users, depending on their needs and technical level.

One type of consumer is a data analyst or business user. These users are mainly interested in understanding trends and comparing regions. For them, the data is delivered through a BI tool such as Google Looker Studio, using the integrated dataset to create visual dashboards. This allows easy access to insights without requiring technical knowledge.

Another type of consumer is a data scientist or technical user. These users may want to perform deeper analysis or build models using the processed data. For this purpose, the data is delivered as a structured CSV file and also stored in a data warehouse (SQLite). This format allows direct access for further analysis using Python, SQL, or other analytical tools.

This multi-channel delivery approach ensures that the same processed data can be reused by different consumers, each one accessing it in the most appropriate format for their use case.

## How AI Can Support the Data Pipeline

IA can support different parts of this data pipeline by improving efficiency, quality, and decision making.

During the data ingestion and validation phases, AI can help detect anomalies and unusual patterns in the data. For example, machine learning models can identify unexpected values, abnormal price changes, or inconsistent records that simple rules might not detect.

In the transformation stage, AI can assist with smarter data cleaning. Instead of using fixed rules, AI models can learn how to impute missing values more accurately based on historical data and correlations between variables. This can improve data quality when datasets become larger and more complex.

For data analysis and insights, AI can be used to automatically detect trends, correlations, and regional patterns in housing affordability. It can also help generate forecasts or highlight communities where affordability is changing rapidly.

Finally, AI can support monitoring and maintenance of the pipeline. By analyzing logs and execution metrics, AI-based systems can detect recurring failures, predict performance bottlenecks, and suggest improvements to the pipeline configuration.

## Privacy Considerations

There are no major privacy concerns in this project because the datasets used are public and aggregated. The data does not contain any personal or identifiable information about individuals.

All information is provided at the level of autonomous communities and dates, using average values such as salaries and housing prices. This means that no specific person can be identified from the data.

Even so, basic good practices are followed. The project only stores the data required for analysis, and access to cloud storage is controlled through credentials. In a real production environment, additional security measures such as restricted access and secret management would be applied.


For this university project, the Azure storage access key is included directly in the code. I am aware that this is not a good practice in a real production environment. However, this decision was made only for academic purposes and in a context of trust.