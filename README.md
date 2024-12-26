# cloud-based-used-cars-price-data-analysis
A data pipeline using AWS and PySpark for batch processing of datasets. The project aims to efficiently process and analyze Wikipedia data, leveraging the power of distributed computing and cloud infrastructure. Key components include AWS EMR, S3, Athena, and PySpark, working together to create a scalable and robust data processing solution.


# Architecture
The pipeline architecture consists of the following key components:
1. Data Storage: AWS S3 serves as the primary data lake, storing raw, preprocessed, and curated datasets.
2. Data Processing:
Jupyter Notebooks on EC2 instances for initial data cleaning and feature engineering
PySpark on EMR for distributed data processing and transformation
3. Data Analysis:
SQL queries via Amazon Athena for ad-hoc analysis
Amazon SageMaker for machine learning model development and AutoML
4. Data Visualization:
AWS QuickSight for creating interactive dashboards and reports
