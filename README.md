# Scalable Data Pipeline for Used Car Price Prediction Using AWS and PySpark

This repository contains the implementation of a comprehensive data pipeline leveraging AWS cloud services and PySpark for the analysis and prediction of used car prices. The pipeline orchestrates the storage, processing, analysis, and visualization of a large-scale dataset, integrating AWS tools and distributed computing capabilities to efficiently process and analyze data.

## Project Overview

This project builds a scalable data pipeline that processes and analyzes used car price data using AWS services and PySpark. The pipeline automates the prediction of car prices, enabling better pricing decisions for used car dealers and individuals. By handling large datasets efficiently, it ensures faster insights and cost-effective processing. The use of Amazon SageMaker AutoML simplifies machine learning model development, while AWS QuickSight provides interactive visualizations for data-driven decision-making. This solution streamlines the process, reduces errors, and improves profitability in used car sales.

### Key Components:
1. **AWS S3**: Used for raw data storage. The large dataset of used car listings is securely stored in Amazon S3, allowing for scalable storage and retrieval.
2. **PySpark & Jupyter Notebooks**: PySpark is employed to handle the distributed data processing tasks. Jupyter Notebooks are used for cleaning, transforming, and performing feature engineering on the dataset.
3. **SQL Analysis (Python Scripts)**: After processing, SQL queries are executed using Python scripts to analyze the data and generate key insights such as trends in car prices, sales distribution, and other statistics.
4. **Amazon SageMaker AutoML**: This service is used to build machine learning models for predicting used car prices. It automates the training, tuning, and evaluation of models, ensuring efficient and accurate predictions.
5. **AWS QuickSight**: After model predictions and SQL analyses, AWS QuickSight is used to visualize the results. It provides interactive dashboards for business stakeholders to easily interpret the data and make data-driven decisions.

## Workflow
![sequence_diag](https://github.com/user-attachments/assets/ad586ace-3f1d-4eb7-b1b0-f12282624a8a)

## Benefits of the Pipeline

- **Scalability**: The use of AWS cloud services and PySpark ensures that the pipeline can handle large datasets efficiently. The solution scales with the increasing amount of data, making it suitable for large-scale analytics.
- **Distributed Processing**: PySpark allows for parallel processing of data, drastically reducing the time required for data transformation and analysis.
- **Automation**: The integration of Amazon SageMaker AutoML automates the machine learning process, reducing the need for manual model selection and tuning.
- **Interactive Visualization**: AWS QuickSight enables the creation of dynamic dashboards that make it easy for users to interact with the data and derive insights.
- **Cloud-Native Architecture**: This solution leverages AWS services that are fully managed, ensuring high availability, security, and ease of integration with other AWS tools.

## Data Insights and Visualizations
![AWS Quicksight dashboard](https://github.com/user-attachments/assets/08b6b494-ec02-45b4-9b50-1fb5f7ee4cde)
1. **Price Trend Over Time**:  
   The **line graph** shows average car prices fluctuating between **$15,000** and **$20,000** throughout the posting period. A notable spike can be observed, where the prices reach approximately **$30,000** at one point.
2. **Geographic Distribution**:  
   The **map visualization** reveals a higher concentration of car listings in the **eastern United States**, particularly along the coast. The **pricing intensity** (shown in blue) varies across regions, with certain areas exhibiting higher average prices than others.
3. **Manufacturer Analysis**:  
   The **bubble chart** demonstrates a positive correlation between the **year** and **price** for various manufacturers. More recent years (2015-2017) tend to have larger bubbles, indicating higher prices. **Ford** stands out as one of the dominant manufacturers.
4. **Paint Color Impact**:  
   The **bar chart** indicates that **purple cars** command the highest average price (around **$28,000**), followed by **custom colors**. On the other hand, **green vehicles** show the lowest average price (approximately **$12,000**). Common colors like **black**, **white**, and **silver** fall in the middle range, between **$15,000-$20,000**.


## Conclusion

This project demonstrates the effective use of AWS cloud-native services and distributed computing to process, analyze, and predict used car prices. By integrating PySpark, Amazon S3, SageMaker AutoML, and QuickSight, the pipeline delivers valuable insights to business users and helps optimize pricing decisions for used car sales.
