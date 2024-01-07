# Dashboard Status Gizi
This documentation provides an overview of the development process and key features of an interactive Tableau dashboard for nutrition status. The dashboard aims to empower data-driven insights for health monitoring and decision-making. It includes information on the design and development of the dashboard, the automated data pipeline ETL application, the utilization of Docker containers for agile deployment, and the creation of a high-performance data model in Google BigQuery.

## **Architecture Overview**
<p align="center"><img src="https://github.com/fakhriyalfians/Dashboard_Status_Gizi/blob/main/images/arc.png" width="500px"></p>

## **Tools and Technologies**
The interactive Tableau dashboard for nutrition status utilizes the following technologies in its development:
- PostgreSQL
- Docker
- Python
- Pandas
- Apache Airflow
- Google Cloud Storage (GCS)
- Google BigQuery
- Google Cloud Composer
- Tableau

## **Automated Data Pipeline ETL Application**
The automated data pipeline ETL (Extract, Transform, Load) application was developed using Airflow Scheduler and Google Cloud Composer. This application automates the process of extracting data from various sources, transforming it into a suitable format, and loading it into Tableau Cloud for seamless integration with the interactive dashboard. The use of Airflow Scheduler and Google Cloud Composer ensures that the data pipeline runs at regular intervals, ensuring timely data refreshes and eliminating the need for manual intervention.

**Workflow DAGs**
<p align="center"><img src="https://github.com/fakhriyalfians/Dashboard_Status_Gizi/blob/main/images/dags.png" width="700px"></p>

## **Visualization**
The visualization of the nutrition status is presented through the Tableau dashboard, 
accessible via the following manual book link: [Dashboard Manual Book]([https://www.linkedin.com/in/humaid-assaidi-244421272/](https://drive.google.com/file/d/1HSb8w8hkTvba-mzhwEEUzwx81huVuHLB/view?usp=sharing))
<p align="center"><img src="https://github.com/fakhriyalfians/Dashboard_Status_Gizi/blob/main/images/dashboard.png" width="500px"></p>
