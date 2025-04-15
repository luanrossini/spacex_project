# spacex_project

Architecture Overview

1. SpaceX API 🚀
What: Public API providing SpaceX launch and mission data.
Role: Source of raw data for the pipeline.

2. Data Extraction Scripts 📥
What: Python scripts that fetch data from the SpaceX API.
Role: Extract data and upload it to Google Cloud Storage (GCS) for staging.

3. Data Loading Scripts 🚚
What: Python script to transfer data from GCS to BigQuery.
Role: Populate BigQuery with raw data for further processing.

4. Data Transformation with dbt 🧹
What: dbt project that transforms raw tables in BigQuery into refined layers.
Role: Create medallion layers (raw → staging (materialized views) → mart) for analysis.

5. Pipeline Orchestration with Airflow ⏱️
What: Airflow DAG that orchestrates extraction, loading, and transformation.
Role: Automate and monitor the execution of the entire pipeline.

6. Local Development with Docker Compose 🐳
What: Docker Compose configuration for running a local Airflow instance.
Role: Simplify development and testing of the pipeline.

7. Containerization & Deployment 🚢
What: Docker image built from the project.
Role: Deployed to Artifact Registry (or GCR) for execution in the cloud.
Note: The image is built for the linux/amd64 platform to be compatible with Cloud Run.

8. Service Account & GCP Secrets 🔑
What: Service account key stored in GCP Secrets Manager.
Role: Securely enable interaction between Cloud Run, GCR, BigQuery, and GCS.

9. Cloud Run Job for Daily Execution ☁️
What: Cloud Run Job that executes the containerized pipeline.
Role: Trigger the Airflow DAG on-demand and exit once complete, ensuring cost-efficient, serverless execution.

10. Scheduling with Cloud Scheduler ⏰
What: Cloud Scheduler job that sends an HTTP POST to trigger the Cloud Run Job.
Role: Automate the daily execution of the pipeline.

11. Entrypoint Script (entrypoint.sh) 🔄
What: Shell script that initializes the Airflow database, forces a scheduler run to parse DAGs, unpauses and triggers the DAG, and finally starts the scheduler for a short period.
Role: Ensures that each container execution fully sets up and runs the pipeline before exiting.