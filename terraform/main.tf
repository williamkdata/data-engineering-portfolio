resource "google_bigquery_dataset" "velib_raw" {
  dataset_id                      = "velib_raw"
  project                         = var.project_id
  location                        = var.region
  description                     = "Donnees brutes Velib' (dlt, station_status/station_information)"
  default_partition_expiration_ms = 5184000000
  default_table_expiration_ms     = 5184000000
}

resource "google_bigquery_dataset" "meteo_raw" {
  dataset_id  = "meteo_raw"
  project     = var.project_id
  location    = var.region
  description = "Donnees brutes meteo Open-Meteo (dlt, weather)"
}

resource "google_bigquery_dataset" "velib_analytics" {
  dataset_id  = "velib_analytics"
  project     = var.project_id
  location    = var.region
  description = "Modeles dbt (staging/intermediate/marts)"
}

resource "google_service_account" "airflow_velib" {
  account_id   = "airflow-velib"
  display_name = "Airflow Velib pipeline"
  project      = var.project_id
}

resource "google_project_iam_member" "airflow_velib_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow_velib.email}"
}

resource "google_bigquery_dataset_iam_member" "velib_raw_editor" {
  dataset_id = google_bigquery_dataset.velib_raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.airflow_velib.email}"
}

resource "google_bigquery_dataset_iam_member" "meteo_raw_editor" {
  dataset_id = google_bigquery_dataset.meteo_raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.airflow_velib.email}"
}

resource "google_bigquery_dataset_iam_member" "velib_analytics_editor" {
  dataset_id = google_bigquery_dataset.velib_analytics.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.airflow_velib.email}"
}
