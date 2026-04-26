# BigQuery Model Sync

A Laravel package to synchronize Eloquent models with Google BigQuery tables.

## Installation

1. Install the package via composer:

```bash
composer require nonsapiens/bigquery-model-sync
```

2. Publish the configuration file and migrations:

```bash
php artisan vendor:publish --tag="bigquery-model-sync-config"
php artisan vendor:publish --tag="bigquery-model-sync-migrations"
```

3. Run the migrations:

```bash
php artisan migrate
```

## Configuration

The configuration is located at `config/bigquery.php`.

### Environment Variables

- `GOOGLE_CLOUD_PROJECT`: Your Google Cloud Project ID.
- `BIGQUERY_DATASET`: The BigQuery dataset where tables will be created.
- `BIGQUERY_LOCATION`: The location for your BigQuery dataset (default: `africa-south1`).
- `BIGQUERY_AUTOSYNC`: Boolean to enable/disable automatic scheduled syncing (default: `true`).
- `BIGQUERY_LOGGING`: Boolean to enable/disable sync logging (default: `true`).
- `BIGQUERY_CREDENTIALS`: Path to your Google Cloud service account JSON file.

### Localhost vs WIF (Workload Identity Federation)

**Localhost:**
Set `BIGQUERY_CREDENTIALS` in your `.env` file to the absolute path of your service account JSON key.

```env
BIGQUERY_CREDENTIALS=/path/to/your/service-account-key.json
```

**Deployed (WIF):**
When deployed on Google Cloud (GKE, Cloud Run, etc.) using Workload Identity Federation, you should NOT provide a service account key. The `BigQueryClient` will automatically use the environment's default credentials. Leave `BIGQUERY_CREDENTIALS` empty or unset in your production environment.

## Usage

### 1. Prepare your Model

Add the `SyncsToBigQuery` trait to the models you want to synchronize.

```php
namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;

class User extends Model
{
    use SyncsToBigQuery;
}
```

### 2. Configure Model Settings (Optional)

You can customize the sync behavior by defining properties or methods in your model:

| Property / Method | Default | Description |
|---|---|---|
| `$fieldsToSync` | `[]` (all fields) | Array of columns to sync. |
| `$syncStrategy` | `BATCH` | `BATCH`, `REPLACE`, or `ON_INSERT`. |
| `$bigQueryTableName`| Model table name | The target BigQuery table name. |
| `$syncSchedule` | `null` | Cron expression for when the model should sync. |
| `$batchSize` | `10000` | Number of records per batch. |
| `$batchField` | `sync_batch_uuid` | Database field used to track sync batches. |

### 3. Create BigQuery Table

Generate the SQL required to create the table in BigQuery:

```bash
php artisan make:bigquery-table --class="App\Models\User"
```

Copy the output and run it in the BigQuery console.

### 4. Prepare Database (if using BATCH or ON_INSERT)

If you are using `BATCH` (default) or `ON_INSERT` strategies, you need a tracking column in your local database. Generate a migration:

```bash
php artisan make:bigquery-model-migration --class="App\Models\User"
php artisan migrate
```

## Syncing Strategies

- **BATCH**: (Default) Synchronizes records in batches. It uses a UUID column to mark records that have been sent. Recommended for most use cases.
- **REPLACE**: Truncates the BigQuery table and re-uploads all data. Good for small tables or when records are frequently updated.
- **ON_INSERT**: Dispatches a job to sync an individual record immediately after it is created in the local database.

## Geodata Mapping

If your model contains latitude and longitude information, the package can automatically map these into a single `GEOGRAPHY` field in BigQuery.

### Configuration

Add the following properties or methods to your model:

| Property / Method | Default | Description |
|---|---|---|
| `$hasGeodata` | `false` | Set to `true` to enable geodata mapping. |
| `$geodataFields` | `['latitude', 'longitude']` | The source fields in your local database. |
| `$mappedGeographyField` | `geolocation` | The name of the `GEOGRAPHY` column in BigQuery. |

### How it Works

1. **Table Generation**: When running `php artisan make:bigquery-table`, the package will exclude the individual latitude and longitude fields from the generated SQL and instead include a single `GEOGRAPHY` field (e.g., ``geolocation``).
2. **Data Transformation**: During synchronization, the package combines the latitude and longitude values into a [WKT (Well-Known Text)](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) `POINT` string: `POINT(longitude latitude)`.
3. **BigQuery Storage**: BigQuery stores this as a native `GEOGRAPHY` type, allowing you to use [BigQuery GIS functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions) for spatial analysis.

> **Note**: The interactive `bigquery:set-model` command will automatically detect if `latitude` and `longitude` fields are present and offer to configure this mapping for you.

## Requirements for Schedule and Queue

### Scheduling

The package includes a scheduled task that runs every minute to check if any models are due for synchronization based on their `$syncSchedule`.

To enable this, ensure the Laravel scheduler is running in your environment:

```bash
* * * * * cd /path-to-your-project && php artisan schedule:run >> /dev/null 2>&1
```

You can disable automatic scheduling by setting `BIGQUERY_AUTOSYNC=false` in your `.env`.

### Queuing

The `ON_INSERT` strategy and the `bigquery:sync --queue` command rely on Laravel's queue system. Ensure you have a queue worker running:

```bash
php artisan queue:work
```

## Commands

### `bigquery:set-model`

An interactive command to configure an Eloquent model for BigQuery synchronization. It guides you through selecting fields, configuring geodata mapping, and choosing a sync strategy.

```bash
php artisan bigquery:set-model
```

- **Interactive Steps:**
    - Select a model from your application.
    - Choose which fields should be synchronized.
    - Configure `GEOGRAPHY` field mapping if `latitude` and `longitude` are present.
    - Choose between immediate (`ON_INSERT`) or scheduled (`BATCH`) synchronization.
    - Generate the necessary database migration for the tracking column.

### `make:bigquery-table`

Generates the `CREATE TABLE` SQL required for BigQuery based on your Eloquent model's schema and configuration.

```bash
php artisan make:bigquery-table --class="App\Models\User"
```

- `--class`: (Required) The fully qualified class name of the model.

### `make:bigquery-model-migration`

Creates a Laravel migration to add the required tracking column (default: `sync_batch_uuid`) to your model's database table.

```bash
php artisan make:bigquery-model-migration --class="App\Models\User"
```

- `--class`: (Required) The fully qualified class name of the model.

### `bigquery:sync`

Synchronizes a specific model with BigQuery. This command is typically called by the scheduler but can be run manually.

```bash
php artisan bigquery:sync --class="App\Models\User" [options]
```

- `--class`: (Required) The fully qualified class name of the model.
- `--force`: Run the sync even if it's not currently due according to the model's `$syncSchedule`.
- `--queue`: Dispatch the sync job to the queue instead of running it immediately in the foreground.
- `--schedule`: Override the cron expression defined in the model.

### `bigquery:sync-all`

Discovers all models using the `SyncsToBigQuery` trait and runs their synchronization if they are due.

```bash
php artisan bigquery:sync-all [--force]
```

- `--force`: Run synchronization for all discovered models, regardless of their schedule.

### `bigquery:truncate`

Truncates the BigQuery tables associated with your models. **Use with caution.**

```bash
php artisan bigquery:truncate {--all|--class="App\Models\User"}
```

- `--all`: Truncate tables for all models using the `SyncsToBigQuery` trait.
- `--class`: A comma-separated list of model classes to truncate.
- **Note**: This command requires manual confirmation and a randomly generated verification code to prevent accidental data loss.
