# Data Engineering Streaming PoC

An end-to-end streaming pipeline that pulls random user records from a public API, pushes them through Kafka, and lands them in Cassandra using Spark Structured Streaming. Everything runs locally in Docker Compose.

## Architecture

```
randomuser.me API
       |
       v
 Airflow DAG  (user_automation)
       |  produces JSON messages
       v
 Kafka topic  (users_created)   <-- Zookeeper, Schema Registry, Control Center
       |
       v
 Spark Structured Streaming  (spark-stream-direct.py, runs on the Spark cluster)
       |  Kafka connector -> parse JSON -> Cassandra connector
       v
 Cassandra  (keyspace: spark_streams, table: created_users)
```

| Component | Image | Host port | Purpose |
|---|---|---|---|
| Airflow API server / UI (`webserver`) | apache/airflow:3.0.5 | 8080 | UI, API and task execution API |
| Airflow `scheduler`, `dag-processor` | apache/airflow:3.0.5 | - | Schedules runs / parses DAGs |
| Postgres | postgres:14.0 | - | Airflow metadata DB |
| Zookeeper | confluentinc/cp-zookeeper:7.4.0 | 2181 | Kafka coordination |
| Kafka broker | confluentinc/cp-server:7.4.0 | 9092 (host), 29092 (in-network) | Message bus |
| Schema Registry | confluentinc/cp-schema-registry:7.4.0 | 8081 | Schema management |
| Control Center | confluentinc/cp-enterprise-control-center:7.4.0 | 9021 | Kafka UI |
| Spark master / worker | apache/spark:3.5.0 | 7077 (RPC), 9090 (UI) | Runs the streaming job |
| Cassandra | cassandra:latest | 9042 | Sink |

### Repository layout

| Path | Description |
|---|---|
| `docker-compose.yml` | All services |
| `dags/kafka-stream.py` | Airflow DAG `user_automation`: fetches a user from the API and sends it to Kafka |
| `spark-stream-direct.py` | Spark job that runs **inside the Spark container**: Kafka -> Cassandra |
| `spark-stream.py` | Earlier version of the Spark job (Spark 4 packages, `localhost` hosts). Not used on the cluster |
| `script/entrypoint.sh` | Webserver startup: installs requirements, migrates the DB, creates the `admin` user, starts the API server |
| `requirements.txt` | Python dependencies (installed in the Airflow containers and the local venv) |

## Important networking notes

- From the **host / WSL**, use `localhost:<published port>` (Kafka `9092`, Cassandra `9042`, Spark master `7077`).
- From **inside containers**, use compose service names (`broker:29092`, `cassandra`, `spark-master:7077`).
- Container bridge IPs (e.g. `172.18.0.3`) are **not** reachable from the host.
- The Spark job runs inside the cluster (Spark 3.5.0, Scala 2.12), so the connector versions must match: `spark-sql-kafka-0-10_2.12:3.5.0` and `spark-cassandra-connector_2.12:3.5.0`.
- Airflow 3 tasks call the API server's execution API, so all Airflow containers share `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`, `AIRFLOW__API_AUTH__JWT_SECRET` and `AIRFLOW__API__SECRET_KEY` (placeholder values in `docker-compose.yml`, change them for anything beyond local dev).

## Prerequisites

- Docker Desktop (with WSL integration if you use WSL)
- Python 3.12 (only needed for the optional local virtualenv)

## Quick start

### 1. Start the stack

```bash
docker compose up -d
docker compose ps -a
```

The webserver takes a few minutes on first start (`pip install`, DB migration, user creation). Wait for it to show `(healthy)`. The scheduler and dag-processor start after that.

If the dag-processor stays in `Created`, start it manually:

```bash
docker compose up -d dag-processor
```

Airflow UI: http://localhost:8080 (user `admin`, password `admin`).

### 2. Create the Cassandra keyspace and table

```bash
docker exec -it cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS spark_streams WITH replication = {'class':'SimpleStrategy','replication_factor':'1'}; CREATE TABLE IF NOT EXISTS spark_streams.created_users (id UUID PRIMARY KEY, first_name TEXT, last_name TEXT, gender TEXT, address TEXT, post_code TEXT, email TEXT, username TEXT, dob TEXT, registered_date TEXT, phone TEXT, picture TEXT);"
```

### 3. Create the Kafka topic (optional)

Kafka also creates the topic on the first message, but creating it first lets the Spark job start cleanly:

```bash
docker exec broker kafka-topics --bootstrap-server broker:29092 --create --topic users_created --partitions 1 --replication-factor 1
```

### 4. Submit the Spark streaming job

```bash
docker cp spark-stream-direct.py de-streaming-poc-spark-master-1:/opt/spark/work-dir/spark-stream-direct.py

docker exec -it de-streaming-poc-spark-master-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/work-dir/spark-stream-direct.py
```

On PowerShell put the `docker exec` command on a single line (remove the `\` line breaks). The first run is slow while the connector jars download. The job keeps running until you stop it.

### 5. Trigger the DAG

In the Airflow UI unpause `user_automation` and click Trigger, or:

```bash
docker exec de-streaming-poc-webserver-1 airflow dags unpause user_automation
docker exec de-streaming-poc-webserver-1 airflow dags trigger user_automation
```

### 6. Verify the data

```bash
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM spark_streams.created_users;"
docker exec -it cassandra cqlsh -e "SELECT first_name, last_name, email FROM spark_streams.created_users LIMIT 5;"
```

## Command reference

### Docker Compose

```bash
docker compose up -d                                   # start / apply changes (only changed services are recreated)
docker compose up -d webserver scheduler dag-processor # restart only the Airflow services
docker compose up -d --no-deps webserver scheduler dag-processor
docker compose ps -a                                   # status of all containers
docker compose logs webserver --tail 60
docker compose logs scheduler --tail 200
docker compose logs dag-processor --tail 20
docker compose down                                    # stop and remove containers
```

### Airflow

```bash
docker exec de-streaming-poc-webserver-1 airflow dags list
docker exec de-streaming-poc-webserver-1 airflow dags list-import-errors
docker exec de-streaming-poc-webserver-1 airflow dags unpause user_automation
docker exec de-streaming-poc-webserver-1 airflow dags trigger user_automation
```

### Kafka

```bash
docker exec broker kafka-topics --bootstrap-server broker:29092 --list
docker exec broker kafka-topics --bootstrap-server broker:29092 --create --topic users_created --partitions 1 --replication-factor 1
```

### Cassandra

```bash
docker exec -it cassandra cqlsh                        # interactive shell
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM spark_streams.created_users;"
```

### Spark

```bash
docker cp spark-stream-direct.py de-streaming-poc-spark-master-1:/opt/spark/work-dir/spark-stream-direct.py
docker exec -it de-streaming-poc-spark-master-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark/work-dir/spark-stream-direct.py
```

Spark master UI: http://localhost:9090

### Docker inspection

```bash
docker inspect --format '{{json .State.Health}}' de-streaming-poc-webserver-1   # healthcheck history
```

### Connectivity checks (from the host / WSL)

```bash
nc -zv localhost 9092   # Kafka
nc -zv localhost 9042   # Cassandra
nc -zv localhost 7077   # Spark master
```

### Local Python virtualenv (optional)

Only needed for editing/linting; the pipeline itself runs entirely in Docker.

**WSL / Linux / macOS**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If pip reports `externally-managed-environment`, you are not inside the venv. Activate it first (the prompt should start with `(.venv)`) and do not use `--break-system-packages`.

**Windows PowerShell**

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned   # only if activation is blocked (this session only)
Get-ExecutionPolicy -List                                          # inspect the current policy
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

A venv created in WSL (has a `bin/` folder) cannot be activated from PowerShell, and a PowerShell one (has a `Scripts/` folder) cannot be used from WSL. Pick one per venv.

> Note: the locally installed `pyspark` is not used by the pipeline. Running `spark-submit` from the host against the Docker cluster will not work, because the driver must be reachable by the executors and the Spark versions must match. Submit from inside the Spark container as shown above.

## Technical incompatibilities encountered

Everything below was hit while getting this project running. They are listed so the next person can skip them.

| # | Incompatibility | Symptom | Resolution |
|---|---|---|---|
| 1 | **Shell mismatch**: `source` is a bash command | PowerShell: `source : The term 'source' is not recognized` | Use `.\.venv\Scripts\Activate.ps1` in PowerShell, or `source .venv/bin/activate` in bash/WSL |
| 2 | **Venv OS mismatch**: the venv was created in WSL (`bin/`, Linux Python), not Windows (`Scripts/`) | `.venv\bin\activate` cannot be activated from PowerShell | Activate it from WSL, or create a separate Windows venv |
| 3 | **System Python is "externally managed"** (PEP 668) on WSL/Ubuntu | `pip install` -> `error: externally-managed-environment`, `No module named 'pyspark'` | The venv was not activated. Activate it first; never use `--break-system-packages` |
| 4 | **PySpark 4.2.0 (venv) vs Spark 3.5.0 (cluster)**: standalone driver and cluster must run the same Spark version | Job could not work against the Docker cluster | Run the job inside the cluster container (Spark 3.5.0) instead of from the host venv |
| 5 | **Java 25 (WSL) vs Spark 3.5**: Spark 3.5 does not support Java 25 | Local `spark-submit` under Java 25 is unsupported | Use the Spark image's bundled Java by submitting inside the container |
| 6 | **Connector vs Spark version**: `spark-cassandra-connector` 3.5.x targets Spark 3.5, while `spark-sql-kafka` was pinned to 4.2.0 (Spark 4) | Mixed Spark 3.5 / Spark 4 artifacts cannot load together, and no Spark 4 Cassandra connector was available | Use Spark 3.5.0 connectors for both: `...kafka-0-10_2.12:3.5.0` and `...cassandra-connector_2.12:3.5.0` |
| 7 | **Scala version mismatch**: artifacts were `_2.13`, the `apache/spark:3.5.0` image is built for Scala `2.12` | Wrong-Scala jars fail at runtime | Use `_2.12` artifacts |
| 8 | **Docker bridge IP not routable from the host**: `spark://172.18.0.3:7077` | `All masters are unresponsive! Giving up.` | Use published ports on `localhost`, or better run the job inside the Docker network. Executors also cannot reach a driver running on the host/WSL |
| 9 | **`spark.jars.packages` set in code is ignored under `spark-submit`**: the JVM is already running when the Python builder executes | `Failed to find data source: kafka` | Pass `--packages ...` on the `spark-submit` command line |
| 10 | **`cassandra-driver` missing in the Spark image**: script imported `cassandra` at the top | `ModuleNotFoundError: No module named 'cassandra'` | Removed the driver from the Spark job; create the keyspace and table with `cqlsh` |
| 11 | **Hostnames differ by location**: `localhost:9092` / `localhost:9042` work on the host, but inside containers they refer to the container itself | Connection failures when the job runs in Docker | In-cluster script uses `broker:29092` and `cassandra` |
| 12 | **Kafka topic did not exist**: the Spark job started before any message had been produced | `UnknownTopicOrPartitionException` | Create the topic first, or trigger the DAG before starting the job |
| 13 | **Healthcheck window shorter than webserver startup**: entrypoint runs `pip install`, `db migrate`, user creation before serving; the healthcheck gave up after about 90s | `Container ...webserver-1 Error dependency webserver failed to start`; scheduler and dag-processor left in `Created` | `retries: 10` and `start_period: 180s` on the webserver healthcheck |
| 14 | **Airflow 3 split architecture**: the dag-processor is a separate process required to parse DAGs | UI showed no DAGs | Make sure the `dag-processor` container is running |
| 15 | **Airflow 3 execution API not reachable / secrets not shared**: tasks call the API server at `http://localhost:8080/execution/` by default, which is the wrong container from the scheduler; JWT secrets differed per container | Task failed with `state changed externally`, log source `No host supplied` | Set `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://webserver:8080/execution/` and shared `AIRFLOW__API_AUTH__JWT_SECRET` / `AIRFLOW__API__SECRET_KEY` on webserver, scheduler and dag-processor |
| 16 | **Obsolete Airflow setting**: `AIRFLOW_WEBSERVER_SECRET_KEY` is not a real Airflow 3 config name | Setting silently had no effect | Replaced by the proper `AIRFLOW__API__SECRET_KEY` |

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Container ...webserver-1 Error dependency webserver failed to start` | Webserver startup is slower than the default healthcheck window. The compose file sets `retries: 10` and `start_period: 180s`. Run `docker compose up -d` again |
| No DAGs in Airflow | The dag-processor is not running. `docker compose up -d dag-processor` |
| Task fails with "state changed externally" / log source "No host supplied" | Airflow 3 execution API URL or JWT secret not shared between containers. See the env vars in `docker-compose.yml` |
| `Failed to find data source: kafka` | Packages must be passed with `--packages` on `spark-submit`; `spark.jars.packages` set in code is ignored |
| `ModuleNotFoundError: No module named 'cassandra'` in the Spark container | The Spark image has no `cassandra-driver`. Create the keyspace/table with `cqlsh` (step 2) and use `spark-stream-direct.py` |
| `UnknownTopicOrPartitionException` | The topic `users_created` does not exist yet. Create it (step 3) or trigger the DAG first |
| `All masters are unresponsive` when submitting from WSL | Container IPs are not reachable from the host, and driver/cluster versions differ. Submit from inside the Spark master container |
