# data-processing

Kafka Connect pipeline that watches directories for files and publishes events to Kafka topics.

## Structure

```
data-processing/
  connect/                        # Kafka Connect Docker image (SpoolDir plugin)
  connectors/
    file-processor/
      backup-data-invoices/       # Default .json invoice files for testing
      invoices/                   # Drop .json invoice files here
      processed/                  # Files moved here after successful processing
      error/                      # Files moved here on failure
      invoices-file-connector.json
  docker-compose.yml
```

## Requirements

- [Docker](https://www.docker.com/) with Compose

## Start

**Linux / macOS:**
```bash
docker compose up --build
```

**Windows (PowerShell):**
```powershell
docker compose up --build
```

## Stop

Stops all containers and **removes volumes** (full clean reset — all Kafka data wiped):

```bash
docker compose down -v
```

To stop without losing data (keeps volumes):

```bash
docker compose down
```

## Register the connector

Wait for Kafka Connect to be ready (usually ~30s), then:

**Linux / macOS:**
```bash
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/file-processor/invoices-file-connector.json
```

**Windows (PowerShell):**
```powershell
Invoke-RestMethod -Method Post `
  -Uri http://localhost:8083/connectors `
  -ContentType "application/json" `
  -InFile "connectors/file-processor/invoices-file-connector.json"
```

## How it works

1. Drop a `.json` file into `connectors/file-processor/invoices/`
2. SpoolDir detects it and publishes the content as an event to `sales.raw.invoice.files.v1`
3. The file is moved to `processed/` on success or `error/` on failure

## Kafka UI

Open [http://localhost:8080](http://localhost:8080) in your browser after starting the stack.
**Linux / macOS:**
```bash
curl http://localhost:8083/connectors/invoices-file-source/status | jq
```

**Windows (PowerShell):**

From there you can:
 - Inspect messages in `sales.processed.results` topic for processed results.

## Check connector status

**Linux / macOS / Windows:**
```bash
docker exec -it data-processing-kafka-1t:8083/connectors/invoices-file-source/status
```

## Consume events (optional)

```bash
docker exec -it <kafka-container> kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sales.raw.invoice.files.v1 \
  --from-beginning
```
