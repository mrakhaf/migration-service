# Migration Service

A high-performance, concurrent data migration service built in Go for transferring patient data between PostgreSQL databases with advanced features including data transformation, dead letter queue (DLQ) support, and comprehensive monitoring.

**Note**: This is an enhanced version (v2.0.0) with improved DLQ support, progress tracking, and concurrent processing capabilities.

## 🚀 Features

- **Concurrent Processing**: Multi-worker architecture with configurable worker pools for high-throughput data migration
- **Data Transformation**: Automatic field mapping and data format conversion between source and target schemas
- **Dead Letter Queue (DLQ)**: Robust error handling with failed record tracking, retry mechanisms, and reprocessing capabilities
- **Batch Processing**: Configurable batch sizes with cursor-based pagination for optimal performance
- **HTTP API**: RESTful interface for monitoring and controlling migration operations
- **Progress Tracking**: Persistent progress tracking with database-backed state management
- **Graceful Shutdown**: Proper cleanup and shutdown handling with configurable timeouts
- **Structured Logging**: JSON-formatted logging with comprehensive error tracking
- **Demo Mode**: Can run without database connections for testing and development
- **Retry Mechanism**: Automatic retry with exponential backoff for failed records
- **Health Monitoring**: Built-in health checks for database connections and service status

## 📋 Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [API Reference](#api-reference)
- [Monitoring](#monitoring)
- [DLQ Management](#dlq-management)
- [Performance Tuning](#performance-tuning)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Source DB     │    │   Migration      │    │   Target DB     │
│   (PostgreSQL)  │───▶│   Service        │───▶│   (PostgreSQL)  │
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Worker Pool    │
                       │   (Concurrent)   │
                       └──────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Transformer    │
                       │   (Field Mapping)│
                       └──────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   DLQ Service    │
                       │   (Error Queue)  │
                       └──────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │ Progress Tracker │
                       │   (Persistent)   │
                       └──────────────────┘
```

### Key Components

- **Database Manager**: Manages connections to source and target databases with health monitoring
- **Worker Pool**: Concurrent processing with configurable worker count and batch distribution
- **Transformer**: Handles data format conversion, field mapping, and validation
- **DLQ Service**: Manages failed records, retry mechanisms, and reprocessing
- **HTTP Server**: Provides monitoring and control APIs with structured responses
- **Migration Service**: Orchestrates the entire migration process with progress tracking
- **Progress Repository**: Persistent storage of migration progress and state

## 📋 Prerequisites

- Go 1.22 or higher
- PostgreSQL 12 or higher (for both source and target databases)
- `uuid-ossp` extension installed on target database

### Installing uuid-ossp Extension

```sql
-- Connect to your target PostgreSQL database
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
```

**Note**: The target database schema is automatically created with the required extension.

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd migration-service
```

### 2. Install Dependencies

```bash
go mod download
go mod verify
```

### 3. Build the Application

```bash
go build -o migration-service ./cmd/main.go
```

## ⚙️ Configuration

The service uses environment variables for configuration. Create a `.env` file in the project root:

```bash
cp .env.example .env
```

### Environment Variables

#### Source Database Configuration
```bash
SOURCE_DB_HOST=localhost
SOURCE_DB_PORT=5432
SOURCE_DB_USER=myuser
SOURCE_DB_PASSWORD=mypassword
SOURCE_DB_NAME=database_emr
SOURCE_DB_SSL_MODE=disable
```

#### Target Database Configuration
```bash
TARGET_DB_HOST=localhost
TARGET_DB_PORT=5432
TARGET_DB_USER=myuser
TARGET_DB_PASSWORD=mypassword
TARGET_DB_NAME=database_simrs
TARGET_DB_SSL_MODE=disable
```

#### Migration Settings
```bash
MIGRATION_BATCH_SIZE=1000          # Records per batch
MIGRATION_WORKER_COUNT=4           # Number of concurrent workers
MIGRATION_MAX_RETRIES=3            # Retry attempts for failed records
MIGRATION_RETRY_DELAY_MS=1000      # Delay between retries (1 second)
MIGRATION_SHUTDOWN_TIMEOUT_MS=30000 # Graceful shutdown timeout (30 seconds)
```

#### Database Connection Settings
```bash
SOURCE_DB_HOST=localhost           # Source database host
SOURCE_DB_PORT=5432                # Source database port
SOURCE_DB_USER=postgres            # Source database user
SOURCE_DB_PASSWORD=password        # Source database password
SOURCE_DB_NAME=database_emr        # Source database name
SOURCE_DB_SSL_MODE=disable         # Source database SSL mode

TARGET_DB_HOST=localhost           # Target database host
TARGET_DB_PORT=5432                # Target database port
TARGET_DB_USER=postgres            # Target database user
TARGET_DB_PASSWORD=password        # Target database password
TARGET_DB_NAME=database_simrs      # Target database name
TARGET_DB_SSL_MODE=disable         # Target database SSL mode
```

#### HTTP Server Settings
```bash
HTTP_PORT=8080                     # API server port
```

### Default Values

If environment variables are not set, the service will use these defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `SOURCE_DB_HOST` | `localhost` | Source database host |
| `SOURCE_DB_PORT` | `5432` | Source database port |
| `SOURCE_DB_USER` | `postgres` | Source database user |
| `SOURCE_DB_PASSWORD` | `password` | Source database password |
| `SOURCE_DB_NAME` | `database_emr` | Source database name |
| `SOURCE_DB_SSL_MODE` | `disable` | SSL mode for source DB |
| `TARGET_DB_HOST` | `localhost` | Target database host |
| `TARGET_DB_PORT` | `5432` | Target database port |
| `TARGET_DB_USER` | `postgres` | Target database user |
| `TARGET_DB_PASSWORD` | `password` | Target database password |
| `TARGET_DB_NAME` | `database_simrs` | Target database name |
| `TARGET_DB_SSL_MODE` | `disable` | SSL mode for target DB |
| `MIGRATION_BATCH_SIZE` | `1000` | Records per batch |
| `MIGRATION_WORKER_COUNT` | `4` | Number of workers |
| `MIGRATION_MAX_RETRIES` | `3` | Maximum retry attempts |
| `MIGRATION_RETRY_DELAY_MS` | `1000` | Retry delay in milliseconds |
| `MIGRATION_SHUTDOWN_TIMEOUT_MS` | `30000` | Shutdown timeout in milliseconds |
| `HTTP_PORT` | `8080` | HTTP server port |

## 🎯 Usage

### Basic Migration

```bash
# Start the migration service
./migration-service
```

### DLQ Reprocessing Mode

```bash
# Reprocess failed records from DLQ
./migration-service -dlq-mode -batch-size 100
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `-config` | Path to configuration file | `config.yaml` |
| `-dlq-mode` | Run in DLQ reprocessing mode | `false` |
| `-batch-size` | Batch size for DLQ reprocessing | `100` |

### Environment Variables

The service supports all environment variables listed in the Configuration section. You can also override them at runtime:

```bash
# Override specific settings
MIGRATION_BATCH_SIZE=5000 MIGRATION_WORKER_COUNT=8 ./migration-service
```

### Demo Mode

The service can run in demo mode without database connections:

```bash
# Start in demo mode (no database required)
./migration-service
```

In demo mode, only the HTTP server starts, allowing you to test the API endpoints.

## 🌐 API Reference

The service provides a RESTful API for monitoring and controlling migration operations.

### Base URL
```
http://localhost:8080
```

### Endpoints

#### Root Information
```http
GET /
```

**Response:**
```json
{
  "service": "Enhanced Migration Service with DLQ Support",
  "description": "Patient data migration service from EMR to SIMRS with Dead Letter Queue support",
  "version": "2.0.0",
  "features": [
    "Batch processing with cursor-based pagination",
    "Worker pool using goroutines",
    "Bulk insert into target table",
    "Retry mechanism with exponential backoff",
    "Dead Letter Queue for failed records",
    "DLQ reprocessing capability",
    "Concurrency safety",
    "Structured logging"
  ],
  "endpoints": {
    "status": "/status",
    "health": "/health",
    "start": "/start",
    "stop": "/stop",
    "dlq-status": "/dlq-status",
    "reprocess-dlq": "/reprocess-dlq"
  }
}
```

#### Health Check
```http
GET /health
```

**Response:**
```json
{
  "status": "healthy",
  "healthy": true,
  "message": "Service is healthy"
}
```

#### Migration Status
```http
GET /status
```

**Response:**
```json
{
  "is_running": true,
  "total_count": 100000,
  "processed_count": 15000,
  "success_count": 14950,
  "failure_count": 50,
  "last_processed_id": 15000,
  "start_time": "2024-01-01T12:00:00Z",
  "elapsed_time": "2m30s",
  "current_status": "running",
  "progress_percentage": 15.0,
  "errors": []
}
```

#### Start Migration
```http
POST /start
```

**Response:**
```json
{
  "status": "started",
  "message": "Migration started successfully"
}
```

#### Stop Migration
```http
POST /stop
```

**Response:**
```json
{
  "status": "stopped",
  "message": "Migration stopped successfully"
}
```

#### DLQ Status
```http
GET /dlq-status
```

**Response:**
```json
{
  "total_records": 25,
  "retry_counts": {
    "0": 15,
    "1": 7,
    "2": 3
  },
  "records": [
    {
      "id": 1,
      "payload": {
        "pasien_uuid": "uuid-here",
        "nama_lengkap": "John Doe",
        "tanggal_lahir": "1990-01-01T00:00:00Z",
        "gender": "M",
        "email": "john@example.com",
        "telepon": "081234567890",
        "alamat_lengkap": "Jl. Contoh No. 123",
        "kota": "Jakarta",
        "provinsi": "DKI Jakarta",
        "kode_pos": "12345",
        "golongan_darah": "O+",
        "nama_kontak_darurat": "Jane Doe",
        "telepon_kontak_darurat": "081234567891",
        "tanggal_registrasi": "2024-01-01T00:00:00Z"
      },
      "error": "Database constraint violation",
      "retry_count": 2,
      "created_at": "2024-01-01T12:00:00Z"
    }
  ]
}
```

#### Reprocess DLQ
```http
POST /reprocess-dlq
Content-Type: application/json

{
  "batch_size": 100
}
```

**Response:**
```json
{
  "status": "started",
  "message": "DLQ reprocessing started successfully",
  "batch_size": 100
}
```

## 📊 Monitoring

### Structured Logging

The service uses JSON-formatted logging for easy parsing and monitoring:

```json
{
  "level": "info",
  "time": "2024-01-01T12:00:00Z",
  "message": "Starting migration service",
  "component": "main",
  "version": "1.0.0"
}
```

### Log Levels

- `debug`: Detailed debugging information
- `info`: General information about service operation
- `warn`: Warning messages
- `error`: Error conditions
- `fatal`: Fatal errors that cause service shutdown

### Metrics to Monitor

1. **Migration Progress**: Track processed vs total records
2. **Worker Status**: Monitor active worker count
3. **DLQ Size**: Watch failed record count
4. **Error Rate**: Monitor retry attempts and failures
5. **Performance**: Batch processing times

## 🔄 DLQ Management

### What is DLQ?

The Dead Letter Queue (DLQ) stores records that failed to migrate due to various reasons such as:
- Data validation errors
- Database constraint violations
- Network connectivity issues
- Transformation errors

### DLQ Schema

The DLQ table is automatically created with the following schema:

```sql
CREATE TABLE dlq_records (
    id SERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    error TEXT NOT NULL,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);
```

**Note**: The DLQ uses a simplified schema compared to the README documentation, with `payload` instead of `original_data` and `error` instead of `error_message`.

### DLQ Operations

#### View DLQ Status
```bash
curl http://localhost:8080/dlq-status
```

#### Reprocess DLQ Records
```bash
# Via API
curl -X POST http://localhost:8080/reprocess-dlq \
  -H "Content-Type: application/json" \
  -d '{"batch_size": 100}'

# Via command line
./migration-service -dlq-mode -batch-size 100
```

#### Manual DLQ Cleanup

```sql
-- View failed records
SELECT id, error, retry_count, created_at 
FROM dlq_records 
ORDER BY created_at DESC;

-- Delete old failed records (after analysis)
DELETE FROM dlq_records 
WHERE created_at < NOW() - INTERVAL '30 days';

-- View retry statistics
SELECT retry_count, COUNT(*) as count
FROM dlq_records 
GROUP BY retry_count 
ORDER BY retry_count;
```

## ⚡ Performance Tuning

### Configuration Optimization

#### High-Volume Migration
```bash
MIGRATION_BATCH_SIZE=5000
MIGRATION_WORKER_COUNT=8
MIGRATION_MAX_RETRIES=5
SOURCE_DB_MAX_CONNS=20
TARGET_DB_MAX_CONNS=40
```

#### Low-Resource Environment
```bash
MIGRATION_BATCH_SIZE=500
MIGRATION_WORKER_COUNT=2
MIGRATION_MAX_RETRIES=2
SOURCE_DB_MAX_CONNS=5
TARGET_DB_MAX_CONNS=10
```

### Database Optimization

#### Source Database
```sql
-- Create indexes for faster reads
CREATE INDEX idx_pasien_id ON pasien(id_pasien);
CREATE INDEX idx_pasien_registrasi ON pasien(tanggal_registrasi);

-- Analyze table for optimal query planning
ANALYZE pasien;
```

#### Target Database
```sql
-- Optimize for bulk inserts
ALTER TABLE pasien SET (fillfactor = 90);

-- Create indexes after migration
CREATE INDEX idx_pasien_uuid ON pasien(pasien_uuid);
CREATE INDEX idx_pasien_email ON pasien(email);

-- Analyze table after migration
ANALYZE pasien;
```

### Network Optimization

For remote databases, consider:
- Connection pooling
- Network compression
- Reduced batch sizes for unstable connections

## 🧪 Development

### Running Tests

```bash
go test ./...
```

### Code Style

The project follows standard Go formatting:
```bash
go fmt ./...
go vet ./...
```

### Local Development Setup

1. **Set up test databases**:
   ```bash
   # Create test databases
   createdb test_source_db
   createdb test_target_db
   
   # Run schema migrations
   psql test_source_db < migration_source.sql
   psql test_target_db < migration_target.sql
   ```

2. **Run with test configuration**:
   ```bash
   SOURCE_DB_NAME=test_source_db TARGET_DB_NAME=test_target_db ./migration-service
   ```

### Debug Mode

Enable debug logging:
```bash
LOG_LEVEL=debug ./migration-service
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Guidelines

- Follow Go best practices and idioms
- Write comprehensive tests with coverage reporting
- Update documentation for new features
- Use structured logging with appropriate levels
- Handle errors gracefully with proper context
- Implement proper resource cleanup and graceful shutdown
- Use goroutines and channels for concurrent operations
- Follow the existing package structure and naming conventions

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For support and questions:

- Create an issue in the repository
- Check the [Wiki](https://github.com/your-repo/migration-service/wiki) for documentation
- Join our [Discord](https://discord.gg/your-server) community

## 🙏 Acknowledgments

- [Go PostgreSQL Driver](https://github.com/jackc/pgx) - Database connectivity
- [Gorilla Mux](https://github.com/gorilla/mux) - HTTP routing
- [Logrus](https://github.com/sirupsen/logrus) - Structured logging
- [Google UUID](https://github.com/google/uuid) - UUID generation
- [joho/godotenv](https://github.com/joho/godotenv) - Environment variable loading

## 📊 Schema Details

### Source Schema (database_emr.pasien)
- `id_pasien` (INT, PRIMARY KEY)
- `nama_depan` (VARCHAR(50))
- `nama_belakang` (VARCHAR(50))
- `tanggal_lahir` (DATE)
- `jenis_kelamin` (VARCHAR(10)) - Values: "Laki-laki", "Perempuan"
- `email` (VARCHAR(100))
- `no_telepon` (VARCHAR(20))
- `alamat` (VARCHAR(200))
- `kota` (VARCHAR(50))
- `provinsi` (VARCHAR(50))
- `kode_pos` (VARCHAR(10))
- `golongan_darah` (VARCHAR(5))
- `kontak_darurat` (VARCHAR(100))
- `no_kontak_darurat` (VARCHAR(20))
- `tanggal_registrasi` (DATE)

### Target Schema (database_simrs.pasien)
- `pasien_uuid` (UUID, PRIMARY KEY, DEFAULT uuid_generate_v4())
- `nama_lengkap` (VARCHAR(100))
- `tanggal_lahir` (DATE)
- `gender` (VARCHAR(10)) - Values: "M", "F"
- `email` (VARCHAR(100))
- `telepon` (VARCHAR(20))
- `alamat_lengkap` (VARCHAR(255))
- `kota` (VARCHAR(50))
- `provinsi` (VARCHAR(50))
- `kode_pos` (VARCHAR(10))
- `golongan_darah` (VARCHAR(5))
- `nama_kontak_darurat` (VARCHAR(100))
- `telepon_kontak_darurat` (VARCHAR(20))
- `tanggal_registrasi` (DATE)

### Data Transformation Rules
- `nama_lengkap` = `nama_depan` + " " + `nama_belakang`
- `gender` mapping: "Laki-laki" → "M", "Perempuan" → "F"
- `pasien_uuid` = Generated UUID
- All other fields are mapped directly with field name changes

---

**Note**: This service is designed for production use with proper error handling, monitoring, and graceful degradation. Always test migrations on a staging environment before running on production data.