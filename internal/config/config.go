package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds the application configuration
type Config struct {
	// Source database configuration
	SourceDB struct {
		Host              string
		Port              int
		User              string
		Password          string
		Database          string
		SSLMode           string
		MaxConns          int
		MinConns          int
		MaxConnLifetime   time.Duration
		MaxConnIdleTime   time.Duration
		HealthCheckPeriod time.Duration
		ConnectTimeout    time.Duration
		ReadTimeout       time.Duration
		WriteTimeout      time.Duration
	}

	// Target database configuration
	TargetDB struct {
		Host              string
		Port              int
		User              string
		Password          string
		Database          string
		SSLMode           string
		MaxConns          int
		MinConns          int
		MaxConnLifetime   time.Duration
		MaxConnIdleTime   time.Duration
		HealthCheckPeriod time.Duration
		ConnectTimeout    time.Duration
		ReadTimeout       time.Duration
		WriteTimeout      time.Duration
	}

	// Migration settings
	Migration struct {
		BatchSize       int
		WorkerCount     int
		MaxRetries      int
		RetryDelay      time.Duration
		ShutdownTimeout time.Duration
	}

	// HTTP server settings
	HTTP struct {
		Port int
	}
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	// Load .env file if it exists
	godotenv.Load()

	cfg := &Config{}

	// Source database configuration
	cfg.SourceDB.Host = getEnv("SOURCE_DB_HOST", "localhost")
	cfg.SourceDB.Port = getEnvAsInt("SOURCE_DB_PORT", 5432)
	cfg.SourceDB.User = getEnv("SOURCE_DB_USER", "postgres")
	cfg.SourceDB.Password = getEnv("SOURCE_DB_PASSWORD", "password")
	cfg.SourceDB.Database = getEnv("SOURCE_DB_NAME", "database_emr")
	cfg.SourceDB.SSLMode = getEnv("SOURCE_DB_SSL_MODE", "disable")

	// Target database configuration
	cfg.TargetDB.Host = getEnv("TARGET_DB_HOST", "localhost")
	cfg.TargetDB.Port = getEnvAsInt("TARGET_DB_PORT", 5432)
	cfg.TargetDB.User = getEnv("TARGET_DB_USER", "postgres")
	cfg.TargetDB.Password = getEnv("TARGET_DB_PASSWORD", "password")
	cfg.TargetDB.Database = getEnv("TARGET_DB_NAME", "database_simrs")
	cfg.TargetDB.SSLMode = getEnv("TARGET_DB_SSL_MODE", "disable")

	// Migration settings
	cfg.Migration.BatchSize = getEnvAsInt("MIGRATION_BATCH_SIZE", 1000)
	cfg.Migration.WorkerCount = getEnvAsInt("MIGRATION_WORKER_COUNT", 4)
	cfg.Migration.MaxRetries = getEnvAsInt("MIGRATION_MAX_RETRIES", 3)
	cfg.Migration.RetryDelay = time.Duration(getEnvAsInt("MIGRATION_RETRY_DELAY_MS", 1000)) * time.Millisecond
	cfg.Migration.ShutdownTimeout = time.Duration(getEnvAsInt("MIGRATION_SHUTDOWN_TIMEOUT_MS", 30000)) * time.Millisecond

	// HTTP server settings
	cfg.HTTP.Port = getEnvAsInt("HTTP_PORT", 8080)

	return cfg, nil
}

// GetSourceDBConnectionString returns the connection string for source database
func (c *Config) GetSourceDBConnectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.SourceDB.Host, c.SourceDB.Port, c.SourceDB.User, c.SourceDB.Password,
		c.SourceDB.Database, c.SourceDB.SSLMode)
}

// GetTargetDBConnectionString returns the connection string for target database
func (c *Config) GetTargetDBConnectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.TargetDB.Host, c.TargetDB.Port, c.TargetDB.User, c.TargetDB.Password,
		c.TargetDB.Database, c.TargetDB.SSLMode)
}

// Helper functions for environment variable parsing
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}
