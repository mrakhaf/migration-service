package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// DatabaseManager manages database connections
type DatabaseManager struct {
	SourceDB *pgxpool.Pool
	TargetDB *pgxpool.Pool
	logger   *logrus.Logger
}

// NewDatabaseManager creates a new database manager
func NewDatabaseManager(sourceConnStr, targetConnStr string, logger *logrus.Logger) (*DatabaseManager, error) {
	sourceConfig, err := pgxpool.ParseConfig(sourceConnStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source database config: %w", err)
	}

	targetConfig, err := pgxpool.ParseConfig(targetConnStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse target database config: %w", err)
	}

	// Configure connection pools
	sourceConfig.MaxConns = 10
	sourceConfig.MinConns = 2
	sourceConfig.MaxConnLifetime = 30 * time.Minute
	sourceConfig.MaxConnIdleTime = 10 * time.Minute
	sourceConfig.HealthCheckPeriod = 1 * time.Minute

	targetConfig.MaxConns = 20
	targetConfig.MinConns = 5
	targetConfig.MaxConnLifetime = 30 * time.Minute
	targetConfig.MaxConnIdleTime = 10 * time.Minute
	targetConfig.HealthCheckPeriod = 1 * time.Minute

	sourceDB, err := pgxpool.NewWithConfig(context.Background(), sourceConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to source database: %w", err)
	}

	targetDB, err := pgxpool.NewWithConfig(context.Background(), targetConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to target database: %w", err)
	}

	// Test connections
	if err := sourceDB.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping source database: %w", err)
	}

	if err := targetDB.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping target database: %w", err)
	}

	logger.Info("Database connections established successfully")

	return &DatabaseManager{
		SourceDB: sourceDB,
		TargetDB: targetDB,
		logger:   logger,
	}, nil
}

// Close closes all database connections
func (dm *DatabaseManager) Close() {
	dm.logger.Info("Closing database connections...")
	dm.SourceDB.Close()
	dm.TargetDB.Close()
	dm.logger.Info("Database connections closed")
}

// HealthCheck performs health check on both databases
func (dm *DatabaseManager) HealthCheck(ctx context.Context) error {
	if err := dm.SourceDB.Ping(ctx); err != nil {
		return fmt.Errorf("source database health check failed: %w", err)
	}

	if err := dm.TargetDB.Ping(ctx); err != nil {
		return fmt.Errorf("target database health check failed: %w", err)
	}

	return nil
}
