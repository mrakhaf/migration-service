package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"migration-service/internal/config"
	"migration-service/internal/database"
	"migration-service/internal/http"
	"migration-service/internal/repository"
	"migration-service/internal/service"
	"migration-service/internal/transformer"

	"github.com/sirupsen/logrus"
)

var (
	configFile = flag.String("config", "config.yaml", "Path to configuration file")
	dlqMode    = flag.Bool("dlq-mode", false, "Run in DLQ reprocessing mode")
	batchSize  = flag.Int("batch-size", 100, "Batch size for DLQ reprocessing")
)

func main() {
	flag.Parse()

	// Setup structured logging
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
	})
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)

	logger.Info("Starting enhanced migration service with DLQ support")

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize database connections
	dbManager, err := database.NewDatabaseManager(
		cfg.GetSourceDBConnectionString(),
		cfg.GetTargetDBConnectionString(),
		logger,
	)
	if err != nil {
		logger.Errorf("Failed to create database manager: %v", err)
		logger.Info("Starting in demo mode without database connections")

		// Start HTTP server in demo mode
		httpServer := http.NewHTTPServer(cfg.HTTP.Port, nil, logger)
		if err := httpServer.Start(); err != nil {
			logger.Fatalf("Failed to start HTTP server: %v", err)
		}

		logger.Info("Enhanced migration service started in demo mode")

		// Wait for interrupt signal to gracefully shutdown
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		logger.Info("Shutting down enhanced migration service...")

		// Stop HTTP server
		if err := httpServer.Stop(); err != nil {
			logger.Errorf("Failed to stop HTTP server: %v", err)
		}

		logger.Info("Enhanced migration service shutdown complete")
		return
	}
	defer dbManager.Close()

	// Check if we're running in DLQ mode
	if *dlqMode {
		logger.Info("Running in DLQ reprocessing mode")
		runDLQMode(cfg, dbManager, logger)
		return
	}

	// Initialize migration service with DLQ support
	migrationService, err := service.NewMigrationService(cfg, logger)
	if err != nil {
		logger.Fatalf("Failed to initialize migration service: %v", err)
	}

	// Initialize HTTP server
	httpServer := http.NewHTTPServer(cfg.HTTP.Port, migrationService, logger)

	// Start HTTP server
	if err := httpServer.Start(); err != nil {
		logger.Fatalf("Failed to start HTTP server: %v", err)
	}

	logger.Info("Enhanced migration service started successfully")

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down enhanced migration service...")

	// Stop HTTP server
	if err := httpServer.Stop(); err != nil {
		logger.Errorf("Failed to stop HTTP server: %v", err)
	}

	// Stop migration service
	if err := migrationService.Stop(); err != nil {
		logger.Errorf("Failed to stop migration service: %v", err)
	}

	logger.Info("Enhanced migration service shutdown complete")
}

// runDLQMode runs the service in DLQ reprocessing mode
func runDLQMode(cfg *config.Config, dbManager *database.DatabaseManager, logger *logrus.Logger) {
	// Initialize repositories
	sourceRepo := repository.NewSourceRepository(dbManager.SourceDB, logger)
	targetRepo := repository.NewTargetRepository(dbManager.TargetDB, logger)
	dlqRepo := repository.NewDLQRepository(dbManager.TargetDB, logger)

	// Initialize transformer
	transformer := transformer.NewTransformer(logger)

	// Initialize DLQ service
	dlqService := service.NewDLQService(dlqRepo, targetRepo, transformer, logger)

	// Get initial DLQ status
	ctx := context.Background()
	status, err := dlqService.GetDLQStatus(ctx)
	if err != nil {
		logger.Fatalf("Failed to get DLQ status: %v", err)
	}

	logger.Infof("DLQ Status: Total records: %d", status.TotalRecords)

	if status.TotalRecords == 0 {
		logger.Info("No records in DLQ to reprocess")
		return
	}

	// Reprocess DLQ
	logger.Infof("Starting DLQ reprocessing with batch size: %d", *batchSize)
	status, err = dlqService.ReprocessDLQ(ctx, *batchSize)
	if err != nil {
		logger.Fatalf("DLQ reprocessing failed: %v", err)
	}

	logger.Infof("DLQ reprocessing completed. Final status: Total records: %d", status.TotalRecords)

	// Use sourceRepo to avoid unused variable warning
	_ = sourceRepo
}
