package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"migration-service/internal/service"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// HTTPServer handles HTTP requests for the migration service
type HTTPServer struct {
	server  *http.Server
	router  *mux.Router
	logger  *logrus.Logger
	service *service.MigrationService
}

// NewHTTPServer creates a new HTTP server
func NewHTTPServer(port int, migrationService *service.MigrationService, logger *logrus.Logger) *HTTPServer {
	router := mux.NewRouter()

	// Create server
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	httpServer := &HTTPServer{
		server:  server,
		router:  router,
		logger:  logger,
		service: migrationService,
	}

	// Setup routes
	httpServer.setupRoutes()

	return httpServer
}

// setupRoutes configures the HTTP routes
func (s *HTTPServer) setupRoutes() {
	// Status endpoint
	s.router.HandleFunc("/status", s.handleStatus).Methods("GET")

	// Health check endpoint
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")

	// Start migration endpoint
	s.router.HandleFunc("/start", s.handleStart).Methods("POST")

	// Stop migration endpoint
	s.router.HandleFunc("/stop", s.handleStop).Methods("POST")

	// DLQ endpoints
	s.router.HandleFunc("/dlq-status", s.handleDLQStatus).Methods("GET")
	s.router.HandleFunc("/reprocess-dlq", s.handleReprocessDLQ).Methods("POST")

	// Default route
	s.router.HandleFunc("/", s.handleRoot).Methods("GET")
}

// handleStatus returns the current migration status
func (s *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Status endpoint called")

	status := s.service.GetStatus()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(status); err != nil {
		s.logger.Errorf("Failed to encode status response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleHealth returns health check information
func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Health check endpoint called")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	err := s.service.HealthCheck(ctx)
	if err != nil {
		s.logger.Errorf("Health check failed: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)

		response := map[string]interface{}{
			"status":  "unhealthy",
			"message": err.Error(),
			"healthy": false,
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			s.logger.Errorf("Failed to encode health response: %v", err)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"status":  "healthy",
		"healthy": true,
		"message": "Service is healthy",
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Errorf("Failed to encode health response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleStart starts the migration process
func (s *HTTPServer) handleStart(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Start migration endpoint called")

	if s.service.IsRunning() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)

		response := map[string]interface{}{
			"status":  "error",
			"message": "Migration is already running",
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			s.logger.Errorf("Failed to encode start response: %v", err)
		}
		return
	}

	// Start migration in a goroutine
	go func() {
		ctx := context.Background()
		if err := s.service.Start(ctx); err != nil {
			s.logger.Errorf("Migration failed to start: %v", err)
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"status":  "started",
		"message": "Migration started successfully",
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Errorf("Failed to encode start response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleStop stops the migration process
func (s *HTTPServer) handleStop(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Stop migration endpoint called")

	if !s.service.IsRunning() && !s.service.IsDLQRunning() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)

		response := map[string]interface{}{
			"status":  "error",
			"message": "Migration is not running",
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			s.logger.Errorf("Failed to encode stop response: %v", err)
		}
		return
	}

	if err := s.service.Stop(); err != nil {
		s.logger.Errorf("Migration failed to stop: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)

		response := map[string]interface{}{
			"status":  "error",
			"message": fmt.Sprintf("Failed to stop migration: %v", err),
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			s.logger.Errorf("Failed to encode stop response: %v", err)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"status":  "stopped",
		"message": "Migration stopped successfully",
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Errorf("Failed to encode stop response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleDLQStatus returns the current DLQ status
func (s *HTTPServer) handleDLQStatus(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("DLQ status endpoint called")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	status, err := s.service.GetDLQStatus(ctx)
	if err != nil {
		s.logger.Errorf("Failed to get DLQ status: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)

		response := map[string]interface{}{
			"status":  "error",
			"message": fmt.Sprintf("Failed to get DLQ status: %v", err),
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			s.logger.Errorf("Failed to encode DLQ status response: %v", err)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(status); err != nil {
		s.logger.Errorf("Failed to encode DLQ status response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleReprocessDLQ triggers DLQ reprocessing
func (s *HTTPServer) handleReprocessDLQ(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Reprocess DLQ endpoint called")

	if s.service.IsRunning() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)

		response := map[string]interface{}{
			"status":  "error",
			"message": "Migration is currently running, DLQ reprocessing cannot start",
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			s.logger.Errorf("Failed to encode reprocess DLQ response: %v", err)
		}
		return
	}

	if s.service.IsDLQRunning() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)

		response := map[string]interface{}{
			"status":  "error",
			"message": "DLQ reprocessing is already running",
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			s.logger.Errorf("Failed to encode reprocess DLQ response: %v", err)
		}
		return
	}

	// Parse request body for batch size
	var request struct {
		BatchSize int `json:"batch_size"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		s.logger.Errorf("Failed to decode request body: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)

		response := map[string]interface{}{
			"status":  "error",
			"message": "Invalid request body",
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			s.logger.Errorf("Failed to encode error response: %v", err)
		}
		return
	}

	// Default batch size if not provided
	if request.BatchSize <= 0 {
		request.BatchSize = 100
	}

	// Start DLQ reprocessing in a goroutine
	go func() {
		ctx := context.Background()
		if err := s.service.ReprocessDLQ(ctx, request.BatchSize); err != nil {
			s.logger.Errorf("DLQ reprocessing failed: %v", err)
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"status":     "started",
		"message":    "DLQ reprocessing started successfully",
		"batch_size": request.BatchSize,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Errorf("Failed to encode reprocess DLQ response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleRoot returns basic information about the service
func (s *HTTPServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"service":     "Enhanced Migration Service with DLQ Support",
		"description": "Patient data migration service from EMR to SIMRS with Dead Letter Queue support",
		"version":     "2.0.0",
		"features": []string{
			"Batch processing with cursor-based pagination",
			"Worker pool using goroutines",
			"Bulk insert into target table",
			"Retry mechanism with exponential backoff",
			"Dead Letter Queue for failed records",
			"DLQ reprocessing capability",
			"Concurrency safety",
			"Structured logging",
		},
		"endpoints": map[string]string{
			"status":        "/status",
			"health":        "/health",
			"start":         "/start",
			"stop":          "/stop",
			"dlq-status":    "/dlq-status",
			"reprocess-dlq": "/reprocess-dlq",
		},
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Errorf("Failed to encode root response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// Start starts the HTTP server
func (s *HTTPServer) Start() error {
	s.logger.Infof("Starting HTTP server on port %d", s.getPort())

	// Start server in a goroutine
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Fatalf("HTTP server failed to start: %v", err)
		}
	}()

	s.logger.Info("HTTP server started successfully")
	return nil
}

// Stop gracefully stops the HTTP server
func (s *HTTPServer) Stop() error {
	s.logger.Info("Stopping HTTP server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.server.Shutdown(ctx); err != nil {
		s.logger.Errorf("HTTP server shutdown failed: %v", err)
		return err
	}

	s.logger.Info("HTTP server stopped")
	return nil
}

// getPort extracts the port from the server address
func (s *HTTPServer) getPort() int {
	// Extract port from address like ":8080"
	if len(s.server.Addr) > 0 && s.server.Addr[0] == ':' {
		var port int
		if _, err := fmt.Sscanf(s.server.Addr, ":%d", &port); err == nil {
			return port
		}
	}
	return 8080 // default port
}
