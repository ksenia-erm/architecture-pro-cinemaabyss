package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Configuration
type Config struct {
	Port                   string
	MonolithURL            string
	MoviesServiceURL       string
	EventsServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
}

var config Config

func main() {
	// Load configuration from environment
	loadConfig()

	// Set up HTTP routes
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/movies", handleMovies)
	http.HandleFunc("/api/users", handleUsers)
	http.HandleFunc("/api/payments", handlePayments)
	http.HandleFunc("/api/subscriptions", handleSubscriptions)

	log.Printf("Starting proxy service on port %s", config.Port)
	log.Printf("Gradual migration enabled: %v", config.GradualMigration)
	log.Printf("Movies migration percent: %d%%", config.MoviesMigrationPercent)

	log.Fatal(http.ListenAndServe(":"+config.Port, nil))
}

func loadConfig() {
	config.Port = getEnv("PORT", "8000")
	config.MonolithURL = getEnv("MONOLITH_URL", "http://localhost:8080")
	config.MoviesServiceURL = getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")
	config.EventsServiceURL = getEnv("EVENTS_SERVICE_URL", "http://localhost:8082")
	config.GradualMigration = getEnv("GRADUAL_MIGRATION", "false") == "true"

	percentStr := getEnv("MOVIES_MIGRATION_PERCENT", "0")
	percent, err := strconv.Atoi(percentStr)
	if err != nil {
		log.Printf("Invalid MOVIES_MIGRATION_PERCENT: %s, using 0", percentStr)
		percent = 0
	}
	config.MoviesMigrationPercent = percent
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

// Route movies requests with percentage-based migration
func handleMovies(w http.ResponseWriter, r *http.Request) {
	if !config.GradualMigration {
		// If gradual migration is disabled, route to monolith
		proxyToMonolith(w, r)
		return
	}

	// Use percentage-based routing
	shouldUseMicroservice := shouldRouteToMicroservice(config.MoviesMigrationPercent)

	if shouldUseMicroservice {
		log.Printf("Routing movies request to microservice (user: %d%%)", config.MoviesMigrationPercent)
		proxyToMoviesService(w, r)
	} else {
		log.Printf("Routing movies request to monolith (user: %d%%)", config.MoviesMigrationPercent)
		proxyToMonolith(w, r)
	}
}

// Route other requests to monolith (not yet migrated)
func handleUsers(w http.ResponseWriter, r *http.Request) {
	proxyToMonolith(w, r)
}

func handlePayments(w http.ResponseWriter, r *http.Request) {
	proxyToMonolith(w, r)
}

func handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	proxyToMonolith(w, r)
}

// Helper function to determine if request should go to microservice
func shouldRouteToMicroservice(percent int) bool {
	if percent <= 0 {
		return false
	}
	if percent >= 100 {
		return true
	}

	// Generate random number 0-99 and check if it's below the percentage
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(100) < percent
}

// Proxy to movies microservice
func proxyToMoviesService(w http.ResponseWriter, r *http.Request) {
	targetURL, err := url.Parse(config.MoviesServiceURL)
	if err != nil {
		http.Error(w, "Invalid movies service URL", http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = targetURL.Scheme
		req.URL.Host = targetURL.Host
		req.URL.Path = strings.Replace(req.URL.Path, "/api/movies", "/api/movies", 1)
		req.Host = targetURL.Host
	}

	proxy.ServeHTTP(w, r)
}

// Proxy to monolith
func proxyToMonolith(w http.ResponseWriter, r *http.Request) {
	targetURL, err := url.Parse(config.MonolithURL)
	if err != nil {
		http.Error(w, "Invalid monolith URL", http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = targetURL.Scheme
		req.URL.Host = targetURL.Host
		req.Host = targetURL.Host
	}

	proxy.ServeHTTP(w, r)
}
