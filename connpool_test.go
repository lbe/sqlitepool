package sqlitepool

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lbe/sqlitepool/internal/queries"
	"github.com/phsym/console-slog"
	_ "modernc.org/sqlite"
)

func init() {
	consoleHandler := console.NewHandler(os.Stderr, &console.HandlerOptions{
		Level:      slog.LevelDebug,
		AddSource:  true,
		TimeFormat: "2006-01-02 15:04:05.000000",
	})

	logger := slog.New(consoleHandler)
	slog.SetDefault(logger)

	slog.Info("sqlitepool tests starting")
}

// setupTestDB creates a temporary SQLite database for testing
func setupTestDB(t testing.TB) (string, func()) {
	// t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}

	db.Close()
	return dbPath, func() {
		// Cleanup will be handled by t.TempDir()
	}
}

func TestNewDbSQLConnPool(t *testing.T) {
	tests := []struct {
		name      string
		config    Config
		wantErr   bool
		errString string
	}{
		{
			name: "invalid max connections",
			config: Config{
				MaxConnections: 0,
				// Stmt:           map[string]string{"test": testSelectSQL},
			},
			wantErr:   true,
			errString: "maxConnections must be greater than 0",
		},
		{
			name: "min idle exceeds max",
			config: Config{
				MaxConnections:     5,
				MinIdleConnections: 10,
				// Stmt:               map[string]string{"test": testSelectSQL},
			},
			wantErr:   true,
			errString: "minIdleConnections (10) cannot exceed maxConnections (5)",
		},
		{
			name: "valid config with defaults",
			config: Config{
				MaxConnections: 8,
				// Stmt:           map[string]string{"test": testSelectSQL},
			},
			wantErr: false,
		},
		{
			name: "valid config with explicit values",
			config: Config{
				MaxConnections:     10,
				MinIdleConnections: 2,
				// Stmt:               map[string]string{"test": testSelectSQL},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath, cleanup := setupTestDB(t)
			defer cleanup()

			ctx := context.Background()
			// Add DriverName to the test case config
			tt.config.DriverName = "sqlite"
			pool, err := NewDbSQLConnPool(ctx, dbPath, tt.config)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if err.Error() != tt.errString {
					t.Errorf("expected error %q, got %q", tt.errString, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if pool == nil {
				t.Fatal("expected non-nil pool")
			}

			if pool.maxConnections != tt.config.MaxConnections {
				t.Errorf("maxConnections = %d, want %d", pool.maxConnections, tt.config.MaxConnections)
			}

			// Check default minIdleConnections
			if tt.config.MinIdleConnections == 0 {
				expected := tt.config.MaxConnections / 4
				if expected < 1 {
					expected = 1
				}
				if pool.minIdleConnections != expected {
					t.Errorf("minIdleConnections = %d, want %d", pool.minIdleConnections, expected)
				}
			}
		})
	}
}

func TestMonitor(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	config := Config{
		DriverName:         "sqlite",
		MaxConnections:     4,
		MinIdleConnections: 2,
		QueriesFunc: func(db queries.DBTX) *queries.Queries {
			return queries.New(db)
		},
		// Stmt:               map[string]string{"test": testSelectSQL},
	}

	pool, err := NewDbSQLConnPool(ctx, dbPath, config)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	// Test maintaining minimum connections
	t.Run("maintains minimum connections", func(t *testing.T) {
		cpcQ := []*CpConn{}
		// Add connections manually since we don't want to wait for the monitor
		for i := int64(0); i < config.MinIdleConnections; i++ {
			// Get a connection properly through the pool's internal method
			slog.Debug("Creating connection for test", "i", i, "current", pool.NumConnections.Load())
			cpc, err := pool.Get()
			if err != nil {
				t.Fatalf("failed to create connection: %v", err)
			}
			err = cpc.Conn.PingContext(ctx)
			if err != nil {
				cpc.Close()
				t.Fatalf("failed to ping connection: %v", err)
			}
			cpcQ = append(cpcQ, cpc)
		}

		for _, cpc := range cpcQ {
			// Return connections to the pool
			pool.Put(cpc)
		}

		// Verify connection count and prepared statements
		idleCount := pool.NumIdleConnections()
		slog.Debug("Created connections and returned to pool", "NumIdleConnections", idleCount)
		if idleCount < int(config.MinIdleConnections) {
			t.Errorf("idle connections = %d, want >= %d", idleCount, config.MinIdleConnections)
		}

		// Verify a connection from the pool has prepared statements
		slog.Debug("Getting connection for test")
		cpc, err := pool.Get()
		if err != nil {
			t.Fatalf("failed to create connection: %v", err)
		}

		// Put the connection back
		slog.Debug("Returning connection to pool")
		pool.Put(cpc)
		slog.Debug("Returned connection to pool")
	})

	t.Run("monitor stops on context cancel", func(t *testing.T) {
		ctxWithCancel, cancel := context.WithCancel(context.Background())
		poolWithCancel, err := NewDbSQLConnPool(ctxWithCancel, dbPath, config)
		if err != nil {
			t.Fatalf("failed to create pool: %v", err)
		}

		// Add one connection to verify it stays after cancel
		cpc, err := poolWithCancel.Get()
		if err != nil {
			t.Fatalf("failed to create connection: %v", err)
		}
		poolWithCancel.Put(cpc)

		go poolWithCancel.Monitor()
		cancel() // Cancel immediately

		// Monitor should have stopped, connection count should remain the same
		initialCount := poolWithCancel.NumIdleConnections()
		time.Sleep(100 * time.Millisecond)
		finalCount := poolWithCancel.NumIdleConnections()

		if finalCount != initialCount {
			t.Errorf("connection count changed after context cancel: initial=%d, final=%d",
				initialCount, finalCount)
		}

		// Cleanup
		poolWithCancel.Close()
	})

	if err := pool.Close(); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestMonitor_Scaling(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	t.Run("grows pool to minIdle", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		config := Config{
			DriverName:         "sqlite",
			MaxConnections:     10,
			MinIdleConnections: 4,
			MonitorInterval:    10 * time.Millisecond,
			QueriesFunc: func(db queries.DBTX) *queries.Queries {
			return queries.New(db)
		},
			// Stmt:               map[string]string{"test": testSelectSQL},
		}

		pool, err := NewDbSQLConnPool(ctx, dbPath, config)
		if err != nil {
			t.Fatalf("failed to create pool: %v", err)
		}
		defer pool.Close()

		go pool.Monitor()

		// Wait for the monitor to create connections
		time.Sleep(200 * time.Millisecond)

		idleCount := pool.NumIdleConnections()
		if idleCount < int(config.MinIdleConnections) {
			t.Errorf("pool did not grow to minIdleConnections. got %d, want %d", idleCount, config.MinIdleConnections)
		}
	})

	t.Run("shrinks pool to minIdle", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		config := Config{
			DriverName:         "sqlite",
			MaxConnections:     10,
			MinIdleConnections: 2,
			MonitorInterval:    20 * time.Millisecond,
			QueriesFunc: func(db queries.DBTX) *queries.Queries {
				return queries.New(db)
			},
			// Stmt:               map[string]string{"test": testSelectSQL},
		}

		pool, err := NewDbSQLConnPool(ctx, dbPath, config)
		if err != nil {
			t.Fatalf("failed to create pool: %v", err)
		}
		defer pool.Close()

		// Manually create connections to exceed minIdle
		conns := make([]*CpConn, 5)
		for i := 0; i < 5; i++ {
			c, err := pool.Get()
			if err != nil {
				t.Fatalf("failed to get connection: %v", err)
			}
			conns[i] = c
		}
		for _, c := range conns {
			pool.Put(c)
		}

		if pool.NumIdleConnections() != 5 {
			t.Fatalf("pre-condition failed: expected 5 idle connections, got %d", pool.NumIdleConnections())
		}

		go pool.Monitor()

		// Wait for the monitor to close connections
		time.Sleep(100 * time.Millisecond)

		idleCount := pool.NumIdleConnections()
		if idleCount > int(config.MinIdleConnections) {
			t.Errorf("pool did not shrink to minIdleConnections. got %d, want %d", idleCount, config.MinIdleConnections)
		}
	})
}

func TestGet(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	config := Config{
		DriverName:         "sqlite",
		MaxConnections:     2,
		MinIdleConnections: 1,
		QueriesFunc: func(db queries.DBTX) *queries.Queries {
			return queries.New(db)
		},
		// Stmt:               map[string]string{"test": testSelectSQL},
	}

	pool, err := NewDbSQLConnPool(ctx, dbPath, config)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer func() {
		if err := pool.Close(); err != nil {
			t.Errorf("failed to close pool: %v", err)
		}
	}()

	t.Run("get connection success", func(t *testing.T) {
		cpc, err := pool.Get()
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		if cpc == nil {
			t.Fatal("expected non-nil connection")
		}

		pool.Put(cpc)
	})

	t.Run("get connection at max capacity", func(t *testing.T) {
		// Get all connections
		conn1, err := pool.Get()
		if err != nil {
			t.Fatalf("failed to get first connection: %v", err)
		}

		conn2, err := pool.Get()
		if err != nil {
			t.Fatalf("failed to get second connection: %v", err)
		}

		// Try to get one more connection
		slog.Debug("Trying to get third connection")
		done := make(chan struct{})
		cancelled := make(chan struct{})

		go func() {
			defer close(done)
			slog.Debug("Getting third connection")
			cpc, err := pool.Get() // This should block
			select {
			case <-cancelled:
				if cpc != nil {
					pool.Put(cpc)
				}
				return
			default:
				if err == nil {
					t.Error("expected Get to block when pool is at capacity")
					pool.Put(cpc)
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}
		}()

		// Wait for goroutine to block
		slog.Debug("Waiting for goroutine to block")
		select {
		case <-done:
			t.Fatal("expected Get to block, but it returned immediately")
		case <-time.After(time.Second):
			// Good - the Get call is blocked as expected
		}

		// Signal cancellation and cleanup
		close(cancelled)

		// Cleanup in reverse order
		pool.Put(conn2)
		pool.Put(conn1)

		// Wait for goroutine to finish before test cleanup
		<-done
	})

	/*
		// t.Run("get after close", func(t *testing.T) {
		// 	if err := pool.Close(); err != nil {
		// 		t.Fatalf("failed to close pool: %v", err)
		// 	}
		// 	cpc, err := pool.Get()
		// 	if err != ErrPoolClosed {
		// 		t.Errorf("expected ErrPoolClosed, got %v", err)
		// 	}
		// 	if cpc != nil {
		// 		t.Error("expected nil connection")
		// 		cpc.Close()
		// 	}
		// })
	*/
}

func TestPut(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	config := Config{
		DriverName:         "sqlite",
		MaxConnections:     2,
		MinIdleConnections: 1,
		QueriesFunc: func(db queries.DBTX) *queries.Queries {
			return queries.New(db)
		},
		// Stmt:               map[string]string{"test": testSelectSQL},
	}

	pool, err := NewDbSQLConnPool(ctx, dbPath, config)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	t.Run("put nil connection", func(t *testing.T) {
		// Should not panic
		pool.Put(nil)
	})

	t.Run("put to active pool", func(t *testing.T) {
		cpc, err := pool.Get()
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}

		// Put the connection back
		pool.Put(cpc)

		// Get the same connection back from the pool
		conn2, err := pool.Get()
		if err != nil {
			t.Fatalf("failed to get connection after put: %v", err)
		}

		pool.Put(conn2)
	})

	t.Run("put to closed pool", func(t *testing.T) {
		cpc, err := pool.Get()
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}

		// Record initial connection count
		initialCount := pool.NumConnections.Load()

		if err := pool.Close(); err != nil {
			t.Fatalf("failed to close pool: %v", err)
		}

		// Put should close the connection and its prepared statements
		pool.Put(cpc)

		// Verify connection count decreased
		finalCount := pool.NumConnections.Load()
		if finalCount != initialCount-1 {
			t.Errorf("connection count = %d, want %d", finalCount, initialCount-1)
		}

		// // Try to use the prepared statement after close - should fail
		// rows2, err := stmt().Query()
		// if err == nil {
		// 	rows2.Close()
		// 	t.Error("prepared statement still usable after connection close")
		// }
	})
}

func TestConcurrency(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	config := Config{
		DriverName:         "sqlite",
		MaxConnections:     5,
		MinIdleConnections: 2,
		QueriesFunc: func(db queries.DBTX) *queries.Queries {
			return queries.New(db)
		},
		// Stmt:               map[string]string{"test": testSelectSQL},
	}

	pool, err := NewDbSQLConnPool(ctx, dbPath, config)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	go pool.Monitor()

	t.Run("concurrent get and put", func(t *testing.T) {
		const numGoroutines = 10
		const iterations = 5

		done := make(chan struct{})
		errChan := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				for j := 0; j < iterations; j++ {
					cpc, err := pool.Get()
					if err != nil {
						if !errors.Is(err, ErrConnectionUnavailable) && !errors.Is(err, ErrPoolClosed) {
							errChan <- err
						}
						continue
					}

					// Simulate some work
					time.Sleep(time.Millisecond * 10)

					pool.Put(cpc)
				}
				done <- struct{}{}
			}()
		}

		// Wait for all goroutines to finish
		for i := 0; i < numGoroutines; i++ {
			select {
			case err := <-errChan:
				t.Errorf("goroutine error: %v", err)
			case <-done:
				// goroutine completed successfully
			case <-time.After(5 * time.Second):
				t.Fatal("test timed out")
			}
		}
	})

	if err := pool.Close(); err != nil {
		t.Fatalf("failed to close pool: %v", err)
	}
}

func TestConnectionError(t *testing.T) {
	err := &ConnectionError{
		Op:  "ping",
		Err: errors.New("connection reset"),
	}

	t.Run("error message", func(t *testing.T) {
		expected := "connection ping failed: connection reset"
		if err.Error() != expected {
			t.Errorf("error message = %q, want %q", err.Error(), expected)
		}
	})

	t.Run("unwrap", func(t *testing.T) {
		underlying := errors.Unwrap(err)
		if underlying == nil || underlying.Error() != "connection reset" {
			t.Errorf("unwrapped error = %v, want 'connection reset'", underlying)
		}
	})
}
