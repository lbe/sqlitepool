// Package sqlitepool provides a connection pool for SQLite databases using database/sql.
// It manages connection lifecycle including acquisition, validation, release and cleanup.
// The pool maintains both maximum total connections and minimum idle connections,
// automatically scaling between these bounds based on demand.
package sqlitepool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lbe/sqlitepool/internal/queries"
)

// Common errors that can be returned by the connection pool.
var (
	// ErrPoolClosed is returned when attempting to get a connection from a closed pool.
	ErrPoolClosed = errors.New("connection pool is closed")
	// ErrConnectionUnavailable is returned when no connections are available and max connections reached.
	ErrConnectionUnavailable = errors.New("no connections available and max connections reached")
)

// ConnectionError represents an error that occurred with a specific connection.
type ConnectionError struct {
	Op  string // the operation that failed (e.g., "ping", "connect")
	Err error  // the underlying error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("connection %s failed: %v", e.Op, e.Err)
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// Config holds the configuration parameters for DbSQLConnPool.
type Config struct {
	// DriverName specifies the name of the database driver to use (e.g., "sqlite").
	DriverName string

	// Open connection in read-only mode.
	ReadOnly bool

	// Maximum number of connections the pool will create.
	MaxConnections int64

	// Minimum number of idle connections to maintain.
	MinIdleConnections int64

	// QueriesFunc is a function to instantiate a Queries object for a new connection.
	QueriesFunc func(db queries.DBTX) *queries.Queries

	// MonitorInterval specifies how often the connection maintenance monitor should run.
	MonitorInterval time.Duration
}

// CpConn wraps an underlying *sql.Conn and holds a set of prepared queries
// associated with that connection.
type CpConn struct {
	// Conn is the underlying database connection.
	Conn *sql.Conn

	// Queries holds the sqlc-generated query methods for this connection.
	Queries *queries.Queries
}

func (cpc *CpConn) Close() error {
	// Close queries first, then connection
	var errs []error

	// Close both queries and connection even if one fails
	qErr := cpc.Queries.Close()
	cErr := cpc.Conn.Close()

	// If either failed, append to errors
	if qErr != nil {
		errs = append(errs, fmt.Errorf("queries close error: %w", qErr))
	}
	if cErr != nil {
		errs = append(errs, fmt.Errorf("connection close error: %w", cErr))
	}

	// Log that we're done
	if len(errs) > 0 {
		// Log and return whichever error we got first
		for i, err := range errs {
			slog.Error("close error", "err", err, "index", i)
		}
		return errs[0] // Return first error
	}
	return nil
}

func (cpc *CpConn) PragmaOptimize(ctx context.Context) {
	const pragmaOptimize = `PRAGMA optimize;`

	if _, err := cpc.Conn.ExecContext(ctx, pragmaOptimize); err != nil {
		slog.Debug("PRAGMA Optimize", "err", err)
	}
}

// DbSQLConnPool manages a pool of SQL database connections.
// It provides thread-safe access to connections through a buffered channel,
// maintains connection health, and automatically scales the pool size.
type DbSQLConnPool struct {
	// Config holds the pool's configuration.
	Config Config

	// context manages connection lifecycles and cancellation.
	ctx context.Context

	// pool is the underlying database/sql connection pool.
	pool *sql.DB

	// connections is a channel of available connections.
	connections chan *CpConn

	// maxConnections is the maximum number of connections in the pool.
	maxConnections int64

	// minIdleConnections is the minimum number of idle connections to maintain.
	minIdleConnections int64

	// monitorInterval specifies how often the monitor should run.
	monitorInterval time.Duration

	// NumConnections is the current number of connections in the pool.
	NumConnections atomic.Int64

	// mu synchronizes access to the connection pool's state.
	mu *sync.Mutex

	// done channel for graceful shutdown of Monitor.
	done chan struct{}

	// closed indicates if the pool has been closed.
	closed bool
}

// newCpConn creates, pings, and initializes a new CpConn, wrapping a raw *sql.Conn.
func (p *DbSQLConnPool) newCpConn() (*CpConn, error) {
	conn, err := p.pool.Conn(p.ctx)
	if err != nil {
		return nil, &ConnectionError{Op: "connect", Err: err}
	}

	err = conn.PingContext(p.ctx)
	if err != nil {
		conn.Close()
		return nil, &ConnectionError{Op: "ping", Err: err}
	}

	cpc := &CpConn{
		Conn:    conn,
		Queries: p.Config.QueriesFunc(conn),
	}

	return cpc, nil
}

// NewDbSQLConnPool creates a new connection pool with the specified parameters.
// driverName and dataSourceName are passed to sql.Open to create the base pool.
// config specifies the pool's connection limits and behavior.
// Returns error if the database connection cannot be established.
func NewDbSQLConnPool(ctx context.Context, dataSourceName string, config Config) (*DbSQLConnPool, error) {
	if config.DriverName == "" {
		return nil, fmt.Errorf("DriverName must be specified in config")
	}
	if config.MaxConnections <= 0 {
		return nil, fmt.Errorf("maxConnections must be greater than 0")
	}

	// Set default minIdleConnections if not specified
	if config.MinIdleConnections <= 0 {
		config.MinIdleConnections = config.MaxConnections / 4
		if config.MinIdleConnections < 1 {
			config.MinIdleConnections = 1
		}
	}

	// Validate minIdleConnections doesn't exceed maxConnections
	if config.MinIdleConnections > config.MaxConnections {
		return nil, fmt.Errorf("minIdleConnections (%d) cannot exceed maxConnections (%d)",
			config.MinIdleConnections, config.MaxConnections)
	}

	// Set default monitorInterval if not specified
	if config.MonitorInterval <= 0 {
		config.MonitorInterval = 1 * time.Minute
	}

	// create a new connection pool
	db, err := sql.Open(config.DriverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open database %s: %w", dataSourceName, err)
	}

	// return a new DbSQLConnPool instance
	return &DbSQLConnPool{
		Config:             config,
		ctx:                ctx,
		pool:               db,
		connections:        make(chan *CpConn, config.MaxConnections),
		maxConnections:     config.MaxConnections,
		minIdleConnections: config.MinIdleConnections,
		monitorInterval:    config.MonitorInterval,
		mu:                 &sync.Mutex{},
		done:               make(chan struct{}),
	}, nil
}

// DB returns the underlying *sql.DB instance. This method should be used with
// caution, primarily for tools like database migrators that require direct access
// to the `*sql.DB` object. The connection pool should not be in active use when
// the returned `*sql.DB` is being manipulated.
func (p *DbSQLConnPool) DB() *sql.DB {
	return p.pool
}

// Get acquires a connection from the pool. It will:
// - Return an existing idle connection if available
// - Create a new connection if below maxConnections
// - Block waiting for a connection if at maxConnections
// Each returned connection is validated with PingContext before return.
// Returns error if connection cannot be established or validated.
func (p *DbSQLConnPool) Get() (*CpConn, error) {
retry:
	// Check if pool is being shut down (under lock)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	p.mu.Unlock()

	// Try to get an idle connection without blocking
	select {
	case cpc := <-p.connections:
		// Got an idle connection, validate it
		if err := cpc.Conn.PingContext(p.ctx); err != nil {
			slog.Warn("Idle connection failed ping, closing and retrying", "error", err)
			cpc.Close()
			p.NumConnections.Add(-1) // Decrement total count for bad connection
			goto retry               // Try again
		}
		return cpc, nil
	default:
		// No idle connections immediately available.
		// Check if we can create a new connection.
		p.mu.Lock()
		if p.NumConnections.Load() < p.maxConnections {
			p.NumConnections.Add(1) // Increment total count before creating
			p.mu.Unlock()           // Release lock before creating connection (potentially long-running)

			cpc, err := p.newCpConn()
			if err != nil {
				p.NumConnections.Add(-1) // Decrement total count if creation failed
				return nil, err
			}

			// Prepare custom queries after connection is established
			cpc.Queries, err = queries.Prepare(p.ctx, cpc.Conn)
			if err != nil {
				slog.Error("error preparing custom queries", "err", err)
				cpc.Close()
				p.NumConnections.Add(-1) // Decrement total count if query prep failed
				return nil, fmt.Errorf("error preparing custom queries: %w", err)
			}
			return cpc, nil
		}
		p.mu.Unlock() // Release lock before blocking on channel

		// Max connections reached, block until one becomes available.
		select {
		case cpc := <-p.connections:
			if err := cpc.Conn.PingContext(p.ctx); err != nil {
				slog.Warn("Blocked-wait idle connection failed ping, closing and retrying", "error", err)
				cpc.Close()
				p.NumConnections.Add(-1) // Decrement total count for bad connection
				goto retry               // Try again
			}
			return cpc, nil
		case <-p.ctx.Done():
			return nil, &ConnectionError{Op: "acquire", Err: p.ctx.Err()}
		}
	}
}

// Put returns a connection to the pool for reuse.
// The connection is added to the idle pool via the connections channel if the pool is not closed.
// If the pool is closed, the connection is closed immediately.
// This operation never blocks as the channel is buffered to maxConnections.
func (p *DbSQLConnPool) Put(cpc *CpConn) {
	if cpc == nil {
		return
	}

	p.mu.Lock()
	if p.closed {
		// Pool is closed, close the connection instead of returning it
		p.NumConnections.Add(-1) // Decrement total count if pool is closed
		p.mu.Unlock()
		// Make sure we decrement and close even if there's an error
		if err := cpc.Close(); err != nil {
			slog.Error("Error closing connection in closed pool", "err", err)
		}
		return
	}
	p.mu.Unlock() // Release lock before trying to send to channel

	// Return connection to the pool
	select {
	case p.connections <- cpc:
		// Successfully returned to pool
	default:
		// Channel is full (shouldn't happen with proper sizing)
		// Close the connection as we can't return it to the pool
		cpc.Close()
		p.NumConnections.Add(-1) // Decrement total count for excess connection
	}
}

// Close initiates a graceful shutdown of the connection pool and releases all resources.
// It signals the Monitor goroutine to stop, closes all idle connections,
// the connections channel, and the underlying sql.DB.
// Returns error if the underlying pool close operation fails.
// After Close() is called, all subsequent Put() operations will close their connections.
func (p *DbSQLConnPool) Close() error {
	// signal Monitor goroutine to stop first and wait a moment for it to stop
	close(p.done)
	time.Sleep(10 * time.Millisecond)

	// Mark pool as closed under a lock
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	p.closed = true
	p.mu.Unlock()

	// Close all waiting connections first
	drainCount := 0
	for {
		select {
		case conn := <-p.connections:
			drainCount++
			if conn != nil {
				// First close queries, then connection
				if err := conn.Queries.Close(); err != nil {
					slog.Error("Failed to close queries during pool shutdown", "err", err)
				}
				if err := conn.Conn.Close(); err != nil {
					slog.Error("Failed to close connection during pool shutdown", "err", err)
				}
				p.NumConnections.Add(-1)
			}
		default:
			close(p.connections)
			return p.pool.Close()
		}
	}
}

// DbStats returns database/sql DBStats for the underlying sql.DB pool.
func (p *DbSQLConnPool) DbStats() sql.DBStats {
	return p.pool.Stats()
}

// NumIdleConnections returns the current number of idle connections in the pool.
func (p *DbSQLConnPool) NumIdleConnections() int {
	return len(p.connections)
}

// Monitor maintains the pool's connection count within configured bounds.
// Running as a goroutine, it periodically:
// - Creates new connections if idle count is below minIdleConnections
// - Closes excess idle connections if above minIdleConnections
// - Validates connections before adding them to the idle pool
// Exits when context is cancelled or done channel is closed.
func (p *DbSQLConnPool) Monitor() {
	ticker := time.NewTicker(p.monitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.done:
			slog.Debug("DbSQLConnPool monitor stopped due to p.done signal.")
			return
		case <-p.ctx.Done():
			slog.Debug("DbSQLConnPool monitor stopped due to context cancellation.")
			return
		case <-ticker.C:
			p.mu.Lock()
			// Check if pool is closed
			if p.closed {
				p.mu.Unlock()
				return
			}
			p.mu.Unlock() // Release lock early

			currentIdle := int64(len(p.connections))
			currentOpen := p.NumConnections.Load()

			// Grow pool to minIdle if needed
			if currentIdle < p.minIdleConnections && currentOpen < p.maxConnections {
				slog.Debug("DbSQLConnPool monitor: attempting to grow pool",
					"current_idle", currentIdle, "min_idle", p.minIdleConnections,
					"current_open", currentOpen, "max_conns", p.maxConnections)

				// Optimistically increment NumConnections to reserve a slot
				p.NumConnections.Add(1)
				cpc, err := p.newCpConn()
				if err != nil {
					slog.Error("DbSQLConnPool monitor: failed to create new connection", "error", err)
					p.NumConnections.Add(-1) // Rollback count on failure
				} else {
					// Ping the new connection before adding to idle pool
					if err := cpc.Conn.PingContext(p.ctx); err != nil {
						slog.Warn("DbSQLConnPool monitor: new connection failed ping, closing it.", "error", err)
						cpc.Close()
						p.NumConnections.Add(-1) // Rollback count on failure
					} else {
						// Add to idle pool. Use select-default to avoid blocking Monitor.
						select {
						case p.connections <- cpc:
							// Successfully added
						default:
							// This case should ideally not be hit if NumConnections was checked correctly.
							slog.Warn("DbSQLConnPool monitor: failed to add new connection to idle channel (channel full), closing it.", "current_idle", currentIdle, "current_open", currentOpen, "max_conns", p.maxConnections)
							cpc.Close()
							p.NumConnections.Add(-1) // Rollback count if not added
						}
					}
				}
			}

			// Shrink pool if excessively idle
			if currentIdle > p.minIdleConnections && currentOpen > p.minIdleConnections {
				slog.Debug("DbSQLConnPool monitor: attempting to shrink pool",
					"current_idle", currentIdle, "min_idle", p.minIdleConnections,
					"current_open", currentOpen)
				select {
				case cpc := <-p.connections:
					cpc.Close()
					p.NumConnections.Add(-1)
				default:
					// No idle connections to close right now
				}
			}
		}
	}
}
