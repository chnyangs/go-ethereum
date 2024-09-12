package nf

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/viper"
)

var (
	DbLock      sync.Mutex
	Db          *sql.DB
	once        sync.Once
	TbP2PServer = "p2pserver"
	TbNodeDisc  = "nodedisc"
	retries     = 3
)

func initDb() error {
	viper.SetConfigName("dbconfig") // The name of the config file without extension
	viper.AddConfigPath("../")      // Path to look for the config file, relative to Folder B/C
	viper.SetConfigType("yaml")     // The file type
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	dbPath := viper.GetString("database.EXECUTION_DB_PATH")
	fmt.Print("dbPath: ", dbPath)
	var nfErr error
	once.Do(func() {
		Db, nfErr = sql.Open("sqlite3", dbPath)
		if nfErr != nil {
			fmt.Println("Error opening database", nfErr)
		}
	})
	return nfErr
}

func InitEecDB() error {
	initDb()
	// Set the maximum number of open and idle connections
	Db.SetMaxOpenConns(1000)
	Db.SetMaxIdleConns(500)

	// Enable WAL mode
	_, nfErr := Db.Exec("PRAGMA journal_mode=WAL;")
	if nfErr != nil {
		return nfErr
	}
	// Create the table if it doesn't exist
	createTableQuery := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		type TEXT,
		name TEXT,
		addr TEXT,
		message TEXT,
		pid TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
`, TbP2PServer)
	_, nfErr = Db.Exec(createTableQuery)
	if nfErr != nil {
		fmt.Printf("Failed to create table: %s", nfErr)
		return nfErr
	}
	// Create the trigger to automatically set created_at if not provided
	createTriggerQueryP2P := fmt.Sprintf(`
	CREATE TRIGGER IF NOT EXISTS set_created_at
	BEFORE INSERT ON %s
	FOR EACH ROW
	WHEN NEW.created_at IS NULL
	BEGIN
		SELECT NEW.created_at = CURRENT_TIMESTAMP;
	END;
`, TbP2PServer)
	_, nfErr = Db.Exec(createTriggerQueryP2P)
	if nfErr != nil {
		return nfErr
	}
	createTableQueryNodeDisc := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			discV TEXT,
			type TEXT,
			agent TEXT,
			msg TEXT,
			tid TEXT,
			tAddr TEXT,
			tKey TEXT,
			nid, TEXT,
			nAddr TEXT,
			nKey TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`, TbNodeDisc)
	_, nfErr = Db.Exec(createTableQueryNodeDisc)
	if nfErr != nil {
		fmt.Println("Error CREATE TABLE nodedisc", nfErr)
	}
	// Create the trigger to automatically set created_at if not provided
	createTriggerQueryNodeDisc := fmt.Sprintf(`
		CREATE TRIGGER IF NOT EXISTS set_created_at
		BEFORE INSERT ON %s
		FOR EACH ROW
		WHEN NEW.created_at IS NULL
		BEGIN
			SELECT NEW.created_at = CURRENT_TIMESTAMP;
		END;
	`, TbNodeDisc)
	_, nfErr = Db.Exec(createTriggerQueryNodeDisc)
	if nfErr != nil {
		fmt.Println("Error CREATE TRIGGER on nodedisc", nfErr)
	}
	return nfErr
}

func InsertLogDynamic(db *sql.DB, tableName string, data map[string]interface{}) error {
	// Lock the database for safe concurrent access
	DbLock.Lock()
	defer DbLock.Unlock()

	// Prepare the slices to hold the columns and placeholders for the query
	var columns []string
	var placeholders []string
	var values []interface{}

	// Loop through the map to dynamically build the query
	for column, value := range data {
		columns = append(columns, column)        // Add the column name
		placeholders = append(placeholders, "?") // Add a placeholder for each value
		values = append(values, value)           // Add the actual value for the placeholder
	}

	// Join the column names and placeholders for the query string
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),      // Join column names with commas
		strings.Join(placeholders, ", ")) // Join placeholders with commas

	// Execute the query with the dynamically created values
	// check if the table lock is held

	for i := 0; i < retries; i++ {
		_, nfErr := db.Exec(query, values...)
		if nfErr != nil {
			if strings.Contains(nfErr.Error(), "database is locked") {
				time.Sleep(100 * time.Millisecond) // Wait and retry
				continue
			}
			return nfErr
		}
		return nil
	}
	return nil
}
