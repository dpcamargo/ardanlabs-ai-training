// This example shows you how to use the Llama3.2 model to generate SQL queries.
//
// # Running the example:
//
//	$ make openwebui
//    Use the OpenWebUI app with the Llama3.2:latest model.
//
// # This requires running the following commands:
//
//	$ make dev-up        // This starts MongoDB and OpenWebIU in docker compose.
//  $ make dev-ollama-up // This starts the Ollama service.

package main

import (
	"bufio"
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ardanlabs/ai-training/foundation/sqldb"
	"github.com/jmoiron/sqlx"
	"github.com/tmc/langchaingo/llms/ollama"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := dbInit(ctx)
	if err != nil {
		return fmt.Errorf("dbInit: %w", err)
	}

	defer db.Close()

	// -------------------------------------------------------------------------

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Ask a question about the garage sale system: ")

	question, _ := reader.ReadString('\n')
	if question == "" {
		return nil
	}

	fmt.Print("Give me a second...\n\n")

	// -------------------------------------------------------------------------

	query, err := getQuery(ctx, question)
	if err != nil {
		return fmt.Errorf("getQuery: %w", err)
	}

	fmt.Println("QUERY:")
	fmt.Println(query)
	fmt.Print("\n")

	// -------------------------------------------------------------------------

	results := map[string]any{}
	if err := sqldb.QueryMap(ctx, db, query, results); err != nil {
		return fmt.Errorf("execQuery: %w", err)
	}

	fmt.Println("RESULT:")
	for k, v := range results {
		fmt.Printf("KEY: %s, VAL: %v\n", k, v)
	}
	fmt.Print("\n")

	return nil
}

var (
	//go:embed prompts/query.txt
	query string
)

func getQuery(ctx context.Context, question string) (string, error) {

	// Open a connection with ollama to access the model.
	llm, err := ollama.New(ollama.WithModel("llama3.2"))
	if err != nil {
		return "", fmt.Errorf("ollama: %w", err)
	}

	prompt := fmt.Sprintf(query, question)

	result, err := llm.Call(ctx, prompt)
	if err != nil {
		return "", fmt.Errorf("call: %w", err)
	}

	return result, nil
}

// =============================================================================

var (
	//go:embed sql/schema.sql
	schemaSQL string

	//go:embed sql/insert.sql
	insertSQL string
)

func dbInit(ctx context.Context) (*sqlx.DB, error) {
	db, err := dbConnection()
	if err != nil {
		return nil, fmt.Errorf("dbConnection: %w", err)
	}

	if err := dbExecute(ctx, db, schemaSQL); err != nil {
		return nil, fmt.Errorf("dbExecute: %w", err)
	}

	if err := dbExecute(ctx, db, insertSQL); err != nil {
		return nil, fmt.Errorf("dbExecute: %w", err)
	}

	return db, nil
}

func dbConnection() (*sqlx.DB, error) {
	db, err := sqldb.Open(sqldb.Config{
		User:         "postgres",
		Password:     "postgres",
		Host:         "localhost:5432",
		Name:         "postgres",
		MaxIdleConns: 0,
		MaxOpenConns: 0,
		DisableTLS:   true,
	})
	if err != nil {
		return nil, fmt.Errorf("connecting to db: %w", err)
	}

	return db, nil
}

func dbExecute(ctx context.Context, db *sqlx.DB, query string) error {
	if err := sqldb.StatusCheck(ctx, db); err != nil {
		return fmt.Errorf("status check database: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if errTx := tx.Rollback(); errTx != nil {
			if errors.Is(errTx, sql.ErrTxDone) {
				return
			}

			err = fmt.Errorf("rollback: %w", errTx)
			return
		}
	}()

	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}
