package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"log"
	"sync"
	"time"
)

const monitor = "tx_monitor"
const monitorCreate = monitor + ":create"
const monitorUpdate = monitor + ":update"
const monitorDelete = monitor + ":delete"
const monitorQuery = monitor + ":query"
const monitorBegin = monitor + ":begin"

type TransactionMonitorInfo struct {
	StartTime  time.Time
	Statements []string
	ConnID     uint32
}

type TransactionMonitor struct {
	transactions sync.Map
	connMap      sync.Map
	callback     CallbackFunc
	explicitTx   sync.Map
}

type CallbackFunc func(operation, sql string, duration time.Duration, tmi *TransactionMonitorInfo, err error)

func RegisterTxMonitor(db *gorm.DB, callback CallbackFunc) error {
	// Check if already registered
	callbacks := db.Callback()
	if callbacks != nil {
		if cp := callbacks.Create().After("gorm:create").Get(monitorBegin); cp != nil {
			return errors.New("tx monitor already registered")
		}
	}

	log.Println("Setting up GORM callbacks")
	monitor := &TransactionMonitor{
		callback: callback,
	}

	monitorCallback := func(scope *gorm.Scope) {
		log.Printf("\nMonitor callback triggered for SQL: %s", scope.SQL)

		// Get the underlying sql.DB or sql.Tx
		commonDB := scope.DB().CommonDB()
		txPtr := ""
		if tx, ok := commonDB.(*sql.Tx); ok {
			txPtr = fmt.Sprintf("%p", tx)
			log.Printf("In transaction. Tx ptr: %s", txPtr)
		} else {
			log.Printf("Not in transaction. DB type: %T", commonDB)
			return
		}

		// Check if this is part of an explicit transaction
		_, isExplicit := monitor.explicitTx.Load(txPtr)
		if !isExplicit {
			log.Printf("Implicit transaction, skipping monitoring")
			return
		}

		// Get connection ID
		connID, err := getConnectionID(commonDB.(*sql.Tx))
		if err != nil {
			log.Printf("Failed to get connection ID: %v", err)
			return
		}

		handleConnectionReuse(monitor, connID, txPtr)

		// Try to get existing TMI
		tmiInterface, ok := monitor.transactions.Load(txPtr)
		if !ok {
			log.Printf("Starting monitoring for transaction %s on connection %d", txPtr, connID)
			tmi := &TransactionMonitorInfo{
				StartTime:  time.Now(),
				Statements: make([]string, 0),
				ConnID:     connID,
			}
			monitor.transactions.Store(txPtr, tmi)
			tmiInterface = tmi
		}

		// Update TMI
		tmi := tmiInterface.(*TransactionMonitorInfo)
		tmi.Statements = append(tmi.Statements, scope.SQL)
		log.Printf("Transaction %s (conn %d) now has %d statements",
			txPtr, connID, len(tmi.Statements))

		// Call callback
		duration := time.Since(tmi.StartTime)
		callback("query", scope.SQL, duration, tmi, scope.DB().Error)
	}

	// Track transaction begin
	db.Callback().Create().Before("gorm:begin_transaction").Register(monitorBegin, func(scope *gorm.Scope) {
		if tx, ok := scope.DB().CommonDB().(*sql.Tx); ok {
			txPtr := fmt.Sprintf("%p", tx)
			if _, exists := monitor.explicitTx.LoadOrStore(txPtr, struct{}{}); !exists {
				connID, err := getConnectionID(scope.DB().CommonDB().(*sql.Tx))
				if err == nil {
					log.Printf("Starting explicit transaction: %s on connection %d", txPtr, connID)
					handleConnectionReuse(monitor, connID, txPtr)
				}
			}
		}
	})

	// Register for all operation types
	db.Callback().Create().After("gorm:create").Register(monitorCreate, monitorCallback)
	db.Callback().Update().After("gorm:update").Register(monitorUpdate, monitorCallback)
	db.Callback().Delete().After("gorm:delete").Register(monitorDelete, monitorCallback)
	db.Callback().Query().After("gorm:query").Register(monitorQuery, monitorCallback)

	return nil
}

func UnregisterTxMonitor(db *gorm.DB) error {
	// Check if already registered
	if cp := db.Callback().Create().Get(monitorBegin); cp == nil {
		return errors.New("tx monitor not registered")
	}

	log.Println("Removing GORM callbacks")
	db.Callback().Create().Before("gorm:begin_transaction").Remove(monitorBegin)
	db.Callback().Create().After("gorm:create").Remove(monitorCreate)
	db.Callback().Update().After("gorm:update").Remove(monitorUpdate)
	db.Callback().Delete().After("gorm:delete").Remove(monitorDelete)
	db.Callback().Query().After("gorm:query").Remove(monitorQuery)

	return nil
}

func getConnectionID(tx *sql.Tx) (uint32, error) {
	var connID uint32
	err := tx.QueryRow("SELECT CONNECTION_ID()").Scan(&connID)
	if err != nil {
		return 0, err
	}
	return connID, nil
}

func handleConnectionReuse(monitor *TransactionMonitor, connID uint32, newTxPtr string) {
	if oldTxPtr, ok := monitor.connMap.Load(connID); ok {
		oldPtr := oldTxPtr.(string)
		if oldPtr != newTxPtr {
			log.Printf("Connection %d reused: old transaction %s -> new transaction %s",
				connID, oldPtr, newTxPtr)
			monitor.transactions.Delete(oldPtr)
			monitor.explicitTx.Delete(oldPtr)
			monitor.connMap.Store(connID, newTxPtr)
		}
	} else {
		monitor.connMap.Store(connID, newTxPtr)
	}
}
