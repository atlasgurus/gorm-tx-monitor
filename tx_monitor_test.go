package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/stretchr/testify/suite"
)

type TxTestSuite struct {
	suite.Suite
	db *gorm.DB
}

func TestTaskSuite(t *testing.T) {
	suite.Run(t, new(TxTestSuite))
}

// User model for testing
type User struct {
	ID   uint
	Name string
}

func (ts *TxTestSuite) SetupSuite() {
	dsn := os.Getenv("DSN")
	ts.Require().NotEmpty(dsn)

	log.Println("Opening database connection")
	var err error
	ts.db, err = gorm.Open("mysql", dsn)
	ts.Require().NoError(err)

	// Enable GORM logging
	ts.db.LogMode(true)

	// Auto migrate the User model
	log.Println("Running auto migration")
	ts.db.AutoMigrate(&User{})
}

func (ts *TxTestSuite) TearDownSuite() {
	if ts.db != nil {
		ts.db.Close()
	}
}

func (ts *TxTestSuite) SetupTest() {
	ts.db.Exec("DELETE FROM users")
}

func (ts *TxTestSuite) TearDownTest() {
	UnregisterTxMonitor(ts.db)
}

func (ts *TxTestSuite) TestAlreadyRegistered() {
	err := RegisterTxMonitor(ts.db, func(operation, sql string, duration time.Duration, tmi *TransactionMonitorInfo, err error) {
	})
	ts.Require().NoError(err)
	err = RegisterTxMonitor(ts.db, func(operation, sql string, duration time.Duration, tmi *TransactionMonitorInfo, err error) {
	})
	ts.Require().Error(err)
}

func (ts *TxTestSuite) TestUnregister() {
	err := RegisterTxMonitor(ts.db, func(operation, sql string, duration time.Duration, tmi *TransactionMonitorInfo, err error) {
		ts.Require().Fail("Callback should not be called after unregister")
	})
	ts.Require().NoError(err)

	err = UnregisterTxMonitor(ts.db)
	ts.Require().NoError(err)

	err = ts.db.Create(&User{Name: "Test User Unregister"}).Error
	ts.Require().NoError(err)

	var user User
	err = ts.db.First(&user).Error
	ts.Require().NoError(err)
	ts.Require().Equal("Test User Unregister", user.Name)
}

func (ts *TxTestSuite) TestOperationsOutsideTransaction() {
	err := RegisterTxMonitor(ts.db, func(operation, sql string, duration time.Duration, tmi *TransactionMonitorInfo, err error) {
		ts.Require().Fail("Callback should not be called")
	})
	err = ts.db.Create(&User{Name: "Test User 1"}).Error
	ts.Require().NoError(err)

	var user User
	err = ts.db.First(&user).Error
	ts.Require().NoError(err)
	ts.Require().Equal("Test User 1", user.Name)
}

func (ts *TxTestSuite) TestOperationsInsideTransaction() {
	callbackCalls := 0
	err := RegisterTxMonitor(ts.db, func(operation, sql string, duration time.Duration, tmi *TransactionMonitorInfo, err error) {
		ts.Require().NoError(err)
		ts.Require().Equal("query", operation)
		ts.Require().NotZero(duration)
		callbackCalls++
	})
	ts.Require().NoError(err)
	tx := ts.db.Begin()
	ts.Require().NoError(tx.Error)

	err = tx.Create(&User{Name: "Test User 2"}).Error
	ts.Require().NoError(err)

	var user2 User
	err = tx.First(&user2).Error
	ts.Require().NoError(err)

	err = tx.Commit().Error
	ts.Require().NoError(err)

	ts.Require().Equal(2, callbackCalls)
}

func (ts *TxTestSuite) TestMultipleOperationsInTransaction() {
	callbackCalls := 0
	var lastTmi *TransactionMonitorInfo
	err := RegisterTxMonitor(ts.db, func(operation, sql string, duration time.Duration, tmi *TransactionMonitorInfo, err error) {
		ts.Require().NoError(err)
		ts.Require().Equal("query", operation)
		ts.Require().NotZero(duration)
		callbackCalls++

		if callbackCalls == 5 {
			ts.Require().Greater(duration, 2*time.Second, "duration should be greater than 5s")
			lastTmi = tmi
		}
	})

	tx := ts.db.Begin()
	ts.Require().NoError(tx.Error)

	for i := 0; i < 3; i++ {
		err := tx.Create(&User{Name: fmt.Sprintf("Test User Batch %d", i)}).Error
		ts.Require().NoError(err)
	}

	tx.Exec("SELECT SLEEP(2)")

	for i := 0; i < 3; i++ {
		err := tx.Create(&User{Name: fmt.Sprintf("Test User Batch %d", i)}).Error
		ts.Require().NoError(err)
	}

	err = tx.Commit().Error
	ts.Require().NoError(err)

	ts.Require().Equal(6, callbackCalls)
	ts.Require().NotNil(lastTmi)
	ts.Require().Equal(6, len(lastTmi.Statements))
}

func (ts *TxTestSuite) TestParallelOperationsInMultipleGoroutines() {
	var wg sync.WaitGroup
	numGoroutines := 50
	numUsersPerGoroutine := 100
	callbackCalls := 0
	var lastTmi sync.Map

	err := RegisterTxMonitor(ts.db, func(operation, sql string, duration time.Duration, tmi *TransactionMonitorInfo, err error) {
		callbackCalls++
		lastTmi.Store(tmi.ConnID, tmi)
	})
	ts.Require().NoError(err)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			tx := ts.db.Begin()
			ts.Require().NoError(tx.Error)
			for j := 0; j < numUsersPerGoroutine; j++ {
				userName := fmt.Sprintf("Goroutine %d User %d", goroutineID, j)
				err := tx.Create(&User{Name: userName}).Error
				ts.Require().NoError(err)
			}
			err := tx.Commit().Error
			ts.Require().NoError(err)
		}(i)
	}

	wg.Wait()

	ts.Require().Equal(numGoroutines*numUsersPerGoroutine, callbackCalls)
	tmiCount := 0
	lastTmi.Range(func(key, value interface{}) bool {
		tmi := value.(*TransactionMonitorInfo)
		ts.Require().Equal(numUsersPerGoroutine, len(tmi.Statements))
		tmiCount++
		return true
	})
	ts.Require().Equal(numGoroutines, tmiCount)
}
