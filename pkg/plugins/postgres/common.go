package postgres

import (
	"context"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"time"
)

func Connect(connector *model.Connector) (*gorm.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		connector.Host,
		connector.Port,
		connector.Username,
		connector.Password,
		connector.Database,
	)
	cachedDB := common_sql.GetCachedConnection(dsn)
	if cachedDB != nil {
		return cachedDB, nil
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error),
	})
	if err != nil {
		return nil, err
	}
	common_sql.SetCachedConnection(dsn, db)
	return db, nil
}

func connectPGX(connector *model.Connector) (*pgxpool.Pool, error) {

	config, err := pgxpool.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		connector.Username,
		connector.Password,
		connector.Host,
		connector.Port,
		connector.Database,
	))
	if err != nil {
		return nil, err
	}

	config.ConnConfig.RuntimeParams["replication"] = "database"
	config.ConnConfig.TLSConfig = nil
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	config.MaxConns = 100
	config.MaxConnIdleTime = 360 * time.Second

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}
	return pool, nil
}
