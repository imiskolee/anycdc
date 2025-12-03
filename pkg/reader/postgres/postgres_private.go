package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"log"
	"strings"
	"time"
)

func (s *PostgresReader) connect() {
	connector, err := config.GetConnector(s.conf.Connector)
	if err != nil {
		panic(err)
	}
	config, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		connector.Username,
		connector.Password,
		connector.Host,
		connector.Port,
		connector.Database,
	))
	if err != nil {
		panic(err)
	}
	config.RuntimeParams["replication"] = "database"
	config.TLSConfig = nil
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(s.ctx, config)
	if err != nil {
		panic(err)
	}
	s.conn = conn
}

func (s *PostgresReader) prepare() error {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)`
	err := s.conn.QueryRow(s.ctx, query, s.conf.Extras[PostgresExtraPublicationName]).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		sql := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
			s.conf.Extras[PostgresExtraPublicationName],
			strings.Join(s.conf.Tables, ","),
		)
		_, err := s.conn.Exec(s.ctx, sql)
		if err != nil {
			return err
		}
	} else {
		var currentLSN pglogrepl.LSN
		query := `select received_lsn from pg_stat_replication where pubname = $1`
		row := s.conn.QueryRow(s.ctx, query, s.conf.Extras[PostgresExtraPublicationName])
		row.Scan(&currentLSN)
		s.clientXLogPos = currentLSN
	}

	currentTables, err := getPublicationTables(s.ctx, s.conn, s.conf.Extras[PostgresExtraPublicationName])
	if err != nil {
		return err
	}
	needAlert, add, drop := compareTables(currentTables, s.conf.Tables)
	if needAlert {
		if err := alterPublicationTables(s.ctx, s.conn, s.conf.Extras[PostgresExtraPublicationName], add, drop); err != nil {
			return err
		}
	}
	return s.prepareSlot()
}

func (s *PostgresReader) prepareSlot() error {
	var exists bool
	err := s.conn.QueryRow(s.ctx, `
		SELECT EXISTS(
			SELECT 1 FROM pg_replication_slots 
			WHERE slot_name = $1 AND slot_type = 'logical'
		)
	`, s.conf.Extras[PostgresExtraSlotName]).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		_, err := s.conn.Exec(s.ctx, fmt.Sprintf(
			"SELECT pg_create_logical_replication_slot('%s', '%s')",
			s.conf.Extras[PostgresExtraSlotName], "pgoutput",
		))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresReader) start() error {
	log.Printf("Starting Reader:%s from LSN %s\n", s.conf.Connector, s.clientXLogPos)
	pluginArgs := []string{
		fmt.Sprintf("publication_names '%s'", s.conf.Extras[PostgresExtraPublicationName]),
		"proto_version '2'",
		"messages 'true'",
		"streaming 'true'",
	}
	err := pglogrepl.StartReplication(
		s.ctx,
		s.conn.PgConn(),
		s.conf.Extras[PostgresExtraSlotName],
		s.clientXLogPos,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArgs,
			Mode:       pglogrepl.LogicalReplication,
		},
	)
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	for {
		ctx, cancel := context.WithTimeout(s.ctx, 1*time.Second)
		msg, err := s.conn.PgConn().ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue // 超时重试
			}
			return fmt.Errorf("接收消息失败: %w", err)
		}
		if err := s.handler(msg); err != nil {
			continue
		}
		_ = pglogrepl.SendStandbyStatusUpdate(context.Background(),
			s.conn.PgConn(),
			pglogrepl.StandbyStatusUpdate{WALWritePosition: s.clientXLogPos})
	}
}

func (s *PostgresReader) getState() pglogrepl.LSN {
	var lsn pglogrepl.LSN
	state := s.opt.StateLoader.Load()
	if state != "" {
		if err := json.Unmarshal([]byte(state), &lsn); err != nil {
			panic(err)
		}
	}
	return lsn
}

func getPublicationTables(ctx context.Context, pool *pgx.Conn, pubName string) ([]string, error) {
	query := `
		SELECT c.relname 
		FROM pg_publication p
		JOIN pg_publication_rel pr ON p.oid = pr.prpubid
		JOIN pg_class c ON pr.prrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE p.pubname = $1
		ORDER BY 1;
	`
	rows, err := pool.Query(ctx, query, pubName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tbl string
		if err := rows.Scan(&tbl); err != nil {
			return nil, err
		}
		tables = append(tables, tbl)
	}
	return tables, rows.Err()
}

func compareTables(current, target []string) (needAlter bool, add []string, drop []string) {
	currentMap := make(map[string]bool)
	for _, tbl := range current {
		currentMap[tbl] = true
	}
	targetMap := make(map[string]bool)
	for _, tbl := range target {
		targetMap[tbl] = true
	}

	// 找出需要新增的表（目标有，当前无）
	for tbl := range targetMap {
		if !currentMap[tbl] {
			add = append(add, tbl)
		}
	}

	// 找出需要删除的表（当前有，目标无）
	for tbl := range currentMap {
		if !targetMap[tbl] {
			drop = append(drop, tbl)
		}
	}

	needAlter = len(add) > 0 || len(drop) > 0
	return
}

func alterPublicationTables(ctx context.Context, pool *pgx.Conn, pubName string, addTables, dropTables []string) error {
	// 开始事务
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 添加表
	for _, tbl := range addTables {
		query := fmt.Sprintf(`ALTER PUBLICATION %s ADD TABLE %s`,
			quoteIdentifier(pubName),
			quoteIdentifier(tbl),
		)
		if _, err := tx.Exec(ctx, query); err != nil {
			return fmt.Errorf("添加表%s失败: %w", tbl, err)
		}
	}

	// 删除表
	for _, tbl := range dropTables {
		query := fmt.Sprintf(`ALTER PUBLICATION %s DROP TABLE %s`,
			quoteIdentifier(pubName),
			quoteIdentifier(tbl),
		)
		if _, err := tx.Exec(ctx, query); err != nil {
			return fmt.Errorf("删除表%s失败: %w", tbl, err)
		}
	}
	// 提交事务
	return tx.Commit(ctx)
}

func quoteIdentifier(ident string) string {
	parts := strings.Split(ident, ".")
	for i, part := range parts {
		parts[i] = pgx.Identifier{part}.Sanitize()
	}
	return strings.Join(parts, ".")
}
