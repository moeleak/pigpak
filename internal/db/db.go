package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps DB access.
type Store struct {
	DB *sql.DB
}

// Open opens the SQLite database and runs migrations.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=foreign_keys(1)", path))
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &Store{DB: db}
	if err := store.Migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

// Close closes the database.
func (s *Store) Close() error {
	if s == nil || s.DB == nil {
		return nil
	}
	return s.DB.Close()
}

// Migrate applies schema changes.
func (s *Store) Migrate(ctx context.Context) error {
	statements := []string{
		`PRAGMA foreign_keys = ON;`,
		`CREATE TABLE IF NOT EXISTS users (
			user_id INTEGER PRIMARY KEY,
			created_at TIMESTAMP NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS directories (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			parent_id INTEGER,
			name TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE,
			FOREIGN KEY(parent_id) REFERENCES directories(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS files (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			dir_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			file_id TEXT NOT NULL,
			file_unique_id TEXT NOT NULL,
			size INTEGER NOT NULL,
			mime_type TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE,
			FOREIGN KEY(dir_id) REFERENCES directories(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS file_parts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_id INTEGER NOT NULL,
			part_index INTEGER NOT NULL,
			telegram_file_id TEXT NOT NULL,
			file_unique_id TEXT NOT NULL,
			size INTEGER NOT NULL,
			created_at TIMESTAMP NOT NULL,
			FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,
			UNIQUE(file_id, part_index)
		);`,
		`CREATE TABLE IF NOT EXISTS webdav_uploads (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			dir_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			total_size INTEGER NOT NULL DEFAULT 0,
			uploaded_size INTEGER NOT NULL DEFAULT 0,
			mime_type TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE,
			FOREIGN KEY(dir_id) REFERENCES directories(id) ON DELETE CASCADE,
			UNIQUE(user_id, dir_id, name)
		);`,
		`CREATE TABLE IF NOT EXISTS webdav_upload_parts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			upload_id INTEGER NOT NULL,
			part_index INTEGER NOT NULL,
			telegram_file_id TEXT NOT NULL,
			file_unique_id TEXT NOT NULL,
			size INTEGER NOT NULL,
			created_at TIMESTAMP NOT NULL,
			FOREIGN KEY(upload_id) REFERENCES webdav_uploads(id) ON DELETE CASCADE,
			UNIQUE(upload_id, part_index)
		);`,
		`CREATE TABLE IF NOT EXISTS shares (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_id INTEGER NOT NULL,
			token TEXT NOT NULL UNIQUE,
			expires_at TIMESTAMP,
			uses INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMP NOT NULL,
			FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS user_state (
			user_id INTEGER PRIMARY KEY,
			current_dir_id INTEGER,
			pending_action TEXT,
			pending_target_id INTEGER,
			pending_payload TEXT,
			updated_at TIMESTAMP NOT NULL,
			FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE,
			FOREIGN KEY(current_dir_id) REFERENCES directories(id) ON DELETE SET NULL
		);`,
		`CREATE TABLE IF NOT EXISTS user_profiles (
			user_id INTEGER PRIMARY KEY,
			username TEXT,
			username_lower TEXT NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS webdav_credentials (
			user_id INTEGER PRIMARY KEY,
			password_salt TEXT NOT NULL,
			password_hash TEXT NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
		);`,
		`CREATE INDEX IF NOT EXISTS idx_dirs_parent ON directories(user_id, parent_id);`,
		`CREATE INDEX IF NOT EXISTS idx_files_dir ON files(user_id, dir_id);`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_user_profiles_username_lower ON user_profiles(username_lower);`,
		`CREATE INDEX IF NOT EXISTS idx_webdav_credentials_user ON webdav_credentials(user_id);`,
		`CREATE INDEX IF NOT EXISTS idx_parts_file ON file_parts(file_id, part_index);`,
		`CREATE INDEX IF NOT EXISTS idx_webdav_uploads_path ON webdav_uploads(user_id, dir_id, name);`,
		`CREATE INDEX IF NOT EXISTS idx_webdav_upload_parts_upload ON webdav_upload_parts(upload_id, part_index);`,
		`CREATE INDEX IF NOT EXISTS idx_shares_token ON shares(token);`,
	}
	for _, stmt := range statements {
		if _, err := s.DB.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// now returns current UTC time.
func now() time.Time {
	return time.Now().UTC()
}
