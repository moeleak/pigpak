package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Directory represents a folder.
type Directory struct {
	ID        int64
	UserID    int64
	ParentID  sql.NullInt64
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// File represents a Telegram-backed file.
type File struct {
	ID           int64
	UserID       int64
	DirID        int64
	Name         string
	FileID       string
	FileUniqueID string
	Size         int64
	MimeType     string
	CreatedAt    time.Time
}

// Share represents a share link.
type Share struct {
	ID        int64
	FileID    int64
	Token     string
	ExpiresAt sql.NullTime
	Uses      int64
	CreatedAt time.Time
}

// UserState keeps UI state for a user.
type UserState struct {
	UserID         int64
	CurrentDirID   sql.NullInt64
	PendingAction  sql.NullString
	PendingTarget  sql.NullInt64
	PendingPayload sql.NullString
	UpdatedAt      time.Time
}

// EnsureUser inserts a user and root directory if missing.
func (s *Store) EnsureUser(ctx context.Context, userID int64) (int64, error) {
	if s == nil {
		return 0, errors.New("nil store")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO users(user_id, created_at) VALUES (?, ?)`, userID, now()); err != nil {
		return 0, err
	}

	var rootID int64
	row := tx.QueryRowContext(ctx, `SELECT id FROM directories WHERE user_id = ? AND parent_id IS NULL LIMIT 1`, userID)
	scanErr := row.Scan(&rootID)
	if scanErr == sql.ErrNoRows {
		res, err := tx.ExecContext(ctx, `INSERT INTO directories(user_id, parent_id, name, created_at, updated_at) VALUES (?, NULL, ?, ?, ?)`, userID, "/", now(), now())
		if err != nil {
			return 0, err
		}
		rootID, err = res.LastInsertId()
		if err != nil {
			return 0, err
		}
	} else if scanErr != nil {
		return 0, scanErr
	}

	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return rootID, nil
}

// GetRootDirID returns the root dir ID for a user.
func (s *Store) GetRootDirID(ctx context.Context, userID int64) (int64, error) {
	var rootID int64
	row := s.DB.QueryRowContext(ctx, `SELECT id FROM directories WHERE user_id = ? AND parent_id IS NULL LIMIT 1`, userID)
	if err := row.Scan(&rootID); err != nil {
		if err == sql.ErrNoRows {
			return s.EnsureUser(ctx, userID)
		}
		return 0, err
	}
	return rootID, nil
}

// GetDirByID fetches a directory by ID.
func (s *Store) GetDirByID(ctx context.Context, userID, dirID int64) (Directory, error) {
	var d Directory
	row := s.DB.QueryRowContext(ctx, `SELECT id, user_id, parent_id, name, created_at, updated_at FROM directories WHERE id = ? AND user_id = ?`, dirID, userID)
	if err := row.Scan(&d.ID, &d.UserID, &d.ParentID, &d.Name, &d.CreatedAt, &d.UpdatedAt); err != nil {
		return d, err
	}
	return d, nil
}

// GetDirByName finds a child directory by name.
func (s *Store) GetDirByName(ctx context.Context, userID, parentID int64, name string) (Directory, error) {
	var d Directory
	row := s.DB.QueryRowContext(ctx, `SELECT id, user_id, parent_id, name, created_at, updated_at FROM directories WHERE user_id = ? AND parent_id = ? AND name = ?`, userID, parentID, name)
	if err := row.Scan(&d.ID, &d.UserID, &d.ParentID, &d.Name, &d.CreatedAt, &d.UpdatedAt); err != nil {
		return d, err
	}
	return d, nil
}

// ListDirs lists directories under a parent.
func (s *Store) ListDirs(ctx context.Context, userID, parentID int64) ([]Directory, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id, user_id, parent_id, name, created_at, updated_at FROM directories WHERE user_id = ? AND parent_id = ? ORDER BY name`, userID, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var dirs []Directory
	for rows.Next() {
		var d Directory
		if err := rows.Scan(&d.ID, &d.UserID, &d.ParentID, &d.Name, &d.CreatedAt, &d.UpdatedAt); err != nil {
			return nil, err
		}
		dirs = append(dirs, d)
	}
	return dirs, rows.Err()
}

// ListFiles lists files under a directory.
func (s *Store) ListFiles(ctx context.Context, userID, dirID int64) ([]File, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id, user_id, dir_id, name, file_id, file_unique_id, size, mime_type, created_at FROM files WHERE user_id = ? AND dir_id = ? ORDER BY name`, userID, dirID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var files []File
	for rows.Next() {
		var f File
		if err := rows.Scan(&f.ID, &f.UserID, &f.DirID, &f.Name, &f.FileID, &f.FileUniqueID, &f.Size, &f.MimeType, &f.CreatedAt); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}

// CreateDir creates a directory under parent.
func (s *Store) CreateDir(ctx context.Context, userID, parentID int64, name string) (Directory, error) {
	res, err := s.DB.ExecContext(ctx, `INSERT INTO directories(user_id, parent_id, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`, userID, parentID, name, now(), now())
	if err != nil {
		return Directory{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return Directory{}, err
	}
	return s.GetDirByID(ctx, userID, id)
}

// RenameDir updates a directory name.
func (s *Store) RenameDir(ctx context.Context, userID, dirID int64, name string) error {
	res, err := s.DB.ExecContext(ctx, `UPDATE directories SET name = ?, updated_at = ? WHERE id = ? AND user_id = ?`, name, now(), dirID, userID)
	if err != nil {
		return err
	}
	count, _ := res.RowsAffected()
	if count == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// MoveDir moves a directory under a new parent.
func (s *Store) MoveDir(ctx context.Context, userID, dirID, newParentID int64) error {
	rootID, err := s.GetRootDirID(ctx, userID)
	if err != nil {
		return err
	}
	if dirID == rootID {
		return errors.New("cannot move root directory")
	}
	if dirID == newParentID {
		return errors.New("cannot move directory into itself")
	}
	isDesc, err := s.isDescendant(ctx, userID, dirID, newParentID)
	if err != nil {
		return err
	}
	if isDesc {
		return errors.New("cannot move directory into its descendant")
	}
	res, err := s.DB.ExecContext(ctx, `UPDATE directories SET parent_id = ?, updated_at = ? WHERE id = ? AND user_id = ?`, newParentID, now(), dirID, userID)
	if err != nil {
		return err
	}
	count, _ := res.RowsAffected()
	if count == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// DeleteDirRecursive deletes a directory and its contents.
func (s *Store) DeleteDirRecursive(ctx context.Context, userID, dirID int64) error {
	rootID, err := s.GetRootDirID(ctx, userID)
	if err != nil {
		return err
	}
	if dirID == rootID {
		return errors.New("cannot delete root directory")
	}
	_, err = s.DB.ExecContext(ctx, `WITH RECURSIVE subtree(id) AS (
		SELECT id FROM directories WHERE id = ? AND user_id = ?
		UNION ALL
		SELECT d.id FROM directories d JOIN subtree s ON d.parent_id = s.id
	) DELETE FROM files WHERE dir_id IN (SELECT id FROM subtree);`, dirID, userID)
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, `WITH RECURSIVE subtree(id) AS (
		SELECT id FROM directories WHERE id = ? AND user_id = ?
		UNION ALL
		SELECT d.id FROM directories d JOIN subtree s ON d.parent_id = s.id
	) DELETE FROM directories WHERE id IN (SELECT id FROM subtree);`, dirID, userID)
	return err
}

// CreateFile inserts a file record.
func (s *Store) CreateFile(ctx context.Context, userID, dirID int64, name, fileID, fileUniqueID string, size int64, mimeType string) (File, error) {
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	res, err := s.DB.ExecContext(ctx, `INSERT INTO files(user_id, dir_id, name, file_id, file_unique_id, size, mime_type, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, userID, dirID, name, fileID, fileUniqueID, size, mimeType, now())
	if err != nil {
		return File{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return File{}, err
	}
	return s.GetFileByID(ctx, userID, id)
}

// UpdateFileTelegram updates the Telegram identifiers for a file.
func (s *Store) UpdateFileTelegram(ctx context.Context, userID, fileID int64, telegramFileID, fileUniqueID string, size int64, mimeType string) error {
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	res, err := s.DB.ExecContext(ctx, `UPDATE files SET file_id = ?, file_unique_id = ?, size = ?, mime_type = ? WHERE id = ? AND user_id = ?`, telegramFileID, fileUniqueID, size, mimeType, fileID, userID)
	if err != nil {
		return err
	}
	count, _ := res.RowsAffected()
	if count == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// GetFileByID fetches a file by ID.
func (s *Store) GetFileByID(ctx context.Context, userID, fileID int64) (File, error) {
	var f File
	row := s.DB.QueryRowContext(ctx, `SELECT id, user_id, dir_id, name, file_id, file_unique_id, size, mime_type, created_at FROM files WHERE id = ? AND user_id = ?`, fileID, userID)
	if err := row.Scan(&f.ID, &f.UserID, &f.DirID, &f.Name, &f.FileID, &f.FileUniqueID, &f.Size, &f.MimeType, &f.CreatedAt); err != nil {
		return f, err
	}
	return f, nil
}

// GetFileByName fetches a file by name within a directory.
func (s *Store) GetFileByName(ctx context.Context, userID, dirID int64, name string) (File, error) {
	var f File
	row := s.DB.QueryRowContext(ctx, `SELECT id, user_id, dir_id, name, file_id, file_unique_id, size, mime_type, created_at FROM files WHERE user_id = ? AND dir_id = ? AND name = ?`, userID, dirID, name)
	if err := row.Scan(&f.ID, &f.UserID, &f.DirID, &f.Name, &f.FileID, &f.FileUniqueID, &f.Size, &f.MimeType, &f.CreatedAt); err != nil {
		return f, err
	}
	return f, nil
}

// RenameFile updates a file name.
func (s *Store) RenameFile(ctx context.Context, userID, fileID int64, name string) error {
	res, err := s.DB.ExecContext(ctx, `UPDATE files SET name = ? WHERE id = ? AND user_id = ?`, name, fileID, userID)
	if err != nil {
		return err
	}
	count, _ := res.RowsAffected()
	if count == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// MoveFile moves a file to another directory.
func (s *Store) MoveFile(ctx context.Context, userID, fileID, newDirID int64) error {
	res, err := s.DB.ExecContext(ctx, `UPDATE files SET dir_id = ? WHERE id = ? AND user_id = ?`, newDirID, fileID, userID)
	if err != nil {
		return err
	}
	count, _ := res.RowsAffected()
	if count == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// DeleteFile removes a file record.
func (s *Store) DeleteFile(ctx context.Context, userID, fileID int64) error {
	res, err := s.DB.ExecContext(ctx, `DELETE FROM files WHERE id = ? AND user_id = ?`, fileID, userID)
	if err != nil {
		return err
	}
	count, _ := res.RowsAffected()
	if count == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// CreateShare creates a share record.
func (s *Store) CreateShare(ctx context.Context, fileID int64, token string, expiresAt *time.Time) (Share, error) {
	var exp any
	if expiresAt != nil {
		exp = expiresAt.UTC()
	}
	res, err := s.DB.ExecContext(ctx, `INSERT INTO shares(file_id, token, expires_at, uses, created_at) VALUES (?, ?, ?, 0, ?)`, fileID, token, exp, now())
	if err != nil {
		return Share{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return Share{}, err
	}
	return s.getShareByID(ctx, id)
}

func (s *Store) getShareByID(ctx context.Context, shareID int64) (Share, error) {
	var sh Share
	row := s.DB.QueryRowContext(ctx, `SELECT id, file_id, token, expires_at, uses, created_at FROM shares WHERE id = ?`, shareID)
	if err := row.Scan(&sh.ID, &sh.FileID, &sh.Token, &sh.ExpiresAt, &sh.Uses, &sh.CreatedAt); err != nil {
		return sh, err
	}
	return sh, nil
}

// GetShareByToken fetches a share and its file.
func (s *Store) GetShareByToken(ctx context.Context, token string) (Share, File, error) {
	var sh Share
	row := s.DB.QueryRowContext(ctx, `SELECT id, file_id, token, expires_at, uses, created_at FROM shares WHERE token = ?`, token)
	if err := row.Scan(&sh.ID, &sh.FileID, &sh.Token, &sh.ExpiresAt, &sh.Uses, &sh.CreatedAt); err != nil {
		return sh, File{}, err
	}
	var f File
	row = s.DB.QueryRowContext(ctx, `SELECT id, user_id, dir_id, name, file_id, file_unique_id, size, mime_type, created_at FROM files WHERE id = ?`, sh.FileID)
	if err := row.Scan(&f.ID, &f.UserID, &f.DirID, &f.Name, &f.FileID, &f.FileUniqueID, &f.Size, &f.MimeType, &f.CreatedAt); err != nil {
		return sh, File{}, err
	}
	return sh, f, nil
}

// IncrementShareUses increments the share use count.
func (s *Store) IncrementShareUses(ctx context.Context, shareID int64) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE shares SET uses = uses + 1 WHERE id = ?`, shareID)
	return err
}

// GetUserState returns the stored user state.
func (s *Store) GetUserState(ctx context.Context, userID int64) (UserState, error) {
	var st UserState
	row := s.DB.QueryRowContext(ctx, `SELECT user_id, current_dir_id, pending_action, pending_target_id, pending_payload, updated_at FROM user_state WHERE user_id = ?`, userID)
	if err := row.Scan(&st.UserID, &st.CurrentDirID, &st.PendingAction, &st.PendingTarget, &st.PendingPayload, &st.UpdatedAt); err != nil {
		return st, err
	}
	return st, nil
}

// EnsureUserState makes sure user_state exists.
func (s *Store) EnsureUserState(ctx context.Context, userID int64) error {
	rootID, err := s.GetRootDirID(ctx, userID)
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, `INSERT OR IGNORE INTO user_state(user_id, current_dir_id, pending_action, pending_target_id, pending_payload, updated_at) VALUES (?, ?, NULL, NULL, NULL, ?)`, userID, rootID, now())
	return err
}

// SetCurrentDir updates current directory.
func (s *Store) SetCurrentDir(ctx context.Context, userID, dirID int64) error {
	if err := s.EnsureUserState(ctx, userID); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, `UPDATE user_state SET current_dir_id = ?, updated_at = ? WHERE user_id = ?`, dirID, now(), userID)
	return err
}

// SetPendingAction sets pending action state.
func (s *Store) SetPendingAction(ctx context.Context, userID int64, action string, targetID int64, payload string) error {
	if err := s.EnsureUserState(ctx, userID); err != nil {
		return err
	}
	var target sql.NullInt64
	if targetID != 0 {
		target = sql.NullInt64{Int64: targetID, Valid: true}
	}
	var payloadNull sql.NullString
	if payload != "" {
		payloadNull = sql.NullString{String: payload, Valid: true}
	}
	_, err := s.DB.ExecContext(ctx, `UPDATE user_state SET pending_action = ?, pending_target_id = ?, pending_payload = ?, updated_at = ? WHERE user_id = ?`, action, target, payloadNull, now(), userID)
	return err
}

// ClearPendingAction clears any pending action.
func (s *Store) ClearPendingAction(ctx context.Context, userID int64) error {
	if err := s.EnsureUserState(ctx, userID); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, `UPDATE user_state SET pending_action = NULL, pending_target_id = NULL, pending_payload = NULL, updated_at = ? WHERE user_id = ?`, now(), userID)
	return err
}

// GetCurrentDirID returns current directory id, creating state if needed.
func (s *Store) GetCurrentDirID(ctx context.Context, userID int64) (int64, error) {
	if err := s.EnsureUserState(ctx, userID); err != nil {
		return 0, err
	}
	var dirID sql.NullInt64
	row := s.DB.QueryRowContext(ctx, `SELECT current_dir_id FROM user_state WHERE user_id = ?`, userID)
	if err := row.Scan(&dirID); err != nil {
		return 0, err
	}
	if dirID.Valid {
		return dirID.Int64, nil
	}
	rootID, err := s.GetRootDirID(ctx, userID)
	if err != nil {
		return 0, err
	}
	if err := s.SetCurrentDir(ctx, userID, rootID); err != nil {
		return 0, err
	}
	return rootID, nil
}

// GetDirPath returns the full path of a directory.
func (s *Store) GetDirPath(ctx context.Context, userID, dirID int64) (string, error) {
	var parts []string
	current := dirID
	for {
		dir, err := s.GetDirByID(ctx, userID, current)
		if err != nil {
			return "", err
		}
		if dir.ParentID.Valid {
			parts = append([]string{dir.Name}, parts...)
			current = dir.ParentID.Int64
			continue
		}
		break
	}
	if len(parts) == 0 {
		return "/", nil
	}
	return "/" + strings.Join(parts, "/"), nil
}

// FindDirByPath resolves a directory path for a user.
func (s *Store) FindDirByPath(ctx context.Context, userID int64, parts []string) (Directory, error) {
	rootID, err := s.GetRootDirID(ctx, userID)
	if err != nil {
		return Directory{}, err
	}
	current, err := s.GetDirByID(ctx, userID, rootID)
	if err != nil {
		return Directory{}, err
	}
	for _, part := range parts {
		if part == "" {
			continue
		}
		child, err := s.GetDirByName(ctx, userID, current.ID, part)
		if err != nil {
			return Directory{}, err
		}
		current = child
	}
	return current, nil
}

// isDescendant checks if targetID is a descendant of dirID.
func (s *Store) isDescendant(ctx context.Context, userID, dirID, targetID int64) (bool, error) {
	row := s.DB.QueryRowContext(ctx, `WITH RECURSIVE subtree(id) AS (
		SELECT id FROM directories WHERE id = ? AND user_id = ?
		UNION ALL
		SELECT d.id FROM directories d JOIN subtree s ON d.parent_id = s.id
	) SELECT 1 FROM subtree WHERE id = ? LIMIT 1`, dirID, userID, targetID)
	var one int
	if err := row.Scan(&one); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return one == 1, nil
}

// ValidateShare checks if share is valid for use.
func ValidateShare(sh Share) error {
	if sh.ExpiresAt.Valid && time.Now().UTC().After(sh.ExpiresAt.Time) {
		return fmt.Errorf("share expired")
	}
	return nil
}
