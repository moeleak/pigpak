package db

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
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

// FilePart represents a chunk of a large file.
type FilePart struct {
	ID             int64
	FileID         int64
	PartIndex      int
	TelegramFileID string
	FileUniqueID   string
	Size           int64
	CreatedAt      time.Time
}

// FilePartInput is used to insert file parts.
type FilePartInput struct {
	PartIndex      int
	TelegramFileID string
	FileUniqueID   string
	Size           int64
}

// WebDAVUpload tracks an in-progress WebDAV upload.
type WebDAVUpload struct {
	ID           int64
	UserID       int64
	DirID        int64
	Name         string
	TotalSize    int64
	UploadedSize int64
	MimeType     string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// WebDAVUploadPart represents a persisted upload part.
type WebDAVUploadPart struct {
	ID             int64
	UploadID       int64
	PartIndex      int
	TelegramFileID string
	FileUniqueID   string
	Size           int64
	CreatedAt      time.Time
}

// WebDAVUploadPartInput is used to insert upload parts.
type WebDAVUploadPartInput struct {
	PartIndex      int
	TelegramFileID string
	FileUniqueID   string
	Size           int64
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

func nameConflictError() error {
	return fmt.Errorf("name already exists: %w", os.ErrExist)
}

func (s *Store) ensureNameAvailable(ctx context.Context, userID, parentID int64, name string, excludeDirID, excludeFileID int64) error {
	dir, err := s.GetDirByName(ctx, userID, parentID, name)
	if err == nil {
		if excludeDirID == 0 || dir.ID != excludeDirID {
			return nameConflictError()
		}
	} else if err != sql.ErrNoRows {
		return err
	}
	file, err := s.GetFileByName(ctx, userID, parentID, name)
	if err == nil {
		if excludeFileID == 0 || file.ID != excludeFileID {
			return nameConflictError()
		}
	} else if err != sql.ErrNoRows {
		return err
	}
	return nil
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

// UpsertUserProfile stores the latest username for a user.
func (s *Store) UpsertUserProfile(ctx context.Context, userID int64, username string) error {
	if username == "" {
		return nil
	}
	usernameLower := strings.ToLower(username)
	if _, err := s.DB.ExecContext(ctx, `DELETE FROM user_profiles WHERE username_lower = ? AND user_id != ?`, usernameLower, userID); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO user_profiles(user_id, username, username_lower, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(user_id) DO UPDATE SET username = excluded.username, username_lower = excluded.username_lower, updated_at = excluded.updated_at`,
		userID, username, usernameLower, now())
	return err
}

// GetUserIDByUsername resolves a Telegram username to a user ID.
func (s *Store) GetUserIDByUsername(ctx context.Context, username string) (int64, error) {
	if username == "" {
		return 0, sql.ErrNoRows
	}
	var userID int64
	usernameLower := strings.ToLower(username)
	row := s.DB.QueryRowContext(ctx, `SELECT user_id FROM user_profiles WHERE username_lower = ?`, usernameLower)
	if err := row.Scan(&userID); err != nil {
		return 0, err
	}
	return userID, nil
}

// SetWebDAVPassword stores a password hash for a user.
func (s *Store) SetWebDAVPassword(ctx context.Context, userID int64, password string) error {
	if password == "" {
		return errors.New("password cannot be empty")
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return err
	}
	hash := hashWebDAVPassword(password, salt)
	_, err := s.DB.ExecContext(ctx, `INSERT INTO webdav_credentials(user_id, password_salt, password_hash, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(user_id) DO UPDATE SET password_salt = excluded.password_salt, password_hash = excluded.password_hash, updated_at = excluded.updated_at`,
		userID, hex.EncodeToString(salt), hash, now())
	return err
}

// WebDAVPasswordSet checks if a password is configured.
func (s *Store) WebDAVPasswordSet(ctx context.Context, userID int64) (bool, error) {
	var one int
	row := s.DB.QueryRowContext(ctx, `SELECT 1 FROM webdav_credentials WHERE user_id = ? LIMIT 1`, userID)
	if err := row.Scan(&one); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return one == 1, nil
}

// VerifyWebDAVPassword validates a password against stored hash.
func (s *Store) VerifyWebDAVPassword(ctx context.Context, userID int64, password string) (bool, error) {
	if password == "" {
		return false, nil
	}
	var saltHex, hash string
	row := s.DB.QueryRowContext(ctx, `SELECT password_salt, password_hash FROM webdav_credentials WHERE user_id = ?`, userID)
	if err := row.Scan(&saltHex, &hash); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		return false, err
	}
	expect := hashWebDAVPassword(password, salt)
	ok := subtle.ConstantTimeCompare([]byte(expect), []byte(hash)) == 1
	return ok, nil
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
	if err := s.ensureNameAvailable(ctx, userID, parentID, name, 0, 0); err != nil {
		return Directory{}, err
	}
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
	dir, err := s.GetDirByID(ctx, userID, dirID)
	if err != nil {
		return err
	}
	parentID := int64(0)
	if dir.ParentID.Valid {
		parentID = dir.ParentID.Int64
	}
	if err := s.ensureNameAvailable(ctx, userID, parentID, name, dirID, 0); err != nil {
		return err
	}
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
	dir, err := s.GetDirByID(ctx, userID, dirID)
	if err != nil {
		return err
	}
	if err := s.ensureNameAvailable(ctx, userID, newParentID, dir.Name, dirID, 0); err != nil {
		return err
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
	if err := s.ensureNameAvailable(ctx, userID, dirID, name, 0, 0); err != nil {
		return File{}, err
	}
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

// CreateFileWithParts inserts a file and its parts.
func (s *Store) CreateFileWithParts(ctx context.Context, userID, dirID int64, name, fileID, fileUniqueID string, size int64, mimeType string, parts []FilePartInput) (File, error) {
	if err := s.ensureNameAvailable(ctx, userID, dirID, name, 0, 0); err != nil {
		return File{}, err
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return File{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	res, err := tx.ExecContext(ctx, `INSERT INTO files(user_id, dir_id, name, file_id, file_unique_id, size, mime_type, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, userID, dirID, name, fileID, fileUniqueID, size, mimeType, now())
	if err != nil {
		return File{}, err
	}
	fileRowID, err := res.LastInsertId()
	if err != nil {
		return File{}, err
	}
	if len(parts) > 1 {
		if err := insertFilePartsTx(ctx, tx, fileRowID, parts); err != nil {
			return File{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return File{}, err
	}
	committed = true
	return s.GetFileByID(ctx, userID, fileRowID)
}

// ReplaceFileWithParts updates a file and replaces its parts.
func (s *Store) ReplaceFileWithParts(ctx context.Context, userID, fileID int64, name, telegramFileID, fileUniqueID string, size int64, mimeType string, parts []FilePartInput) error {
	file, err := s.GetFileByID(ctx, userID, fileID)
	if err != nil {
		return err
	}
	if err := s.ensureNameAvailable(ctx, userID, file.DirID, name, 0, fileID); err != nil {
		return err
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	res, err := tx.ExecContext(ctx, `UPDATE files SET name = ?, file_id = ?, file_unique_id = ?, size = ?, mime_type = ? WHERE id = ? AND user_id = ?`, name, telegramFileID, fileUniqueID, size, mimeType, fileID, userID)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return sql.ErrNoRows
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_parts WHERE file_id = ?`, fileID); err != nil {
		return err
	}
	if len(parts) > 1 {
		if err := insertFilePartsTx(ctx, tx, fileID, parts); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
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
	file, err := s.GetFileByID(ctx, userID, fileID)
	if err != nil {
		return err
	}
	if err := s.ensureNameAvailable(ctx, userID, file.DirID, name, 0, fileID); err != nil {
		return err
	}
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
	file, err := s.GetFileByID(ctx, userID, fileID)
	if err != nil {
		return err
	}
	if err := s.ensureNameAvailable(ctx, userID, newDirID, file.Name, 0, fileID); err != nil {
		return err
	}
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

// ListFileParts returns the parts for a file ordered by index.
func (s *Store) ListFileParts(ctx context.Context, fileID int64) ([]FilePart, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id, file_id, part_index, telegram_file_id, file_unique_id, size, created_at FROM file_parts WHERE file_id = ? ORDER BY part_index`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var parts []FilePart
	for rows.Next() {
		var p FilePart
		if err := rows.Scan(&p.ID, &p.FileID, &p.PartIndex, &p.TelegramFileID, &p.FileUniqueID, &p.Size, &p.CreatedAt); err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, rows.Err()
}

func insertFilePartsTx(ctx context.Context, tx *sql.Tx, fileID int64, parts []FilePartInput) error {
	for _, part := range parts {
		if _, err := tx.ExecContext(ctx, `INSERT INTO file_parts(file_id, part_index, telegram_file_id, file_unique_id, size, created_at) VALUES (?, ?, ?, ?, ?, ?)`, fileID, part.PartIndex, part.TelegramFileID, part.FileUniqueID, part.Size, now()); err != nil {
			return err
		}
	}
	return nil
}

// GetWebDAVUpload loads a WebDAV upload by name within a directory.
func (s *Store) GetWebDAVUpload(ctx context.Context, userID, dirID int64, name string) (WebDAVUpload, error) {
	var u WebDAVUpload
	row := s.DB.QueryRowContext(ctx, `SELECT id, user_id, dir_id, name, total_size, uploaded_size, mime_type, created_at, updated_at FROM webdav_uploads WHERE user_id = ? AND dir_id = ? AND name = ?`, userID, dirID, name)
	if err := row.Scan(&u.ID, &u.UserID, &u.DirID, &u.Name, &u.TotalSize, &u.UploadedSize, &u.MimeType, &u.CreatedAt, &u.UpdatedAt); err != nil {
		return u, err
	}
	return u, nil
}

// CreateWebDAVUpload inserts a new WebDAV upload session.
func (s *Store) CreateWebDAVUpload(ctx context.Context, userID, dirID int64, name string, totalSize int64) (WebDAVUpload, error) {
	if totalSize < 0 {
		totalSize = 0
	}
	createdAt := now()
	res, err := s.DB.ExecContext(ctx, `INSERT INTO webdav_uploads(user_id, dir_id, name, total_size, uploaded_size, mime_type, created_at, updated_at) VALUES (?, ?, ?, ?, 0, '', ?, ?)`, userID, dirID, name, totalSize, createdAt, createdAt)
	if err != nil {
		return WebDAVUpload{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return WebDAVUpload{}, err
	}
	return WebDAVUpload{
		ID:           id,
		UserID:       userID,
		DirID:        dirID,
		Name:         name,
		TotalSize:    totalSize,
		UploadedSize: 0,
		MimeType:     "",
		CreatedAt:    createdAt,
		UpdatedAt:    createdAt,
	}, nil
}

// UpdateWebDAVUploadTotal updates the expected total size for an upload.
func (s *Store) UpdateWebDAVUploadTotal(ctx context.Context, uploadID int64, totalSize int64) error {
	if totalSize < 0 {
		totalSize = 0
	}
	_, err := s.DB.ExecContext(ctx, `UPDATE webdav_uploads SET total_size = ?, updated_at = ? WHERE id = ?`, totalSize, now(), uploadID)
	return err
}

// DeleteWebDAVUpload removes an upload session and its parts.
func (s *Store) DeleteWebDAVUpload(ctx context.Context, uploadID int64) error {
	_, err := s.DB.ExecContext(ctx, `DELETE FROM webdav_uploads WHERE id = ?`, uploadID)
	return err
}

// ListWebDAVUploadParts returns the parts for a WebDAV upload ordered by index.
func (s *Store) ListWebDAVUploadParts(ctx context.Context, uploadID int64) ([]WebDAVUploadPart, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id, upload_id, part_index, telegram_file_id, file_unique_id, size, created_at FROM webdav_upload_parts WHERE upload_id = ? ORDER BY part_index`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var parts []WebDAVUploadPart
	for rows.Next() {
		var p WebDAVUploadPart
		if err := rows.Scan(&p.ID, &p.UploadID, &p.PartIndex, &p.TelegramFileID, &p.FileUniqueID, &p.Size, &p.CreatedAt); err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, rows.Err()
}

// AddWebDAVUploadPart stores a new part and updates upload progress.
func (s *Store) AddWebDAVUploadPart(ctx context.Context, uploadID int64, part WebDAVUploadPartInput, mimeType string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	createdAt := now()
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO webdav_upload_parts(upload_id, part_index, telegram_file_id, file_unique_id, size, created_at) VALUES (?, ?, ?, ?, ?, ?)`, uploadID, part.PartIndex, part.TelegramFileID, part.FileUniqueID, part.Size, createdAt)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected > 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE webdav_uploads SET uploaded_size = uploaded_size + ?, updated_at = ? WHERE id = ?`, part.Size, createdAt, uploadID); err != nil {
			return err
		}
	} else if _, err := tx.ExecContext(ctx, `UPDATE webdav_uploads SET updated_at = ? WHERE id = ?`, createdAt, uploadID); err != nil {
		return err
	}
	if mimeType != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE webdav_uploads SET mime_type = CASE WHEN mime_type = '' THEN ? ELSE mime_type END WHERE id = ?`, mimeType, uploadID); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func hashWebDAVPassword(password string, salt []byte) string {
	h := sha256.New()
	_, _ = h.Write(salt)
	_, _ = h.Write([]byte(password))
	return hex.EncodeToString(h.Sum(nil))
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
