package webdav

import (
	"context"
	"crypto/subtle"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/webdav"

	"pigpak/internal/config"
	"pigpak/internal/db"
	"pigpak/internal/telegram"
)

// Server hosts the WebDAV endpoint.
type Server struct {
	cfg   config.Config
	store *db.Store
	tg    *telegram.Client
}

// NewServer creates a WebDAV server.
func NewServer(cfg config.Config, store *db.Store, tg *telegram.Client) (*Server, error) {
	if cfg.WebDAVOwnerID == 0 {
		return nil, errors.New("WEB_DAV_OWNER_ID or STORAGE_CHAT_ID must be set for WebDAV")
	}
	return &Server{cfg: cfg, store: store, tg: tg}, nil
}

// Handler builds the WebDAV handler.
func (s *Server) Handler() http.Handler {
	fs := &davFS{store: s.store, tg: s.tg, ownerID: s.cfg.WebDAVOwnerID, storageChatID: s.cfg.StorageChatID}
	h := &webdav.Handler{
		Prefix:     "/",
		FileSystem: fs,
		LockSystem: webdav.NewMemLS(),
	}
	if s.cfg.WebDAVUser == "" || s.cfg.WebDAVPassword == "" {
		return h
	}
	return basicAuth(h, s.cfg.WebDAVUser, s.cfg.WebDAVPassword)
}

// ListenAndServe starts the WebDAV server.
func (s *Server) ListenAndServe() error {
	server := &http.Server{
		Addr:              s.cfg.WebDAVAddr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return server.ListenAndServe()
}

func basicAuth(next http.Handler, user, pass string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || subtle.ConstantTimeCompare([]byte(u), []byte(user)) != 1 || subtle.ConstantTimeCompare([]byte(p), []byte(pass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="webdav"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type davFS struct {
	store         *db.Store
	tg            *telegram.Client
	ownerID       int64
	storageChatID int64
}

func (fs *davFS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	parentParts, base := splitPath(name)
	if base == "" {
		return nil
	}
	parentDir, err := fs.store.FindDirByPath(ctx, fs.ownerID, parentParts)
	if err != nil {
		return err
	}
	_, err = fs.store.CreateDir(ctx, fs.ownerID, parentDir.ID, base)
	return err
}

func (fs *davFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	if name == "." {
		name = "/"
	}
	entry, err := fs.resolve(ctx, name)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if flag&(os.O_CREATE|os.O_WRONLY|os.O_RDWR) != 0 {
			return fs.createUploadFile(ctx, name, flag)
		}
		return nil, err
	}
	if entry.isDir {
		if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0 {
			return nil, errors.New("cannot write to directory")
		}
		return newDirFile(ctx, fs.store, fs.ownerID, entry.dir.ID), nil
	}

	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE) != 0 {
		return fs.createUploadFile(ctx, name, flag)
	}
	return newReadFile(ctx, fs.tg, entry.file), nil
}

func (fs *davFS) RemoveAll(ctx context.Context, name string) error {
	entry, err := fs.resolve(ctx, name)
	if err != nil {
		return err
	}
	if entry.isDir {
		return fs.store.DeleteDirRecursive(ctx, fs.ownerID, entry.dir.ID)
	}
	return fs.store.DeleteFile(ctx, fs.ownerID, entry.file.ID)
}

func (fs *davFS) Rename(ctx context.Context, oldName, newName string) error {
	entry, err := fs.resolve(ctx, oldName)
	if err != nil {
		return err
	}
	parentParts, base := splitPath(newName)
	if base == "" {
		return errors.New("invalid target name")
	}
	parentDir, err := fs.store.FindDirByPath(ctx, fs.ownerID, parentParts)
	if err != nil {
		return err
	}
	if entry.isDir {
		if err := fs.store.MoveDir(ctx, fs.ownerID, entry.dir.ID, parentDir.ID); err != nil {
			return err
		}
		return fs.store.RenameDir(ctx, fs.ownerID, entry.dir.ID, base)
	}
	if err := fs.store.MoveFile(ctx, fs.ownerID, entry.file.ID, parentDir.ID); err != nil {
		return err
	}
	return fs.store.RenameFile(ctx, fs.ownerID, entry.file.ID, base)
}

func (fs *davFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	entry, err := fs.resolve(ctx, name)
	if err != nil {
		return nil, err
	}
	if entry.isDir {
		return dirInfo(entry.dir), nil
	}
	return fileInfo(entry.file), nil
}

func (fs *davFS) createUploadFile(ctx context.Context, name string, flag int) (webdav.File, error) {
	if fs.storageChatID == 0 {
		return nil, errors.New("STORAGE_CHAT_ID is required for WebDAV uploads")
	}
	parentParts, base := splitPath(name)
	if base == "" {
		return nil, errors.New("invalid file name")
	}
	parentDir, err := fs.store.FindDirByPath(ctx, fs.ownerID, parentParts)
	if err != nil {
		return nil, err
	}
	var existing *db.File
	if entry, err := fs.resolve(ctx, name); err == nil && !entry.isDir {
		existing = &entry.file
	}
	return newUploadFile(ctx, fs.tg, fs.store, fs.ownerID, fs.storageChatID, parentDir.ID, base, existing), nil
}

type davEntry struct {
	isDir bool
	dir   db.Directory
	file  db.File
}

func (fs *davFS) resolve(ctx context.Context, name string) (davEntry, error) {
	clean := path.Clean("/" + name)
	if clean == "/" {
		rootID, err := fs.store.GetRootDirID(ctx, fs.ownerID)
		if err != nil {
			return davEntry{}, err
		}
		dir, err := fs.store.GetDirByID(ctx, fs.ownerID, rootID)
		if err != nil {
			return davEntry{}, err
		}
		return davEntry{isDir: true, dir: dir}, nil
	}
	parts := strings.Split(strings.TrimPrefix(clean, "/"), "/")
	parentParts := parts[:len(parts)-1]
	base := parts[len(parts)-1]
	parentDir, err := fs.store.FindDirByPath(ctx, fs.ownerID, parentParts)
	if err != nil {
		return davEntry{}, err
	}
	if dir, err := fs.store.GetDirByName(ctx, fs.ownerID, parentDir.ID, base); err == nil {
		return davEntry{isDir: true, dir: dir}, nil
	}
	file, err := fs.store.GetFileByName(ctx, fs.ownerID, parentDir.ID, base)
	if err != nil {
		return davEntry{}, os.ErrNotExist
	}
	return davEntry{isDir: false, file: file}, nil
}

func splitPath(name string) ([]string, string) {
	clean := path.Clean("/" + name)
	if clean == "/" {
		return nil, ""
	}
	parts := strings.Split(strings.TrimPrefix(clean, "/"), "/")
	if len(parts) == 1 {
		return nil, parts[0]
	}
	return parts[:len(parts)-1], parts[len(parts)-1]
}

type davFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (fi davFileInfo) Name() string       { return fi.name }
func (fi davFileInfo) Size() int64        { return fi.size }
func (fi davFileInfo) Mode() os.FileMode  { return fi.mode }
func (fi davFileInfo) ModTime() time.Time { return fi.modTime }
func (fi davFileInfo) IsDir() bool        { return fi.isDir }
func (fi davFileInfo) Sys() any           { return nil }

func dirInfo(dir db.Directory) os.FileInfo {
	return davFileInfo{name: dir.Name, size: 0, mode: os.ModeDir | 0o755, modTime: dir.UpdatedAt, isDir: true}
}

func fileInfo(file db.File) os.FileInfo {
	return davFileInfo{name: file.Name, size: file.Size, mode: 0o644, modTime: file.CreatedAt, isDir: false}
}

// dirFile implements webdav.File for directory listing.
type dirFile struct {
	ctx    context.Context
	store  *db.Store
	userID int64
	dirID  int64
	infos  []os.FileInfo
	pos    int
}

func newDirFile(ctx context.Context, store *db.Store, userID, dirID int64) *dirFile {
	return &dirFile{ctx: ctx, store: store, userID: userID, dirID: dirID}
}

func (d *dirFile) Stat() (os.FileInfo, error) {
	dir, err := d.store.GetDirByID(d.ctx, d.userID, d.dirID)
	if err != nil {
		return nil, err
	}
	return dirInfo(dir), nil
}

func (d *dirFile) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (d *dirFile) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("seek not supported on directory")
}

func (d *dirFile) Write(p []byte) (int, error) {
	return 0, errors.New("write not supported on directory")
}

func (d *dirFile) Close() error { return nil }

func (d *dirFile) Readdir(count int) ([]os.FileInfo, error) {
	if d.infos == nil {
		dirs, err := d.store.ListDirs(d.ctx, d.userID, d.dirID)
		if err != nil {
			return nil, err
		}
		files, err := d.store.ListFiles(d.ctx, d.userID, d.dirID)
		if err != nil {
			return nil, err
		}
		for _, dir := range dirs {
			d.infos = append(d.infos, dirInfo(dir))
		}
		for _, file := range files {
			d.infos = append(d.infos, fileInfo(file))
		}
	}
	if d.pos >= len(d.infos) {
		return nil, io.EOF
	}
	if count <= 0 {
		count = len(d.infos) - d.pos
	}
	end := d.pos + count
	if end > len(d.infos) {
		end = len(d.infos)
	}
	chunk := d.infos[d.pos:end]
	d.pos = end
	return chunk, nil
}

// readFile streams from Telegram.
type readFile struct {
	ctx      context.Context
	tg       *telegram.Client
	file     db.File
	filePath string
	offset   int64
	reader   io.ReadCloser
	mu       sync.Mutex
}

func newReadFile(ctx context.Context, tg *telegram.Client, file db.File) *readFile {
	return &readFile{ctx: ctx, tg: tg, file: file}
}

func (f *readFile) Stat() (os.FileInfo, error) {
	return fileInfo(f.file), nil
}

func (f *readFile) ensurePath() (string, error) {
	if f.filePath != "" {
		return f.filePath, nil
	}
	info, err := f.tg.GetFile(f.ctx, f.file.FileID)
	if err != nil {
		return "", err
	}
	f.filePath = info.FilePath
	return f.filePath, nil
}

func (f *readFile) ensureReader() error {
	if f.reader != nil {
		return nil
	}
	path, err := f.ensurePath()
	if err != nil {
		return err
	}
	reader, err := f.tg.DownloadFile(f.ctx, path, f.offset)
	if err != nil {
		return err
	}
	f.reader = reader
	return nil
}

func (f *readFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ensureReader(); err != nil {
		return 0, err
	}
	n, err := f.reader.Read(p)
	f.offset += int64(n)
	if err == io.EOF {
		_ = f.reader.Close()
		f.reader = nil
	}
	return n, err
}

func (f *readFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = f.offset + offset
	case io.SeekEnd:
		newOffset = f.file.Size + offset
	default:
		return f.offset, errors.New("invalid seek")
	}
	if newOffset < 0 {
		return f.offset, errors.New("negative seek")
	}
	f.offset = newOffset
	if f.reader != nil {
		_ = f.reader.Close()
		f.reader = nil
	}
	return f.offset, nil
}

func (f *readFile) Write(p []byte) (int, error) {
	return 0, errors.New("read-only")
}

func (f *readFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reader != nil {
		return f.reader.Close()
	}
	return nil
}

func (f *readFile) Readdir(count int) ([]os.FileInfo, error) {
	return nil, errors.New("not a directory")
}

// uploadFile streams uploads into Telegram.
type uploadFile struct {
	ctx           context.Context
	tg            *telegram.Client
	store         *db.Store
	ownerID       int64
	storageChatID int64
	parentDirID   int64
	name          string
	existing      *db.File
	pipeW         *io.PipeWriter
	done          chan uploadResult
	started       bool
	mu            sync.Mutex
}

type uploadResult struct {
	msg *telegram.Message
	err error
}

func newUploadFile(ctx context.Context, tg *telegram.Client, store *db.Store, ownerID, storageChatID, parentDirID int64, name string, existing *db.File) *uploadFile {
	return &uploadFile{ctx: ctx, tg: tg, store: store, ownerID: ownerID, storageChatID: storageChatID, parentDirID: parentDirID, name: name, existing: existing, done: make(chan uploadResult, 1)}
}

func (f *uploadFile) start() error {
	pr, pw := io.Pipe()
	f.pipeW = pw
	f.started = true
	go func() {
		msg, err := f.tg.UploadDocument(f.ctx, f.storageChatID, f.name, pr)
		f.done <- uploadResult{msg: msg, err: err}
	}()
	return nil
}

func (f *uploadFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.started {
		if err := f.start(); err != nil {
			return 0, err
		}
	}
	return f.pipeW.Write(p)
}

func (f *uploadFile) Close() error {
	f.mu.Lock()
	if f.started {
		_ = f.pipeW.Close()
	}
	f.mu.Unlock()
	if !f.started {
		return nil
	}
	res := <-f.done
	if res.err != nil {
		return res.err
	}
	if res.msg == nil || res.msg.Document == nil {
		return errors.New("telegram upload returned no document")
	}
	doc := res.msg.Document
	if f.existing != nil {
		return f.store.UpdateFileTelegram(f.ctx, f.ownerID, f.existing.ID, doc.FileID, doc.FileUniqueID, doc.FileSize, doc.MimeType)
	}
	_, err := f.store.CreateFile(f.ctx, f.ownerID, f.parentDirID, f.name, doc.FileID, doc.FileUniqueID, doc.FileSize, doc.MimeType)
	return err
}

func (f *uploadFile) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (f *uploadFile) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("seek not supported on upload")
}

func (f *uploadFile) Stat() (os.FileInfo, error) {
	return davFileInfo{name: f.name, size: 0, mode: 0o644, modTime: time.Now().UTC(), isDir: false}, nil
}

func (f *uploadFile) Readdir(count int) ([]os.FileInfo, error) {
	return nil, errors.New("not a directory")
}
