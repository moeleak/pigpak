package webdav

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	return &Server{cfg: cfg, store: store, tg: tg}, nil
}

// Handler builds the WebDAV handler.
func (s *Server) Handler() http.Handler {
	fs := &davFS{
		store:         s.store,
		tg:            s.tg,
		storageChatID: s.cfg.StorageChatID,
		maxPartSize:   s.cfg.MaxPartSizeBytes,
	}
	h := &webdav.Handler{
		Prefix:     "/",
		FileSystem: fs,
		LockSystem: webdav.NewMemLS(),
	}
	return s.wrapAuth(h)
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

func (s *Server) wrapAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok || username == "" {
			w.Header().Set("WWW-Authenticate", `Basic realm="webdav"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		userID, err := s.store.GetUserIDByUsername(r.Context(), username)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				w.Header().Set("WWW-Authenticate", `Basic realm="webdav"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		ok, err = s.store.VerifyWebDAVPassword(r.Context(), userID, password)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="webdav"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), webdavUserKey{}, userID)
		ctx = context.WithValue(ctx, webdavContentLengthKey{}, r.ContentLength)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type davFS struct {
	store         *db.Store
	tg            *telegram.Client
	storageChatID int64
	maxPartSize   int64
}

type webdavUserKey struct{}
type webdavContentLengthKey struct{}

func (fs *davFS) userID(ctx context.Context) (int64, error) {
	val := ctx.Value(webdavUserKey{})
	if val == nil {
		return 0, errors.New("missing webdav user")
	}
	userID, ok := val.(int64)
	if !ok || userID == 0 {
		return 0, errors.New("invalid webdav user")
	}
	return userID, nil
}

func (fs *davFS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	userID, err := fs.userID(ctx)
	if err != nil {
		return err
	}
	parentParts, base := splitPath(name)
	if base == "" {
		return nil
	}
	parentDir, err := fs.store.FindDirByPath(ctx, userID, parentParts)
	if err != nil {
		return err
	}
	_, err = fs.store.CreateDir(ctx, userID, parentDir.ID, base)
	return err
}

func (fs *davFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	userID, err := fs.userID(ctx)
	if err != nil {
		return nil, err
	}
	if name == "." {
		name = "/"
	}
	entry, err := fs.resolve(ctx, userID, name)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if flag&(os.O_CREATE|os.O_WRONLY|os.O_RDWR) != 0 {
			return fs.createUploadFile(ctx, userID, name, flag)
		}
		return nil, err
	}
	if entry.isDir {
		if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0 {
			return nil, errors.New("cannot write to directory")
		}
		return newDirFile(ctx, fs.store, userID, entry.dir.ID), nil
	}

	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE) != 0 {
		return fs.createUploadFile(ctx, userID, name, flag)
	}
	parts, err := fs.store.ListFileParts(ctx, entry.file.ID)
	if err != nil {
		return nil, err
	}
	return newReadFile(ctx, fs.tg, entry.file, parts), nil
}

func (fs *davFS) RemoveAll(ctx context.Context, name string) error {
	userID, err := fs.userID(ctx)
	if err != nil {
		return err
	}
	entry, err := fs.resolve(ctx, userID, name)
	if err != nil {
		return err
	}
	if entry.isDir {
		return fs.store.DeleteDirRecursive(ctx, userID, entry.dir.ID)
	}
	return fs.store.DeleteFile(ctx, userID, entry.file.ID)
}

func (fs *davFS) Rename(ctx context.Context, oldName, newName string) error {
	userID, err := fs.userID(ctx)
	if err != nil {
		return err
	}
	entry, err := fs.resolve(ctx, userID, oldName)
	if err != nil {
		return err
	}
	parentParts, base := splitPath(newName)
	if base == "" {
		return errors.New("invalid target name")
	}
	parentDir, err := fs.store.FindDirByPath(ctx, userID, parentParts)
	if err != nil {
		return err
	}
	if entry.isDir {
		if err := fs.store.MoveDir(ctx, userID, entry.dir.ID, parentDir.ID); err != nil {
			return err
		}
		return fs.store.RenameDir(ctx, userID, entry.dir.ID, base)
	}
	if err := fs.store.MoveFile(ctx, userID, entry.file.ID, parentDir.ID); err != nil {
		return err
	}
	return fs.store.RenameFile(ctx, userID, entry.file.ID, base)
}

func (fs *davFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	userID, err := fs.userID(ctx)
	if err != nil {
		return nil, err
	}
	entry, err := fs.resolve(ctx, userID, name)
	if err != nil {
		return nil, err
	}
	if entry.isDir {
		return dirInfo(entry.dir), nil
	}
	return fileInfo(entry.file), nil
}

func (fs *davFS) createUploadFile(ctx context.Context, userID int64, name string, flag int) (webdav.File, error) {
	if fs.storageChatID == 0 {
		return nil, errors.New("STORAGE_CHAT_ID is required for WebDAV uploads")
	}
	parentParts, base := splitPath(name)
	if base == "" {
		return nil, errors.New("invalid file name")
	}
	parentDir, err := fs.store.FindDirByPath(ctx, userID, parentParts)
	if err != nil {
		return nil, err
	}
	var existing *db.File
	if entry, err := fs.resolve(ctx, userID, name); err == nil && !entry.isDir {
		existing = &entry.file
	}
	contentLength, _ := ctx.Value(webdavContentLengthKey{}).(int64)
	return newUploadFile(ctx, fs.tg, fs.store, userID, fs.storageChatID, parentDir.ID, base, existing, fs.maxPartSize, contentLength), nil
}

type davEntry struct {
	isDir bool
	dir   db.Directory
	file  db.File
}

func (fs *davFS) resolve(ctx context.Context, userID int64, name string) (davEntry, error) {
	clean := path.Clean("/" + name)
	if clean == "/" {
		rootID, err := fs.store.GetRootDirID(ctx, userID)
		if err != nil {
			return davEntry{}, err
		}
		dir, err := fs.store.GetDirByID(ctx, userID, rootID)
		if err != nil {
			return davEntry{}, err
		}
		return davEntry{isDir: true, dir: dir}, nil
	}
	parts := strings.Split(strings.TrimPrefix(clean, "/"), "/")
	parentParts := parts[:len(parts)-1]
	base := parts[len(parts)-1]
	parentDir, err := fs.store.FindDirByPath(ctx, userID, parentParts)
	if err != nil {
		return davEntry{}, err
	}
	if dir, err := fs.store.GetDirByName(ctx, userID, parentDir.ID, base); err == nil {
		return davEntry{isDir: true, dir: dir}, nil
	}
	file, err := fs.store.GetFileByName(ctx, userID, parentDir.ID, base)
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
	name := dir.Name
	if !dir.ParentID.Valid {
		name = ""
	}
	return davFileInfo{name: name, size: 0, mode: os.ModeDir | 0o755, modTime: dir.UpdatedAt, isDir: true}
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
	if count <= 0 {
		if d.pos >= len(d.infos) {
			return nil, nil
		}
		count = len(d.infos) - d.pos
	} else if d.pos >= len(d.infos) {
		return nil, io.EOF
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
	ctx        context.Context
	tg         *telegram.Client
	file       db.File
	filePath   string
	parts      []db.FilePart
	partIndex  int
	partOffset int64
	partPaths  map[int]string
	offset     int64
	totalSize  int64
	reader     io.ReadCloser
	mu         sync.Mutex
}

func newReadFile(ctx context.Context, tg *telegram.Client, file db.File, parts []db.FilePart) *readFile {
	total := file.Size
	if total == 0 && len(parts) > 0 {
		for _, part := range parts {
			total += part.Size
		}
	}
	return &readFile{
		ctx:       ctx,
		tg:        tg,
		file:      file,
		parts:     parts,
		totalSize: total,
		partPaths: make(map[int]string),
	}
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

func (f *readFile) ensurePartPath(index int) (string, error) {
	if path, ok := f.partPaths[index]; ok {
		return path, nil
	}
	part := f.parts[index]
	info, err := f.tg.GetFile(f.ctx, part.TelegramFileID)
	if err != nil {
		return "", err
	}
	f.partPaths[index] = info.FilePath
	return info.FilePath, nil
}

func (f *readFile) ensureReader() error {
	if f.reader != nil {
		return nil
	}
	if len(f.parts) == 0 {
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
	if f.partIndex >= len(f.parts) {
		return io.EOF
	}
	path, err := f.ensurePartPath(f.partIndex)
	if err != nil {
		return err
	}
	reader, err := f.tg.DownloadFile(f.ctx, path, f.partOffset)
	if err != nil {
		return err
	}
	f.reader = reader
	return nil
}

func (f *readFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.parts) == 0 {
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
	for {
		if f.partIndex >= len(f.parts) {
			return 0, io.EOF
		}
		if err := f.ensureReader(); err != nil {
			if err == io.EOF {
				return 0, io.EOF
			}
			return 0, err
		}
		n, err := f.reader.Read(p)
		f.partOffset += int64(n)
		f.offset += int64(n)
		if err == io.EOF {
			_ = f.reader.Close()
			f.reader = nil
			if f.partOffset >= f.parts[f.partIndex].Size {
				f.partIndex++
				f.partOffset = 0
				if n > 0 {
					return n, nil
				}
				continue
			}
		}
		return n, err
	}
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
		if f.totalSize == 0 {
			f.totalSize = f.file.Size
		}
		newOffset = f.totalSize + offset
	default:
		return f.offset, errors.New("invalid seek")
	}
	if newOffset < 0 {
		return f.offset, errors.New("negative seek")
	}
	if f.totalSize > 0 && newOffset > f.totalSize {
		return f.offset, errors.New("seek beyond end")
	}
	f.offset = newOffset
	if len(f.parts) > 0 {
		f.partIndex, f.partOffset = locatePart(f.parts, newOffset)
	}
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

func locatePart(parts []db.FilePart, offset int64) (int, int64) {
	var total int64
	for i, part := range parts {
		if offset < total+part.Size {
			return i, offset - total
		}
		total += part.Size
	}
	return len(parts), 0
}

// uploadFile streams uploads into Telegram, splitting into parts when needed.
type uploadFile struct {
	ctx           context.Context
	tg            *telegram.Client
	store         *db.Store
	ownerID       int64
	storageChatID int64
	parentDirID   int64
	name          string
	existing      *db.File
	maxPartSize    int64
	splitFromStart bool
	partIndex      int
	totalSize      int64
	parts          []db.FilePartInput
	mimeType       string
	current        *uploadPart
	closed         bool
	aborted        bool
	abortErr       error
	doneCh         chan struct{}
	mu             sync.Mutex
}

type uploadPart struct {
	index int
	size  int64
	pipeW *io.PipeWriter
	done  chan uploadResult
}

type uploadResult struct {
	msg *telegram.Message
	err error
}

func newUploadFile(ctx context.Context, tg *telegram.Client, store *db.Store, ownerID, storageChatID, parentDirID int64, name string, existing *db.File, maxPartSize int64, contentLength int64) *uploadFile {
	if maxPartSize <= 0 {
		maxPartSize = 1900 * 1024 * 1024
	}
	splitFromStart := contentLength > maxPartSize
	f := &uploadFile{
		ctx:           ctx,
		tg:            tg,
		store:         store,
		ownerID:       ownerID,
		storageChatID: storageChatID,
		parentDirID:   parentDirID,
		name:          name,
		existing:      existing,
		maxPartSize:    maxPartSize,
		splitFromStart: splitFromStart,
		doneCh:         make(chan struct{}),
	}
	go f.watchContext()
	return f
}

func (f *uploadFile) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			return written, errors.New("upload already closed")
		}
		if err := f.ctx.Err(); err != nil {
			f.abortLocked(err)
		}
		if f.aborted {
			err := f.abortErr
			f.mu.Unlock()
			if err == nil {
				err = errors.New("upload canceled")
			}
			return written, err
		}
		if f.current == nil {
			if err := f.startPartLocked(); err != nil {
				f.mu.Unlock()
				return written, err
			}
		}
		remaining := f.maxPartSize - f.current.size
		if remaining <= 0 {
			f.mu.Unlock()
			if err := f.finishPart(); err != nil {
				return written, err
			}
			continue
		}
		toWrite := int64(len(p))
		if toWrite > remaining {
			toWrite = remaining
		}
		pipeW := f.current.pipeW
		f.mu.Unlock()

		n, err := pipeW.Write(p[:int(toWrite)])
		f.mu.Lock()
		if f.aborted {
			abortErr := f.abortErr
			f.mu.Unlock()
			if abortErr == nil {
				abortErr = errors.New("upload canceled")
			}
			return written + n, abortErr
		}
		if f.current != nil {
			f.current.size += int64(n)
		}
		f.totalSize += int64(n)
		f.mu.Unlock()
		written += n
		p = p[n:]
		if err != nil {
			return written, err
		}
		f.mu.Lock()
		needFinish := f.current != nil && f.current.size >= f.maxPartSize
		f.mu.Unlock()
		if needFinish {
			if err := f.finishPart(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (f *uploadFile) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	if f.aborted {
		err := f.abortErr
		close(f.doneCh)
		f.mu.Unlock()
		if err == nil {
			err = errors.New("upload canceled")
		}
		return err
	}
	f.mu.Unlock()

	if err := f.finishPart(); err != nil {
		f.mu.Lock()
		close(f.doneCh)
		f.mu.Unlock()
		return err
	}

	f.mu.Lock()
	parts := append([]db.FilePartInput(nil), f.parts...)
	totalSize := f.totalSize
	mimeType := f.mimeType
	name := f.name
	existing := f.existing
	close(f.doneCh)
	f.mu.Unlock()

	if len(parts) == 0 {
		return errors.New("empty upload")
	}
	first := parts[0]
	if existing != nil {
		return f.store.ReplaceFileWithParts(f.ctx, f.ownerID, existing.ID, name, first.TelegramFileID, first.FileUniqueID, totalSize, mimeType, parts)
	}
	if len(parts) > 1 {
		_, err := f.store.CreateFileWithParts(f.ctx, f.ownerID, f.parentDirID, name, first.TelegramFileID, first.FileUniqueID, totalSize, mimeType, parts)
		return err
	}
	_, err := f.store.CreateFile(f.ctx, f.ownerID, f.parentDirID, name, first.TelegramFileID, first.FileUniqueID, totalSize, mimeType)
	return err
}

func (f *uploadFile) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (f *uploadFile) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("seek not supported on upload")
}

func (f *uploadFile) Stat() (os.FileInfo, error) {
	return davFileInfo{name: f.name, size: f.totalSize, mode: 0o644, modTime: time.Now().UTC(), isDir: false}, nil
}

func (f *uploadFile) Readdir(count int) ([]os.FileInfo, error) {
	return nil, errors.New("not a directory")
}

func (f *uploadFile) startPartLocked() error {
	if f.aborted {
		return f.abortErr
	}
	if err := f.ctx.Err(); err != nil {
		f.abortLocked(err)
		return err
	}
	pr, pw := io.Pipe()
	partIndex := f.partIndex
	filename := f.partFilename(partIndex)
	done := make(chan uploadResult, 1)
	go func() {
		msg, err := f.tg.UploadDocument(f.ctx, f.storageChatID, filename, pr)
		done <- uploadResult{msg: msg, err: err}
	}()
	f.current = &uploadPart{
		index: partIndex,
		pipeW: pw,
		done:  done,
	}
	return nil
}

func (f *uploadFile) finishPart() error {
	f.mu.Lock()
	part := f.current
	f.mu.Unlock()
	if part == nil {
		return nil
	}
	_ = part.pipeW.Close()
	res := <-part.done
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.current == part {
		f.current = nil
	}
	if res.err != nil {
		f.abortLocked(res.err)
		return res.err
	}
	if res.msg == nil || res.msg.Document == nil {
		err := errors.New("telegram upload returned no document")
		f.abortLocked(err)
		return err
	}
	doc := res.msg.Document
	size := doc.FileSize
	if size == 0 {
		size = part.size
	}
	f.parts = append(f.parts, db.FilePartInput{
		PartIndex:      part.index,
		TelegramFileID: doc.FileID,
		FileUniqueID:   doc.FileUniqueID,
		Size:           size,
	})
	if f.mimeType == "" && doc.MimeType != "" {
		f.mimeType = doc.MimeType
	}
	f.partIndex++
	return nil
}

func (f *uploadFile) partFilename(index int) string {
	if index == 0 && !f.splitFromStart {
		return f.name
	}
	return fmt.Sprintf("%s.part%03d", f.name, index+1)
}

func (f *uploadFile) watchContext() {
	select {
	case <-f.ctx.Done():
		f.mu.Lock()
		if !f.closed {
			f.abortLocked(f.ctx.Err())
		}
		f.mu.Unlock()
	case <-f.doneCh:
	}
}

func (f *uploadFile) abortLocked(err error) {
	if f.aborted {
		return
	}
	f.aborted = true
	if err == nil {
		err = errors.New("upload canceled")
	}
	f.abortErr = err
	if f.current != nil {
		_ = f.current.pipeW.CloseWithError(err)
	}
}
