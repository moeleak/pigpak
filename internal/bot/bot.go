package bot

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"strings"
	"time"

	"pigpak/internal/config"
	"pigpak/internal/db"
	"pigpak/internal/telegram"
)

// Bot coordinates Telegram updates and storage.
type Bot struct {
	cfg         config.Config
	store       *db.Store
	tg          *telegram.Client
	botUsername string
}

// New creates a bot instance.
func New(cfg config.Config, store *db.Store, tg *telegram.Client) *Bot {
	return &Bot{cfg: cfg, store: store, tg: tg, botUsername: cfg.BotUsername}
}

// Run starts polling and handling updates.
func (b *Bot) Run(ctx context.Context) error {
	if b.botUsername == "" {
		if me, err := b.tg.GetMe(ctx); err == nil {
			b.botUsername = me.Username
		}
	}

	offset := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		updates, err := b.tg.GetUpdates(ctx, offset, int(b.cfg.PollTimeout.Seconds()))
		if err != nil {
			log.Printf("getUpdates error: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		for _, upd := range updates {
			offset = upd.UpdateID + 1
			if upd.Message != nil {
				b.handleMessage(ctx, upd.Message)
				continue
			}
			if upd.CallbackQuery != nil {
				b.handleCallback(ctx, upd.CallbackQuery)
			}
		}
	}
}

func (b *Bot) handleMessage(ctx context.Context, msg *telegram.Message) {
	if msg == nil || msg.From == nil {
		return
	}
	userID := msg.From.ID
	chatID := msg.Chat.ID

	if err := b.store.EnsureUserState(ctx, userID); err != nil {
		log.Printf("ensure user state: %v", err)
		return
	}

	if msg.Text != "" {
		if b.handleStart(ctx, userID, chatID, msg.Text) {
			return
		}
		if b.handlePendingText(ctx, userID, chatID, msg.Text) {
			return
		}
		if strings.HasPrefix(msg.Text, "/help") {
			b.sendHelp(ctx, chatID)
			return
		}
		b.sendDirectoryView(ctx, userID, chatID, 0, 0)
		return
	}

	if file := extractFile(msg); file != nil {
		b.handleUpload(ctx, userID, chatID, file)
		return
	}
}

func (b *Bot) handleStart(ctx context.Context, userID, chatID int64, text string) bool {
	if !strings.HasPrefix(text, "/start") {
		return false
	}
	parts := strings.Fields(text)
	if len(parts) > 1 {
		payload := parts[1]
		if strings.HasPrefix(payload, "share_") {
			token := strings.TrimPrefix(payload, "share_")
			b.handleSharePreview(ctx, userID, chatID, token)
			return true
		}
	}
	b.sendHelp(ctx, chatID)
	b.sendDirectoryView(ctx, userID, chatID, 0, 0)
	return true
}

func (b *Bot) handlePendingText(ctx context.Context, userID, chatID int64, text string) bool {
	state, err := b.store.GetUserState(ctx, userID)
	if err != nil {
		return false
	}
	if !state.PendingAction.Valid {
		return false
	}
	action := state.PendingAction.String
	switch action {
	case "mkdir":
		name := strings.TrimSpace(text)
		if name == "" || strings.Contains(name, "/") {
			b.sendText(ctx, chatID, "Folder name is invalid.")
			return true
		}
		parentID := state.PendingTarget.Int64
		if _, err := b.store.CreateDir(ctx, userID, parentID, name); err != nil {
			b.sendText(ctx, chatID, fmt.Sprintf("Create folder failed: %v", err))
			return true
		}
		_ = b.store.ClearPendingAction(ctx, userID)
		b.sendDirectoryView(ctx, userID, chatID, parentID, 0)
		return true
	case "rename_dir":
		name := strings.TrimSpace(text)
		if name == "" || strings.Contains(name, "/") {
			b.sendText(ctx, chatID, "Folder name is invalid.")
			return true
		}
		dirID := state.PendingTarget.Int64
		if err := b.store.RenameDir(ctx, userID, dirID, name); err != nil {
			b.sendText(ctx, chatID, fmt.Sprintf("Rename folder failed: %v", err))
			return true
		}
		_ = b.store.ClearPendingAction(ctx, userID)
		b.sendDirectoryView(ctx, userID, chatID, dirID, 0)
		return true
	case "rename_file":
		name := strings.TrimSpace(text)
		if name == "" || strings.Contains(name, "/") {
			b.sendText(ctx, chatID, "File name is invalid.")
			return true
		}
		fileID := state.PendingTarget.Int64
		file, err := b.store.GetFileByID(ctx, userID, fileID)
		if err != nil {
			b.sendText(ctx, chatID, fmt.Sprintf("File not found: %v", err))
			return true
		}
		if err := b.store.RenameFile(ctx, userID, fileID, name); err != nil {
			b.sendText(ctx, chatID, fmt.Sprintf("Rename file failed: %v", err))
			return true
		}
		_ = b.store.ClearPendingAction(ctx, userID)
		b.sendDirectoryView(ctx, userID, chatID, file.DirID, 0)
		return true
	default:
		return false
	}
}

func (b *Bot) sendHelp(ctx context.Context, chatID int64) {
	text := "Send files to upload. Use the buttons to browse folders, share files, and manage directories."
	_, _ = b.tg.SendMessage(ctx, chatID, text, nil)
}

func (b *Bot) sendText(ctx context.Context, chatID int64, text string) {
	_, _ = b.tg.SendMessage(ctx, chatID, text, nil)
}

func (b *Bot) handleUpload(ctx context.Context, userID, chatID int64, file *incomingFile) {
	dirID, err := b.store.GetCurrentDirID(ctx, userID)
	if err != nil {
		b.sendText(ctx, chatID, "Failed to locate current folder.")
		return
	}
	rec, err := b.store.CreateFile(ctx, userID, dirID, file.Name, file.FileID, file.FileUniqueID, file.Size, file.MimeType)
	if err != nil {
		b.sendText(ctx, chatID, fmt.Sprintf("Save file failed: %v", err))
		return
	}
	b.sendFileDetail(ctx, userID, chatID, rec, "")
}

func (b *Bot) handleSharePreview(ctx context.Context, userID, chatID int64, token string) {
	share, file, err := b.store.GetShareByToken(ctx, token)
	if err != nil {
		b.sendText(ctx, chatID, "Share not found.")
		return
	}
	if err := db.ValidateShare(share); err != nil {
		b.sendText(ctx, chatID, "Share link expired.")
		return
	}
	text := fmt.Sprintf("Shared file: %s\nSize: %s", file.Name, formatBytes(file.Size))
	markup := &telegram.InlineKeyboardMarkup{InlineKeyboard: [][]telegram.InlineKeyboardButton{
		{{Text: "Save to my drive", CallbackData: fmt.Sprintf("share_save:%s", token)}},
	}}
	_, _ = b.tg.SendMessage(ctx, chatID, text, markup)
}

func (b *Bot) handleCallback(ctx context.Context, cb *telegram.CallbackQuery) {
	if cb == nil || cb.From == nil {
		return
	}
	userID := cb.From.ID
	if err := b.store.EnsureUserState(ctx, userID); err != nil {
		log.Printf("ensure user state: %v", err)
	}
	if cb.Message == nil {
		_ = b.tg.AnswerCallbackQuery(ctx, cb.ID, "")
		return
	}
	chatID := cb.Message.Chat.ID
	msgID := cb.Message.MessageID
	data := cb.Data

	_ = b.tg.AnswerCallbackQuery(ctx, cb.ID, "")

	switch {
	case strings.HasPrefix(data, "nav:"):
		parts := strings.Split(data, ":")
		if len(parts) < 3 {
			return
		}
		dirID := parseInt64(parts[1])
		page := int(parseInt64(parts[2]))
		_ = b.store.SetCurrentDir(ctx, userID, dirID)
		b.editDirectoryView(ctx, userID, chatID, msgID, dirID, page)
	case strings.HasPrefix(data, "file:"):
		fileID := parseInt64(strings.TrimPrefix(data, "file:"))
		file, err := b.store.GetFileByID(ctx, userID, fileID)
		if err != nil {
			b.sendText(ctx, chatID, "File not found.")
			return
		}
		b.editFileDetail(ctx, userID, chatID, msgID, file, "")
	case strings.HasPrefix(data, "mkdir:"):
		dirID := parseInt64(strings.TrimPrefix(data, "mkdir:"))
		_ = b.store.SetPendingAction(ctx, userID, "mkdir", dirID, "")
		b.sendText(ctx, chatID, "Send folder name.")
	case strings.HasPrefix(data, "rndir:"):
		dirID := parseInt64(strings.TrimPrefix(data, "rndir:"))
		_ = b.store.SetPendingAction(ctx, userID, "rename_dir", dirID, "")
		b.sendText(ctx, chatID, "Send new folder name.")
	case strings.HasPrefix(data, "rnfile:"):
		fileID := parseInt64(strings.TrimPrefix(data, "rnfile:"))
		_ = b.store.SetPendingAction(ctx, userID, "rename_file", fileID, "")
		b.sendText(ctx, chatID, "Send new file name.")
	case strings.HasPrefix(data, "deldir:"):
		dirID := parseInt64(strings.TrimPrefix(data, "deldir:"))
		if err := b.store.DeleteDirRecursive(ctx, userID, dirID); err != nil {
			b.sendText(ctx, chatID, fmt.Sprintf("Delete folder failed: %v", err))
			return
		}
		rootID, _ := b.store.GetRootDirID(ctx, userID)
		b.editDirectoryView(ctx, userID, chatID, msgID, rootID, 0)
	case strings.HasPrefix(data, "delfile:"):
		fileID := parseInt64(strings.TrimPrefix(data, "delfile:"))
		file, err := b.store.GetFileByID(ctx, userID, fileID)
		if err != nil {
			b.sendText(ctx, chatID, "File not found.")
			return
		}
		if err := b.store.DeleteFile(ctx, userID, fileID); err != nil {
			b.sendText(ctx, chatID, fmt.Sprintf("Delete file failed: %v", err))
			return
		}
		b.editDirectoryView(ctx, userID, chatID, msgID, file.DirID, 0)
	case strings.HasPrefix(data, "sendfile:"):
		fileID := parseInt64(strings.TrimPrefix(data, "sendfile:"))
		file, err := b.store.GetFileByID(ctx, userID, fileID)
		if err != nil {
			b.sendText(ctx, chatID, "File not found.")
			return
		}
		_, _ = b.tg.SendDocument(ctx, chatID, file.FileID, file.Name, nil)
	case strings.HasPrefix(data, "share:"):
		parts := strings.Split(data, ":")
		if len(parts) != 3 {
			return
		}
		fileID := parseInt64(parts[1])
		days := parseInt64(parts[2])
		file, err := b.store.GetFileByID(ctx, userID, fileID)
		if err != nil {
			b.sendText(ctx, chatID, "File not found.")
			return
		}
		var expiresAt *time.Time
		if days > 0 {
			exp := time.Now().UTC().Add(time.Duration(days) * 24 * time.Hour)
			expiresAt = &exp
		}
		token := randomToken(16)
		share, err := b.store.CreateShare(ctx, file.ID, token, expiresAt)
		if err != nil {
			b.sendText(ctx, chatID, fmt.Sprintf("Share failed: %v", err))
			return
		}
		link := b.shareURL(share.Token)
		b.editFileDetail(ctx, userID, chatID, msgID, file, link)
	case strings.HasPrefix(data, "mvfile:"):
		fileID := parseInt64(strings.TrimPrefix(data, "mvfile:"))
		_ = b.store.SetPendingAction(ctx, userID, "move_file", fileID, "")
		rootID, _ := b.store.GetRootDirID(ctx, userID)
		b.editDirectoryPicker(ctx, userID, chatID, msgID, rootID)
	case strings.HasPrefix(data, "mvdir:"):
		dirID := parseInt64(strings.TrimPrefix(data, "mvdir:"))
		_ = b.store.SetPendingAction(ctx, userID, "move_dir", dirID, "")
		rootID, _ := b.store.GetRootDirID(ctx, userID)
		b.editDirectoryPicker(ctx, userID, chatID, msgID, rootID)
	case strings.HasPrefix(data, "pick:"):
		dirID := parseInt64(strings.TrimPrefix(data, "pick:"))
		b.editDirectoryPicker(ctx, userID, chatID, msgID, dirID)
	case strings.HasPrefix(data, "picksel:"):
		dirID := parseInt64(strings.TrimPrefix(data, "picksel:"))
		state, err := b.store.GetUserState(ctx, userID)
		if err != nil || !state.PendingAction.Valid {
			b.sendText(ctx, chatID, "No pending action.")
			return
		}
		switch state.PendingAction.String {
		case "move_file":
			fileID := state.PendingTarget.Int64
			if err := b.store.MoveFile(ctx, userID, fileID, dirID); err != nil {
				b.sendText(ctx, chatID, fmt.Sprintf("Move file failed: %v", err))
				return
			}
		case "move_dir":
			dirToMove := state.PendingTarget.Int64
			if err := b.store.MoveDir(ctx, userID, dirToMove, dirID); err != nil {
				b.sendText(ctx, chatID, fmt.Sprintf("Move folder failed: %v", err))
				return
			}
		default:
			b.sendText(ctx, chatID, "Unsupported action.")
			return
		}
		_ = b.store.ClearPendingAction(ctx, userID)
		b.editDirectoryView(ctx, userID, chatID, msgID, dirID, 0)
	case strings.HasPrefix(data, "share_save:"):
		token := strings.TrimPrefix(data, "share_save:")
		share, file, err := b.store.GetShareByToken(ctx, token)
		if err != nil {
			b.sendText(ctx, chatID, "Share not found.")
			return
		}
		if err := db.ValidateShare(share); err != nil {
			b.sendText(ctx, chatID, "Share expired.")
			return
		}
		currentDir, _ := b.store.GetCurrentDirID(ctx, userID)
		_, err = b.store.CreateFile(ctx, userID, currentDir, file.Name, file.FileID, file.FileUniqueID, file.Size, file.MimeType)
		if err != nil {
			b.sendText(ctx, chatID, fmt.Sprintf("Save failed: %v", err))
			return
		}
		_ = b.store.IncrementShareUses(ctx, share.ID)
		b.editDirectoryView(ctx, userID, chatID, msgID, currentDir, 0)
	default:
		return
	}
}

func (b *Bot) sendDirectoryView(ctx context.Context, userID, chatID int64, dirID int64, page int) {
	if dirID == 0 {
		dirID, _ = b.store.GetCurrentDirID(ctx, userID)
	}
	text, markup, err := b.directoryView(ctx, userID, dirID, page)
	if err != nil {
		b.sendText(ctx, chatID, fmt.Sprintf("Failed to load directory: %v", err))
		return
	}
	_, _ = b.tg.SendMessage(ctx, chatID, text, markup)
}

func (b *Bot) editDirectoryView(ctx context.Context, userID, chatID int64, msgID int, dirID int64, page int) {
	text, markup, err := b.directoryView(ctx, userID, dirID, page)
	if err != nil {
		b.sendText(ctx, chatID, fmt.Sprintf("Failed to load directory: %v", err))
		return
	}
	_, _ = b.tg.EditMessageText(ctx, chatID, msgID, text, markup)
}

func (b *Bot) sendFileDetail(ctx context.Context, userID, chatID int64, file db.File, link string) {
	text, markup := b.fileDetailView(file, link)
	_, _ = b.tg.SendMessage(ctx, chatID, text, markup)
}

func (b *Bot) editFileDetail(ctx context.Context, userID, chatID int64, msgID int, file db.File, link string) {
	text, markup := b.fileDetailView(file, link)
	_, _ = b.tg.EditMessageText(ctx, chatID, msgID, text, markup)
}

func (b *Bot) editDirectoryPicker(ctx context.Context, userID, chatID int64, msgID int, dirID int64) {
	text, markup, err := b.directoryPicker(ctx, userID, dirID)
	if err != nil {
		b.sendText(ctx, chatID, fmt.Sprintf("Failed to load picker: %v", err))
		return
	}
	_, _ = b.tg.EditMessageText(ctx, chatID, msgID, text, markup)
}

func (b *Bot) shareURL(token string) string {
	base := b.cfg.ShareBaseURL
	if base == "" && b.botUsername != "" {
		base = fmt.Sprintf("https://t.me/%s", b.botUsername)
	}
	if base == "" {
		return fmt.Sprintf("share_%s", token)
	}
	return fmt.Sprintf("%s?start=share_%s", base, token)
}

func parseInt64(value string) int64 {
	var out int64
	_, _ = fmt.Sscanf(value, "%d", &out)
	return out
}

func extractFile(msg *telegram.Message) *incomingFile {
	if msg.Document != nil {
		name := msg.Document.FileName
		if name == "" {
			name = fmt.Sprintf("file_%s", msg.Document.FileUniqueID)
		}
		return &incomingFile{
			Name:         name,
			FileID:       msg.Document.FileID,
			FileUniqueID: msg.Document.FileUniqueID,
			Size:         msg.Document.FileSize,
			MimeType:     msg.Document.MimeType,
		}
	}
	if msg.Audio != nil {
		name := msg.Audio.FileName
		if name == "" {
			name = fmt.Sprintf("audio_%s", msg.Audio.FileUniqueID)
		}
		return &incomingFile{
			Name:         name,
			FileID:       msg.Audio.FileID,
			FileUniqueID: msg.Audio.FileUniqueID,
			Size:         msg.Audio.FileSize,
			MimeType:     msg.Audio.MimeType,
		}
	}
	if msg.Video != nil {
		name := msg.Video.FileName
		if name == "" {
			name = fmt.Sprintf("video_%s", msg.Video.FileUniqueID)
		}
		return &incomingFile{
			Name:         name,
			FileID:       msg.Video.FileID,
			FileUniqueID: msg.Video.FileUniqueID,
			Size:         msg.Video.FileSize,
			MimeType:     msg.Video.MimeType,
		}
	}
	if len(msg.Photo) > 0 {
		photo := msg.Photo[len(msg.Photo)-1]
		return &incomingFile{
			Name:         fmt.Sprintf("photo_%s.jpg", photo.FileUniqueID),
			FileID:       photo.FileID,
			FileUniqueID: photo.FileUniqueID,
			Size:         photo.FileSize,
			MimeType:     "image/jpeg",
		}
	}
	return nil
}

type incomingFile struct {
	Name         string
	FileID       string
	FileUniqueID string
	Size         int64
	MimeType     string
}

func (b *Bot) directoryView(ctx context.Context, userID, dirID int64, page int) (string, *telegram.InlineKeyboardMarkup, error) {
	dir, err := b.store.GetDirByID(ctx, userID, dirID)
	if err != nil {
		return "", nil, err
	}
	pathText, err := b.store.GetDirPath(ctx, userID, dirID)
	if err != nil {
		return "", nil, err
	}
	dirs, err := b.store.ListDirs(ctx, userID, dirID)
	if err != nil {
		return "", nil, err
	}
	files, err := b.store.ListFiles(ctx, userID, dirID)
	if err != nil {
		return "", nil, err
	}

	entries := buildEntries(dirs, files)
	pageSize := b.cfg.PageSize
	if pageSize <= 0 {
		pageSize = 8
	}
	totalPages := (len(entries) + pageSize - 1) / pageSize
	if totalPages == 0 {
		totalPages = 1
	}
	if page < 0 || page >= totalPages {
		page = 0
	}
	start := page * pageSize
	end := start + pageSize
	if end > len(entries) {
		end = len(entries)
	}

	text := fmt.Sprintf("Folder: %s\nFolders: %d | Files: %d\nSend files in this chat to upload.", pathText, len(dirs), len(files))
	markup := buildDirectoryKeyboard(dir, entries[start:end], page, totalPages)
	return text, markup, nil
}

func (b *Bot) directoryPicker(ctx context.Context, userID, dirID int64) (string, *telegram.InlineKeyboardMarkup, error) {
	dir, err := b.store.GetDirByID(ctx, userID, dirID)
	if err != nil {
		return "", nil, err
	}
	pathText, err := b.store.GetDirPath(ctx, userID, dirID)
	if err != nil {
		return "", nil, err
	}
	dirs, err := b.store.ListDirs(ctx, userID, dirID)
	if err != nil {
		return "", nil, err
	}
	text := fmt.Sprintf("Select destination folder.\nCurrent: %s", pathText)
	markup := buildPickerKeyboard(dir, dirs)
	return text, markup, nil
}

func (b *Bot) fileDetailView(file db.File, link string) (string, *telegram.InlineKeyboardMarkup) {
	text := fmt.Sprintf("File: %s\nSize: %s\nType: %s\nCache ID: %s", file.Name, formatBytes(file.Size), file.MimeType, file.FileUniqueID)
	if link != "" {
		text += fmt.Sprintf("\nShare link: %s", link)
	}
	markup := buildFileKeyboard(file, link)
	return text, markup
}

func buildEntries(dirs []db.Directory, files []db.File) []entry {
	var entries []entry
	for _, d := range dirs {
		entries = append(entries, entry{
			Label: "[DIR] " + d.Name,
			Callback: fmt.Sprintf("nav:%d:0", d.ID),
		})
	}
	for _, f := range files {
		entries = append(entries, entry{
			Label: "[FILE] " + f.Name,
			Callback: fmt.Sprintf("file:%d", f.ID),
		})
	}
	return entries
}

func buildDirectoryKeyboard(dir db.Directory, entries []entry, page, totalPages int) *telegram.InlineKeyboardMarkup {
	var rows [][]telegram.InlineKeyboardButton
	for _, e := range entries {
		rows = append(rows, []telegram.InlineKeyboardButton{{Text: e.Label, CallbackData: e.Callback}})
	}
	if totalPages > 1 {
		row := []telegram.InlineKeyboardButton{}
		if page > 0 {
			row = append(row, telegram.InlineKeyboardButton{Text: "Prev", CallbackData: fmt.Sprintf("nav:%d:%d", dir.ID, page-1)})
		}
		if page < totalPages-1 {
			row = append(row, telegram.InlineKeyboardButton{Text: "Next", CallbackData: fmt.Sprintf("nav:%d:%d", dir.ID, page+1)})
		}
		rows = append(rows, row)
	}
	rows = append(rows, []telegram.InlineKeyboardButton{{Text: "New Folder", CallbackData: fmt.Sprintf("mkdir:%d", dir.ID)}})
	if dir.ParentID.Valid {
		rows = append(rows, []telegram.InlineKeyboardButton{{Text: "Rename Folder", CallbackData: fmt.Sprintf("rndir:%d", dir.ID)}})
		rows = append(rows, []telegram.InlineKeyboardButton{{Text: "Move Folder", CallbackData: fmt.Sprintf("mvdir:%d", dir.ID)}})
		rows = append(rows, []telegram.InlineKeyboardButton{{Text: "Delete Folder", CallbackData: fmt.Sprintf("deldir:%d", dir.ID)}})
		rows = append(rows, []telegram.InlineKeyboardButton{{Text: "Up", CallbackData: fmt.Sprintf("nav:%d:0", dir.ParentID.Int64)}})
	}
	return &telegram.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func buildPickerKeyboard(dir db.Directory, dirs []db.Directory) *telegram.InlineKeyboardMarkup {
	var rows [][]telegram.InlineKeyboardButton
	for _, d := range dirs {
		rows = append(rows, []telegram.InlineKeyboardButton{{Text: "[DIR] " + d.Name, CallbackData: fmt.Sprintf("pick:%d", d.ID)}})
	}
	rows = append(rows, []telegram.InlineKeyboardButton{{Text: "Select this folder", CallbackData: fmt.Sprintf("picksel:%d", dir.ID)}})
	if dir.ParentID.Valid {
		rows = append(rows, []telegram.InlineKeyboardButton{{Text: "Up", CallbackData: fmt.Sprintf("pick:%d", dir.ParentID.Int64)}})
	}
	rows = append(rows, []telegram.InlineKeyboardButton{{Text: "Cancel", CallbackData: fmt.Sprintf("nav:%d:0", dir.ID)}})
	return &telegram.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func buildFileKeyboard(file db.File, link string) *telegram.InlineKeyboardMarkup {
	rows := [][]telegram.InlineKeyboardButton{
		{{Text: "Send", CallbackData: fmt.Sprintf("sendfile:%d", file.ID)}, {Text: "Delete", CallbackData: fmt.Sprintf("delfile:%d", file.ID)}},
		{{Text: "Rename", CallbackData: fmt.Sprintf("rnfile:%d", file.ID)}, {Text: "Move", CallbackData: fmt.Sprintf("mvfile:%d", file.ID)}},
		{{Text: "Share 1d", CallbackData: fmt.Sprintf("share:%d:1", file.ID)}, {Text: "Share 3d", CallbackData: fmt.Sprintf("share:%d:3", file.ID)}},
		{{Text: "Share 7d", CallbackData: fmt.Sprintf("share:%d:7", file.ID)}, {Text: "Share 30d", CallbackData: fmt.Sprintf("share:%d:30", file.ID)}},
		{{Text: "Share forever", CallbackData: fmt.Sprintf("share:%d:0", file.ID)}},
		{{Text: "Back", CallbackData: fmt.Sprintf("nav:%d:0", file.DirID)}},
	}
	if link != "" {
		rows = append(rows, []telegram.InlineKeyboardButton{{Text: "Open share link", URL: link}})
	}
	return &telegram.InlineKeyboardMarkup{InlineKeyboard: rows}
}

type entry struct {
	Label    string
	Callback string
}

func formatBytes(size int64) string {
	if size < 1024 {
		return fmt.Sprintf("%d B", size)
	}
	units := []string{"KB", "MB", "GB", "TB"}
	value := float64(size)
	unit := "B"
	for _, u := range units {
		value = value / 1024
		unit = u
		if value < 1024 {
			break
		}
	}
	return fmt.Sprintf("%.1f %s", value, unit)
}

func randomToken(length int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		for i := range b {
			b[i] = alphabet[i%len(alphabet)]
		}
		return string(b)
	}
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}
