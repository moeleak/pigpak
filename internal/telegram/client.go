package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Client wraps Telegram Bot API calls.
type Client struct {
	Token  string
	APIURL string
	HTTP   *http.Client
}

// NewClient creates a Telegram client.
func NewClient(token, apiURL string) *Client {
	return &Client{
		Token:  token,
		APIURL: apiURL,
		HTTP: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Update represents a Telegram update.
type Update struct {
	UpdateID      int            `json:"update_id"`
	Message       *Message       `json:"message,omitempty"`
	CallbackQuery *CallbackQuery `json:"callback_query,omitempty"`
}

// Message is a Telegram message payload.
type Message struct {
	MessageID int     `json:"message_id"`
	From      *User   `json:"from,omitempty"`
	Chat      Chat    `json:"chat"`
	Date      int64   `json:"date"`
	Text      string  `json:"text,omitempty"`
	Caption   string  `json:"caption,omitempty"`
	Document  *Document `json:"document,omitempty"`
	Photo     []PhotoSize `json:"photo,omitempty"`
	Audio     *Audio  `json:"audio,omitempty"`
	Video     *Video  `json:"video,omitempty"`
}

// User is a Telegram user.
type User struct {
	ID        int64  `json:"id"`
	Username  string `json:"username,omitempty"`
	FirstName string `json:"first_name,omitempty"`
}

// Chat represents a chat.
type Chat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

// Document represents a document file.
type Document struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FileName     string `json:"file_name"`
	MimeType     string `json:"mime_type"`
	FileSize     int64  `json:"file_size"`
}

// PhotoSize represents a photo size.
type PhotoSize struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	Width        int    `json:"width"`
	Height       int    `json:"height"`
	FileSize     int64  `json:"file_size"`
}

// Audio represents an audio file.
type Audio struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FileName     string `json:"file_name"`
	MimeType     string `json:"mime_type"`
	FileSize     int64  `json:"file_size"`
	Duration     int    `json:"duration"`
}

// Video represents a video file.
type Video struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FileName     string `json:"file_name"`
	MimeType     string `json:"mime_type"`
	FileSize     int64  `json:"file_size"`
	Duration     int    `json:"duration"`
	Width        int    `json:"width"`
	Height       int    `json:"height"`
}

// CallbackQuery is an inline callback payload.
type CallbackQuery struct {
	ID      string   `json:"id"`
	From    *User    `json:"from,omitempty"`
	Message *Message `json:"message,omitempty"`
	Data    string   `json:"data"`
}

// File represents a Telegram file.
type File struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FileSize     int64  `json:"file_size"`
	FilePath     string `json:"file_path"`
}

type apiResponse[T any] struct {
	OK          bool   `json:"ok"`
	Result      T      `json:"result"`
	Description string `json:"description"`
	ErrorCode   int    `json:"error_code"`
}

func (c *Client) apiURL(method string) string {
	return fmt.Sprintf("%s/bot%s/%s", c.APIURL, c.Token, method)
}

func (c *Client) fileURL(filePath string) string {
	return fmt.Sprintf("%s/file/bot%s/%s", c.APIURL, c.Token, filePath)
}

func (c *Client) doJSON(ctx context.Context, method string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiURL(method), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("telegram api status: %s", resp.Status)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// GetUpdates polls for updates.
func (c *Client) GetUpdates(ctx context.Context, offset int, timeoutSec int) ([]Update, error) {
	payload := map[string]any{
		"offset":  offset,
		"timeout": timeoutSec,
		"allowed_updates": []string{"message", "callback_query"},
	}
	var resp apiResponse[[]Update]
	if err := c.doJSON(ctx, "getUpdates", payload, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("telegram getUpdates failed: %s", resp.Description)
	}
	return resp.Result, nil
}

// GetMe returns bot info.
func (c *Client) GetMe(ctx context.Context) (*User, error) {
	var resp apiResponse[User]
	if err := c.doJSON(ctx, "getMe", map[string]any{}, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("telegram getMe failed: %s", resp.Description)
	}
	return &resp.Result, nil
}

// SendMessage sends a text message.
func (c *Client) SendMessage(ctx context.Context, chatID int64, text string, markup *InlineKeyboardMarkup) (*Message, error) {
	payload := map[string]any{
		"chat_id": chatID,
		"text":    text,
	}
	if markup != nil {
		payload["reply_markup"] = markup
	}
	var resp apiResponse[Message]
	if err := c.doJSON(ctx, "sendMessage", payload, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("telegram sendMessage failed: %s", resp.Description)
	}
	return &resp.Result, nil
}

// EditMessageText edits a message.
func (c *Client) EditMessageText(ctx context.Context, chatID int64, messageID int, text string, markup *InlineKeyboardMarkup) (*Message, error) {
	payload := map[string]any{
		"chat_id":    chatID,
		"message_id": messageID,
		"text":       text,
	}
	if markup != nil {
		payload["reply_markup"] = markup
	}
	var resp apiResponse[Message]
	if err := c.doJSON(ctx, "editMessageText", payload, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("telegram editMessageText failed: %s", resp.Description)
	}
	return &resp.Result, nil
}

// AnswerCallbackQuery acknowledges callbacks.
func (c *Client) AnswerCallbackQuery(ctx context.Context, callbackID, text string) error {
	payload := map[string]any{
		"callback_query_id": callbackID,
	}
	if text != "" {
		payload["text"] = text
	}
	var resp apiResponse[bool]
	if err := c.doJSON(ctx, "answerCallbackQuery", payload, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("telegram answerCallbackQuery failed: %s", resp.Description)
	}
	return nil
}

// SendDocument sends a document by file_id.
func (c *Client) SendDocument(ctx context.Context, chatID int64, fileID, caption string, markup *InlineKeyboardMarkup) (*Message, error) {
	payload := map[string]any{
		"chat_id":  chatID,
		"document": fileID,
	}
	if caption != "" {
		payload["caption"] = caption
	}
	if markup != nil {
		payload["reply_markup"] = markup
	}
	var resp apiResponse[Message]
	if err := c.doJSON(ctx, "sendDocument", payload, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("telegram sendDocument failed: %s", resp.Description)
	}
	return &resp.Result, nil
}

// UploadDocument uploads a document to a chat.
func (c *Client) UploadDocument(ctx context.Context, chatID int64, filename string, reader io.Reader) (*Message, error) {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	resultCh := make(chan error, 1)
	go func() {
		defer pw.Close()
		if err := mw.WriteField("chat_id", strconv.FormatInt(chatID, 10)); err != nil {
			resultCh <- err
			return
		}
		part, err := mw.CreateFormFile("document", filename)
		if err != nil {
			resultCh <- err
			return
		}
		if _, err := io.Copy(part, reader); err != nil {
			resultCh <- err
			return
		}
		resultCh <- mw.Close()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiURL("sendDocument"), pr)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("telegram upload status: %s", resp.Status)
	}
	var apiResp apiResponse[Message]
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}
	if !apiResp.OK {
		return nil, fmt.Errorf("telegram upload failed: %s", apiResp.Description)
	}
	if err := <-resultCh; err != nil {
		return nil, err
	}
	return &apiResp.Result, nil
}

// GetFile retrieves file metadata.
func (c *Client) GetFile(ctx context.Context, fileID string) (*File, error) {
	payload := map[string]any{"file_id": fileID}
	var resp apiResponse[File]
	if err := c.doJSON(ctx, "getFile", payload, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("telegram getFile failed: %s", resp.Description)
	}
	return &resp.Result, nil
}

// DownloadFile opens a file stream from Telegram.
func (c *Client) DownloadFile(ctx context.Context, filePath string, offset int64) (io.ReadCloser, error) {
	fileURL := c.fileURL(filePath)
	reqURL, err := url.Parse(fileURL)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("telegram file download status: %s", resp.Status)
	}
	return resp.Body, nil
}
