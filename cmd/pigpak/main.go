package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pigpak/internal/bot"
	"pigpak/internal/config"
	"pigpak/internal/db"
	"pigpak/internal/telegram"
	"pigpak/internal/webdav"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		log.Fatalf("data dir error: %v", err)
	}

	store, err := db.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("db open error: %v", err)
	}
	defer store.Close()

	tg := telegram.NewClient(cfg.BotToken, cfg.TelegramAPIURL)
	botRunner := bot.New(cfg, store, tg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if cfg.WebDAVEnable {
		srv, err := webdav.NewServer(cfg, store, tg)
		if err != nil {
			log.Fatalf("webdav error: %v", err)
		}
		go func() {
			log.Printf("webdav listening on %s", cfg.WebDAVAddr)
			if err := srv.ListenAndServe(); err != nil {
				log.Printf("webdav server stopped: %v", err)
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
		time.Sleep(1 * time.Second)
		os.Exit(0)
	}()

	if err := botRunner.Run(ctx); err != nil {
		log.Printf("bot stopped: %v", err)
	}
}
