package auralog

import (
	"context"
	"log/slog"
)

type SlogHandler struct {
	client *Client
	level  slog.Leveler
	attrs  []slog.Attr
	groups []string
}

func NewSlogHandler(client *Client, level slog.Leveler) *SlogHandler {
	if level == nil {
		level = slog.LevelInfo
	}
	return &SlogHandler{client: client, level: level}
}

func (h *SlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

func (h *SlogHandler) Handle(_ context.Context, record slog.Record) error {
	if h.client == nil {
		return nil
	}
	metadata := Metadata{
		"go_slog_level": record.Level.String(),
		"go_slog_time":  record.Time.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
	for _, attr := range h.attrs {
		metadata[attr.Key] = attr.Value.Any()
	}
	record.Attrs(func(attr slog.Attr) bool {
		metadata[attr.Key] = attr.Value.Any()
		return true
	})
	h.client.Log(levelFromSlog(record.Level), record.Message, metadata)
	return nil
}

func (h *SlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	clone.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &clone
}

func (h *SlogHandler) WithGroup(name string) slog.Handler {
	clone := *h
	clone.groups = append(append([]string(nil), h.groups...), name)
	return &clone
}

func levelFromSlog(level slog.Level) Level {
	switch {
	case level >= slog.LevelError:
		return LevelError
	case level >= slog.LevelWarn:
		return LevelWarn
	case level <= slog.LevelDebug:
		return LevelDebug
	default:
		return LevelInfo
	}
}
