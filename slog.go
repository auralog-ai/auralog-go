package auralog

import (
	"context"
	"log/slog"
)

type groupedAttr struct {
	groups []string
	attr   slog.Attr
}

// SlogHandler is a slog.Handler that forwards records to Auralog.
//
// It is safe to share through slog.Logger values. Use NewSlogHandler rather
// than constructing it directly.
type SlogHandler struct {
	client *Client
	level  slog.Leveler
	attrs  []groupedAttr
	groups []string
}

// NewSlogHandler constructs a slog.Handler that forwards records to Auralog.
//
// The handler respects slog levels and WithGroup nesting. Handle does not read
// trace IDs from context; set client trace IDs explicitly with SetTraceID when
// your application enters a new request or operation scope.
func NewSlogHandler(client *Client, level slog.Leveler) *SlogHandler {
	if level == nil {
		level = slog.LevelInfo
	}
	return &SlogHandler{client: client, level: level}
}

// Enabled reports whether level is enabled for this handler.
func (h *SlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

// Handle converts a slog record into an Auralog entry.
func (h *SlogHandler) Handle(_ context.Context, record slog.Record) error {
	if h.client == nil {
		return nil
	}
	metadata := Metadata{
		"go_slog_level": record.Level.String(),
		"go_slog_time":  record.Time.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
	for _, attr := range h.attrs {
		addSlogAttr(metadata, attr.groups, attr.attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		addSlogAttr(metadata, h.groups, attr)
		return true
	})
	h.client.Log(levelFromSlog(record.Level), record.Message, metadata)
	return nil
}

// WithAttrs returns a handler with attrs attached to every future record.
func (h *SlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	clone.attrs = append([]groupedAttr(nil), h.attrs...)
	for _, attr := range attrs {
		clone.attrs = append(clone.attrs, groupedAttr{
			groups: append([]string(nil), h.groups...),
			attr:   attr,
		})
	}
	return &clone
}

// WithGroup returns a handler that nests subsequent attributes under name.
func (h *SlogHandler) WithGroup(name string) slog.Handler {
	clone := *h
	clone.groups = append(append([]string(nil), h.groups...), name)
	return &clone
}

func addSlogAttr(metadata Metadata, groups []string, attr slog.Attr) {
	if attr.Equal(slog.Attr{}) {
		return
	}
	current := metadata
	for _, group := range groups {
		if group == "" {
			continue
		}
		next, ok := current[group].(Metadata)
		if !ok {
			next = Metadata{}
			current[group] = next
		}
		current = next
	}
	current[attr.Key] = attr.Value.Any()
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
