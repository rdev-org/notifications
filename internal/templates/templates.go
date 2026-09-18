package templates

import "embed"

// FS embeds all HTML files in services/notifications/internal/templates/
//go:embed *.html
var FS embed.FS
