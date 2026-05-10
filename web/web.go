// Package web embeds the HTML templates and static assets shipped with
// media-finder so the binary is self-contained at runtime.
package web

import "embed"

// TemplatesFS holds every HTML template under web/templates (including
// the partials/ subdirectory).
//
//go:embed templates
var TemplatesFS embed.FS

// StaticFS holds every file under web/static — CSS, JS, images, etc.
//
//go:embed static
var StaticFS embed.FS
