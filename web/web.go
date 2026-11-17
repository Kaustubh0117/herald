// Package web embeds the browser client and the demo page into the Go
// binary, so herald-dev serves them from wherever it is run.
package web

import "embed"

//go:embed herald-client.js demo
var FS embed.FS
