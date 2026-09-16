// Package web serves the built dashboard. The Vite build writes into dist/,
// which is embedded into the binary so a deploy is one file. dist/ is
// git-ignored apart from .gitkeep, so a Go-only build still compiles and
// serves a short "not built" page instead.
package web

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed all:dist
var dist embed.FS

// Handler serves static files with an SPA fallback to index.html.
func Handler() http.Handler {
	sub, _ := fs.Sub(dist, "dist")
	files := http.FS(sub)
	fileServer := http.FileServer(files)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := path.Clean("/" + r.URL.Path)
		if f, err := files.Open(p); err == nil {
			if st, err := f.Stat(); err == nil && !st.IsDir() {
				f.Close()
				if strings.HasPrefix(p, "/assets/") {
					w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				}
				fileServer.ServeHTTP(w, r)
				return
			}
			f.Close()
		}
		if f, err := files.Open("/index.html"); err == nil {
			f.Close()
			w.Header().Set("Cache-Control", "no-cache")
			r.URL.Path = "/"
			fileServer.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(200)
		w.Write([]byte("<!doctype html><title>dispatch</title><p style=\"font-family:system-ui;padding:2rem\">API is up. The dashboard was not built into this binary: run <code>npm --prefix web run build</code> and rebuild.</p>"))
	})
}
