package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const sessionCookie = "dispatch_session"
const sessionTTL = 30 * 24 * time.Hour

// sessionKey derives the cookie-signing key. A dedicated SessionSecret is
// used when set; otherwise the API token, so a single secret configures
// everything for a one-person deployment.
func (s *Server) sessionKey() []byte {
	if len(s.SessionSecret) > 0 {
		return s.SessionSecret
	}
	sum := sha256.Sum256([]byte("dispatch-session:" + s.Token))
	return sum[:]
}

func (s *Server) sign(exp int64) string {
	mac := hmac.New(sha256.New, s.sessionKey())
	mac.Write([]byte(strconv.FormatInt(exp, 10)))
	return strconv.FormatInt(exp, 10) + "." + hex.EncodeToString(mac.Sum(nil))
}

func (s *Server) validSession(v string) bool {
	i := strings.IndexByte(v, '.')
	if i < 0 {
		return false
	}
	exp, err := strconv.ParseInt(v[:i], 10, 64)
	if err != nil || time.Now().Unix() > exp {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(v), []byte(s.sign(exp))) == 1
}

// authenticated reports how the request is authorised, if at all.
func (s *Server) authenticated(r *http.Request) (string, bool) {
	if s.Token != "" && r.Header.Get("Authorization") == "Bearer "+s.Token {
		return "token", true
	}
	if c, err := r.Cookie(sessionCookie); err == nil && s.validSession(c.Value) {
		return "cookie", true
	}
	if s.Token == "" && s.Password == "" {
		return "open", true // local demo with neither configured
	}
	return "", false
}

func secure(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	if s.Password == "" {
		write(w, 503, map[string]string{"error": "dashboard login is not configured (set DASHBOARD_PASSWORD)"})
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		write(w, 400, map[string]string{"error": "invalid body"})
		return
	}
	if subtle.ConstantTimeCompare([]byte(req.Password), []byte(s.Password)) != 1 {
		time.Sleep(300 * time.Millisecond) // blunt but effective against casual guessing
		write(w, 401, map[string]string{"error": "wrong password"})
		return
	}
	exp := time.Now().Add(sessionTTL)
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: s.sign(exp.Unix()), Path: "/", Expires: exp, HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: secure(r)})
	write(w, 200, map[string]any{"authenticated": true, "expires": exp})
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: secure(r)})
	write(w, 200, map[string]any{"authenticated": false})
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	method, ok := s.authenticated(r)
	write(w, 200, map[string]any{"authenticated": ok, "method": method, "login_configured": s.Password != ""})
}
