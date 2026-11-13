package protocol

// ClientMessage is everything a client ever sends. A heartbeat is simply a
// ClientMessage with no operations: it still carries the digest, which is
// how registration drift is detected without extra traffic.
type ClientMessage struct {
	Token      *Token // nil means "please hand me a token" (handshake)
	Nonce      uint64 // echoed in token grants so stale grants are ignored
	Digest     Digest
	Register   []ObjectID
	Unregister []ObjectID
	FullSync   bool // Register holds the client's complete intended set
	Acks       []Notification
	Addr       string // which gateway holds this client's connection
}

// ServerMessage is everything a Registrar ever replies with.
type ServerMessage struct {
	NewToken      *Token // set on handshake or when the old token is stale
	Nonce         uint64
	Digest        Digest // registrar's view of the client's registrations
	RequestSync   bool   // digests disagree: resend the full set
	Registered    []ObjectID
	Unregistered  []ObjectID
	Notifications []Notification
}
