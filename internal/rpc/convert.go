// Package rpc runs Herald's servers as separate processes. Server adapters
// expose a Matcher, Registrar or Gateway over gRPC; client adapters
// implement the same interfaces the in-process Cell uses (MatcherAPI,
// RegistrarAPI, PushFunc), so server.Matcher and server.Registrar run
// unchanged whether their peers are in the same process or not.
package rpc

import (
	pb "github.com/Kaustubh0117/herald/gen/heraldv1"
	p "github.com/Kaustubh0117/herald/internal/protocol"
)

func tokenToPB(t *p.Token) *pb.Token {
	if t == nil {
		return nil
	}
	return &pb.Token{ClientId: string(t.Client), Cell: t.Cell, Epoch: t.Epoch}
}

func tokenFromPB(t *pb.Token) *p.Token {
	if t == nil {
		return nil
	}
	return &p.Token{Client: p.ClientID(t.ClientId), Cell: t.Cell, Epoch: t.Epoch}
}

func notifToPB(n p.Notification) *pb.Notification {
	return &pb.Notification{ObjectId: string(n.Object), Version: int64(n.Version), Unknown: n.Unknown, Seq: n.Seq,
		Stamps: stampsToPB(n.Stamps)}
}

func notifFromPB(n *pb.Notification) p.Notification {
	return p.Notification{Object: p.ObjectID(n.ObjectId), Version: p.Version(n.Version), Unknown: n.Unknown, Seq: n.Seq,
		Stamps: stampsFromPB(n.Stamps)}
}

// stampsToPB sends only the stages that were stamped.
func stampsToPB(s p.Stamps) []*pb.StageStamp {
	var out []*pb.StageStamp
	for st, at := range s {
		if at != 0 {
			out = append(out, &pb.StageStamp{Stage: p.Stage(st).String(), AtMicros: at})
		}
	}
	return out
}

func stampsFromPB(ss []*pb.StageStamp) (s p.Stamps) {
	for _, x := range ss {
		if st, ok := p.StageByName(x.Stage); ok {
			s[st] = x.AtMicros
		}
	}
	return s
}

func notifsToPB(ns []p.Notification) []*pb.Notification {
	out := make([]*pb.Notification, len(ns))
	for i, n := range ns {
		out[i] = notifToPB(n)
	}
	return out
}

func notifsFromPB(ns []*pb.Notification) []p.Notification {
	out := make([]p.Notification, len(ns))
	for i, n := range ns {
		out[i] = notifFromPB(n)
	}
	return out
}

func idsToPB(ids []p.ObjectID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}

func idsFromPB(ids []string) []p.ObjectID {
	out := make([]p.ObjectID, len(ids))
	for i, id := range ids {
		out[i] = p.ObjectID(id)
	}
	return out
}

// digestFromPB copies a 32-byte digest. A malformed one decodes as the
// empty digest, which simply triggers a registration sync.
func digestFromPB(b []byte) (d p.Digest) {
	if len(b) == len(d) {
		copy(d[:], b)
	}
	return d
}

func clientMessageToPB(m p.ClientMessage) *pb.ClientMessage {
	return &pb.ClientMessage{
		Token: tokenToPB(m.Token), Nonce: m.Nonce, Digest: m.Digest[:],
		Register: idsToPB(m.Register), Unregister: idsToPB(m.Unregister),
		FullSync: m.FullSync, Acks: notifsToPB(m.Acks), GatewayAddr: m.Addr,
	}
}

func clientMessageFromPB(m *pb.ClientMessage) p.ClientMessage {
	return p.ClientMessage{
		Token: tokenFromPB(m.Token), Nonce: m.Nonce, Digest: digestFromPB(m.Digest),
		Register: idsFromPB(m.Register), Unregister: idsFromPB(m.Unregister),
		FullSync: m.FullSync, Acks: notifsFromPB(m.Acks), Addr: m.GatewayAddr,
	}
}

func serverMessageToPB(m p.ServerMessage) *pb.ServerMessage {
	return &pb.ServerMessage{
		NewToken: tokenToPB(m.NewToken), Nonce: m.Nonce, Digest: m.Digest[:],
		RequestSync: m.RequestSync, Registered: idsToPB(m.Registered),
		Unregistered: idsToPB(m.Unregistered), Notifications: notifsToPB(m.Notifications),
	}
}

func serverMessageFromPB(m *pb.ServerMessage) p.ServerMessage {
	return p.ServerMessage{
		NewToken: tokenFromPB(m.NewToken), Nonce: m.Nonce, Digest: digestFromPB(m.Digest),
		RequestSync: m.RequestSync, Registered: idsFromPB(m.Registered),
		Unregistered: idsFromPB(m.Unregistered), Notifications: notifsFromPB(m.Notifications),
	}
}
