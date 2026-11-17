// Herald browser client: the same protocol and rules as internal/client.
// The app calls register()/unregister() and gets notify(obj, version) or
// notifyUnknown(obj); tokens, retries, digests and recovery stay in here.
export class HeraldClient {
  constructor(url, listener, { storage = globalThis.localStorage, tickMs = 15000, key = "herald-token" } = {}) {
    Object.assign(this, { url, listener, storage, tickMs, key });
    this.intents = new Set();   // the source of truth
    this.toReg = new Set();     // queued, not yet confirmed by a reply
    this.toUnreg = new Set();
    this.needFull = true;       // whatever the server has, our intents start empty
    this.acks = [];
    this.seen = new Map();      // highest version delivered per object
    this.nonce = 0;
    this.inflight = null;       // at most one request in flight per socket
    this.tok = JSON.parse(storage?.getItem(key) ?? "null");
  }

  register(id) {
    if (this.intents.has(id)) return;
    this.intents.add(id); this.toUnreg.delete(id); this.toReg.add(id);
    queueMicrotask(() => this.tick());
  }

  unregister(id) {
    if (!this.intents.has(id)) return;
    this.intents.delete(id); this.toReg.delete(id); this.toUnreg.add(id);
    queueMicrotask(() => this.tick());
  }

  start() {
    this.stopped = false;
    this.connect(250);
    this.timer = setInterval(() => this.tick(), this.tickMs); // heartbeat
  }

  stop() {
    this.stopped = true;
    clearInterval(this.timer);
    this.ws?.close();
  }

  connect(backoff) {
    const ws = new WebSocket(this.url);
    this.ws = ws;
    ws.onopen = () => { backoff = 250; this.tick(); };
    ws.onmessage = (ev) => {
      const f = JSON.parse(ev.data);
      if (f.Push) this.onPush(f.Push);
      if (f.Resp && this.inflight) {
        const sent = this.inflight;
        this.inflight = null;
        this.handle(sent, f.Resp);
        if (!this.tok || this.needFull || this.toReg.size || this.toUnreg.size) this.tick();
      }
    };
    ws.onclose = () => {
      this.inflight = null; // treat as lost: nothing was cleared, so it is all retried
      if (this.ws === ws && !this.stopped) setTimeout(() => this.connect(Math.min(backoff * 2, 10000)), backoff);
    };
  }

  async tick() {
    if (this.ws?.readyState !== WebSocket.OPEN || this.inflight) return;
    // Snapshot everything synchronously, so the digest matches the ops we send.
    const m = { Token: this.tok, Acks: [...this.acks] };
    this.inflight = m;
    const intents = [...this.intents];
    if (!this.tok) {
      m.Nonce = ++this.nonce;
    } else if (this.needFull) {
      m.FullSync = true;
      m.Register = intents;
    } else {
      m.Register = [...this.toReg];
      m.Unregister = [...this.toUnreg];
    }
    m.Digest = await digest(intents);
    if (this.inflight !== m || this.ws?.readyState !== WebSocket.OPEN) return;
    this.ws.send(JSON.stringify({ Msg: m }));
  }

  handle(sent, r) {
    if (r.NewToken) {
      if (!sent.Token && r.Nonce !== this.nonce) return; // grant for an older handshake
      this.tok = r.NewToken;
      this.needFull = true; // new server state: always reconcile
      this.storage?.setItem(this.key, JSON.stringify(this.tok));
      return;
    }
    // The server processed what we sent: clear exactly those items.
    if (sent.FullSync) {
      this.needFull = false;
      const had = new Set(sent.Register);
      this.toReg = new Set([...this.intents].filter((id) => !had.has(id)));
      this.toUnreg = new Set([...had].filter((id) => !this.intents.has(id)));
    } else {
      sent.Register.forEach((id) => this.toReg.delete(id));
      sent.Unregister.forEach((id) => this.toUnreg.delete(id));
    }
    this.acks = this.acks.slice(sent.Acks.length);
    // Registration Sync: the server applied exactly what we sent.
    if (r.RequestSync || !sameBytes(r.Digest, sent.Digest)) this.needFull = true;
    this.onPush(r.Notifications ?? []);
  }

  // Ack every notification for a registered object; deliver only versions
  // newer than any already delivered. Unknown-version signals always go out.
  onPush(ns) {
    for (const n of ns) {
      if (!this.intents.has(n.Object)) continue; // never ack what we can't deliver
      this.acks.push(n);
      if (!n.Unknown) {
        if (n.Version <= (this.seen.get(n.Object) ?? -Infinity)) continue;
        this.seen.set(n.Object, n.Version);
        this.listener.notify(n.Object, n.Version);
      } else {
        this.listener.notifyUnknown(n.Object);
      }
    }
  }
}

// XOR of SHA-256(objectId) over the set: must match protocol.Digest in Go,
// which encodes [32]byte as a JSON array of numbers.
async function digest(ids) {
  const d = new Uint8Array(32);
  for (const id of ids) {
    const h = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(id)));
    for (let i = 0; i < 32; i++) d[i] ^= h[i];
  }
  return Array.from(d);
}

function sameBytes(a, b) {
  return a.length === b.length && a.every((x, i) => x === b[i]);
}
