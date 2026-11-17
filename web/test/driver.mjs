// Runs herald-client.js in Node against a Herald server and prints one line
// per callback, so internal/gateway's Go test can script a scenario around it.
// Usage: node driver.mjs ws://host/ws products:1
import { HeraldClient } from "../herald-client.js";

const [url, ...objects] = process.argv.slice(2);
const mem = new Map();
const storage = { getItem: (k) => mem.get(k) ?? null, setItem: (k, v) => mem.set(k, v) };

const c = new HeraldClient(url, {
  notify: (obj, v) => console.log(`notify ${obj} ${v}`),
  notifyUnknown: (obj) => console.log(`unknown ${obj}`),
}, { storage, tickMs: 50 });
objects.forEach((o) => c.register(o));
c.start();
process.stdin.on("end", () => { c.stop(); process.exit(0); }).resume();
