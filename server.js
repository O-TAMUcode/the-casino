// THE CASINO オンライン対戦サーバー（依存パッケージなし / Node.js 18 以上）
// 使い方:  node server.js        （環境変数 PORT で番号を変更。既定 3000）
// 同じフォルダ階層の ../index.html を配信し、/ws で対戦用 WebSocket を受け付けます。
'use strict';
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.env.PORT) || 3000;
const HTML = process.env.GAME_HTML || path.join(__dirname, '..', 'index.html');
const MAX_FRAME = 64 * 1024;       // 1メッセージの上限
const MAX_SOCKETS = 2000;          // 同時接続の上限
const RATE_LIMIT = 80;             // 1秒あたりの最大メッセージ数
const ID_RE = /^[A-Za-z0-9_-]{1,64}$/;

const peers = new Map();           // peerId -> socket state
const conns = new Map();           // cid -> { a, b, active }
let sockCount = 0;

function page() {
  let h = fs.readFileSync(HTML, 'utf8');
  return h.replace("const SERVER_URL='';", "const SERVER_URL='SAME';");
}
const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/index.html') {
    try {
      const body = page();
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(body);
    } catch (e) { res.writeHead(500); res.end('index.html が見つかりません: ' + HTML); }
  } else if (u === '/healthz') { res.writeHead(200); res.end('ok'); }
  else { res.writeHead(404); res.end('not found'); }
});

/* ---- 最小限の WebSocket 実装（テキストのみ） ---- */
function frame(op, payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}
function sendJson(st, obj) {
  if (!st || st.closed) return;
  try { st.sock.write(frame(1, Buffer.from(JSON.stringify(obj)))); } catch (e) {}
}
function closeSock(st, code) {
  if (st.closed) return;
  try {
    const p = Buffer.alloc(2); p.writeUInt16BE(code || 1000);
    st.sock.write(frame(8, p));
  } catch (e) {}
  cleanup(st);
  try { st.sock.end(); } catch (e) {}
  setTimeout(() => { try { st.sock.destroy(); } catch (e) {} }, 500);
}
function cleanup(st) {
  if (st.closed) return;
  st.closed = true; sockCount--;
  if (st.id && peers.get(st.id) === st) peers.delete(st.id);
  for (const [cid, c] of conns) {
    if (c.a === st || c.b === st) {
      const other = c.a === st ? c.b : c.a;
      if (c.active) sendJson(other, { t: 'cl', from: st.id, cid });
      else if (c.a !== st) {} else sendJson(other, { t: 'cl', from: st.id, cid });
      conns.delete(cid);
    }
  }
}

function onMessage(st, m) {
  if (!m || typeof m !== 'object') return;
  if (m.t === 'reg') {
    if (st.id) return;
    let id = m.id;
    if (id !== undefined && id !== null) {
      if (typeof id !== 'string' || !ID_RE.test(id)) return sendJson(st, { t: 'err', type: 'invalid-id' });
      if (peers.has(id)) return sendJson(st, { t: 'err', type: 'unavailable-id' });
    } else {
      do { id = crypto.randomBytes(9).toString('base64url'); } while (peers.has(id));
    }
    st.id = id; peers.set(id, st);
    return sendJson(st, { t: 'open', id });
  }
  if (!st.id) return;
  const cid = typeof m.cid === 'string' && m.cid.length <= 40 ? m.cid : null;
  if (!cid) return;
  if (m.t === 'conn') {
    const to = peers.get(m.to);
    if (!to || to === st || conns.has(cid)) return sendJson(st, { t: 'cerr', cid, type: 'peer-unavailable' });
    conns.set(cid, { a: st, b: to, active: false });
    return sendJson(to, { t: 'conn', from: st.id, cid });
  }
  const c = conns.get(cid);
  if (!c || (c.a !== st && c.b !== st)) return;
  const other = c.a === st ? c.b : c.a;
  if (m.t === 'ok') { if (c.b === st && !c.active) { c.active = true; sendJson(other, { t: 'ok', from: st.id, cid }); } return; }
  if (!c.active) return;
  if (m.t === 'd') return sendJson(other, { t: 'd', from: st.id, cid, data: m.data });
  if (m.t === 'cl') { conns.delete(cid); return sendJson(other, { t: 'cl', from: st.id, cid }); }
}

server.on('upgrade', (req, sock) => {
  const u = req.url.split('?')[0];
  const key = req.headers['sec-websocket-key'];
  if (u !== '/ws' || !key || sockCount >= MAX_SOCKETS) { sock.end('HTTP/1.1 400 Bad Request\r\n\r\n'); return; }
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  sock.setNoDelay(true);
  const st = { sock, id: null, closed: false, buf: Buffer.alloc(0), frag: [], fragLen: 0, alive: Date.now(), win: Date.now(), cnt: 0 };
  sockCount++;
  sock.on('data', (d) => {
    st.buf = Buffer.concat([st.buf, d]);
    if (st.buf.length > MAX_FRAME * 2) return closeSock(st, 1009);
    for (;;) {
      const b = st.buf;
      if (b.length < 2) return;
      const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; const big = b.readBigUInt64BE(2); if (big > BigInt(MAX_FRAME)) return closeSock(st, 1009); len = Number(big); off = 10; }
      if (!masked) return closeSock(st, 1002);
      if (len > MAX_FRAME) return closeSock(st, 1009);
      if (b.length < off + 4 + len) return;
      const mask = b.slice(off, off + 4);
      const pl = Buffer.from(b.slice(off + 4, off + 4 + len));
      for (let i = 0; i < pl.length; i++) pl[i] ^= mask[i & 3];
      st.buf = b.slice(off + 4 + len);
      st.alive = Date.now();
      if (op === 8) return closeSock(st, 1000);
      if (op === 9) { try { sock.write(frame(10, pl)); } catch (e) {} continue; }
      if (op === 10) continue;
      if (op === 1 || op === 0) {
        st.frag.push(pl); st.fragLen += pl.length;
        if (st.fragLen > MAX_FRAME) return closeSock(st, 1009);
        if (!fin) continue;
        const txt = Buffer.concat(st.frag).toString('utf8'); st.frag = []; st.fragLen = 0;
        const now = Date.now();
        if (now - st.win > 1000) { st.win = now; st.cnt = 0; }
        if (++st.cnt > RATE_LIMIT) return closeSock(st, 1008);
        let m; try { m = JSON.parse(txt); } catch (e) { continue; }
        onMessage(st, m);
      }
    }
  });
  sock.on('close', () => cleanup(st));
  sock.on('error', () => cleanup(st));
  st.ping = setInterval(() => {
    if (st.closed) return clearInterval(st.ping);
    if (Date.now() - st.alive > 70000) { clearInterval(st.ping); return closeSock(st, 1001); }
    try { sock.write(frame(9, Buffer.alloc(0))); } catch (e) {}
  }, 25000);
  sock.on('close', () => clearInterval(st.ping));
});

server.listen(PORT, () => console.log('THE CASINO サーバー起動: http://localhost:' + PORT + '/  （対戦用 WebSocket は /ws）'));
