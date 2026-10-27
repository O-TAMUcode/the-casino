// THE CASINO オンライン対戦サーバー（依存パッケージなし / Node.js 18 以上）
// 使い方:  node server.js        （環境変数 PORT で番号を変更。既定 3000）
// 同じ場所（または1つ上）の index.html を配信し、/ws で対戦用 WebSocket を受け付けます。
'use strict';
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.env.PORT) || 3000;
const HTML = process.env.GAME_HTML || [path.join(__dirname, 'index.html'), path.join(__dirname, '..', 'index.html')].find(p => fs.existsSync(p)) || path.join(__dirname, 'index.html');
const MAX_FRAME = 64 * 1024;       // 1メッセージの上限
const MAX_SOCKETS = 2000;          // 同時接続の上限
const RATE_LIMIT = 80;             // 1秒あたりの最大メッセージ数
const ID_RE = /^[A-Za-z0-9_-]{1,64}$/;

const peers = new Map();           // peerId -> socket state
const conns = new Map();           // cid -> { a, b, active }
let sockCount = 0;

function baseUrl(req) {
  const host = String(req.headers['x-forwarded-host'] || req.headers.host || '').split(',')[0].trim();
  const proto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim() === 'http' || /^(localhost|127\.)/.test(host) ? 'http' : 'https';
  return host ? proto + '://' + host + '/' : '';
}
function page(req) {
  let h = fs.readFileSync(HTML, 'utf8');
  h = h.replace("const SERVER_URL='';", "const SERVER_URL='SAME';");
  const base = baseUrl(req);
  if (base) h = h.split('https://the-casino-1.onrender.com/').join(base);   /* 検索エンジンに伝える正しいURLを、いまのサイトのURLに合わせる */
  return h;
}
const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/index.html') {
    try {
      const body = page(req);
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(body);
    } catch (e) { res.writeHead(500); res.end('index.html が見つかりません: ' + HTML); }
  } else if (/^\/google[0-9a-f]{16}\.html$/.test(u)) {
    /* Google Search Console の「HTMLファイル」確認用：同じ場所に置いたファイルを、そのまま配信する */
    try {
      const body = fs.readFileSync(path.join(path.dirname(HTML), u.slice(1)));
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(body);
    } catch (e) { res.writeHead(404); res.end('not found'); }
  } else if (u === '/robots.txt') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('User-agent: *\nAllow: /\nSitemap: ' + baseUrl(req) + 'sitemap.xml\n');
  } else if (u === '/sitemap.xml') {
    res.writeHead(200, { 'Content-Type': 'application/xml; charset=utf-8' });
    res.end('<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>' + baseUrl(req) + '</loc></url></urlset>\n');
  } else if (u === '/healthz') { res.writeHead(200); res.end('ok'); }
  else { res.writeHead(404); res.end('not found'); }
});


/* ================= フレンド・チャット（アカウント） =================
   保存先: 環境変数 SUPABASE_URL / SUPABASE_KEY があれば Supabase、なければメモリ（再起動で消える） */
const SB_URL = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SB_KEY = process.env.SUPABASE_KEY || '';
const SECRET = process.env.SESSION_SECRET || SB_KEY || crypto.randomBytes(32).toString('hex');
const NAME_RE = /^[^\s<>"'&\/\\%?#,()*:;=\u0000-\u001f]{1,10}$/u;
const nameKey = (n) => n.normalize('NFKC').toLowerCase();

const mem = { users: [], friends: [], msgs: [], nid: 1 };
const MemStore = {
  async userByKey(k) { return mem.users.find(u => u.name_key === k) || null; },
  async userById(id) { return mem.users.find(u => u.id === id) || null; },
  async usersByIds(ids) { return mem.users.filter(u => ids.includes(u.id)); },
  async createUser(u) { if (mem.users.some(x => x.name_key === u.name_key)) return null; const r = { ...u, id: mem.nid++ }; mem.users.push(r); return r; },
  async friendRows(uid) { return mem.friends.filter(f => f.a === uid || f.b === uid).map(f => ({ ...f })); },
  async friendInsert(a, b) { mem.friends.push({ a, b, status: 'pending' }); },
  async friendAccept(a, b) { const f = mem.friends.find(f => f.a === a && f.b === b); if (f) f.status = 'accepted'; },
  async friendDelete(x, y) { mem.friends = mem.friends.filter(f => !((f.a === x && f.b === y) || (f.a === y && f.b === x))); },
  async setSave(id, t) { const u = mem.users.find(x => x.id === id); if (u) u.save = t; },
  async allUsers() { return mem.users.map(u => ({ id: u.id, name: u.name, created: u.created, save: u.save || null })); },
  async deleteAll() { mem.users = []; mem.friends = []; mem.msgs = []; },
  async deleteUser(id) { mem.users = mem.users.filter(u => u.id !== id); mem.friends = mem.friends.filter(f => f.a !== id && f.b !== id); mem.msgs = mem.msgs.filter(m => m.sender !== id && m.receiver !== id); },
  async msgInsert(s, r, t) { const m = { id: mem.nid++, sender: s, receiver: r, text: t, ts: new Date().toISOString() }; mem.msgs.push(m); return m; },
  async msgHist(x, y, n) { return mem.msgs.filter(m => (m.sender === x && m.receiver === y) || (m.sender === y && m.receiver === x)).slice(-n); },
};
async function sb(method, table, query, body, prefer) {
  const r = await fetch(SB_URL + '/rest/v1/' + table + (query || ''), {
    method,
    headers: Object.assign({ apikey: SB_KEY, 'Content-Type': 'application/json' }, SB_KEY.startsWith('sb_') ? {} : { Authorization: 'Bearer ' + SB_KEY }, prefer ? { Prefer: prefer } : {}),
    body: body ? JSON.stringify(body) : undefined,
  });
  const t = await r.text();
  if (!r.ok) { const e = new Error('supabase ' + r.status + ' ' + t.slice(0, 200)); e.status = r.status; throw e; }
  return t ? JSON.parse(t) : null;
}
const SbStore = {
  async userByKey(k) { const r = await sb('GET', 'users', '?name_key=eq.' + encodeURIComponent(k) + '&limit=1'); return r[0] || null; },
  async userById(id) { const r = await sb('GET', 'users', '?id=eq.' + id + '&limit=1'); return r[0] || null; },
  async usersByIds(ids) { if (!ids.length) return []; return sb('GET', 'users', '?id=in.(' + ids.join(',') + ')'); },
  async createUser(u) { try { const r = await sb('POST', 'users', '', u, 'return=representation'); return r[0]; } catch (e) { if (e.status === 409) return null; throw e; } },
  async friendRows(uid) { return sb('GET', 'friends', '?or=(a.eq.' + uid + ',b.eq.' + uid + ')'); },
  async friendInsert(a, b) { await sb('POST', 'friends', '', { a, b, status: 'pending' }); },
  async friendAccept(a, b) { await sb('PATCH', 'friends', '?a=eq.' + a + '&b=eq.' + b, { status: 'accepted' }); },
  async friendDelete(x, y) { await sb('DELETE', 'friends', '?or=(and(a.eq.' + x + ',b.eq.' + y + '),and(a.eq.' + y + ',b.eq.' + x + '))'); },
  async setSave(id, t) { await sb('PATCH', 'users', '?id=eq.' + id, { save: t }); },
  async allUsers() { return sb('GET', 'users', '?select=id,name,created,save&order=id.asc&limit=1000'); },
  async deleteAll() {
    await sb('DELETE', 'messages', '?id=gt.0');
    await sb('DELETE', 'friends', '?a=gt.0');
    await sb('DELETE', 'users', '?id=gt.0');
  },
  async deleteUser(id) {
    await sb('DELETE', 'messages', '?or=(sender.eq.' + id + ',receiver.eq.' + id + ')');
    await sb('DELETE', 'friends', '?or=(a.eq.' + id + ',b.eq.' + id + ')');
    await sb('DELETE', 'users', '?id=eq.' + id);
  },
  async msgInsert(s, r, t) { const q = await sb('POST', 'messages', '', { sender: s, receiver: r, text: t }, 'return=representation'); return q[0]; },
  async msgHist(x, y, n) {
    const q = await sb('GET', 'messages', '?or=(and(sender.eq.' + x + ',receiver.eq.' + y + '),and(sender.eq.' + y + ',receiver.eq.' + x + '))&order=id.desc&limit=' + n);
    return q.reverse();
  },
};
const store = SB_URL && SB_KEY ? SbStore : MemStore;
console.log(store === SbStore ? 'フレンド機能: Supabase に保存します' : 'フレンド機能: メモリに保存します（再起動すると消えます。SUPABASE_URL と SUPABASE_KEY を設定すると残ります）');

const scrypt = (p, s) => new Promise((res, rej) => crypto.scrypt(p, s, 32, (e, k) => e ? rej(e) : res(k)));
const sign = (uid, exp) => crypto.createHmac('sha256', SECRET).update(uid + '.' + exp).digest('base64url');
const makeToken = (uid) => { const exp = Date.now() + 30 * 24 * 3600 * 1000; return uid + '.' + exp + '.' + sign(uid, exp); };
function checkToken(t) {
  if (typeof t !== 'string') return null;
  const [uid, exp, sig] = t.split('.');
  if (!uid || !exp || !sig || Number(exp) < Date.now()) return null;
  const want = sign(uid, exp);
  if (sig.length !== want.length || !crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(want))) return null;
  return Number(uid);
}
const online = new Map();          // userId -> Set(st)
const fails = new Map();           // name_key -> { n, t }
const isOn = (id) => (online.get(id) || new Set()).size > 0;
const roomOf = (id) => { for (const s of (online.get(id) || [])) if (s.room) return s.room; return ''; };
const toUser = (id, o) => (online.get(id) || []).forEach(s => sendJson(s, o));

async function friendList(uid) {
  const rows = await store.friendRows(uid);
  const ids = rows.map(r => r.a === uid ? r.b : r.a);
  const us = await store.usersByIds(ids);
  return rows.map(r => {
    const id = r.a === uid ? r.b : r.a, u = us.find(x => x.id === id);
    return { id, name: u ? u.name : '?', st: r.status === 'accepted' ? 'friend' : (r.a === uid ? 'out' : 'in'), on: isOn(id), room: roomOf(id) };
  });
}
async function pushFriends(uid) { const l = await friendList(uid); toUser(uid, { t: 'friends', list: l }); }
async function announce(uid, on) {
  const l = await friendList(uid);
  const room = on ? roomOf(uid) : '';
  l.filter(f => f.st === 'friend').forEach(f => toUser(f.id, { t: 'pres', id: uid, on, room }));
}
async function areFriends(a, b) { const rows = await store.friendRows(a); return rows.some(r => r.status === 'accepted' && ((r.a === a && r.b === b) || (r.a === b && r.b === a))); }
async function loginOk(st, u) {
  st.uid = u.id; st.uname = u.name;
  if (!online.has(u.id)) online.set(u.id, new Set());
  online.get(u.id).add(st);
  sendJson(st, { t: 'auth', ok: true, token: makeToken(u.id), me: { id: u.id, name: u.name }, save: typeof u.save === 'string' ? u.save : null });
  sendJson(st, { t: 'friends', list: await friendList(u.id) });
  if (online.get(u.id).size === 1) announce(u.id, true).catch(() => {});
}
function socialGone(st) {
  if (!st.uid) return;
  const set = online.get(st.uid);
  if (set) { set.delete(st); if (!set.size) { online.delete(st.uid); announce(st.uid, false).catch(() => {}); } }
  st.uid = null;
}
async function social(st, m) {
  try { await socialInner(st, m); }
  catch (e) { console.error('social:', e.message); sendJson(st, { t: 'err', text: 'サーバーでエラーが起きました' }); }
}

/* ---- 管理者（環境変数 ADMIN_PASSWORD で守る） ---- */
const ADMIN_PASS = process.env.ADMIN_PASSWORD || '';
const admFail = { n: 0, t: 0 };
function adminOk(p) {
  if (!ADMIN_PASS || typeof p !== 'string') return false;
  const a = crypto.createHash('sha256').update(p).digest(), b = crypto.createHash('sha256').update(ADMIN_PASS).digest();
  return crypto.timingSafeEqual(a, b);
}
const SMAXB = 9999n * 10n ** 48n;
function parseSave(t) { try { const o = JSON.parse(t); return o && typeof o === 'object' ? o : {}; } catch (e) { return {}; } }
function ownOf(save) { const o = parseSave(save); return Array.isArray(o.o) ? o.o.filter(x => typeof x === 'string' && /^([a-z][0-9]{1,2}|\*)$/.test(x)).slice(0, 200) : null; }
function validOp(op) {
  if (!op || typeof op !== 'object') return null;
  if (op.k === 'bal') return (op.mode === 'add' || op.mode === 'set') && typeof op.v === 'string' && /^-?[0-9]{1,52}$/.test(op.v) ? { k: 'bal', mode: op.mode, v: op.v } : null;
  if (op.k === 'own') return typeof op.id === 'string' && /^[a-z][0-9]{1,2}$/.test(op.id) ? { k: 'own', id: op.id, on: !!op.on } : null;
  if (op.k === 'ownall') return { k: 'ownall', on: !!op.on };
  return null;
}
async function adminOp(id, op0) {
  const op = validOp(op0);
  if (!op) return;
  const u = await store.userById(id);
  if (!u) return;
  const o = parseSave(u.save);
  if (op.k === 'bal') {
    let b = 0n; try { b = BigInt(o.b || '0'); } catch (e) {}
    let nb = op.mode === 'set' ? BigInt(op.v) : b + BigInt(op.v);
    if (nb < 0n) nb = 0n; if (nb > SMAXB) nb = SMAXB;
    o.b = String(nb);
  } else if (op.k === 'own') {
    const set = new Set(Array.isArray(o.o) && o.o[0] !== '*' ? o.o : []);
    if (Array.isArray(o.o) && o.o[0] === '*') { /* 全部持っている状態から1つ外す場合は、表示用の一覧を持てないので * のまま */ if (op.on) { return; } }
    if (!(Array.isArray(o.o) && o.o[0] === '*')) { op.on ? set.add(op.id) : set.delete(op.id); o.o = [...set]; }
  } else if (op.k === 'ownall') {
    o.o = op.on ? ['*'] : ['d0', 'c0', 'm0', 't0', 's0'];
  }
  if (isOn(id)) toUser(id, { t: 'adjust', op });
  else o.adm = (Array.isArray(o.adm) ? o.adm : []).concat([op]).slice(-100);
  await store.setSave(id, JSON.stringify(o));
}
function balOf(save) { try { const o = JSON.parse(save); return /^[0-9]{1,60}$/.test(o.b || '') ? o.b : null; } catch (e) { return null; } }
async function adminList(st) {
  const us = await store.allUsers();
  sendJson(st, { t: 'adlist', online: online.size, users: us.map(u => ({ id: u.id, name: u.name, created: u.created, on: isOn(u.id), bal: balOf(u.save), own: ownOf(u.save) })) });
}
async function adminRemove(id) {
  const fl = await friendList(id);
  const sess = [...(online.get(id) || [])];
  await store.deleteUser(id);
  online.delete(id);
  sess.forEach(s2 => { s2.uid = null; sendJson(s2, { t: 'auth', ok: false, loggedOut: true, deleted: true }); });
  await Promise.all(fl.map(x => pushFriends(x.id)));
}
async function adminCmd(st, m) {
  if (m.t === 'adlist') return adminList(st);
  if (m.t === 'adop') { await adminOp(Number(m.id), m.op); return adminList(st); }
  if (m.t === 'addel') { await adminRemove(Number(m.id)); return adminList(st); }
  if (m.t === 'adreset') {
    const id = Number(m.id);
    await store.setSave(id, null);
    toUser(id, { t: 'reset' });
    return adminList(st);
  }
  if (m.t === 'adall') {
    if (m.text !== '全削除') return sendJson(st, { t: 'adm', ok: true, err: '確認の文字が違います' });
    const all = [...online.values()].flatMap(set => [...set]);
    await store.deleteAll();
    online.clear();
    all.forEach(s2 => { s2.uid = null; sendJson(s2, { t: 'auth', ok: false, loggedOut: true, deleted: true }); });
    return adminList(st);
  }
}
async function socialInner(st, m) {
  if (m.t === 'adm') {
    if (!ADMIN_PASS) return sendJson(st, { t: 'adm', ok: false, err: '管理者パスワードが未設定です（サーバーの環境変数 ADMIN_PASSWORD を設定してください）' });
    const now = Date.now();
    if (admFail.n >= 5 && now - admFail.t < 600000) return sendJson(st, { t: 'adm', ok: false, err: '失敗が続いたため、10分ほど待ってください' });
    if (adminOk(m.pass)) { admFail.n = 0; st.admin = true; return sendJson(st, { t: 'adm', ok: true }); }
    admFail.n = (now - admFail.t < 600000 ? admFail.n : 0) + 1; admFail.t = now;
    return sendJson(st, { t: 'adm', ok: false, err: 'パスワードが違います' });
  }
  if (m.t === 'adlist' || m.t === 'addel' || m.t === 'adreset' || m.t === 'adall' || m.t === 'adop') {
    if (!st.admin) return sendJson(st, { t: 'adm', ok: false, err: '管理者としてログインしていません' });
    return adminCmd(st, m);
  }
  if (m.t === 'register' || m.t === 'login') {
    const name = typeof m.name === 'string' ? m.name.trim() : '', pass = typeof m.pass === 'string' ? m.pass : '';
    if (!NAME_RE.test(name)) return sendJson(st, { t: 'auth', ok: false, err: '名前は10文字までで、記号（< > " \' & / \\ % ? # , ( ) * : ; =）と空白は使えません' });
    if (pass.length < 6 || pass.length > 64) return sendJson(st, { t: 'auth', ok: false, err: 'パスワードは6〜64文字にしてください' });
    const key = nameKey(name);
    const f = fails.get(key);
    if (f && f.n >= 8 && Date.now() - f.t < 600000) return sendJson(st, { t: 'auth', ok: false, err: '失敗が続いたため、10分ほど待ってからやり直してください' });
    if (m.t === 'register') {
      if ([...name].length > 10) return sendJson(st, { t: 'auth', ok: false, err: '名前は10文字までです' });
      if (++st.regs > 3) return sendJson(st, { t: 'auth', ok: false, err: '登録の回数が多すぎます' });
      const salt = crypto.randomBytes(16).toString('hex');
      const u = await store.createUser({ name, name_key: key, salt, hash: (await scrypt(pass, salt)).toString('hex') });
      if (!u) return sendJson(st, { t: 'auth', ok: false, err: 'その名前はすでに使われています' });
      return loginOk(st, u);
    }
    const u = await store.userByKey(key);
    const ok = u && crypto.timingSafeEqual(await scrypt(pass, u.salt), Buffer.from(u.hash, 'hex'));
    if (!ok) { fails.set(key, { n: (f && Date.now() - f.t < 600000 ? f.n : 0) + 1, t: Date.now() }); return sendJson(st, { t: 'auth', ok: false, err: '名前かパスワードが違います' }); }
    fails.delete(key);
    return loginOk(st, u);
  }
  if (m.t === 'check') {
    const name = typeof m.name === 'string' ? m.name.trim() : '';
    const now = Date.now();
    if (!st.chkAt || now - st.chkAt > 60000) { st.chkAt = now; st.chkN = 0; }
    if (++st.chkN > 40) return;
    if (!NAME_RE.test(name) || [...name].length > 10) return sendJson(st, { t: 'chk', name, r: 'bad' });
    const u = await store.userByKey(nameKey(name));
    return sendJson(st, { t: 'chk', name, r: u ? 'exists' : 'new' });
  }
  if (m.t === 'token') {
    const id = checkToken(m.token);
    const u = id ? await store.userById(id) : null;
    if (!u) return sendJson(st, { t: 'auth', ok: false, tokenBad: true });
    return loginOk(st, u);
  }
  if (!st.uid) return sendJson(st, { t: 'auth', ok: false });
  const me = st.uid;
  if (m.t === 'logout') { socialGone(st); return sendJson(st, { t: 'auth', ok: false, loggedOut: true }); }
  if (m.t === 'save') {
    if (typeof m.data !== 'string' || m.data.length > 3000) return;
    try { await store.setSave(me, m.data); }
    catch (e) { console.error('save:', e.message); if (!st.saveWarn) { st.saveWarn = true; sendJson(st, { t: 'savefail' }); } }
    return;
  }
  if (m.t === 'room') {
    const r = typeof m.room === 'string' && /^[A-Za-z0-9_-]{0,40}$/.test(m.room) ? m.room : '';
    if (st.room === r) return;
    st.room = r; announce(me, true).catch(() => {}); return;
  }
  if (m.t === 'invite') {
    const to = Number(m.to), r = typeof m.room === 'string' && /^[A-Za-z0-9_-]{1,40}$/.test(m.room) ? m.room : '';
    if (!r || !(await areFriends(me, to))) return;
    if (!isOn(to)) return sendJson(st, { t: 'notice', text: '相手はいまオフラインです' });
    toUser(to, { t: 'invite', from: me, name: st.uname, room: r });
    return;
  }
  if (m.t === 'friends') return sendJson(st, { t: 'friends', list: await friendList(me) });
  if (m.t === 'delacct') {
    const u = await store.userById(me);
    const key = u ? u.name_key : '';
    const f = fails.get(key);
    if (f && f.n >= 8 && Date.now() - f.t < 600000) return sendJson(st, { t: 'err', text: '失敗が続いたため、10分ほど待ってからやり直してください' });
    const ok = u && typeof m.pass === 'string' && crypto.timingSafeEqual(await scrypt(m.pass, u.salt), Buffer.from(u.hash, 'hex'));
    if (!ok) { fails.set(key, { n: (f && Date.now() - f.t < 600000 ? f.n : 0) + 1, t: Date.now() }); return sendJson(st, { t: 'err', text: 'パスワードが違います' }); }
    const fl = await friendList(me);
    const sess = [...(online.get(me) || [])];
    await store.deleteUser(me);
    online.delete(me);
    sess.forEach(s2 => { s2.uid = null; sendJson(s2, { t: 'auth', ok: false, loggedOut: true, deleted: true }); });
    await Promise.all(fl.map(x => pushFriends(x.id)));
    return;
  }
  if (m.t === 'freq') {
    const name = typeof m.name === 'string' ? m.name.trim() : '';
    const u = name ? await store.userByKey(nameKey(name)) : null;
    if (!u) return sendJson(st, { t: 'err', text: 'その名前のユーザーは見つかりません' });
    if (u.id === me) return sendJson(st, { t: 'err', text: '自分には申請できません' });
    const rows = await store.friendRows(me);
    const r = rows.find(r => (r.a === me && r.b === u.id) || (r.a === u.id && r.b === me));
    if (r && r.status === 'accepted') return sendJson(st, { t: 'err', text: 'すでにフレンドです' });
    if (r && r.a === me) return sendJson(st, { t: 'err', text: 'すでに申請中です' });
    if (r) { await store.friendAccept(u.id, me); sendJson(st, { t: 'notice', text: u.name + ' さんとフレンドになりました' }); toUser(u.id, { t: 'notice', text: st.uname + ' さんとフレンドになりました' }); }
    else { await store.friendInsert(me, u.id); sendJson(st, { t: 'notice', text: u.name + ' さんに申請しました' }); toUser(u.id, { t: 'notice', text: st.uname + ' さんからフレンド申請が届きました' }); }
    await pushFriends(me); await pushFriends(u.id); return;
  }
  const other = Number(m.id);
  if (m.t === 'facc') { await store.friendAccept(other, me); await pushFriends(me); await pushFriends(other); toUser(other, { t: 'notice', text: st.uname + ' さんがフレンド申請を承認しました' }); return; }
  if (m.t === 'fdel') { await store.friendDelete(me, other); await pushFriends(me); await pushFriends(other); return; }
  if (m.t === 'hist') {
    if (!(await areFriends(me, other))) return;
    const l = await store.msgHist(me, other, 50);
    return sendJson(st, { t: 'hist', id: other, list: l.map(x => ({ from: x.sender, text: x.text, ts: x.ts })) });
  }
  if (m.t === 'msg') {
    const text = typeof m.text === 'string' ? m.text.trim().slice(0, 300) : '';
    const to = Number(m.to);
    if (!text) return;
    if (!(await areFriends(me, to))) return sendJson(st, { t: 'err', text: 'フレンドにだけ送れます' });
    const x = await store.msgInsert(me, to, text);
    const o = { t: 'msg', from: me, to, text, ts: x.ts };
    toUser(me, o); toUser(to, o);
  }
}

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
  if (st.kind === 'social') socialGone(st);
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
  if (st.kind === 'social') return void social(st, m);
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
  if ((u !== '/ws' && u !== '/social') || !key || sockCount >= MAX_SOCKETS) { sock.end('HTTP/1.1 400 Bad Request\r\n\r\n'); return; }
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  sock.setNoDelay(true);
  const st = { sock, kind: u === '/social' ? 'social' : 'peer', regs: 0, uid: null, id: null, closed: false, buf: Buffer.alloc(0), frag: [], fragLen: 0, alive: Date.now(), win: Date.now(), cnt: 0 };
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
