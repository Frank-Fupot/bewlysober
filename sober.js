// ==UserScript==
// @name         BFF Dashboard (Dynamics: head+tail continue + Fetch All NO time limit)
// @namespace    https://example.com/
// @version      1.2.1
// @description  首次抓关注动态前50页；点“更新”抓【新动态(头部增量)】+【旧动态(尾部续抓未抓页)】并持久化；“抓取全部动态”不设发布时间边界：只要还有页就一直抓到has_more=0（分块落盘，重启秒开）；筛选20min-4h；按时间排序；白色毛玻璃7列；视频页极简+音量50%；播完标记已看并上报历史
// @match        https://www.bilibili.com/*
// @grant        GM_addStyle
// @run-at       document-start
// ==/UserScript==

(() => {
  'use strict';

  /************ 配置 ************/
  const CFG = {
    MIN_SEC: 20 * 60,
    MAX_SEC: 4 * 60 * 60,

    // 首次只抓动态前 N 页
    INITIAL_MAX_PAGES: 50,

    // 点击“更新”：头部增量 / 尾部续抓 每次最多抓多少页
    UPDATE_HEAD_MAX_PAGES: 20,
    UPDATE_TAIL_MAX_PAGES: 20,

    // “抓取全部动态”：每块抓多少页（循环直到has_more=0）
    FETCH_ALL_CHUNK_PAGES: 50,

    // 抓取节奏：越大越稳（-412 风控就调大到 1200~2500）
    SLEEP_BETWEEN_REQ_MS: 900,
    RETRY: 6,

    // 关注列表（用于左侧UP名单）
    FOLLOW_PAGE_SIZE: 50,
    MAX_FOLLOW_PAGES: 60,

    FORCE_DASH_ON_NON_VIDEO: true,
    DASH_HASH: '#/bff',

    // 本地最多保留多少条合格视频（避免无限增长撑爆浏览器存储）
    MAX_STORE_VIDEOS: 20000,
  };

  const LS = {
    WATCHED: 'bff_watched_v1',
    LAST_SELECTED_UP: 'bff_last_selected_up_v10',
    LOCK: 'bff_lock_v9',
  };

  const DB = {
    name: 'bff_dyn_db',
    ver: 1,
    store: 'kv',
    KEY_INDEX: 'index',
    // index 结构：
    // {
    //   t,
    //   followings,
    //   videos,              // 合格视频(20m-4h)去重后的数组
    //   headNewestTs,        // 已缓存中最新 created
    //   tailOffset,          // 尾部续抓用 offset（上次抓到的“下一页offset”）
    //   tailHasMore          // 尾部是否还有更多
    // }
  };

  /**************** 路由与视频页极简 ****************/
  const isVideoPage = () => location.pathname.startsWith('/video/');
  const isDashboard = () => location.hash.startsWith(CFG.DASH_HASH);

  if (CFG.FORCE_DASH_ON_NON_VIDEO && !isVideoPage()) {
    if (!isDashboard()) location.hash = CFG.DASH_HASH;
  }

  if (isVideoPage()) {
    GM_addStyle(`
      html, body { margin:0 !important; padding:0 !important; background:#000 !important; }
      #bili-header-container, .bili-header, header, .international-header,
      .right-container, .recommend-list, #comment, .comment, .reply-box,
      .video-info-container, .video-toolbar-container, .up-panel-container,
      aside, .aside, .bili-footer, footer,
      .bpx-player-ad-wrap, .bpx-player-ending-wrap, .bpx-player-recommend,
      .bpx-player-sponsor, .bpx-player-promote {
        display:none !important; visibility:hidden !important;
      }
      #bilibili-player, .bpx-player-container, .bpx-player-primary-area {
        position: fixed !important; inset: 0 !important;
        width: 100vw !important; height: 100vh !important;
        z-index: 999999 !important; background:#000 !important;
      }
      body { overflow: hidden !important; }
    `);
  }

  /**************** Utils ****************/
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));

  function escapeHtml(s) {
    return String(s || '')
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#039;');
  }

  function getCookie(name) {
    const m = document.cookie.match(new RegExp('(?:^|;\\s*)' + name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '=([^;]*)'));
    return m ? decodeURIComponent(m[1]) : '';
  }

  function parseDurationToSec(d) {
    if (typeof d === 'number') return d;
    if (!d || typeof d !== 'string') return 0;
    const parts = d.split(':').map(x => parseInt(x, 10));
    if (parts.some(Number.isNaN)) return 0;
    if (parts.length === 2) return parts[0] * 60 + parts[1];
    if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
    return 0;
  }

  function fmtSec(sec) {
    sec = Math.max(0, Math.floor(sec));
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    if (h > 0) return `${h}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
    return `${m}:${String(s).padStart(2,'0')}`;
  }

  function durationInRange(sec) {
    return sec >= CFG.MIN_SEC && sec <= CFG.MAX_SEC;
  }

  function loadWatchedMap() {
    try { return JSON.parse(localStorage.getItem(LS.WATCHED) || '{}') || {}; }
    catch { return {}; }
  }
  function saveWatchedMap(map) { localStorage.setItem(LS.WATCHED, JSON.stringify(map)); }
  function markWatched(bvid) {
    const map = loadWatchedMap();
    map[bvid] = Date.now();
    saveWatchedMap(map);
  }

  function acquireLock(ttlMs = 30000) {
    const now = Date.now();
    try {
      const raw = localStorage.getItem(LS.LOCK);
      if (raw) {
        const lock = JSON.parse(raw);
        if (lock?.t && now - lock.t < ttlMs) return false;
      }
      localStorage.setItem(LS.LOCK, JSON.stringify({ t: now }));
      return true;
    } catch {
      localStorage.setItem(LS.LOCK, JSON.stringify({ t: now }));
      return true;
    }
  }
  function releaseLock() { try { localStorage.removeItem(LS.LOCK); } catch {} }

  /**************** IndexedDB KV ****************/
  function openDB() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(DB.name, DB.ver);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(DB.store)) {
          db.createObjectStore(DB.store, { keyPath: 'k' });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async function kvGet(key) {
    const db = await openDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(DB.store, 'readonly');
      const st = tx.objectStore(DB.store);
      const req = st.get(key);
      req.onsuccess = () => resolve(req.result ? req.result.v : null);
      req.onerror = () => reject(req.error);
    });
  }

  async function kvSet(key, value) {
    const db = await openDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(DB.store, 'readwrite');
      tx.objectStore(DB.store).put({ k: key, v: value });
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  /**************** B站 API ****************/
  async function apiGet(url) {
    const res = await fetch(url, { credentials: 'include' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const json = await res.json();
    if (typeof json?.code !== 'undefined' && json.code !== 0) {
      const e = new Error(json.message || json.msg || `API code=${json.code}`);
      e.apiCode = json.code;
      throw e;
    }
    return json.data;
  }

  async function apiPostForm(url, formObj) {
    const body = new URLSearchParams();
    Object.entries(formObj).forEach(([k,v]) => body.set(k, String(v)));
    const res = await fetch(url, {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
      body,
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const json = await res.json();
    if (typeof json?.code !== 'undefined' && json.code !== 0) {
      const e = new Error(json.message || json.msg || `API code=${json.code}`);
      e.apiCode = json.code;
      throw e;
    }
    return json.data;
  }

  async function withRetry(fn, retries = CFG.RETRY) {
    let last;
    for (let i = 0; i < retries; i++) {
      try { return await fn(); }
      catch (e) {
        last = e;
        const wait = (e?.apiCode === -412) ? (4000 + i * 4500) : (800 + i * 1100);
        await sleep(wait);
      }
    }
    throw last;
  }

  const apiNav = () => apiGet('https://api.bilibili.com/x/web-interface/nav');
  const apiViewByBvid = (bvid) => apiGet(`https://api.bilibili.com/x/web-interface/view?bvid=${encodeURIComponent(bvid)}`);
  const apiHistory = (pn=1, ps=300) => apiGet(`https://api.bilibili.com/x/v2/history?pn=${pn}&ps=${ps}`);

  async function apiReportHistory({ aid, cid, progress }) {
    const csrf = getCookie('bili_jct');
    return apiPostForm('https://api.bilibili.com/x/v2/history/report', {
      aid, cid, progress, platform: 'web', csrf
    });
  }

  async function apiFollowings(vmid, pn) {
    return apiGet(`https://api.bilibili.com/x/relation/followings?vmid=${vmid}&pn=${pn}&ps=${CFG.FOLLOW_PAGE_SIZE}&order=desc`);
  }

  async function loadAllFollowings() {
    const nav = await withRetry(() => apiNav());
    if (!nav?.mid) throw new Error('未登录或无法获取账号信息');

    const followings = [];
    let total = Infinity;

    for (let pn = 1; pn <= CFG.MAX_FOLLOW_PAGES; pn++) {
      const data = await withRetry(() => apiFollowings(nav.mid, pn));
      const list = data?.list || [];
      const t = Number(data?.total || 0);
      if (t > 0) total = t;

      list.forEach(x => followings.push({ mid: x.mid, uname: x.uname, face: x.face }));
      if (followings.length >= total) break;
      if (list.length < CFG.FOLLOW_PAGE_SIZE) break;

      await sleep(CFG.SLEEP_BETWEEN_REQ_MS);
    }
    return followings;
  }

  async function apiDynamicAll(offset = '') {
    const tz = new Date().getTimezoneOffset();
    const url = new URL('https://api.bilibili.com/x/polymer/web-dynamic/v1/feed/all');
    url.searchParams.set('type', 'all');
    url.searchParams.set('timezone_offset', String(tz));
    if (offset) url.searchParams.set('offset', offset);
    return apiGet(url.toString());
  }

  /**************** 动态解析：抽取视频动态 ****************/
  function parseDynamicVideoItem(it) {
    const author = it?.modules?.module_author || {};
    const dyn = it?.modules?.module_dynamic || {};
    const major = dyn?.major || {};
    const arc = major?.archive || major?.ugc || null;
    if (!arc) return null;

    const bvid = arc?.bvid || arc?.bvid_str || '';
    const title = arc?.title || '';
    const cover = arc?.cover || arc?.pic || '';
    const durationText = arc?.duration_text || arc?.duration || '';
    const sec = parseDurationToSec(durationText);

    const pubTs = Number(author?.pub_ts || 0) || 0;
    const upMid = Number(author?.mid || 0) || 0;
    const upName = author?.name || '';
    const upFace = author?.face || '';

    if (!bvid || !title || !cover || !sec) return null;

    return {
      bvid,
      title,
      pic: cover,
      durationSec: sec,
      lengthText: typeof durationText === 'string' ? durationText : fmtSec(sec),
      created: pubTs,
      upMid,
      upName,
      upFace,
    };
  }

  async function crawlDynamicsPages({
    startOffset = '',
    stopWhenOlderThanTs = 0,
    maxPages = Infinity,
    onProgress
  } = {}) {
    let offset = startOffset;
    let page = 0;
    const out = [];

    while (page < maxPages) {
      page++;
      onProgress?.(`动态抓取第 ${page} 页...`);

      const data = await withRetry(() => apiDynamicAll(offset));
      const items = data?.items || [];
      const hasMore = !!data?.has_more;
      const nextOffset = data?.offset || '';

      let pageMinTs = Infinity;

      for (const it of items) {
        const ts = Number(it?.modules?.module_author?.pub_ts || 0) || 0;
        if (ts > 0) pageMinTs = Math.min(pageMinTs, ts);

        const v = parseDynamicVideoItem(it);
        if (!v) continue;
        if (!durationInRange(v.durationSec)) continue; // 仍然筛 20min-4h
        out.push(v);
      }

      // 仅用于“头部增量”优化；抓取全部时不会传 stopWhenOlderThanTs
      if (stopWhenOlderThanTs && pageMinTs !== Infinity && pageMinTs <= stopWhenOlderThanTs) {
        offset = nextOffset || offset;
        return { videos: out, endOffset: offset, hasMore, pageCount: page };
      }

      if (!hasMore || !nextOffset) {
        offset = nextOffset || offset;
        return { videos: out, endOffset: offset, hasMore, pageCount: page };
      }

      offset = nextOffset;
      await sleep(CFG.SLEEP_BETWEEN_REQ_MS);
    }

    return { videos: out, endOffset: offset, hasMore: true, pageCount: page };
  }

  function mergeVideos(existing, incoming) {
    const m = new Map();
    for (const v of (existing || [])) m.set(v.bvid, v);
    for (const v of (incoming || [])) if (!m.has(v.bvid)) m.set(v.bvid, v);

    const arr = Array.from(m.values());
    arr.sort((a,b) => (b.created||0) - (a.created||0));
    if (arr.length > CFG.MAX_STORE_VIDEOS) return arr.slice(0, CFG.MAX_STORE_VIDEOS);
    return arr;
  }

  /**************** Watched 判定 ****************/
  async function loadRecentHistoryMap() {
    try {
      const list = await apiHistory(1, 300);
      const map = new Map();
      (list || []).forEach(it => { if (it.bvid) map.set(it.bvid, it); });
      return map;
    } catch {
      return new Map();
    }
  }

  function isWatched(v, watchedMap, historyMap) {
    if (watchedMap[v.bvid]) return true;
    const h = historyMap.get(v.bvid);
    if (!h) return false;
    if (h.progress === -1) return true;
    const dur = Number(h.duration || v.durationSec || 0);
    const prog = Number(h.progress || 0);
    return dur > 0 && prog >= dur - 5;
  }

  /**************** Search ****************/
  function parseQuery(q) {
    const tokens = (q || '').trim().split(/\s+/).filter(Boolean);
    const rule = { any: [], up: [], title: [], bv: [], mid: [] };
    for (const t of tokens) {
      const m = t.match(/^(\w+):(.*)$/);
      if (m) {
        const k = m[1].toLowerCase();
        const v = (m[2] || '').toLowerCase();
        if (k in rule) rule[k].push(v);
        else rule.any.push(t.toLowerCase());
      } else rule.any.push(t.toLowerCase());
    }
    return rule;
  }
  function matchVideo(v, rule) {
    const up = (v.upName || '').toLowerCase();
    const title = (v.title || '').toLowerCase();
    const bv = (v.bvid || '').toLowerCase();
    const mid = String(v.upMid || '');
    for (const x of rule.up) if (!up.includes(x)) return false;
    for (const x of rule.title) if (!title.includes(x)) return false;
    for (const x of rule.bv) if (!bv.includes(x)) return false;
    for (const x of rule.mid) if (!mid.includes(x)) return false;
    for (const x of rule.any) {
      if (!(title.includes(x) || up.includes(x) || bv.includes(x) || mid.includes(x))) return false;
    }
    return true;
  }

  /**************** UI ****************/
  function mountDashboardShell() {
    const root = document.createElement('div');
    root.id = 'bff-app';
    root.innerHTML = `
      <div class="bff-bg"></div>

      <div class="bff-topbar">
        <div class="bff-title">Follow Filter</div>
        <div class="bff-sub">抓取全部动态：不设发布时间边界（有页就抓）· 最终按时间排序</div>
        <div class="bff-actions">
          <input class="bff-search" placeholder="搜索：关键字 / up:xxx title:xxx bv:BV... mid:123" />
          <button class="bff-btn" data-act="fetchall">抓取全部动态</button>
          <button class="bff-btn bff-btn-primary" data-act="update">更新</button>
        </div>
      </div>

      <div class="bff-grid">
        <aside class="bff-panel bff-left">
          <div class="bff-panel-title">关注UP</div>
          <div class="bff-up-list"></div>
        </aside>

        <main class="bff-panel bff-mid">
          <div class="bff-panel-title">
            <span class="bff-mid-title">未看</span>
            <span class="bff-mid-meta" id="bff-status"></span>
          </div>
          <div class="bff-video-grid"></div>
        </main>

        <aside class="bff-panel bff-right">
          <div class="bff-panel-title">已看完</div>
          <div class="bff-watched-list"></div>
        </aside>
      </div>

      <div class="bff-mask" style="display:none">
        <div class="bff-mask-card">
          <div class="bff-mask-title">正在抓取动态...</div>
          <div class="bff-mask-text" id="bff-mask-text"></div>
          <div class="bff-mask-text2">抓取结果分块落盘保存；中断/重启后仍然秒开已有内容。</div>
        </div>
      </div>

      <div class="bff-toast" style="display:none"></div>
    `;
    document.documentElement.appendChild(root);

    GM_addStyle(`
      :root{
        --bff-text: rgba(18, 22, 32, .92);
        --bff-text2: rgba(18, 22, 32, .55);
        --bff-border: rgba(0,0,0,.06);
        --bff-glass: rgba(255,255,255,.62);
        --bff-radius: 16px;
      }
      body > *:not(#bff-app) { display:none !important; }
      body{
        background:
          radial-gradient(1200px 800px at 18% 22%, rgba(120, 190, 255, .28) 0%, transparent 55%),
          radial-gradient(1000px 700px at 85% 75%, rgba(255, 255, 255, .75) 0%, transparent 60%),
          linear-gradient(180deg, #f6f9ff, #eef2f9) !important;
      }
      #bff-app{ position: fixed; inset:0; z-index: 999999; color: var(--bff-text);
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, "PingFang SC", "Microsoft YaHei", sans-serif;
      }
      #bff-app .bff-bg{
        position:absolute; inset:0;
        background: rgba(255,255,255,.12);
        backdrop-filter: blur(18px);
      }
      .bff-topbar{
        position: relative;
        display:flex; align-items:center; gap:14px;
        padding: 14px 18px;
        border-bottom: 1px solid var(--bff-border);
        background: var(--bff-glass);
        backdrop-filter: blur(18px);
      }
      .bff-title{ font-weight: 900; letter-spacing:.2px; }
      .bff-sub{ color: var(--bff-text2); font-size: 12px; }
      .bff-actions{ margin-left:auto; display:flex; align-items:center; gap:10px; }
      .bff-search{
        width: 420px; max-width: 45vw;
        padding: 9px 10px;
        border-radius: 12px;
        border: 1px solid var(--bff-border);
        background: rgba(255,255,255,.78);
        color: var(--bff-text);
        outline: none;
      }
      .bff-btn{
        padding: 9px 12px;
        border-radius: 12px;
        border: 1px solid var(--bff-border);
        background: rgba(255,255,255,.78);
        color: var(--bff-text);
        cursor:pointer;
      }
      .bff-btn-primary{
        background: rgba(79,155,255,.14);
        border-color: rgba(79,155,255,.28);
      }

      .bff-grid{
        position: relative;
        height: calc(100vh - 58px);
        display: grid;
        grid-template-columns: 220px repeat(5, 1fr) 280px;
        gap: 12px;
        padding: 12px;
        box-sizing: border-box;
      }
      .bff-panel{
        border: 1px solid var(--bff-border);
        background: var(--bff-glass);
        border-radius: var(--bff-radius);
        backdrop-filter: blur(18px);
        overflow: hidden;
        min-height: 0;
        box-shadow: 0 10px 30px rgba(18, 28, 45, .08);
      }
      .bff-left{ grid-column: 1 / span 1; display:flex; flex-direction:column; }
      .bff-mid{ grid-column: 2 / span 5; display:flex; flex-direction:column; }
      .bff-right{ grid-column: 7 / span 1; display:flex; flex-direction:column; }

      .bff-panel-title{
        padding: 10px 12px;
        border-bottom: 1px solid var(--bff-border);
        display:flex; align-items:center; justify-content:space-between;
        color: var(--bff-text2);
        font-size: 12px;
      }
      .bff-mid-title{ color: var(--bff-text); font-weight: 900; font-size: 13px; }

      .bff-up-list, .bff-watched-list{ padding: 10px; overflow: auto; }
      .bff-up-item{
        display:flex; align-items:center; gap:10px;
        padding: 8px 10px;
        border-radius: 12px;
        cursor:pointer;
        border: 1px solid transparent;
      }
      .bff-up-item:hover{ background: rgba(255,255,255,.65); border-color: rgba(0,0,0,.06); }
      .bff-up-item.active{ border-color: rgba(79,155,255,.38); background: rgba(79,155,255,.10); }
      .bff-up-face{ width: 26px; height: 26px; border-radius: 50%; background: rgba(0,0,0,.05); object-fit: cover; }
      .bff-up-name{ font-size: 13px; color: var(--bff-text); overflow:hidden; white-space:nowrap; text-overflow:ellipsis; }

      .bff-video-grid{
        padding: 10px;
        overflow: auto;
        display:grid;
        grid-template-columns: repeat(5, minmax(0, 1fr));
        gap: 10px;
      }
      .bff-card{
        border: 1px solid var(--bff-border);
        background: rgba(255,255,255,.62);
        border-radius: 14px;
        overflow:hidden;
        cursor:pointer;
        min-height: 210px;
        display:flex; flex-direction:column;
      }
      .bff-cover{
        height: 122px;
        background-size: cover;
        background-position: center;
        position:relative;
      }
      .bff-dur{
        position:absolute; right:8px; bottom:8px;
        font-size: 12px;
        background: rgba(0,0,0,.55);
        color: rgba(255,255,255,.95);
        padding: 3px 7px;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,.18);
      }
      .bff-body{ padding: 10px; display:flex; flex-direction:column; gap:6px; }
      .bff-title2{
        font-size: 13px; line-height: 1.25;
        display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical;
        overflow:hidden;
      }
      .bff-meta{ color: var(--bff-text2); font-size: 12px; display:flex; gap:8px; flex-wrap:wrap; }

      .bff-w-item{
        border: 1px solid var(--bff-border);
        background: rgba(255,255,255,.62);
        border-radius: 12px;
        padding: 8px 10px;
        margin-bottom: 8px;
        cursor:pointer;
      }

      .bff-mask{
        position: fixed; inset: 0; z-index: 1000001;
        background: rgba(255,255,255,.30);
        backdrop-filter: blur(12px);
        display:flex; align-items:center; justify-content:center;
      }
      .bff-mask-card{
        width: min(720px, 92vw);
        border: 1px solid var(--bff-border);
        background: rgba(255,255,255,.75);
        border-radius: 18px;
        padding: 16px 18px;
        box-shadow: 0 18px 60px rgba(18, 28, 45, .12);
      }
      .bff-mask-title{ font-weight: 900; margin-bottom: 8px; }
      .bff-mask-text{ font-size: 13px; margin-bottom: 8px; }
      .bff-mask-text2{ color: var(--bff-text2); font-size: 12px; }

      .bff-toast{
        position: fixed; left: 50%; bottom: 18px;
        transform: translateX(-50%);
        padding: 10px 12px;
        border-radius: 999px;
        border: 1px solid var(--bff-border);
        background: rgba(255,255,255,.85);
        backdrop-filter: blur(18px);
        color: var(--bff-text);
        font-size: 12px;
        z-index: 1000002;
      }
    `);

    return root;
  }

  function toast(msg, ms = 1600) {
    const el = document.querySelector('#bff-app .bff-toast');
    if (!el) return;
    el.textContent = msg;
    el.style.display = 'block';
    clearTimeout(toast._t);
    toast._t = setTimeout(() => { el.style.display = 'none'; }, ms);
  }

  function openVideoSameTab(bvid) {
    location.href = `https://www.bilibili.com/video/${encodeURIComponent(bvid)}`;
  }

  /**************** Index build / update ****************/
  async function buildIndexInitial50({ showMask } = {}) {
    if (!acquireLock()) throw new Error('已有抓取任务在运行');
    try {
      showMask?.('加载关注UP列表...');
      const followings = await loadAllFollowings();

      showMask?.(`首次抓取动态（前 ${CFG.INITIAL_MAX_PAGES} 页）...`);
      const r = await crawlDynamicsPages({
        startOffset: '',
        maxPages: CFG.INITIAL_MAX_PAGES,
        onProgress: (msg) => showMask?.(msg),
      });

      const videos = mergeVideos([], r.videos);
      const headNewestTs = videos[0]?.created || 0;

      const tailOffset = r.endOffset || '';
      const tailHasMore = !!r.hasMore;

      const index = {
        t: Date.now(),
        followings,
        videos,
        headNewestTs,
        tailOffset,
        tailHasMore,
      };

      await kvSet(DB.KEY_INDEX, index);
      return index;
    } finally {
      releaseLock();
    }
  }

  async function updateIndexHeadAndTail({ showMask } = {}) {
    if (!acquireLock()) throw new Error('已有抓取任务在运行');
    try {
      let index = await kvGet(DB.KEY_INDEX);
      if (!index) return await buildIndexInitial50({ showMask });

      showMask?.('更新：加载关注UP列表...');
      const followings = await loadAllFollowings();

      const oldHeadNewest = index.headNewestTs || 0;

      showMask?.('更新：抓新动态（头部增量）...');
      const headR = await crawlDynamicsPages({
        startOffset: '',
        stopWhenOlderThanTs: oldHeadNewest,
        maxPages: CFG.UPDATE_HEAD_MAX_PAGES,
        onProgress: (msg) => showMask?.(`新动态：${msg}`),
      });

      let tailR = { videos: [], endOffset: index.tailOffset || '', hasMore: index.tailHasMore !== false };
      if (index.tailHasMore && index.tailOffset) {
        showMask?.('更新：续抓旧动态（尾部未抓页）...');
        tailR = await crawlDynamicsPages({
          startOffset: index.tailOffset,
          maxPages: CFG.UPDATE_TAIL_MAX_PAGES,
          onProgress: (msg) => showMask?.(`旧动态：${msg}`),
        });
      } else {
        showMask?.('更新：尾部已无更多旧页可抓。');
      }

      const merged = mergeVideos(index.videos || [], [...(headR.videos || []), ...(tailR.videos || [])]);
      const headNewestTs = merged[0]?.created || oldHeadNewest;

      index = {
        t: Date.now(),
        followings,
        videos: merged,
        headNewestTs,
        tailOffset: tailR.endOffset || index.tailOffset || '',
        tailHasMore: !!tailR.hasMore,
      };

      await kvSet(DB.KEY_INDEX, index);
      return index;
    } finally {
      releaseLock();
    }
  }

  /**
   * ✅ 抓取全部动态（无发布时间边界）：
   * - 不做 stopWhenOlderThanTs 限制
   * - 只要 has_more=true 就持续抓
   * - 每抓一块就落盘，保证中断/重启也无需等待
   * - 若 tailOffset 为空：从 offset='' 开始（即从最新一路抓到最旧）
   */
  async function fetchAllDynamicsNoTimeLimit({ showMask } = {}) {
    if (!acquireLock()) throw new Error('已有抓取任务在运行');
    try {
      let index = await kvGet(DB.KEY_INDEX);
      if (!index) index = await buildIndexInitial50({ showMask });

      showMask?.('抓取全部：加载关注UP列表...');
      const followings = await loadAllFollowings();

      // 如果 tailOffset 为空，说明还没有“尾部游标”，就从最新开始全量往旧抓
      if (!index.tailOffset) {
        index.tailOffset = '';
        index.tailHasMore = true;
      }

      let round = 0;
      while (index.tailHasMore) {
        round++;
        showMask?.(`抓取全部：第 ${round} 轮（每轮 ${CFG.FETCH_ALL_CHUNK_PAGES} 页）... 当前合格 ${index.videos.length}`);

        const tailR = await crawlDynamicsPages({
          startOffset: index.tailOffset,     // '' => 从最新开始；否则从尾部游标继续
          stopWhenOlderThanTs: 0,            // ✅ 不设时间边界
          maxPages: CFG.FETCH_ALL_CHUNK_PAGES,
          onProgress: (msg) => showMask?.(`抓取全部（第${round}轮）：${msg} · 当前合格 ${index.videos.length}`),
        });

        index.videos = mergeVideos(index.videos || [], tailR.videos || []);
        index.headNewestTs = index.videos[0]?.created || index.headNewestTs || 0;
        index.tailOffset = tailR.endOffset || index.tailOffset;
        index.tailHasMore = !!tailR.hasMore;
        index.followings = followings;
        index.t = Date.now();

        // 每轮落盘
        await kvSet(DB.KEY_INDEX, index);

        // 如果接口不给 offset 了，无法继续
        if (!index.tailOffset && index.tailHasMore) break;
      }

      return index;
    } finally {
      releaseLock();
    }
  }

  /**************** Dashboard ****************/
  async function initDashboard() {
    if (document.readyState === 'loading') {
      await new Promise(r => document.addEventListener('DOMContentLoaded', r, { once: true }));
    }

    const app = mountDashboardShell();
    const $upList = app.querySelector('.bff-up-list');
    const $grid = app.querySelector('.bff-video-grid');
    const $watched = app.querySelector('.bff-watched-list');
    const $status = app.querySelector('#bff-status');
    const $search = app.querySelector('.bff-search');
    const $mask = app.querySelector('.bff-mask');
    const $maskText = app.querySelector('#bff-mask-text');

    const showMask = (t) => { $maskText.textContent = t || ''; $mask.style.display = 'flex'; };
    const hideMask = () => { $mask.style.display = 'none'; };
    const setStatus = (t) => { $status.textContent = t || ''; };

    let selectedUpMid = localStorage.getItem(LS.LAST_SELECTED_UP) || 'all';

    // 启动直接读缓存（秒开）
    let index = await kvGet(DB.KEY_INDEX);
    let historyMap = await loadRecentHistoryMap();

    function render() {
      if (!index) {
        setStatus('尚无缓存：请点右上角“更新”或“抓取全部动态”');
        $upList.innerHTML = '';
        $grid.innerHTML = '';
        $watched.innerHTML = '';
        return;
      }

      const rule = parseQuery($search.value || '');
      const hasQuery = ($search.value || '').trim().length > 0;
      const watchedMap = loadWatchedMap();

      // 左侧UP列表（按关注顺序）
      $upList.innerHTML = '';
      const mkUp = (mid, face, name, active) => {
        const div = document.createElement('div');
        div.className = 'bff-up-item' + (active ? ' active' : '');
        div.innerHTML = `
          <img class="bff-up-face" src="${face || ''}" referrerpolicy="no-referrer" />
          <div class="bff-up-name">${escapeHtml(name)}</div>
        `;
        div.addEventListener('click', () => {
          selectedUpMid = String(mid);
          localStorage.setItem(LS.LAST_SELECTED_UP, selectedUpMid);
          render();
        });
        return div;
      };

      $upList.appendChild(mkUp('all', '', '全部关注', selectedUpMid === 'all'));
      for (const u of (index.followings || [])) {
        $upList.appendChild(mkUp(u.mid, u.face, u.uname, String(u.mid) === String(selectedUpMid)));
      }

      // 数据源：全部 或 单UP
      let source = index.videos || [];
      if (selectedUpMid !== 'all') {
        const mid = Number(selectedUpMid);
        source = source.filter(v => v.upMid === mid);
      }

      if (hasQuery) source = source.filter(v => matchVideo(v, rule));

      // 时间排序
      source = source.slice().sort((a,b) => (b.created||0) - (a.created||0));

      // 未看/已看
      const unwatched = [];
      const watched = [];
      for (const v of source) {
        if (isWatched(v, watchedMap, historyMap)) watched.push(v);
        else unwatched.push(v);
      }

      // 中间未看
      $grid.innerHTML = '';
      if (unwatched.length === 0) {
        const div = document.createElement('div');
        div.style.gridColumn = '1 / -1';
        div.style.padding = '14px';
        div.style.color = 'rgba(18,22,32,.55)';
        div.textContent = '没有未看视频。';
        $grid.appendChild(div);
      } else {
        for (const v of unwatched) {
          const card = document.createElement('div');
          card.className = 'bff-card';
          card.title = v.title;
          card.innerHTML = `
            <div class="bff-cover" style="background-image:url('${v.pic}@480w_270h_1c.jpg');">
              <div class="bff-dur">${v.lengthText || fmtSec(v.durationSec)}</div>
            </div>
            <div class="bff-body">
              <div class="bff-title2">${escapeHtml(v.title)}</div>
              <div class="bff-meta">
                <span>${escapeHtml(v.upName)}</span>
                <span>${v.bvid}</span>
              </div>
            </div>
          `;
          card.addEventListener('click', () => openVideoSameTab(v.bvid));
          $grid.appendChild(card);
        }
      }

      // 右侧已看
      $watched.innerHTML = '';
      for (const v of watched) {
        const item = document.createElement('div');
        item.className = 'bff-w-item';
        item.title = v.title;
        item.innerHTML = `
          <div class="bff-w-title">${escapeHtml(v.title)}</div>
          <div class="bff-w-meta">
            <span>${escapeHtml(v.upName)}</span>
            <span>${v.lengthText || fmtSec(v.durationSec)}</span>
          </div>
        `;
        item.addEventListener('click', () => openVideoSameTab(v.bvid));
        $watched.appendChild(item);
      }

      setStatus(`合格视频：${index.videos.length} · 未看：${unwatched.length} · 已看：${watched.length} · 尾部更多：${index.tailHasMore ? '是' : '否'}`);
    }

    // 更新按钮：抓新页 + 续抓旧页（并保存）
    app.querySelector('[data-act="update"]').addEventListener('click', async () => {
      try {
        showMask('更新中...');
        index = await updateIndexHeadAndTail({ showMask });
        historyMap = await loadRecentHistoryMap();
        hideMask();
        render();
        toast('更新完成（已保存）');
      } catch (e) {
        hideMask();
        toast(`更新失败：${e.message}`, 2600);
      }
    });

    // 抓取全部动态（无发布时间边界）
    app.querySelector('[data-act="fetchall"]').addEventListener('click', async () => {
      try {
        showMask('抓取全部动态中（无发布时间边界）...');
        index = await fetchAllDynamicsNoTimeLimit({ showMask });
        historyMap = await loadRecentHistoryMap();
        hideMask();
        render();
        toast('抓取全部完成（已保存）', 2200);
      } catch (e) {
        hideMask();
        toast(`抓取全部失败：${e.message}`, 2800);
      }
    });

    $search.addEventListener('input', () => render());

    // 没有缓存：首次自动抓50页并保存（下次秒开）
    if (!index) {
      try {
        showMask('首次初始化：抓取动态前50页...');
        index = await buildIndexInitial50({ showMask });
        hideMask();
      } catch (e) {
        hideMask();
        toast(`首次抓取失败：${e.message}`, 3000);
      }
    }

    index = await kvGet(DB.KEY_INDEX);
    render();
  }

  /**************** Video minimal + volume + ended ****************/
  async function initVideoMinimal() {
    if (document.readyState === 'loading') {
      await new Promise(r => document.addEventListener('DOMContentLoaded', r, { once: true }));
    }

    const bvid = (location.pathname.match(/\/video\/(BV[a-zA-Z0-9]+)/)?.[1]) || '';
    if (!bvid) return;

    const bar = document.createElement('div');
    bar.id = 'bff-minibar';
    bar.innerHTML = `
      <button class="bff-mini-btn" data-act="back">返回筛选</button>
      <div class="bff-mini-txt">播完自动标记已看完并返回</div>
    `;
    document.documentElement.appendChild(bar);

    GM_addStyle(`
      #bff-minibar{
        position: fixed; top: 12px; left: 12px;
        z-index: 1000000;
        display:flex; gap:10px; align-items:center;
        padding: 10px 12px;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,.18);
        background: rgba(0,0,0,.28);
        color: rgba(255,255,255,.92);
        backdrop-filter: blur(18px);
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, "PingFang SC", "Microsoft YaHei", sans-serif;
      }
      #bff-minibar .bff-mini-btn{
        padding: 8px 10px;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,.18);
        background: rgba(255,255,255,.10);
        color: rgba(255,255,255,.92);
        cursor: pointer;
      }
    `);

    bar.querySelector('[data-act="back"]').addEventListener('click', () => {
      location.href = `https://www.bilibili.com/${CFG.DASH_HASH}`;
    });

    const video = await waitVideoEl(20000);
    if (!video) return;

    try { video.muted = false; video.volume = 0.5; } catch {}

    video.addEventListener('ended', async () => {
      try {
        markWatched(bvid);
        const view = await apiViewByBvid(bvid);
        const aid = view?.aid;
        const cid = view?.cid || view?.pages?.[0]?.cid;
        const duration = view?.duration;
        if (aid && cid && duration) await apiReportHistory({ aid, cid, progress: duration });
      } catch (e) {
        console.warn('[BFF] ended report failed', e);
      } finally {
        location.href = `https://www.bilibili.com/${CFG.DASH_HASH}`;
      }
    }, { once: true });
  }

  async function waitVideoEl(timeoutMs) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const v = document.querySelector('video');
      if (v) return v;
      await sleep(200);
    }
    return null;
  }

  /**************** Entry ****************/
  (async () => {
    try {
      if (isVideoPage()) initVideoMinimal();
      else if (isDashboard()) initDashboard();
      else if (CFG.FORCE_DASH_ON_NON_VIDEO) initDashboard();
    } catch (e) {
      console.error('[BFF] fatal', e);
    }
  })();

})();