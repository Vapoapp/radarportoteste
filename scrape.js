#!/usr/bin/env node
'use strict';

// ============================================================
// SENTINELA — scraper + ingestão GitHub-only v3
//
// Sem banco SQLite. Tudo em arquivos de texto.
//
// Arquivos gerados:
//   data/vessels.json                        ← pauta atual (index.html)
//   data/estado_atual.json                   ← foto por embarcação (IMO)
//   data/eventos/YYYY/MM/YYYY-MM-DD.jsonl    ← histórico permanente
//   data/snapshots/YYYY/MM/YYYY-MM-DD.jsonl.gz ← coletas brutas 30 dias
//
// Fluxo a cada execução:
//   1. Coleta os 4 portos → grava vessels.json
//   2. Carrega hashes já conhecidos do .jsonl do dia (deduplicação)
//   3. Carrega estado_atual.json
//   4. Para cada embarcação:
//      a. Calcula hash(porto+inicio+imo+tipo+de+para)
//      b. Se hash já existe → repetição, ignora
//      c. Se hash novo → acrescenta no .jsonl + atualiza estado_atual
//   5. Salva estado_atual.json (se mudou)
//   6. Acrescenta snapshot compactado do dia
//   7. Limpa snapshots com mais de 30 dias
//   8. Retorna código 0 se houve eventos novos, 2 se não houve
//      (o workflow usa esse código para decidir se commita)
// ============================================================

const https  = require('https');
const http   = require('http');
const fs     = require('fs');
const path   = require('path');
const crypto = require('crypto');
const zlib   = require('zlib');

// ── CONFIG ────────────────────────────────────────────────────────────────────
const DATA_DIR      = path.join(__dirname, 'data');
const VESSELS_PATH  = path.join(DATA_DIR, 'vessels.json');
const ESTADO_PATH   = path.join(DATA_DIR, 'estado_atual.json');
const EVENTOS_DIR   = path.join(DATA_DIR, 'eventos');
const SNAPSHOTS_DIR = path.join(DATA_DIR, 'snapshots');
const METRICAS_DIR     = path.join(DATA_DIR, 'metricas');
const EMBARCACOES_DIR  = path.join(DATA_DIR, 'embarcacoes');

const PORTOS = [
  { id:'rio',     nome:'Rio de Janeiro', url:'https://silog.portosrio.gov.br/silog/pesquisa.aspx?WCI=relPrePautaSimplificado&Mv=Link&sqlCodDominio=1&sqlFLG_PUBLICO_EXTERNO=1' },
  { id:'niteroi', nome:'Niterói',        url:'https://silog.portosrio.gov.br/silog/pesquisa.aspx?WCI=relPrePautaSimplificado&Mv=Link&sqlCodDominio=2&sqlFLG_PUBLICO_EXTERNO=1' },
  { id:'itaguai', nome:'Itaguaí',        url:'https://silog.portosrio.gov.br/silog/pesquisa.aspx?WCI=relPrePautaSimplificado&Mv=Link&sqlCodDominio=3&sqlFLG_PUBLICO_EXTERNO=1' },
  { id:'angra',   nome:'Angra dos Reis', url:'https://silog.portosrio.gov.br/silog/pesquisa.aspx?WCI=relPrePautaSimplificado&Mv=Link&sqlCodDominio=4&sqlFLG_PUBLICO_EXTERNO=1' },
];

// ── UTILS ─────────────────────────────────────────────────────────────────────

function sha256(s) {
  return crypto.createHash('sha256').update(s, 'utf8').digest('hex');
}

/**
 * Normaliza nome de embarcação para agrupamento e ranking.
 *   "Delta Cardinal (REB)"  → "DELTA CARDINAL"
 *   "A.H. Camoglì"          → "A.H. CAMOGLI"
 *   "  MSC  ALBANY  "       → "MSC ALBANY"
 */
function normalizeNavio(name) {
  if (!name || typeof name !== 'string') return '';
  return name
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s*\(REB\)\s*/gi, ' ')
    .replace(/[^\x20-\x7E]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase();
}

/**
 * Converte "DD/MM/YYYY HH:MM" → ISO-8601 UTC
 * A pauta usa horário de Brasília (UTC-3, sem horário de verão desde 2019).
 * "01/04/2026 06:00" (Brasília) → "2026-04-01T09:00:00.000Z" (UTC)
 */
function parseInicio(s) {
  const m = (s || '').match(/^(\d{2})\/(\d{2})\/(\d{4}) (\d{2}):(\d{2})$/);
  if (!m) return new Date(0).toISOString();
  const utc = Date.UTC(
    parseInt(m[3], 10),
    parseInt(m[2], 10) - 1,
    parseInt(m[1], 10),
    parseInt(m[4], 10) + 3,  // UTC-3 → UTC
    parseInt(m[5], 10)
  );
  return new Date(utc).toISOString();
}

/**
 * Data de Brasília no formato YYYY-MM-DD (para nomear arquivos do dia).
 * Usa UTC-3 fixo.
 */
function diaBrasilia(isoUtc) {
  const d = new Date(new Date(isoUtc).getTime() - 3 * 60 * 60 * 1000);
  return d.toISOString().slice(0, 10); // "YYYY-MM-DD"
}

/** em_fundeio: 1 se destino contém "Fundeio" e não é SAÍDA */
function calcFundeio(tipo, para) {
  if (/saída|saida/i.test(tipo)) return false;
  return /fundeio/i.test(para || '');
}

/**
 * Hash de identidade do evento — sem horário, sem porto.
 * Identifica "o quê" a embarcação faz, de onde e para onde.
 * Usado pela máquina de estados para detectar remarcações.
 */
function hashIdentidade(v) {
  return sha256(`${v.imo}|${v.tipo}|${v.de}|${v.para}`);
}

/**
 * Hash completo para o arquivo .jsonl — inclui porto e horário.
 * Usado apenas como identificador único de registro gravado.
 */
function hashEvento(v) {
  return sha256(`${v.porto}|${v.inicio}|${v.imo}|${v.tipo}|${v.de}|${v.para}`);
}

/**
 * Máquina de estados por embarcação.
 *
 * Transições válidas:
 *   (vazio | SAÍDA)      + ENTRADA  → novo evento
 *   (ENTRADA | MUDANÇA)  + MUDANÇA  → novo evento
 *   (ENTRADA | MUDANÇA)  + SAÍDA    → novo evento
 *
 * Remarcações (mesmo tipo consecutivo):
 *   (ENTRADA | MUDANÇA)  + ENTRADA  → remarcação: atualiza horário
 *   (SAÍDA)              + SAÍDA    → remarcação: atualiza horário
 *   (SAÍDA | vazio)      + MUDANÇA  → evento órfão: ignora
 *
 * @param {string|null} ultimoTipo  — último tipo gravado para este IMO
 * @param {string}      novoTipo    — tipo do evento sendo processado
 * @returns {'novo'|'remarcacao'|'orfao'}
 */
function classificarEvento(ultimoTipo, novoTipo) {
  const ultimo = (ultimoTipo || '').toLowerCase();
  const novo   = novoTipo.toLowerCase();

  const isEntrada = s => /entrada/.test(s);
  const isSaida   = s => /sa[íi]da/.test(s);
  const isMudanca = s => /mudan[cç]a/.test(s);

  // sem histórico
  if (!ultimoTipo) {
    if (isEntrada(novo)) return 'novo';
    if (isSaida(novo))   return 'novo';   // pode acontecer para embarcações sem histórico
    if (isMudanca(novo)) return 'orfao';  // mudança sem entrada prévia
  }

  // último foi SAÍDA
  if (isSaida(ultimo)) {
    if (isEntrada(novo)) return 'novo';
    if (isSaida(novo))   return 'remarcacao';
    if (isMudanca(novo)) return 'orfao';
  }

  // último foi ENTRADA ou MUDANÇA (embarcação dentro do porto)
  if (isEntrada(ultimo) || isMudanca(ultimo)) {
    if (isEntrada(novo)) return 'remarcacao';
    if (isMudanca(novo)) return 'novo';
    if (isSaida(novo))   return 'novo';
  }

  return 'novo'; // fallback seguro
}

// ── FILESYSTEM ────────────────────────────────────────────────────────────────

function mkdirp(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function readJson(filePath, fallback) {
  try { return JSON.parse(fs.readFileSync(filePath, 'utf8')); }
  catch { return fallback; }
}

/**
 * Tenta adquirir lock exclusivo sobre um arquivo.
 *
 * Estratégia:
 *   1. Se o .lock não existe → cria com O_EXCL (atômico) e retorna o path.
 *   2. Se o .lock existe mas é mais velho que LOCK_STALE_MS → processo morreu,
 *      remove o lock stale e tenta criar novamente.
 *   3. Se o .lock existe e é recente → outro processo ativo, retorna null.
 *
 * O arquivo de lock contém o PID do processo dono, útil para diagnóstico.
 */
const LOCK_STALE_MS = 30000; // lock com mais de 30s é considerado morto

function acquireLock(filePath) {
  const lock = filePath + '.lock';
  try {
    // tenta criar diretamente (caminho mais comum — sem lock existente)
    fs.writeFileSync(lock, String(process.pid), { flag: 'wx' });
    return lock;
  } catch {
    // lock existe — verifica se é stale (processo morreu)
    try {
      const stat = fs.statSync(lock);
      const age  = Date.now() - stat.mtimeMs;
      if (age >= LOCK_STALE_MS) {
        // lock abandonado → remove e tenta de novo
        fs.unlinkSync(lock);
        fs.writeFileSync(lock, String(process.pid), { flag: 'wx' });
        console.warn(`⚠ lock stale removido (${Math.round(age / 1000)}s): ${lock}`);
        return lock;
      }
    } catch { /* stat ou unlink falhou — outro processo ganhou a corrida */ }
    return null;
  }
}

function releaseLock(lockPath) {
  try { if (lockPath) fs.unlinkSync(lockPath); } catch {}
}

/**
 * Escreve JSON de forma atômica:
 *   1. Adquire lock exclusivo
 *   2. Grava em arquivo .tmp
 *   3. Rename atômico .tmp → destino
 *   4. Libera lock
 * Garante que leitores nunca veem arquivo parcialmente escrito.
 * Se outro processo já tiver o lock, aguarda até LOCK_TIMEOUT_MS e desiste.
 */
const LOCK_TIMEOUT_MS = 5000;
const LOCK_RETRY_MS   = 50;

function writeJson(filePath, data) {
  mkdirp(path.dirname(filePath));
  const tmp  = filePath + '.tmp';
  const lock = filePath + '.lock';

  // aguarda lock com timeout
  const deadline = Date.now() + LOCK_TIMEOUT_MS;
  let lockAcquired = null;
  while (Date.now() < deadline) {
    lockAcquired = acquireLock(filePath);
    if (lockAcquired) break;
    // espera síncrona simples (execução single-threaded, não bloqueia event loop)
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, LOCK_RETRY_MS);
  }
  if (!lockAcquired) {
    // timeout: grava direto sem lock (melhor do que perder o dado)
    console.warn(`⚠ writeJson: timeout ao aguardar lock em ${filePath}, gravando sem lock`);
    fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
    fs.renameSync(tmp, filePath);
    return;
  }
  try {
    fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
    fs.renameSync(tmp, filePath);  // atômico no mesmo filesystem
  } finally {
    releaseLock(lockAcquired);
  }
}

/** Retorna path do .jsonl de eventos para uma data "YYYY-MM-DD" */
function eventosPath(dia) {
  const [ano, mes] = dia.split('-');
  return path.join(EVENTOS_DIR, ano, mes, `${dia}.jsonl`);
}

/** Retorna path do snapshot .jsonl.gz para uma data "YYYY-MM-DD" */
function snapshotPath(dia) {
  const [ano, mes] = dia.split('-');
  return path.join(SNAPSHOTS_DIR, ano, mes, `${dia}.jsonl.gz`);
}

/**
 * Carrega o estado do .jsonl do dia.
 * Retorna:
 *   hashesGravados — Set com todos os hash_evento já gravados (evita duplicata de linha)
 *   identidadeMap  — Map<hash_identidade, evento> com o evento mais recente por identidade
 *                    Usado pela máquina de estados para detectar remarcações.
 */
function carregarEstadoDoDia(dia) {
  const p = eventosPath(dia);
  const hashesGravados = new Set();
  const identidadeMap  = new Map(); // hash_identidade → evento

  if (!fs.existsSync(p)) return { hashesGravados, identidadeMap };

  const linhas = fs.readFileSync(p, 'utf8').split('\n').filter(Boolean);
  for (const linha of linhas) {
    try {
      const ev = JSON.parse(linha);
      if (ev.hash_evento)     hashesGravados.add(ev.hash_evento);
      if (ev.hash_identidade) {
        const anterior = identidadeMap.get(ev.hash_identidade);
        if (!anterior || ev.inicio_evento > anterior.inicio_evento) {
          identidadeMap.set(ev.hash_identidade, ev);
        }
      }
    } catch {}
  }
  return { hashesGravados, identidadeMap };
}

/**
 * Acrescenta uma linha ao .jsonl de eventos.
 * Cria o arquivo e diretórios se necessário.
 */
function appendEvento(dia, obj) {
  const p = eventosPath(dia);
  mkdirp(path.dirname(p));
  fs.appendFileSync(p, JSON.stringify(obj) + '\n', 'utf8');
}

/**
 * Atualiza o inicio_evento de um evento já gravado no .jsonl do dia.
 * Reescreve o arquivo substituindo a linha com o hash_identidade informado.
 * Usado quando a máquina de estados detecta uma remarcação de horário.
 */
function atualizarInicioEvento(dia, hashIdentidade, inicioAnterior, novoInicio, novoHashEvento, agora) {
  const p = eventosPath(dia);
  if (!fs.existsSync(p)) return;

  const lockAcquired = acquireLock(p);
  if (!lockAcquired) {
    console.warn(`⚠ atualizarInicioEvento: não conseguiu lock em ${p}, pulando atualização`);
    return;
  }
  try {
    const linhas = fs.readFileSync(p, 'utf8').split('\n').filter(Boolean);
    let atualizou = false;
    const atualizadas = linhas.map(l => {
      try {
        const ev = JSON.parse(l);
        // precisa bater hash_identidade E o inicio anterior — evita colidir
        // com outro evento legítimo da mesma identidade no mesmo dia
        if (!atualizou && ev.hash_identidade === hashIdentidade && ev.inicio_evento === inicioAnterior) {
          ev.inicio_evento  = novoInicio;
          ev.hash_evento    = novoHashEvento;
          ev.atualizado_em  = agora;
          ev.remarcado      = true;
          atualizou = true;
        }
        return JSON.stringify(ev);
      } catch { return l; }
    });
    // escrita atômica: .tmp → rename
    const tmp = p + '.tmp';
    fs.writeFileSync(tmp, atualizadas.join('\n') + '\n', 'utf8');
    fs.renameSync(tmp, p);
  } finally {
    releaseLock(lockAcquired);
  }
}

/**
 * Acrescenta linha de snapshot compactado no .jsonl.gz do dia.
 * Cada linha é um JSON.stringify comprimido em gzip+base64.
 */
function appendSnapshot(dia, snapshotObj) {
  const p = snapshotPath(dia);
  mkdirp(path.dirname(p));
  const linha = zlib.gzipSync(JSON.stringify(snapshotObj)).toString('base64') + '\n';
  fs.appendFileSync(p, linha, 'utf8');
}

/**
 * Remove snapshots de dias anteriores ao limite de retenção (30 dias).
 * Varre SNAPSHOTS_DIR/YYYY/MM/ e deleta arquivos fora da janela.
 */
function limparSnapshotsAntigos() {
  if (!fs.existsSync(SNAPSHOTS_DIR)) return;
  const limite = new Date();
  limite.setUTCDate(limite.getUTCDate() - 30);
  const limiteStr = limite.toISOString().slice(0, 10); // "YYYY-MM-DD"

  let removidos = 0;
  for (const ano of fs.readdirSync(SNAPSHOTS_DIR)) {
    const anoDir = path.join(SNAPSHOTS_DIR, ano);
    if (!fs.statSync(anoDir).isDirectory()) continue;
    for (const mes of fs.readdirSync(anoDir)) {
      const mesDir = path.join(anoDir, mes);
      if (!fs.statSync(mesDir).isDirectory()) continue;
      for (const arquivo of fs.readdirSync(mesDir)) {
        const dia = arquivo.replace('.jsonl.gz', '');
        if (dia < limiteStr) {
          fs.unlinkSync(path.join(mesDir, arquivo));
          removidos++;
        }
      }
      // remove dir vazio
      if (fs.readdirSync(mesDir).length === 0) fs.rmdirSync(mesDir);
    }
    if (fs.readdirSync(anoDir).length === 0) fs.rmdirSync(anoDir);
  }
  if (removidos > 0) console.log(`🗑  Snapshots antigos removidos: ${removidos}`);
}

// ── HTTP ──────────────────────────────────────────────────────────────────────

function fetchUrl(url, redirectCount = 0) {
  if (redirectCount > 5) return Promise.reject(new Error('Too many redirects'));
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, {
      headers: {
        'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9',
      },
      timeout: 30000,
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        req.destroy();
        const next = res.headers.location.startsWith('http')
          ? res.headers.location
          : new URL(res.headers.location, url).href;
        return fetchUrl(next, redirectCount + 1).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        req.destroy();
        return reject(new Error('HTTP ' + res.statusCode));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        let html = buf.toString('utf8');
        const m = html.match(/charset=["']?([a-zA-Z0-9-]+)/i);
        if (m) {
          const enc = m[1].toLowerCase().replace('-', '');
          if (['iso88591', 'latin1', 'windows1252'].includes(enc)) html = buf.toString('latin1');
        }
        resolve(html);
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

async function fetchWithRetry(url, retries = 3) {
  let lastErr;
  for (let i = 0; i < retries; i++) {
    try { return await fetchUrl(url); } catch (e) {
      lastErr = e;
      if (i < retries - 1) {
        const delay = (i + 1) * 3000;
        console.log(`  retry ${i + 1}/${retries - 1} em ${delay / 1000}s...`);
        await new Promise(r => setTimeout(r, delay));
      }
    }
  }
  throw lastErr;
}

// ── HTML PARSER ───────────────────────────────────────────────────────────────

function stripHtml(s) {
  return s
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi,  '&')
    .replace(/&lt;/gi,   '<')
    .replace(/&gt;/gi,   '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#\d+;/g,  '')
    .trim();
}

function parseVessels(html, portoNome) {
  const vessels = [];
  const rowRe   = /<tr([^>]*)>([\s\S]*?)<\/tr>/gi;
  let row;
  while ((row = rowRe.exec(html)) !== null) {
    const attrs   = row[1] || '';
    const content = row[2] || '';
    if ((attrs + content).toLowerCase().includes('cancelado')) continue;
    if (content.toLowerCase().includes('colspan'))             continue;
    const cells = [];
    const tdRe  = /<td[^>]*>([\s\S]*?)<\/td>/gi;
    let td;
    while ((td = tdRe.exec(content)) !== null) cells.push(stripHtml(td[1]));
    if (cells.length >= 7) {
      const navio = cells[2];
      if (navio && navio.length > 1) {
        vessels.push({
          porto:  portoNome,
          inicio: cells[0],
          imo:    cells[1],
          navio,
          tipo:   cells[3],
          de:     cells[4],
          para:   cells[5],
          agente: cells[6],
        });
      }
    }
  }
  return vessels;
}

// ── MÉTRICAS ─────────────────────────────────────────────────────────────────

/**
 * Gera data/metricas/radar.json e data/metricas/fundeio.json.
 * Roda após cada ciclo de ingestão, mesmo sem eventos novos,
 * para manter os JSONs frescos a cada coleta bem-sucedida.
 */
function gerarMetricas(estado, diaHoje, agora) {
  mkdirp(METRICAS_DIR);
  const estadoList = Object.values(estado);

  // ── fundeio agora ──────────────────────────────────────────────────────────
  const emFundeio = estadoList.filter(e => e.em_fundeio);
  const fundeioList = emFundeio
    .sort((a, b) => a.ultima_movimentacao_em.localeCompare(b.ultima_movimentacao_em))
    .map(e => ({
      imo:                  e.imo,
      navio:                e.navio,
      porto:                e.porto_atual,
      local:                e.destino_atual || e.origem_atual || '',
      ultima_movimentacao:  e.ultima_movimentacao_em,
    }));

  // ── contadores do dia (lê o .jsonl já gravado) ────────────────────────────
  let entradas = 0, saidas = 0, mudancas = 0;
  const pEventos = eventosPath(diaHoje);  // usa diaHoje (Brasília), não UTC
  if (fs.existsSync(pEventos)) {
    const linhas = fs.readFileSync(pEventos, 'utf8').split('\n').filter(Boolean);
    for (const linha of linhas) {
      try {
        const e = JSON.parse(linha);
        // campo correto é tipo_evento, não tipo
        if (/entrada/i.test(e.tipo_evento))  entradas++;
        else if (/saída|saida/i.test(e.tipo_evento)) saidas++;
        else if (/mudança|mudanca/i.test(e.tipo_evento)) mudancas++;
      } catch {}
    }
  }

  // ── radar ──────────────────────────────────────────────────────────────────
  const radar = {
    fundeio_agora:  emFundeio.length,
    entradas_hoje:  entradas,
    saidas_hoje:    saidas,
    mudancas_hoje:  mudancas,
    total_hoje:     entradas + saidas + mudancas,
    atualizado_em:  agora,
  };

  writeJson(path.join(METRICAS_DIR, 'radar.json'),   radar);
  writeJson(path.join(METRICAS_DIR, 'fundeio.json'), fundeioList);

  // ── índice nome → IMO (busca por nome no relatório) ───────────────────────
  // Formato: [{ imo, navio, navio_normalizado, porto }]
  // Ordenado por nome normalizado para facilitar busca no front
  const indice = estadoList
    .map(e => ({
      imo:               e.imo,
      navio:             e.navio,
      navio_normalizado: e.navio_normalizado,
      porto:             e.porto_atual,
    }))
    .sort((a, b) => a.navio_normalizado.localeCompare(b.navio_normalizado));

  writeJson(path.join(METRICAS_DIR, 'indice-embarcacoes.json'), indice);
}

// ── RELATÓRIOS POR EMBARCAÇÃO ────────────────────────────────────────────────

/**
 * Lê todos os eventos de um intervalo de dias.
 * diasAtras: quantos dias para trás a partir de hoje
 * diasAte:   limite inferior (ex: diasAte=30 lê apenas dias 30..diasAtras-1)
 */
function loadEventosPeriodo(diasAtras, diasAte = 0) {
  const eventos = [];
  const agoraMs = Date.now();
  for (let i = diasAte; i < diasAtras; i++) {
    const dia = diaBrasilia(new Date(agoraMs - i * 86400000).toISOString());
    const p   = eventosPath(dia);
    if (!fs.existsSync(p)) continue;
    const linhas = fs.readFileSync(p, 'utf8').split('\n').filter(Boolean);
    for (const l of linhas) {
      try { eventos.push(JSON.parse(l)); } catch {}
    }
  }
  return eventos;
}

/**
 * Tempo médio de estadia: pareia ENTRADA→SAÍDA no MESMO porto.
 * Descarta pares com duração < 0.5h ou > 720h (inconsistentes).
 */
function tempoMedioEstadia(eventos) {
  const porPorto = {};
  for (const e of eventos) {
    if (!porPorto[e.porto]) porPorto[e.porto] = [];
    porPorto[e.porto].push(e);
  }
  const duracoes = [];
  for (const ev of Object.values(porPorto)) {
    const ord = [...ev].sort((a, b) => a.inicio_evento.localeCompare(b.inicio_evento));
    let entrada = null;
    for (const e of ord) {
      if (/entrada/i.test(e.tipo_evento)) {
        entrada = e;
      } else if (/sa[íi]da/i.test(e.tipo_evento) && entrada) {
        const h = (new Date(e.inicio_evento) - new Date(entrada.inicio_evento)) / 3600000;
        if (h >= 0.5 && h <= 720) duracoes.push(h);
        entrada = null;
      }
    }
  }
  if (!duracoes.length) return null;
  return Math.round(duracoes.reduce((a, b) => a + b, 0) / duracoes.length * 10) / 10;
}

/**
 * Tempo médio em fundeio: detecta pela flag em_fundeio_apos_evento.
 */
function tempoMedioFundeio(eventos) {
  const ord = [...eventos].sort((a, b) => a.inicio_evento.localeCompare(b.inicio_evento));
  const duracoes = [];
  let entFundeio = null;
  for (const e of ord) {
    if (e.em_fundeio_apos_evento && !entFundeio) {
      entFundeio = e;
    } else if (!e.em_fundeio_apos_evento && entFundeio) {
      const h = (new Date(e.inicio_evento) - new Date(entFundeio.inicio_evento)) / 3600000;
      if (h >= 0.5 && h <= 720) duracoes.push(h);
      entFundeio = null;
    }
  }
  if (!duracoes.length) return null;
  return Math.round(duracoes.reduce((a, b) => a + b, 0) / duracoes.length * 10) / 10;
}

/**
 * Gera data/embarcacoes/{IMO}.json para cada embarcação com eventos recentes.
 * ev30 e ev365 são carregados sem sobreposição:
 *   ev30  = dias 0..29  (janela de 30 dias)
 *   ev365 = dias 30..364 (janela de 31 a 365 dias atrás)
 */
function gerarRelatoriosEmbarcacoes(estado, agora) {
  mkdirp(EMBARCACOES_DIR);

  // três janelas sem sobreposição:
  //   ev30  = dias   0-29
  //   ev180 = dias  30-179
  //   ev365 = dias 180-364
  const ev30  = loadEventosPeriodo(30);
  const ev180 = loadEventosPeriodo(180, 30);
  const ev365 = loadEventosPeriodo(365, 180);

  // agrupa por IMO sem sobreposição
  const porIMO = {};
  for (const [arr, chave] of [[ev30,'ev30'],[ev180,'ev180'],[ev365,'ev365']]) {
    for (const e of arr) {
      if (!e.imo) continue;
      if (!porIMO[e.imo]) porIMO[e.imo] = { ev30:[], ev180:[], ev365:[] };
      porIMO[e.imo][chave].push(e);
    }
  }

  const contar = (arr, re) => arr.filter(e => re.test(e.tipo_evento)).length;
  const ultimo = (arr, re) => arr
    .filter(e => re.test(e.tipo_evento))
    .sort((a, b) => b.inicio_evento.localeCompare(a.inicio_evento))[0]?.inicio_evento || null;

  const RE_ENTRADA = /entrada/i;
  const RE_SAIDA   = /sa[íi]da/i;
  const RE_MUDANCA = /mudan[cç]a/i;

  // acumulador para o ranking (calculado na mesma passagem)
  const rankingAcc = {};

  let gerados = 0;
  for (const [imo, { ev30: e30, ev180: e180, ev365: e365 }] of Object.entries(porIMO)) {
    const todos = [...e30, ...e180, ...e365].sort((a, b) =>
      b.inicio_evento.localeCompare(a.inicio_evento)
    );
    const est   = estado[imo];
    const navio = todos[0]?.navio || est?.navio || imo;
    const porto = est?.porto_atual || todos[0]?.porto || '';
    const todos_asc = [...todos].reverse();

    // manobras por janela (arrays já particionados — sem refiltrar por data)
    const man30  = e30.length;
    const man180 = e30.length + e180.length;
    const man365 = e30.length + e180.length + e365.length;

    const relatorio = {
      imo,
      navio,
      gerado_em: agora,

      estado_atual: est ? {
        porto:               est.porto_atual,
        em_fundeio:          est.em_fundeio,
        local:               est.destino_atual || est.origem_atual || '',
        ultimo_tipo_evento:  est.ultimo_tipo_evento,
        ultima_movimentacao: est.ultima_movimentacao_em,
      } : null,

      metricas: {
        manobras_30d:   man30,
        manobras_180d:  man180,
        manobras_365d:  man365,
        entradas_30d:   contar(e30,  RE_ENTRADA),
        saidas_30d:     contar(e30,  RE_SAIDA),
        mudancas_30d:   contar(e30,  RE_MUDANCA),
        entradas_365d:  contar(e30,  RE_ENTRADA) + contar(e180, RE_ENTRADA) + contar(e365, RE_ENTRADA),
        saidas_365d:    contar(e30,  RE_SAIDA)   + contar(e180, RE_SAIDA)   + contar(e365, RE_SAIDA),
        ultima_entrada: ultimo(todos, RE_ENTRADA),
        ultima_saida:   ultimo(todos, RE_SAIDA),
        tempo_medio_estadia_horas: tempoMedioEstadia(todos_asc),
        tempo_medio_fundeio_horas: tempoMedioFundeio(todos_asc),
      },

      historico_recente: todos.slice(0, 10).map(e => ({
        data:  e.inicio_evento,
        tipo:  e.tipo_evento,
        porto: e.porto,
        de:    e.origem,
        para:  e.destino,
      })),
    };

    writeJson(path.join(EMBARCACOES_DIR, imo + '.json'), relatorio);
    gerados++;

    // acumula para o ranking (só embarcações com ao menos 1 manobra)
    if (man365 > 0) {
      rankingAcc[imo] = { imo, navio, porto, man30, man180, man365 };
    }
  }

  // gera ranking-atividade.json a partir do acumulador (sem reler arquivos)
  const rankSort = (campo) =>
    Object.values(rankingAcc)
      .sort((a, b) => b[campo] - a[campo])
      .slice(0, 10)
      .map(({ imo, navio, porto, man30, man180, man365 }, i) => ({
        posicao: i + 1, imo, navio, porto,
        manobras: campo === 'man30' ? man30 : campo === 'man180' ? man180 : man365,
      }));

  writeJson(path.join(METRICAS_DIR, 'ranking-atividade.json'), {
    periodos: {
      '30d':  rankSort('man30'),
      '180d': rankSort('man180'),
      '365d': rankSort('man365'),
    },
    atualizado_em: agora,
  });

  console.log(`  -> data/embarcacoes/ (${gerados} arquivos)`);
  console.log(`  -> data/metricas/ranking-atividade.json`);
}


// ── MAIN ──────────────────────────────────────────────────────────────────────

async function main() {
  mkdirp(DATA_DIR);

  const agora    = new Date().toISOString();
  const diaHoje  = diaBrasilia(agora);

  // ── 1. coleta os 4 portos ──────────────────────────────────────────────────
  const allVessels = [];
  const portStatus = {};
  let   anySuccess = false;

  for (const porto of PORTOS) {
    try {
      process.stdout.write(`Coletando ${porto.nome}... `);
      const html    = await fetchWithRetry(porto.url);
      const vessels = parseVessels(html, porto.nome);
      allVessels.push(...vessels);
      portStatus[porto.id] = { ok: true, count: vessels.length };
      console.log(`OK — ${vessels.length} registros`);
      anySuccess = true;
    } catch (e) {
      console.log(`ERRO — ${e.message}`);
      portStatus[porto.id] = { ok: false, error: e.message };
    }
  }

  if (!anySuccess) {
    console.error('Todos os portos falharam.');
    process.exit(1);
  }

  // ── 4. carrega estado do dia (hashes gravados + mapa de identidade) ────────
  const { hashesGravados, identidadeMap } = carregarEstadoDoDia(diaHoje);

  // ── 5. carrega estado atual ───────────────────────────────────────────────
  const estado = readJson(ESTADO_PATH, {});  // { [imo]: { ...campos } }

  // ── helpers inline ────────────────────────────────────────────────────────
  function buildEvento(v, inicioISO, navioNorm, emFundeio, hash, hashIdent, agora) {
    return {
      porto:                  v.porto,
      inicio_evento:          inicioISO,
      imo:                    v.imo,
      navio:                  v.navio,
      navio_normalizado:      navioNorm,
      tipo_evento:            v.tipo,
      origem:                 v.de,
      destino:                v.para,
      agente:                 v.agente,
      em_fundeio_apos_evento: emFundeio,
      hash_evento:            hash,
      hash_identidade:        hashIdent,
      criado_em:              agora,
    };
  }

  function atualizarEstado(estado, v, inicioISO, navioNorm, emFundeio, hash, agora) {
    const estadoAtual = estado[v.imo];
    const jaTemEstado = estadoAtual && estadoAtual.ultima_movimentacao_em;
    const maisRecente = !jaTemEstado || inicioISO >= estadoAtual.ultima_movimentacao_em;
    if (maisRecente) {
      estado[v.imo] = {
        imo:                    v.imo,
        navio:                  v.navio,
        navio_normalizado:      navioNorm,
        porto_atual:            v.porto,
        ultimo_tipo_evento:     v.tipo,
        origem_atual:           v.de,
        destino_atual:          v.para,
        agente_atual:           v.agente,
        em_fundeio:             emFundeio,
        ultima_movimentacao_em: inicioISO,
        ultimo_hash_evento:     hash,
        atualizado_em:          agora,
      };
    }
  }

  // ── 6. filtra eventos futuros ────────────────────────────────────────────────
  // A pauta inclui manobras programadas para o futuro. O estado_atual e as
  // métricas devem refletir apenas o que já aconteceu até agora.
  const agoraMs = new Date(agora).getTime();
  const futuros = allVessels.filter(v => new Date(parseInicio(v.inicio)).getTime() > agoraMs);
  const vessels = allVessels.filter(v => new Date(parseInicio(v.inicio)).getTime() <= agoraMs);

  if (futuros.length > 0) {
    console.log(`  ⏭ ${futuros.length} evento(s) futuros ignorados (ainda não ocorreram)`);
  }

  // grava vessels.json com TODOS os eventos do dia (pauta completa para o frontend)
  // mas processa apenas os passados na máquina de estados
  const jsonData = { updatedAt: agora, portos: portStatus, vessels: allVessels };
  writeJson(VESSELS_PATH, jsonData);
  appendSnapshot(diaHoje, jsonData);

  // ── 6b. ordena por horário antes de processar ─────────────────────────────
  // Garante que a máquina de estados recebe os eventos em ordem cronológica,
  // independente da ordem em que o HTML foi gerado pela fonte.
  vessels.sort((a, b) => {
    const ta = parseInicio(a.inicio);
    const tb = parseInicio(b.inicio);
    if (ta !== tb) return ta.localeCompare(tb);
    if (a.imo !== b.imo) return String(a.imo).localeCompare(String(b.imo));
    return String(a.tipo).localeCompare(String(b.tipo));
  });

  // ── 6b. deduplica eventos com mesmo IMO + tipo + horário ────────────────────
  // A fonte (SILOG) às vezes publica dois eventos idênticos em tipo e horário
  // para a mesma embarcação com origens diferentes — erro de entrada na pauta.
  // Após o sort, o último da lista (índice maior) é o mais recente coletado,
  // que é o que queremos manter.
  {
    const seen = new Map(); // `${imo}|${tipo}|${inicio}` → índice
    for (let i = 0; i < vessels.length; i++) {
      const v   = vessels[i];
      const key = `${v.imo}|${v.tipo}|${v.inicio}`;
      if (seen.has(key)) {
        const prevIdx = seen.get(key);
        console.log(`  ✂ duplicata de horário removida: ${v.navio} — ${v.tipo} ${v.inicio} (de: "${vessels[prevIdx].de}" descartado, mantido: "${v.de}")`);
        vessels.splice(prevIdx, 1);
        i--;
      }
      seen.set(key, i);
    }
  }

  // ── 7. processa cada embarcação (máquina de estados) ─────────────────────
  let novos = 0, remarcados = 0, orfaos = 0, repetidos = 0;

  for (const v of vessels) {
    const inicioISO  = parseInicio(v.inicio);
    const navioNorm  = normalizeNavio(v.navio);
    const emFundeio  = calcFundeio(v.tipo, v.para);
    const ultimoTipo = estado[v.imo]?.ultimo_tipo_evento || null;

    const hash      = hashEvento(v);
    const hashIdent = hashIdentidade(v);

    // proteção contra jobs paralelos: linha idêntica já gravada → ignora
    if (hashesGravados.has(hash)) {
      repetidos++;
      continue;
    }

    const classificacao = classificarEvento(ultimoTipo, v.tipo);

    if (classificacao === 'orfao') {
      orfaos++;
      console.log(`  ⚠ órfão ignorado: ${v.navio} (IMO ${v.imo}) — ${v.tipo} sem estado anterior`);
      continue;
    }

    if (classificacao === 'remarcacao') {
      const anterior = identidadeMap.get(hashIdent);
      if (anterior && inicioISO === anterior.inicio_evento) {
        // horário idêntico → repetição pura
        repetidos++;
        continue;
      }
      if (anterior) {
        // horário diferente → remarcação: atualiza linha no .jsonl
        atualizarInicioEvento(diaHoje, hashIdent, anterior.inicio_evento, inicioISO, hash, agora);
        identidadeMap.set(hashIdent, { ...anterior, inicio_evento: inicioISO, hash_evento: hash });
        hashesGravados.add(hash);
        remarcados++;
        console.log(`  ↺ remarcado: ${v.navio} — ${v.tipo} ${anterior.inicio_evento} → ${inicioISO}`);
      } else {
        // identidade não está no dia atual (ex.: SAÍDA remarcou de 23h→01h do dia seguinte)
        // trata como novo evento no novo dia
        const ev = buildEvento(v, inicioISO, navioNorm, emFundeio, hash, hashIdent, agora);
        appendEvento(diaHoje, ev);
        hashesGravados.add(hash);
        identidadeMap.set(hashIdent, ev);
        novos++;
      }
      atualizarEstado(estado, v, inicioISO, navioNorm, emFundeio, hash, agora);
      continue;
    }

    // 'novo' — transição válida na máquina de estados
    const ev = buildEvento(v, inicioISO, navioNorm, emFundeio, hash, hashIdent, agora);
    appendEvento(diaHoje, ev);
    hashesGravados.add(hash);
    identidadeMap.set(hashIdent, ev);
    novos++;
    atualizarEstado(estado, v, inicioISO, navioNorm, emFundeio, hash, agora);
  }

  // ── 7. salva estado_atual.json (se houve eventos novos ou remarcações) ────
  if (novos > 0 || remarcados > 0) {
    writeJson(ESTADO_PATH, estado);
  }


  // ── 8. gera métricas ──────────────────────────────────────────────────────
  gerarMetricas(estado, diaHoje, agora);
  gerarRelatoriosEmbarcacoes(estado, agora);

  // ── 9. limpeza de snapshots antigos ───────────────────────────────────────
  limparSnapshotsAntigos();

  // ── 10. resumo ─────────────────────────────────────────────────────────────
  console.log(`\n✓ ${allVessels.length} registros na pauta | ${vessels.length} já ocorridos processados`);
  console.log(`  ${novos} eventos novos | ${remarcados} remarcações | ${orfaos} órfãos ignorados | ${repetidos} repetições`);
  if (novos > 0 || remarcados > 0) {
    console.log(`  → data/eventos/${diaHoje.replace(/-/g, '/')}.jsonl`);
    console.log(`  → data/estado_atual.json`);
  }

  // Código de saída: 0 = houve mudança (workflow commita), 2 = sem mudança
  process.exit(novos > 0 || remarcados > 0 ? 0 : 2);
}

main().catch(e => { console.error(e); process.exit(1); });
