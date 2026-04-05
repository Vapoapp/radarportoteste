#!/usr/bin/env node
'use strict';

// ============================================================
// SENTINELA — scraper + ingestão GitHub-only
//
// Sem banco SQLite. Tudo em arquivos de texto.
//
// Arquivos gerados:
//   data/vessels.json                          ← pauta atual (index.html)
//   data/estado_atual.json                     ← foto por embarcação (IMO)
//   data/eventos/YYYY/MM/YYYY-MM-DD.jsonl      ← histórico permanente
//   data/snapshots/YYYY/MM/YYYY-MM-DD.jsonl.gz ← coletas brutas 30 dias
//   data/descartes/YYYY-MM-DD.jsonl            ← auditoria de eventos descartados
//
// Fluxo a cada execução:
//   1. Coleta os 4 portos → grava vessels.json
//   2. Carrega hashes já conhecidos do .jsonl do dia (deduplicação)
//   3. Carrega estado_atual.json
//   4. Para cada embarcação (em ordem cronológica):
//      a. Calcula hash_evento(porto+inicio+imo+tipo+de+para)
//         e   hash_identidade(imo+tipo+de+para) — sem horário
//      b. Se hash_evento já existe → repetição, ignora
//      c. Classifica: novo | remarcacao | orfao
//         - remarcação: substitui a linha INTEIRA no .jsonl (nunca append)
//         - orfao: mudança sem estado de embarcação dentro do porto → ignora
//      d. Evento novo → append no .jsonl + atualiza estado_atual
//   5. Salva estado_atual.json (se mudou)
//   6. Acrescenta snapshot compactado do dia
//   7. Limpa snapshots com mais de 30 dias
//   8. Retorna código 0 se houve eventos novos, 2 se não houve
//      (o workflow usa esse código para decidir se commita)
//
// Regras de máquina de estados:
//   entrada→entrada                              = remarcação (substitui)
//   entrada→mudança                              = novo
//   entrada→saída                                = novo
//   mudança→mudança (rota exata, Δt qualquer)   = remarcação (substitui)
//   mudança→mudança (rota parcial, Δt<2h)       = remarcação (substitui)
//   mudança→mudança (rota diferente ou Δt≥2h)   = novo
//   mudança→saída                                = novo
//   saída→saída                                  = remarcação (substitui)
//   saída→entrada                                = novo
//   saída→mudança                                = órfão (descarta)
//   (vazio)→mudança                              = órfão (descarta)
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
const DESCARTES_DIR    = path.join(DATA_DIR, 'descartes');

const PORTOS = [
  { id:'rio',     nome:'Rio de Janeiro', url:'https://silog.portosrio.gov.br/silog/pesquisa.aspx?WCI=relPrePautaSimplificado&Mv=Link&sqlCodDominio=1&sqlFLG_PUBLICO_EXTERNO=1' },
  { id:'niteroi', nome:'Niterói',        url:'https://silog.portosrio.gov.br/silog/pesquisa.aspx?WCI=relPrePautaSimplificado&Mv=Link&sqlCodDominio=2&sqlFLG_PUBLICO_EXTERNO=1' },
  { id:'itaguai', nome:'Itaguaí',        url:'https://silog.portosrio.gov.br/silog/pesquisa.aspx?WCI=relPrePautaSimplificado&Mv=Link&sqlCodDominio=3&sqlFLG_PUBLICO_EXTERNO=1' },
  { id:'angra',   nome:'Angra dos Reis', url:'https://silog.portosrio.gov.br/silog/pesquisa.aspx?WCI=relPrePautaSimplificado&Mv=Link&sqlCodDominio=4&sqlFLG_PUBLICO_EXTERNO=1' },
];

/**
 * Modo de validação de coerência de origem para mudança/saída.
 *
 *   'exata'   — só aceita se destino anterior == nova origem (normalizado).
 *               Mais rígido. Rejeita truncamentos e abreviações do SILOG.
 *
 *   'parcial' — aceita se um contém o outro (dest.includes(orig) ou vice-versa).
 *               Mais tolerante. Cobre "berço 1 norte" ↔ "berço 1".
 *               Loga aviso para cada aceitação parcial (rastreável).
 *
 * Ajuste conforme o comportamento observado na fonte (SILOG).
 * Se o SILOG for consistente nos nomes → use 'exata'.
 * Se truncar com frequência → use 'parcial'.
 */
const ORIGEM_COERENTE_MODO = 'parcial'; // 'exata' | 'parcial'

// ── UTILS ─────────────────────────────────────────────────────────────────────

function sha256(s) {
  return crypto.createHash('sha256').update(s, 'utf8').digest('hex');
}

/**
 * Normalização canônica de texto para comparações internas.
 * Remove acentos, colapsa espaços, converte para minúsculas.
 * Usada em todos os pontos onde strings do SILOG são comparadas entre si
 * (sobreposição de rota, coerência de origem, deduplicação).
 * Não deve ser usada para exibição — apenas para comparação.
 */
function normStr(s) {
  return (s == null ? '' : String(s))
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')   // remove acentos
    .replace(/[-\u2013\u2014\/\\|]/g, ' ') // hífens e separadores → espaço
    .replace(/[^\w\s]/g, '')            // remove pontuação restante
    .replace(/\s+/g, ' ')              // colapsa espaços múltiplos
    .trim()
    .toLowerCase();
}

/**
 * Retorna true se a string representa uma data ISO válida.
 * Protege cálculos de diffH contra Invalid Date contaminando decisões.
 */
function dataValida(d) {
  return typeof d === 'string' && !Number.isNaN(new Date(d).getTime());
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

/**
 * Registra evento descartado em data/descartes/YYYY-MM-DD.jsonl.
 *
 * Classes:
 *   parser_descartado        — linha ignorada no parseVessels antes de entrar no lote
 *   orfao_sem_historico      — mudança sem nenhum histórico anterior para o IMO
 *   orfao_estado_avancado    — evento chegou atrasado, estado já avançou (ex: SAÍDA→MUDANÇA)
 *   orfao_inesperado_X_para_Y — combinação de tipos não prevista (X e Y substituídos pelos tipos reais normalizados)
 *   remarcacao_update_falhou — classificou como remarcação, achou anterior, mas atualizarEvento() falhou
 *   remarcacao_sem_anterior  — classificou como remarcação mas remarcacaoMap não tinha anterior na janela
 *   inicio_invalido          — campo inicio não parseável, retornou epoch 1970 (descartado antes de processar)
 *
 * Não usa lock — append atômico por linha é suficiente para este arquivo de auditoria.
 * Falhas de escrita são silenciosas para não interromper o fluxo principal.
 */
function registrarDescarte(dia, classe, v, extras = {}) {
  try {
    mkdirp(DESCARTES_DIR);
    const p = path.join(DESCARTES_DIR, `${dia}.jsonl`);
    const registro = {
      timestamp_execucao: new Date().toISOString(),
      classe,
      navio:  v.navio  || null,
      imo:    v.imo    || null,
      tipo:   v.tipo   || null,
      inicio: v.inicio || null,
      porto:  v.porto  || null,
      de:     v.de     || null,
      para:   v.para   || null,
      ...extras,
    };
    fs.appendFileSync(p, JSON.stringify(registro) + '\n', 'utf8');
  } catch (e) {
    console.warn(`⚠ registrarDescarte: falha ao gravar (${e.message})`);
  }
}

/** em_fundeio: 1 se destino contém "Fundeio" e não é SAÍDA */
function calcFundeio(tipo, para) {
  if (/saída|saida/i.test(tipo)) return false;
  return /fundeio/i.test(para || '');
}

/**
 * Hash de identidade do evento — sem horário, sem porto.
 * Inclui de|para — usado como identificador estável do registro gravado
 * e como chave de deduplicação de contagem em relatórios.
 * NÃO é usado como chave de busca de remarcação (ver chaveRemarcacao).
 */
function hashIdentidade(v) {
  return sha256(`${v.imo}|${v.tipo}|${v.de}|${v.para}`);
}

/**
 * Chave de remarcação — apenas IMO + tipo normalizado (sem de/para).
 * Usada para localizar o evento anterior na detecção de remarcação,
 * mesmo quando de/para são corrigidos/alterados na nova versão da manobra.
 * Ex.: entrada→entrada do mesmo navio é remarcação mesmo se o berço foi corrigido.
 */
function chaveRemarcacao(imo, tipo) {
  return `${imo}|${normStr(tipo)}`;
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
 *   (vazio | SAÍDA)      + ENTRADA  → novo
 *   (vazio | SAÍDA)      + SAÍDA    → novo (sem histórico) / remarcação (pós-saída)
 *   (ENTRADA | MUDANÇA)  + MUDANÇA  → novo ou remarcação (ver regras abaixo)
 *   (ENTRADA | MUDANÇA)  + SAÍDA    → novo
 *
 * Remarcações:
 *   (ENTRADA | MUDANÇA)  + ENTRADA        → remarcação sempre
 *   (SAÍDA)              + SAÍDA          → remarcação sempre
 *   (MUDANÇA)            + MUDANÇA, rota exata (de E para iguais)
 *                                           → remarcação mesmo além de 2h (reagendamento)
 *   (MUDANÇA)            + MUDANÇA, rota parcial (de OU para igual), Δt < 2h
 *                                           → remarcação (correção rápida de berço/destino)
 *   (MUDANÇA)            + MUDANÇA, rota diferente ou Δt ≥ 2h sem rota exata
 *                                           → novo evento
 *
 * Órfãos (descartados):
 *   (vazio | SAÍDA)      + MUDANÇA  → órfão (mudança sem embarcação dentro do porto)
 *
 * Nota: validação de coerência de origem NÃO é feita aqui.
 * Ela serve apenas para desempatar duplicatas na deduplicação inicial (maisCoerente).
 * Um evento único com origem "incoerente" é aceito — o SILOG pode ter dados incompletos.
 *
 * @param {string|null} ultimoTipo   — último tipo gravado para este IMO
 * @param {string}      novoTipo     — tipo do evento sendo processado
 * @param {string|null} ultimoInicio — ISO do último evento gravado
 * @param {string}      novoInicio   — ISO do novo evento
 * @param {object}      [rota]       — { ultimoDe, ultimoPara, novoDe, novoPara }
 *                                     Necessário para MUDANÇA→MUDANÇA.
 * @returns {'novo'|'remarcacao'|'orfao'}
 */
function classificarEvento(ultimoTipo, novoTipo, ultimoInicio, novoInicio, rota = {}) {
  const ultimo = (ultimoTipo || '').toLowerCase();
  const novo   = novoTipo.toLowerCase();

  const isEntrada = s => /entrada/.test(s);
  const isSaida   = s => /sa[íi]da/.test(s);
  const isMudanca = s => /mudan[cç]a/.test(s);

  // sem histórico
  if (!ultimoTipo) {
    if (isEntrada(novo)) return 'novo';
    if (isSaida(novo))   return 'novo';
    if (isMudanca(novo)) return 'orfao'; // mudança sem embarcação dentro do porto
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

    if (isMudanca(novo)) {
      // MUDANÇA→MUDANÇA: duas regras distintas baseadas na similaridade de rota.
      //
      //   Regra 1 — rota EXATA (de E para iguais): remarcação sem limite de tempo,
      //             desde que o novo horário seja posterior (diffH >= 0).
      //             Cobre reagendamentos tardios da mesma operação (ex: 11:50 → 15:00).
      //
      //   Regra 2 — rota PARCIAL (de OU para igual): remarcação apenas dentro de 2h.
      //             Cobre correção de berço/destino na mesma janela curta.
      //             NÃO considera encadeamento (ultimoPara==novoDe) — isso é sequência real.
      //
      //   Fora dessas condições → evento novo.
      if (isMudanca(ultimo) && ultimoInicio && novoInicio) {
        if (!dataValida(ultimoInicio) || !dataValida(novoInicio)) {
          console.warn(`  ⚠ MUDANÇA→MUDANÇA: data inválida (ultimoInicio="${ultimoInicio}" novoInicio="${novoInicio}") — tratado como novo`);
        } else {
          const diffH = (new Date(novoInicio) - new Date(ultimoInicio)) / 3600000;
          if (diffH >= 0) {
            const { ultimoDe = '', ultimoPara = '', novoDe = '', novoPara = '' } = rota;
            const mesmaRotaExata = normStr(novoDe)   === normStr(ultimoDe)
                                && normStr(novoPara) === normStr(ultimoPara);
            const rotaParcial    = normStr(novoDe)   === normStr(ultimoDe)
                                || normStr(novoPara) === normStr(ultimoPara);

            if (mesmaRotaExata) {
              console.log(`  ℹ MUDANÇA→MUDANÇA remarcação: rota exata, horário adiado ${diffH.toFixed(1)}h (ant: "${ultimoDe}"→"${ultimoPara}" | novo: "${novoDe}"→"${novoPara}")`);
              return 'remarcacao';
            }
            if (diffH < 2 && rotaParcial) {
              console.log(`  ℹ MUDANÇA→MUDANÇA remarcação: rota parcial <2h (ant: "${ultimoDe}"→"${ultimoPara}" | novo: "${novoDe}"→"${novoPara}")`);
              return 'remarcacao';
            }
            if (diffH < 2) {
              console.log(`  ℹ MUDANÇA→MUDANÇA evento novo: rota diferente <2h (ant: "${ultimoDe}"→"${ultimoPara}" | novo: "${novoDe}"→"${novoPara}")`);
            }
          }
        }
      }
      return 'novo';
    }

    if (isSaida(novo)) return 'novo';
  }

  // fallback conservador: combinação desconhecida → órfão
  console.warn(`  ⚠ classificarEvento: combinação inesperada ultimo="${ultimoTipo}" novo="${novoTipo}" → tratado como órfão`);
  return 'orfao';
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
 * Se não conseguir lock dentro de LOCK_TIMEOUT_MS, loga aviso e ABORTA —
 * não grava sem lock. Filosofia consistente com appendEvento e atualizarEvento.
 * Retorna true se gravou, false se abortou.
 */
const LOCK_TIMEOUT_MS = 5000;
const LOCK_RETRY_MS   = 50;

function writeJson(filePath, data) {
  mkdirp(path.dirname(filePath));
  const tmp = filePath + '.tmp';

  const deadline = Date.now() + LOCK_TIMEOUT_MS;
  let lockAcquired = null;
  while (Date.now() < deadline) {
    lockAcquired = acquireLock(filePath);
    if (lockAcquired) break;
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, LOCK_RETRY_MS);
  }
  if (!lockAcquired) {
    console.warn(`⚠ writeJson: timeout ao aguardar lock em ${filePath}, arquivo NÃO gravado (sem fallback inseguro)`);
    return false;
  }
  try {
    fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
    fs.renameSync(tmp, filePath);  // atômico no mesmo filesystem
    return true;
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
 * Carrega o estado do .jsonl do dia e dos N dias anteriores.
 * Retorna:
 *   hashesGravados — Set com todos os hash_evento do DIA ATUAL (evita duplicata de linha)
 *   remarcacaoMap  — Map<chaveRemarcacao(imo,tipo), evento> com o evento mais recente
 *                    por IMO+tipo na janela carregada. Usado para detectar remarcações
 *                    mesmo quando de/para muda ou a remarcação cruza virada de dia.
 *
 * diasAnteriores: quantos dias anteriores incluir no remarcacaoMap (padrão 3).
 * Hashes só são registrados para o dia atual — dias anteriores contribuem apenas
 * ao mapa de remarcação, para não bloquear re-gravação de hash em dia diferente.
 */
const JANELA_REMARCACAO_DIAS = 3;

function carregarEstadoDoDia(dia, diasAnteriores = JANELA_REMARCACAO_DIAS) {
  const hashesGravados = new Set();
  const remarcacaoMap  = new Map(); // chaveRemarcacao(imo,tipo) → evento mais recente

  function processarArquivo(p, registrarHashes) {
    if (!fs.existsSync(p)) return;
    const linhas = fs.readFileSync(p, 'utf8').split('\n').filter(Boolean);
    for (const linha of linhas) {
      try {
        const ev = JSON.parse(linha);
        if (registrarHashes && ev.hash_evento) hashesGravados.add(ev.hash_evento);
        if (ev.imo && ev.tipo_evento) {
          const chave    = chaveRemarcacao(ev.imo, ev.tipo_evento);
          const anterior = remarcacaoMap.get(chave);
          if (!anterior || ev.inicio_evento > anterior.inicio_evento) {
            remarcacaoMap.set(chave, ev);
          }
        }
      } catch {}
    }
  }

  // dia atual — registra hashes para deduplicação
  processarArquivo(eventosPath(dia), true);

  // dias anteriores — apenas para remarcacaoMap (cobre virada de dia e correções tardias)
  const diaBaseMs = new Date(dia + 'T12:00:00Z').getTime();
  for (let i = 1; i <= diasAnteriores; i++) {
    const diaAnt = diaBrasilia(new Date(diaBaseMs - i * 86400000).toISOString());
    processarArquivo(eventosPath(diaAnt), false);
  }

  return { hashesGravados, remarcacaoMap };
}

/**
 * Acrescenta uma linha ao .jsonl de eventos com lock exclusivo.
 * Cria o arquivo e diretórios se necessário.
 * Se não conseguir lock dentro do timeout, loga aviso e ABORTA —
 * não grava sem lock. Filosofia igual ao atualizarEvento: corretude
 * antes de completude.
 * Retorna true se gravou, false se abortou.
 */
function appendEvento(dia, obj) {
  const p = eventosPath(dia);
  mkdirp(path.dirname(p));

  const deadline = Date.now() + LOCK_TIMEOUT_MS;
  let lockAcquired = null;
  while (Date.now() < deadline) {
    lockAcquired = acquireLock(p);
    if (lockAcquired) break;
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, LOCK_RETRY_MS);
  }
  if (!lockAcquired) {
    console.warn(`⚠ appendEvento: timeout ao aguardar lock em ${p}, evento NÃO gravado (sem fallback inseguro)`);
    return false;
  }
  try {
    fs.appendFileSync(p, JSON.stringify(obj) + '\n', 'utf8');
    return true;
  } finally {
    releaseLock(lockAcquired);
  }
}

/**
 * Atualiza um evento já gravado no .jsonl com os dados mais recentes.
 * Reescreve a linha inteira, identificando-a pela chave_remarcacao (imo|tipo)
 * + inicio_evento anterior. Funciona mesmo quando de/para mudam na remarcação.
 * Nunca faz append como fallback — se não encontrar a linha, loga e retorna false.
 *
 * Percorre a mesma janela de dias que carregarEstadoDoDia (JANELA_REMARCACAO_DIAS),
 * garantindo que se o remarcacaoMap encontrou o anterior, atualizarEvento consegue
 * substituí-lo independentemente de em qual dia ele foi gravado.
 *
 * @param {string} diaInicio   — "YYYY-MM-DD" do evento anterior (ponto de partida da busca)
 * @param {string} chaveRemarc — chaveRemarcacao(imo, tipo)
 * @param {string} inicioAnt   — ISO do evento a substituir
 * @param {object} novoEvento  — objeto completo do novo evento
 * @returns {boolean}          — true se atualizou, false se não encontrou/falhou
 */
function atualizarEvento(diaInicio, chaveRemarc, inicioAnt, novoEvento) {
  // Gera lista de dias a tentar: diaInicio e até JANELA_REMARCACAO_DIAS anteriores
  const diasTentar = [diaInicio];
  const baseMs = new Date(diaInicio + 'T12:00:00Z').getTime();
  for (let i = 1; i <= JANELA_REMARCACAO_DIAS; i++) {
    diasTentar.push(diaBrasilia(new Date(baseMs - i * 86400000).toISOString()));
  }
  // Remove duplicatas (caso diaInicio já seja um dos calculados)
  const diasUnicos = [...new Set(diasTentar)];

  for (const dia of diasUnicos) {
    const p = eventosPath(dia);
    if (!fs.existsSync(p)) continue;

    // Espera lock com o mesmo timeout usado em appendEvento e writeJson —
    // sem retry aqui seria a única função de IO do sistema sem tolerância a contenção,
    // inflando artificialmente a contagem de remarcacao_update_falhou.
    const deadline = Date.now() + LOCK_TIMEOUT_MS;
    let lockAcquired = null;
    while (Date.now() < deadline) {
      lockAcquired = acquireLock(p);
      if (lockAcquired) break;
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, LOCK_RETRY_MS);
    }
    if (!lockAcquired) {
      console.warn(`⚠ atualizarEvento: timeout ao aguardar lock em ${p}, atualização ignorada (sem fallback)`);
      return false;
    }
    let lockReleased = false;
    try {
      const linhas = fs.readFileSync(p, 'utf8').split('\n').filter(Boolean);
      let atualizou = false;
      const atualizadas = linhas.map(l => {
        try {
          const ev = JSON.parse(l);
          if (!atualizou
              && chaveRemarcacao(ev.imo, ev.tipo_evento) === chaveRemarc
              && ev.inicio_evento === inicioAnt) {
            atualizou = true;
            return JSON.stringify(novoEvento);
          }
          return JSON.stringify(ev);
        } catch { return l; }
      });

      if (!atualizou) {
        releaseLock(lockAcquired);
        lockReleased = true;
        continue; // tenta o próximo dia da janela
      }

      // escrita atômica: .tmp → rename
      const tmp = p + '.tmp';
      fs.writeFileSync(tmp, atualizadas.join('\n') + '\n', 'utf8');
      fs.renameSync(tmp, p);
      return true;
    } finally {
      if (!lockReleased) releaseLock(lockAcquired);
    }
  }

  console.warn(`⚠ atualizarEvento: linha não encontrada para chave=${chaveRemarc} inicio=${inicioAnt} (janela de ${diasUnicos.length} dias), atualização ignorada`);
  return false;
}

/**
 * Acrescenta linha de snapshot compactado no .jsonl.gz do dia com lock exclusivo.
 * Cada linha é um JSON.stringify comprimido em gzip+base64.
 * Sem lock dentro do timeout → loga aviso e não grava (sem fallback inseguro).
 * Retorna true se gravou, false se abortou.
 */
function appendSnapshot(dia, snapshotObj) {
  const p = snapshotPath(dia);
  mkdirp(path.dirname(p));
  const linha = zlib.gzipSync(JSON.stringify(snapshotObj)).toString('base64') + '\n';

  const deadline = Date.now() + LOCK_TIMEOUT_MS;
  let lockAcquired = null;
  while (Date.now() < deadline) {
    lockAcquired = acquireLock(p);
    if (lockAcquired) break;
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, LOCK_RETRY_MS);
  }
  if (!lockAcquired) {
    console.warn(`⚠ appendSnapshot: timeout ao aguardar lock em ${p}, snapshot NÃO gravado (sem fallback inseguro)`);
    return false;
  }
  try {
    fs.appendFileSync(p, linha, 'utf8');
    return true;
  } finally {
    releaseLock(lockAcquired);
  }
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

function parseVessels(html, portoNome, diaHoje) {
  const vessels = [];
  let   descartados = 0;
  const rowRe   = /<tr([^>]*)>([\s\S]*?)<\/tr>/gi;
  let row;
  while ((row = rowRe.exec(html)) !== null) {
    const attrs   = row[1] || '';
    const content = row[2] || '';
    if ((attrs + content).toLowerCase().includes('cancelado')) {
      // linha de cancelamento — descarte esperado, não auditável (sem dados de navio)
      continue;
    }
    if (content.toLowerCase().includes('colspan')) {
      // linha estrutural (cabeçalho, separador) — descarte esperado
      continue;
    }
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
      } else {
        // linha com células suficientes mas navio vazio — incomum, audita
        descartados++;
        registrarDescarte(diaHoje, 'parser_descartado', {
          porto: portoNome, inicio: cells[0], imo: cells[1],
          navio: navio || '', tipo: cells[3], de: cells[4], para: cells[5],
        }, { motivo_parser: 'navio_vazio', cells_length: cells.length });
      }
    } else if (cells.length > 0) {
      // linha com dados mas células insuficientes — candidata a CARLOS DRUMMOND / SABLE
      descartados++;
      registrarDescarte(diaHoje, 'parser_descartado', {
        porto: portoNome, inicio: cells[0] || '', imo: cells[1] || '',
        navio: cells[2] || '', tipo: cells[3] || '', de: cells[4] || '', para: cells[5] || '',
      }, {
        motivo_parser: 'cells_lt_7',
        cells_length:  cells.length,
        cells_raw:     cells,
        content_snippet: content.slice(0, 300),
      });
    }
  }
  return { vessels, descartados };
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
  // Usa hash_evento como chave — único por registro gravado. Como remarcações
  // já foram consolidadas in-place no .jsonl, cada linha representa exatamente
  // um evento lógico real: não há risco de inflar nem de subcontar.
  let entradas = 0, saidas = 0, mudancas = 0;
  const pEventos = eventosPath(diaHoje);
  if (fs.existsSync(pEventos)) {
    const vistos = new Set();
    const linhas = fs.readFileSync(pEventos, 'utf8').split('\n').filter(Boolean);
    for (const linha of linhas) {
      try {
        const e     = JSON.parse(linha);
        const chave = e.hash_evento || linha;
        if (vistos.has(chave)) continue;
        vistos.add(chave);
        if (/entrada/i.test(e.tipo_evento))              entradas++;
        else if (/saída|saida/i.test(e.tipo_evento))     saidas++;
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

  const contar = (arr, re) => {
    // Usa hash_evento como chave — ele é único por registro gravado no .jsonl
    // (inclui porto + horário + imo + tipo + de + para).
    // Isso evita subcontagem de eventos reais com mesma rota em dias diferentes
    // (problema de hash_identidade+dia) e também não infla por remarcações
    // (que já foram consolidadas in-place no .jsonl antes de chegar aqui).
    const vistos = new Set();
    let n = 0;
    for (const e of arr) {
      const chave = e.hash_evento || JSON.stringify(e);
      if (vistos.has(chave)) continue;
      vistos.add(chave);
      if (re.test(e.tipo_evento)) n++;
    }
    return n;
  };
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
    // Usa hash_evento como chave de deduplicação — único por linha gravada no .jsonl.
    // Cada janela já é disjunta (ev30=0-29d, ev180=30-179d, ev365=180-364d), portanto
    // a contagem cumulativa é feita com um Set unificado para evitar qualquer
    // dupla contagem caso um hash apareça em mais de uma fatia (edge case de re-leitura).
    const contarUnicosSet = (arrs) => {
      const s = new Set();
      for (const arr of arrs)
        for (const e of arr) s.add(e.hash_evento || JSON.stringify(e));
      return s.size;
    };
    const man30  = contarUnicosSet([e30]);
    const man180 = contarUnicosSet([e30, e180]);
    const man365 = contarUnicosSet([e30, e180, e365]);

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
  let   parseDescartados = 0;

  for (const porto of PORTOS) {
    try {
      process.stdout.write(`Coletando ${porto.nome}... `);
      const html                        = await fetchWithRetry(porto.url);
      const { vessels, descartados }    = parseVessels(html, porto.nome, diaHoje);
      allVessels.push(...vessels);
      parseDescartados += descartados;
      portStatus[porto.id] = { ok: true, count: vessels.length };
      console.log(`OK — ${vessels.length} registros${descartados ? ` (${descartados} descartados pelo parser)` : ''}`);
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

  // ── 4. carrega estado do dia (hashes gravados + mapa de remarcação) ────────
  const { hashesGravados, remarcacaoMap } = carregarEstadoDoDia(diaHoje);

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
  const vesselsGravado  = writeJson(VESSELS_PATH, jsonData);
  const snapshotGravado = appendSnapshot(diaHoje, jsonData);
  if (!vesselsGravado)  console.warn('⚠ vessels.json não atualizado neste ciclo (lock timeout)');
  if (!snapshotGravado) console.warn('⚠ snapshot não gravado neste ciclo (lock timeout)');

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

  // ── 6b. deduplica eventos com mesmo IMO + horário ────────────────────────────
  // A fonte (SILOG) às vezes publica dois eventos no mesmo IMO+horário:
  //   Caso A: mesmo tipo  → erro de entrada (de/para diferentes). Desempata por coerência.
  //   Caso B: tipos diferentes (ex: ENTRADA vs MUDANÇA) → conflito operacional.
  //           A MUDANÇA só vence se a origem dela bater com o último destino
  //           visto na sequência do próprio lote (não o estado persistido em disco).
  //           Caso contrário a ENTRADA é mantida (comportamento conservador).
  //
  // Chave de agrupamento: imo|inicio (sem tipo) — para capturar o Caso B.
  //
  // Fonte de verdade para coerência: estadoSequencialPorIMO, construído em memória
  // durante a varredura cronológica do lote. Isso garante que o evento de 09:40
  // já terá atualizado o destino antes de avaliarmos o conflito de 17:00.
  //
  // Fluxo:
  //   1ª passagem: agrupa por imo|inicio, resolve conflitos dentro do par.
  //               Usa estadoSequencialPorIMO atualizado evento a evento.
  //   2ª etapa:   re-ordena cronologicamente.
  {
    // normalização: usa normStr() — função canônica centralizada nas utils

    function bate(dest, orig) {
      if (!dest || !orig) return false;
      if (dest === orig) return true;
      if (ORIGEM_COERENTE_MODO === 'parcial') return dest.includes(orig) || orig.includes(dest);
      return false;
    }

    const isMudanca = s => /mudan[cç]a/i.test(s || '');
    const isEntrada = s => /entrada/i.test(s || '');

    // Estado sequencial em memória: { [imo]: { destino_atual } }
    // Inicializa com o estado persistido em disco como ponto de partida,
    // e vai sendo atualizado a cada evento não-conflitante processado na ordem.
    // Isso garante que ao avaliar um conflito de 17:00, o destino do evento
    // de 09:40 (do mesmo lote) já está refletido aqui.
    const estadoSeq = {};
    for (const [imo, est] of Object.entries(estado)) {
      estadoSeq[imo] = { destino_atual: est.destino_atual || null };
    }

    function ultDestinoSeq(imo) {
      return estadoSeq[imo]?.destino_atual || null;
    }

    function atualizarSeq(v) {
      if (!estadoSeq[v.imo]) estadoSeq[v.imo] = { destino_atual: null };
      // SAÍDA zera o destino: impede que destino antigo contamine
      // decisões de coerência de eventos futuros do mesmo IMO.
      if (/sa[íi]da/i.test(v.tipo || '')) {
        estadoSeq[v.imo].destino_atual = null;
        return;
      }
      if (v.para) estadoSeq[v.imo].destino_atual = v.para;
    }

    /**
     * Desempate entre dois eventos do mesmo tipo no mesmo IMO+horário.
     * Retorna true se candidato é preferível a atual.
     *
     * Prioridade de critérios:
     *   1. Candidato coerente com destino anterior e atual não → candidato vence.
     *   2. Ambos coerentes → prefere o que tem mais campos preenchidos (de+para).
     *   3. Ambos incoerentes → prefere o que tem de/para preenchidos vs vazio.
     *   4. Empate completo → mantém atual (conservador).
     */
    function maisCoerente(candidato, atual) {
      const d     = normStr(ultDestinoSeq(candidato.imo));
      const cOrig = normStr(candidato.de);
      const aOrig = normStr(atual.de);
      const candCoerente  = bate(d, cOrig);
      const atualCoerente = bate(d, aOrig);

      // critério 1: só um é coerente
      if (candCoerente && !atualCoerente) return true;
      if (!candCoerente && atualCoerente) return false;

      // critério 2 e 3: desempate por completude de campos (de + para preenchidos)
      const completude = v => (v.de ? 1 : 0) + (v.para ? 1 : 0);
      const cComp = completude(candidato);
      const aComp = completude(atual);
      if (cComp !== aComp) return cComp > aComp;

      // critério 4: empate → mantém atual
      return false;
    }

    /**
     * Conflito ENTRADA vs MUDANÇA no mesmo IMO+horário.
     * Consulta estadoSeq — que já reflete os eventos anteriores do lote —
     * para decidir se a origem da MUDANÇA é coerente.
     * Retorna o evento vencedor.
     */
    function resolverConflitoEntradaMudanca(entrada, mudanca) {
      const ultDest = ultDestinoSeq(mudanca.imo);
      if (!ultDest) {
        console.log(`  ✂ conflito ENTRADA×MUDANÇA: sem destino anterior para IMO ${entrada.imo} — mantida ENTRADA (conservador)`);
        return entrada;
      }
      const d    = normStr(ultDest);
      const orig = normStr(mudanca.de);
      if (bate(d, orig)) {
        console.log(`  ✂ conflito ENTRADA×MUDANÇA: MUDANÇA vence por coerência (de: "${mudanca.de}" bate com destino anterior: "${ultDest}") — ENTRADA descartada`);
        return mudanca;
      }
      console.log(`  ✂ conflito ENTRADA×MUDANÇA: MUDANÇA não coerente (de: "${mudanca.de}" ≠ destino anterior: "${ultDest}") — mantida ENTRADA`);
      return entrada;
    }

    const seen   = new Map(); // key → índice no result
    const result = [];

    for (const v of vessels) {
      const key = `${v.imo}|${v.inicio}`;
      if (seen.has(key)) {
        const prevIdx = seen.get(key);
        const atual   = result[prevIdx];

        // Desfaz a atualização sequencial do primeiro evento (atual),
        // que foi registrada prematuramente quando ele chegou sem conflito.
        // Agora que há disputa, o estado correto é o que existia ANTES dele.
        // Restauramos usando o snapshot guardado no momento em que atual entrou.
        if (atual._snapSeqAntes !== undefined) {
          estadoSeq[atual.imo] = atual._snapSeqAntes
            ? { destino_atual: atual._snapSeqAntes }
            : { destino_atual: null };
        }

        let vencedor;
        // Caso B: conflito ENTRADA vs MUDANÇA (em qualquer ordem de chegada)
        if ((isEntrada(atual.tipo) && isMudanca(v.tipo)) ||
            (isMudanca(atual.tipo) && isEntrada(v.tipo))) {
          const entrada = isEntrada(atual.tipo) ? atual : v;
          const mudanca = isMudanca(atual.tipo) ? atual : v;
          vencedor = resolverConflitoEntradaMudanca(entrada, mudanca);
        // Caso A: mesmo tipo — desempata por coerência de origem
        } else if (maisCoerente(v, atual)) {
          console.log(`  ✂ duplicata resolvida por coerência: ${v.navio} — ${v.tipo} ${v.inicio} (de: "${atual.de}" → preferido: "${v.de}")`);
          vencedor = v;
        } else {
          console.log(`  ✂ duplicata de horário removida: ${v.navio} — ${v.tipo} ${v.inicio} (de: "${v.de}" descartado, mantido: "${atual.de}")`);
          vencedor = atual;
        }
        // Preserva o snapshot antes de gravar o vencedor em result,
        // para que um eventual 3º evento no mesmo imo|inicio possa restaurar corretamente.
        if (atual._snapSeqAntes !== undefined && vencedor._snapSeqAntes === undefined) {
          vencedor._snapSeqAntes = atual._snapSeqAntes;
        }
        result[prevIdx] = vencedor;
        // agora sim: avança com o vencedor real da disputa
        atualizarSeq(vencedor);
      } else {
        // Guarda snapshot do destino atual ANTES de avançar,
        // para poder restaurar caso chegue um segundo evento disputando este horário.
        const snapAntes = ultDestinoSeq(v.imo);
        v._snapSeqAntes = snapAntes; // campo temporário, removido abaixo
        seen.set(key, result.length);
        result.push(v);
        // avança provisoriamente — pode ser desfeito se houver conflito
        atualizarSeq(v);
      }
    }

    // Remove campos temporários de controle antes de passar para a máquina de estados
    for (const ev of result) delete ev._snapSeqAntes;

    // re-ordena cronologicamente após substituições
    result.sort((a, b) => {
      const ta = parseInicio(a.inicio);
      const tb = parseInicio(b.inicio);
      if (ta !== tb) return ta.localeCompare(tb);
      if (a.imo !== b.imo) return String(a.imo).localeCompare(String(b.imo));
      return String(a.tipo).localeCompare(String(b.tipo));
    });
    vessels.length = 0;
    vessels.push(...result);
  }

  // ── 7. processa cada embarcação (máquina de estados) ─────────────────────
  let novos = 0, remarcados = 0, orfaos = 0, repetidos = 0;
  let remarcUpdateFalhou = 0, remarcSemAnterior = 0, iniciosInvalidos = 0;

  for (const v of vessels) {
    const inicioISO  = parseInicio(v.inicio);

    // Guard: data inválida retorna 1970 em parseInicio.
    // Descartar antes de qualquer processamento — evita contaminar estado_atual.json
    // com timestamp absurdo e influenciar classificações futuras silenciosamente.
    if (!dataValida(inicioISO) || inicioISO.startsWith('1970-01-01')) {
      console.warn(`  ✗ inicio_invalido: ${v.navio} (IMO ${v.imo}) — "${v.inicio}" não é uma data válida, evento descartado`);
      registrarDescarte(diaHoje, 'inicio_invalido', v, {
        inicio_raw: v.inicio,
        motivo: 'parseInicio_retornou_epoch',
      });
      iniciosInvalidos++;
      continue;
    }
    const navioNorm  = normalizeNavio(v.navio);
    const emFundeio  = calcFundeio(v.tipo, v.para);
    const estadoIMO  = estado[v.imo];

    // ultimoTipo: prefere estado_atual.json (mais recente e completo).
    // Fallback para remarcacaoMap quando estado está vazio — cobre o caso de
    // início do zero ou bootstrap, onde estado_atual.json foi deletado mas os
    // .jsonl de dias anteriores ainda existem e foram carregados no mapa.
    // Sem esse fallback, todas as MUDANÇAs de navios conhecidos virariam órfãos
    // desnecessariamente só porque o estado foi resetado.
    let ultimoTipo   = estadoIMO?.ultimo_tipo_evento   || null;
    let ultimoInicio = estadoIMO?.ultima_movimentacao_em || null;
    let ultimoDe     = estadoIMO?.origem_atual  || '';
    let ultimoPara   = estadoIMO?.destino_atual || '';

    if (!ultimoTipo) {
      // tenta recuperar do remarcacaoMap — itera pelas chaves deste IMO
      // (pode ter ENTRADA, MUDANÇA ou SAÍDA como chave separada)
      for (const tipo of ['ENTRADA', 'MUDANÇA', 'SAÍDA', 'SAIDA']) {
        const ant = remarcacaoMap.get(chaveRemarcacao(v.imo, tipo));
        if (ant && (!ultimoInicio || ant.inicio_evento > ultimoInicio)) {
          ultimoTipo   = ant.tipo_evento;
          ultimoInicio = ant.inicio_evento;
          ultimoDe     = ant.origem   || '';
          ultimoPara   = ant.destino  || '';
        }
      }
      if (ultimoTipo) {
        console.log(`  ℹ estado recuperado do histórico: ${v.navio} (IMO ${v.imo}) ultimo_tipo=${ultimoTipo} em ${ultimoInicio}`);
      }
    }

    const hash      = hashEvento(v);
    const hashIdent = hashIdentidade(v);

    // proteção contra jobs paralelos: linha idêntica já gravada → ignora
    if (hashesGravados.has(hash)) {
      repetidos++;
      continue;
    }

    const classificacao = classificarEvento(
      ultimoTipo, v.tipo,
      ultimoInicio, inicioISO,
      {
        ultimoDe,
        ultimoPara,
        novoDe:     v.de   || '',
        novoPara:   v.para || '',
      }
    );

    if (classificacao === 'orfao') {
      orfaos++;

      // ── Separação de subclasse de órfão ────────────────────────────────────
      // Permite distinguir em auditoria:
      //   sem_historico    — nenhum estado anterior para o IMO (bootstrap, navio novo)
      //   estado_avancado  — evento chegou atrasado, máquina já avançou além dele
      //                      (caso típico: SILOG publica MUDANÇA retroativa após SAÍDA já gravada)
      //   inesperado_X_Y   — combinação de tipos não mapeada → revela novos padrões do SILOG
      const isSaidaT   = s => /sa[íi]da/i.test(s || '');
      const isMudancaT = s => /mudan[cç]a/i.test(s || '');

      let subclasse;
      if (!ultimoTipo) {
        subclasse = 'sem_historico';
      } else if (isSaidaT(ultimoTipo) && isMudancaT(v.tipo)) {
        subclasse = 'estado_avancado';
      } else {
        subclasse = `inesperado_${normStr(ultimoTipo)}_para_${normStr(v.tipo)}`;
      }

      console.log(`  ⚠ órfão [${subclasse}]: ${v.navio} (IMO ${v.imo}) — ${v.tipo} ${v.inicio}`);

      registrarDescarte(diaHoje, 'orfao_' + subclasse, v, {
        ultimo_tipo:             ultimoTipo,
        ultima_movimentacao_em:  estadoIMO?.ultima_movimentacao_em || null,
        destino_atual:           estadoIMO?.destino_atual          || null,
        origem_atual:            estadoIMO?.origem_atual           || null,
      });

      continue;
    }

    if (classificacao === 'remarcacao') {
      const chaveRemarc = chaveRemarcacao(v.imo, v.tipo);
      const anterior    = remarcacaoMap.get(chaveRemarc);

      if (anterior) {
        // Verifica se é repetição pura: horário, rota E campos relevantes idênticos.
        // Inclui agente — se o SILOG corrigir o agente mantendo mesmo horário e rota,
        // isso é uma atualização real e não deve ser descartada como repetição.
        // Compara campos normalizados para tolerar variações textuais do SILOG
        // (diferenças de acento, hífen, capitalização no mesmo conteúdo).
        // horário e porto são comparados literalmente — são valores estruturados.
        const mesmoCampos = inicioISO          === anterior.inicio_evento
                         && normStr(v.de)      === normStr(anterior.origem)
                         && normStr(v.para)    === normStr(anterior.destino)
                         && normStr(v.agente)  === normStr(anterior.agente)
                         && normStr(v.navio)   === normStr(anterior.navio)
                         && v.porto            === anterior.porto;
        if (mesmoCampos) {
          repetidos++;
          continue;
        }
        const novoEv        = buildEvento(v, inicioISO, navioNorm, emFundeio, hash, hashIdent, agora);
        novoEv.remarcado    = true;
        novoEv.atualizado_em = agora;
        // dia do evento anterior pode ser diferente do dia atual (virada de dia)
        const diaEvAnterior = diaBrasilia(anterior.inicio_evento);
        const ok = atualizarEvento(diaEvAnterior, chaveRemarc, anterior.inicio_evento, novoEv);
        if (ok) {
          // remove hash antigo do set — o evento foi substituído no .jsonl
          // e o hash antigo não representa mais nenhuma linha válida
          if (anterior.hash_evento) hashesGravados.delete(anterior.hash_evento);
          remarcacaoMap.set(chaveRemarc, novoEv);
          hashesGravados.add(hash);
          remarcados++;
          const diffs = [];
          if (inicioISO      !== anterior.inicio_evento) diffs.push(`horário: ${anterior.inicio_evento}→${inicioISO}`);
          if (v.de           !== anterior.origem)         diffs.push(`de: "${anterior.origem}"→"${v.de}"`);
          if (v.para         !== anterior.destino)        diffs.push(`para: "${anterior.destino}"→"${v.para}"`);
          if (v.agente       !== anterior.agente)         diffs.push(`agente: "${anterior.agente}"→"${v.agente}"`);
          if (v.navio        !== anterior.navio)          diffs.push(`navio: "${anterior.navio}"→"${v.navio}"`);
          if (v.porto        !== anterior.porto)          diffs.push(`porto: "${anterior.porto}"→"${v.porto}"`);
          console.log(`  ↺ remarcado: ${v.navio} — ${v.tipo} [${diffs.join(' | ')}]`);
          // atualiza estado apenas quando histórico foi consolidado com sucesso
          atualizarEstado(estado, v, inicioISO, navioNorm, emFundeio, hash, agora);
        } else {
          // Classe 3: remarcação classificada corretamente, anterior encontrado no mapa,
          // mas atualizarEvento() não achou a linha no arquivo (ou falha de lock/IO).
          // O evento some silenciosamente do histórico — esse é o caso mais traiçoeiro.
          console.warn(`  ✗ remarcacao_update_falhou: ${v.navio} (IMO ${v.imo}) — ${v.tipo} ${inicioISO} chave=${chaveRemarc} anterior=${anterior.inicio_evento}`);
          remarcUpdateFalhou++;
          registrarDescarte(diaHoje, 'remarcacao_update_falhou', v, {
            chave_remarcacao:      chaveRemarc,
            anterior_encontrado:   true,
            anterior_inicio_evento: anterior.inicio_evento,
            anterior_hash_evento:  anterior.hash_evento || null,
            arquivo_alvo:          eventosPath(diaBrasilia(anterior.inicio_evento)),
          });
        }
        // se não ok: atualizarEvento já logou aviso; não faz append, não avança estado
      } else {
        // Classe 4: classificação foi 'remarcacao' mas remarcacaoMap não tem anterior
        // na janela carregada (bootstrap, histórico insuficiente, correção muito tardia).
        // Comportamento conservador: não faz append — criar linha nova quebraria a
        // regra de "remarcação substitui, nunca duplica".
        remarcSemAnterior++;
        console.warn(`  ⚠ remarcacao_sem_anterior: ${v.navio} (IMO ${v.imo}) — ${v.tipo} ${inicioISO} chave=${chaveRemarc} (janela de ${JANELA_REMARCACAO_DIAS + 1} dias)`);
        registrarDescarte(diaHoje, 'remarcacao_sem_anterior', v, {
          chave_remarcacao:    chaveRemarc,
          anterior_encontrado: false,
          janela_dias:         JANELA_REMARCACAO_DIAS + 1,
        });
      }
      continue;
    }

    // 'novo' — transição válida na máquina de estados
    const ev = buildEvento(v, inicioISO, navioNorm, emFundeio, hash, hashIdent, agora);
    const gravou = appendEvento(diaHoje, ev);
    if (gravou) {
      hashesGravados.add(hash);
      remarcacaoMap.set(chaveRemarcacao(v.imo, v.tipo), ev);
      novos++;
      atualizarEstado(estado, v, inicioISO, navioNorm, emFundeio, hash, agora);
    }
  }

  // ── 7. salva estado_atual.json (se houve eventos novos ou remarcações) ────
  if (novos > 0 || remarcados > 0) {
    const estadoGravado = writeJson(ESTADO_PATH, estado);
    if (!estadoGravado) console.warn('⚠ estado_atual.json não atualizado neste ciclo (lock timeout) — estado em memória pode estar à frente do disco');
  }


  // ── 8. gera métricas ──────────────────────────────────────────────────────
  gerarMetricas(estado, diaHoje, agora);
  gerarRelatoriosEmbarcacoes(estado, agora);

  // ── 9. limpeza de snapshots antigos ───────────────────────────────────────
  limparSnapshotsAntigos();

  // ── 10. resumo ─────────────────────────────────────────────────────────────
  console.log(`\n✓ ${allVessels.length} registros na pauta | ${vessels.length} já ocorridos processados`);
  console.log(`  ${novos} eventos novos | ${remarcados} remarcações | ${repetidos} repetições`);
  console.log(`  ${orfaos} órfãos (sem histórico / estado avançado / inesperado) | ${remarcSemAnterior} remarcações sem anterior | ${remarcUpdateFalhou} remarcações update falhou`);
  if (iniciosInvalidos > 0) {
    console.log(`  ${iniciosInvalidos} datas inválidas descartadas (inicio_invalido)`);
  }
  const totalDescartesAuditados = parseDescartados + orfaos + remarcSemAnterior + remarcUpdateFalhou + iniciosInvalidos;
  if (totalDescartesAuditados > 0) {
    console.log(`  ${totalDescartesAuditados} descartes auditados → data/descartes/${diaHoje}.jsonl`);
    if (parseDescartados > 0) {
      console.log(`    - ${parseDescartados} descartados pelo parser`);
    }
  }
  if (novos > 0 || remarcados > 0) {
    console.log(`  → data/eventos/${diaHoje.replace(/-/g, '/')}.jsonl`);
    console.log(`  → data/estado_atual.json`);
  }

  // Código de saída: 0 = houve mudança (workflow commita), 2 = sem mudança
  process.exit(novos > 0 || remarcados > 0 ? 0 : 2);
}

main().catch(e => { console.error(e); process.exit(1); });
