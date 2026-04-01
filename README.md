# Sentinela — Portos do Rio

Sistema de monitoramento e inteligência operacional dos portos do Rio de Janeiro, Niterói, Itaguaí e Angra dos Reis.

## Estrutura do repositório

```
sentinela/
├── scrape.js                           ← scraper + ingestão + métricas + ranking
├── README.md
├── .github/workflows/
│   └── scrape.yml                      ← cron a cada 2 horas
│
├── radarportuario/
│   └── index.html                      ← radar operacional (radar.html)
│
├── relatorioembarcacao/
│   └── index.html                      ← relatório por IMO (embarcacao.html)
│
└── data/                               ← gerado automaticamente pelo scraper
    ├── vessels.json                    ← pauta atual (lida pelo Sentinela/index.html)
    ├── estado_atual.json               ← foto atual de cada embarcação por IMO
    │
    ├── metricas/
    │   ├── radar.json                  ← contadores do dia (fundeio, entradas, saídas)
    │   ├── fundeio.json                ← lista de embarcações em fundeio agora
    │   └── ranking-atividade.json      ← Top 10 por manobras (30d / 180d / 365d)
    │
    ├── embarcacoes/
    │   └── {IMO}.json                  ← relatório completo por embarcação
    │
    ├── eventos/
    │   └── YYYY/MM/YYYY-MM-DD.jsonl    ← histórico permanente (append-only)
    │
    └── snapshots/
        └── YYYY/MM/YYYY-MM-DD.jsonl.gz ← coletas brutas (retenção 30 dias)
```

## Fluxo do scraper

A cada execução (`scrape.js`):

1. Coleta os 4 portos → grava `data/vessels.json`
2. Acrescenta snapshot compactado do dia em `snapshots/`
3. Carrega hashes do `.jsonl` do dia (deduplicação)
4. Para cada embarcação:
   - Calcula `hash(porto+inicio+imo+tipo+de+para)`
   - Se hash já existe → ignora (repetição)
   - Se hash novo → acrescenta em `eventos/` + atualiza `estado_atual.json`
5. Gera `metricas/radar.json` e `metricas/fundeio.json`
6. Gera `embarcacoes/{IMO}.json` para cada embarcação com histórico recente
7. Gera `metricas/ranking-atividade.json` (Top 10 por período)
8. Limpa snapshots com mais de 30 dias
9. Retorna `exit 0` se houve eventos novos ou `exit 2` se não houve — o workflow usa `|| true` e `if: always()`, então o commit acontece sempre que houver qualquer mudança em `data/`

## Arquivos de dados

| Arquivo | Formato | Retenção | Uso |
|---|---|---|---|
| `vessels.json` | JSON | Substituído a cada coleta | Sentinela (monitor) |
| `estado_atual.json` | JSON `{ [imo]: {...} }` | Permanente | Radar, relatórios |
| `metricas/radar.json` | JSON | Substituído a cada coleta | Radar — contadores |
| `metricas/fundeio.json` | JSON array | Substituído a cada coleta | Radar — fundeio |
| `metricas/ranking-atividade.json` | JSON | Substituído a cada coleta | Radar — ranking |
| `metricas/indice-embarcacoes.json` | JSON array | Substituído a cada coleta | Busca por nome no relatório |
| `embarcacoes/{IMO}.json` | JSON | Substituído a cada coleta | Relatório individual |
| `eventos/YYYY/MM/DD.jsonl` | JSON Lines | Permanente | Histórico, métricas |
| `snapshots/YYYY/MM/DD.jsonl.gz` | gzip+base64 | 30 dias | Auditoria |

## Métricas por embarcação

```json
{
  "imo": "9619438",
  "navio": "MSC ALBANY",
  "estado_atual": { "porto": "Rio de Janeiro", "em_fundeio": false, ... },
  "metricas": {
    "manobras_30d": 4,
    "manobras_180d": 18,
    "manobras_365d": 37,
    "entradas_30d": 1,   "saidas_30d": 1,   "mudancas_30d": 2,
    "entradas_365d": 14, "saidas_365d": 13,
    "ultima_entrada": "...", "ultima_saida": "...",
    "tempo_medio_estadia_horas": 18.4,
    "tempo_medio_fundeio_horas": 7.2
  },
  "historico_recente": [...]
}
```

## Regras principais

**Deduplicação** — `hash_evento` = SHA-256 de `porto|inicio|imo|tipo|de+para`. Hash já existente → linha ignorada.

**Fora de ordem** — `estado_atual` só atualiza se `inicio_evento >= ultima_movimentacao_em`. Evento antigo entra no histórico mas não sobrescreve o estado.

**Fundeio** — `em_fundeio = true` quando `para` contém "Fundeio" e tipo não é SAÍDA.

**Manobra** — qualquer evento: ENTRADA + MUDANÇA + SAÍDA.

**Fuso** — pauta publica horário de Brasília (UTC-3). `parseInicio()` converte para UTC antes de gravar.

## Dependências

Nenhuma — só módulos nativos do Node.js (`https`, `fs`, `crypto`, `zlib`).

```bash
node scrape.js
```
