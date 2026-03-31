# Sprint IW51 DANI — true_parallel perfeito + rerun inteligente

> **Demandante:** DANI
> **Modulo principal:** `sap_automation/iw51.py` (~2161 linhas)
> **Objetivo:** Garantir que o modo `true_parallel` com 3 sessoes SAP funcione com 100% de confiabilidade, que o rerun com mesmo `run_id` reexecute apenas o que faltou, e que tudo seja gravado atomicamente no `projeto_Dani2.xlsm` e `iw51_progress.csv`.
> **Data:** 2026-03-31

---

## Indice

1. [Diagnostico do Estado Atual](#1-diagnostico-do-estado-atual)
2. [Analise de Rerun com Mesmo run_id](#2-analise-de-rerun-com-mesmo-run_id)
3. [Analise de Thread Safety](#3-analise-de-thread-safety)
4. [Analise de Durabilidade (Workbook + Ledger)](#4-analise-de-durabilidade-workbook--ledger)
5. [Epicos e Stories](#5-epicos-e-stories)
6. [Cenarios de Rerun](#6-cenarios-de-rerun)
7. [Sequenciamento de Implementacao](#7-sequenciamento-de-implementacao)
8. [Estrategia de Testes](#8-estrategia-de-testes)
9. [Configuracao Final](#9-configuracao-final)
10. [Riscos e Mitigacoes](#10-riscos-e-mitigacoes)
11. [Criterios de Aceite da Sprint](#11-criterios-de-aceite-da-sprint)

---

## 1. Diagnostico do Estado Atual

### O que ja existe e funciona

| Componente | Localizacao | Status |
|---|---|---|
| COM marshaling main→worker (`CoMarshalInterThreadInterfaceInStream`) | `_marshal_sap_application` L86 | OK, funcional |
| COM unmarshal no worker thread (`CoGetInterfaceAndReleaseStream`) | `_unmarshal_sap_application` L105 | OK, funcional |
| `CoInitialize`/`CoUninitialize` por worker | `_run_iw51_parallel_worker` L1461 | OK |
| Reattach via `Sessions` collection (non-blocking) | `_reattach_iw51_session` L831 | OK |
| Validacao de prontidao (`Busy` + `findById(wnd[0])` + `okcd` + tipo janela) | `_wait_session_ready` L679 | OK, robusta |
| Popup detection e dismissal | `_dismiss_popup_if_present` L702 | OK, chamado em todos os pontos criticos |
| Reset de transacao antes de cada item | `_prepare_iw51_item_surface` L935 | OK, usa `/n` + popup dismiss |
| Circuit breaker (fast fails + slow fails) | `_run_iw51_parallel_worker` L1621/L1659 | OK |
| Session locator por ID estavel | `Iw51SessionLocator` L328 | OK |
| Distribuicao round-robin | `split_iw51_work_items_evenly` L1192 | OK |
| Queue pattern (workers→main thread) | `_run_iw51_true_parallel_workers` L1743 | OK |
| Atomic workbook save (temp + `os.replace`) | `_save_workbook_atomic` L444 | OK |
| Ledger append-only (`iw51_progress.csv`) | `append_iw51_progress_ledger` L472 | OK |
| Working copy reuse no rerun | `_copy_working_workbook` L429 | OK |
| Ledger state reload no rerun | `_load_iw51_ledger_state` L450 | OK, com limitacoes |
| Reconciliacao workbook→ledger | `_collect_iw51_workbook_done_rows` L519 | OK |
| Fallback true_parallel→interleaved | `run_iw51_demandante` L2003 | OK |
| Worker state tracking (`Iw51WorkerState`) | L334 | OK |
| Manifest parcial persistido a cada batch | `_apply_worker_results` L1949 | OK |
| `DismissPopup` antes de cada operacao critica | `execute_iw51_item` L968 | OK, 7 call sites |
| Session recreation se slot perdido | `_reattach_iw51_session` L890 | OK |

### O que precisa de melhoria

| Problema | Localizacao | Impacto |
|---|---|---|
| **Ledger acumula duplicatas no rerun** | `_load_iw51_ledger_state` L450 | Ledger cresce indefinidamente; ultima entrada pode conflitar com anterior |
| **Items `failed` no ledger NAO sao terminais** | `_load_iw51_ledger_state` L464 | Correto para retry, mas sem classificacao retriable vs terminal |
| **Reconciliacao ledger→workbook unidirecional** | `run_iw51_demandante` L1847 | Items com success no ledger mas sem FEITO=SIM no workbook nao sao reconciliados |
| **Workbook sync em batch de 250** | `_persist_progress` L1935 | Gap entre batch N e N+1: crash perde FEITO dos items do batch incompleto |
| **`successful_rows += 1` sem lock explicito** | `_apply_worker_results` L1957 | Safe NA PRATICA (single consumer), mas fragil se refatorado |
| **Sem sinal de cancelamento para workers** | `_run_iw51_parallel_worker` L1449 | Nao ha como parar graciosamente se todos entraram em circuit breaker |
| **`maximize()` no setup de novas sessoes** | `ensure_sap_sessions` L749/L782 | Pode causar erro 619 se chamado durante processamento paralelo |
| **Sem metricas de rate/ETA no main thread** | `run_iw51_demandante` | So workers logam progresso; main thread nao tem visibilidade consolidada |

---

## 2. Analise de Rerun com Mesmo run_id

### Fluxo Atual de Rerun

```
run_iw51_demandante(run_id="20260331T090000")
│
├── _copy_working_workbook()
│   └── Se working_path.exists() → REUSA (L436-438)
│       Senao → shutil.copy2() da fonte
│
├── _load_iw51_ledger_state(ledger_path)
│   └── Le iw51_progress.csv
│       success_rows = {rows com status="success"}
│       terminal_rows = {rows com status="success" OU "rejected"}
│       ⚠ Items com status="failed" NAO entram em terminal_rows
│       ⚠ Se mesmo row_index aparece N vezes, TODAS sao lidas (set dedup por index)
│
├── load_iw51_work_items(completed_row_indices=ledger_terminal_rows)
│   └── Para cada row no workbook:
│       - Se FEITO=SIM → skip
│       - Se row_index in completed_rows → skip
│       - Se dados incompletos → rejected
│       - Senao → work_item
│
├── _collect_iw51_workbook_done_rows()
│   └── Rows com FEITO=SIM no workbook que NAO estao em ledger_terminal_rows
│       → Importa para o ledger como "success" (reconciliacao)
│
├── _sync_workbook_done_rows(ledger_success_rows)
│   └── Garante que TODAS as rows de success no ledger tenham FEITO=SIM no workbook
│
└── [processa apenas items restantes]
```

### Bugs e Gaps Identificados

**Bug 1: Ledger acumula duplicatas**

O ledger e append-only (`append_iw51_progress_ledger` L472). No rerun, `_load_iw51_ledger_state` le TODAS as linhas do CSV e usa `set.add()` — portanto duplicatas de `row_index` sao dedupadas na leitura. Porem o CSV em si cresce indefinidamente. Apos N reruns, o mesmo `row_index` pode aparecer N vezes no CSV.

**Impacto:** Performance degradada na leitura do ledger + confusao na analise manual.

**Bug 2: Sem classificacao de failure retriable vs terminal**

`_load_iw51_ledger_state` (L464) trata `status="failed"` como NAO terminal, o que significa que items falhados serao reexecutados no rerun. Isso e correto para falhas transientes (timeout, erro 619). Porem, falhas deterministas (dados invalidos detectados pelo SAP apos validacao) serao retentadas infinitamente.

**Impacto:** Reruns processam items que nunca vao ter sucesso.

**Bug 3: Gap de reconciliacao ledger→workbook**

A reconciliacao `_collect_iw51_workbook_done_rows` (L519) importa rows com FEITO=SIM no workbook que NAO estao no ledger. Porem o inverso NAO acontece de forma automatica na inicializacao: se o ledger tem `status="success"` mas o workbook NAO tem FEITO=SIM (crash entre ledger write e workbook sync), a reconciliacao na L1866 resolve isso. **Este gap esta parcialmente coberto.**

**A reconciliacao bidirecional ja funciona**, mas de forma implicita:
1. Workbook→Ledger: `_collect_iw51_workbook_done_rows` importa FEITO=SIM ausentes do ledger
2. Ledger→Workbook: `_sync_workbook_done_rows` (L1866) escreve FEITO=SIM para todos `ledger_success_rows`

**O gap real:** Entre o momento que o ledger recebe o success e o proximo sync batch (250 items), se houver crash, o workbook nao tera FEITO=SIM. Na PROXIMA run, a reconciliacao L1866 resolve. Mas se o usuario abrir o workbook ENTRE runs, vera items sem FEITO que ja foram processados.

---

## 3. Analise de Thread Safety

### Arquitetura de Concorrencia

```
Main Thread                          Worker Threads (N=3)
─────────────                        ────────────────────
_run_iw51_true_parallel_workers()    _run_iw51_parallel_worker() x3
│                                    │
│  marshal SAP app → N streams       │  CoInitialize()
│                                    │  unmarshal stream → Application proxy
│  result_queue = Queue()            │  reattach session
│                                    │  for item in items:
│  while not all done:               │    process item
│    msg = result_queue.get()  ◄─────│────result_queue.put({results, failures})
│    _apply_worker_results()         │  result_queue.put({type: "done"})
│      ├── successful_rows += 1      │  CoUninitialize()
│      ├── pending_sync_rows.add()   │
│      ├── failed_rows.append()      │
│      ├── append_iw51_progress_ledger()
│      ├── _persist_progress()       │
│      │   └── _sync_workbook_done_rows()
│      └── manifest write            │
│                                    │
│  final sync remaining rows         │
│  final manifest write              │
```

### Veredicto: Thread-Safe NA PRATICA

**Por que funciona sem lock:**

1. **Single consumer pattern** — `_apply_worker_results` e chamada EXCLUSIVAMENTE pela main thread dentro do `while` loop de `_run_iw51_true_parallel_workers` (L1781-1794). A main thread le um `message` por vez de `result_queue.get()`, processa, e so entao le o proximo.

2. **Variaveis mutadas so pela main thread:**
   - `successful_rows` (L1957) — `int += 1` — so main thread
   - `pending_sync_rows` (L1958) — `set.add()` — so main thread
   - `failed_rows` (L1961) — `list.append()` — so main thread
   - `workbook` / `sheet` (via `_sync_workbook_done_rows`) — so main thread

3. **Workers NAO tocam em nenhuma variavel compartilhada** — eles so colocam mensagens na `result_queue`. Cada worker tem seu proprio `worker_state` (um objeto separado na lista `worker_states`).

4. **`result_queue`** e uma `queue.Queue` do Python — thread-safe por design.

### Fragilidade: Refatoracao pode quebrar

Se alguem chamar `_apply_worker_results` de dentro de uma worker thread (por engano), tudo quebra. Nao ha guard explicito. A thread safety depende de uma invariante arquitetural nao documentada.

### Recomendacao

Adicionar um `assert threading.current_thread() is threading.main_thread()` ou equivalente no inicio de `_apply_worker_results` como guard documentacional. Nao adicionar lock — seria overhead desnecessario e mascararia violacao da invariante.

---

## 4. Analise de Durabilidade (Workbook + Ledger)

### Ordem de Gravacao Atual

Para cada batch de resultados recebido de um worker:

```
1. append_iw51_progress_ledger()  ← LEDGER recebe success/failure
2. _persist_progress()
   └── se pending_sync_rows >= 250:
       └── _sync_workbook_done_rows()  ← WORKBOOK recebe FEITO=SIM
           └── _save_workbook_atomic()  ← temp file + os.replace()
3. manifest write (JSON)
```

### Cenarios de Crash

| Cenario | Ledger | Workbook | Manifest | Recuperacao no Rerun |
|---|---|---|---|---|
| Crash apos step 1, antes de step 2 | OK (success gravado) | FEITO ausente | Desatualizado | Reconciliacao L1866 aplica FEITO |
| Crash durante `_save_workbook_atomic` (temp write) | OK | Intacto (replace nao rodou) | Desatualizado | Reconciliacao L1866 aplica FEITO |
| Crash apos step 2 (sync), antes de step 3 | OK | OK | Desatualizado | Tudo ok; manifest sera reescrito |
| Crash durante step 1 (ledger append) | Possivel corrupcao parcial | FEITO ausente | Desatualizado | Se row nao esta no ledger, sera reexecutado |
| Crash entre batch N (synced) e batch N+1 (pending) | Items N+1 no ledger mas sem FEITO | Somente batch N tem FEITO | Desatualizado | Reconciliacao L1866 aplica FEITO do batch N+1 |

### Veredicto

O sistema e **duravel na pratica** porque:
1. O ledger e a fonte de verdade (append-only, gravado ANTES do workbook)
2. A reconciliacao bidirecional no startup resolve divergencias
3. `_save_workbook_atomic` evita corrupcao do `.xlsm`

**Gap principal:** O usuario pode abrir o workbook entre runs e ver items sem FEITO que ja foram processados. Isso causa confusao mas nao perda de dados — o proximo rerun corrige.

**Melhoria proposta:** Reduzir `workbook_sync_batch_size` de 250 para 1 (sync apos cada item) ou para um valor menor (25-50). Trade-off: mais writes de disco, mas gap menor.

---

## 5. Epicos e Stories

### Epico 1: Rerun Inteligente com Mesmo run_id

> Garantir que rerun reexecute APENAS o que faltou, com classificacao inteligente de falhas.

#### Story 1.1 — Classificacao de falhas: retriable vs terminal

**Arquivo:** `sap_automation/iw51.py`, funcao `_load_iw51_ledger_state`
**Mudanca:** Criar 3 categorias de status no ledger:
- `success` — terminal, item criado no SAP. NUNCA reexecutar.
- `rejected` — terminal, dados invalidos na planilha. NUNCA reexecutar.
- `failed` — default retriable. Sera reexecutado no rerun.
- **NOVO:** `failed_terminal` — falha determinista detectada pelo SAP (ex: instalacao invalida, PN inexistente). NAO reexecutar.

**Logica de classificacao** em `_process_iw51_item_with_retry`:
- Se o SAP retornou mensagem de erro especifica (ex: "Instalacao nao encontrada", "PN invalido") → `failed_terminal`
- Se foi timeout, erro 619, disconnection → `failed` (retriable)
- Heuristica: se AMBAS as tentativas falharam com o mesmo erro NAO-sistemico, marcar como `failed_terminal`

**Mudanca em `_load_iw51_ledger_state`:**
```python
if status == "success":
    success_rows.add(row_index)
    terminal_rows.add(row_index)
elif status in ("rejected", "failed_terminal"):
    terminal_rows.add(row_index)
# status="failed" → NAO terminal → sera reexecutado
```

**Criterio de aceite:**
- Items `failed_terminal` nao sao reexecutados no rerun
- Items `failed` (retriable) sao reexecutados
- Items `success` e `rejected` continuam terminais

#### Story 1.2 — Deduplicacao do ledger no reload

**Arquivo:** `sap_automation/iw51.py`, funcao `_load_iw51_ledger_state`
**Mudanca:** Ao ler o ledger, manter apenas a ULTIMA entrada por `row_index`. Se o mesmo `row_index` aparece como `failed` e depois como `success`, a ultima entrada (`success`) vence.

**Implementacao:**
```python
last_entry_by_row: dict[int, str] = {}
for row in reader:
    row_index = int(row["row_index"])
    status = row["status"]
    last_entry_by_row[row_index] = status

for row_index, status in last_entry_by_row.items():
    if status == "success":
        success_rows.add(row_index)
        terminal_rows.add(row_index)
    elif status in ("rejected", "failed_terminal"):
        terminal_rows.add(row_index)
```

**Criterio de aceite:**
- Rerun com ledger contendo `[failed, failed, success]` para mesmo `row_index` trata como `success` (terminal)
- Rerun com ledger contendo `[failed, failed]` trata como `failed` (retriable)

#### Story 1.3 — Compactacao periodica do ledger (P2)

**Arquivo:** `sap_automation/iw51.py`
**Nova funcao:** `_compact_iw51_ledger(ledger_path: Path) -> int`

**Logica:**
1. Le todo o CSV
2. Mantem apenas a ultima entrada por `row_index`
3. Reescreve o CSV atomicamente (temp + `os.replace`)
4. Retorna numero de linhas removidas

**Call site:** Inicio de `run_iw51_demandante`, apos `_load_iw51_ledger_state`, se o ledger tiver mais de 2x o numero de rows unicas.

**Prioridade:** P2 — so necessario se reruns frequentes degradarem performance.

**Criterio de aceite:**
- Ledger compactado tem exatamente 1 entrada por `row_index`
- Atomicidade mantida (temp + replace)

---

### Epico 2: Garantia de Gravacao (Durabilidade)

> Minimizar o gap entre ledger e workbook FEITO para que o usuario veja estado correto a qualquer momento.

#### Story 2.1 — Reducao do batch size de sync

**Arquivo:** `sap_automation/iw51.py`, funcao `_persist_progress` L1935
**Mudanca:** Reduzir `workbook_sync_batch_size` default de 250 para 25.

**Trade-off:**
- 250: sync a cada ~83s (se 3 items/s) → gap maximo de 249 items sem FEITO
- 25: sync a cada ~8s → gap maximo de 24 items sem FEITO
- 1: sync a cada item → maximo durabilidade mas overhead de IO significativo

**Decisao:** Default 25. Configuravel via JSON. O usuario pode ajustar para 1 se precisar de durabilidade maxima.

**Mudanca no config:**
```json
"workbook_sync_batch_size": 25
```

**Criterio de aceite:**
- Sync ocorre a cada 25 items (default)
- Config `workbook_sync_batch_size: 1` funciona para sync imediato
- Nenhuma perda de performance mensuravel com batch=25

#### Story 2.2 — Flush do ledger ANTES do workbook sync

**Arquivo:** `sap_automation/iw51.py`, funcao `_apply_worker_results` L1949
**Mudanca:** Garantir que `append_iw51_progress_ledger` (L1963) faz `flush()` no file handle apos escrever.

**Implementacao em `append_iw51_progress_ledger`:**
```python
with ledger_path.open("a", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(handle, ...)
    for row in rows:
        writer.writerow(row)
    handle.flush()
    os.fsync(handle.fileno())  # garante que OS gravou em disco
```

**Por que:** Se o processo crashar entre `writerow` e o proximo `open`, o OS pode nao ter flushado o buffer. `fsync` garante que o ledger esta no disco ANTES de qualquer sync de workbook.

**Criterio de aceite:**
- Apos `append_iw51_progress_ledger`, dados estao no disco (verificavel com `os.fsync`)
- Nenhuma perda de progresso mesmo com kill -9

#### Story 2.3 — Reconciliacao bidirecional explicita no startup

**Arquivo:** `sap_automation/iw51.py`, funcao `run_iw51_demandante`
**Mudanca:** Tornar explicita a reconciliacao que ja existe implicitamente:

1. **Workbook→Ledger** (ja existe em L1847): `_collect_iw51_workbook_done_rows` importa FEITO=SIM ausentes do ledger
2. **Ledger→Workbook** (ja existe em L1866): `_sync_workbook_done_rows` escreve FEITO=SIM para todos `ledger_success_rows`
3. **NOVO: Logging explicito** de quantos items foram reconciliados em cada direcao
4. **NOVO: Validacao de integridade** — se `len(ledger_success_rows) != count(FEITO=SIM no workbook)` apos reconciliacao, logar warning

**Criterio de aceite:**
- Log explicito: "Reconciled N rows workbook→ledger, M rows ledger→workbook"
- Warning se contagens divergem apos reconciliacao
- Nenhuma mudanca funcional (a reconciliacao ja funciona)

---

### Epico 3: Thread Safety Explicita

> Documentar e proteger a invariante de single-consumer que garante thread safety.

#### Story 3.1 — Guard de thread no `_apply_worker_results`

**Arquivo:** `sap_automation/iw51.py`, funcao `_apply_worker_results` L1949
**Mudanca:** Adicionar assertion no inicio:

```python
def _apply_worker_results(
    worker_index: int,
    worker_results: list[Iw51ItemResult],
    worker_failures: list[dict[str, Any]],
) -> None:
    assert threading.current_thread() is threading.main_thread(), (
        f"_apply_worker_results must be called from the main thread, "
        f"got {threading.current_thread().name}"
    )
    ...
```

**Por que:** Se alguem refatorar o codigo e chamar esta funcao de dentro de um worker thread, o assert falha imediatamente em vez de causar race conditions silenciosas.

**Criterio de aceite:**
- Assert presente e testado
- Nenhum overhead mensuravel (assertion e O(1))

#### Story 3.2 — Documentacao da invariante de concorrencia

**Arquivo:** `sap_automation/iw51.py`
**Mudanca:** Adicionar docstring/comentario no topo de `_run_iw51_true_parallel_workers`:

```python
"""
Thread safety contract:
- Worker threads ONLY communicate via result_queue.put()
- Main thread is the SOLE consumer of result_queue
- _apply_worker_results is called EXCLUSIVELY by the main thread
- No shared mutable state between workers — each has its own:
  - session (COM proxy, thread-local after unmarshal)
  - worker_state (unique index in list)
  - items (pre-partitioned, no overlap)
"""
```

**Criterio de aceite:**
- Invariante documentada no codigo
- Desenvolvedores futuros entendem por que lock nao e necessario

---

### Epico 4: Robustez do true_parallel

> Hardening do modo paralelo para resistir a falhas de sessao, popups e crashes.

#### Story 4.1 — Stagger de startup dos workers

**Arquivo:** `sap_automation/iw51.py`, funcao `_run_iw51_true_parallel_workers` L1743
**Mudanca:** Adicionar delay de 1-2s entre o start de cada worker thread.

```python
for worker_index, group in enumerate(groups, start=1):
    thread = threading.Thread(...)
    thread.start()
    threads.append(thread)
    if worker_index < len(groups):
        time.sleep(1.5)  # stagger para evitar contencao no unmarshal
```

**Por que:** Se todos os workers tentarem `_unmarshal_sap_application` + `_reattach_iw51_session` simultaneamente, o servidor SAP pode ficar sobrecarregado. O stagger garante que cada worker esta estabilizado antes do proximo comecar.

**Criterio de aceite:**
- Delay configuravel (nova key no config: `worker_stagger_seconds`, default 1.5)
- Workers iniciam com 1.5s de intervalo
- Nenhum impacto significativo no tempo total (3s de overhead para 3 workers)

#### Story 4.2 — Cancel event para shutdown gracioso

**Arquivo:** `sap_automation/iw51.py`, funcao `_run_iw51_parallel_worker`
**Mudanca:** Aceitar `cancel_event: threading.Event` e verificar no topo de cada iteracao:

```python
for next_item_index, item in enumerate(items, start=1):
    if cancel_event.is_set():
        remaining = items[next_item_index - 1:]
        # marcar restantes como skipped
        break
    ...
```

**Mudanca em `_run_iw51_true_parallel_workers`:**
- Criar `cancel_event = threading.Event()` antes dos threads
- Passar para cada worker
- Na main thread, se TODOS os workers ativos estiverem em `circuit_breaker`, setar o evento

**Por que:** Se worker 1 entra em circuit_breaker e worker 2 tambem, worker 3 continua processando sozinho. Se worker 3 tambem falhar, nao ha como parar o processamento. Com o cancel event, a main thread pode detectar esta situacao e encerrar todos.

**Criterio de aceite:**
- Workers param quando `cancel_event` e setado
- Items nao processados marcados como `failed` com `error="Cancelled: all workers in circuit_breaker"`
- Sem data race no cancel

#### Story 4.3 — Protecao contra `maximize()` durante processamento

**Arquivo:** `sap_automation/iw51.py`, funcao `ensure_sap_sessions` L735
**Mudanca:** O `maximize()` e chamado apenas durante o setup (L749, L782), ANTES do processamento paralelo iniciar. Isso e seguro. Porem, se `_reattach_iw51_session` precisar recriar uma sessao (L890), o `maximize()` NAO e chamado — correto.

**Verificacao:** Confirmar que `execute_iw51_item` NAO chama `maximize()`. Resultado: **confirmado** — `execute_iw51_item` (L968) usa `session.findById(_IW51_MAIN_WINDOW_ID)` para obter referencia da janela mas NUNCA chama `maximize()`. Diferente do DW, o IW51 ja esta correto neste ponto.

**Nenhuma mudanca necessaria.** Apenas documentar esta decisao.

#### Story 4.4 — Timeout global por worker

**Arquivo:** `sap_automation/iw51.py`, funcao `_run_iw51_parallel_worker`
**Mudanca:** Adicionar timeout global configuravel por worker. Se o worker exceder este timeout, para de processar e marca items restantes como failed.

**Nova config:** `worker_timeout_seconds` (default: 3600 = 1 hora)

```python
worker_deadline = time.monotonic() + settings.worker_timeout_seconds
...
for item in items:
    if time.monotonic() > worker_deadline:
        # marcar restantes como timeout
        break
```

**Por que:** Evita que um worker fique preso indefinidamente se o SAP parar de responder mas nao gerar excecao (ex: dialog modal nao detectado).

**Criterio de aceite:**
- Worker para apos timeout
- Items restantes marcados com `status="failed"`, `error="Worker timeout exceeded"`
- Timeout configuravel no JSON

---

### Epico 5: Performance e Observabilidade

> Metricas consolidadas e logging aprimorado para diagnostico.

#### Story 5.1 — Progresso consolidado na main thread

**Arquivo:** `sap_automation/iw51.py`, funcao `_apply_worker_results`
**Mudanca:** A cada N resultados coletados, logar estado consolidado:

```python
total_collected = successful_rows + len(failed_rows)
if total_collected % 50 == 0 or total_collected == total_items:
    elapsed = time.perf_counter() - run_started_at
    rate = successful_rows / elapsed if elapsed > 0 else 0
    eta = (total_items - total_collected) / rate if rate > 0 else float("inf")
    logger.info(
        "IW51 PROGRESS total=%s/%s ok=%s fail=%s elapsed_s=%.1f rate=%.2f/s eta_s=%.0f",
        total_collected, total_items, successful_rows, len(failed_rows),
        elapsed, rate, eta,
    )
```

**Criterio de aceite:**
- Log a cada 50 items coletados
- Rate e ETA calculados e logados
- Funciona em todos os modos (parallel, interleaved, sequential)

#### Story 5.2 — Metricas finais por worker no manifest

**Arquivo:** `sap_automation/iw51.py`, `Iw51WorkerState`
**Mudanca:** Adicionar campos ao `Iw51WorkerState`:

```python
@dataclass
class Iw51WorkerState:
    ...
    first_item_started_at: float = 0.0  # timestamp do primeiro item
    last_item_finished_at: float = 0.0  # timestamp do ultimo item
    avg_item_seconds: float = 0.0  # media de tempo por item
```

**Calculo:** Ao final de cada worker, calcular `avg_item_seconds = elapsed / items_processed`.

**Criterio de aceite:**
- Manifest final inclui rate e media por worker
- Permite comparar performance entre sessoes

#### Story 5.3 — Log de inicio com resumo do rerun

**Arquivo:** `sap_automation/iw51.py`, funcao `run_iw51_demandante`
**Mudanca:** Apos carregar ledger e reconciliar, logar resumo claro:

```python
logger.info(
    "IW51 RERUN SUMMARY run_id=%s "
    "ledger_success=%s ledger_rejected=%s ledger_failed_retriable=%s "
    "workbook_feito=%s new_items=%s total_pending=%s",
    run_id,
    len(ledger_success_rows),
    len(ledger_rejected_rows),  # novo set
    len(ledger_failed_retriable),  # novo set
    len(workbook_done_reconciled),
    len(new_items),
    len(items),
)
```

**Criterio de aceite:**
- Log de resumo no inicio de cada rerun
- Numeros batem com o que sera processado

---

## 6. Cenarios de Rerun

### Cenario A: Primeira execucao — 500 items, 3 workers, sucesso parcial

```
Run 1: run_id=20260331T090000
├── Workbook: 500 rows com dados, 0 FEITO=SIM
├── Ledger: nao existe
├── Items carregados: 500
├── Distribuicao: worker1=167, worker2=167, worker3=166
├── Resultado:
│   ├── worker1: 167 ok, 0 fail
│   ├── worker2: 150 ok, 17 fail (3 timeout, 14 erro 619)
│   └── worker3: 166 ok, 0 fail
├── Ledger final: 500 entries (483 success, 17 failed)
├── Workbook final: 483 FEITO=SIM
└── Manifest: status=partial, successful=483, failed=17
```

### Cenario B: Rerun — mesmo run_id, reprocessa falhados

```
Run 2: run_id=20260331T090000 (RERUN)
├── Workbook: 500 rows, 483 FEITO=SIM
├── Ledger existente: 500 entries
│   ├── _load_iw51_ledger_state():
│   │   ├── success_rows = {483 indices}
│   │   ├── terminal_rows = {483 indices} (so success + rejected)
│   │   └── failed: 17 indices → NAO terminal → SERA reexecutado
├── load_iw51_work_items(completed_row_indices=terminal_rows):
│   ├── 483 rows skippadas (FEITO=SIM ou terminal)
│   └── 17 items carregados (os falhados da run anterior)
├── Distribuicao: worker1=6, worker2=6, worker3=5
├── Resultado:
│   ├── worker1: 6 ok
│   ├── worker2: 5 ok, 1 fail_terminal (instalacao invalida)
│   └── worker3: 5 ok
├── Ledger final: 517 entries (499 success, 1 failed_terminal, 17 failed antigos)
│   └── Dedup na leitura: 500 unicos (499 success, 1 failed_terminal)
├── Workbook final: 499 FEITO=SIM
└── Manifest: status=partial (1 failed_terminal restante)
```

### Cenario C: Rerun apos crash mid-run

```
Run 1: Crash apos 200/500 items processados
├── Ledger: 200 entries (180 success, 20 failed)
├── Workbook: ~175 FEITO=SIM (ultimo batch incompleto)
│
Run 2 (RERUN): run_id=20260331T090000
├── Working copy: existe → REUSA
├── _load_iw51_ledger_state(): 180 success, 20 failed (non-terminal)
├── _collect_iw51_workbook_done_rows(): 0 (175 FEITO ja estao no ledger)
│   (na verdade 5 success rows que NAO tem FEITO no workbook)
├── _sync_workbook_done_rows(ledger_success=180):
│   └── Aplica FEITO=SIM nas 5 rows que faltavam → 180 FEITO=SIM
├── load_iw51_work_items(completed=180):
│   └── 320 items carregados (300 novos + 20 retriable)
├── Processa 320 items normalmente
└── Tudo sincronizado
```

### Cenario D: Rerun com workbook editado manualmente

```
Situacao: Usuario abriu workbook e marcou 10 rows como FEITO=SIM manualmente
│
Run 2 (RERUN):
├── _load_iw51_ledger_state(): N success do ledger
├── _collect_iw51_workbook_done_rows():
│   └── Detecta 10 rows com FEITO=SIM que NAO estao no ledger
│   └── Importa para ledger como success (attempt=0, error="Imported from workbook")
├── load_iw51_work_items():
│   └── 10 rows extras skippadas (agora estao em terminal_rows)
└── Nao reprocessa items marcados manualmente
```

---

## 7. Sequenciamento de Implementacao

```
Fase 1 ──────────────────────────────────────────────────
  Epico 1: Rerun Inteligente (Stories 1.1, 1.2)
  Epico 2: Durabilidade (Stories 2.1, 2.2, 2.3)
  Fundacao — tudo depende de rerun e gravacao corretos.

Fase 2 ──────────────────────────────────────────────────
  Epico 3: Thread Safety (Stories 3.1, 3.2)
  Guards e documentacao. Zero risco, maximo beneficio.

Fase 3 ──────────────────────────────────────────────────
  Epico 4: Robustez (Stories 4.1, 4.2, 4.4)
  Stagger, cancel event, timeout global.

Fase 4 ──────────────────────────────────────────────────
  Epico 5: Observabilidade (Stories 5.1, 5.2, 5.3)
  Logging e metricas. Nao bloqueia nada.

Fase 5 (P2) ─────────────────────────────────────────────
  Story 1.3: Compactacao de ledger
  Story 4.3: Documentacao maximize (ja OK)
  Polimento.
```

### Dependencias entre stories

| Story | Depende de |
|---|---|
| 1.2 (dedup ledger) | 1.1 (classificacao de falhas) |
| 1.3 (compactacao) | 1.2 (dedup) |
| 2.3 (reconciliacao explicita) | 1.1 (classificacao) |
| 3.1 (guard de thread) | Nenhuma |
| 4.2 (cancel event) | Nenhuma |
| 4.4 (timeout global) | 4.2 (cancel event) |
| 5.1 (progresso consolidado) | Nenhuma |
| 5.3 (log de resumo) | 1.1 (classificacao) |

---

## 8. Estrategia de Testes

### Testes unitarios (Linux, sem SAP)

| Teste | O que valida |
|---|---|
| `test_load_ledger_state_dedup_last_entry_wins` | Ledger com duplicatas: ultima entrada por `row_index` vence |
| `test_load_ledger_state_failed_is_retriable` | Items `failed` NAO entram em `terminal_rows` |
| `test_load_ledger_state_failed_terminal_is_terminal` | Items `failed_terminal` entram em `terminal_rows` |
| `test_rerun_skips_success_and_rejected` | `load_iw51_work_items` pula rows com `success` e `rejected` no ledger |
| `test_rerun_includes_failed_items` | Items `failed` no ledger sao recarregados como work items |
| `test_workbook_sync_batch_size_1` | Sync ocorre apos cada item quando batch=1 |
| `test_ledger_flush_fsync` | Apos `append_iw51_progress_ledger`, dados estao no disco |
| `test_reconciliation_bidirectional` | Workbook→Ledger e Ledger→Workbook ambos funcionam |
| `test_compact_ledger_keeps_last_entry` | Compactacao mantem ultima entrada por `row_index` |

### Testes de thread safety (Linux, sem SAP)

| Teste | O que valida |
|---|---|
| `test_apply_worker_results_asserts_main_thread` | Assertion falha se chamado de worker thread |
| `test_parallel_workers_isolated_state` | 3 workers fake, cada um atualiza apenas SEU `worker_state` |
| `test_cancel_event_stops_all_workers` | Workers param apos cancel event, items restantes marcados como failed |
| `test_worker_timeout_aborts` | Worker para apos timeout, items restantes marcados como failed |

### Testes de integracao (Windows, com SAP GUI)

| Teste | O que valida |
|---|---|
| `test_true_parallel_3_sessions_10_items` | 10 items processados em 3 sessoes paralelas, resultados corretos |
| `test_rerun_processes_only_failed` | 2a execucao com mesmo `run_id` processa apenas falhados |
| `test_crash_recovery_workbook_consistent` | Apos kill e rerun, workbook + ledger consistentes |
| `test_circuit_breaker_per_worker` | Worker com dados invalidos entra em circuit_breaker, outros continuam |

Marcados com `@pytest.mark.skipif(sys.platform != "win32")` e `@pytest.mark.integration`.

---

## 9. Configuracao Final

### `sap_iw69_batch_config.json` — secao IW51 apos sprint

```json
{
    "iw51": {
        "default_demandante": "DANI",
        "demandantes": {
            "DANI": {
                "workbook_path": "projeto_Dani2.xlsm",
                "sheet_name": "Macro1",
                "session_count": 3,
                "execution_mode": "true_parallel",
                "inter_item_sleep_seconds": 0,
                "max_rows_per_run": 3000,
                "post_login_wait_seconds": 6,
                "workbook_sync_batch_size": 25,
                "circuit_breaker_fast_fail_threshold": 10,
                "circuit_breaker_slow_fail_threshold": 30,
                "per_step_timeout_seconds": 30,
                "session_recovery_mode": "soft",
                "worker_stagger_seconds": 1.5,
                "worker_timeout_seconds": 3600
            }
        }
    }
}
```

### Novas chaves de configuracao

| Chave | Tipo | Default | Descricao |
|---|---|---|---|
| `workbook_sync_batch_size` | int | 25 (era 250) | Sync FEITO no workbook a cada N items |
| `worker_stagger_seconds` | float | 1.5 | Delay entre start de workers |
| `worker_timeout_seconds` | float | 3600 | Timeout global por worker (segundos) |

---

## 10. Riscos e Mitigacoes

| Risco | Prob. | Impacto | Mitigacao |
|---|---|---|---|
| Classificacao errada de falha como `failed_terminal` | Media | Alto | Heuristica conservadora: so marcar terminal se AMBAS tentativas falharam com mesmo erro determinista |
| `os.fsync` desacelera ledger write | Baixa | Baixo | fsync adiciona ~1ms por write; negligivel para items que levam ~1-3s cada |
| Workbook sync com batch=25 sobrecarrega disco | Baixa | Baixo | `_save_workbook_atomic` ja e eficiente; 25 items = sync a cada ~8s |
| Stagger de 1.5s entre workers acumula 3s de overhead | Baixa | Baixo | 3s num run de 30min+ e irrelevante |
| Cancel event falha se worker esta bloqueado em COM call | Media | Medio | Timeout global (Story 4.4) garante que worker para eventualmente |
| Assertion `main_thread()` falha em frameworks async | Baixa | Medio | Guard verifica `threading.current_thread().name` como fallback |
| Compactacao de ledger durante write concorrente | Baixa | Alto | Compactacao so roda no startup, ANTES de qualquer worker iniciar |
| `maximize()` no setup causa erro 619 em sessoes existentes | Baixa | Medio | `maximize()` so e chamado ANTES do processamento paralelo; sessions sao criadas sequencialmente |

---

## 11. Criterios de Aceite da Sprint

### Must Have (sem estes, sprint falha)

- [ ] Rerun com mesmo `run_id` reexecuta APENAS items `failed` (retriable) e items novos
- [ ] Items `success` e `rejected` NUNCA sao reexecutados
- [ ] Ledger deduplicado na leitura (ultima entrada vence)
- [ ] Workbook sync com batch=25 (default) — gap maximo de 24 items sem FEITO
- [ ] Ledger flush com `os.fsync` antes de qualquer workbook sync
- [ ] Reconciliacao bidirecional (workbook↔ledger) no startup com log explicito
- [ ] Assertion de main thread em `_apply_worker_results`
- [ ] Invariante de thread safety documentada em `_run_iw51_true_parallel_workers`
- [ ] Todos os testes unitarios passam
- [ ] Todos os testes de thread safety passam

### Should Have

- [ ] Classificacao `failed_terminal` para falhas deterministas do SAP
- [ ] Stagger de 1.5s entre startup de workers
- [ ] Cancel event para shutdown gracioso
- [ ] Timeout global por worker (default 1h)
- [ ] Progresso consolidado logado a cada 50 items pela main thread
- [ ] Metricas por worker no manifest (rate, media)

### Nice to Have (P2, proxima sprint)

- [ ] Compactacao periodica do ledger
- [ ] Endpoint de progresso no API (`GET /api/v1/extractions/iw51/progress`)
- [ ] Dashboard de metricas em tempo real via WebSocket

---

## Anexo: Fundamentacao Tecnica

### Por que o single-consumer pattern e suficiente

No modelo de concorrencia do IW51 true_parallel:

1. **Workers** (N threads) produzem resultados via `result_queue.put()` — operacao thread-safe por `queue.Queue`
2. **Main thread** consome exclusivamente com `result_queue.get()` — uma mensagem por vez
3. **`_apply_worker_results`** modifica estado compartilhado (`successful_rows`, `pending_sync_rows`, `failed_rows`) mas SOMENTE e chamada pela main thread

Isso e equivalente ao padrao Actor Model: workers sao actors que enviam mensagens, main thread e o unico receptor. Nao existe acesso concorrente a variaveis mutaveis, portanto nao ha race conditions.

### Por que `_IW51_SAP_COM_LOCK` nao e usado em true_parallel

O `_IW51_SAP_COM_LOCK` (L61) existe para o modo `interleaved`, onde a main thread alterna entre sessoes. Em true_parallel, cada worker tem seu proprio COM apartment (STA) via `CoInitialize()`, portanto operacoes COM sao isoladas por thread. Lock seria contraproducente — serializaria operacoes que podem rodar em paralelo.

A flag `serialize_com_calls=False` (L1573) desativa o lock explicitamente no modo parallel.

### Diferenca entre IW51 e DW no contexto de paralelismo

| Aspecto | IW51 (DANI) | DW |
|---|---|---|
| `maximize()` no caminho quente | NAO (so no setup) | SIM (causa erro 619) |
| COM marshaling implementado | SIM, funcional | SIM, nunca ativado |
| Queue pattern | SIM (`result_queue`) | NAO (loop sequencial) |
| Popup detection | SIM (7 call sites) | NAO |
| Session readiness validation | SIM (robusta) | SIM (basica) |
| Workbook sync | SIM (batch atomico) | N/A (CSV direto) |
| True parallel funcional | SIM (com gaps de durabilidade) | NAO (ainda sequencial) |

**Conclusao:** O IW51 esta significativamente mais maduro que o DW para paralelismo. Os gaps sao de durabilidade e rerun, nao de arquitetura fundamental.

### Referencia de funcoes criticas

| Funcao | Linha | Responsabilidade |
|---|---|---|
| `_load_iw51_ledger_state` | L450 | Carrega estado do ledger (success/terminal rows) |
| `_collect_iw51_workbook_done_rows` | L519 | Reconcilia FEITO=SIM do workbook → ledger |
| `load_iw51_work_items` | L553 | Carrega items pendentes, skippando terminais |
| `_save_workbook_atomic` | L444 | Salva workbook via temp + `os.replace` |
| `append_iw51_progress_ledger` | L472 | Append-only write no ledger CSV |
| `_sync_workbook_done_rows` | L1201 | Escreve FEITO=SIM no workbook + save atomico |
| `_persist_progress` | L1935 | Sync condicional baseado em batch size |
| `_apply_worker_results` | L1949 | Single-consumer callback (main thread) |
| `_run_iw51_true_parallel_workers` | L1743 | Orquestrador de threads com queue pattern |
| `_run_iw51_parallel_worker` | L1449 | Worker thread body (CoInit → unmarshal → process) |
| `_process_iw51_item_with_retry` | L1090 | Processamento com retry para erros sistemicos |
| `_dismiss_popup_if_present` | L702 | Detecta e dispensa popups/modais |
| `_wait_session_ready` | L679 | Valida prontidao da sessao SAP |
| `_reattach_iw51_session` | L831 | Reconecta sessao por ID estavel |
| `run_iw51_demandante` | L1799 | Orquestrador principal (entry point) |
