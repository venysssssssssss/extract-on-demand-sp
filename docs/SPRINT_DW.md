# Sprint DW — Execucao Paralela de 3 Sessoes SAP GUI

> **Demandante:** DW
> **Modulo principal:** `sap_automation/dw.py`
> **Objetivo:** Garantir que 3 sessoes SAP funcionem em paralelo real, cada uma processando sua fatia de reclamacoes simultaneamente.
> **Data:** 2026-03-30

---

## Indice

1. [Diagnostico do Estado Atual](#1-diagnostico-do-estado-atual)
2. [Causa Raiz dos Erros em Producao](#2-causa-raiz-dos-erros-em-producao)
3. [Arquitetura Alvo](#3-arquitetura-alvo)
4. [Epicos e Stories](#4-epicos-e-stories)
5. [Sequenciamento de Implementacao](#5-sequenciamento-de-implementacao)
6. [Estrategia de Testes](#6-estrategia-de-testes)
7. [Configuracao Final](#7-configuracao-final)
8. [Riscos e Mitigacoes](#8-riscos-e-mitigacoes)
9. [Criterios de Aceite da Sprint](#9-criterios-de-aceite-da-sprint)

---

## 1. Diagnostico do Estado Atual

### O que ja existe e funciona

| Componente | Localizacao | Status |
|---|---|---|
| Criacao de 3 sessoes via `CreateSession()` | `ensure_sap_sessions` | OK |
| COM marshaling main→worker (`CoMarshalInterThreadInterfaceInStream`) | `_marshal_sap_application` | OK, nunca usado |
| COM unmarshal no worker thread (`CoGetInterfaceAndReleaseStream`) | `_unmarshal_sap_application` | OK, nunca usado |
| `CoInitialize`/`CoUninitialize` por worker | `_worker_run` | OK |
| Reattach via `Sessions` collection (non-blocking) | `_reattach_dw_session` | OK |
| Validacao de prontidao (`Busy` + `findById("wnd[0]")`) | `_wait_session_ready` | OK, precisa reforco |
| Circuit breaker (10 fast fails → abort) | `_worker_run` | OK |
| Session locator por ID estavel | `DwSessionLocator` | OK |
| Distribuicao round-robin | `split_work_items_evenly` | OK |
| Escrita incremental de CSV | `write_dw_csv` | OK, nao e thread-safe |

### O que NAO funciona

| Problema | Localizacao | Impacto |
|---|---|---|
| **Workers executam SEQUENCIALMENTE** num `for` loop | `run_dw_demandante` L943 | Zero paralelismo real |
| **`maximize()` chamado a cada item** | `execute_dw_item` L601 | Rouba foco das outras sessoes → erro 619 |
| **Sem deteccao de popup/modal** | `execute_dw_item` | `findById` falha silenciosamente se tem dialog `wnd[1]` |
| **Sem reset de transacao** antes de cada item | `execute_dw_item` | Sessao pode estar em tela inesperada |
| **Constantes de circuit breaker hard-coded** | `_DW_MAX_CONSECUTIVE_FAILURES` | Nao configuravel por demandante |
| **Sem sinal de cancelamento** para workers | `_worker_run` | Nao ha como parar graciosamente |

---

## 2. Causa Raiz dos Erros em Producao

### Erro 1: `MK_E_SYNTAX` / `Sintaxe invalida` (primeira tentativa paralela)

**O que aconteceu:** `ThreadPoolExecutor` criou workers em threads COM STA separadas. `GetObject("SAPGUI")` usa a ROT (Running Object Table), que e apartment-scoped. Workers nao enxergavam o entry `SAPGUI`.

**Solucao ja no codigo:** `_marshal_sap_application` serializa a `Application` COM na main thread e `_unmarshal_sap_application` deserializa na worker thread. **Esse codigo existe mas nunca foi ativado** porque o orquestrador roda sequencial.

### Erro 2: `The control could not be found by id` (erro 619)

**O que aconteceu:** Apos corrigir o marshaling, os 3 workers rodaram em threads mas TODOS falharam com erro 619 em ~0.27s.

**Causa raiz identificada (pesquisa SAP Community):**

1. **`main_window.maximize()`** — chamado a cada item (L601). Quando worker A maximiza `ses[0]`, o SAP GUI troca o foco da janela. Workers B e C, que estao no meio de `findById`, recebem erro 619 porque o contexto de janela mudou.

2. **`connection.Children()` bloqueava** — a API `Children` trava quando qualquer sessao esta busy. Se worker A esta processando e worker B tenta `Children(1)`, fica travado ate A terminar. Ja corrigido para usar `Sessions` (non-blocking).

3. **Sessoes nao validadas apos `CreateSession()`** — O `CreateSession()` e assincrono. A sessao aparece na colecao mas a arvore UI (`wnd[0]`, `tbar[0]`, etc.) pode nao estar populada ainda.

**Conclusao:** O paralelismo COM funciona. O problema e de **concorrencia de UI no SAP GUI**, nao de threading/COM. Os fixes sao cirurgicos.

---

## 3. Arquitetura Alvo

```
                         run_dw_demandante()
                               │
                    ┌──────────┼──────────┐
                    │          │          │
              ┌─────▼────┐ ┌──▼──────┐ ┌─▼────────┐
              │ marshal   │ │ marshal │ │ marshal  │
              │ stream[0] │ │ stream[1]│ │ stream[2]│
              └─────┬────┘ └──┬──────┘ └─┬────────┘
                    │         │          │
          ┌────────▼──┐ ┌───▼──────┐ ┌─▼──────────┐
          │ Thread 1  │ │ Thread 2 │ │ Thread 3   │
          │ CoInit()  │ │ CoInit() │ │ CoInit()   │
          │ unmarshal │ │ unmarshal│ │ unmarshal  │
          │ ses[0]    │ │ ses[1]   │ │ ses[2]     │
          │           │ │          │ │            │
          │ ┌───────┐ │ │┌───────┐ │ │ ┌───────┐ │
          │ │item A │ │ ││item B │ │ │ │item C │ │
          │ │item D │ │ ││item E │ │ │ │item F │ │
          │ │item G │ │ ││item H │ │ │ │item I │ │
          │ │  ...  │ │ ││  ...  │ │ │ │  ...  │ │
          │ └───────┘ │ │└───────┘ │ │ └───────┘ │
          │ CoUninit()│ │CoUninit()│ │ CoUninit() │
          └─────┬─────┘ └────┬────┘ └──────┬─────┘
                │            │             │
                └────────────┼─────────────┘
                             │
                    CSV consolidado + manifest
```

### Principios

1. **Afinidade fixa worker↔sessao** — worker 1 SEMPRE opera em `ses[0]`, nunca toca em `ses[1]` ou `ses[2]`
2. **Sessao obtida UMA vez** por worker no inicio, reutilizada para todos os items
3. **Sem `maximize()`** no caminho quente — so no setup inicial
4. **Popup/modal detection** antes de cada operacao critica
5. **Reset de transacao** (`/nIW53`) antes de cada item para estado limpo
6. **Escrita CSV thread-safe** com `threading.Lock`
7. **Fallback sequencial** configuravel se paralelo falhar

---

## 4. Epicos e Stories

### Epico 1: Blindagem do Ciclo de Vida das Sessoes

> Pre-requisito para paralelismo. Sem estas correcoes, o modo paralelo falha com erro 619.

#### Story 1.1 — Remover `maximize()` do caminho quente

**Arquivo:** `sap_automation/dw.py`, funcao `execute_dw_item`
**Mudanca:** Remover `main_window.maximize()` da L601. O `maximize()` ja e chamado durante `ensure_sap_sessions` no setup. Nao precisa repetir a cada item.
**Risco:** Zero — janelas SAP nao des-maximizam sozinhas.
**Impacto:** Elimina a causa raiz do erro 619 em modo paralelo.

#### Story 1.2 — Validacao robusta de prontidao da sessao

**Arquivo:** `sap_automation/dw.py`, funcao `_wait_session_ready`
**Mudanca:** Alem de `Busy == False` + `findById("wnd[0]")`, validar tambem:
- `session.ActiveWindow` nao e None
- Tipo da janela nao e `"GuiModalWindow"` (popup aberto)
- Campo OK code (`wnd[0]/tbar[0]/okcd`) acessivel — prova que arvore UI esta completa

**Por que:** `findById("wnd[0]")` pode ter sucesso antes da toolbar estar carregada. O campo OK code e o ultimo a aparecer.

#### Story 1.3 — Deteccao e tratamento de popup/modal

**Arquivo:** `sap_automation/dw.py`
**Nova funcao:** `_dismiss_popup_if_present(session, logger) -> bool`

**Logica:**
1. Tenta `session.findById("wnd[1]")` — se existe, tem popup
2. Le tipo do popup e mensagem para logging
3. Tenta `sendVKey(0)` (Enter) para dispensar dialogo informativo
4. Se falhar, tenta botoes `btn[0]` ou `SPOP-OPTION1`
5. Retorna `True` se dispensou, `False` caso contrario

**Call sites:** Inicio de `execute_dw_item` + apos cada `wait_not_busy`

#### Story 1.4 — Reset de transacao antes de cada item

**Arquivo:** `sap_automation/dw.py`, funcao `execute_dw_item`
**Mudanca:** A funcao ja usa `/nIW53` via `_normalize_transaction_code`. O reforco e:
- Antes de `set_text` do OK code, chamar `_dismiss_popup_if_present`
- Se `sendVKey(0)` falhar apos setar transacao, tentar `/n` (volta ao menu) + re-navegar

**Por que:** Se a sessao esta numa tela inesperada (popup, tela de erro), o `/nIW53` falha. Precisamos limpar antes.

#### Story 1.5 — Recuperacao de sessao no retry

**Arquivo:** `sap_automation/dw.py`, funcao `_process_dw_item_with_retry`
**Mudanca:** Quando `attempt > 1` (retry), apos reattach:
1. `_dismiss_popup_if_present(session, logger)`
2. `set_text(session, _DW_OKCODE_ID, "/n")` + `sendVKey(0)` — volta ao menu
3. `wait_not_busy(session)`

**Por que:** A sessao pode estar numa tela de erro ou com popup aberto. Sem limpar, o retry falha igual.

---

### Epico 2: Execucao Paralela Real via ThreadPoolExecutor

> Nucleo da sprint. Transforma o loop sequencial em execucao paralela com 3 threads.

#### Story 2.1 — Marshaling no thread principal e distribuicao

**Arquivo:** `sap_automation/dw.py`, funcao `run_dw_demandante`
**Mudanca:** Substituir o `for` loop sequencial (L943-970) por:

```python
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. Marshal application para cada worker
marshaled_streams = _marshal_sap_application(len(groups), logger)

# 2. Executar workers em paralelo
with ThreadPoolExecutor(max_workers=len(groups)) as executor:
    futures = {
        executor.submit(
            _worker_run,
            worker_index=wi,
            session_locator=session_locators[wi - 1],
            items=group,
            settings=settings,
            logger=logger,
            marshaled_app_stream=marshaled_streams[wi - 1],
        ): wi
        for wi, group in enumerate(groups, start=1)
    }
    for future in as_completed(futures):
        wi = futures[future]
        worker_results, worker_failures = future.result()
        # coletar resultados (mesma logica atual)
```

**Decisoes tecnicas:**
- `_worker_run` ja tem `_co_initialize()`/`CoUninitialize()` — nada muda
- `_worker_run` ja aceita `marshaled_app_stream` — nada muda
- Cada worker recebe seu `session_locator` exclusivo — afinidade fixa
- `logger` e thread-safe (modulo `logging` do Python serializa writes)
- `max_workers=len(groups)` garante 1 thread por sessao, sem contencao

#### Story 2.2 — Coleta thread-safe de resultados e escrita CSV

**Arquivo:** `sap_automation/dw.py`
**Mudanca:** Criar um `threading.Lock` para proteger:
- `data_rows` (array de dados do CSV)
- `write_dw_csv()` (escrita em disco)
- Contador `successful_rows`

```python
csv_lock = threading.Lock()

for future in as_completed(futures):
    worker_results, worker_failures = future.result()
    failed_rows.extend(worker_failures)  # extend e thread-safe em CPython
    with csv_lock:
        for result in worker_results:
            data_index = row_index_to_data_index[result.row_index]
            data_rows[data_index][output_column_index] = result.observacao
            successful_rows += 1
        if worker_results:
            write_dw_csv(...)
```

**Por que incremental:** Se o processo crashar apos worker 1 completar, os resultados de worker 1 ja estao salvos no CSV.

#### Story 2.3 — Fallback sequencial configuravel

**Arquivo:** `sap_automation/dw.py`
**Nova config:** `"parallel_mode": true` no perfil DW (default: `true`)
**Novo campo:** `DwSettings.parallel_mode: bool`

**Logica em `run_dw_demandante`:**
- Se `settings.parallel_mode is False` ou `settings.session_count <= 1` → loop sequencial (comportamento atual)
- Senao → `ThreadPoolExecutor` (Story 2.1)

**Por que:** Kill-switch para producao. Se o modo paralelo apresentar problemas, basta trocar `"parallel_mode": false` no JSON e reiniciar.

---

### Epico 3: Distribuicao de Trabalho

#### Story 3.1 — Manter round-robin como default

A funcao `split_work_items_evenly` (L276) ja distribui items por modulo. E simples, testada, e funciona bem quando todos os items tem tempo de processamento similar.

**Nenhuma mudanca necessaria** nesta funcao.

#### Story 3.2 — Fila dinamica como opcao futura (P2)

**Nova config:** `"work_distribution": "queue"` (opcoes: `"round_robin"`, `"queue"`)
**Prioridade:** P2 — implementar so se round-robin mostrar desbalanceamento em producao

**Conceito:**
- Criar `queue.Queue` com todos os items
- Cada worker puxa items com `get_nowait()` ate a fila esvaziar
- Worker que processa mais rapido pega mais items automaticamente

**Beneficio:** Quando items tem tempo variavel (reclamacoes com texto longo vs vazio), a fila se auto-balanceia.

**Decisao:** Comecar com round-robin. Se os logs mostrarem desbalanceamento significativo (worker 1 termina em 10min, worker 3 em 30min), implementar fila na proxima sprint.

---

### Epico 4: Estado por Worker e Observabilidade

#### Story 4.1 — Dataclass `DwWorkerState`

**Arquivo:** `sap_automation/dw.py`
**Nova dataclass:**

```python
@dataclass
class DwWorkerState:
    worker_index: int
    session_id: str
    status: str = "idle"  # idle | running | completed | failed | circuit_breaker
    items_total: int = 0
    items_processed: int = 0
    items_ok: int = 0
    items_failed: int = 0
    last_ok_complaint_id: str = ""
    current_complaint_id: str = ""
    consecutive_failures: int = 0
    elapsed_seconds: float = 0.0
```

**Uso:** Cada worker atualiza SEU proprio `DwWorkerState`. Sem contencao porque cada worker escreve em chave unica do dict.

#### Story 4.2 — Progresso no `_worker_run`

**Arquivo:** `sap_automation/dw.py`, funcao `_worker_run`
**Mudanca:** Aceitar `worker_state: DwWorkerState | None = None`. Atualizar campos a cada item:
- `status = "running"` no inicio
- `current_complaint_id` antes de processar
- `items_processed += 1` apos processar
- `items_ok` ou `items_failed` conforme resultado
- `status = "completed"` ou `"circuit_breaker"` no fim

**Logging:** O log de progresso (a cada 50 items) ja existe. Adicionar o `worker_state` nao muda o logging.

#### Story 4.3 — Worker states no manifest

**Arquivo:** `sap_automation/dw.py`, `DwManifest`
**Mudanca:** Adicionar campo `worker_states: list[dict[str, Any]]` ao manifest.
**Resultado:** O manifest final inclui estado de cada worker — util para diagnostico pos-mortem.

---

### Epico 5: Shutdown Graceful

#### Story 5.1 — Sinal de cancelamento via `threading.Event`

**Arquivo:** `sap_automation/dw.py`
**Mudanca em `_worker_run`:** Aceitar `cancel_event: threading.Event | None = None`. No topo de cada iteracao:

```python
if cancel_event is not None and cancel_event.is_set():
    # Marcar items restantes como skipped
    break
```

**Mudanca em `run_dw_demandante`:** Criar o evento antes do executor. Se controller detectar que TODOS os workers entraram em circuit-breaker, setar o evento para parar tudo.

#### Story 5.2 — Preservacao de resultados parciais

**Ja implementado parcialmente** — CSV e escrito apos cada worker completar. Em modo paralelo com `as_completed`, cada future que resolve dispara escrita. Se 2 de 3 workers completarem e o 3o falhar, os resultados dos 2 ja estao no disco.

---

### Epico 6: Configuracao e Resiliencia

#### Story 6.1 — Novas chaves de configuracao

**Arquivo:** `sap_iw69_batch_config.json`, secao `dw.demandantes.DW`

```json
{
    "session_count": 3,
    "transaction_code": "IW53",
    "parallel_mode": true,
    "circuit_breaker_fast_fail_threshold": 10,
    "circuit_breaker_slow_fail_threshold": 30,
    "per_step_timeout_transaction_seconds": 30,
    "per_step_timeout_query_seconds": 180,
    "session_recovery_mode": "soft"
}
```

| Chave | Tipo | Default | Descricao |
|---|---|---|---|
| `parallel_mode` | bool | `true` | `false` = loop sequencial, `true` = ThreadPoolExecutor |
| `circuit_breaker_fast_fail_threshold` | int | `10` | Consecutive fast fails (< 0.5s) para abort |
| `circuit_breaker_slow_fail_threshold` | int | `30` | Consecutive failures totais para abort |
| `per_step_timeout_transaction_seconds` | float | `30` | Timeout para navegar para transacao |
| `per_step_timeout_query_seconds` | float | `180` | Timeout para executar busca |
| `session_recovery_mode` | str | `"soft"` | `"soft"` = re-navega, `"hard"` = fecha+reabre sessao |

#### Story 6.2 — Atualizar `DwSettings` e `load_dw_settings`

**Arquivo:** `sap_automation/dw.py`
**Mudanca:** Adicionar campos ao `DwSettings` frozen dataclass. Atualizar `load_dw_settings` para ler do config com defaults sensiveis.

#### Story 6.3 — Substituir constantes hard-coded

**Mudanca:** Trocar `_DW_MAX_CONSECUTIVE_FAILURES` por `settings.circuit_breaker_fast_fail_threshold` e o multiplicador `* 3` por `settings.circuit_breaker_slow_fail_threshold`.

---

## 5. Sequenciamento de Implementacao

```
Fase 1 ──────────────────────────────────────────────────
  Epico 1: Blindagem de sessoes (Stories 1.1–1.5)
  Pre-requisito para tudo. Pode ser testado em modo sequencial.

Fase 2 ──────────────────────────────────────────────────
  Epico 2: Paralelismo real (Stories 2.1–2.3)
  Nucleo da sprint. Depende da Fase 1.

Fase 3 ──────────────────────────────────────────────────
  Epico 4: Estado por worker (Stories 4.1–4.3)
  Epico 5: Shutdown graceful (Stories 5.1–5.2)
  Constroem sobre a infra paralela.

Fase 4 ──────────────────────────────────────────────────
  Epico 6: Configuracao (Stories 6.1–6.3)
  Epico 3: Fila dinamica (Story 3.2, P2)
  Polimento final.

Fase 5 ──────────────────────────────────────────────────
  Testes completos (unitarios + thread-safety + integracao Windows)
```

### Dependencias entre stories

| Story | Depende de |
|---|---|
| 2.1 (ThreadPoolExecutor) | 1.1 (remover maximize) — obrigatorio |
| 4.2 (progresso no worker) | 4.1 (DwWorkerState) |
| 5.1 (cancel event) | 2.1 (modo paralelo) |
| 6.3 (substituir constantes) | 6.2 (campos no DwSettings) |
| 2.2 (CSV thread-safe) | 2.1 (ThreadPoolExecutor) |
| 1.5 (recovery no retry) | 1.3 (popup detection) |

---

## 6. Estrategia de Testes

### Testes unitarios (Linux, sem SAP)

| Teste | O que valida |
|---|---|
| `test_dismiss_popup_returns_false_when_no_popup` | `findById("wnd[1]")` lanca exception → retorna `False` |
| `test_dismiss_popup_dismisses_info_dialog` | `findById("wnd[1]")` retorna fake modal → `sendVKey(0)` chamado |
| `test_execute_dw_item_does_not_call_maximize` | Fake window nao recebe `maximize()` durante `execute_dw_item` |
| `test_worker_state_updates_during_processing` | Campos de `DwWorkerState` atualizados corretamente |
| `test_cancel_event_stops_worker` | Setar evento apos 2 items → restantes skipped |
| `test_settings_loads_parallel_mode_and_thresholds` | Novos campos do config carregados com defaults |

### Teste de thread-safety (Linux, sem SAP)

| Teste | O que valida |
|---|---|
| `test_parallel_workers_do_not_interfere` | 3 workers com fake sessions no ThreadPoolExecutor, todos items processados, sem contaminacao cruzada |
| `test_csv_write_thread_safe` | 3 writers concorrentes com lock, CSV final tem todos os dados |

### Testes de integracao (Windows, com SAP GUI)

| Teste | O que valida |
|---|---|
| `test_3_sessions_created_and_ready` | `ensure_sap_sessions` cria 3 sessoes, todas passam `_wait_session_ready` |
| `test_parallel_execution_10_items` | 10 reclamacoes processadas em 3 sessoes paralelas, resultados corretos |
| `test_circuit_breaker_triggers_on_invalid_ids` | IDs invalidos disparam circuit breaker em < 30s |

Marcados com `@pytest.mark.skipif(sys.platform != "win32")` e `@pytest.mark.integration`.

---

## 7. Configuracao Final

### `sap_iw69_batch_config.json` — secao DW apos sprint

```json
{
    "dw": {
        "default_demandante": "DW",
        "demandantes": {
            "DW": {
                "input_path": "BASE RECLAMAÇÕES 2026- ATUALIZADO(BASE) (1)(1).csv",
                "input_encoding": "cp1252",
                "delimiter": "\t",
                "id_column": "ID Reclamação",
                "output_column": "OBSERVAÇÃO",
                "transaction_code": "IW53",
                "session_count": 3,
                "parallel_mode": true,
                "post_login_wait_seconds": 6,
                "max_rows_per_run": 3000,
                "circuit_breaker_fast_fail_threshold": 10,
                "circuit_breaker_slow_fail_threshold": 30,
                "per_step_timeout_transaction_seconds": 30,
                "per_step_timeout_query_seconds": 180,
                "session_recovery_mode": "soft"
            }
        }
    }
}
```

---

## 8. Riscos e Mitigacoes

| Risco | Prob. | Impacto | Mitigacao |
|---|---|---|---|
| Erro 619 persiste apos remover `maximize` | Baixa | Alto | Stagger de 1s entre inicio dos workers; se falhar, sleep de 500ms entre operacoes de UI |
| Dialogo nativo Windows rouba foco | Media | Medio | DW nao exporta arquivos — so le texto. Sem dialogo de export |
| Logger com contencao em 3 threads | Baixa | Baixo | `logging` Python e thread-safe por design. Nenhuma acao |
| CSV corrompido se crash durante write | Media | Medio | Lock + write para temp file + rename atomico |
| Limite de sessoes SAP excedido | Baixa | Alto | Config ja limita a 3; SAP permite ate 6 por conexao (`rdisp/max_alt_modes`) |
| Worker A trava esperando `Children()` | Baixa | Alto | Ja migrado para `Sessions` (non-blocking). `Children` so como fallback |
| `CoMarshalInterThreadInterfaceInStream` falha | Baixa | Alto | Cada stream e single-use; indice do list corresponde ao worker. Se falhar, worker retorna failure total e orquestrador continua com os outros |
| Ganho de performance < 3x | Alta | Baixo | Esperado — gargalo pode ser o servidor SAP, nao o script. Mesmo 1.5x-2x ja justifica |

---

## 9. Criterios de Aceite da Sprint

### Must Have (sem estes, sprint falha)

- [ ] 3 sessoes SAP processam items EM PARALELO via `ThreadPoolExecutor`
- [ ] `maximize()` removido do caminho quente
- [ ] Popup/modal detection ativo antes de cada operacao
- [ ] Circuit breaker funciona em cada thread independente
- [ ] CSV escrito de forma thread-safe com resultados incrementais
- [ ] `parallel_mode: false` funciona como fallback sequencial
- [ ] Todos os testes unitarios passam
- [ ] Teste de thread-safety com fake sessions passa

### Should Have

- [ ] `DwWorkerState` no manifest para diagnostico
- [ ] `cancel_event` para shutdown graceful
- [ ] Thresholds de circuit breaker configuraveis
- [ ] Timeouts por etapa configuraveis

### Nice to Have (P2, proxima sprint)

- [ ] Fila dinamica (`queue.Queue`) como alternativa a round-robin
- [ ] Endpoint de progresso no API (`GET /progress`)
- [ ] Recovery mode `"hard"` (fecha+reabre sessao)

---

## Anexo: Fundamentacao Tecnica

### Por que `Sessions` e nao `Children`

> SAP GUI Scripting API — "Collection vs. Children, the Busy Difference" (SAP Community):
> A propriedade `Children` da classe `GuiConnection` **bloqueia a execucao** antes de ser possivel usar a propriedade `Busy`. O script fica travado na linha que acessa `connection.Children` ate a sessao estar disponivel novamente.

`connection.Sessions` retorna imediatamente, sem bloqueio.

### Por que COM marshaling funciona

Cada thread COM em Python opera num STA (Single-Threaded Apartment). Objetos COM criados num STA nao sao visiveis em outro. O marshaling serializa o ponteiro IDispatch da `Application` num stream binario que pode ser transportado entre threads. Cada stream e consumido exatamente uma vez com `CoGetInterfaceAndReleaseStream`, que cria um proxy COM valido no STA da worker thread.

### Por que `maximize()` causa erro 619

O SAP GUI e uma aplicacao single-window-focus. Quando `maximize()` e chamado numa sessao, o sistema operacional traz aquela janela para frente. Outras sessoes perdem o status de "janela ativa". Operacoes COM como `findById` que dependem da arvore visual da janela falham com "control could not be found by id" porque o SAP Frontend Server nao resolve IDs de controles em janelas nao-ativas durante transicoes de foco.

### Referencia de fontes

- [SAP GUI Scripting: Collection vs. Children](https://community.sap.com/t5/technology-blog-posts-by-members/sap-gui-scripting-collection-vs-children-the-busy-difference/ba-p/13308941)
- [How to Execute SAP GUI Scripting Parallel](https://blogs.sap.com/2021/08/26/how-to-execute-sap-gui-scripting-parallel/)
- [SAP GUI Scripting API 7.61 (PDF)](https://help.sap.com/doc/9215986e54174174854b0af6bb14305a/760.01/en-US/sap_gui_scripting_api_761.pdf)
- [Error 619 discussions — SAP Community](https://community.sap.com/t5/technology-q-a/vba-sap-gui-script-error-quot-619-quot-control-could-not-be-found-by-id/qaq-p/630772)
- [Python multiprocessing SAP GUI (GitHub Gist)](https://gist.github.com/marvintensuan/4b6254dbda81397752b4e07416500fee)
- [rdisp/max_alt_modes — SAP Community](https://community.sap.com/t5/technology-q-a/parameter-rdisp-max-alt-modes-multiple-sessions/qaq-p/2833065)
- [CoMarshalInterThreadInterfaceInStream rules](https://devblogs.microsoft.com/oldnewthing/20151021-00/?p=91311)
