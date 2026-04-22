# Sprint 2 — Resiliência, Orquestração e Padronização de Workers

## Objetivo

Consolidar o modelo de execução paralela do projeto, extraindo a lógica duplicada de workers (presente em `IW51` e `DW`) para um núcleo comum de execução, e implementar mecanismos avançados de resiliência, incluindo retentativas inteligentes de conexão SAP e suporte a execuções retomáveis (Resumable Runs) no `BatchOrchestrator`.

---

## Escopo

| Item | Dentro do escopo | Fora do escopo |
|------|-----------------|----------------|
| Refatoração `SapWorkerPool` (Core) | Sim | — |
| Padronização de Marshalling COM | Sim | — |
| Mecanismo de Heartbeat e Telemetria | Sim | — |
| Retentativas (RetryPolicy) em `get_session` | Sim | — |
| Resumable Runs no `BatchOrchestrator` | Sim | — |
| Migração completa de `IW51` e `DW` para o Core | — | Sprint 3 (Spike aqui) |
| Dashboard de monitoramento real-time | — | Backlog |

---

## Contexto Técnico

### Estado atual

O projeto possui dois fluxos paralelos maduros (`IW51` e `DW`), mas ambos implementam sua própria lógica de:
1.  **Marshalling COM:** Necessário para passar objetos SAP entre threads.
2.  **Gerenciamento de Sessão:** Abertura de janelas extras via `/o` e re-attach por locator (con/ses).
3.  **Estados de Worker:** Dataclasses `WorkerState` quase idênticas.
4.  **Circuit Breaker:** Lógica de `fast_fail` e interrupção por falhas consecutivas.

Além disso, o `BatchOrchestrator` (fluxo `IW69`) é puramente sequencial e "frágil": se a conexão SAP cair no meio de um run ID de 3 objetos, o run falha e precisa ser reiniciado do zero, desperdiçando o trabalho já feito.

---

## Arquitetura de Módulos (Proposta)

### 1. `sap_automation/execution.py` (Expansão)

**Novas Abstrações:**

- `SapRetryPolicy`: Define estratégias de backoff e filtros de exceções retentáveis (ex: RPC Server Unavailable).
- `SapWorker`: Interface base para tarefas que rodam dentro do pool.
- `SapParallelPool`: Orquestrador de threads/processos que gerencia o ciclo de vida do marshalling COM e distribuição de `WorkItems`.

### 2. `sap_automation/resilience.py` (Novo)

**Responsabilidade:** Centralizar a inteligência de recuperação de erros.

```python
class ResilienceManager:
    @staticmethod
    def is_retriable(exc: Exception) -> bool: ...
    @staticmethod
    def is_terminal(exc: Exception) -> bool: ... # Erros de negócio (ex: PN inválido)
```

### 3. `sap_automation/batch.py` (Melhoria)

**Resumable State:**
O `BatchOrchestrator` passará a consultar um `state.json` (ou usar o próprio `batch_manifest.json` parcial) antes de iniciar. Se um objeto `CA` já consta como `success` para aquele `run_id`, ele será pulado.

---

## Detalhamento das Tarefas

### T1: Extração do `SapWorkerPool`
- Criar `sap_automation/worker_core.py`.
- Implementar `COMStreamManager` para encapsular `CoMarshalInterThreadInterfaceInStream`.
- Definir `BaseWorkerState` com telemetria básica (items_ok, items_failed, elapsed).
- Implementar o loop de processamento com suporte a `CircuitBreaker` configurável.

### T2: Retry Automático no Bootstrap
- Modificar `LogonPadSessionProvider.get_session` para usar `SapRetryPolicy`.
- Adicionar suporte a `max_retries` e `exponential_backoff` na conexão inicial com o SAP Logon Pad.
- Implementar detecção de "zombie sessions" (sessões que existem no COM mas estão desconectadas do servidor).

### T3: Batch Resumption (Checkpointing)
- Implementar `CheckpointManager` no `BatchOrchestrator`.
- Gravar progresso após cada objeto bem-sucedido.
- Adicionar flag `--resume` (ou tornar default baseado no `run_id` existente).
- Garantir que a consolidação final considere objetos de múltiplas tentativas.

### T4: Telemetria e Heartbeat
- Padronizar o envio de `worker_heartbeat` para o logger/manifesto.
- Adicionar `timestamp_start` e `timestamp_end` por item de trabalho para análise de performance.
- Incluir `memory_usage` e `cpu_usage` (opcional via `psutil`) no estado do worker.

---

## Definição de Pronto (DoP)
- [ ] `SapWorkerPool` testado com mock de objetos COM.
- [ ] `BatchOrchestrator` consegue retomar um run após falha simulada (kill process).
- [ ] `LogonPadSessionProvider` recupera conexão após erro de "RPC Server Unavailable".
- [ ] Manifesto do Batch inclui telemetria detalhada de execução.
- [ ] Zero regressão nos fluxos sincronos legados.
