# README SAP Operacional

## Implementacao atual

O repositorio agora contem uma entrega executavel da automacao modular focada em `IW69`, com `IW59` como etapa complementar, `IW51` como fluxo dedicado para a Dani e `DW` como fluxo de leitura de observacoes a partir da base de reclamacoes.

O deploy agora segue modelo hibrido:
- `control plane` containerizado: API FastAPI, scheduler, Postgres e Redis
- `sap-runner-windows` fora do Docker: processo residente que consome jobs e executa SAP GUI/COM no desktop interativo

O modo hibrido preserva totalmente o comportamento antigo:
- os endpoints legados `/api/v1/extractions/...` continuam **sincronos** e executam o fluxo imediatamente
- os endpoints novos `/api/v1/jobs/...` fazem **enfileiramento assincrono** para uso com scheduler + runner Windows

Observacao de escopo: o fluxo `IW59` agora existe como etapa complementar pós-`IW69`, dirigida pelas notas do universo `CA`. `IW67` continua apenas como placeholder.

### Entry points

- `sap_iw69_batch.py`: runner batch oficial para executar `CA`, `RL` e `WB` em uma unica chamada.
- `sap_iw51_dani.py`: runner CLI para o fluxo `IW51` da demandante `DANI`.
- `sap_dw.py`: runner CLI para o fluxo `DW`, lendo `ID Reclamação` de uma base CSV e preenchendo `OBSERVAÇÃO`.
- `sap_automation/api.py`: app FastAPI com dois modos: endpoints legados sincronos de extracao e endpoints novos de jobs/schedules/runners para o control plane.
- `sap_automation/scheduler.py`: processo de agenda persistida para materializar jobs por horario.
- `sap_automation/runner.py`: runner Windows para consumir jobs e executar os fluxos SAP serialmente.
- `sap_gui_export_compat.py`: camada de compatibilidade do runner legado por objeto, reutilizada pelo batch.
- `sap_iw69_batch_config.json`: configuracao oficial dos steps SAP GUI, com perfis de `IW69` por demandante.

### Docker e operacao 24x7

Subir o control plane:

```bash
docker compose up --build api scheduler postgres redis
```

Subir o scheduler local:

```bash
python -m sap_automation.scheduler
```

Subir o runner Windows:

```bash
python -m sap_automation.runner
```

### Exemplo de execucao

```bash
python3 sap_iw69_batch.py \
  --run-id 20260310T090000 \
  --reference 202603 \
  --from-date 2026-01-01 \
  --demandante IGOR \
  --output-root output
```

Executar o fluxo `IW51` da Dani:

```bash
python3 sap_iw51_dani.py \
  --run-id 20260326T090000 \
  --demandante DANI \
  --output-root output
```

Executar o fluxo `IW51` por HTTP no modo legado sincrono:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/extractions/iw51 \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id": "20260326T090000",
    "demandante": "DANI",
    "output_root": "output",
    "config_path": "sap_iw69_batch_config.json",
    "max_rows": 4
  }'
```

Executar o fluxo `DW`:

```bash
python3 sap_dw.py \
  --run-id 20260327T160000 \
  --demandante DW \
  --output-root output
```

### API FastAPI

Subir a API:

```bash
uvicorn sap_automation.api:app --host 0.0.0.0 --port 8000
```

Healthcheck:

```bash
curl http://127.0.0.1:8000/health
```

Executar a extracao completa `CA + RL + WB` no modo legado sincrono:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/extractions/iw69 \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id": "20260310T090000",
    "reference": "202603",
    "from_date": "2026-01-01",
    "to_date": "2026-01-31",
    "demandante": "IGOR",
    "output_root": "output",
    "objects": ["CA", "RL", "WB"],
    "config_path": "sap_iw69_batch_config.json"
  }'
```

Executar somente a `IW59` usando o `CA` ja extraido de um `run_id` existente no modo legado sincrono:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/extractions/iw59 \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id": "20260326T100000",
    "demandante": "MANU",
    "output_root": "output",
    "config_path": "sap_iw69_batch_config.json"
  }'
```

Executar a `IW59` standalone do demandante `KELLY`, filtrando por `Modificado por` a partir do CSV de `BRS`:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/extractions/iw59 \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id": "20260402T090000",
    "demandante": "KELLY",
    "output_root": "output",
    "config_path": "sap_iw69_batch_config.json",
    "input_csv_path": "brs_filtrados.csv"
  }'
```

Executar o fluxo `DW` por HTTP no modo legado sincrono:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/extractions/dw \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id": "20260327T160000",
    "demandante": "DW",
    "output_root": "output",
    "config_path": "sap_iw69_batch_config.json",
    "max_rows": 30
  }'
```

Consultar jobs:

```bash
curl http://127.0.0.1:8000/api/v1/jobs
```

Enfileirar `IW69` no modo assincrono:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/jobs/iw69 \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id": "20260310T090000",
    "reference": "202603",
    "from_date": "2026-01-01",
    "to_date": "2026-01-31",
    "demandante": "IGOR",
    "output_root": "output",
    "objects": ["CA", "RL", "WB"],
    "config_path": "sap_iw69_batch_config.json"
  }'
```

Enfileirar `IW51` no modo assincrono:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/jobs/iw51 \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id": "20260326T090000",
    "demandante": "DANI",
    "output_root": "output",
    "config_path": "sap_iw69_batch_config.json",
    "max_rows": 4
  }'
```

Enfileirar `IW59` no modo assincrono:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/jobs/iw59 \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id": "20260326T100000",
    "demandante": "MANU",
    "output_root": "output",
    "config_path": "sap_iw69_batch_config.json"
  }'
```

Enfileirar `DW` no modo assincrono:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/jobs/dw \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id": "20260327T160000",
    "demandante": "DW",
    "output_root": "output",
    "config_path": "sap_iw69_batch_config.json",
    "max_rows": 30
  }'
```

Cadastrar um schedule:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/schedules \
  -H 'Content-Type: application/json' \
  -d '{
    "schedule_id": "igor-mensal",
    "enabled": true,
    "flow_type": "iw69",
    "demandante": "IGOR",
    "cron_expression": "0 6 * * 1-5",
    "timezone": "America/Bahia",
    "payload_template": {
      "reference": "202604",
      "from_date": "2026-04-01",
      "to_date": "2026-04-30",
      "output_root": "output",
      "objects": ["CA", "RL", "WB"],
      "config_path": "sap_iw69_batch_config.json"
    }
  }'
```

Perfis de `IW69` por demandante:

- `IGOR`: o `IW59` roda automaticamente apos o batch `IW69` para cada objeto bem-sucedido entre `CA`, `RL` e `WB`; ele filtra cada universo por `statusuar = ENCE`, executa em lotes de `5000` notas e gera CSVs finais separados em `output/runs/{run_id}/iw59/normalized/iw59_ca_{reference}_{run_id}.csv`, `iw59_rl_{reference}_{run_id}.csv` e `iw59_wb_{reference}_{run_id}.csv`, além do manifesto agregado `output/runs/{run_id}/iw59/metadata/iw59_{run_id}.manifest.json`
- `IGOR`: alem do CSV canonico de cada objeto em `ca/normalized`, `rl/normalized` e `wb/normalized`, o batch agora materializa um recorte de notas abertas em `*_abertas.csv`, removendo `ENCE`, `ENCE DEFE`, `ENCE DEFE INDE`, `ENCE DUPL`, `ENCE IMPR`, `ENCE INDE` e `ENCE PROC`
- `MANU`: herda o fluxo do `IGOR`, mas sobrescreve o `CA`; as datas de `IW69` seguem exatamente o `from_date` e `to_date` enviados no request; o `IW59` filtra notas `CA` com `statusuar` em `ENCE`, `ENCE DEFE`, `ENCE DEFE INDE`, `ENCE DUPL`, `ENCE IMPR`, `ENCE INDE` e `ENCE PROC`
- `KELLY`: usa `IW59` em modo standalone por `Modificado por`; o fluxo lê `brs_filtrados.csv` na coluna `BRS`, remove duplicados e executa o universo inteiro em lotes de `100` BRs por requisição SAP
- `IW59`: a janela inteligente de `Modificado em` agora e contrato global da transacao para todos os demandantes que a utilizam; por padrao, ate o 5o dia util do mes a selecao usa o mes anterior inteiro, e depois disso usa `01/mm/aaaa` ate a data corrente do mes atual; perfis especificos ainda podem sobrescrever `transition_business_day` ou desabilitar a regra via config
- `KELLY`: usa a mesma regra inteligente do 5o dia util apenas para escolher o mes de referencia; depois divide esse mes em 3 janelas fixas de `Modificado em` (`01-10`, `11-20` e `21-fim do mes`), repetindo cada janela para todos os lotes de `100` BRs
- `KELLY`: o calendario util considera fins de semana, feriados nacionais fixos e observancias moveis amplamente usadas na operacao brasileira (`Carnaval`, `Sexta-feira Santa` e `Corpus Christi`), com suporte deterministicamente validado ate `2040`
- `KELLY`: os exports parciais sao mantidos em `output/runs/{run_id}/iw59/raw/iw59_kelly_{reference}_{run_id}_wXX_YYY.txt` e o consolidado final sai em `output/runs/{run_id}/iw59/normalized/iw59_kelly_{reference}_{run_id}.csv`

Perfil de `IW51` por demandante:

- `DANI`: lê `projeto_Dani2.xlsm`, cria uma cópia de trabalho em `output/runs/{run_id}/iw51/working/`, usa a nota modelo fixa `389496787`, preenche `PN`, `INSTALAÇÃO` e `TIPOLOGIA`, distribui os itens pendentes em 3 sessões SAP com afinidade fixa worker↔sessão, usa `process_parallel` como modo padrão de produção com 3 processos filhos isolados e um supervisor único para workbook/ledger/manifest, mantém `interleaved` como fallback seguro e `true_parallel` como opção explícita/experimental, grava progresso em `iw51_progress.csv`, aplica regra `last-entry-wins` no rerun do ledger, classifica `failed_terminal` como falha terminal, reconcilia workbook↔ledger na inicialização, sincroniza `FEITO=SIM` na cópia de trabalho em lotes de `100` itens por padrão com throttle adicional por tempo (`workbook_sync_min_interval_seconds`) e teto de pendências (`workbook_sync_max_pending_rows`) para reduzir regravações pesadas do `.xlsm`, desacopla a frequência de escrita do `iw51_manifest.json` via `manifest_sync_min_interval_seconds`, suporta `worker_start_stagger_seconds`, `worker_heartbeat_seconds`, `worker_item_timeout_seconds`, `worker_restart_limit`, `session_rebuild_backoff_seconds` e `message_filter_retry_window_seconds`, e consegue reconstruir o `iw51_progress.csv` a partir do `sap_session.log` em cenário de recuperação. O loader aceita tanto planilha com cabeçalho (`PN`, `INSTALAÇÃO`, `TIPOLOGIA`) quanto a `Macro1` sem cabeçalho, tratando as colunas `A/B/C` como `PN/INSTALAÇÃO/TIPOLOGIA` e a `D` como `FEITO`; linhas vazias intermediárias não encerram mais a leitura nem a reconciliação

Perfil de `DW` por demandante:

- `DW`: lê `BASE RECLAMAÇÕES 2026- ATUALIZADO(BASE) (1)(1).csv`, divide as notas pendentes em 3 sessões SAP com afinidade fixa worker↔sessão, extrai o texto da aba de observação e grava a coluna `OBSERVAÇÃO` no próprio CSV; por padrão o perfil processa todas as linhas pendentes (`max_rows_per_run = 0`)
- `DW`: suporta `parallel_mode=true|false` no config para alternar entre execução paralela real por worker/sessão e fallback sequencial; no perfil padrão, os 3 workers sobem com `worker_start_stagger_seconds = 2.0` e dormem `0.5s` entre um item e outro
- `DW`: usa escrita incremental e atômica do CSV, agora com batching (`csv_sync_batch_size`) e throttle temporal (`csv_sync_min_interval_seconds`) para reduzir regravações do arquivo completo; também publica `worker_states` no manifesto final para diagnóstico por sessão
- `DW`: mantém `output/runs/{run_id}/dw/dw_progress.csv` como ledger por linha; no rerun do mesmo `run_id`, sucessos já gravados no ledger são reconciliados de volta para o CSV antes de montar a fila pendente
- `DW`: também gera `output/runs/{run_id}/dw/dw_observacoes_debug.csv` com colunas simples `worker`, `complaint_id` e `observacao`, já normalizando o texto para remover cabeçalhos SAP (data/hora/usuário) e juntar quebras artificiais de linha

Consultar o manifesto agregado:

```bash
curl "http://127.0.0.1:8000/api/v1/extractions/iw69/20260310T090000/manifest?output_root=output"
```

### Logon Pad e credenciais

O fluxo de sessao agora suporta abertura da conexao SAP a partir do Logon pad, sem depender de sessao previamente aberta, quando `global.logon_pad.enabled = true` em `sap_iw69_batch_config.json`.

Credenciais:

- copiar `.env.example` para `.env`
- preencher `SAP_USERNAME` e `SAP_PASSWORD`
- o loader tambem aceita aliases legados `SAP_USER` e `SAP_PASS`
- opcionalmente preencher `SAP_CLIENT` e `SAP_LANGUAGE`
- opcionalmente preencher `SAP_POST_LOGIN_SLEEP_SECONDS` para esperar alguns segundos apos o login SAP

Configuracao relevante no JSON:

- `global.logon_pad.enabled`
- `global.logon_pad.workspace_name`
- `global.logon_pad.connection_description`
- `global.logon_pad.multiple_logon_action`
- `global.stop_on_object_failure`

Politica de execucao por objeto:

- por padrao, o batch so avanca para o proximo objeto se o objeto atual concluir com sucesso
- se `CA` falhar, `RL` e `WB` nao iniciam; se `RL` falhar, `WB` nao inicia

Logging de sessao:

- durante cada execucao, o bootstrap de conexao/login SAP escreve logs em tempo real no terminal
- o mesmo log fica persistido em `output/runs/{run_id}/logs/sap_session.log`

### Layout de saida

- `output/runs/{run_id}/ca/raw`, `.../normalized`, `.../metadata`
- `output/runs/{run_id}/rl/raw`, `.../normalized`, `.../metadata`
- `output/runs/{run_id}/wb/raw`, `.../normalized`, `.../metadata`
- `output/runs/{run_id}/iw51/working/projeto_Dani2.xlsm`
- `output/runs/{run_id}/iw51/iw51_progress.csv`
- `output/runs/{run_id}/iw51/iw51_manifest.json`
- `output/runs/{run_id}/consolidated/notes.csv`
- `output/runs/{run_id}/consolidated/interactions.csv`
- `output/latest/legacy/BASE_AUTOMACAO_CA.txt`
- `output/latest/legacy/BASE_AUTOMACAO_RL.txt`
- `output/latest/legacy/BASE_AUTOMACAO_WB.txt`

### Comportamento do batch

- cada objeto `IW69` roda de forma independente;
- falha em um objeto nao aborta os demais;
- o manifesto agregado sai em `output/runs/{run_id}/batch_manifest.json`;
- a consolidacao inicial gera base por `Nota` e base de interacoes, marcando status `partial` quando houver objetos faltantes.

### Pendencias explicitas desta fase

- `IW59`: contrato declarado, mas implementacao concreta bloqueada ate receber o script SAP GUI gravado da transacao.
- `IW67`: contrato declarado, mas implementacao concreta bloqueada ate receber o script SAP GUI gravado da transacao.
- `SLA`, calendario util/feriados e classificacao final dentro/fora do prazo ainda nao foram implementados.

## Objetivo

Consolidar a rotina operacional descrita na call `CALL - IGOR.mp4` e nos documentos auxiliares para responder:

- quais transacoes e extracoes devem ser feitas dentro do SAP;
- quais bases sao geradas;
- quais transformacoes precisam acontecer fora do SAP;
- quais regras de negocio impactam prazo, classificacao e acompanhamento;
- quais pontos ainda dependem de validacao com acesso real.

## Fontes utilizadas

Foram usados os arquivos abaixo como base de conhecimento:

- `industria_on_demand_sp/output_call_igor/transcript_full_with_timestamps.txt`
- `/home/vanys/Downloads/RL PRAZO 5.xlsx`
- `/home/vanys/Downloads/EXTRAÇÃO_NOTAS 2.xlsm`
- `/home/vanys/Downloads/ca prazo 5.xlsx`
- `/home/vanys/Downloads/Extração de relatorio 1 3.docx`
- `/home/vanys/Downloads/02.Calendário Faturamento 02_2026 Baixa Tensão.xlsb`

Observacao: o usuario mencionou "4 documentos", mas foram fornecidos 5 arquivos Office. Este README usa todos os 5.

## Sistemas e artefatos

- `SAP / Zap`: sistema usado para consulta operacional e extracao das notas.
- `Nexus`: citado na call como ferramenta/tabulador paralela, mas nao identificado como transacao SAP.
- `IW69`: transacao explicitamente citada nos documentos.
- `IW59`: transacao explicitamente citada nos documentos.
- `CRM_663` ou `ZK1663`: transacao/tela citada na transcricao para fluxo `MOP` e `fora MOP`, mas com baixa confianca no nome por ruido da fala.
- `EXTRAÇÃO_NOTAS 2.xlsm`: workbook de consolidacao que consome arquivos texto `BASE_AUTOMACAO_RL.txt`, `BASE_AUTOMACAO_WB.txt` e `BASE_AUTOMACAO_CA.txt`.
- `RL PRAZO 5.xlsx` e `ca prazo 5.xlsx`: planilhas de classificacao de prazo e pivots de dentro/fora do prazo.
- `02.Calendário Faturamento 02_2026 Baixa Tensão.xlsb`: calendario de dias uteis, feriados, lotes e janelas de faturamento.

## Transacoes SAP citadas

### Transacoes com alta confianca

| Transacao | Confianca | Finalidade operacional | Evidencia |
|---|---|---|---|
| `IW69` | alta | Extracao base de notas para RL, WB e CA | `Extração de relatorio 1 3.docx` menciona `IW69`; `EXTRAÇÃO_NOTAS 2.xlsm` tem bases `RL`, `WB` e `CA` com layout tipico dessa extracao |
| `IW59` | alta | Extracao complementar para historico/modificacao por nota | `Extração de relatorio 1 3.docx` menciona `IW59`; `RL PRAZO 5.xlsx` tem aba `iw59` com `Modificado em`, `Modificado às`, `Modificado por` |

### Transacoes/telas com media ou baixa confianca

| Transacao/Tela | Confianca | Finalidade operacional | Evidencia |
|---|---|---|---|
| `CRM_663` | media | Visao usada para fluxo `MOP` / `fora MOP` | Transcricao em `00:43:30 - 00:44:00` menciona `CRM underline 663` |
| `ZK1663` | baixa a media | Possivel nome correto da transacao especifica do fluxo `MOP` | Transcricao em `00:47:00 - 00:47:30` menciona `w52 nao ZK 1663` |
| `IW52` | baixa | Provavel ruido de transcricao, nao tratado como transacao confirmada | Mesmo trecho sugere correcao para `ZK1663`, nao confirmacao de `IW52` |

## O que deve ser feito dentro do SAP

### 1. Extrair base RL em `IW69`

Objetivo:

- obter as notas do universo RL com os campos principais para analise de prazo e classificacao.

Campos observados nas bases:

- `Nota`
- `Descricao`
- `StatUsuár.`
- `Data`
- `Hora`
- `Encerram.`
- `Concl.desj`
- `Texto` ou `Texto code parte obj`
- `PtOb`
- `Texto de code para problema`
- `Dano`
- `Criado por`

Codigos e filtros associados ao universo RL:

- `0073`
- `0085`
- `0036`
- `0072`
- `0065`
- `0144`
- `0122`
- `0123`
- `0168`
- `0172`

Esses codigos aparecem agrupados em `Extração de relatorio 1 3.docx` na linha `RL 00730085003600720065 0144 0122 0123 0168 0172`.

### 2. Extrair base WB em `IW69`

Objetivo:

- obter o universo WB com codigos de parte-objeto ligados a manifestacoes e servicos digitais/comerciais.

Codigos identificados no documento:

- `MNFD` = Manifestacao Full Digital
- `ALDV`
- `CDAU` = Cadastro de Debito Automatico
- `DFRA`
- `MILP` = Iluminacao Publica
- `REAT`
- `VMBT`
- `LNAS` = Ligacao Nova apto. e salas comerciais
- `TRNM`

Esses codigos aparecem em `Extração de relatorio 1 3.docx` na secao `WB's`.

### 3. Extrair base CA em `IW69`

Objetivo:

- obter o universo de Comunicacao entre Areas / Nao Conformidade / Fax / Gravacao.

Codigos identificados:

- `AFAX`
- `CDMC`
- `GRAV`
- `ACCO`
- `NCCI`
- `NCON`

Esses codigos aparecem em `Extração de relatorio 1 3.docx` na secao `CAs`.

### 4. Extrair historico/complemento em `IW59`

Objetivo:

- trazer o historico de modificacao para cruzar com a base principal das notas.

Campos observados:

- `Nota`
- `Status usuário`
- `Data da nota`
- `Modificado em`
- `Modificado às`
- `Modificado por`
- `Hora iníc.des.`
- `Concl.desejada`
- `Cliente`
- `Rua`
- `Instalação`
- `CenTrab respon.`

Uso pratico:

- medir interacao real por operador;
- saber quem modificou;
- cruzar alteracoes com prazo;
- identificar quando houve atuacao mesmo sem fechamento definitivo.

### 5. Consultar a tela/transacao do fluxo `MOP` / `fora MOP`

Objetivo:

- identificar notas que ficam fora do fluxo normal por divergencia.

Regra descrita na call:

- o que esta `dentro da MOP` segue o fluxo padrao;
- o que fica `fora da MOP` cai em limbo por erro, divergencia ou inconsistencias;
- essas notas precisam ser puxadas e tratadas separadamente;
- o fator decisivo e o dia atualmente em tratamento na MOP, porque puxar o mesmo dia mistura os universos.

Status:

- o fluxo existe e foi mostrado na call;
- o nome exato da transacao precisa ser confirmado no SAP com acesso real;
- o trecho mais confiavel da transcricao aponta para `CRM_663` ou `ZK1663`.

## Layouts e filtros operacionais identificados

### Layouts citados explicitamente

- `OUVIDORIA / OV_ATNP`
- `IW69 / WB LAURIA`
- `IW59 / CALLCENTER1`

### Regras de categorizacao especificas

Call center emergencial/comercial/site:

- `0047`
- `0009`
- `0010`

Aguardando fax:

- `001` = Aguardando Fax (`ccenter-religa`)
- `site` = Aguardando Fax (`ccenter-site`) no documento nao ficou numericamente completo
- `002` = Aguardando Fax (`reativa`)
- `003` a `009` e vazio = Aguardando Fax (`outros`)
- `010` = Aguardando Fax (`TN`)

Observacoes:

- o documento tambem diz `Afx tn é do Felipe macro`;
- ha uma mencao a `AFAX RELIGA – Brenda`.

## Bases e arquivos que devem ser extraidos

### Bases note-level

Do SAP devem sair pelo menos estas tres bases texto:

- `BASE_AUTOMACAO_RL.txt`
- `BASE_AUTOMACAO_WB.txt`
- `BASE_AUTOMACAO_CA.txt`

Evidencia:

- `EXTRAÇÃO_NOTAS 2.xlsm` possui conexoes de texto para:
  - `BASE_AUTOMACAO_CA.txt`
  - `BASE_AUTOMACAO_RL.txt`
  - `BASE_AUTOMACAO_WB.txt`

### Base complementar de historico

Do SAP deve sair uma base tipo `IW59` para complementar:

- modificacao da nota;
- usuario modificador;
- cliente, rua, instalacao;
- centro/cenario de trabalho responsavel.

## Transformacoes necessarias fora do SAP

### 1. Padronizar tipos e datas

As planilhas mostram dois formatos de data:

- serial do Excel/SAP (`46054`, `46059`);
- data textual (`05.02.2026`).

Necessario:

- converter tudo para `date`/`datetime`;
- separar `data`, `hora`, `encerramento`, `conclusao desejada`;
- preservar timezone/regra local da operacao.

### 2. Aplicar regra do horario SAP `+4 horas`

A call afirma explicitamente que o SAP aparece com `+4 horas` em relacao ao horario operacional.

Impacto:

- uma nota fechada `20:30` pode aparecer como fechamento do dia seguinte;
- isso afeta calculo de dentro/fora do prazo;
- isso tambem explica notas aparentemente fechadas no sabado.

Regra minima:

- toda comparacao de prazo baseada em fechamento/modificacao precisa considerar esse offset antes de classificar.

### 3. Cruzar `IW69` com `IW59` por `Nota`

Chave principal:

- `Nota`

Objetivo do cruzamento:

- trazer o evento principal da nota;
- complementar com `modificado por`, `modificado em`, `modificado às`;
- permitir contagem de interacoes por operador e nao apenas status final.

### 4. Classificar por universo de negocio

Minimo esperado:

- `RL`
- `WB`
- `CA`
- `MOP`
- `fora MOP`

Preferencialmente tambem:

- `PtOb`
- `Texto code parte obj`
- `Texto code problema`
- `Dano`

### 5. Calcular SLA / dentro e fora do prazo

As planilhas `RL PRAZO 5.xlsx` e `ca prazo 5.xlsx` ja trazem pivots de `Dentro do Prazo` e `Fora Prazo`.

Para reproduzir isso fora do Excel:

- definir data/hora efetiva de inicio;
- definir data/hora efetiva de encerramento ou ultima acao;
- aplicar correcao de `+4 horas` do SAP;
- usar calendario de dias uteis e feriados;
- comparar contra `Concl.desj` e regras especificas do tipo.

### 6. Aplicar calendario de dias uteis e feriados

O arquivo `02.Calendário Faturamento 02_2026 Baixa Tensão.xlsb` traz:

- feriados;
- funcao/parametro `diatrabalho`;
- observacoes de apresentacao e vencimento;
- quantidade de dias uteis entre apresentacao e vencimento;
- lotes e cronograma mensal.

Pontos explicitos extraidos:

- feriados em fevereiro/2026: `16/02/2026` e `17/02/2026` (Carnaval);
- `5 dias uteis` entre apresentacao e vencimento na regra geral;
- algumas excecoes de lotes com `6 a 8 dias uteis`;
- impressao/remessa/apresentacao de lotes secundarios em marcos subsequentes.

Mesmo que esse calendario tenha sido feito para faturamento, ele deve ser usado como fonte de feriados/dias uteis quando a rotina de prazo depender da mesma agenda corporativa.

### 7. Contabilizar interacoes, nao apenas encerramentos

Este e um ponto central da call:

- olhar so o estado final da nota perde trabalho executado no meio do fluxo;
- se uma nota foi tratada varias vezes por operadores diferentes, isso precisa aparecer;
- `Zap`/`SAP` podem mostrar apenas o fim da historia, nao o caminho.

Portanto, o output operacional deve ter pelo menos dois niveis:

- `nota consolidada`;
- `interacao/acao por operador`, quando disponivel.

## Regras de negocio identificadas

### Regras gerais

- O status final da nota nao e suficiente para medir produtividade.
- A base de acompanhamento precisa considerar historico/modificacao.
- O sistema pode travar com volume alto de extracao.
- A operacao usa planilha de acompanhamento paralela para controlar fechados e abertos.
- Parte da automacao atual ainda depende de macro e conversao manual de TXT.

### Regras de prazo por tipo

As seguintes regras aparecem no `.docx` e devem ser tratadas como parametrizacao inicial:

| Tipo / Codigo | Prazo indicado |
|---|---|
| `ACCO` | `10` |
| `AFAX RELIGA` | `24h` |
| `AFAX COMPROVANTE` | `3` |
| `AFAX TN` | `3` |
| `CDMC` | `5` |
| `GRAV` | `5` |
| `MOP` | `3` |
| `NCON` / `Nao Conformidade` | `10` |
| `Alteracao Data de Vencimento` | `1` |
| `Cadastro Debito Automatico` | `3` |
| `Iluminacao Publica` | `3` |
| `Ligacao Nova Sala Comercial` | `3` |

Observacao:

- o documento nao explicita se todos os numeros estao em dias corridos, dias uteis ou outra unidade;
- `AFAX RELIGA` esta explicitamente em `24h`;
- os demais devem ser confirmados com a operacao antes de virarem regra automatica definitiva.

### Regras de `MOP` / `fora MOP`

- `Dentro da MOP`: segue o fluxo normal da operacao.
- `Fora da MOP`: notas com erro/divergencia que nao conseguiram subir para o fluxo correto.
- Essas notas ficam em limbo e precisam de tratamento especifico.
- Nao se deve puxar o mesmo dia que esta sendo trabalhado dentro da MOP, para nao misturar universos.
- O gargalo operacional acontece quando o tratamento dentro da MOP atrasa e segura a liberacao do `fora MOP`.

## O que deve ser extraido de cada fonte

### De `IW69`

Extrair tres familias de base:

- `RL`
- `WB`
- `CA`

Campos minimos:

- `Nota`
- `Descricao`
- `Status usuario`
- `Data`
- `Hora`
- `Encerram.`
- `Concl.desj`
- `Texto code parte obj`
- `PtOb`
- `Texto code problema`
- `Dano`
- `Criado por`

### De `IW59`

Extrair:

- `Nota`
- `Modificado em`
- `Modificado às`
- `Modificado por`
- `Cliente`
- `Rua`
- `Instalacao`
- `Cenario/Centro de trabalho responsavel`

### Da transacao/tela `MOP`

Extrair:

- notas dentro da MOP;
- notas fora da MOP;
- dia/lote em tratamento;
- condicao de divergencia ou erro que impede subida ao fluxo normal.

### Das planilhas de prazo

Usar como referencia:

- validacao de pivot `Dentro/Fora`;
- agrupamento por gestor/analista/tipo;
- exemplos de categorizacao real em producao.

### Do calendario

Usar como referencia:

- feriados;
- dias uteis;
- lotes;
- janelas de faturamento/apresentacao/vencimento.

## Fluxo operacional recomendado

1. Extrair `RL`, `WB` e `CA` no SAP via `IW69`.
2. Extrair base complementar no SAP via `IW59`.
3. Extrair ou consultar o universo `MOP / fora MOP` na transacao/tela especifica.
4. Salvar as bases brutas em TXT/CSV padronizado.
5. Converter para tabela estruturada.
6. Normalizar datas, horas e codigos.
7. Aplicar correcao do horario `+4 horas` do SAP.
8. Cruzar bases por `Nota`.
9. Classificar universo de negocio e subtipo.
10. Calcular SLA com calendario e regras de prazo.
11. Gerar saidas analiticas:
    - nota consolidada;
    - interacoes por operador;
    - dentro/fora do prazo;
    - backlog `MOP` e `fora MOP`.

## Saidas analiticas desejadas

- base consolidada por `Nota`;
- base de interacoes por `Nota x Operador`;
- painel `Dentro/Fora do Prazo`;
- backlog de `abertas`, `encerradas`, `redirecionadas`;
- fila `MOP`;
- fila `fora MOP`;
- contagem por gestor, operador, tipo e codigo.

## Nuances importantes

- Nem tudo que esta no SAP aparece no layout exportado.
- O layout de extracao influencia diretamente o que sera possivel medir.
- Parte da operacao ainda usa macro e conversao de TXT para Excel.
- Sem acesso as transacoes/telas corretas nao da para fechar a automacao ponta a ponta.
- O nome exato da transacao `CRM_663` / `ZK1663` precisa ser confirmado.
- `Nexus` foi citado como ferramenta de tabulacao/apoio, nao como transacao SAP confirmada.

## Transacoes SAP consolidadas para retorno rapido

Estas sao as transacoes/telas que aparecem como necessarias no material analisado:

- `IW69`
- `IW59`
- `CRM_663` ou `ZK1663` para `MOP / fora MOP` (confirmar nome exato no ambiente)

Itens citados mas nao tratados aqui como transacao SAP confirmada:

- `Nexus`
- `Zap` como nome operacional do sistema/ambiente
- codigos de negocio como `AFAX`, `ACCO`, `MNFD`, `NCON`, `CDAU`, `MILP`, `LNAS`

## Lacunas para validacao com a operacao

- Confirmar o nome correto da transacao `MOP / fora MOP`.
- Confirmar a unidade de prazo de cada codigo (`dias uteis`, `dias corridos` ou `horas`).
- Confirmar se o `+4 horas` vale para toda classificacao ou apenas para determinados eventos.
- Confirmar se `IW59` e suficiente para recuperar todas as interacoes ou se existe outra transacao de historico mais completa.
- Confirmar o layout oficial que deve ser exportado de `IW69` e `IW59`.
