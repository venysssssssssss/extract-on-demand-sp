# Sprint 1 — Automação do SAP Logon Pad: Conexão e Autenticação

## Objetivo

Automatizar o ciclo de vida pré-sessão do SAP GUI: selecionar o workspace **00 SAP ERP**, abrir a conexão **H181 RP1 ENEL SP CCS Produção (without SSO)**, e autenticar com credenciais carregadas de `.env`. Ao final desta sprint, o `BatchOrchestrator` será capaz de iniciar uma sessão SAP do zero, sem intervenção manual no SAP Logon pad.

---

## Escopo

| Item | Dentro do escopo | Fora do escopo |
|------|-----------------|----------------|
| Abertura de conexão via COM | Sim | — |
| Login com credenciais do `.env` | Sim | — |
| Tratamento de popup de logon múltiplo | Sim | — |
| `.env.example` com variáveis necessárias | Sim | — |
| Feature flag no config JSON | Sim | — |
| Retry automático de conexão | — | Sprint futura |
| Suporte a SSO | — | Sprint futura |
| Automação de IW59/IW67 | — | Sprints 2+ |

---

## Contexto Técnico

### Estado atual

O `SapSessionProvider` (`sap_automation/execution.py`) é um wrapper fino que delega para `sap_gui_export_compat.connect_sap_session(config)`. Essa função usa acesso COM baseado em índice (`connection_index: 0`, `session_index: 0`) e **assume que a conexão SAP já está aberta manualmente**. Não existe:

- Seleção de workspace no SAP Logon pad
- Abertura de conexão por nome/descrição
- Carregamento de credenciais via `.env`
- Hierarquia de erros específicos para falhas de conexão/login

### Modelo COM do SAP GUI

```
win32com.client.Dispatch("SapROTWr.SapROTWrapper")
  └─ GetROTEntry("SAPGUI")
       └─ GetScriptingEngine()  →  Application
            ├─ .OpenConnection(description, sync)  →  GuiConnection
            │    └─ .Children(0)  →  GuiSession
            │         ├─ .findById("wnd[0]/usr/txtRSYST-BNAME")  →  campo usuário
            │         ├─ .findById("wnd[0]/usr/pwdRSYST-BCODE")  →  campo senha
            │         ├─ .findById("wnd[0]/usr/txtRSYST-MESSION") →  campo client
            │         ├─ .findById("wnd[0]/usr/txtRSYST-LANGU")  →  campo idioma
            │         └─ .findById("wnd[0]").sendVKey(0)          →  Enter (submeter)
            └─ .ConnectionCount, .Connections  →  enumeração
```

### Decisão arquitetural: workspace_name é apenas documentação

`application.OpenConnection(description)` funciona independentemente de qual workspace está "selecionado" na árvore do SAP Logon pad — o nome da conexão é globalmente único no catálogo. Portanto, **não faremos automação da árvore de workspaces via Win32 UI** (que seria frágil e dependente de versão). O `workspace_name` será armazenado no config apenas para documentação e logging.

Se futuramente for comprovado que o COM API não enxerga conexões sem selecionar o workspace primeiro, isso será tratado como spike separado com `pywinauto`.

---

## Arquitetura de Módulos

### Visão geral da integração

```
                    ┌────────────────────────────┐
                    │  sap_iw69_batch_config.json │
                    │  (logon_pad section)        │
                    └──────────┬─────────────────┘
                               │
                    ┌──────────▼─────────────────┐
                    │  service.py                 │
                    │  create_session_provider()  │
                    │  (factory — lê config)      │
                    └──────────┬─────────────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
     logon_pad.enabled?     false            true
              │                │                │
              │    ┌───────────▼──────┐  ┌──────▼───────────────────┐
              │    │SapSessionProvider│  │LogonPadSessionProvider   │
              │    │(existente, sem   │  │(NOVO — orquestra tudo)   │
              │    │ alterações)      │  │                          │
              │    └──────────────────┘  └──────┬───────────────────┘
              │                                 │
              │                    ┌────────────┼────────────┐
              │                    │            │            │
              │           ┌────────▼───┐ ┌──────▼─────┐ ┌───▼──────────┐
              │           │Credentials │ │SapConnectio│ │SapLogin      │
              │           │Loader      │ │nOpener     │ │Handler       │
              │           │(.env)      │ │(COM)       │ │(tela login)  │
              │           └────────────┘ └──────┬─────┘ └──────────────┘
              │                                 │
              │                        ┌────────▼────────┐
              │                        │SapApplication   │
              │                        │Provider (COM)   │
              │                        └─────────────────┘
              │
     ┌────────▼──────────────────────────────────────────┐
     │  SessionProvider (Protocol)                        │
     │  def get_session(config, logger) -> Any            │
     │  Ambas implementações satisfazem este contrato     │
     └───────────────────────────────────────────────────┘
```

---

## Novos Módulos — Especificação Detalhada

### 1. `sap_automation/errors.py`

**Responsabilidade única:** Definir a hierarquia de exceções do domínio SAP.

```
SapAutomationError (base)
├── SapLogonPadError
│   ├── SapLogonNotRunningError     → SAP Logon pad não está aberto
│   ├── ConnectionNotFoundError     → descrição da conexão não encontrada
│   └── LogonTimeoutError           → timeout ao abrir conexão
├── SapLoginError
│   ├── LoginFailedError            → credenciais erradas ou conta bloqueada
│   ├── MultipleLogonError          → popup de logon múltiplo não tratado
│   └── LoginTimeoutError           → tela de login não resolveu no tempo
└── SapCredentialsError
    └── MissingCredentialError      → variável do .env não definida
```

**Nuances de implementação:**
- Cada exceção deve incluir mensagem descritiva com contexto (ex: `ConnectionNotFoundError` deve incluir a descrição que foi tentada)
- `MissingCredentialError` deve nomear a variável ausente e orientar o usuário a criar `.env` a partir de `.env.example`
- Todas herdam de `SapAutomationError` para permitir catch genérico no orquestrador
- Nenhuma exceção deve expor credenciais em mensagens de erro ou logs

---

### 2. `sap_automation/credentials.py`

**Responsabilidade única:** Carregar e validar credenciais SAP do ambiente (`.env`).

#### Dataclass: `SapCredentials`

```
@dataclass(frozen=True)
class SapCredentials:
    username: str
    password: str
    client: str       # número do client SAP (ex: "300")
    language: str      # idioma do login (ex: "PT")
```

- Frozen (imutável) — consistente com todos os contracts do projeto
- `__post_init__` valida que `username` e `password` não estão vazios
- `client` e `language` podem ser strings vazias (campos opcionais na tela de login)
- **NUNCA** implementar `__repr__` ou `__str__` que exponha `password`

#### Classe: `CredentialsLoader`

```
class CredentialsLoader:
    def __init__(self, env_path: Path | None = None) -> None
    def load(self) -> SapCredentials
```

**Comportamento detalhado:**
1. Chama `dotenv.load_dotenv(self._env_path)` — se `env_path` é `None`, usa o default do dotenv (`.env` no diretório atual)
2. Lê as variáveis: `SAP_USERNAME`, `SAP_PASSWORD`, `SAP_CLIENT`, `SAP_LANGUAGE`
3. Se `SAP_USERNAME` ou `SAP_PASSWORD` estiverem ausentes ou vazias → `MissingCredentialError` com nome da variável
4. `SAP_CLIENT` e `SAP_LANGUAGE` defaultam para `""` se ausentes
5. Retorna `SapCredentials` frozen

**Variáveis de ambiente (mapeamento):**

| Variável `.env` | Campo `SapCredentials` | Obrigatória |
|-----------------|----------------------|-------------|
| `SAP_USERNAME` | `username` | Sim |
| `SAP_PASSWORD` | `password` | Sim |
| `SAP_CLIENT` | `client` | Não (default: `""`) |
| `SAP_LANGUAGE` | `language` | Não (default: `""`) |

---

### 3. `sap_automation/logon.py`

**Responsabilidade única:** Interação com o SAP Logon pad via COM — obter Application e abrir conexão por nome.

#### Dataclass: `LogonConfig`

```
@dataclass(frozen=True)
class LogonConfig:
    connection_description: str    # ex: "H181 RP1 ENEL SP CCS Produção (without SSO)"
    workspace_name: str            # ex: "00 SAP ERP" (apenas logging/documentação)
    synchronous: bool = True       # True = aguarda tela de login aparecer
    logon_timeout_seconds: float = 45.0
    multiple_logon_action: str = "continue"  # "continue" | "terminate_other" | "fail"
```

- `connection_description` é o nome exato conforme aparece no SAP Logon pad
- `workspace_name` armazenado para logging — **não usado na lógica COM**
- `multiple_logon_action` define comportamento quando aparece popup de sessão duplicada

#### Classe: `SapApplicationProvider`

```
class SapApplicationProvider:
    def get_application(self) -> Any
```

**Comportamento detalhado:**
1. `win32com.client.Dispatch("SapROTWr.SapROTWrapper")`
2. `rot_wrapper.GetROTEntry("SAPGUI")`
3. Se retornar `None` → `SapLogonNotRunningError("SAP Logon pad não está em execução. Inicie o saplogon.exe antes de executar a automação.")`
4. `sap_gui.GetScriptingEngine()` → retorna o objeto `Application`
5. Se scripting engine não disponível → `SapLogonPadError("SAP GUI Scripting não está habilitado. Verifique as configurações do servidor e cliente SAP.")`

#### Classe: `SapConnectionOpener`

```
class SapConnectionOpener:
    def __init__(self, app_provider: SapApplicationProvider) -> None
    def open_connection(self, config: LogonConfig) -> Any
```

**Comportamento detalhado:**
1. Obtém `application` via `self._app_provider.get_application()`
2. Loga: `f"Abrindo conexão: '{config.connection_description}' (workspace: {config.workspace_name})"`
3. Tenta `application.OpenConnection(config.connection_description, config.synchronous)`
4. Se `pywintypes.com_error` → captura e lança `ConnectionNotFoundError` incluindo:
   - A descrição tentada
   - Lista de conexões disponíveis (via `application.Connections` se acessível)
5. Se timeout (medir com timer próprio comparando `logon_timeout_seconds`) → `LogonTimeoutError`
6. Retorna o objeto `GuiConnection`

**Enumeração de conexões disponíveis (para diagnóstico):**
```
for i in range(application.ConnectionCount):
    conn = application.Children(i)
    # conn.Description, conn.SystemName, etc.
```
Isso é usado apenas na mensagem de erro para ajudar o usuário a identificar o nome correto.

---

### 4. `sap_automation/login.py`

**Responsabilidade única:** Preencher tela de login SAP e tratar cenários pós-login.

#### Classe: `SapLoginHandler`

```
class SapLoginHandler:
    def login(self, session: Any, credentials: SapCredentials, config: LogonConfig) -> None
```

**Comportamento detalhado — sequência completa:**

**Passo 1: Verificar se está na tela de login**
- Checar se `session.findById("wnd[0]/usr/txtRSYST-BNAME")` existe
- Se não existe, a sessão pode já estar logada → verificar `session.Info.SystemName`
- Se já logado → retornar (idempotência)
- Se nem login nem logado → `LoginFailedError("Estado inesperado da sessão SAP")`

**Passo 2: Preencher campos**
- `session.findById("wnd[0]/usr/txtRSYST-BNAME").Text = credentials.username`
- `session.findById("wnd[0]/usr/pwdRSYST-BCODE").Text = credentials.password`
- Se `credentials.client` não vazio E campo client visível:
  - `session.findById("wnd[0]/usr/txtRSYST-MESSION").Text = credentials.client`
- Se `credentials.language` não vazio E campo idioma visível:
  - `session.findById("wnd[0]/usr/txtRSYST-LANGU").Text = credentials.language`

**Passo 3: Submeter**
- `session.findById("wnd[0]").sendVKey(0)` (Enter)
- Aguardar `session.Busy == False` com timeout de `config.logon_timeout_seconds`

**Passo 4: Tratar popup de logon múltiplo**
- Verificar se `wnd[1]` apareceu (popup)
- Detectar conteúdo: se contém radio buttons de logon múltiplo (`MULTI_LOGON_OPT`)
- Baseado em `config.multiple_logon_action`:
  - `"continue"`: selecionar radio `MULTI_LOGON_OPT1` (continuar com este logon) → confirmar
  - `"terminate_other"`: selecionar radio `MULTI_LOGON_OPT2` (encerrar outro logon) → confirmar
  - `"fail"`: lançar `MultipleLogonError` com orientação ao usuário

**Passo 5: Verificar sucesso do login**
- Checar barra de status: `session.findById("wnd[0]/sbar").Text`
- Se contém mensagem de erro (ex: "senha incorreta", "conta bloqueada", "não autorizado") → `LoginFailedError` com a mensagem da barra de status
- Se `session.Info.SystemName` está populado e não vazio → login bem-sucedido
- Se nenhuma condição satisfeita dentro do timeout → `LoginTimeoutError`

**Passo 6: Tratar popup de informação pós-login (se existir)**
- Alguns sistemas mostram popup de aviso de expiração de senha
- Se `wnd[1]` aparece com botão de confirmação → pressionar Enter para fechar
- Não é erro — apenas informativo

---

### 5. Alteração: `sap_automation/execution.py`

**O que muda:**
- Adicionar `SessionProvider` Protocol no topo do arquivo
- Adicionar classe `LogonPadSessionProvider`
- Manter `SapSessionProvider` e `StepExecutor` **inalterados**

#### Protocol: `SessionProvider`

```
class SessionProvider(Protocol):
    def get_session(self, config: dict[str, Any], logger: logging.Logger | None = None) -> Any: ...
```

- Define o contrato que ambas implementações satisfazem
- Permite Liskov Substitution — qualquer código que use `SessionProvider` funciona com ambas

#### Classe: `LogonPadSessionProvider`

```
class LogonPadSessionProvider:
    def __init__(
        self,
        *,
        credentials_loader: CredentialsLoader,
        app_provider: SapApplicationProvider,
        connection_opener: SapConnectionOpener,
        login_handler: SapLoginHandler,
    ) -> None

    def get_session(self, config: dict[str, Any], logger: logging.Logger | None = None) -> Any
```

**Comportamento detalhado:**
1. Se `self._session` já existe (cache) → retornar imediatamente
2. Carregar credenciais: `self._credentials_loader.load()`
3. Construir `LogonConfig` a partir de `config["global"]["logon_pad"]`:
   - `connection_description` = `config["global"]["logon_pad"]["connection_description"]`
   - `workspace_name` = `config["global"]["logon_pad"].get("workspace_name", "")`
   - `logon_timeout_seconds` = `config["global"].get("login_timeout_seconds", 45)`
   - `multiple_logon_action` = `config["global"]["logon_pad"].get("multiple_logon_action", "continue")`
4. Abrir conexão: `connection = self._connection_opener.open_connection(logon_config)`
5. Obter sessão: `session = connection.Children(config["global"].get("session_index", 0))`
6. Fazer login: `self._login_handler.login(session, credentials, logon_config)`
7. Cachear: `self._session = session`
8. Retornar `session`

**Tratamento de erros:** Todas as exceções da hierarquia `SapAutomationError` propagam para o chamador (`BatchOrchestrator.run()`), que já tem try/except para produzir `ObjectManifest` com status `"failed"`. Como a sessão é obtida antes do loop de objetos, uma falha aqui resulta em **batch inteiro falhado** (comportamento correto — sem sessão, nenhuma extração é possível).

---

### 6. Alteração: `sap_automation/legacy_runner.py`

**O que muda (minimal diff):**
- Tipo do `session_provider` no construtor: de `SapSessionProvider` para `SessionProvider`
- Import de `SessionProvider` em vez de (ou além de) `SapSessionProvider`

Nenhuma outra alteração. A `LegacyExportService` já usa `session_provider.get_session()` — o Protocol garante compatibilidade.

---

### 7. Alteração: `sap_automation/service.py`

**O que muda:**
- Nova função factory `create_session_provider(config)`
- `create_batch_orchestrator()` passa a aceitar config opcional e usar a factory

#### Nova função: `create_session_provider`

```
def create_session_provider(config: dict[str, Any] | None = None) -> SessionProvider
```

**Lógica:**
1. Extrair `logon_pad_cfg = (config or {}).get("global", {}).get("logon_pad", {})`
2. Se `logon_pad_cfg.get("enabled", False)` é `True`:
   - Instanciar `CredentialsLoader(env_path=logon_pad_cfg.get("env_path"))`
   - Instanciar `SapApplicationProvider()`
   - Instanciar `SapConnectionOpener(app_provider)`
   - Instanciar `SapLoginHandler()`
   - Retornar `LogonPadSessionProvider(credentials_loader=..., app_provider=..., connection_opener=..., login_handler=...)`
3. Senão → retornar `SapSessionProvider()` (comportamento legado, zero alteração)

#### Alteração: `create_batch_orchestrator`

Adicionar parâmetro `config: dict[str, Any] | None = None` e usar `create_session_provider(config)` internamente.

---

### 8. Alteração: `sap_iw69_batch_config.json`

**Adicionar** seção `logon_pad` dentro de `global`:

```json
{
  "global": {
    "connection_index": 0,
    "session_index": 0,
    "auto_login": true,
    "logon_pad": {
      "enabled": true,
      "workspace_name": "00 SAP ERP",
      "connection_description": "H181 RP1 ENEL SP CCS Produção (without SSO)",
      "multiple_logon_action": "continue"
    },
    ...campos existentes permanecem...
  }
}
```

- `connection_index` e `session_index` permanecem para backward compatibility quando `logon_pad.enabled = false`
- Nenhum campo existente é removido ou renomeado

---

### 9. Alteração: `sap_automation/batch.py`

**O que muda:**
- Na função `main()`, carregar config antes de criar o orquestrador
- Passar config para `create_batch_orchestrator()`

Atualmente o config é carregado dentro de `BatchOrchestrator.run()`. A mudança é carregar antecipadamente para informar a factory de qual `SessionProvider` usar.

---

### 10. Alteração: `pyproject.toml`

**Adicionar dependência:**

```toml
[tool.poetry.dependencies]
python-dotenv = "^1.0"
```

---

### 11. Novo arquivo: `.env.example`

```env
# ─── SAP GUI Credentials ───────────────────────────────
# Copie este arquivo para .env e preencha os valores.
# O .env NUNCA deve ser commitado no repositório.

SAP_USERNAME=
SAP_PASSWORD=
SAP_CLIENT=
SAP_LANGUAGE=PT
```

---

## Análise SOLID — Classe por Classe

| Princípio | Classe | Justificativa |
|-----------|--------|---------------|
| **S** — Single Responsibility | `CredentialsLoader` | Faz apenas uma coisa: carregar credenciais do `.env` |
| **S** | `SapApplicationProvider` | Faz apenas uma coisa: obter o objeto Application COM |
| **S** | `SapConnectionOpener` | Faz apenas uma coisa: abrir uma conexão nomeada |
| **S** | `SapLoginHandler` | Faz apenas uma coisa: preencher login e tratar pós-login |
| **S** | `LogonPadSessionProvider` | Orquestra as 4 classes acima em sequência |
| **O** — Open/Closed | `LogonConfig` | Novas conexões/workspaces = nova config, zero mudança de código |
| **O** | Feature flag `enabled` | Fluxo existente intocado quando desabilitado |
| **L** — Liskov Substitution | `SessionProvider` Protocol | `LogonPadSessionProvider` e `SapSessionProvider` são intercambiáveis |
| **I** — Interface Segregation | `SessionProvider` | Interface mínima: 1 método `get_session()` |
| **I** | Classes separadas | `SapLoginHandler` não depende de `CredentialsLoader` — recebe `SapCredentials` prontas |
| **D** — Dependency Inversion | `LogonPadSessionProvider` | Recebe todas dependências via construtor (injeção) |
| **D** | `create_session_provider()` | Factory encapsula decisão de instanciação |

---

## Estratégia de Configuração — Camadas

```
Camada 1: .env (segredos)
  └─ SAP_USERNAME, SAP_PASSWORD, SAP_CLIENT, SAP_LANGUAGE
  └─ Carregado por python-dotenv em CredentialsLoader
  └─ Já no .gitignore

Camada 2: sap_iw69_batch_config.json (config operacional)
  └─ logon_pad.enabled, workspace_name, connection_description
  └─ SEM segredos aqui

Camada 3: CLI arguments (overrides, sprint futura)
  └─ Possível --no-logon-pad flag
```

---

## Estratégia de Testes

### Princípio central

Toda interação COM está atrás de classes injetáveis. Testes injetam fakes/mocks — **nenhum teste requer SAP GUI instalado**.

### Novos arquivos de teste

#### `tests/test_credentials.py`

| Teste | O que valida |
|-------|-------------|
| `test_load_credentials_from_env` | `CredentialsLoader` lê variáveis do `.env` corretamente via `monkeypatch` |
| `test_load_credentials_missing_username` | Lança `MissingCredentialError` nomeando `SAP_USERNAME` |
| `test_load_credentials_missing_password` | Lança `MissingCredentialError` nomeando `SAP_PASSWORD` |
| `test_load_credentials_optional_client_language` | `client` e `language` defaultam para `""` quando ausentes |
| `test_sap_credentials_frozen` | Tentativa de mutação lança `FrozenInstanceError` |
| `test_credentials_repr_hides_password` | `repr(credentials)` ou `str(credentials)` não contém a senha |

#### `tests/test_logon.py`

| Teste | O que valida |
|-------|-------------|
| `test_application_provider_returns_engine` | Mock de `win32com.client.Dispatch` → retorna scripting engine |
| `test_application_provider_sap_not_running` | `GetROTEntry` retorna `None` → `SapLogonNotRunningError` |
| `test_connection_opener_calls_open_connection` | Verifica que `OpenConnection` recebe descrição e flag sync corretos |
| `test_connection_opener_not_found` | `OpenConnection` lança COM error → `ConnectionNotFoundError` |
| `test_logon_config_frozen` | Imutabilidade do dataclass |

#### `tests/test_login.py`

| Teste | O que valida |
|-------|-------------|
| `test_login_fills_fields_and_submits` | Mock session com `findById` → verifica `.Text` setado e `sendVKey(0)` chamado |
| `test_login_handles_multiple_logon_continue` | Popup `wnd[1]` aparece → radio `OPT1` selecionado e confirmado |
| `test_login_handles_multiple_logon_fail` | `multiple_logon_action = "fail"` → `MultipleLogonError` |
| `test_login_detects_wrong_password` | Status bar com mensagem de erro → `LoginFailedError` |
| `test_login_skips_if_already_logged_in` | `session.Info.SystemName` populado → retorna sem erro |
| `test_login_fills_client_and_language_when_provided` | Campos opcionais preenchidos quando não vazios |
| `test_login_skips_optional_fields_when_empty` | `client=""` → campo não tocado |

#### `tests/test_logon_session_provider.py`

| Teste | O que valida |
|-------|-------------|
| `test_full_flow_orchestration` | Injeta 4 fakes → credenciais carregadas, conexão aberta, login feito, sessão retornada |
| `test_session_cached_after_first_call` | Duas chamadas a `get_session()` → `open_connection` chamado apenas uma vez |
| `test_propagates_credentials_error` | `CredentialsLoader.load()` lança → exceção propaga |
| `test_propagates_connection_error` | `ConnectionOpener.open_connection()` lança → exceção propaga |
| `test_propagates_login_error` | `LoginHandler.login()` lança → exceção propaga |

#### Alteração: `tests/test_batch.py` (existente)

- **Nenhuma alteração necessária** — os testes existentes usam `_FakeSessionProvider` e `_FakeExportService`, que continuam funcionando
- A factory em `service.py` pode ter um teste adicional verificando que `logon_pad.enabled = false` retorna `SapSessionProvider`

---

## Sequência de Implementação (ordenada por dependências)

```
Tarefa 1  ─── pyproject.toml ──────────── poetry add python-dotenv
    │
Tarefa 2  ─── .env.example ───────────── criar na raiz do projeto
    │
Tarefa 3  ─── sap_automation/errors.py ── hierarquia de exceções
    │
Tarefa 4  ─── sap_automation/credentials.py ── SapCredentials + CredentialsLoader
    │         depende de: errors.py
    │
Tarefa 5  ─── sap_automation/logon.py ─── LogonConfig + SapApplicationProvider + SapConnectionOpener
    │         depende de: errors.py
    │
Tarefa 6  ─── sap_automation/login.py ─── SapLoginHandler
    │         depende de: errors.py, credentials.py (SapCredentials), logon.py (LogonConfig)
    │
Tarefa 7  ─── sap_automation/execution.py ── SessionProvider Protocol + LogonPadSessionProvider
    │         depende de: credentials.py, logon.py, login.py
    │
Tarefa 8  ─── sap_automation/legacy_runner.py ── widening de type hint para SessionProvider
    │         depende de: Tarefa 7
    │
Tarefa 9  ─── sap_automation/service.py ── create_session_provider() factory
    │         depende de: Tarefa 7
    │
Tarefa 10 ─── sap_automation/batch.py ─── main() carrega config e passa para factory
    │         depende de: Tarefa 9
    │
Tarefa 11 ─── sap_iw69_batch_config.json ── seção logon_pad
    │
Tarefa 12 ─── tests/ ─────────────────── todos os novos testes
              depende de: Tarefas 3-10
```

---

## Riscos e Mitigações

| Risco | Impacto | Mitigação |
|-------|---------|-----------|
| String de `connection_description` deve ser match exato | Conexão não encontrada | Logar conexões disponíveis na mensagem de erro; documentar formato exato no `.env.example` |
| Versão do SAP Logon pad diferente | ProgID COM pode variar | Tentar múltiplos ProgIDs (`SapROTWr.SapROTWrapper`, `SAPGUI`); documentar versões suportadas |
| IDs de campos de login variam por versão SAP | Login falha silenciosamente | IDs standard (`txtRSYST-BNAME`, `pwdRSYST-BCODE`) são estáveis entre releases SAP — risco baixo |
| Timing do popup de logon múltiplo | Race condition | Poll por `wnd[1]` com timeout curto (2-3s) após pressionar Enter |
| `python-dotenv` não carrega no Windows | Credenciais ausentes | Mensagem de erro explícita apontando para `.env.example` |
| Quebrar fluxo existente | Regressão | Feature flag `logon_pad.enabled` default `false` — comportamento existente intocado a menos que explicitamente habilitado |

---

## Critérios de Aceite

- [ ] `.env.example` criado com as 4 variáveis documentadas
- [ ] `python-dotenv` adicionado como dependência
- [ ] `CredentialsLoader` carrega credenciais do `.env` e valida obrigatórias
- [ ] `SapApplicationProvider` obtém Application COM ou lança erro claro
- [ ] `SapConnectionOpener` abre "H181 RP1 ENEL SP CCS Produção (without SSO)" por nome
- [ ] `SapLoginHandler` preenche tela de login e trata popup de logon múltiplo
- [ ] `LogonPadSessionProvider` orquestra o ciclo completo e cacheia a sessão
- [ ] `SessionProvider` Protocol permite substituição transparente
- [ ] Feature flag `logon_pad.enabled` controla qual provider é instanciado
- [ ] Config JSON atualizado com seção `logon_pad`
- [ ] Todos os testes passam sem SAP GUI instalado (100% mockado)
- [ ] Nenhum teste existente quebrado
- [ ] Nenhuma credencial exposta em logs, repr, ou mensagens de erro
- [ ] Hierarquia de exceções com mensagens actionable em português
