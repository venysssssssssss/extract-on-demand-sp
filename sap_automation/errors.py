from __future__ import annotations


class SapAutomationError(RuntimeError):
    """Base para erros de automação SAP."""


class SapLogonPadError(SapAutomationError):
    """Falhas no ciclo de vida do SAP Logon pad."""


class SapLogonNotRunningError(SapLogonPadError):
    def __init__(self) -> None:
        super().__init__(
            "SAP Logon pad não está em execução. Inicie o saplogon.exe antes de executar a automação."
        )


class ConnectionNotFoundError(SapLogonPadError):
    def __init__(self, *, description: str, available_connections: list[str] | None = None) -> None:
        available = available_connections or []
        detail = (
            f" Conexões disponíveis: {', '.join(available)}."
            if available
            else " Não foi possível enumerar conexões disponíveis no SAP Logon pad."
        )
        super().__init__(
            "Conexão SAP não encontrada no Logon pad. "
            f"Descrição tentada: '{description}'.{detail}"
        )


class LogonTimeoutError(SapLogonPadError):
    def __init__(self, *, description: str, timeout_seconds: float) -> None:
        super().__init__(
            "Tempo esgotado ao abrir a conexão SAP no Logon pad. "
            f"Descrição: '{description}'. Timeout: {timeout_seconds:.1f}s."
        )


class SapLoginError(SapAutomationError):
    """Falhas durante a autenticação SAP."""


class LoginFailedError(SapLoginError):
    def __init__(self, message: str) -> None:
        super().__init__(f"Falha no login SAP. {message}")


class MultipleLogonError(SapLoginError):
    def __init__(self, message: str) -> None:
        super().__init__(f"Popup de logon múltiplo não tratado. {message}")


class LoginTimeoutError(SapLoginError):
    def __init__(self, *, timeout_seconds: float) -> None:
        super().__init__(
            "Tempo esgotado aguardando a conclusão do login SAP. "
            f"Timeout: {timeout_seconds:.1f}s."
        )


class SapCredentialsError(SapAutomationError):
    """Falhas ao carregar credenciais SAP."""


class MissingCredentialError(SapCredentialsError):
    def __init__(self, variable_name: str) -> None:
        super().__init__(
            "Credencial SAP obrigatória ausente no ambiente. "
            f"Variável: '{variable_name}'. "
            "Crie um arquivo .env a partir de .env.example e preencha os valores necessários."
        )
