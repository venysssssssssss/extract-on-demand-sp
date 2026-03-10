from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SapHeaderProfile:
    object_code: str
    known_aliases: dict[str, str] = field(default_factory=dict)
    required_tokens: tuple[str, ...] = ()
    optional_tokens: tuple[str, ...] = ()
    preamble_negative_tokens: tuple[str, ...] = ()
    documented_fields: tuple[str, ...] = ()
    duplicate_column_semantics: dict[tuple[str, int], str] = field(default_factory=dict)
    allow_multiline_header: bool = True


_COMMON_KNOWN_ALIASES: dict[str, str] = {
    "nota": "nota",
    "numerodanota": "nota",
    "numeronota": "nota",
    "descricao": "descricao",
    "descricao_da_nota": "descricao",
    "statususuario": "status_usuario",
    "statususuar": "status_usuario",
    "statususuar": "status_usuario",
    "statusdousuario": "status_usuario",
    "data": "data",
    "hora": "hora",
    "encerram": "encerramento",
    "encerramento": "encerramento",
    "concldesj": "conclusao_desejada",
    "concldesejada": "conclusao_desejada",
    "textocodeparteobj": "texto_code_parte_obj",
    "textocdparteobj": "texto_code_parte_obj",
    "ptob": "ptob",
    "parteobjeto": "ptob",
    "textocodeparaproblema": "texto_code_problema",
    "textocodeproblema": "texto_code_problema",
    "dano": "dano",
    "criadopor": "criado_por",
    "modificadoem": "modificado_em",
    "modificadoas": "modificado_as",
    "modificadopor": "modificado_por",
    "cliente": "cliente",
    "rua": "rua",
    "instalacao": "instalacao",
    "centrtrabrespon": "centro_trabalho_responsavel",
    "centrtrabrespons": "centro_trabalho_responsavel",
    "centrabrespon": "centro_trabalho_responsavel",
}

_COMMON_OPTIONAL_TOKENS: tuple[str, ...] = (
    "nota",
    "descricao",
    "statususuario",
    "data",
    "hora",
    "encerramento",
    "conclusaodesejada",
    "cliente",
    "rua",
)

_COMMON_NEGATIVE_TOKENS: tuple[str, ...] = (
    "pagina",
    "relatorio",
    "usuario",
    "emitidoem",
)


def get_header_profile(object_code: str) -> SapHeaderProfile:
    token = str(object_code or "").strip().upper() or "DEFAULT"
    documented_fields = (
        "Nota",
        "Descricao",
        "Status usuario",
        "Data",
        "Hora",
    )
    return SapHeaderProfile(
        object_code=token,
        known_aliases=dict(_COMMON_KNOWN_ALIASES),
        required_tokens=("nota",),
        optional_tokens=_COMMON_OPTIONAL_TOKENS,
        preamble_negative_tokens=_COMMON_NEGATIVE_TOKENS,
        documented_fields=documented_fields,
        duplicate_column_semantics={},
        allow_multiline_header=True,
    )
