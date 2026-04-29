from __future__ import annotations

import argparse
import csv
import logging
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import openpyxl

DEFAULT_CHUNK_SIZE = 5000
DEFAULT_CREATED_BY = "BRMARI"
DEFAULT_INPUT_PATH = Path("output/runs/20260330T171500/iw51/working/projeto_Dani2.xlsm")
FALLBACK_INPUT_PATH = Path("projeto_Dani2.xlsm")


@dataclass(frozen=True)
class DaniValidationRow:
    cliente: str
    instalacao: str
    descricao: str


def chunk_list(values: list[str], chunk_size: int) -> Iterable[list[str]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero.")
    for index in range(0, len(values), chunk_size):
        yield values[index : index + chunk_size]


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _normalize_header(value: str) -> str:
    token = _strip_accents(value).strip().upper()
    return "_".join("".join(ch if ch.isalnum() else " " for ch in token).split())


def _normalize_key_value(value: str) -> str:
    token = str(value or "").strip()
    if token.endswith(".0") and token[:-2].isdigit():
        token = token[:-2]
    if token.isdigit():
        token = token.lstrip("0") or "0"
    return token.upper()


def _normalize_description(value: str) -> str:
    return " ".join(_strip_accents(str(value or "")).upper().split())


def _cell_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def _row_key(row: DaniValidationRow) -> tuple[str, str, str]:
    return (
        _normalize_key_value(row.cliente),
        _normalize_key_value(row.instalacao),
        _normalize_description(row.descricao),
    )


def _deduplicate_preserving_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = _normalize_key_value(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(str(value).strip())
    return result


def read_dani_excel(file_path: Path) -> list[DaniValidationRow]:
    wb = openpyxl.load_workbook(file_path, data_only=True)
    ws = wb.active
    data: list[DaniValidationRow] = []

    col_cliente = 0
    col_instalacao = 1
    col_descricao = 2

    for row in ws.iter_rows(min_row=2):
        if len(row) <= max(col_cliente, col_instalacao, col_descricao):
            continue
        cliente = _cell_to_text(row[col_cliente].value)
        if not cliente:
            continue
        instalacao = _cell_to_text(row[col_instalacao].value)
        descricao = _cell_to_text(row[col_descricao].value)
        data.append(DaniValidationRow(cliente=cliente, instalacao=instalacao, descricao=descricao))
    return data


def execute_iw59_chunk(
    session: Any,
    clientes: list[str],
    export_path: Path,
    *,
    created_by: str = DEFAULT_CREATED_BY,
) -> None:
    session.findById("wnd[0]").maximize()
    session.findById("wnd[0]/tbar[0]/okcd").text = "/nIW59"
    session.findById("wnd[0]").sendVKey(0)

    session.findById("wnd[0]/usr/ctxtDATUV").text = ""
    session.findById("wnd[0]/usr/ctxtDATUB").text = ""
    session.findById("wnd[0]/usr/ctxtDATUV").setFocus()
    session.findById("wnd[0]/usr/ctxtDATUV").caretPosition = 0

    # Multi selection CLIENTE
    session.findById("wnd[0]/usr/btn%_KUNUM_%_APP_%-VALU_PUSH").press()

    import win32clipboard
    win32clipboard.OpenClipboard()
    win32clipboard.EmptyClipboard()
    win32clipboard.SetClipboardText("\r\n".join(clientes))
    win32clipboard.CloseClipboard()

    # Paste from clipboard
    session.findById("wnd[1]/tbar[0]/btn[24]").press()
    session.findById("wnd[1]/tbar[0]/btn[8]").press()

    # Criado por
    session.findById("wnd[0]/usr/ctxtERNAM-LOW").text = created_by
    session.findById("wnd[0]/usr/ctxtVARIANT").text = "/misV"

    # Executar
    session.findById("wnd[0]/tbar[1]/btn[8]").press()

    # Exportar arquivo local
    session.findById("wnd[0]/mbar/menu[0]/menu[11]/menu[2]").select()
    session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]").select()
    session.findById("wnd[1]/tbar[0]/btn[0]").press()

    dir_path = str(export_path.parent.absolute())
    file_name = export_path.name

    session.findById("wnd[1]/usr/ctxtDY_PATH").text = dir_path
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = file_name
    session.findById("wnd[1]/tbar[0]/btn[11]").press()


def _read_text_export(path: Path) -> str:
    errors: list[str] = []
    for encoding in ("cp1252", "utf-8-sig", "utf-8", "utf-16", "latin-1"):
        try:
            return path.read_text(encoding=encoding)
        except Exception as exc:
            errors.append(f"{encoding}: {exc}")
    raise RuntimeError(f"Could not decode SAP TXT export {path}: {' | '.join(errors[:3])}")


def _resolve_column(header: list[str], aliases: set[str]) -> int:
    normalized = [_normalize_header(item) for item in header]
    for index, name in enumerate(normalized):
        if name in aliases:
            return index
    return -1


def parse_sap_text_export(path: Path) -> tuple[list[str], list[DaniValidationRow], list[str]]:
    content = _read_text_export(path)
    lines = [line for line in content.splitlines() if line.strip()]
    if not lines:
        return [], [], []

    delimiter = max(("\t", ";", ","), key=lambda token: sum(line.count(token) for line in lines[:20]))
    parsed_rows = list(csv.reader(lines, delimiter=delimiter))
    header = [c.strip() for c in parsed_rows[0]]
    data: list[DaniValidationRow] = []
    raw_lines: list[str] = []

    col_cliente = _resolve_column(header, {"CLIENTE", "CLIENTES", "CLIENT", "KUNUM"})
    col_instalacao = _resolve_column(header, {"INSTALACAO", "INSTALACOES", "ANLAGE"})
    col_descricao = _resolve_column(header, {"DESCRICAO", "DESCRICOES", "DESCRIPTION", "KTEXT", "TEXTO"})
    missing = [
        name
        for name, index in (
            ("CLIENTE", col_cliente),
            ("INSTALACAO", col_instalacao),
            ("DESCRICAO", col_descricao),
        )
        if index < 0
    ]
    if missing:
        raise RuntimeError(f"SAP TXT export missing required columns {missing}: path={path} header={header}")

    for row, raw_line in zip(parsed_rows[1:], lines[1:], strict=False):
        cliente = row[col_cliente].strip() if len(row) > col_cliente else ""
        instalacao = row[col_instalacao].strip() if len(row) > col_instalacao else ""
        descricao = row[col_descricao].strip() if len(row) > col_descricao else ""
        if not cliente and not instalacao and not descricao:
            continue
        data.append(DaniValidationRow(cliente=cliente, instalacao=instalacao, descricao=descricao))
        raw_lines.append(raw_line)

    return header, data, raw_lines


def compare_dani_rows(
    original_data: list[DaniValidationRow],
    extracted_data: list[DaniValidationRow],
) -> list[dict[str, str]]:
    extracted_keys = {_row_key(row) for row in extracted_data}
    results: list[dict[str, str]] = []
    for original in original_data:
        found = _row_key(original) in extracted_keys
        results.append(
            {
                "CLIENTE_ORIGINAL": original.cliente,
                "INSTALACAO_ORIGINAL": original.instalacao,
                "DESCRICAO_ORIGINAL": original.descricao,
                "ENCONTRADO_NO_SAP": "SIM" if found else "NAO",
            }
        )
    return results


def resolve_default_input_path() -> Path:
    return DEFAULT_INPUT_PATH if DEFAULT_INPUT_PATH.exists() else FALLBACK_INPUT_PATH


def run_valida_dani(
    input_path: Path,
    out_dir: Path,
    config_path: Path | None = None,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    created_by: str = DEFAULT_CREATED_BY,
) -> dict[str, Any]:
    logger = logging.getLogger("valida_dani")

    if not input_path.exists():
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {input_path}")

    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Lendo dados originais...")
    original_data = read_dani_excel(input_path)
    logger.info(f"Foram lidas {len(original_data)} linhas válidas da planilha.")

    clientes_unicos = _deduplicate_preserving_order(row.cliente for row in original_data)
    logger.info(f"Clientes únicos para consultar: {len(clientes_unicos)}")

    logger.info("Conectando ao SAP...")
    try:
        from sap_automation.config import load_export_config
        from sap_automation.service import create_session_provider

        cfg_path = config_path if config_path else Path("sap_iw69_batch_config.json")
        config = load_export_config(cfg_path)
        provider = create_session_provider(config)
        session = provider.get_session(config=config, logger=logger)
    except Exception as e:
        raise RuntimeError(f"Erro ao conectar ao SAP. Detalhes: {e}")

    chunks = list(chunk_list(clientes_unicos, chunk_size))
    extracted_files = []

    for i, chunk in enumerate(chunks, 1):
        export_file = out_dir / f"iw59_export_chunk_{i}.txt"
        logger.info(f"Processando chunk {i}/{len(chunks)} com {len(chunk)} clientes...")
        execute_iw59_chunk(session, chunk, export_file, created_by=created_by)
        extracted_files.append(export_file)

    logger.info("Juntando arquivos extraídos e extraindo dados...")
    all_extracted_data: list[DaniValidationRow] = []
    unified_file_path = out_dir / "extracao_completa_SAP.txt"
    unified_csv_path = out_dir / "extracao_completa_SAP.csv"

    with open(unified_file_path, "w", encoding="utf-8") as out_unified:
        for i, f_path in enumerate(extracted_files):
            header, data, raw_lines = parse_sap_text_export(f_path)
            if i == 0 and header:
                out_unified.write("\t".join(header) + "\n")
            all_extracted_data.extend(data)
            for raw_line in raw_lines:
                out_unified.write(raw_line + "\n")

    with open(unified_csv_path, "w", newline="", encoding="utf-8-sig") as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(["CLIENTE", "INSTALACAO", "DESCRICAO"])
        for item in all_extracted_data:
            writer.writerow([item.cliente, item.instalacao, item.descricao])

    logger.info("Comparando com planilha original...")
    comparison_file = out_dir / "resultado_validacao.csv"
    comparison_rows = compare_dani_rows(original_data, all_extracted_data)
    missing_count = sum(1 for row in comparison_rows if row["ENCONTRADO_NO_SAP"] == "NAO")

    with open(comparison_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "CLIENTE_ORIGINAL",
                "INSTALACAO_ORIGINAL",
                "DESCRICAO_ORIGINAL",
                "ENCONTRADO_NO_SAP",
            ],
        )
        writer.writeheader()
        writer.writerows(comparison_rows)

    return {
        "status": "success" if missing_count == 0 else "missing_rows",
        "unified_file_path": str(unified_file_path),
        "unified_csv_path": str(unified_csv_path),
        "comparison_file": str(comparison_file),
        "total_originais": len(original_data),
        "total_extraidos": len(all_extracted_data),
        "total_faltantes": missing_count,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validação DANI - IW59")
    parser.add_argument("--input", default=str(resolve_default_input_path()), help="Caminho para a planilha original DANI")
    parser.add_argument("--output-dir", default="output/valida_dani", help="Diretório de saída")
    parser.add_argument("--config", default="sap_iw69_batch_config.json", help="Caminho para configuração do SAP")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Tamanho dos lotes de clientes")
    parser.add_argument("--created-by", default=DEFAULT_CREATED_BY, help="Valor do campo Criado por na IW59")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("valida_dani")

    try:
        result = run_valida_dani(
            Path(args.input),
            Path(args.output_dir),
            Path(args.config),
            chunk_size=args.chunk_size,
            created_by=args.created_by,
        )
        logger.info(f"Processo concluído com sucesso.")
        logger.info(f"Arquivo unificado TXT: {result['unified_file_path']}")
        logger.info(f"Arquivo unificado CSV: {result['unified_csv_path']}")
        logger.info(f"Resultado da validação: {result['comparison_file']}")
        logger.info(f"Faltantes na validação: {result['total_faltantes']}")
        return 0
    except Exception as e:
        logger.error(f"Erro na validação DANI: {e}")
        return 1
