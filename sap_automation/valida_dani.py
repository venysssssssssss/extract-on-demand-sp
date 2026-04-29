from __future__ import annotations

import argparse
import csv
import logging
import os
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import openpyxl

DEFAULT_CHUNK_SIZE = 5000
DEFAULT_CREATED_BY = "BR0041761455"
DEFAULT_INPUT_PATH = Path("output/runs/20260330T171500/iw51/working/projeto_Dani2.xlsm")
FALLBACK_INPUT_PATH = Path("projeto_Dani2.xlsm")
LOG_FILE_NAME = "valida_dani.log"
CHUNK_LOG_FILE_NAME = "valida_dani_chunks.csv"
KUNUM_MULTISELECT_BUTTON_ID = "wnd[0]/usr/btn%_KUNUM_%_APP_%-VALU_PUSH"
KUNUM_CLEAR_SELECTION_BUTTON_ID = "wnd[1]/tbar[0]/btn[16]"
KUNUM_SINGLE_VALUE_ID = (
    "wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/"
    "tblSAPLALDBSINGLE/ctxtRSCSEL_255-SLOW_I[1,0]"
)


@dataclass(frozen=True)
class DaniValidationRow:
    cliente: str
    instalacao: str
    descricao: str
    nota: str = ""


@dataclass(frozen=True)
class ChunkExecutionLog:
    chunk_index: int
    total_chunks: int
    client_count: int
    first_cliente: str
    last_cliente: str
    export_path: str
    status: str
    duration_seconds: float
    error: str = ""


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


def _clean_cliente_value(value: Any) -> str:
    return _cell_to_text(value).replace(",", "").strip()


def _row_key(row: DaniValidationRow) -> tuple[str, str, str]:
    return (
        _normalize_key_value(row.cliente),
        _normalize_key_value(row.instalacao),
        _normalize_description(row.descricao),
    )


def _join_notas(rows: Iterable[DaniValidationRow]) -> str:
    notas: list[str] = []
    seen: set[str] = set()
    for row in rows:
        nota = _cell_to_text(row.nota)
        if not nota or nota in seen:
            continue
        seen.add(nota)
        notas.append(nota)
    return ";".join(notas)


def _deduplicate_preserving_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned_value = _clean_cliente_value(value)
        normalized = _normalize_key_value(cleaned_value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(cleaned_value)
    return result


def resolve_input_path(path: str | Path) -> Path:
    raw = str(path or "").strip()
    if not raw:
        return resolve_default_input_path()
    expanded = Path(os.path.expanduser(raw))
    if expanded.exists():
        return expanded
    if "\\" in raw:
        normalized = raw.replace("\\", "/")
        candidate = Path(os.path.expanduser(normalized))
        if candidate.exists():
            return candidate
        return candidate
    return expanded


def configure_run_logger(logger: logging.Logger, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / LOG_FILE_NAME
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] logger=%(name)s %(message)s"
    )
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler) and Path(handler.baseFilename) == log_path:
            return log_path
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return log_path


def write_chunk_log(out_dir: Path, rows: list[ChunkExecutionLog]) -> Path:
    path = out_dir / CHUNK_LOG_FILE_NAME
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "chunk_index",
                "total_chunks",
                "client_count",
                "first_cliente",
                "last_cliente",
                "export_path",
                "status",
                "duration_seconds",
                "error",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "chunk_index": row.chunk_index,
                    "total_chunks": row.total_chunks,
                    "client_count": row.client_count,
                    "first_cliente": row.first_cliente,
                    "last_cliente": row.last_cliente,
                    "export_path": row.export_path,
                    "status": row.status,
                    "duration_seconds": f"{row.duration_seconds:.3f}",
                    "error": row.error,
                }
            )
    return path


def _find_sap_control(session: Any, control_id: str, *, optional: bool, logger: logging.Logger | None) -> Any | None:
    try:
        return session.findById(control_id)
    except Exception as exc:
        if optional:
            if logger is not None:
                logger.info("SAP optional_control_missing id=%s error=%s", control_id, exc)
            return None
        raise


def _press_sap_control(
    session: Any,
    control_id: str,
    *,
    optional: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    control = _find_sap_control(session, control_id, optional=optional, logger=logger)
    if control is None:
        return
    control.press()
    if logger is not None:
        logger.info("SAP press id=%s optional=%s", control_id, optional)


def _set_sap_text(
    session: Any,
    control_id: str,
    value: str,
    *,
    optional: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    control = _find_sap_control(session, control_id, optional=optional, logger=logger)
    if control is None:
        return
    control.text = value
    if logger is not None:
        logger.info("SAP set_text id=%s chars=%s optional=%s", control_id, len(value), optional)


def _copy_clientes_to_clipboard(clientes: list[str], *, logger: logging.Logger | None = None) -> None:
    import win32clipboard

    clipboard_open = False
    try:
        win32clipboard.OpenClipboard()
        clipboard_open = True
        win32clipboard.EmptyClipboard()
        win32clipboard.SetClipboardText("\r\n".join(clientes))
        if logger is not None:
            logger.info("Clipboard loaded clientes=%s chars=%s", len(clientes), len("\r\n".join(clientes)))
    finally:
        if clipboard_open:
            win32clipboard.CloseClipboard()


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
        cliente = _clean_cliente_value(row[col_cliente].value)
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
    logger: logging.Logger | None = None,
) -> None:
    clientes = [_clean_cliente_value(cliente) for cliente in clientes]
    clientes = [cliente for cliente in clientes if cliente]
    if not clientes:
        raise ValueError("clientes chunk cannot be empty.")
    if len(clientes) > DEFAULT_CHUNK_SIZE:
        raise ValueError(f"IW59 cliente chunk exceeds {DEFAULT_CHUNK_SIZE}: {len(clientes)}")

    if logger is not None:
        logger.info(
            "IW59 chunk_start clientes=%s first_cliente=%s last_cliente=%s export_path=%s created_by=%s",
            len(clientes),
            clientes[0],
            clientes[-1],
            export_path,
            created_by,
        )

    session.findById("wnd[0]").maximize()
    session.findById("wnd[0]/tbar[0]/okcd").text = "/nIW59"
    session.findById("wnd[0]").sendVKey(0)

    session.findById("wnd[0]/usr/ctxtDATUV").text = ""
    session.findById("wnd[0]/usr/ctxtDATUB").text = ""
    session.findById("wnd[0]/usr/ctxtDATUV").setFocus()
    session.findById("wnd[0]/usr/ctxtDATUV").caretPosition = 0

    _press_sap_control(session, KUNUM_MULTISELECT_BUTTON_ID, logger=logger)
    _press_sap_control(session, KUNUM_CLEAR_SELECTION_BUTTON_ID, optional=True, logger=logger)
    _set_sap_text(session, KUNUM_SINGLE_VALUE_ID, "", optional=True, logger=logger)
    _copy_clientes_to_clipboard(clientes, logger=logger)
    _press_sap_control(session, "wnd[1]/tbar[0]/btn[24]", logger=logger)
    _press_sap_control(session, "wnd[1]/tbar[0]/btn[0]", optional=True, logger=logger)
    _press_sap_control(session, "wnd[1]/tbar[0]/btn[8]", logger=logger)

    # Criado por
    session.findById("wnd[0]/usr/ctxtERNAM-LOW").text = created_by
    session.findById("wnd[0]/usr/ctxtVARIANT").text = "/misV"
    session.findById("wnd[0]/usr/ctxtERNAM-LOW").setFocus()
    session.findById("wnd[0]/usr/ctxtERNAM-LOW").caretPosition = len(created_by)

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
    if logger is not None:
        logger.info("IW59 chunk_export_requested export_path=%s", export_path)


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
    col_nota = _resolve_column(
        header,
        {"NOTA", "NOTAS", "QMNUM", "NOTIFICACAO", "NUMERO_NOTA", "NUMERO_DA_NOTA"},
    )
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
        nota = row[col_nota].strip() if col_nota >= 0 and len(row) > col_nota else ""
        if not cliente and not instalacao and not descricao:
            continue
        data.append(DaniValidationRow(cliente=cliente, instalacao=instalacao, descricao=descricao, nota=nota))
        raw_lines.append(raw_line)

    return header, data, raw_lines


def compare_dani_rows(
    original_data: list[DaniValidationRow],
    extracted_data: list[DaniValidationRow],
) -> list[dict[str, str]]:
    extracted_by_key: dict[tuple[str, str, str], list[DaniValidationRow]] = {}
    for row in extracted_data:
        extracted_by_key.setdefault(_row_key(row), []).append(row)
    results: list[dict[str, str]] = []
    for original in original_data:
        matching_rows = extracted_by_key.get(_row_key(original), [])
        found = bool(matching_rows)
        results.append(
            {
                "CLIENTE_ORIGINAL": original.cliente,
                "INSTALACAO_ORIGINAL": original.instalacao,
                "DESCRICAO_ORIGINAL": original.descricao,
                "NOTAS_SAP": _join_notas(matching_rows),
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
    input_path = resolve_input_path(input_path)

    if not input_path.exists():
        raise FileNotFoundError(
            "Arquivo de entrada não encontrado na máquina onde a aplicação está rodando: "
            f"{input_path}. Envie input_path no curl apontando para um arquivo local nessa máquina."
        )
    if chunk_size > DEFAULT_CHUNK_SIZE:
        raise ValueError(f"chunk_size cannot exceed {DEFAULT_CHUNK_SIZE}. Received: {chunk_size}")

    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = configure_run_logger(logger, out_dir)
    started_at = time.perf_counter()
    logger.info(
        "VALIDA_DANI start input_path=%s out_dir=%s config_path=%s chunk_size=%s created_by=%s log_path=%s",
        input_path,
        out_dir,
        config_path or Path("sap_iw69_batch_config.json"),
        chunk_size,
        created_by,
        log_path,
    )

    logger.info("VALIDA_DANI phase=read_original start")
    original_data = read_dani_excel(input_path)
    logger.info("VALIDA_DANI phase=read_original rows=%s source_column=A", len(original_data))

    clientes_unicos = _deduplicate_preserving_order(row.cliente for row in original_data)
    logger.info(
        "VALIDA_DANI phase=build_clientes total_rows=%s unique_clientes=%s chunk_size=%s",
        len(original_data),
        len(clientes_unicos),
        chunk_size,
    )
    if not clientes_unicos:
        raise ValueError(f"Nenhum valor válido encontrado na coluna A da planilha: {input_path}")

    logger.info("VALIDA_DANI phase=sap_connect start")
    try:
        from sap_automation.config import load_export_config
        from sap_automation.service import create_session_provider

        cfg_path = config_path if config_path else Path("sap_iw69_batch_config.json")
        config = load_export_config(cfg_path)
        provider = create_session_provider(config)
        session = provider.get_session(config=config, logger=logger)
    except Exception as e:
        logger.exception("VALIDA_DANI phase=sap_connect status=failed")
        raise RuntimeError(f"Erro ao conectar ao SAP. Detalhes: {e}")
    logger.info("VALIDA_DANI phase=sap_connect status=success")

    chunks = list(chunk_list(clientes_unicos, chunk_size))
    extracted_files: list[Path] = []
    chunk_logs: list[ChunkExecutionLog] = []

    for i, chunk in enumerate(chunks, 1):
        export_file = out_dir / f"iw59_export_chunk_{i}.txt"
        chunk_started_at = time.perf_counter()
        logger.info(
            "VALIDA_DANI phase=iw59_chunk status=start chunk=%s/%s clientes=%s first_cliente=%s last_cliente=%s export_path=%s",
            i,
            len(chunks),
            len(chunk),
            chunk[0] if chunk else "",
            chunk[-1] if chunk else "",
            export_file,
        )
        try:
            execute_iw59_chunk(session, chunk, export_file, created_by=created_by, logger=logger)
        except Exception as exc:
            duration = time.perf_counter() - chunk_started_at
            chunk_logs.append(
                ChunkExecutionLog(
                    chunk_index=i,
                    total_chunks=len(chunks),
                    client_count=len(chunk),
                    first_cliente=chunk[0] if chunk else "",
                    last_cliente=chunk[-1] if chunk else "",
                    export_path=str(export_file),
                    status="failed",
                    duration_seconds=duration,
                    error=str(exc),
                )
            )
            write_chunk_log(out_dir, chunk_logs)
            logger.exception(
                "VALIDA_DANI phase=iw59_chunk status=failed chunk=%s/%s duration_s=%.3f",
                i,
                len(chunks),
                duration,
            )
            raise
        duration = time.perf_counter() - chunk_started_at
        chunk_logs.append(
            ChunkExecutionLog(
                chunk_index=i,
                total_chunks=len(chunks),
                client_count=len(chunk),
                first_cliente=chunk[0] if chunk else "",
                last_cliente=chunk[-1] if chunk else "",
                export_path=str(export_file),
                status="success",
                duration_seconds=duration,
            )
        )
        write_chunk_log(out_dir, chunk_logs)
        logger.info(
            "VALIDA_DANI phase=iw59_chunk status=success chunk=%s/%s duration_s=%.3f",
            i,
            len(chunks),
            duration,
        )
        extracted_files.append(export_file)

    logger.info("VALIDA_DANI phase=parse_exports start files=%s", len(extracted_files))
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
            logger.info(
                "VALIDA_DANI phase=parse_export file=%s rows=%s header_cols=%s",
                f_path,
                len(data),
                len(header),
            )

    with open(unified_csv_path, "w", newline="", encoding="utf-8-sig") as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(["CLIENTE", "INSTALACAO", "DESCRICAO", "NOTA"])
        for item in all_extracted_data:
            writer.writerow([item.cliente, item.instalacao, item.descricao, item.nota])
    logger.info(
        "VALIDA_DANI phase=parse_exports status=success extracted_rows=%s unified_txt=%s unified_csv=%s",
        len(all_extracted_data),
        unified_file_path,
        unified_csv_path,
    )

    logger.info("VALIDA_DANI phase=compare start")
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
                "NOTAS_SAP",
                "ENCONTRADO_NO_SAP",
            ],
        )
        writer.writeheader()
        writer.writerows(comparison_rows)
    total_duration = time.perf_counter() - started_at
    logger.info(
        "VALIDA_DANI finish status=%s original_rows=%s extracted_rows=%s missing_rows=%s duration_s=%.3f comparison_file=%s chunk_log=%s",
        "success" if missing_count == 0 else "missing_rows",
        len(original_data),
        len(all_extracted_data),
        missing_count,
        total_duration,
        comparison_file,
        out_dir / CHUNK_LOG_FILE_NAME,
    )

    return {
        "status": "success" if missing_count == 0 else "missing_rows",
        "unified_file_path": str(unified_file_path),
        "unified_csv_path": str(unified_csv_path),
        "comparison_file": str(comparison_file),
        "log_file": str(log_path),
        "chunk_log_file": str(out_dir / CHUNK_LOG_FILE_NAME),
        "total_originais": len(original_data),
        "total_extraidos": len(all_extracted_data),
        "total_faltantes": missing_count,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validação DANI - IW59")
    parser.add_argument("--input", default=str(DEFAULT_INPUT_PATH), help="Caminho para a planilha original DANI")
    parser.add_argument("--output-dir", default="output/valida_dani", help="Diretório de saída")
    parser.add_argument("--config", default="sap_iw69_batch_config.json", help="Caminho para configuração do SAP")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Tamanho dos lotes de clientes")
    parser.add_argument("--created-by", default=DEFAULT_CREATED_BY, help="Valor do campo Criado por na IW59")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("valida_dani")

    try:
        result = run_valida_dani(
            resolve_input_path(args.input),
            Path(args.output_dir),
            Path(args.config),
            chunk_size=args.chunk_size,
            created_by=args.created_by,
        )
        logger.info(f"Processo concluído com sucesso.")
        logger.info(f"Arquivo unificado TXT: {result['unified_file_path']}")
        logger.info(f"Arquivo unificado CSV: {result['unified_csv_path']}")
        logger.info(f"Resultado da validação: {result['comparison_file']}")
        logger.info(f"Log detalhado: {result['log_file']}")
        logger.info(f"Log de chunks: {result['chunk_log_file']}")
        logger.info(f"Faltantes na validação: {result['total_faltantes']}")
        return 0
    except Exception as e:
        logger.exception(f"Erro na validação DANI: {e}")
        return 1
