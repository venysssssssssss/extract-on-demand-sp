import argparse
import csv
import logging
from pathlib import Path
from typing import Any

import openpyxl

def chunk_list(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def read_dani_excel(file_path: Path) -> list[dict[str, str]]:
    wb = openpyxl.load_workbook(file_path, data_only=True)
    ws = wb.active
    data = []
    header = [str(cell.value or "").strip() for cell in ws[1]]
    
    col_cliente = -1
    col_instalacao = -1
    col_descricao = -1
    
    for i, col_name in enumerate(header):
        upper_name = col_name.upper()
        if "CLIENTE" in upper_name:
            col_cliente = i
        elif "INSTALA" in upper_name or "INSTALACAO" in upper_name:
            col_instalacao = i
        elif "DESCRI" in upper_name or "DESCRICAO" in upper_name:
            col_descricao = i

    if col_cliente == -1: col_cliente = 0
    if col_instalacao == -1: col_instalacao = 1
    if col_descricao == -1: col_descricao = 2

    for row in ws.iter_rows(min_row=2):
        if len(row) <= max(col_cliente, col_instalacao, col_descricao):
            continue
        cliente = str(row[col_cliente].value or "").strip()
        if not cliente:
            continue
        instalacao = str(row[col_instalacao].value or "").strip()
        descricao = str(row[col_descricao].value or "").strip()
        data.append({
            "CLIENTE": cliente,
            "INSTALACAO": instalacao,
            "DESCRICAO": descricao
        })
    return data

def execute_iw59_chunk(session: Any, clientes: list[str], export_path: Path) -> None:
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
    
    # Criado por BRMARI (ID do SAP)
    session.findById("wnd[0]/usr/ctxtERNAM-LOW").text = "BR0041761455"
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

def parse_sap_text_export(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    try:
        content = path.read_text(encoding="cp1252", errors="ignore")
    except Exception:
        content = path.read_text(encoding="utf-16", errors="ignore")
        
    lines = content.splitlines()
    if not lines:
        return [], []
        
    header = [c.strip() for c in lines[0].split("\t")]
    data = []
    
    col_cliente, col_instalacao, col_descricao = -1, -1, -1
    for i, col_name in enumerate(header):
        upper_name = col_name.upper()
        if upper_name in ["CLIENTE", "CLIENTES", "CLIENT", "KUNUM"]:
            col_cliente = i
        elif upper_name in ["INSTALACAO", "INSTALAÇÃO", "ANLAGE"]:
            col_instalacao = i
        elif upper_name in ["DESCRICAO", "DESCRIÇÃO", "DESCRIPTION", "KTEXT"]:
            col_descricao = i
            
    for line in lines[1:]:
        parts = line.split("\t")
        cliente = parts[col_cliente].strip() if col_cliente >= 0 and len(parts) > col_cliente else ""
        instalacao = parts[col_instalacao].strip() if col_instalacao >= 0 and len(parts) > col_instalacao else ""
        descricao = parts[col_descricao].strip() if col_descricao >= 0 and len(parts) > col_descricao else ""
        
        data.append({
            "CLIENTE": cliente,
            "INSTALACAO": instalacao,
            "DESCRICAO": descricao,
            "RAW_LINE": line
        })
        
    return header, data

def run_valida_dani(input_path: Path, out_dir: Path) -> dict[str, Any]:
    logger = logging.getLogger("valida_dani")
    
    if not input_path.exists():
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {input_path}")
        
    out_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("Lendo dados originais...")
    original_data = read_dani_excel(input_path)
    logger.info(f"Foram lidas {len(original_data)} linhas válidas da planilha.")
    
    clientes_unicos = list(set([d["CLIENTE"] for d in original_data]))
    logger.info(f"Clientes únicos para consultar: {len(clientes_unicos)}")
    
    logger.info("Conectando ao SAP...")
    try:
        import win32com.client
        SapGuiAuto = win32com.client.GetObject("SAPGUI")
        application = SapGuiAuto.GetScriptingEngine
        connection = application.Children(0)
        session = connection.Children(0)
    except Exception as e:
        raise RuntimeError(f"Erro ao conectar ao SAP. Verifique se o SAP Logon está aberto. Detalhes: {e}")
        
    chunks = list(chunk_list(clientes_unicos, 5000))
    extracted_files = []
    
    for i, chunk in enumerate(chunks, 1):
        export_file = out_dir / f"iw59_export_chunk_{i}.txt"
        logger.info(f"Processando chunk {i}/{len(chunks)} com {len(chunk)} clientes...")
        execute_iw59_chunk(session, chunk, export_file)
        extracted_files.append(export_file)
        
    logger.info("Juntando arquivos extraídos e extraindo dados...")
    all_extracted_data = []
    unified_file_path = out_dir / "extracao_completa_SAP.txt"
    unified_csv_path = out_dir / "extracao_completa_SAP.csv"
    
    global_header = []
    with open(unified_file_path, "w", encoding="utf-8") as out_unified:
        for i, f_path in enumerate(extracted_files):
            header, data = parse_sap_text_export(f_path)
            if i == 0 and header:
                global_header = header
                out_unified.write("\t".join(header) + "\n")
            for item in data:
                all_extracted_data.append(item)
                out_unified.write(item.get("RAW_LINE", "") + "\n")
                
    with open(unified_csv_path, "w", newline="", encoding="utf-8-sig") as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(["CLIENTE", "INSTALACAO", "DESCRICAO"])
        for item in all_extracted_data:
            writer.writerow([item.get("CLIENTE"), item.get("INSTALACAO"), item.get("DESCRICAO")])
            
    logger.info("Comparando com planilha original...")
    comparison_file = out_dir / "resultado_validacao.csv"
    
    with open(comparison_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["CLIENTE_ORIGINAL", "INSTALACAO_ORIGINAL", "DESCRICAO_ORIGINAL", "ENCONTRADO_NO_SAP"])
        
        for orig in original_data:
            c_orig = orig["CLIENTE"]
            c_no_zeros = c_orig.lstrip("0") if c_orig.isdigit() else c_orig
            i_orig = orig["INSTALACAO"]
            i_no_zeros = i_orig.lstrip("0") if i_orig.isdigit() else i_orig
            
            found = "NAO"
            for ext in all_extracted_data:
                ext_c = ext["CLIENTE"]
                ext_c_no_zeros = ext_c.lstrip("0") if ext_c.isdigit() else ext_c
                ext_i = ext["INSTALACAO"]
                ext_i_no_zeros = ext_i.lstrip("0") if ext_i.isdigit() else ext_i
                
                if c_no_zeros == ext_c_no_zeros and i_no_zeros == ext_i_no_zeros:
                    found = "SIM"
                    break
                    
            writer.writerow([c_orig, i_orig, orig["DESCRICAO"], found])
            
    return {
        "status": "success",
        "unified_file_path": str(unified_file_path),
        "unified_csv_path": str(unified_csv_path),
        "comparison_file": str(comparison_file),
        "total_originais": len(original_data),
        "total_extraidos": len(all_extracted_data)
    }

def main() -> int:
    parser = argparse.ArgumentParser(description="Validação DANI - IW59")
    parser.add_argument("--input", default="projeto_Dani2.xlsm", help="Caminho para a planilha original DANI")
    parser.add_argument("--output-dir", default="output/valida_dani", help="Diretório de saída")
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("valida_dani")
    
    try:
        result = run_valida_dani(Path(args.input), Path(args.output_dir))
        logger.info(f"Processo concluído com sucesso.")
        logger.info(f"Arquivo unificado TXT: {result['unified_file_path']}")
        logger.info(f"Arquivo unificado CSV: {result['unified_csv_path']}")
        logger.info(f"Resultado da validação: {result['comparison_file']}")
        return 0
    except Exception as e:
        logger.error(f"Erro na validação DANI: {e}")
        return 1
