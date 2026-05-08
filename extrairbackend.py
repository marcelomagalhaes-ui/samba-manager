"""
extrairbackend.py
=================
Utilitário para extrair e concatenar todo o código-fonte do Samba Export Control Desk
em um único arquivo de texto (.txt). Ideal para backup e para enviar como 
contexto para assistentes de IA.
"""
import sys
from pathlib import Path
from datetime import datetime

# Define a raiz do projeto (onde este script está rodando)
ROOT = Path(__file__).resolve().parent

# Pastas e arquivos principais que queremos mapear
TARGET_DIRS = ["agents", "config", "dashboards", "models", "parsers", "services"]
TARGET_FILES = ["main.py", "requirements.txt", "README.md"]

def extrair_codigo():
    print(f"\n🎷 SAMBA EXPORT - Extração de Código Fonte")
    print("="*60)

    # Gera um nome de arquivo com a data e hora atuais para não sobrescrever backups antigos
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = f"backup_codigo_samba_{timestamp}.txt"
    output_path = ROOT / output_filename

    total_files = 0
    total_lines = 0

    with open(output_path, "w", encoding="utf-8") as outfile:
        outfile.write(f"=== SAMBA EXPORT CONTROL DESK - EXPORTAÇÃO DE CÓDIGO ===\n")
        outfile.write(f"Data da extração: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n")

        # 1. Extrair arquivos soltos na raiz (main.py, requirements, etc)
        for file_name in TARGET_FILES:
            file_path = ROOT / file_name
            if file_path.exists():
                try:
                    with open(file_path, "r", encoding="utf-8") as infile:
                        content = infile.read()
                        lines_count = len(content.splitlines())
                        
                        outfile.write(f"{'='*80}\n")
                        outfile.write(f"ARQUIVO: {file_name}\n")
                        outfile.write(f"{'='*80}\n\n")
                        outfile.write(content)
                        outfile.write("\n\n")
                        
                        total_files += 1
                        total_lines += lines_count
                        print(f"  ✓ Raiz: {file_name} ({lines_count} linhas)")
                except Exception as e:
                    print(f"  ❌ Erro ao ler {file_name}: {e}")

        # 2. Extrair todos os arquivos .py das pastas do sistema
        for folder_name in TARGET_DIRS:
            folder_path = ROOT / folder_name
            if folder_path.exists() and folder_path.is_dir():
                # rglob("*.py") busca recursivamente todos os arquivos Python
                for filepath in sorted(folder_path.rglob("*.py")):
                    # Ignorar arquivos cacheados do python
                    if "__pycache__" in str(filepath):
                        continue
                        
                    rel_path = filepath.relative_to(ROOT)
                    try:
                        with open(filepath, "r", encoding="utf-8") as infile:
                            content = infile.read()
                            lines_count = len(content.splitlines())
                            
                            outfile.write(f"{'='*80}\n")
                            outfile.write(f"ARQUIVO: {rel_path}\n")
                            outfile.write(f"{'='*80}\n\n")
                            outfile.write(content)
                            outfile.write("\n\n")
                            
                            total_files += 1
                            total_lines += lines_count
                            print(f"  ✓ Pasta: {rel_path} ({lines_count} linhas)")
                    except Exception as e:
                        print(f"  ❌ Erro ao ler {rel_path}: {e}")

    print("="*60)
    print(f"✅ Extração concluída com sucesso!")
    print(f"📊 Resumo: {total_files} arquivos processados | {total_lines} linhas de código.")
    print(f"💾 Arquivo gerado: {output_filename} (salvo na raiz do projeto)\n")

if __name__ == "__main__":
    extrair_codigo()