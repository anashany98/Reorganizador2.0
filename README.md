# Reorganizador 2.0

Herramienta local para copiar o mover archivos desde una carpeta origen hacia una carpeta destino, conservar trazabilidad y guardar metadatos en CSV, SQLite y opcionalmente SQL Server.

## Que hace

- Recorre una carpeta origen de forma recursiva.
- Copia o mueve archivos a destino.
- Mantiene la jerarquia original o reorganiza por tipo, fecha, tipo-fecha, categoria o proyecto.
- Extrae metadatos basicos: nombre, extension, MIME, tamaño, fechas, hashes y rutas.
- Detecta gestor y numero de proyecto a partir de rutas tipo `Gestores/2025/MAR/250076/...`.
- Evita repetir trabajo con modo incremental.
- Verifica el hash del destino despues de copiar o mover, salvo que se use `--no-verify`.
- Resuelve conflictos con `rename`, `overwrite`, `skip` u `overwrite-if-newer`.
- Puede deduplicar archivos iguales con hardlinks (`--dedup`) cuando el sistema de archivos lo permite.
- Incluye una interfaz web local para usuarios internos.

## Requisitos

- Python 3.10 o superior.
- Windows recomendado para los lanzadores PowerShell.
- Dependencias:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r reorganizador_v2\requirements.txt
```

## Uso rapido con PowerShell

Escaneo seguro sin copiar:

```powershell
.\launch_reorganizador_v2.ps1 -Command scan `
  -Source "C:\ruta\origen" `
  -Dest "D:\ruta\destino" `
  -DryRun
```

Copia real con verificacion de hash:

```powershell
.\launch_reorganizador_v2.ps1 -Command scan `
  -Source "C:\ruta\origen" `
  -Dest "D:\ruta\destino" `
  -OrganizeBy type-date `
  -CsvOut "CSV-BD\metadatos.csv" `
  -SqliteDb "CSV-BD\metadatos.db"
```

Previsualizar antes de procesar:

```powershell
.\launch_reorganizador_v2.ps1 -Command preview `
  -Source "C:\ruta\origen" `
  -Projects "250076,250077"
```

Verificar copias ya registradas:

```powershell
.\launch_reorganizador_v2.ps1 -Command verify `
  -SqliteDb "CSV-BD\metadatos.db" `
  -HashAlgo sha256
```

## Uso directo con Python

```powershell
python -m reorganizador_v2.main scan `
  --source "C:\ruta\origen" `
  --dest "D:\ruta\destino" `
  --organize-by type-date `
  --hash-algo sha256 `
  --csv-out "CSV-BD\metadatos.csv" `
  --sqlite-db "CSV-BD\metadatos.db"
```

Opciones utiles:

- `--dry-run`: simula sin copiar ni mover.
- `--move`: mueve archivos en vez de copiarlos.
- `--organize-by`: `flat`, `type`, `date`, `type-date`, `hierarchical-type-ext`, `project-type`.
- `--conflict`: `rename`, `overwrite`, `skip`, `overwrite-if-newer`.
- `--projects`: lista separada por comas o ruta a TXT/CSV con numeros de proyecto.
- `--dedup`: usa hardlinks para duplicados cuando es posible.
- `--no-incremental`: ignora cache previa.
- `--no-verify`: no recalcula hash del destino.
- `--excel-out`: genera un Excel de auditoria al terminar.

## Interfaz web local

```powershell
.\launch_web.ps1
```

El servidor escucha por defecto en `http://127.0.0.1:8000`. Si el puerto esta ocupado y no se indico `-Port`, el lanzador busca otro puerto disponible.

## Datos generados

Cada registro incluye:

- nombre, extension, MIME y tamaño;
- fechas de creacion, modificacion y acceso;
- hash de origen, hash de destino y resultado de verificacion;
- ruta origen y destino;
- gestor y proyecto detectados;
- accion (`copy`, `move`, `hardlink`, `skip`, `scan`) y estado;
- error, si lo hubo.

## Recomendaciones de operacion interna

- Ejecutar primero con `-DryRun`.
- Usar SQLite como fuente principal de trazabilidad (`CSV-BD\metadatos.db`).
- Mantener `--no-verify` desactivado salvo que haya una razon operativa clara.
- Probar filtros de proyecto con `preview` antes de una copia real.
- No versionar bases de datos, CSV, logs ni salidas de auditoria generadas.

## Pruebas

```powershell
python -m pytest -q
```
