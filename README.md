# Reorganizador 2.0
 La idea general es tener una herramienta que copie o mueva archivos desde una estructura como `Gestores/AÑO/GESTOR/PROYECTO` hacia otra carpeta destino, manteniendo la jerarquía y guardando un montón de información extra sobre cada archivo.

## Qué hace
- Recorre la carpeta origen y respeta la misma estructura en la carpeta destino.
- Según la opción de organización, puede crear subcarpetas por tipo de archivo (`pdf`, `jpg`, etc.).
- Saca metadatos básicos: tamaño, fechas, tipo MIME, hashes, rutas.
- Detecta automáticamente el nombre del gestor y el número de proyecto leyendo la ruta.
- Puede copiar o mover archivos y, si quieres, verificar que el hash del destino sea el mismo.
- Guarda todos los datos en tres sitios al mismo tiempo (si los configuras): CSV, SQLite y SQL Server.
- Tiene modo incremental, así que no repite trabajo si ya procesó un archivo.
- Hay un comando que vigila la carpeta con Watchdog para copiar archivos nuevos al vuelo.
- También existe un comando de verificación que recalcula los hashes para comprobar que todo sigue bien.

## Requisitos
- Python 3.10 o superior.
- Dependencias del archivo `extractor_v2/requirements.txt`:
  - `rich`
  - `watchdog`
  - `pyodbc` (solo si vas a usar SQL Server)
  - `tqdm`

Instalación rápida:
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r extractor_v2/requirements.txt
```

## Comandos principales
Todos los comandos se lanzan con `python -m extractor_v2.main ...`.

### Escaneo (scan)
```bash
python -m extractor_v2.main scan ^
  --source "C:\ruta\origen\Gestores" ^
  --dest "C:\ruta\destino\Gestores" ^
  --organize-by type-date ^
  --csv-out "CSV-BD\metadatos.csv" ^
  --sqlite-db "CSV-BD\metadatos.db" ^
  --dry-run
```
Opciones útiles:
- `--organize-by`: `flat`, `type`, `date`, `type-date`.
- `--move`: en vez de copiar, mueve.
- `--dry-run`: muestra lo que haría sin tocar nada (muy recomendable al principio).
- `--hash-algo`: `sha256`, `sha1`, `md5` o `none`.
- `--no-incremental`: rehace todo aunque esté guardado en la BD/CSV.
- `--no-verify`: salta la verificación del hash en la copia.
- `--threads` y `--processes`: ajusta la concurrencia cuando hay muchos archivos.

### Monitorización (watch)
```bash
python -m extractor_v2.main watch ^
  --source "C:\carpeta\a\vigilar" ^
  --dest "C:\carpeta\destino" ^
  --organize-by type ^
  --csv-out "CSV-BD\metadatos.csv" ^
  --sqlite-db "CSV-BD\metadatos.db"
```
Escucha cambios en tiempo real y aplica la misma lógica de organización y guardado.

### Verificación (verify)
```bash
python -m extractor_v2.main verify ^
  --sqlite-db "CSV-BD\metadatos.db" ^
  --hash-algo sha256
```
Vuelve a calcular el hash en origen y destino y avisa si falta algo o si no coincide.

## Datos que se guardan
Por cada archivo se almacena:
- Nombre, extensión, tipo MIME y tamaño.
- Fechas de creación, modificación y acceso.
- Algoritmo de hash y valores calculados.
- Rutas de origen y destino.
- Acción realizada (`copy`, `move`, `skip`), estado y errores si los hubo.
- Campo `verified` para saber si la verificación salió bien.
- **Gestor y proyecto**, que salen de la ruta del archivo (`Gestores/2025/MAR/250076/...`).

## Dónde se guardan los datos
- **CSV**: usa `metadatos.csv`, muy útil para abrir en Excel o compartir.
- **SQLite**: la tabla `files` se crea sola (`metadatos.db`) y se actualiza con `INSERT` o `UPDATE` según corresponda.
- **SQL Server**: opcional, usando `pyodbc` y la tabla `dbo.files`. Si la tabla existe pero le faltan columnas nuevas, se añaden automáticamente.

## Logs
- Se guardan en `logs/AAAA-MM-DD.log`.
- En la consola se usan colores y tablas gracias a la librería Rich.
- Los mensajes “Adding missing 'gestor' column…” aparecen cuando la BD todavía no tenía esas columnas; solo sale una vez.

## Consejos personales
- Siempre empiezo con `--dry-run` para ver si las carpetas y rutas quedan como espero.
- No borres el CSV ni la base de datos si quieres que el modo incremental funcione.
- Si activas `--move`, asegúrate de tener copia de seguridad, por si acaso.
- Programa verificaciones de vez en cuando para asegurarte de que la copia sigue intacta.


