# AGENTS.md
Repository guidance for coding agents working in `descargar_datos`.

## Scope and stack
- Language: Python
- Dependency file: `requirements.txt` (no `pyproject.toml` found)
- Virtual environment: `venv/`
- Main scripts: `descargar_datos.py`, `primer_vistazo.py`, `reorganizar_datos.py`, `eliminar_carpetas_fx.py`
- Data/output path: `datos/`

## Mandatory Python environment workflow
Always run Python from the local venv.
1. Check whether `venv/` exists.
2. If missing, create it.
3. Install `uv` into that venv.
4. Install dependencies using `uv pip install ...`.
5. If dependencies change, update `requirements.txt` with pinned versions.

PowerShell setup:
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install uv
uv pip install -r requirements.txt
```
CMD setup:
```cmd
python -m venv venv
venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install uv
uv pip install -r requirements.txt
```
When dependencies are added/updated:
```powershell
uv pip freeze > requirements.txt
```

## Build, lint, and test commands
No formal build toolchain (Makefile/tox/nox) is configured.
Treat script execution + checks as the validation pipeline.

Install project dependencies:
```powershell
uv pip install -r requirements.txt
```
Run key scripts (smoke):
```powershell
python descargar_datos.py --source_id MIROC6 --directorio_salida datos
python primer_vistazo.py
python reorganizar_datos.py
python eliminar_carpetas_fx.py
```
Recommended quality tools (install if absent):
```powershell
uv pip install ruff black mypy pytest
```
Lint/format/type checks:
```powershell
ruff check .
black --check .
mypy .
```
Auto-fix formatting/import order (only when requested):
```powershell
ruff check . --fix
black .
```

Tests (pytest conventions; no committed `tests/` dir detected yet):
Run full suite:
```powershell
pytest -q
```
Run one test file:
```powershell
pytest tests/test_descargar_datos.py -q
```
Run a single test function (preferred single-test command):
```powershell
pytest tests/test_descargar_datos.py::test_nombre_caso -q
```
Run one class test:
```powershell
pytest tests/test_descargar_datos.py::TestClase::test_metodo -q
```
Run by keyword when node id is unknown:
```powershell
pytest -q -k "descarga and not lento"
```

## Code style and implementation rules
Follow existing repository style first; when unclear, use these rules.

### Imports
- Order imports as: standard library, third-party, local.
- Keep imports explicit; avoid wildcard imports.
- Remove unused imports.
- Keep import blocks stable and grouped.

### Formatting
- Use Black-compatible formatting (88 char target).
- Use 4 spaces indentation.
- Keep functions focused and readable.
- Avoid large unrelated reformatting in feature PRs.

### Types
- Add type hints to new or changed function signatures.
- Keep return types explicit for public functions.
- Use one optional style per file (`Optional[T]` or `T | None`).
- Prefer concrete container types where practical.

### Naming
- Modules/files: `snake_case.py`
- Variables/functions: `snake_case`
- Classes: `PascalCase`
- Constants: `UPPER_SNAKE_CASE`
- Private helpers: leading underscore
- Keep established Spanish domain names when editing existing code.

### Docstrings and comments
- Add docstrings for public classes/functions.
- Keep docstrings short and behavior-focused.
- Add comments only for non-obvious logic.
- Explain why, not what.

### Error handling
- Validate inputs early and fail fast.
- Catch specific exceptions when possible.
- Avoid bare `except:` blocks.
- Log actionable context for operational failures.
- Preserve tracebacks when wrapping (`raise ... from e`).

### Logging and output
- Prefer `logging` for operational scripts.
- Keep CLI output concise and useful.
- Do not spam logs in tight loops unless debugging.
- Use consistent levels: `debug/info/warning/error`.

### Paths and file I/O
- Prefer `pathlib.Path` over string path manipulation.
- Use explicit file encodings (`utf-8`) for text.
- Create directories with `mkdir(parents=True, exist_ok=True)`.
- Be careful with destructive actions (`unlink`, `rmtree`).

### Data and performance
- Prefer vectorized pandas operations when feasible.
- Use context managers for NetCDF and file access.
- Avoid unnecessary full-memory loads of large data.

## Testing guidance for new code
- Add tests in `tests/` as `test_*.py`.
- Cover at least one happy path and one edge/error case.
- For parsing/path logic, prefer deterministic unit tests.
- Mock network/heavy I/O when possible.
- Add regression tests for bug fixes.

## Agent operating rules for this repo
- Read related script(s) before editing to match local style.
- Make minimal, targeted changes; avoid unrelated refactors.
- Do not delete user datasets in `datos/` unless explicitly requested.
- If dependencies are changed, pin and update `requirements.txt`.
- Start verification with narrow checks, then broader checks.

## Cursor/Copilot rules status
Checked and not found at generation time:
- `.cursorrules`
- `.cursor/rules/`
- `.github/copilot-instructions.md`
If these files appear later, treat them as mandatory and merge their rules into this document.
