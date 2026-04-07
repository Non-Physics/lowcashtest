@echo off
setlocal
set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%\..\..
set PYTHON_EXE=%PROJECT_ROOT%\.venv-trader32\Scripts\python.exe

if not exist "%PYTHON_EXE%" (
  echo 未找到交易环境 Python: "%PYTHON_EXE%"
  exit /b 1
)

"%PYTHON_EXE%" "%SCRIPT_DIR%运行交易日常.py" %*
