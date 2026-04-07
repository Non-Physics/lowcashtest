@echo off
setlocal
set PROJECT_ROOT=%~dp0
call "%PROJECT_ROOT%entrypoints\trading\运行交易日常.cmd" %*
