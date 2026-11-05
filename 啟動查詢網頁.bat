@echo off
chcp 65001 >nul
title Taiwan Stock DataLake - 數據查詢工作台
cd /d "%~dp0"

echo ============================================================
echo   Taiwan Stock DataLake 數據湖查詢平台
echo   資料來源: GitHub 雲端 (dj8111/taiwan-stock-datalake)
echo ============================================================
echo.

:: 1. 優先使用本專案虛擬環境 Python (.venv)
set PYTHON_CMD=
if exist ".venv\Scripts\python.exe" (
    set PYTHON_CMD=.venv\Scripts\python.exe
) else (
    where python >nul 2>nul
    if %ERRORLEVEL% EQU 0 (
        set PYTHON_CMD=python
    ) else (
        where py >nul 2>nul
        if %ERRORLEVEL% EQU 0 (
            set PYTHON_CMD=py
        )
    )
)

if "%PYTHON_CMD%"=="" (
    echo [錯誤] 找不到 Python，請先確認已安裝 Python！
    echo.
    pause
    exit /b 1
)

echo [資訊] 使用環境: %PYTHON_CMD%
echo [資訊] 正在啟動伺服器並自動開啟瀏覽器...
echo [提示] 伺服器運作中，請勿關閉此視窗 (關閉視窗即停止服務)。
echo ============================================================
echo.

"%PYTHON_CMD%" app.py

pause
