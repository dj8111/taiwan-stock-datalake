@echo off
title Taiwan Stock DataLake Web Console
cd /d "%~dp0"

echo ============================================================
echo   Taiwan Stock DataLake Web Console
echo   Data Source: Local Parquet DataLake & GitHub
echo   Starting server and opening browser at http://localhost:8888 ...
echo ============================================================
echo.

where py >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    py -3.12 app.py
) else (
    where python >nul 2>nul
    if %ERRORLEVEL% EQU 0 (
        python app.py
    ) else (
        echo [ERROR] Python was not found on your system!
        pause
        exit /b 1
    )
)

pause
