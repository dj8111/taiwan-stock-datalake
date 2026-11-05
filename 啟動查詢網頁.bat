@echo off
title Taiwan Stock DataLake Web Console
cd /d "%~dp0"

echo ============================================================
echo   Taiwan Stock DataLake Web Console
echo   Data Source: GitHub (dj8111/taiwan-stock-datalake)
echo   Starting server and opening browser at http://localhost:8888 ...
echo ============================================================
echo.

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" app.py
) else (
    where python >nul 2>nul
    if %ERRORLEVEL% EQU 0 (
        python app.py
    ) else (
        where py >nul 2>nul
        if %ERRORLEVEL% EQU 0 (
            py app.py
        ) else (
            echo [ERROR] Python was not found on your system!
            echo Please install Python from https://www.python.org/
            pause
            exit /b 1
        )
    )
)

pause
