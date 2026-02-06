# Soul Sense - Tauri Environment Setup Script
# This script ensures Rust and Tauri CLI are installed.

$ErrorActionPreference = "Stop"

Write-Host "Checking for Rust installation..." -ForegroundColor Cyan

try {
    $rustVersion = & rustc --version
    Write-Host "✅ Rust is already installed: $rustVersion" -ForegroundColor Green
} catch {
    Write-Host "⚠️ Rust not found. Installing Rust..." -ForegroundColor Yellow
    
    $installerPath = "$env:TEMP\rustup-init.exe"
    Invoke-WebRequest -Uri "https://static.rust-lang.org/rustup/dist/x86_64-pc-windows-msvc/rustup-init.exe" -OutFile $installerPath
    
    Write-Host "Running rustup-init... this may take a few minutes."
    & $installerPath -y
    
    Remove-Item $installerPath
    
    Write-Host "✅ Rust installed successfully." -ForegroundColor Green
    Write-Host "NOTE: You may need to restart your terminal for PATH changes to take effect." -ForegroundColor Cyan
}

Write-Host "Checking for Tauri CLI..." -ForegroundColor Cyan
try {
    $tauriVersion = npx tauri --version
    Write-Host "✅ Tauri CLI is available via npx." -ForegroundColor Green
} catch {
    Write-Host "Installing Tauri CLI globally..." -ForegroundColor Yellow
    npm install -g @tauri-apps/cli
    Write-Host "✅ Tauri CLI installed." -ForegroundColor Green
}

Write-Host "Building Python Backend Sidecar..." -ForegroundColor Cyan
try {
    # Ensure PyInstaller is installed in the current python env
    python -m pip install pyinstaller
    
    # Build the sidecar
    cd .. # Go to project root if script is run from scripts/
    python -m PyInstaller --onefile --name soul-sense-backend --clean backend/fastapi/sidecar.py
    
    # Identify target triple
    $target = & rustc -vV | Select-String "host: " | ForEach-Object { $_.ToString().Split(": ")[1].Trim() }
    
    # Move to Tauri binaries
    $binDir = "frontend-web\src-tauri\binaries"
    if (!(Test-Path $binDir)) { New-Item -ItemType Directory -Path $binDir }
    move "dist\soul-sense-backend.exe" "$binDir\soul-sense-backend-$target.exe"
    
    Write-Host "✅ Sidecar built and moved to $binDir" -ForegroundColor Green
} catch {
    Write-Host "❌ Failed to build sidecar: $_" -ForegroundColor Red
}

Write-Host "`nReady for Phase 1 development!" -ForegroundColor Magenta
Write-Host "Run 'npm run tauri dev' inside 'frontend-web' to start." -ForegroundColor Cyan
