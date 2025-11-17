# Quick Fix Script for EnhancedPutSQL Not Showing in UI

param(
    [Parameter(Mandatory=$true)]
    [string]$NiFiLibPath
)

Write-Host "=== EnhancedPutSQL Quick Fix ===" -ForegroundColor Cyan
Write-Host ""

# Step 1: Verify NAR location
Write-Host "1. Checking NAR location..." -ForegroundColor Yellow
$narPath = Join-Path $NiFiLibPath "nifi-enhanced-putsql-nar-2.1.0.nar"
if (Test-Path $narPath) {
    Write-Host "   [OK] NAR found at: $narPath" -ForegroundColor Green
} else {
    Write-Host "   [FAIL] NAR NOT found!" -ForegroundColor Red
    Write-Host "   Copy NAR to: $NiFiLibPath" -ForegroundColor Yellow
    $sourceNar = "nifi-enhanced-putsql-nar\target\nifi-enhanced-putsql-nar-2.1.0.nar"
    if (Test-Path $sourceNar) {
        Write-Host "   Copying from: $sourceNar" -ForegroundColor Cyan
        Copy-Item $sourceNar $NiFiLibPath -Force
        Write-Host "   [OK] NAR copied!" -ForegroundColor Green
    } else {
        Write-Host "   [FAIL] Source NAR not found. Run: mvn clean package -DskipTests" -ForegroundColor Red
        exit 1
    }
}

# Step 2: Remove old versions
Write-Host "`n2. Checking for old versions..." -ForegroundColor Yellow
$oldVersions = Get-ChildItem $NiFiLibPath -Filter "*enhanced-putsql*.nar" -ErrorAction SilentlyContinue | 
    Where-Object {$_.Name -ne "nifi-enhanced-putsql-nar-2.1.0.nar"}
if ($oldVersions) {
    Write-Host "   [WARN] Found old versions:" -ForegroundColor Yellow
    $oldVersions | ForEach-Object { 
        Write-Host "     - $($_.Name)" -ForegroundColor Gray
        Remove-Item $_.FullName -Force
        Write-Host "       [OK] Removed" -ForegroundColor Green
    }
} else {
    Write-Host "   [OK] No old versions found" -ForegroundColor Green
}

# Step 3: Verify service file
Write-Host "`n3. Verifying NAR structure..." -ForegroundColor Yellow
Add-Type -AssemblyName System.IO.Compression.FileSystem
$nar = [System.IO.Compression.ZipFile]::OpenRead($narPath)
$service = $nar.Entries | Where-Object {$_.FullName -eq "META-INF/services/org.apache.nifi.processor.Processor"}
if ($service) {
    $stream = $service.Open()
    $reader = New-Object System.IO.StreamReader($stream)
    $content = $reader.ReadToEnd().Trim()
    $reader.Close()
    $stream.Close()
    if ($content -match "com\.acme\.nifi\.processors\.EnhancedPutSQL") {
        Write-Host "   [OK] Service file correct" -ForegroundColor Green
    } else {
        Write-Host "   [FAIL] Service file incorrect" -ForegroundColor Red
    }
} else {
    Write-Host "   [FAIL] Service file NOT found in NAR!" -ForegroundColor Red
}
$nar.Dispose()

# Step 4: Summary
Write-Host "`n=== Summary ===" -ForegroundColor Cyan
Write-Host "NAR Location: $(if (Test-Path $narPath) {'[OK]'} else {'[FAIL]'})" -ForegroundColor $(if (Test-Path $narPath) {'Green'} else {'Red'})
Write-Host "Old Versions: $(if ($oldVersions) {'[WARN] Removed'} else {'[OK]'})" -ForegroundColor $(if ($oldVersions) {'Yellow'} else {'Green'})
Write-Host "Service File: $(if ($service) {'[OK]'} else {'[FAIL]'})" -ForegroundColor $(if ($service) {'Green'} else {'Red'})

Write-Host "`n=== CRITICAL NEXT STEPS ===" -ForegroundColor Red
Write-Host "1. FULLY STOP NiFi (not just reload)" -ForegroundColor Yellow
Write-Host "2. Wait 30 seconds" -ForegroundColor Yellow
Write-Host "3. START NiFi" -ForegroundColor Yellow
Write-Host "4. Check logs/nifi-app.log for errors" -ForegroundColor Yellow
Write-Host "5. Search for 'EnhancedPutSQL' in processor palette" -ForegroundColor Yellow
Write-Host "6. Clear browser cache (Ctrl+F5)" -ForegroundColor Yellow

Write-Host "`nDone! Restart NiFi now." -ForegroundColor Green

