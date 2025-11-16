# EnhancedPutSQL Deployment Verification Script
# Run this script to verify your NAR deployment

param(
    [string]$NiFiLibPath = ""
)

Write-Host "=== EnhancedPutSQL Deployment Verification ===" -ForegroundColor Cyan
Write-Host ""

# Step 1: Verify NAR file exists
Write-Host "1. Checking NAR file..." -ForegroundColor Yellow
$narPath = "nifi-enhanced-putsql-nar\target\nifi-enhanced-putsql-nar-2.1.0.nar"
if (Test-Path $narPath) {
    $narFile = Get-Item $narPath
    Write-Host "   [OK] NAR file found: $($narFile.Name)" -ForegroundColor Green
    Write-Host "   Size: $($narFile.Length) bytes" -ForegroundColor Gray
    Write-Host "   Modified: $($narFile.LastWriteTime)" -ForegroundColor Gray
} else {
    Write-Host "   [FAIL] NAR file NOT found at: $narPath" -ForegroundColor Red
    Write-Host "   Run: mvn clean package -DskipTests" -ForegroundColor Yellow
    exit 1
}

# Step 2: Verify service file in NAR
Write-Host "`n2. Checking service file in NAR..." -ForegroundColor Yellow
Add-Type -AssemblyName System.IO.Compression.FileSystem
$nar = [System.IO.Compression.ZipFile]::OpenRead($narPath)
$serviceFile = $nar.Entries | Where-Object {$_.FullName -eq "META-INF/services/org.apache.nifi.processor.Processor"}
if ($serviceFile) {
    $stream = $serviceFile.Open()
    $reader = New-Object System.IO.StreamReader($stream)
    $content = $reader.ReadToEnd().Trim()
    $reader.Close()
    $stream.Close()
    
    if ($content -match "com\.acme\.nifi\.processors\.EnhancedPutSQL") {
        Write-Host "   [OK] Service file found with correct class name" -ForegroundColor Green
        $lines = $content -split "`n"
        $className = $lines[$lines.Length - 1].Trim()
        Write-Host "   Class: $className" -ForegroundColor Gray
    } else {
        Write-Host "   [FAIL] Service file found but class name incorrect" -ForegroundColor Red
        Write-Host "   Content: $content" -ForegroundColor Gray
    }
} else {
    Write-Host "   [FAIL] Service file NOT found in NAR!" -ForegroundColor Red
}
$nar.Dispose()

# Step 3: Check if deployed to NiFi
if ($NiFiLibPath -ne "") {
    Write-Host "`n3. Checking NiFi deployment..." -ForegroundColor Yellow
    $deployedNar = Join-Path $NiFiLibPath "nifi-enhanced-putsql-nar-2.1.0.nar"
    if (Test-Path $deployedNar) {
        Write-Host "   [OK] NAR found in NiFi lib directory" -ForegroundColor Green
        
        # Check for old versions
        $oldVersions = Get-ChildItem $NiFiLibPath -Filter "*enhanced-putsql*.nar" | Where-Object {$_.Name -ne "nifi-enhanced-putsql-nar-2.1.0.nar"}
        if ($oldVersions) {
            Write-Host "   [WARN] Old NAR versions found (remove these):" -ForegroundColor Yellow
            $oldVersions | ForEach-Object { Write-Host "     - $($_.Name)" -ForegroundColor Gray }
        }
    } else {
        Write-Host "   [FAIL] NAR NOT found in NiFi lib directory: $NiFiLibPath" -ForegroundColor Red
        Write-Host "   Copy command:" -ForegroundColor Yellow
        $fullPath = (Get-Item $narPath).FullName
        Write-Host "   Copy-Item `"$fullPath`" `"$NiFiLibPath`" -Force" -ForegroundColor Cyan
    }
} else {
    Write-Host "`n3. NiFi deployment check skipped (provide -NiFiLibPath parameter)" -ForegroundColor Gray
}

# Step 4: Verify processor class
Write-Host "`n4. Checking processor class..." -ForegroundColor Yellow
$classFile = "nifi-enhanced-putsql-processors\target\classes\com\acme\nifi\processors\EnhancedPutSQL.class"
if (Test-Path $classFile) {
    Write-Host "   [OK] Processor class compiled" -ForegroundColor Green
} else {
    Write-Host "   [FAIL] Processor class NOT found" -ForegroundColor Red
    Write-Host "   Run: mvn clean compile" -ForegroundColor Yellow
}

# Step 5: Summary
Write-Host "`n=== Summary ===" -ForegroundColor Cyan
$narOk = Test-Path $narPath
$serviceOk = $serviceFile -ne $null
$classOk = Test-Path $classFile

Write-Host "   NAR File: $(if ($narOk) {'[OK]'} else {'[FAIL]'})" -ForegroundColor $(if ($narOk) {'Green'} else {'Red'})
Write-Host "   Service File: $(if ($serviceOk) {'[OK]'} else {'[FAIL]'})" -ForegroundColor $(if ($serviceOk) {'Green'} else {'Red'})
Write-Host "   Processor Class: $(if ($classOk) {'[OK]'} else {'[FAIL]'})" -ForegroundColor $(if ($classOk) {'Green'} else {'Red'})

Write-Host "`n=== Next Steps ===" -ForegroundColor Cyan
Write-Host "1. Copy NAR to NiFi lib directory" -ForegroundColor Yellow
Write-Host "2. Remove any old versions of the NAR" -ForegroundColor Yellow
Write-Host "3. FULLY restart NiFi (stop completely, wait 30s, start)" -ForegroundColor Yellow
Write-Host "4. Check NiFi logs for errors" -ForegroundColor Yellow
Write-Host "5. Search for 'EnhancedPutSQL' in NiFi processor palette" -ForegroundColor Yellow

Write-Host "`nDone!" -ForegroundColor Green
