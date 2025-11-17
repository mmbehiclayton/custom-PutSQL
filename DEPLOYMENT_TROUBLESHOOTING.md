# EnhancedPutSQL Deployment Troubleshooting

## Processor Not Showing in UI - Step-by-Step Fix

### Step 1: Verify NAR File Location
The NAR file must be in NiFi's `lib/` directory (NOT `lib/bootstrap/`).

**Windows:**
```powershell
# Typical locations:
C:\nifi\lib\
C:\nifi\nifi-2.1.0\lib\
C:\Program Files\Apache NiFi\lib\
```

**Linux:**
```bash
/opt/nifi/nifi-current/lib/
/opt/nifi/lib/
```

**Verify:**
```powershell
# Check if NAR exists
Get-ChildItem "C:\path\to\nifi\lib\" -Filter "*enhanced-putsql*.nar"

# Should show:
# nifi-enhanced-putsql-nar-2.1.0.nar
```

### Step 2: Remove ALL Old Versions
Multiple NAR files can cause conflicts:

```powershell
# Find all versions
Get-ChildItem "C:\path\to\nifi\lib\" -Filter "*enhanced-putsql*.nar"

# Remove old versions (keep only 2.1.0)
Get-ChildItem "C:\path\to\nifi\lib\" -Filter "*enhanced-putsql*.nar" | 
    Where-Object {$_.Name -ne "nifi-enhanced-putsql-nar-2.1.0.nar"} | 
    Remove-Item -Force
```

### Step 3: FULL NiFi Restart (Critical!)
**DO NOT** just reload - you must do a **COMPLETE STOP and START**:

```powershell
# Windows Service
Stop-Service nifi
Start-Sleep -Seconds 30  # Wait for complete shutdown
Start-Service nifi

# OR Manual Start
# 1. Stop NiFi (Ctrl+C in terminal, wait for shutdown)
# 2. Wait 30 seconds
# 3. Start NiFi: .\bin\run-nifi.bat
```

**Why?** NiFi caches processor classes. A full restart is required to reload custom processors.

### Step 4: Check NiFi Logs
Look for errors in `logs/nifi-app.log`:

```powershell
# Windows
Get-Content "C:\path\to\nifi\logs\nifi-app.log" -Tail 100 | Select-String -Pattern "EnhancedPutSQL|NoClassDefFoundError|ClassNotFoundException|Failed to instantiate"
```

**Common Errors:**

1. **ClassNotFoundException:**
```
ERROR o.a.n.p.StandardProcessorNode: Failed to instantiate processor
java.lang.ClassNotFoundException: com.acme.nifi.processors.EnhancedPutSQL
```
**Fix:** NAR not in correct location or not fully restarted.

2. **NoClassDefFoundError:**
```
ERROR: java.lang.NoClassDefFoundError: org/apache/nifi/dbcp/DBCPService
```
**Fix:** This is expected and handled by the code. If it appears, it means the processor is trying to load but DBCPService isn't available. Check that `nifi-dbcp-service-api` is in NiFi's classpath.

3. **Processor Loading:**
```
INFO o.a.n.r.StandardProcessorNode: Successfully loaded processor com.acme.nifi.processors.EnhancedPutSQL
```
**Good!** This means the processor loaded successfully.

### Step 5: Verify NAR Structure
Run the verification script:

```powershell
.\verify-deployment.ps1 -NiFiLibPath "C:\path\to\nifi\lib"
```

**Expected Output:**
```
[OK] NAR file found
[OK] Service file found with correct class name
[OK] Processor class compiled
```

### Step 6: Check Processor Search
In NiFi UI:
1. Click "Add Processor" (drag icon)
2. Search for: `EnhancedPutSQL`
3. Also try: `Enhanced` or `PutSQL`
4. Check all categories (not just "SQL")

### Step 7: Verify Extension Manifest
The NAR should contain a valid extension manifest:

```powershell
Add-Type -AssemblyName System.IO.Compression.FileSystem
$nar = [System.IO.Compression.ZipFile]::OpenRead("nifi-enhanced-putsql-nar\target\nifi-enhanced-putsql-nar-2.1.0.nar")
$manifest = $nar.Entries | Where-Object {$_.FullName -eq "META-INF/docs/extension-manifest.xml"}
$stream = $manifest.Open()
$reader = New-Object System.IO.StreamReader($stream)
$content = $reader.ReadToEnd()
# Should contain: <name>com.acme.nifi.processors.EnhancedPutSQL</name>
$reader.Close()
$stream.Close()
$nar.Dispose()
```

### Step 8: Check NiFi Version Compatibility
Verify NiFi version matches:

```powershell
# Check nifi.properties
Get-Content "C:\path\to\nifi\conf\nifi.properties" | Select-String "nifi.version"

# Should show: nifi.version=2.1.0
```

### Step 9: Clear Browser Cache
Sometimes the UI caches processor lists:

1. Hard refresh: `Ctrl + F5`
2. Clear browser cache
3. Try incognito/private mode

### Step 10: Check File Permissions
Ensure NiFi can read the NAR file:

```powershell
# Check permissions
Get-Acl "C:\path\to\nifi\lib\nifi-enhanced-putsql-nar-2.1.0.nar"

# Ensure NiFi user/process has Read access
```

## Common Issues and Solutions

### Issue: "Processor appears in logs but not in UI"
**Solution:**
1. Clear browser cache (Ctrl+F5)
2. Check if processor is in a different category
3. Search with different terms

### Issue: "NAR file exists but processor doesn't load"
**Solution:**
1. Check `nifi.properties` for `nifi.nar.library.directory` setting
2. Verify NAR file isn't corrupted (check file size)
3. Ensure no duplicate NAR files exist

### Issue: "ClassNotFoundException in logs"
**Solution:**
1. Verify NAR is in correct `lib` directory (not `lib/bootstrap`)
2. Check that NiFi version is exactly 2.1.0
3. Ensure all dependencies are available in NiFi runtime

### Issue: "Processor loads but throws errors on use"
**Solution:**
1. Check `nifi-app.log` for specific error messages
2. Verify DBCP Controller Services are configured correctly
3. Check that required properties are set

## Verification Checklist

- [ ] NAR file exists in `lib/` directory
- [ ] No old/duplicate NAR files present
- [ ] NiFi fully restarted (not just reloaded)
- [ ] No errors in `nifi-app.log`
- [ ] Service file present in NAR root
- [ ] Extension manifest contains processor name
- [ ] Processor class compiled successfully
- [ ] NiFi version is 2.1.0
- [ ] Browser cache cleared
- [ ] Searched for processor in UI with multiple terms

## Still Not Working?

If the processor still doesn't appear after following all steps:

1. **Check Bootstrap Log:**
   ```powershell
   Get-Content "logs\nifi-bootstrap.log" -Tail 50
   ```

2. **Check for Multiple NiFi Instances:**
   - Ensure you're checking the correct NiFi instance
   - Verify NAR is in the correct installation

3. **Try Clean NiFi Installation:**
   - Test on a fresh NiFi 2.1.0 installation
   - Copy only the NAR file
   - Restart and check

4. **Verify NAR File Integrity:**
   ```powershell
   # Check file size (should be ~53KB)
   (Get-Item "nifi-enhanced-putsql-nar\target\nifi-enhanced-putsql-nar-2.1.0.nar").Length
   
   # Rebuild if needed
   mvn clean package -DskipTests
   ```

5. **Check Java Version:**
   ```powershell
   java -version
   # Should be Java 11 or higher
   ```

6. **Contact Support:**
   - Share `nifi-app.log` errors
   - Share NAR file verification results
   - Share NiFi version and Java version

## Quick Test Script

Run this to verify everything:

```powershell
# 1. Check NAR exists
$narPath = "C:\path\to\nifi\lib\nifi-enhanced-putsql-nar-2.1.0.nar"
if (Test-Path $narPath) {
    Write-Host "[OK] NAR file exists" -ForegroundColor Green
} else {
    Write-Host "[FAIL] NAR file NOT found" -ForegroundColor Red
    exit 1
}

# 2. Check service file
Add-Type -AssemblyName System.IO.Compression.FileSystem
$nar = [System.IO.Compression.ZipFile]::OpenRead($narPath)
$service = $nar.Entries | Where-Object {$_.FullName -eq "META-INF/services/org.apache.nifi.processor.Processor"}
if ($service) {
    Write-Host "[OK] Service file found" -ForegroundColor Green
} else {
    Write-Host "[FAIL] Service file NOT found" -ForegroundColor Red
}
$nar.Dispose()

# 3. Check for old versions
$oldVersions = Get-ChildItem (Split-Path $narPath) -Filter "*enhanced-putsql*.nar" | 
    Where-Object {$_.Name -ne "nifi-enhanced-putsql-nar-2.1.0.nar"}
if ($oldVersions) {
    Write-Host "[WARN] Old versions found: $($oldVersions.Name)" -ForegroundColor Yellow
} else {
    Write-Host "[OK] No old versions" -ForegroundColor Green
}

Write-Host "`nNext: Fully restart NiFi and check UI" -ForegroundColor Cyan
```

