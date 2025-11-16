# EnhancedPutSQL Processor Not Showing in NiFi UI - Troubleshooting Guide

## Quick Checklist

### 1. Verify NAR File Location
- **Windows**: `C:\nifi\lib\` or `C:\nifi\lib\bootstrap\`
- **Linux**: `/opt/nifi/nifi-current/lib/` or `/opt/nifi/nifi-current/lib/bootstrap/`
- **Check**: The NAR file should be: `nifi-enhanced-putsql-nar-2.1.0.nar`
- **Action**: Ensure the file is in the correct `lib` directory (not `lib/bootstrap` unless configured)

### 2. Remove Old/Conflicting NAR Files
```powershell
# Windows - Check for old versions
Get-ChildItem "C:\path\to\nifi\lib\" -Filter "*enhanced-putsql*" -Recurse

# Remove any old versions
Remove-Item "C:\path\to\nifi\lib\*enhanced-putsql*.nar" -Force
```

### 3. Full NiFi Restart (Critical!)
**DO NOT** just reload - you must do a FULL restart:
1. **Stop NiFi completely**
   ```powershell
   # Windows
   Stop-Service nifi
   # OR if running manually
   # Press Ctrl+C and wait for complete shutdown
   ```
2. **Wait 30 seconds** for all processes to terminate
3. **Start NiFi**
   ```powershell
   # Windows
   Start-Service nifi
   # OR if running manually
   .\bin\run-nifi.bat
   ```
4. **Wait for NiFi to fully start** (check logs for "NiFi started successfully")

### 4. Check NiFi Logs for Errors
Look in `logs/nifi-app.log` for:
- `ClassNotFoundException`
- `NoClassDefFoundError`
- `com.acme.nifi.processors.EnhancedPutSQL`
- Any errors mentioning "EnhancedPutSQL" or "enhanced-putsql"

**Common Errors:**
```
ERROR o.a.n.p.StandardProcessorNode: Failed to instantiate processor
ERROR o.a.n.p.StandardProcessorNode: Unable to load processor class
```

### 5. Verify NAR File Contents
The NAR should contain:
- `META-INF/services/org.apache.nifi.processor.Processor` with content: `com.acme.nifi.processors.EnhancedPutSQL`
- `META-INF/bundled-dependencies/nifi-enhanced-putsql-processors-2.1.0.jar`
- `META-INF/docs/extension-manifest.xml` with processor listed

### 6. Check Processor Search in UI
1. Open NiFi UI
2. Click "Add Processor" (drag icon)
3. **Search for**: `EnhancedPutSQL` or `Enhanced` or `PutSQL`
4. Check if it appears in any category

### 7. Verify NiFi Version Compatibility
- **Required**: NiFi 2.1.0
- **Check**: `nifi.properties` should show `nifi.version=2.1.0`
- **Verify**: NAR was built with NiFi 2.1.0 dependencies

### 8. Check NAR Dependencies
The NAR requires these NiFi dependencies (should be provided by NiFi runtime):
- `nifi-api` (2.1.0)
- `nifi-dbcp-service-api` (2.1.0)
- `nifi-standard-processors` (2.1.0)

### 9. Verify Processor Class Can Be Loaded
Check if the processor class is accessible:
```powershell
# Extract and check the JAR
Add-Type -AssemblyName System.IO.Compression.FileSystem
$jar = [System.IO.Compression.ZipFile]::OpenRead("nifi-enhanced-putsql-nar\target\nifi-enhanced-putsql-nar-2.1.0.nar")
$jarEntry = $jar.Entries | Where-Object {$_.Name -like "*.jar"}
# Should find: nifi-enhanced-putsql-processors-2.1.0.jar
```

### 10. Rebuild and Redeploy
If all else fails:
```powershell
# Clean rebuild
mvn clean package -DskipTests

# Verify new NAR
Get-Item "nifi-enhanced-putsql-nar\target\nifi-enhanced-putsql-nar-2.1.0.nar"

# Copy to NiFi
Copy-Item "nifi-enhanced-putsql-nar\target\nifi-enhanced-putsql-nar-2.1.0.nar" "C:\path\to\nifi\lib\" -Force

# Full restart NiFi
```

## Common Issues and Solutions

### Issue: Processor appears in logs but not in UI
**Solution**: Clear browser cache and hard refresh (Ctrl+F5)

### Issue: "ClassNotFoundException" in logs
**Solution**: 
- Verify NAR is in correct `lib` directory
- Check that NiFi version matches (2.1.0)
- Ensure all dependencies are available

### Issue: "NoClassDefFoundError" in logs
**Solution**: 
- This is expected for DBCPService - the code handles this
- If it's for another class, check dependency scopes in pom.xml

### Issue: NAR file exists but processor doesn't load
**Solution**:
1. Check `nifi.properties` for `nifi.nar.library.directory` setting
2. Verify NAR file permissions (should be readable)
3. Check for multiple NAR files with same processor (remove duplicates)

### Issue: Processor loads but throws errors
**Solution**: Check `nifi-app.log` for specific error messages

## Verification Commands

### Check NAR Service File
```powershell
Add-Type -AssemblyName System.IO.Compression.FileSystem
$nar = [System.IO.Compression.ZipFile]::OpenRead("nifi-enhanced-putsql-nar\target\nifi-enhanced-putsql-nar-2.1.0.nar")
$service = $nar.Entries | Where-Object {$_.FullName -eq "META-INF/services/org.apache.nifi.processor.Processor"}
$stream = $service.Open()
$reader = New-Object System.IO.StreamReader($stream)
$reader.ReadToEnd()
$reader.Close()
$stream.Close()
$nar.Dispose()
```

**Expected Output:**
```
com.acme.nifi.processors.EnhancedPutSQL
```

## Still Not Working?

1. **Check NiFi Bootstrap Log**: `logs/nifi-bootstrap.log`
2. **Check NiFi App Log**: `logs/nifi-app.log` (look for startup errors)
3. **Verify NAR File Hash**: Ensure file wasn't corrupted during copy
4. **Try Different NiFi Instance**: Test on a clean NiFi installation
5. **Check Java Version**: NiFi 2.1.0 requires Java 11+

## Contact Information

If the processor still doesn't appear after following all steps:
- Check NiFi logs for specific error messages
- Verify NAR file integrity
- Ensure NiFi version is exactly 2.1.0


