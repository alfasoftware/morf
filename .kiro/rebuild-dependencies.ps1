# Quick script to rebuild and install morf-core and morf-testsupport dependencies
# Use this when Maven jar packaging fails due to Windows file permission issues

Write-Host "Building morf-core and morf-testsupport..." -ForegroundColor Cyan

# Create jars from compiled classes
Write-Host "`nCreating jars from compiled classes..." -ForegroundColor Yellow
jar -cf morf-core/target/morf-core-2.28.1-SNAPSHOT.jar -C morf-core/target/classes .
jar -cf morf-testsupport/target/morf-testsupport-2.28.1-SNAPSHOT.jar -C morf-testsupport/target/classes .

# Create Maven repository directories
Write-Host "`nCreating Maven repository directories..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "D:\.m2\repository\org\alfasoftware\morf-core\2.28.1-SNAPSHOT" | Out-Null
New-Item -ItemType Directory -Force -Path "D:\.m2\repository\org\alfasoftware\morf-testsupport\2.28.1-SNAPSHOT" | Out-Null

# Copy jars to Maven repository
Write-Host "`nCopying jars to Maven repository..." -ForegroundColor Yellow
Copy-Item morf-core/target/morf-core-2.28.1-SNAPSHOT.jar D:\.m2\repository\org\alfasoftware\morf-core\2.28.1-SNAPSHOT\morf-core-2.28.1-SNAPSHOT.jar
Copy-Item morf-testsupport/target/morf-testsupport-2.28.1-SNAPSHOT.jar D:\.m2\repository\org\alfasoftware\morf-testsupport\2.28.1-SNAPSHOT\morf-testsupport-2.28.1-SNAPSHOT.jar

# Copy poms to Maven repository
Write-Host "`nCopying poms to Maven repository..." -ForegroundColor Yellow
Copy-Item morf-core/pom.xml D:\.m2\repository\org\alfasoftware\morf-core\2.28.1-SNAPSHOT\morf-core-2.28.1-SNAPSHOT.pom
Copy-Item morf-testsupport/pom.xml D:\.m2\repository\org\alfasoftware\morf-testsupport\2.28.1-SNAPSHOT\morf-testsupport-2.28.1-SNAPSHOT.pom

Write-Host "`nDependencies installed successfully!" -ForegroundColor Green
Write-Host "You can now run tests from morf-postgresql module" -ForegroundColor Green
