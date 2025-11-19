# Build Notes for Morf Project

## Known Issues

### Maven JAR Packaging Error on Windows
**Issue**: Maven fails to package jars with error: "Incorrect function" when trying to access file permissions
```
Failed to execute goal org.apache.maven.plugins:maven-jar-plugin:3.3.0:jar (default-jar) on project morf-core: 
Error assembling JAR: Failed to determine inclusion status for: Z:\...\pom.xml: Incorrect function
```

**Root Cause**: Windows file system issue with `WindowsAclFileAttributeView.getOwner()` - likely related to file permissions or network drives.

**Workaround for Testing**:
1. Compile classes without packaging: `mvn compile test-compile -DskipTests`
2. Manually create jars from compiled classes:
   ```powershell
   jar -cf morf-core/target/morf-core-2.28.1-SNAPSHOT.jar -C morf-core/target/classes .
   jar -cf morf-testsupport/target/morf-testsupport-2.28.1-SNAPSHOT.jar -C morf-testsupport/target/classes .
   ```
3. Copy to local Maven repository:
   ```powershell
   # Create directories
   New-Item -ItemType Directory -Force -Path "D:\.m2\repository\org\alfasoftware\morf-core\2.28.1-SNAPSHOT"
   New-Item -ItemType Directory -Force -Path "D:\.m2\repository\org\alfasoftware\morf-testsupport\2.28.1-SNAPSHOT"
   
   # Copy jars
   Copy-Item morf-core/target/morf-core-2.28.1-SNAPSHOT.jar D:\.m2\repository\org\alfasoftware\morf-core\2.28.1-SNAPSHOT\
   Copy-Item morf-testsupport/target/morf-testsupport-2.28.1-SNAPSHOT.jar D:\.m2\repository\org\alfasoftware\morf-testsupport\2.28.1-SNAPSHOT\
   
   # Copy poms
   Copy-Item morf-core/pom.xml D:\.m2\repository\org\alfasoftware\morf-core\2.28.1-SNAPSHOT\morf-core-2.28.1-SNAPSHOT.pom
   Copy-Item morf-testsupport/pom.xml D:\.m2\repository\org\alfasoftware\morf-testsupport\2.28.1-SNAPSHOT\morf-testsupport-2.28.1-SNAPSHOT.pom
   ```
4. Run tests from morf-postgresql: `mvn test -Dtest=TestPostgreSQLDialect#testMethodName*`

## Quick Test Workflow

For testing changes in morf-postgresql module:

1. **If dependencies are already in local Maven repo** (D:\.m2\repository\org\alfasoftware\):
   ```powershell
   cd morf-postgresql
   mvn test -Dtest=TestClassName#testMethodName*
   ```

2. **If dependencies need to be rebuilt**:
   - Use the workaround above to manually install morf-core and morf-testsupport
   - Then run tests from morf-postgresql

3. **Verify compilation only** (fastest):
   ```powershell
   mvn compiler:compile compiler:testCompile -DskipTests
   ```

## Module Dependencies
- morf-postgresql depends on: morf-core, morf-testsupport
- morf-testsupport depends on: morf-core
- Build order: morf-core → morf-testsupport → morf-postgresql

## Test Execution
- Tests can run from compiled classes even if jar packaging fails
- Use `-Dtest=ClassName#methodName*` for specific test methods
- Wildcard `*` works for running multiple tests with same prefix
