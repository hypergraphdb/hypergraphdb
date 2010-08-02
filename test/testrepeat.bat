rem repeat running a given HG program

set JAVA_HOME=C:\java6_32
set PROGRAM=hgtest.tx.AbruptExit
set HGDB_HOME=d:/classlib/hypergraphdb-1.0
set TESTBIN=C:\Users\bolerio\workspace\hgtest\bin

set JAVA_EXEC="%JAVA_HOME%/bin/java"
%JAVA_EXEC% -version 2>&1 | find "64-Bit" >nul:

if errorlevel 1 (
   REM echo 32-Bit 
   set HGDB_NATIVE=%HGDB_HOME%\native
) else (
   REM  echo 64-Bit
   set HGDB_NATIVE=%HGDB_HOME%\native\amd64
)
REM echo HGDB_NATIVE:  %HGDB_NATIVE%
set PATH=%HGDB_NATIVE%;%PATH%

set THOME=%CD%

cd %HGDB_HOME%
set HG_JARS=
echo set HG_JARS=%%~1;%%HG_JARS%%>append.bat
dir /s/b *.jar > tmpList.txt
FOR /F "usebackq tokens=1* delims=" %%i IN (tmpList.txt) do (call append.bat "%%i")
del append.bat
del tmpList.txt

cd %THOME%

set CLASSPATH=%HG_JARS%;%TESTBIN%
echo CLASSPATH is %CLASSPATH%

:START
%JAVA_EXEC% -cp %CLASSPATH% -Djava.library.path=%HGDB_NATIVE%  %PROGRAM%
GOTO START
