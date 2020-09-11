set LOG=%~dp0test-input.log
echo log file = %LOG%
del %LOG%

cd mysql-input
echo "mysql-input/test.bat"
call test.bat >> %LOG%
cd ..

cd oracle-input
echo "oracle-input/test.bat"
call test.bat >> %LOG%
cd ..

cd postgresql-input
echo "postgresql-input/test.bat"
call test.bat >> %LOG%
cd ..

cd sqlserver-input
echo "sqlserver-input/test.bat"
call test.bat >> %LOG%
echo "sqlserver-input/test-jtds.bat"
call test-jtds.bat >> %LOG%
cd ..

cd db2-input
echo "db2-input/test.bat"
call test.bat >> %LOG%
cd ..

grep "FAILED" %LOG%

IF "%ERRORLEVEL%" == "0" (ECHO "FAILED!") ELSE (ECHO "OK!")
