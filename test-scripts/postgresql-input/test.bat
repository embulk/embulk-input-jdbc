SETLOCAL
SET PGPASSWORD=XXXXXXXX
"C:\Program Files\PostgreSQL\9.4\bin\psql.exe" -d testdb -U test_user -w -f test.sql
ENDLOCAL

del data\test000.00.csv
CALL embulk run test.yml

echo "diff data/test_expected.csv data/test000.00.csv"
diff data/test_expected.csv data/test000.00.csv

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-input-postgresql FAILED!")
