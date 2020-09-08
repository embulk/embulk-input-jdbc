osql -d TESTDB -U TEST_USER -P XXXXXXXX -i test.sql

del data\test000.00.csv
CALL embulk run test-jtds.yml

echo "diff data/test_expected.csv data/test000.00.csv"
diff data/test_expected.csv data/test000.00.csv

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-input-sqlserver FAILED!")
