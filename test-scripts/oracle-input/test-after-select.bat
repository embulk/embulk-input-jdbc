sqlplus TEST_USER/XXXXXXXX@localhost/TESTDB @test.sql

del data\test000.00.csv
CALL embulk run test-after-select.yml

echo "diff data/test_expected.csv data/test000.00.csv"
diff data/test_expected.csv data/test000.00.csv

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-input-oracle (after-select) FAILED!")

sqlplus TEST_USER/XXXXXXXX@localhost/TESTDB @select.sql

echo "diff data/test-after-select_expected.txt data/temp.txt"
diff data/test-after-select_expected.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" ECHO "OK!"

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-input-oracle (after-select) FAILED!")
