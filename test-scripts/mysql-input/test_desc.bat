mysql -uTEST_USER -pXXXXXXXX -DTESTDB < test.sql

del data\test000.00.csv
CALL embulk run test_desc.yml

echo "diff data/test_desc_expected.csv data/test000.00.csv"
diff data/test_desc_expected.csv data/test000.00.csv

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-input-mysql desc FAILED!")
