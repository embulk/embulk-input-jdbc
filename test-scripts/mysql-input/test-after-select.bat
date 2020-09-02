mysql -uTEST_USER -pXXXXXXXX -DTESTDB < test.sql

CALL embulk run test-after-select.yml

echo "diff data/test_expected.csv data/test000.00.csv"
diff data/test_expected.csv data/test000.00.csv

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-input-mysql (after-select) FAILED!")

mysql -uTEST_USER -pXXXXXXXX -DTESTDB -e"SELECT * FROM EMBULK_INPUT" > data/temp.txt

echo "diff data/test-after-select_expected.txt data/temp.txt"
diff data/test-after-select_expected.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-input-mysql (after-select) FAILED!")
