mysql -uTEST_USER -pXXXXXXXX -DTESTDB < test.sql

DEL next.yml
del data\test000.00.csv
CALL embulk run test_inc.yml -c next.yml

echo "diff data/test_expected.csv data/test000.00.csv"
diff data/test_expected.csv data/test000.00.csv

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-input-mysql inc(1) FAILED!")

mysql -uTEST_USER -pXXXXXXXX -DTESTDB < test_inc.sql
del data\test000.00.csv
CALL embulk run test_inc.yml -c next.yml

echo "diff data/test_inc_expected.csv data/test000.00.csv"
diff data/test_inc_expected.csv data/test000.00.csv

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-input-mysql inc(2) FAILED!")
