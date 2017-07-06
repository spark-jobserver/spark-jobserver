@IF NOT DEFINED SPARK_HOME (
 ECHO SPARK_HOME MUST BE SET
 EXIT /B -1
)

@IF NOT DEFINED HADOOP_HOME (
 ECHO HADOOP_HOME MUST BE SET
 EXIT /B -1
)

mkdir C:\tmp\hive
%HADOOP_HOME%\bin\winutils chmod -R 777 \tmp\hive

setlocal
cd %~dp0

set PYTHONPATH=%~dp0;%SPARK_HOME%\python\lib\pyspark.zip;%SPARK_HOME%\python\lib\py4j-0.9-src.zip;%PYTHONPATH%
python test\apitests.py
set exitCode=%ERRORLEVEL%
REM This sleep is here so that all of Spark's shutdown stdout if written before we exit,
REM so that we return cleanly to the command prompt.
timeout /t 2 /nobreak > nil
endlocal
exit /B %exitCode%
