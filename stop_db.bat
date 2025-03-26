@SET POSTGRES_HOME=..\database\pgsql
@SET PATH="%POSTGRES_HOME%\bin";%PATH%
@SET PGDATA=%POSTGRES_HOME%\data

@ECHO OFF
"%POSTGRES_HOME%\bin\pg_ctl" -D "%PGDATA%" stop -m immediate
ECHO Database stopped !!!
EXIT /b
