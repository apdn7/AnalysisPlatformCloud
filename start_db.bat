REM Put me in the extracted postgres root folder(which contains
REM a "bin" folder), and run it.

REM The script sets environment variables helpful for PostgreSQL
@SET POSTGRES_HOME=..\database\pgsql
@SET PATH="%POSTGRES_HOME%\bin";%PATH%
@SET PGDATA=%POSTGRES_HOME%\data
@SET PGDATABASE=postgres
@SET PGUSER=postgres
@SET PGLOCALEDIR=%POSTGRES_HOME%\share\locale

ECHO.
IF EXIST %PGDATA% (
ECHO This instance already initialized.
ECHO.
) ELSE (
ECHO First run, wait for initializing.
"%POSTGRES_HOME%\bin\initdb" -U postgres -A trust
ECHO.
)
"%POSTGRES_HOME%\bin\pg_ctl" -D "%PGDATA%" -l logfile start
ECHO.
ECHO postgres listening on port %PGPORT%

