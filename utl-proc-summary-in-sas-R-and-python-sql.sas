%let pgm=utl-proc-summary-in-sas-R-and-python-sql;

  SAS proc summary in sas R and python sql

  Problem
      a. sum height and weight from sashelp.class by age
      b. sum height and weight from sashelp.class by sex

  Four solutions

        1. SAS proc summary
        2. SAS Sql
        3. R Sql
        4. Python Sql

github
https://tinyurl.com/5y76xszm
https://github.com/rogerjdeangelis/utl-proc-summary-in-sas-R-and-python-sql

Python sql
https://tinyurl.com/yhm2wkbv
https://github.com/rogerjdeangelis?tab=repositories&q=pandasql+in%3Areadme&type=&language=&sort=

R sql
https://tinyurl.com/5n8d36hs
https://github.com/rogerjdeangelis?tab=repositories&q=sqldf+in%3Areadme&type=&language=&sort=

StackOverflow
https://tinyurl.com/5n73pkzd
https://stackoverflow.com/questions/70980753/trouble-with-groupby-loop-in-python-pyspark

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;

libname sd1 "d:/sd1";

data sd1.have;
  set sashelp.class;
run;quit;


/*******************************************************************/
/*                                                                 */
/* Up to 40 obs from SD1.HAVE total obs=19 05MAR2022:14:04:46      */
/*                                                                 */
/* Obs    NAME       SEX    AGE    HEIGHT    WEIGHT                */
/*                                                                 */
/*   1    Alfred      M      14     69.0      112.5                */
/*   2    Alice       F      13     56.5       84.0                */
/*   3    Barbara     F      13     65.3       98.0                */
/*   4    Carol       F      14     62.8      102.5                */
/*   5    Henry       M      14     63.5      102.5                */
/*   6    James       M      12     57.3       83.0                */
/*   7    Jane        F      12     59.8       84.5                */
/*  ....                                                           */
/*                                                                 */
/*******************************************************************/

/*           _               _
  ___  _   _| |_ _ __  _   _| |_
 / _ \| | | | __| `_ \| | | | __|
| (_) | |_| | |_| |_) | |_| | |_
 \___/ \__,_|\__| .__/ \__,_|\__|
                |_|
*/

/*******************************************************************/
/*                                                                 */
/* Up to 40 obs WORK.SMRSEX total obs=3 05MAR2022:14:06:56         */
/*                                                                 */
/* Obs    SEX    HEIGHT    WEIGHT                                  */
/*                                                                 */
/*  1      F      545.3     811.0                                  */
/*  2      M      639.1    1089.5                                  */
/*                                                                 */
/* Up to 40 obs from SMRAGE total obs=7 05MAR2022:14:08:31         */
/*                                                                 */
/* Obs    AGE    HEIGHT    WEIGHT                                  */
/*                                                                 */
/*  1      11     108.8     135.5                                  */
/*  2      12     297.2     472.0                                  */
/*  3      13     184.3     266.0                                  */
/*  4      14     259.6     407.5                                  */
/*  5      15     262.5     469.5                                  */
/*  6      16      72.0     150.0                                  */
/*                                                                 */
/*******************************************************************/

/*         _       _   _
 ___  ___ | |_   _| |_(_) ___  _ __  ___
/ __|/ _ \| | | | | __| |/ _ \| `_ \/ __|
\__ \ (_) | | |_| | |_| | (_) | | | \__ \
|___/\___/|_|\__,_|\__|_|\___/|_| |_|___/
 ___ _   _ _ __ ___  _ __ ___   __ _ _ __ _   _
/ __| | | | `_ ` _ \| `_ ` _ \ / _` | `__| | | |
\__ \ |_| | | | | | | | | | | | (_| | |  | |_| |
|___/\__,_|_| |_| |_|_| |_| |_|\__,_|_|   \__, |
                                          |___/
*/

proc summary data=sd1.class nway;
  class sex;
  var height weight;
  output out=smrSex(drop=_:) sum=;
run;quit;

proc summary data=sd1.class nway;
  class age;
  var height weight;
  output out=smrAge(drop=_:) sum=;
run;quit;

options validvarname=upcase;

libname sd1 "d:/sd1";

data sd1.have;
  set sashelp.class;
run;quit;

/*___                                _
|___ \     ___  __ _ ___   ___  __ _| |
  __) |   / __|/ _` / __| / __|/ _` | |
 / __/ _  \__ \ (_| \__ \ \__ \ (_| | |
|_____(_) |___/\__,_|___/ |___/\__, |_|
                                  |_|
*/

proc sql;
  create
     Table want_sex as
  select
     sex
     ,sum(height) as height
     ,sum(weight) as weight
  from
      sashelp.class
  group
      by sex
  ;
  create
     Table want_age as
  select
     age
     ,sum(height) as height
     ,sum(weight) as weight
  from
      sashelp.class
  group
      by age
;quit;

/*____    ____              _
|___ /   |  _ \   ___  __ _| |
  |_ \   | |_) | / __|/ _` | |
 ___) |  |  _ <  \__ \ (_| | |
|____(_) |_| \_\ |___/\__, |_|
                         |_|
*/

%utlfkil(d:/xpt/want_r.xpt);

%utl_submit_r64("
  library(haven);
  library(SASxport);
  library(sqldf);
  have<-read_sas('d:/sd1/have.sas7bdat');
  have;
  want_sex<-sqldf('
      select
         sex
         ,sum(height) as height
         ,sum(weight) as weight
      from
         have
      group
          by sex
  ');
  want_sex;
  want_age<-sqldf('
      select
         age
         ,sum(height) as height
         ,sum(weight) as weight
      from
          have
      group
          by age
      ');
  want_age;
  write.xport(want_sex, want_age,file='d:/xpt/want_r.xpt');
");

libname xpt xport 'd:/xpt/want_r.xpt';

proc print data=xpt.want_sex;
run;quit;

proc print data=xpt.want_age;
run;quit;

/*  _                  _   _                             _
| || |     _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| || |_   | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
|__   _|  | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
   |_|(_) | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
          |_|    |___/                                |_|
*/

%utlfkil(d:/xpt/want_age.xpt);
%utlfkil(d:/xpt/want_sex.xpt);

%utl_pybegin39;
parmcards4;
from os import path
import pandas as pd
import xport
import xport.v56
import pyreadstat
import numpy as np
import pandas as pd
from pandasql import sqldf
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
have, meta = pyreadstat.read_sas7bdat('d:/sd1/have.sas7bdat')
print(have);
want_sex = pdsql("""
      select
         sex
         ,sum(height) as height
         ,sum(weight) as weight
      from
         have
      group
          by sex
  """);
want_age = pdsql("""
      select
         age
         ,sum(height) as height
         ,sum(weight) as weight
      from
         have
      group
          by age
  """);
print(want_sex);
ds = xport.Dataset(want_age, name='want_age')
with open('d:/xpt/want_age.xpt', 'wb') as f:
    xport.v56.dump(ds, f)
ds = xport.Dataset(want_sex, name='want_sex')
with open('d:/xpt/want_sex.xpt', 'wb') as f:
    xport.v56.dump(ds, f)
;;;;
%utl_pyend39;

libname xpt xport 'd:/xpt/want_sex.xpt';

proc print data=xpt.want_sex;
run;quit;

libname xpt xport 'd:/xpt/want_age.xpt';

proc print data=xpt.want_age;
run;quit;

/*
 _ __ ___   __ _  ___ _ __ ___  ___
| `_ ` _ \ / _` |/ __| `__/ _ \/ __|
| | | | | | (_| | (__| | | (_) \__ \
|_| |_| |_|\__,_|\___|_|  \___/|___/

*/

%macro utl_pybegin39;
%utlfkil(c:/temp/py_pgm.py);
%utlfkil(c:/temp/py_pgm.log);
filename ft15f001 "c:/temp/py_pgm.py";
%mend utl_pybegin39;

%macro utl_pyend39;
run;quit;
* EXECUTE THE PYTHON PROGRAM;
options noxwait noxsync;
filename rut pipe  "d:\Python310\python.exe c:/temp/py_pgm.py 2> c:/temp/py_pgm.log";
run;quit;
data _null_;
  file print;
  infile rut;
  input;
  put _infile_;
  putlog _infile_;
run;quit;
data _null_;
  infile " c:/temp/py_pgm.log";
  input;
  putlog _infile_;
run;quit;
%mend utl_pyend39;

%macro utl_submit_py64_310(
      pgm
     ,return=  /* name for the macro variable from Python */
     )/des="Semi colon separated set of python commands - drop down to python";

  * write the program to a temporary file;
  filename py_pgm "%sysfunc(pathname(work))/py_pgm.py" lrecl=32766 recfm=v;
  data _null_;
    length pgm  $32755 cmd $1024;
    file py_pgm ;
    pgm=&pgm;
    semi=countc(pgm,";");
      do idx=1 to semi;
        cmd=cats(scan(pgm,idx,";"));
        if cmd=:". " then
           cmd=trim(substr(cmd,2));
         put cmd $char384.;
         putlog cmd $char384.;
      end;
  run;quit;
  %let _loc=%sysfunc(pathname(py_pgm));
  %put &_loc;
  filename rut pipe  "d:\Python310\python.exe &_loc";
  data _null_;
    file print;
    infile rut;
    input;
    put _infile_;
  run;
  filename rut clear;
  filename py_pgm clear;

  * use the clipboard to create macro variable;
  %if "&return" ^= "" %then %do;
    filename clp clipbrd ;
    data _null_;
     length txt $200;
     infile clp;
     input;
     putlog "*******  " _infile_;
     call symputx("&return",_infile_,"G");
    run;quit;
  %end;

%mend utl_submit_py64_310;

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
