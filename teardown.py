#!python3.7.2
# -*- coding: utf-8 -*-

"""
   reduce CLI
   Author : Alex B
   Status : Development
   # export AWS_DEFAULT_PROFILE=dev
"""

import os, sys
import json
from time import sleep
sys.path.append("../../../..")  # hbo

from hbo.dq.cli.include.common import SUCCESS, e, pprint, YAML_CONFIG, CFG_FILE
from hbo.dq.cli.__init__ import __version__
import os, sys
from hbo.dq.cli.include.utils import init
import builtins
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module='psycopg2')
home = os.path.dirname(sys.argv[0])
if not home:
    home = os.path.dirname(os.path.abspath(__file__))



app_name = os.path.basename(os.path.splitext(__file__)[0])
logger, job_name, ts, jid, *_ = init(home, app_name)
builtins.app_init = (home, app_name, logger)

from hbo.dq.cli.include.dq_cli import get_cli, DqCliQuitError
from hbo.dq.cli.include.dq_metadb import DqMetadb
from hbo.dq.cli.include.common import SUCCESS, e
from hbo.dq.cli.include.tools import  format_output, OutputSettings

if YAML_CONFIG:
    from hbo.dq.cli.include.config import YamlConfig as Config
else:
    from hbo.dq.cli.include.config import PyConfig as Config

from hbo.dq.cli.include.common import environments, exec_type, SYNC_EXEC, ASYNC_EXEC




from hbo.dq.cli.include.athena_execute      import AthenaExecute
from hbo.dq.cli.include.postgres_execute    import PostgresExecute
from hbo.dq.cli.include.redshift_execute    import RedshiftExecute
from hbo.dq.cli.include.snowflake_execute   import SnowflakeExecute

from hbo.dq.cli.include.common import executors, EXEC_ATHENA, EXEC_REDSHIFT, EXEC_POSTGRES, EXEC_SNOWFLAKE

def get_executor(name,compute,conf):
    if   name == executors[EXEC_ATHENA]:    return AthenaExecute    (compute=compute, conf=conf)
    elif name == executors[EXEC_REDSHIFT]:  return RedshiftExecute  (compute=compute, conf=conf)
    elif name == executors[EXEC_POSTGRES]:  return PostgresExecute  (compute=compute, conf=conf)
    elif name == executors[EXEC_SNOWFLAKE]: return SnowflakeExecute (compute=compute, conf=conf)
    else:
        logger.error(f'Unknown executor "{name}"')

from datetime import datetime

n = datetime.now()
t = n.timetuple()

current_year, *_ = t

s3_staging_dir=None
import yaml
import sqlparse
import click

click.disable_unicode_literals_warning = True


@click.command()
@click.option('-r', '--rule_id', default=None, help='Rule Id to submit.')
@click.option('-cf', '--config_file', help='YAML config file.')

def cli(**kwargs):
    global home, app_name
    global s3_staging_dir

    config_file = kwargs.get('config_file', None)

    rule_id = kwargs.get('rule_id', None)

    assert config_file, 'Please, provide config file <ENV>.yml'
    runtime = os.path.basename(config_file).split('.')[0]
    assert runtime in environments, f'Runtime must be one of {", ".join(environments)}.'
    if 1:
        assert config_file
        config_path = os.path.join(home, config_file)
        assert os.path.isfile(config_path), 'Cannot find config file\n%s' % config_path
        config = Config(config_path=config_path, env=runtime, job_id=jid, logger=logger)
    if 0:
        uc=None
        uc_file='teardown.yml'

        with open(uc_file, 'r') as yfh:
            data=yfh.read()
            uc= yaml.load(data)
            #pprint(y['usecase'])
        assert uc
        sql=sqlparse.parse(uc['usecase']['left'].strip())
        pprint( sql)
        e(0)
    if 0:
        import sqlite3
        localdb='teardown.db'
        if os.path.isfile(localdb): os.remove(localdb)
        conn = sqlite3.connect(localdb)
        local_insert(conn)
        local_select(conn)
        conn.close()
    if 1:

        at_meta = config.get_meta_for_compute('athena')
        #pprint(at_meta)

        submit_athena_async(num_years=num_years, year_queue=at_queue, db_meta=at_meta, usecase=None)
        out=[]
        for k in sorted(at_queue.keys()):
            v=at_queue[k]
            at_json, err, status = v
            j = json.loads(at_json)
            ex_id = j.get('QueryExecutionId')
            out.append([k,default_month,ex_id, err, status])


        headers = ['Year' if not default_year else f'Year({default_year})','Month' if not default_month else f'Month({default_month})','Query ID', 'Error','Status']


        #print()
        print(get_formatted(None, out, headers))

    if 1:
        sn_meta = config.get_meta_for_compute('snowflake')
        #(sn_meta)
        #e(0)
        sn_queue = {}
        ex_snow_async(num_years=num_years, year_queue=sn_queue, db_meta=sn_meta)
        out = []
        for k in sorted(sn_queue.keys()):
            v=sn_queue[k]
            at_json, err, status = v
            out.append([k,default_month,len(at_json), err, status])
        headers = ['Year' if not default_year else f'Year({default_year})','Month' if not default_month else f'Month({default_month})','JSON length', 'Error','Status']


        print(get_formatted(None, out, headers))
        sn_data={}

        sn_cleanup(sn_queue,sn_data)
    if 1:
        import sqlite3
        #if
        localdb='teardown.db'
        if os.path.isfile(localdb): os.remove(localdb)
        conn = sqlite3.connect(localdb)
        local_dump(conn,sn_data,'left')
    #e(0)
    if 1:
        assert len(at_queue.keys()) == num_years
        #at_queue[current_year]='{\n    "QueryExecutionId": "61f60e26-5635-4467-a2de-eb99cc1a5e01"\n}\n'

        s3_queue={}
        at_timeout = 3

        at_get_q_results(at_queue, year_queue=s3_queue, timeout=at_timeout)


        out=[]
        for k in sorted(s3_queue.keys()):
            v=s3_queue[k]
            at_json, err, status = v
            out.append([k,default_month,len(at_json), err, status])

        headers = ['Year' if not default_year else f'Year({default_year})','Month' if not default_month else f'Month({default_month})','JSON length', 'Error','Status']


        print(get_formatted(None, out, headers))


        at_data={}
        at_cleanup(s3_queue, at_data)
    if 1:
        import sqlite3
        #localdb='teardown.db'
        #if os.path.isfile(localdb): os.remove(localdb)
        #conn = sqlite3.connect(localdb)
        local_dump(conn,at_data,'right')
    if 1:
        out=[]
        local_teardown(conn, out)
        #e(0)
        headers = ['Year' if not default_year else f'Year({default_year})',
                   'Month' if not default_month else f'Month({default_month})', 'Date', 'Athena', 'Snowflake', 'Diff']
        print(get_formatted('\nAthena v.s. Snowflake "by date" count difference report.', out, headers))
    if 0:

        out=[]

        reduce([at_data,sn_data], out)

        headers = ['Year' if not default_year else f'Year({default_year})','Month' if not default_month else f'Month({default_month})','Date', 'Athena', 'Snowflake', 'Diff']
        print(get_formatted('\nAthena v.s. Snowflake "by date" count difference report.' ,out,headers))
    if 0:
        assert runtime
        store = config.get_metadb()

        metadb = DqMetadb(compute=store, conf=config)
        #Snowflake
        if 0:
            ex_athena()
        #Athena

        if 1:
            ex_snowflake(config)

def local_teardown(conn, out):
    sql='SELECT a.year, a.dt, a.cnt, b.cnt, a.cnt-b.cnt diff from left a, right b where a.year=b.year and a.dt=b.dt '
    cur = conn.cursor()
    for row in cur.execute(sql):
        #print(float(row[4]) )
        if float(row[4]) !=0:
            out.append([row[0], row[1].split('-')[1],row[1], comma_me(str(row[2])), comma_me(str(row[3])), comma_me(str(row[4]))])

def local_dump(conn,data, table):

    ddl=f'CREATE TABLE {table} (year int, dt text, cnt real)'
    try:
        local_create_table(conn,ddl)
    except:
        raise
        pass
    out=[]
    for year, val in data.items():
        for dd, cnt in val.items():
            out.append([year,  dd, cnt])

    local_insert(conn, table, out)





def local_insert(conn,table, data):
    cur = conn.cursor()
    cur.executemany(f'INSERT INTO {table} VALUES (?,?,?)', data)
    conn.commit()
def local_select(conn, table):
    cur = conn.cursor()
    sql=f'SELECT * FROM {table} ORDER BY 1,2,3'
    for row in cur.execute(sql):
        print(row)
def local_create_table(conn, ddl):
    cur = conn.cursor()
    cur.execute(ddl)
    return conn



def ex_snowflake_sync(config):
    formatted=True
    expanded=False
    exename = executors[EXEC_SNOWFLAKE]
    compute = config.get_meta_for_compute(exename)
    exec = get_executor(exename, compute, config)

    # q='select dt, count(1) cnt from (SELECT dt FROM dcm.appboy_users_daily_push LIMIT 1) t group by 1 order by 1'
    # q = 'select distinct dt FROM dcm.appboy_users_daily_push'
    q = """
    SELECT * FROM (
    SELECT '2019-03-11' dt, 11 cnt 
    UNION ALL 
    SELECT '2019-03-10' dt, 99 cnt
    UNION ALL 
    SELECT '2019-03-09' dt, 42 cnt
    ) u ORDER BY 1
    """
    q = 'select dt, count(1) from marketing_dev.publish.user_stream_detail WHERE yr=\'2018\' and mo=\'12\' group by 1 order by 1'
    q = 'select dt, count(1) from marketing_dev.publish.user_stream_detail WHERE yr=\'2018\'  group by 1 order by 1'
    # q='select * from TABLE_STORAGE_METRICS'
    if formatted:
        out = exec.run_formatted(q, expanded=expanded)
        for s in out:
            print(s)
    else:
        _, cur, *_ = exec.execute(q)
        for s in cur.fetchall():
            print(s)

def ex_athena():
    # --result-configuration "OutputLocation=s3://aws-athena-query-results-613630599026-us-east-2/alexb/"

    exename = executors[EXEC_ATHENA]
    compute = config.get_meta_for_compute(exename)
    exec = get_executor(exename, compute, config)

    q = 'select dt, count(1) from (SELECT dt FROM marketing_prod.publish.user_stream_detail LIMIT 1) t group by 1'
    q = 'select dt, count(1) cnt from (SELECT dt FROM dcm.appboy_users_daily_push LIMIT 1) t group by 1 order by 1'
    # q = 'select distinct dt FROM dcm.appboy_users_daily_push'
    q = """
                    SELECT * FROM (
                    SELECT '2019-03-11' dt, 11 cnt 
                    UNION ALL 
                    SELECT '2019-03-10' dt, 99 cnt
                    UNION ALL 
                    SELECT '2019-03-09' dt, 42 cnt
                    ) u ORDER BY 1
                    """
    q = 'select "$path" from dcm.appboy_users_daily_push'
    q = """
                    SELECT dt, count(*) FROM dcm.appboy_users_daily_push where (yr='2017' and mo='12' --and dt='2017-12-14' 
                    ) GROUP BY dt
                    UNION ALL
                    SELECT dt, count(*) FROM dcm.appboy_users_daily_push where (yr='2018' and mo='02' --and dt='2017-12-14'        
                    ) GROUP BY dt"""
    q = """
                    SELECT dt, count(*) FROM dcm.appboy_users_daily_push where (yr='2017' and mo='12' --and dt in (dt) 
                    ) GROUP BY dt
                    """
    q = """
                SELECT dt, count(*) FROM dcm.appboy_users_daily_push WHERE (yr='2017' and mo='12') GROUP BY dt
                """
    q = """
                DESCRIBE dcm.appboy_users_daily_push
                """
    if formatted:
        out = exec.run_formatted(q, expanded=expanded)
        for s in out:
            print(s)
    else:
        _, cur, *_ = exec.execute(q)
        for s in cur.fetchall():
            print(s)
    #


from time import sleep
from threading import Thread, Lock
from subprocess import Popen, PIPE

delay = 1




class DatabaseWorker(Thread):
    __lock = Lock()

    def __init__(self,  cmd, year,result_queue):
        Thread.__init__(self)
        #self.db = db
        self.cmd = cmd
        self.year=year
        self.result_queue = result_queue

    def run(self):
        result = []
        err=None
        retcode=None
        #logger.info("Connecting to database...")
        try:

            p = Popen([self.cmd], stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
            result = p.stdout.read().decode('utf-8')
            #pprint (result)
            retcode = p.returncode
            if 0:
                while line:
                    result.append(line)
                    line = p.stdout.readline().decode('utf-8')
                    #print(line)
            if 1:

                err = p.stderr.read().decode('utf-8')
                #print (err)

            #print(p.returncode)
        except Exception as e:
            logger.error("Unable to access database %s" % str(e))
        self.result_queue[self.year]=[result,err, retcode]
ON_POSIX = 'posix' in sys.builtin_module_names

EXIT_ON_ERROR=True

my_env = os.environ.copy()

env_vars = ["SNOWSQL_ACCOUNT", "SNOWSQL_USER", "SNOWSQL_DATABASE", "SNOWSQL_ROLE", "SNOWSQL_SCHEMA",
            "SNOWSQL_WAREHOUSE", "SNOWSQL_PWD"]
if 0:
    for v in env_vars:
        my_env[v] = os.getenv(v.upper(), None)
        assert my_env[v], f"export {v}='{v.lower()}'"

class SnowDatabaseWorker(Thread):
    __lock = Lock()

    def __init__(self,  cmd, year, result_queue, db_meta):
        Thread.__init__(self)
        #self.db = db
        self.cmd = cmd
        self.result_queue = result_queue
        self.year=year
        self.db_meta=db_meta

        my_env["SNOWSQL_ACCOUNT"]   = db_meta['account_name']
        my_env["SNOWSQL_USER"]      = 'marketing.etl.dev@hbo.com'
        my_env["SNOWSQL_DATABASE"]  = db_meta['database_name']
        my_env["SNOWSQL_ROLE"]      = db_meta['role_name']
        my_env["SNOWSQL_SCHEMA"]    = db_meta['schema_name']
        my_env["SNOWSQL_WAREHOUSE"] = db_meta['warehouse_name']
        my_env["SNOWSQL_PWD"] = os.getenv("SNOWSQL_PWD")
        self.my_env = my_env
    def run(self):
        result = []
        err=None
        retcode=None
        #logger.info("Connecting to database...")
        try:

            p = Popen(self.cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, env=self.my_env)
            result = p.stdout.read().decode('utf-8')
            #pprint (result)
            retcode = p.returncode
            if 0:
                while line:
                    result.append(line)
                    line = p.stdout.readline().decode('utf-8')
                    #print(line)
            if 1:

                err = p.stderr.read().decode('utf-8')
                #print (err)


        except Exception as e:
            logger.error("Unable to access database %s" % str(e))
        self.result_queue[self.year]=[result,err, retcode]

def warn(msg):
    print ('-'*60)
    print(msg)
    print('-' * 60)
class AthenaQueryResultsWorker(Thread):
    __lock = Lock()

    def __init__(self, cmd, year, result_queue, timeout=5):
        Thread.__init__(self)
        # self.db = db
        self.cmd = cmd
        self.year = year
        self.result_queue = result_queue
        self.timeout=timeout

    def run(self):
        import time
        start_time = time.time()
        result = []
        err=None
        retcode=None
        #logger.info("Downloading file...")
        i=1
        time.sleep(self.timeout)
        while not result:
            try:

                p = Popen(self.cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
                result = p.stdout.read().decode('utf-8')
                # pprint (result)
                retcode = p.returncode
                if 0:
                    while line:
                        result.append(line)
                        line = p.stdout.readline().decode('utf-8')
                        # print(line)
                if 1:
                    err = p.stderr.read().decode('utf-8')
                    if 'Current state:' in err:
                        state= err.strip().split('Current state:')[1].strip

                        if state in ('RUNNING','QUEUED'):
                            elapsed_time = time.time() - start_time
                            print(f'Athena:{self.year}:{i}:{time.strftime("%M%S", time.gmtime(elapsed_time))}:{state}')

                            time.sleep(self.timeout)
                            i +=1
                            err=state
                    elif err:
                        result= ['ERROR',err]



            except Exception as e:
                logger.error("Unable to access S3 %s" % str(e))
                err=e
                result=[]
        self.result_queue[self.year] = [result,err, retcode]


def submit_athena_async(num_years, year_queue, db_meta, usecase):
    global default_year
    workers = []
    plen=40
    delay = 0.3
    ptitle=' Athena submit:'
    print (db_meta['s3_staging_dir'])
    printProgressBar(0, num_years, prefix=ptitle, suffix='Complete', length=plen)

    mo = f'\'{default_month}\'' if default_month else 'mo'

    if default_year:
        q = f"Select dt, count(1) cnt from dcm.user_stream_detail WHERE (yr='{default_year}' and mo={mo}) GROUP BY 1"
        # q = f"Select dt, count(1) cnt from dcm.user_stream_detail WHERE (yr='{2018 - wid}') GROUP BY 1"
        cmd = f"""aws athena start-query-execution --query-string "{q}" --region '{db_meta['region_name']}' --result-configuration "OutputLocation={db_meta['s3_staging_dir']}" """
        worker = DatabaseWorker(cmd=cmd, year=default_year, result_queue=year_queue)
        workers.append(worker)
        worker.start()
    else:
        for wid in range(num_years):
            q = f"Select dt, count(1) cnt from dcm.user_stream_detail WHERE (yr='{current_year - wid}' and mo={mo}) GROUP BY 1"
            #q = f"Select dt, count(1) cnt from dcm.user_stream_detail WHERE (yr='{2018 - wid}') GROUP BY 1"
            #cmd = f"""aws athena start-query-execution --query-string "{q}" --region '{db_meta['region_name']}' --result-configuration "{db_meta['s3_staging_dir']}" """
            cmd = f"""aws athena start-query-execution --query-string "{q}" --region '{db_meta['region_name']}' --result-configuration "OutputLocation={db_meta['s3_staging_dir']}" """
            worker = DatabaseWorker(cmd=cmd, year=current_year-wid, result_queue=year_queue)
            workers.append(worker)
            worker.start()

    # Wait for the job to be done
    i=0

    while len(year_queue) < num_years:
        if i<num_years:
            printProgressBar(i, num_years, prefix=ptitle, suffix='Complete', length=plen)
        sleep(delay)
        i+=1

    #printProgressBar(i, num_years, prefix=ptitle, suffix='Complete', length=plen)
    if 1:
        for wid in range(num_years):
            workers[wid].join()

    printProgressBar(num_years, num_years, prefix=ptitle, suffix='Complete', length=plen)


EXIT_ON_ERROR=True

def ex_snow_async(num_years, year_queue, db_meta):
    global default_year
    plen=40
    delay = 2
    ptitle=' Snowflake execute:'
    printProgressBar(0, num_years, prefix=ptitle, suffix='Complete', length=plen)
    workers = []
    mo =f'\'{default_month}\'' if default_month else 'mo'
    if default_year:
        cmd = ['/Applications/SnowSQL.app/Contents/MacOS/snowsql',
           '-o', f'exit_on_error={EXIT_ON_ERROR}',
           '-o', 'friendly=False',
           '-o', 'output_format=plain',
           '-o', 'header=False',
           '-o', 'log_level=CRITICAL',
           '-q',
           f'select dt, count(1) from marketing_dev.publish.user_stream_detail WHERE yr=\'{default_year}\' and mo={mo} group by 1 order by 1']

        worker = SnowDatabaseWorker(cmd=cmd, year=default_year, result_queue=year_queue, db_meta=db_meta)
        workers.append(worker)
        worker.start()
    else:

        for wid in range(num_years):
            cmd = ['/Applications/SnowSQL.app/Contents/MacOS/snowsql',
                   '-o', f'exit_on_error={EXIT_ON_ERROR}',
                   '-o', 'friendly=False',
                   '-o', 'output_format=plain',
                   '-o', 'header=False',
                   '-o', 'log_level=CRITICAL',
                   '-q',
                   f'select dt, count(1) from marketing_dev.publish.user_stream_detail WHERE yr=\'{current_year-wid}\' and mo={mo} group by 1 order by 1']

            worker = SnowDatabaseWorker(cmd=cmd, year=current_year-wid, result_queue=year_queue, db_meta=db_meta)
            workers.append(worker)
            worker.start()

    i=0

    while len(year_queue) < num_years:
        if i<num_years:
            printProgressBar(i, num_years, prefix=ptitle, suffix='Complete', length=plen)
        #print (i)
        sleep(delay)
        i+=1

    #printProgressBar(i, num_years, prefix=ptitle, suffix='Complete', length=plen)
    if 1:
        for wid in range(num_years):
            workers[wid].join()

    printProgressBar(num_years, num_years, prefix=ptitle, suffix='Complete', length=plen)

def sn_cleanup(raw, result):
    for year, val in raw.items():
        result[year]={}
        sn_raw, err, status = val
        for line in sn_raw.strip().split(os.linesep):
            if not 'Time Elapsed:' in line:
                #pprint(line.split())
                day, count= line.split()
                assert count, f"Count is not defined for {day}"
                result[year][day]=int(count)

def at_cleanup(raw, result):
    for k, v in raw.items():
        result[k] = {}
        at_json, err, status = v
        #pprint(at_json)
        j = json.loads(at_json)
        assert 'ResultSet' in j.keys()
        assert 'Rows' in j['ResultSet'].keys()
        for row in j['ResultSet']['Rows'][1:]:
            #print(row)
            day, count = [x['VarCharValue'] for x in row['Data']]
            result[k][day] = int(count)

def at_get_q_results(in_queue, year_queue, timeout):

    workers = []
    plen=40
    delay = 3
    ptitle=' Athena download:'
    printProgressBar(0, num_years*3, prefix=ptitle, suffix='Complete', length=plen)

    for year, val in in_queue.items():

        at_q_json, err, status = val
        j = json.loads(at_q_json)
        ex_id = j.get('QueryExecutionId')
        assert ex_id, year
        cmd = ['aws',
               'athena',
               'get-query-results',
               '--region',
               'us-east-1',
               '--query-execution-id',
               ex_id.strip('"')]

        worker = AthenaQueryResultsWorker(cmd=cmd, year=year, result_queue=year_queue, timeout=timeout)
        workers.append(worker)
        worker.start()

    i=0

    while len(year_queue) < num_years:
        if i<num_years*3:
            printProgressBar(i, num_years*3, prefix=ptitle, suffix='Complete', length=plen)
        #print (i)
        sleep(delay)
        i+=1

    #printProgressBar(i, num_years, prefix=ptitle, suffix='Complete', length=plen)
    if 1:
        for wid in range(num_years):
            workers[wid].join()

    printProgressBar(num_years*3, num_years*3, prefix=ptitle, suffix='Complete', length=plen)
from  itertools import zip_longest

import re

def comma_me(amount):
    orig = amount
    new = re.sub("^(-?\d+)(\d{3})", '\g<1>,\g<2>', amount)
    if orig == new:
        return new
    else:
        return comma_me(new)

def reduce(data, out):
    right,left =data
    i=0
    m=0
    prev_mo=None
    for ry,ly in zip_longest(sorted(right.keys()), sorted(left.keys())):
        #print (ry,ly)
        y = ry if ry else ly
        #out[y]=[]
        i=0
        m=0
        prev_mo = None
        if 1:
            for rd,ld in zip_longest(sorted(right[ry].keys()), sorted(left[ly].keys())):
                rcnt=right[ry][rd] if ry and rd else 0
                lcnt=left[ly][ld] if ly and ld else 0
                dd=rd if rd else ld

                mon=dd.split('-')[1]
                if prev_mo and prev_mo != mon:
                    m=0
                #else:


                if rcnt-lcnt or show_match:
                    mo = mon if m == 0 else ''
                    out.append([y if i==0 else '',mo ,   dd,rcnt, lcnt, comma_me(str(rcnt-lcnt))])
                    i +=1
                    m +=1
                    prev_mo = mon
        #break





def get_formatted(title,data,headers,join = True):
    " Return string output for the sql to be run "
    expanded = False

    formatted = []
    settings = OutputSettings(table_format='psql', dcmlfmt='d', floatfmt='g',
                                   expanded=expanded)

    for title, rows, headers, status, sql, success, is_special in [[title, data, headers, None, None, None, False]]:
        formatted.extend(format_output(title, rows, headers, status, settings))
    if join:
        formatted = '\n'.join(formatted)

    return formatted

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    # Print New Line on Complete
    if iteration == total:

        print()


if __name__ == "__main__":
    num_years = 1
    at_queue = {}
    default_month= '02'
    default_year = None # 2019 #current_year
    show_match=False

    if 1:
        cli()



    if 0:


        submit_athena_async(num_years=num_years, year_queue=at_queue)
        out=[]
        for k in sorted(at_queue.keys()):
            v=at_queue[k]
            at_json, err, status = v
            j = json.loads(at_json)
            ex_id = j.get('QueryExecutionId')
            out.append([k,default_month,ex_id, err, status])


        headers = ['Year' if not default_year else f'Year({default_year})','Month' if not default_month else f'Month({default_month})','Query ID', 'Error','Status']


        #print()
        print(get_formatted(None, out, headers))



    if 0:

        sn_queue = {}
        ex_snow_async(num_years=num_years, year_queue=sn_queue)
        out = []
        for k in sorted(sn_queue.keys()):
            v=sn_queue[k]
            at_json, err, status = v
            out.append([k,default_month,len(at_json), err, status])
        headers = ['Year' if not default_year else f'Year({default_year})','Month' if not default_month else f'Month({default_month})','JSON length', 'Error','Status']


        print(get_formatted(None, out, headers))
        sn_data={}

        sn_cleanup(sn_queue,sn_data)
    #e(0)


    if 0:
        assert len(at_queue.keys()) == num_years
        #at_queue[current_year]='{\n    "QueryExecutionId": "61f60e26-5635-4467-a2de-eb99cc1a5e01"\n}\n'

        s3_queue={}
        at_timeout = 3

        at_get_q_results(at_queue, year_queue=s3_queue, timeout=at_timeout)


        out=[]
        for k in sorted(s3_queue.keys()):
            v=s3_queue[k]
            at_json, err, status = v
            out.append([k,default_month,len(at_json), err, status])

        headers = ['Year' if not default_year else f'Year({default_year})','Month' if not default_month else f'Month({default_month})','JSON length', 'Error','Status']


        print(get_formatted(None, out, headers))


        at_data={}
        at_cleanup(s3_queue, at_data)


        out=[]

        reduce([at_data,sn_data], out)

        headers = ['Year' if not default_year else f'Year({default_year})','Month' if not default_month else f'Month({default_month})','Date', 'Athena', 'Snowflake', 'Diff']
        print(get_formatted('\nAthena v.s. Snowflake "by date" count difference report.' ,out,headers))



