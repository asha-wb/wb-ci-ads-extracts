import datetime
import psycopg2
import logging
import random
import time

def get_redshift_database_connection(host, user, password, dbname, port=5439):
    """get a redshift connection"""
    retries = 0
    retries_allowed = 3
    logger = logging.getLogger('etldata_logging')
    while retries < retries_allowed:
        try:
            conn = psycopg2.connect(
                host=host,
                user=user,
                password=password,
                dbname=dbname,
                port=port)
            if conn is None:
                raise Exception('Redshift connection failed')

            return conn
        except (psycopg2.DatabaseError, psycopg2.Error, Exception) as e:
            logger.error('Caught exception: %s', str(e))
            retries += 1
            if retries >= retries_allowed:
                raise
            else:
                time.sleep(random.randint(5, 10))

def update_file_dtls(conn, *args, **kwargs):
    query = """
        INSERT INTO etldata.etl_process_file_dtls
        (
	        cidw_etlload_id
	        ,cidw_etlprocess_id
	        ,processname
	        ,context
	        ,source
	        ,destination
	        ,filename
	        ,size_bytes
	        ,rows_processed
	        ,transfer_status
	        ,transfer_dtm
	        ,duration_seconds
	        ,cidw_batch_id
        )
        VALUES
        ( 
	        {cidw_etlload_id}
	        ,{cidw_etlprocess_id}
	        ,'{processname}'
	        ,'{context}'
	        ,'{source}'
	        ,'{destination}'
	        ,'{filename}'
	        ,{size_bytes}
	        ,{rows_processed}
	        ,'{transfer_status}'
	        ,'{transfer_dtm}'
	        ,{duration_seconds}
	        ,{cidw_batch_id}
        );"""

    query = query.format(**kwargs)
    return run_redshift_query(conn, query)


def get_etlprocess_dtls(conn, etlprocessname):
    query = """
        SELECT
             ep.cidw_etlprocess_id
            ,ep.cidw_etlload_id
            ,ep.cidw_batch_id
            ,ep.cidw_lastcompletiondttm
        FROM
        (
            --Get the latest cidw_etlload_id for the process
            SELECT
                 pe.cidw_etlprocess_id      AS cidw_etlprocess_id
                ,MAX(pe.cidw_etlload_id)    AS cidw_etlload_id
                ,(SELECT max(cidw_batch_id) FROM etldata.etl_batch_dtls) as cidw_batch_id
                ,pd.lastcompletiondttm      AS cidw_lastcompletiondttm
            FROM
                etldata.etl_process_exec_dtls AS pe
            INNER JOIN 
                etldata.etl_process_dtls AS pd
                ON pe.cidw_etlprocess_id = pd.cidw_etlprocess_id
            WHERE pd.processname = '{etlprocessname}'
            GROUP BY pe.cidw_etlprocess_id, pd.lastcompletiondttm
        ) AS ep;
    """

    logger = logging.getLogger('etldata_logging')
    query = query.format(etlprocessname=etlprocessname)
    retries = 0
    retries_allowed = 3
    cursor = None

    while retries < retries_allowed:
        try:
            cursor = conn.cursor()
            if cursor is None:
                raise Exception('Failed to get redshift cursor')

            cursor.execute(query)
            response = cursor.fetchone()
            ret = {
                'cidw_process_id': response[0],
                'cidw_etl_load_id': response[1],
                'cidw_batch_id': response[2],
                'cidw_lastcompletiondttm': response[3]
            }

            cursor.close()
            return ret
        except (psycopg2.DatabaseError, psycopg2.Error, Exception) as e:
            logger.error('Caught exception: %s', str(e))
            retries += 1
            if retries >= retries_allowed:
                raise
            else:
                time.sleep(random.randint(5, 10))
        finally:
            if cursor != None:
                cursor.close()

def update_etlprocess_dtls(conn, etlprocessname, state, rows):
    """updates the redshift etldata details for the process being run"""
    query = ''
    if state == 'start':
        # updates process execution details to show I for initialized
        query = """INSERT INTO etldata.etl_process_exec_dtls
                    (
                        cidw_etlprocess_id,
                        status,
                        startdttm,
                        enddttm,
                        context
                    )
                    SELECT cidw_etlprocess_id,
                        'I'       AS status,
                        GETDATE() AS startdttm,
                        NULL      AS enddttm,
                        processcategory
                    FROM   etldata.etl_process_dtls
                    WHERE  lower(processname) = lower('{etlprocessname}');"""

    elif state == 'finish':
        # updates process execution details to show S for success
        query = """UPDATE etldata.etl_process_exec_dtls
                    SET     status        = 'S',--success
                            enddttm       = GETDATE(),
                            rowsprocessed = {rows},
                            cidw_batch_id = (select max(cidw_batch_id) from etldata.etl_batch_dtls)
                    WHERE
                        cidw_etlprocess_id = (
                            SELECT cidw_etlprocess_id -- qualify the process_id
                            FROM   etldata.etl_process_dtls
                            WHERE  lower(processname) = lower('{etlprocessname}') limit 1
                        )
                    AND startdttm = (
                            SELECT max(startdttm) -- get the max process execution start date for this process's execution
                            FROM   etldata.etl_process_exec_dtls
                            WHERE  cidw_etlprocess_id = (SELECT cidw_etlprocess_id
                                                         FROM   etldata.etl_process_dtls
                                                         WHERE  lower(processname) = lower('{etlprocessname}') limit 1)
                        );"""

    elif state == 'fail':
        # updates process execution details to show F for fail
        query = """UPDATE etldata.etl_process_exec_dtls
                    SET
                        status        = 'F',--failed
                        enddttm       = GETDATE(),
                        rowsprocessed = {rows},
                        cidw_batch_id = (select max(cidw_batch_id) from etldata.etl_batch_dtls)
                    WHERE
                        cidw_etlprocess_id = (
                            SELECT cidw_etlprocess_id -- qualify the process_id
                            FROM   etldata.etl_process_dtls
                            WHERE  lower(processname) = lower('{etlprocessname}') limit 1
                        )
                        AND startdttm = (
                            SELECT max(startdttm) -- get the max process execution start date for this process's execution
                            FROM   etldata.etl_process_exec_dtls
                            WHERE  cidw_etlprocess_id = (SELECT cidw_etlprocess_id
                                                         FROM   etldata.etl_process_dtls
                                                         WHERE  lower(processname) = lower('{etlprocessname}') limit 1)
                        );"""

    elif state == 'process_dtls':
        # updates main process details with the last completed run time
        query = """UPDATE etldata.etl_process_dtls
                    SET    lastcompletiondttm = etl.enddttm
                    FROM (
                            SELECT cidw_etlprocess_id, enddttm
                            FROM   etldata.etl_process_exec_dtls
                            WHERE
                                cidw_etlprocess_id = (
                                        SELECT cidw_etlprocess_id -- qualify the process_id
                                        FROM   etldata.etl_process_dtls
                                        WHERE  lower(processname) = lower('{etlprocessname}') limit 1
                                )
                         AND startdttm = (
                            SELECT max(startdttm) -- get the max process execution start date for this process's execution
                            FROM   etldata.etl_process_exec_dtls
                            WHERE  startdttm is not null and enddttm is not null -- make sure to only include successful completions
                            AND    cidw_etlprocess_id = (SELECT cidw_etlprocess_id
                                                         FROM   etldata.etl_process_dtls
                                                         WHERE  lower(processname) = lower('{etlprocessname}') limit 1) 
                            limit 1)
                    )  etl
                    WHERE etl.cidw_etlprocess_id = etldata.etl_process_dtls.cidw_etlprocess_id;"""

    query = query.format(etlprocessname=etlprocessname, rows=rows)
    return run_redshift_query(conn, query)

def run_redshift_query(conn, query):
    logger = logging.getLogger('etldata_logging')
    retries = 0
    retries_allowed = 3
    cursor = None

    while retries < retries_allowed:
        try:
            cursor = conn.cursor()
            if cursor is None:
                raise Exception('Failed to get redshift cursor')

            cursor.execute(query)
            conn.commit()
            cursor.close()
            cursor = None
            return True
        except (psycopg2.DatabaseError, psycopg2.Error, Exception) as e:
            logger.error('Caught exception: %s', str(e))
            retries += 1
            if retries >= retries_allowed:
                raise
            else:
                time.sleep(random.randint(5, 10))
        finally:
            if cursor != None:
                conn.commit()
                cursor.close()
