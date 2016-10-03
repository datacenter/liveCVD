import sys
sys.path.append('/usr/local/lib/python2.7/dist-packages/')

import os
import subprocess
import shlex
import time

import requests
import json

import datetime

import logging

import logstash

import splunklib.client as client
import splunklib.results as results

from add_tag import *

sys.path.append('/etc/collectd/')
from globals import *

import logstash

os.unsetenv("LD_LIBRARY_PATH")

argv_ar = sys.argv

hadoop_jar_dev_test_path = '/opt/mapr/hadoop/hadoop-0.20.2/hadoop-0.20.2-dev-test.jar' #
hadoop_jar_dev_ex_path = '/opt/mapr/hadoop/hadoop-0.20.2/hadoop-0.20.2-dev-examples.jar' #

start_collect_cmd = 'sudo ansible-playbook /etc/ansible/start_collectd_src.yaml' #
stop_collect_cmd = 'sudo ansible-playbook /etc/ansible/stop_collectd_src.yaml' #

monitor_start_collect_cmd = 'service collectd start' #
monitor_stop_collect_cmd = 'service collectd stop' #
#monitor_start_collect_cmd = '/opt/collectd/sbin/collectd' #
#monitor_stop_collect_cmd = 'pkill -9 collectd' #

start_collect_cmd_sp = shlex.split(start_collect_cmd)
stop_collect_cmd_sp = shlex.split(stop_collect_cmd)
list_jobs_cmd_sp = shlex.split(list_jobs_cmd)
monitor_start_collect_cmd_sp = shlex.split(monitor_start_collect_cmd)
monitor_stop_collect_cmd_sp = shlex.split(monitor_stop_collect_cmd)
job_status_cmd_sp = shlex.split(job_status_cmd)

fname = 'monitoring_log.txt'

HOST = "localhost"
PORT = 8090
USERNAME = "admin"
PASSWORD = "admin"

def append_to_file(filename, str_to_write):
    with open(filename, 'a') as fp:
        fp.write(str_to_write)

def get_avg_stats(job_tag_name):
    try:
        service = client.connect(username=USERNAME, password=PASSWORD, port=PORT)
    except client.socket.error:
        append_to_file(fname, '\n' + 'Error Connecting to splunk lib!!!' + '\n')
        return {}

    jobs = service.jobs
    res_dict = {}

    query = "search type=collectd sourcetype=server_stats plugin=cpu JobTagName=%s| stats avg(cpu_util) as avg_cpu_util | eval avg_cpu_util = round(avg_cpu_util,2)| stats values(avg_cpu_util)" % (job_tag_name)
    qresults = service.jobs.oneshot(query)
    reader = results.ResultsReader(qresults)
    for item in reader:
        res_dict.update({"avg_cpu": item.values()[0]})
        break

    query = "search type=collectd sourcetype=server_stats plugin=memory JobTagName=%s| stats values(mem_util) as util values(mem_buffered) as buff values(mem_cached) as cache values(mem_free) as free values(mem_slab_recl) as slab values(mem_slab_unrecl) as unslab  by Hostname,_time | eval mem_util=(util*100)/(buff+cache+free+slab+unslab+util) | stats avg(mem_util) as mem_util | eval mem_util = round(mem_util,2) | stats values(mem_util)" % (job_tag_name)
    qresults = service.jobs.oneshot(query)
    reader = results.ResultsReader(qresults)
    for item in reader:
        res_dict.update({"avg_ram": item.values()[0]})
        break
    
    query= "search type=collectd sourcetype=server_stats plugin=interface plugin_instance=all-interfaces collectd_type=if_octets JobTagName=%s| stats values(Rx) as valueRx by Hostname, _time | stats earliest(_time) AS Earliest, earliest(valueRx) as start_Rx , latest(_time) AS Latest, latest(valueRx) as end_Rx by Hostname | eval total = (end_Rx-start_Rx)/(Latest-Earliest) | eval Rx_Rate= (total*8)/1000000 |  stats avg(Rx_Rate) as Rx_Rate | eval Rx_Rate = round(Rx_Rate,2) | stats values(Rx_Rate)" % (job_tag_name)
    qresults = service.jobs.oneshot(query)
    reader = results.ResultsReader(qresults)
    for item in reader:
        res_dict.update({"avg_inf_rx": item.values()[0]})
        break

    query= "search type=collectd sourcetype=server_stats plugin=interface plugin_instance=all-interfaces collectd_type=if_octets JobTagName=%s| stats values(Tx) as valueTx by Hostname, _time | stats earliest(_time) AS Earliest, earliest(valueTx) as start_Tx , latest(_time) AS Latest, latest(valueTx) as end_Tx by Hostname | eval total = (end_Tx-start_Tx)/(Latest-Earliest) | eval Tx_Rate= (total*8)/1000000 |  stats avg(Tx_Rate) as Tx_Rate | eval Tx_Rate = round(Tx_Rate,2) | stats values(Tx_Rate)" % (job_tag_name)
    qresults = service.jobs.oneshot(query)
    reader = results.ResultsReader(qresults)
    for item in reader:
        res_dict.update({"avg_inf_tx": item.values()[0]})
        break

    query= "search type=collectd sourcetype=server_stats JobTagName=%s plugin=readdisk collectd_type=disk_octets | stats values(read) as valueRead by Hostname, _time| stats earliest(_time) AS Earliest, earliest(valueRead) as start_read , latest(_time) AS Latest, latest(valueRead) as end_read by Hostname | eval total = (end_read-start_read)/(Latest-Earliest) | eval disk_read = (total/(1024*1000)) | stats avg(disk_read) as disk_read | eval disk_read = round(disk_read,2)| stats values(disk_read)" % (job_tag_name)
    qresults = service.jobs.oneshot(query)
    reader = results.ResultsReader(qresults)
    for item in reader:
        res_dict.update({"avg_disk": item.values()[0]})
        break
    return res_dict

def get_formated_date(date_val):
    return str(datetime.datetime.fromtimestamp(int(str(date_val)[:10])).strftime('%B %d %Y %H:%M:%S'))

def get_http_response(url, data='{}'):
    rest_data_dict = {}
    try: 
        response = requests.get(url, data=data)
        if response.ok:
            rest_data_dict = json.loads(response.content)
        else:
            response.raise_for_status()
    except:
        print 'Error Connecting :', url
    return rest_data_dict

def send_to_logstash(msg, data_dict):
    logger = logging.getLogger('python-logstash-logger')
    logger.setLevel(logging.INFO)
    logger.addHandler(logstash.TCPLogstashHandler(logstash_host, 5000, version=1))
    logger.info(msg, extra=data_dict)
    return 

def send_final_app_stats(running_job_id, map_cnt, reduce_cnt):
    for resource_master in master_nodes:
        all_apps_data_dict = get_http_response(apps_url % (resource_master) + '/' + running_job_id)
        if not all_apps_data_dict:
            continue 
        app_data_dict = all_apps_data_dict.get('app', {})
        new_dict = {}
        for app_key in app_key_ar:
            new_dict['application_'+app_key] = app_data_dict[app_key]
        new_dict['application_startedTime'] = get_formated_date(new_dict['application_startedTime'])
        application_finishedTime = new_dict['application_finishedTime']
        if int(application_finishedTime) != 0:
            new_dict['application_finishedTime'] = get_formated_date(application_finishedTime)
        else:
            new_dict['application_finishedTime'] = '0'
        new_dict['application_elapsedTime'] = int(new_dict['application_elapsedTime']) / 1000

        if 0:#new_dict['application_finalStatus'] == 'SUCCEEDED':
             new_dict['job_mapsCompleted'] = map_cnt
             new_dict['job_mapsPending'] = 0
             new_dict['job_mapsRunning'] = 0
             new_dict['job_mapProgress'] = 100

             new_dict['job_reducesCompleted'] = reduce_cnt
             new_dict['job_reducesPending'] = 0
             new_dict['job_reducesRunning'] = 0
             if reduce_cnt:
                 new_dict['job_reduceProgress'] = 100
             else:
                 new_dict['job_reduceProgress'] = 0

        cluster_id = int(new_dict['application_id'].split('_')[1])
        new_dict['job_reducesTotal'] = reduce_cnt
        new_dict['job_mapsTotal'] = map_cnt
        new_dict['cluster_id'] = cluster_id
        new_dict['JobTagID'] = running_job_id
        new_dict['job_id'] = running_job_id
	new_dict['plugin'] = 'hadoop_dynamic_data'
        new_dict['stat_type'] = 'hadoop'
        send_to_logstash('App Final Data', new_dict)
    return

def get_job_info(running_job_id, running_flg):
    app_id = 'application' + running_job_id.strip('job')
    job_tag_dict = {}
    for resource_master in master_nodes:
        all_apps_data_dict = get_http_response(apps_url % (resource_master) + '/' + running_job_id)
        if not all_apps_data_dict:
            continue 
        app_data_dict = all_apps_data_dict.get('app', {})
        #if app_data_dict:
        #    job_tag_dict['Job_State'] = app_data_dict['state']
        #    job_tag_dict['Job_elapsedTime'] = str(app_data_dict['elapsedTime'])
        #    job_tag_dict['Job_Type'] = app_data_dict['applicationType']
        #    job_tag_dict['Job_Name'] = app_data_dict['name']
        if running_flg:
            time.sleep(5)
            cnt = 0
            while cnt < 6:
                jobs_data_dict = get_http_response(jobs_url%(resource_master , app_data_dict['id']))
                all_app_jobs = jobs_data_dict.get('jobs', {}).get('job', [])
                for job_data_dict in all_app_jobs:
                    job_tag_dict['job_mapsTotal'] = str(job_data_dict['mapsTotal'])
                    job_tag_dict['job_reducesTotal'] = str(job_data_dict['reducesTotal'])
                    job_tag_dict['job_reduceProgress'] = str(job_data_dict['reduceProgress'])
                    job_tag_dict['job_mapProgress'] = str(job_data_dict['mapProgress'])
                if jobs_data_dict:
                    break
                time.sleep(5)
                cnt += 1
        break
    return job_tag_dict

def write_tosplunk_log(argv_ar, tag_dict):
    str_t = "\n"
    str_t += "Job="+argv_ar[1]
    for tagname, tagval in tag_dict.items():
        str_t += "," + tagname + "="+tagval
    str_t += ",JobName="+argv_ar[2]
    str_t += ",NumofTrials="+argv_ar[3]
    if argv_ar[1] == "Teragen":
        str_t += ",DataSize_GB="+argv_ar[4]
        str_t += ",BlockSize_KB="+argv_ar[5]
        str_t += ",NumofMappers="+argv_ar[6]
    elif argv_ar[1] == "Terasort":
        str_t += ",NumofReducers="+argv_ar[4]
    elif argv_ar[1] == "DFSIO_WRITE" or argv_ar[1] == "DFSIO_READ":
        str_t += ",NumofFiles="+argv_ar[4]
        str_t += ",SizeperFile="+argv_ar[5]
    str_t += ",Server_Stats=View"
    str_t += ",Switch_Stats=View"
    str_t += ",Hadoop_Stats=View,"
    append_to_file('newfile.txt', str_t)
    append_to_file(fname, str_t)
    return

def get_runnning_job_ids():
    running_job_ids = []
    p_obj = subprocess.Popen(['ssh', rm_host] + list_jobs_cmd_sp, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
    outp, err = p_obj.communicate()
    jobs_out_sp = outp.split('\n')
    ind = 0
    for elm in jobs_out_sp:
        if 'Total jobs' in elm:
            break
        ind += 1

    if 'JobId' in jobs_out_sp[ind+1]:
        for line_elm in jobs_out_sp[ind+2:]:
            job_id = line_elm.split('\t')[0].strip()
            if job_id:
                running_job_ids.append(job_id)
    return running_job_ids

def get_finished_job_map_reduce_cnts(out_ar):
    map_cnt = 0
    reduce_cnt = 0
    map_txt = 'Number of maps:' 
    reduce_txt = 'Number of reduces:'
    for el in out_ar:
        if map_txt in el:
            map_cnt = el.split(map_txt)[1].strip()
        if reduce_txt in el:
            reduce_cnt =  el.split(reduce_txt)[1].strip()
            break
    return map_cnt, reduce_cnt

def run_trial(job_name, cnt, total_trial_cnt):
    start_hadoop_cmd  =''
    reset_cnt_cmd = ''

    if job_type == 'DFSIO_WRITE':
        num_files = argv_ar[4]
        size_per_file = argv_ar[5] + 'GB'
        start_hadoop_cmd = 'sudo -u %s hadoop jar %s TestDFSIO -write -nrFiles %s -fileSize %s' % (hadoop_uname, hadoop_jar_dev_test_path, num_files, size_per_file)
    elif job_type == 'DFSIO_READ':
        num_files = argv_ar[4]
        size_per_file = argv_ar[5] + 'GB'
        start_hadoop_cmd = 'sudo -u %s hadoop jar %s TestDFSIO -read -nrFiles %s -fileSize %s' % (hadoop_uname, hadoop_jar_dev_test_path, num_files, size_per_file)
    elif job_type == 'Teragen':
        reset_cnt_cmd = 'hadoop fs -rm -r -f -skipTrash /terasort-input'
        #hadoop jar /opt/mapr/hadoop/hadoop-0.20.2/hadoop-0.20.2-dev-examples.jar teragen -Ddfs.blocksize=536870912 -Dio.file.buffer.size=131072 -Dmapreduce.map.java.opts=-Xmx1536m -Dmapreduce.map.memory.mb=2048 -Dmapreduce.task.io.sort.mb=768 -Dyarn.app.mapreduce.am.resource.mb=1024 -Dmapred.map.tasks=512 10000000000  /terasort-input
        data_size = 10000000000  * int(argv_ar[4])
        block_size = 1024 * 1024 * int(argv_ar[5]) #-Ddfs.blocksize
        mapred_map_tasks = argv_ar[6] # -Dmapred.map.tasks
        start_hadoop_cmd = 'sudo -u %s hadoop jar %s teragen -Ddfs.blocksize=%s -Dmapred.map.tasks=%s %s /terasort-input' % (hadoop_uname, hadoop_jar_dev_ex_path, block_size,  mapred_map_tasks, data_size)
    elif job_type == 'Terasort':
        reset_cnt_cmd = 'hadoop fs -rm -r -f -skipTrash /terasort-output'
        #hadoop jar /opt/mapr/hadoop/hadoop-0.20.2/hadoop-0.20.2-dev-examples.jar terasort -Ddfs.blocksize=536870912 -Dio.file.buffer.size=131072 -Dmapreduce.map.memory.mb=2048 -Dmapreduce.reduce.memory.mb=4096 -Dmapreduce.task.io.sort.factor=100 -Dmapreduce.task.io.sort.mb=1024 -Dmapreduce.map.java.opts=-Xmx1536m -Dmapreduce.reduce.java.opts=-Xmx3072m -Dmapreduce.map.output.compress=true -Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.Lz4Codec -Dyarn.app.mapreduce.am.resource.mb=1024 -Dmapreduce.job.reduces=780 -Dmapreduce.map.disk=0.1 -Dmapreduce.reduce.disk=0.2 /terasort-input /terasort-output
        mapred_job_reduce = argv_ar[4] #-Dmapreduce.job.reduces
        start_hadoop_cmd = 'sudo -u %s hadoop jar %s terasort -Dmapreduce.map.output.compress=true -Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.Lz4Codec -Dmapreduce.job.reduces=%s /terasort-input /terasort-output' % (hadoop_uname, hadoop_jar_dev_ex_path, mapred_job_reduce)

    if start_hadoop_cmd:
        with open(log_stash_file_name, 'w') as fp:
            pass 
        append_to_file(fname, '\n' + start_hadoop_cmd + '\n')
        append_to_file(fname, '\n\n' + '+ ' * 20 + ' Trial No: ' + str(cnt) + ' of ' + str(total_trial_cnt)  + ' ' + '+ ' * 20 + '\nStarting Collectd')
        proc_obj = subprocess.Popen(start_collect_cmd_sp, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        out, err = proc_obj.communicate()
        if err:
            append_to_file(fname, '\nError Starting Collectd')
            sys.exit()

        append_to_file(fname, '\nStarting Monitor Node Collectd')        
        proc_obj = subprocess.Popen(monitor_start_collect_cmd_sp , stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        out, err = proc_obj.communicate()
        if err:
            append_to_file(fname, '\nError Starting Monitor Node Collectd')
            sys.exit()

        date_val = time.strftime('%m-%d-%Y', time.gmtime())
        job_name_tag = job_name + '_' + str(cnt)

        tag_dict = {}
        tag_dict['JobTagName'] = job_name_tag
        tag_dict['Date'] = date_val

        tag_dict['Job_Status'] = 'Scheduled'
        write_tosplunk_log(argv_ar, tag_dict)
        append_to_file(fname, '\nStarting Tagging..')
        add_custom_tag(tag_dict)

        append_to_file(fname, '\nStarting Hadoop Job')
        if reset_cnt_cmd:
            append_to_file(fname, '\nreset_cnt_cmd: ' + reset_cnt_cmd + '\n')
            reset_cnt_cmd_sp = shlex.split(reset_cnt_cmd)
            proc_obj = subprocess.Popen(['ssh', rm_host] + reset_cnt_cmd_sp, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
            out, err = proc_obj.communicate()
            append_to_file(fname, '\nreset_cnt_cmd Done: \n')

        start_hadoop_cmd_sp = shlex.split(start_hadoop_cmd)
        append_to_file(fname, '\nHadoop cmd: ' + start_hadoop_cmd + '\n')

        t1 = time.time()
        start_time = time.strftime('%m-%d-%Y %H:%M:%S', time.localtime(t1))
        tag_dict['Job_Start_Time'] = start_time

        proc_obj = subprocess.Popen(['ssh', rm_host] + start_hadoop_cmd_sp,  stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        ccnt = 0 
        while True:
            if 0:#job_type == 'Terasort':
                append_to_file(fname, '\n Tera Sort! Additional sleep for 45 seconds')
                time.sleep(45)
            append_to_file(fname, '\nCollecting Hadoop Job List: '+ str(ccnt))
            time.sleep(1)
            ccnt += 1
            running_job_ids = get_runnning_job_ids()

            if running_job_ids or (ccnt > 60):
                break

        append_to_file(fname, '\nRunning Jobs: '+ str(running_job_ids))
        map_cnt, reduce_cnt = 0, 0
        if running_job_ids:
            ####Tagging####
            running_job_id = running_job_ids[0]
            tag_dict['JobTagID'] = running_job_id
            tag_dict['Job_Status'] = 'Running'
            job_tag_dict = get_job_info(running_job_id, 1)
            tag_dict.update(job_tag_dict)
            write_tosplunk_log(argv_ar, tag_dict) 
            out, err = proc_obj.communicate()

            proc_obj = subprocess.Popen(['ssh', rm_host] + job_status_cmd_sp + [running_job_id],  stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
            out, err = proc_obj.communicate()
            map_cnt, reduce_cnt = get_finished_job_map_reduce_cnts((out + err).split('\n'))

            send_final_app_stats(running_job_id, map_cnt, reduce_cnt)
            append_to_file(fname, '\nJob Ended')
            append_to_file(fname, '\nStopped Tagging')
        else:
            append_to_file(fname, '\nNo Job Running')

        t2 = time.time()
        job_completion_time = '%.*f' % (1, t2-t1)
        tag_dict['Time_Seconds'] = job_completion_time

        end_time = time.strftime('%m-%d-%Y %H:%M:%S', time.localtime(t2))
        tag_dict['Job_End_Time'] = end_time

        if map_cnt+reduce_cnt ==0:
            tag_dict['Job_Status'] = 'Error'
        else:
            tag_dict['Job_Status'] = 'Finished'
    
        tag_dict.update(get_avg_stats(job_name_tag))
        tag_dict.update(job_tag_dict)
        write_tosplunk_log(argv_ar, tag_dict)
        time.sleep(1)

        add_default_tag()
        stop_logstash_service()

        append_to_file(fname, '\nStopping Monitor Node Collectd')        
        proc_obj = subprocess.Popen(monitor_stop_collect_cmd_sp , stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        out, err = proc_obj.communicate()
        if err:
            append_to_file(fname, '\nError Stopping Monitor Node Collectd')
            sys.exit()

        proc_obj = subprocess.Popen(stop_collect_cmd_sp, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        out, err = proc_obj.communicate()
        if err:
            append_to_file(fname, '\nError Stopping Collectd')
            sys.exit()

        append_to_file(fname, '\nStopped Collectd\n' + '-'*50 + '\n')

if len(argv_ar) > 3:
    running_job_ids = get_runnning_job_ids()
    if running_job_ids:
        append_to_file(fname, '\n\n' + '\nAlready Job is RUNNNING!!!!!!' + 'Job Ids: ' + str(running_job_ids) + '\n\n')
        sys.exit()

    job_type = argv_ar[1]
    job_name = argv_ar[2]
    trial_cnt = argv_ar[3]
    try:
        trial_cnt = int(trial_cnt)
    except: 
        trial_cnt = 0

    total_trial_cnt = trial_cnt
    cnt = 1

    while trial_cnt > 0:
        run_trial(job_name, cnt, total_trial_cnt)
        trial_cnt -= 1
        cnt += 1
