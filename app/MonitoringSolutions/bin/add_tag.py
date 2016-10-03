import subprocess
import shlex

fname = 'monitoring_log.txt'
logstash_fname = '/opt/logstash/bin/conf.d/tag.conf'

def append_to_file(filename, str_to_write):
    with open(filename, 'a') as fp:
        fp.write(str_to_write)

def add_custom_tag(tag_dict):
    data =[]
    data.append('filter {\n')
    data.append(' mutate {\n')
    data.append('   add_field => {\n')
    for tagkey, tag_val in tag_dict.items():
        data.append('    ' + tagkey + ' => \"'+tag_val+'\"\n')
    data.append('   }\n')
    data.append('  }\n')
    data.append('}')
    append_to_file(fname, '\nLOGSTASH CONF FILE: \n' + ''.join(data))
    with open(logstash_fname,'w') as conf_file:
        conf_file.writelines(data)
    stop_logstash_service()
    start_logstash_service()

def add_default_tag():
    add_custom_tag({'JobTagName':'default'})
    stop_logstash_service()

def stop_logstash_service():
    #subprocess.Popen(['pkill', '-9', 'logstash'])
    a = subprocess.Popen("sudo ps -aux | grep logstash | awk {'print $2'} | sudo xargs kill -9", shell=True)
    a.communicate()

def start_logstash_service():
    start_cmd = 'sudo /opt/logstash/bin/logstash -f /opt/logstash/bin/conf.d/'
    subprocess.Popen(shlex.split(start_cmd))
