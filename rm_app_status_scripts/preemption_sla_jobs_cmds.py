import json, sys, urllib2, time,re, socket, os
from optparse import OptionParser

base_url = None 
def get_url(url, headers={'Accept': 'application/json','Content-Type': 'application/json'}):
    if url is None:
        return
    try:
        req = urllib2.Request(url, headers=headers)
        response = urllib2.urlopen(req) #urllib2.urlopen(url)
        retcode = response.getcode()
        jmxdata = response.read()
        if retcode == 200:
            data = json.loads(jmxdata)
            return data
    except urllib2.URLError, e:
        if isinstance(e, urllib2.HTTPError):
            print >>sys.stderr, str(e.code) + " " + str(e.msg)
    return None

def get_queue_capacity(queue_name='default', total_mb=None, data=None):
    if data is None:
        j_url = base_url + '/ws/v1/cluster/scheduler'
        data = get_url(j_url)
    if data is not None and type(data) == dict:
        if 'scheduler' in data and 'schedulerInfo' in data['scheduler'] and 'type' in data['scheduler']['schedulerInfo'] and re.match('capacityScheduler', data['scheduler']['schedulerInfo']['type'], re.I) is not None and 'queues' in data['scheduler']['schedulerInfo'] and 'queue' in data['scheduler']['schedulerInfo']['queues']:
            for queue in data['scheduler']['schedulerInfo']['queues']['queue']:
                if queue['queueName'] ==  queue_name: 
                    queue_mb = 0
                    #print >>sys.stderr, total_mb
                    if total_mb is not None:
                        queue_mb = int(total_mb/queue['capacity'])
                        qr = { 'name': queue['queueName'], 'cap' : queue['capacity'], 'maxCap': queue['maxCapacity'], 'ulf': queue['userLimitFactor'], 'queue_mb': queue_mb }
                        return qr
                    #print >>sys.stderr, str(queue['queueName']) + ' ' + str(queue['capacity']) + ' ' + str(queue['maxCapacity']) + ' ' + str(queue['userLimitFactor']) + ' ' + str(queue_mb)

def get_cluster_resoruces():
    cm_url = base_url + '/ws/v1/cluster/metrics'
    data = get_url(cm_url)
    #print >>sys.stderr, str(data)
    if data is not None and type(data) == dict and 'clusterMetrics' in data and data['clusterMetrics'] is not None and 'totalMB' in data['clusterMetrics'] and data['clusterMetrics'] is not None and re.match('^\d+$', str(data['clusterMetrics']['totalMB'])) is not None:
        #print >>sys.stderr, data['clusterMetrics']['totalMB']
            return data['clusterMetrics']['totalMB']

def create_varying_app_parital_cmds(qu_res, initial_cmd, min_mb=1024):
    if qu_res is None:
        return None
    am_mb = min_mb
    remaining_mb = qu_res['queue_mb'] - am_mb 
    #num_tasks = int(remaining_mb/min_mb)
    num_tasks = num_maps = num_reduces = int(remaining_mb/min_mb)
    #num_maps = int(num_tasks/2)
    #num_reduces = int(num_tasks/2)
    #print remaining_mb
    print num_tasks
    print num_maps
    print num_maps
    #return
    time_sleep = 285000
    time_sleep1 = 142000
    par_cmds = [ "%s -m %d -mt 50000 -r %d -rt 50000" % (initial_cmd, num_maps, num_reduces),
                 "%s -m %d -mt 140000 -r %d -rt 140000" % (initial_cmd,num_maps, num_reduces),
                 "%s -m %d -mt 285000 -r %d -rt 285000" % (initial_cmd, num_maps, num_reduces),
                 "%s -m %d -mt 435000 -r %d -rt 435000" % (initial_cmd, num_maps, num_reduces),
                 "%s -m %d -mt %d -r %d -rt %d" % (initial_cmd ,num_maps,time_sleep, num_reduces, time_sleep),
                 "%s -m %d -mt %d -r %d -rt %d" % (initial_cmd, int(num_maps/2), time_sleep, int(num_reduces/2), time_sleep),
                 "%s -m %d -mt %d -r %d -rt %d" % (initial_cmd, num_maps * 2 , int(time_sleep/2), num_reduces * 2, int(time_sleep/2)),
                 "%s -m %d -mt %d -r %d -rt %d" % (initial_cmd, num_maps * 10 , int((time_sleep/10) - 4000), num_reduces * 10, int((time_sleep/10) - 4000)),
                 "%s -m %d -mt %d -r %d -rt %d" % (initial_cmd, num_maps * 54 , 5000, num_reduces * 12.5, 5000),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 2, min_mb * 2, min_mb * 3, num_maps/2, time_sleep1, num_reduces/3, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 2, min_mb * 2, min_mb * 4, num_maps/2, time_sleep1, num_reduces/4, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 2, min_mb * 3, min_mb * 4, num_maps/3, time_sleep1, num_reduces/4, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 2, min_mb * 4, min_mb * 3, num_maps/4, time_sleep1, num_reduces/3, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 3, min_mb * 2, min_mb * 2, num_maps/2, time_sleep1, num_reduces/2, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 3, min_mb * 3, min_mb * 4, num_maps/3, time_sleep1, num_reduces/4, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 3, min_mb * 2, min_mb * 3, num_maps/2, time_sleep1, num_reduces/3, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 3, min_mb * 2, min_mb * 4, num_maps/2, time_sleep1, num_reduces/4, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 3, min_mb * 4, min_mb * 3, num_maps/4, time_sleep1, num_reduces/3, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 3, min_mb * 4, min_mb * 2, num_maps/4, time_sleep1, num_reduces/2, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 4, min_mb * 2, min_mb * 2, num_maps/2, time_sleep1, num_reduces/2, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 4, min_mb * 3, min_mb * 3, num_maps/3, time_sleep1, num_reduces/3, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 4, min_mb * 3, min_mb * 4, num_maps/3, time_sleep1, num_reduces/4, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 4, min_mb * 4, min_mb * 3, num_maps/4, time_sleep1, num_reduces/3, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 4, min_mb * 4, min_mb     * 2, num_maps/4, time_sleep1, num_reduces/2, time_sleep1),
                 "%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * 4, min_mb * 2, min_mb * 3, num_maps/2, time_sleep1, num_reduces/3, time_sleep1)
                 ]
    #time_sleep = 142000
    for i in range(2,5):
        #print i
        par_cmds.append("%s -Dyarn.app.mapreduce.am.resource.mb=%d -m %d -mt %d -r %d -rt %d" % (initial_cmd, min_mb * i, num_maps * i , time_sleep1, num_reduces * i, time_sleep1))
        par_cmds.append("%s -Dmapreduce.map.memory.mb=%d -m %d -mt %d -r %d -rt %d" % (initial_cmd, min_mb * i , int(num_maps/i) , time_sleep1, num_reduces * i, time_sleep1))
        par_cmds.append("%s -Dmapreduce.reduce.memory.mb=%d -m %d -mt %d -r %d -rt %d" % (initial_cmd, min_mb * i, num_maps * i, time_sleep1, num_reduces/i, time_sleep1))
        par_cmds.append("%s -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (initial_cmd, min_mb * i, min_mb * i, min_mb * i, num_maps/i, time_sleep, num_reduces/i, time_sleep))
    return par_cmds
    '''
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 2, min_mb * 2, min_mb * 3, num_maps/2, time_sleep, num_reduces/3, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 2, min_mb * 2, min_mb * 4, num_maps/2, time_sleep, num_reduces/4, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 2, min_mb * 3, min_mb * 4, num_maps/3, time_sleep, num_reduces/4, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 2, min_mb * 4, min_mb * 3, num_maps/4, time_sleep, num_reduces/3, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 3, min_mb * 2, min_mb * 2, num_maps/2, time_sleep, num_reduces/2, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 3, min_mb * 3, min_mb * 4, num_maps/3, time_sleep, num_reduces/4, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 3, min_mb * 2, min_mb * 3, num_maps/2, time_sleep, num_reduces/3, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 3, min_mb * 2, min_mb * 4, num_maps/2, time_sleep, num_reduces/4, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 3, min_mb * 4, min_mb * 3, num_maps/4, time_sleep, num_reduces/3, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 3, min_mb * 4, min_mb * 2, num_maps/4, time_sleep, num_reduces/2, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 4, min_mb * 2, min_mb * 2, num_maps/2, time_sleep, num_reduces/2, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 4, min_mb * 3, min_mb * 3, num_maps/3, time_sleep, num_reduces/3, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 4, min_mb * 3, min_mb * 4, num_maps/3, time_sleep, num_reduces/4, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 4, min_mb * 4, min_mb * 3, num_maps/4, time_sleep, num_reduces/4, time_sleep)
    print " -Dyarn.app.mapreduce.am.resource.mb=%d -Dmapreduce.map.memory.mb=%d -Dmapreduce.reduce.memory.mb=%d  -m %d -mt %d -r %d -rt %d " % (min_mb * 4, min_mb * 2, min_mb * 3, num_maps/2, time_sleep, num_reduces/3, time_sleep)
    '''

def main():
    global base_url
    parser = OptionParser()
    parser.add_option("-u", "--rm-webui", dest="base_url", action='store', type="string", help="[REQUIRED] Resource Manager Web UI address e.g. http://host:port")
    parser.add_option("-m", "--min-allocaiton-mb",action="store", dest="min_allocation_mb", type="int", default=1024, help="Minimum Allocation MB from yarn-site/yarn-default xml's")
    parser.add_option("-f", "--mr-framwork", action="store", dest="mr_framework", type="string", default="yarn", help="Mapreduce Framework Name")
    parser.add_option("-q", "--queue", action="store", dest="queue_name", type="string", default="hive1", help="CS application queue to be which sholud be submitted") 
    parser.add_option("-j", "--jar", action="store", dest="test_jar", type="string", help="[REQUIRED] MapReduce/Tez tests jar", default='/grid/4/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-tests.jar')
    parser.add_option("-p", "--test-class", action="store", dest="test_class_name", type="string", default="sleep", help="Test Example to be used. It accepts only either sleep or mrrsleep")
    parser.add_option("-s", "--submit", action="store_true", dest="submit", default=False, help="Submit Jobs or not")
    (options, args) = parser.parse_args()
    if not options.test_jar:
        parser.error("Required Option:  Test Jar not provided")
    #if not os.path.exists(options.test_jar):
    #    parser.error("Required Option:  Test Jar not %s does not exists" % options.test_jar)
    if options.test_class_name not in ["sleep", "mrrsleep" ]:
        parser.error("-p/--test-class  Only accepts sleep or mrrsleep. But got %s" % options.test_class_name)
    if not options.base_url:
        parser.error("-u/--rm-webui not provided")
    base_url = options.base_url
    cluster_total_mb = get_cluster_resoruces()
    j_url = base_url + '/ws/v1/cluster/scheduler'
    data = get_url(j_url)
    qr =  get_queue_capacity('hive1', total_mb=cluster_total_mb, data=data)
    initial_cmd = 'yarn jar %s %s -Dmapreduce.framework.name=%s -Dmapreduce.job.queuename=%s -Dtez.queue.name=%s ' % (options.test_jar, options.test_class_name, options.mr_framework,
            options.queue_name, options.queue_name)
    partial_cmds = create_varying_app_parital_cmds(qr, initial_cmd,  min_mb=options.min_allocation_mb)
    print "\n".join(partial_cmds)



main()
