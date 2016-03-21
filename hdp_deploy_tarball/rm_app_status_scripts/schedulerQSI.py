import json, sys, urllib2, time,re
from optparse import OptionParser
prev_time = 0
diff_time = 0
empty = 0
tolerance = 20
sleep_time = 4
parser = OptionParser()
parser.add_option("-a", "--rm-webapp-address", dest="rm_addr", action='store', type="string", help="[REQUIRED] RM Webapp address e.g. http://host:port/ws/v1/timeline")
parser.add_option("-q", "--queues", dest="queues", action='store', type="string", default=None, help="Queue/Queues, Commait queues")
(options, args) = parser.parse_args()
if not options.rm_addr:
    print >>sys.stderr, "-a/--rm-webapp-address not provided"
    sys.exit(1)
j_url = options.rm_addr + '/ws/v1/cluster/scheduler'
#http://cn042-10.l42scl.hortonworks.com:8088/ws/v1/cluster/apps?states=RUNNING,NEW,SUBMITTED,ACCEPTED
def get_jmx(url, headers={'Accept': 'application/json','Content-Type': 'application/json'}):
    if url is None:
         return
    try:
        req = urllib2.Request(url, headers=headers)
        response = urllib2.urlopen(req) #urllib2.urlopen(url)
        retcode = response.getcode()
        jmxdata = response.read() 
        if retcode == 200:
            data = json.loads(jmxdata)
            return data #['beans']
    except urllib2.URLError, e:
         if isinstance(e, urllib2.HTTPError):
             print sys.stderr, str(e.code) + " " + str(e.msg)

    return None

def print_queue_state_info(qsi, diff_time, c_time, s_time):
     f_params = [ 'queueName', 'state', 'userLimit', 'userLimitFactor', 'capacity', 'maxCapacity', 'absoluteMaxCapacity', 'usedCapacity', 'absoluteUsedCapacity', 
                  'maxApplicationsPerUser', 'maxApplications', 'preemptionDisabled', 'numApplications', 'numActiveApplications', 'numPendingApplications', 'numContainers' , 'pendingContainers', ]
     u_params = [ 'username', 'numPendingApplications', 'numActiveApplications', 'resourcesUsed', 'AMResourceUsed', 'userResourceLimit', 'resources']
     r_params = [ 'memory', 'vCores']
     rp_params = [ 'used', 'reserved', 'pending', 'amUsed' ]
     if qsi:
         qsi_str = str(diff_time) + ',' + str(c_time) + ',' +  s_time + ','
         for p in f_params:
              if p in [ 'pendingContainers' ]:  #Skipping for 2.3.x
                  continue
              qsi_str =qsi_str + str(qsi[p]) + ','
         #print qsi_str 
         if 'users' in qsi and type(qsi['users']) == dict and 'user' in qsi['users'] and type(qsi['users']['user']) == list:
             for u in qsi['users']['user']:
                 usr_str = ''
                 for up in u_params:
                     if up in  [ 'resources' ]:  #Skipping for 2.3.x
                         continue
                     if up in [ 'resourcesUsed', 'AMResourceUsed', 'userResourceLimit' ]:
                         usr_str = usr_str + up + '=' + str(u[up]['memory']) + '-' + str(u[up]['vCores']) + ':'
                     elif 'resources' == up and 'resourceUsagesByPartition' in u[up]:
                         for rup in u[up]['resourceUsagesByPartition']:
                              for rp in rp_params:
                                   #print str(rp) + str(rup[rp]['memory'])  
                                   usr_str  = usr_str + rp + '=' + str(rup[rp]['memory']) + '_' + str(rup[rp]['vCores']) + ':'
                     else:
                         usr_str = usr_str + str(u[up]) + ':'
                 qsi_str = qsi_str + usr_str + ','   
         print >>sys.stderr, qsi_str
         #print qsi['queueName'], qsi['userLimit'], qsi['numContainers'],qsi['maxApplicationsPerUser'], qsi['pendingContainers'], qsi['maxCapacity'], qsi['capacity'], qsi['usedCapacity'], qsi['userLimitFactor'], qsi['userLimit'])

def get_queues(expected_queue=None):
      global prev_time
      global diff_time
      global empty
      #j_url = 'http://cn042-10.l42scl.hortonworks.com:8088/ws/v1/cluster/scheduler' 
      data = get_jmx(j_url)
      if data is not None and type(data) == dict:
          if 'scheduler' in data and 'schedulerInfo' in data['scheduler'] and 'type' in data['scheduler']['schedulerInfo'] and re.match('capacityScheduler', data['scheduler']['schedulerInfo']['type'], re.I) is not None and 'queues' in data['scheduler']['schedulerInfo'] and 'queue' in data['scheduler']['schedulerInfo']['queues']:
             c_time = time.time()
             if prev_time != 0:
                 diff_time += c_time - prev_time
             s_time = time.strftime("%Y/%m/%d %H:%M:%S",time.gmtime(c_time))
             for queue in data['scheduler']['schedulerInfo']['queues']['queue']:
                  if expected_queue:
                     if queue['queueName'] in expected_queue:
                         print_queue_state_info(queue, diff_time, c_time, s_time)
                         break
                  else:
                      print_queue_state_info(queue, diff_time, c_time, s_time)
             print >>sys.stderr, '============================================================================================================================================================'
             prev_time = c_time
          else:
             empty = empty + 1
             return
      else:
          empty = empty + 1
          return    

def run():
    queue_name = None
    if options.queues:
        queue_name = options.queues.split(',')
    while True:
        get_queues(queue_name) 
        #break
        time.sleep(sleep_time)
        if empty > tolerance:
            break

run()
