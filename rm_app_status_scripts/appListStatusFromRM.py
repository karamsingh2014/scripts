import json, sys, urllib2, time,re
from optparse import OptionParser
prev_time = 0
diff_time = 0
sep = ' '
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
j_url = options.rm_addr + '/ws/v1/cluster/apps?states=RUNNING,NEW,NEW_SAVING,SUBMITTED,ACCEPTED'
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

def print_application(app, diff_time, c_time, s_time):
     #app_keys = ['id', 'user', 'queue', 'state', 'priority', 'runningContainers', 'queueUsagePercentage', 'allocatedMB', 'allocatedVCores', 'applicationType', 'progress', 'clusterUsagePercentage',
     #            'numAMContainerPreempted', 'preemptedResourceMB', 'preemptedResourceVCores', 'resourceRequests']
     app_keys = ['id', 'user', 'queue', 'state', 'runningContainers', 'queueUsagePercentage', 'allocatedMB', 'allocatedVCores', 'applicationType', 'progress', 'clusterUsagePercentage', 
                 'numAMContainerPreempted', 'preemptedResourceMB', 'preemptedResourceVCores', 'resourceRequests']
     rq_keys = [ 'numContainers' ]
     if app:
         containers = 0
         app_str = str(diff_time) + sep + str(c_time) + sep +  s_time + sep
         for k in app_keys:
             if k not in app:
                 if k == 'resourceRequests':
                     containers += 0
                 else:
                     app_str = app_str + 'N/F' + sep
                 continue
             if k == 'resourceRequests':
                 for rq in app[k]:
                     if 'numContainers' in rq:
                          containers += int(rq['numContainers'])
             else:
                 app_str = app_str + str(app[k]) + sep
         print >>sys.stderr,  app_str + str(containers)

def get_applications(expected_queue=None):
      global prev_time
      global diff_time
      global empty
      data = get_jmx(j_url)
      if data is not None and type(data) == dict:
          if 'apps' in  data and type(data['apps']) == dict and 'app' in data['apps'] and type(data['apps']['app']) == list and len(data['apps']['app']) > 0:
               if empty:
                   empty = 0
               c_time = int(time.time())
               if prev_time != 0:
                   diff_time += c_time - prev_time
               s_time = time.strftime("%Y/%m/%d %H:%M:%S",time.gmtime(c_time))
               for app in data['apps']['app']:
                    if expected_queue:
                        if app['queue'] not in expected_queue:
                            continue
                    print_application(app,diff_time, c_time, s_time)
               print >>sys.stderr, '============================================================================================================================================================'
               prev_time = c_time
          else:
             empty = empty + 1
             return
      else:
          empty = empty + 1
          return

def run():
    queue_name = None #options.queues
    if options.queues:
        queue_name = options.queues.split(',') #sys.argv[1].split(',')
    while True:
        get_applications(queue_name) 
        #break
        time.sleep(sleep_time)
        if empty > tolerance:
            break

run()
