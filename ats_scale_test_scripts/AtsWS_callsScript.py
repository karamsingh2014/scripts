#
#
# Copyright  (c) 2011-2012, Hortonworks Inc.  All rights reserved.
#
#
# Except as expressly permitted in a written Agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution or other exploitation of all or any part of the contents
# of this file is strictly prohibited.
#
#
import os, re, time, logging, sys, urllib, subprocess, urllib2
import httplib, ssl
import traceback
from optparse import OptionParser
loglevel = 'INFO'
#log_format = "%(asctime)s|%(name)s|%(levelname)s|%(process)d|%(thread)d|%(threadName)s|%(message)s"
log_format = "[%(filename)s:%(lineno)s:%(levelname)s - %(funcName)20s() ] %(message)s"
logformatter = logging.Formatter(log_format)
#logging.basicConfig(filename=log_file, filemode='a+', level=loglevel, format=config.option.log_format)
console = logging.StreamHandler()
console.setLevel(loglevel)
console.setFormatter(logformatter)
logging.getLogger('').addHandler(console)

logger = logging.getLogger(__name__)
CWD = os.path.dirname(os.path.realpath(__file__))
REC = re.compile(r"(\r\n|\n)$")
LOG_PATH = '' 
num_queries = 0
class HTTPSConnectionV3(httplib.HTTPSConnection):
    def __init__(self, *args, **kwargs):
        httplib.HTTPSConnection.__init__(self, *args, **kwargs)

    def connect(self):
        sock = socket.create_connection((self.host, self.port), self.timeout)
        if self._tunnel_host:
            self.sock = sock
            self._tunnel()
        try:
            self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl.PROTOCOL_TLSv1)
        except ssl.SSLError, e:
            logger.error(e)
            logger.info("Trying SSLv2/3.")
            self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl.PROTOCOL_SSLv23)

class HTTPSHandlerV3(urllib2.HTTPSHandler):
    def https_open(self, req):
        return self.do_open(HTTPSConnectionV3, req)

# install opener
urllib2.install_opener(urllib2.build_opener(HTTPSHandlerV3()))

class YARN:
    ats_addr = ""
    @classmethod
    def getLogsApplicationID(cls, appId, appOwner=None, logoutput=False):
        log_file =  os.path.join(LOG_PATH, appId + '.log')
        if os.path.exists(log_file) and os.path.getsize(log_file):
            f = open(log_file)
            stdout = ''.join(f.readlines())
            f.close()
            stdout = REC.sub("", stdout, 1)
            return 0, stdout
        else:
            return -1, ""
        '''
        cmd = "yarn  logs -applicationId " + appId 
        if appOwner is not None:
            cmd += " -appOwner " + appOwner
        #cmd = cls._decoratedcmd(cmd)
        logger.info("RUNNING: " + cmd)
        stdout = ""
        osenv = os.environ.copy()
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, cwd=CWD, env=osenv)
        while proc.poll() is None:
            stdoutline = proc.stdout.readline()
            if stdoutline:
                stdout += stdoutline
                if logoutput:
                    logger.info(stdoutline.strip())
        remaining = proc.communicate()
        remaining = remaining[0].strip()
        if remaining != "":
            stdout += remaining
            if logoutput:
                for line in remaining.split("\n"):
                    logger.info(line.strip())
        stdout = REC.sub("", stdout, 1)
        return proc.returncode, stdout
        '''

    @classmethod
    def httpRequest(cls, url, headers={}, data='', method='GET', retry=1):
        global num_queries
        retcode = -1
        retdata = None
        retheaders = {}
        num_queries = 1 + num_queries
        #print >>sys.stderr, "Query: %s %s %s %s" % (method, str(url), str(headers), str(data))
        for attempt in range(retry):
            s_time = time.time()
            try:
               opener = urllib2.build_opener(HTTPSHandlerV3())
               urllib2.install_opener(opener)
               request = urllib2.Request(url)
               for hkey, hvalue in headers.items():
                   request.add_header(hkey, hvalue)
               if data != "": request.add_data(data)
               request.get_method = lambda: method
               response = opener.open(request)
               retcode = response.getcode()
               retdata = response.read()
               retheaders = response.headers
               response.close()
               #print >> sys.stderr, "Timetaken for query %s: %s" %(str(url), str(time.time() - s_time))
               break
            except urllib2.URLError, e:
               #print >> sys.stderr, "Timetaken for query %s: %s" %(str(url), str(time.time() - s_time))
               if isinstance(e, urllib2.HTTPError):
                  retcode = e.code
                  retdata = e.msg
                  retheaders = e.headers
                  break
               else:
                  logger.info("Sleep 10 secs due to URL error before retry")
                  time.sleep(10)
        return retcode, retdata, retheaders
          
    @classmethod
    def getJSON(cls,content):
        try:
           import json
           jsoncontent = json.loads(content)
        except ImportError:
           jsoncontent = eval(content.replace('null', 'None').replace('true', 'True').replace('false', 'False'))
        except ValueError:
           jsoncontent = None
        return jsoncontent


    @classmethod
    def parsed_ws_response_result(cls, http_response, use_xml=False, append_xml_root_tag_in_return=True, use_xm2list=False):
        if http_response is not None and len(http_response.strip("\n").strip("\r").strip()) > 0:
           is_html = re.findall('^<html>.*</html>.*$', http_response, re.S)
           if is_html is None:
               return http_response
           if len(is_html) > 0:
               return http_response
           if not use_xml:
              parsed_json = cls.getJSON(http_response)
              if parsed_json is not None and type(parsed_json) == dict:
                 return parsed_json
        return http_response


    @classmethod
    def query_yarn_web_service(cls, ws_url, user, query_headers=None, data=None, use_xml=False, http_method='GET', 
                               also_check_modified_config_for_spnego=False, do_not_use_curl_in_secure_mode=False,
                               use_user_auth_in_un_secure_mode=True, user_delegation_token=None, renew_cancel_delegation_token_use_curl=False,
                               cookie_string=None, use_xm2list=False):
        logger.debug("query_yarn_web_service start Accessing " + ws_url)
        append_xml_root_tag_in_return = True
        yarn_new_api = ['/ws/v1/cluster/apps/new-application', '/ws/v1/cluster/delegation-token', 'http.*/ws/v1/cluster/apps/application_\w+_\w+/state.*']
        is_cookie_present = cookie_string is not None and len(cookie_string) > 0
        ws_header = {'Accept': 'application/json',
                     'Content-Type': 'application/json'}
        if use_xml is True:
           ws_header = {'Accept': 'application/xml',
                        'Content-Type': 'application/xml'}
           if yarn_new_api[0] in ws_url or yarn_new_api[1] in ws_url or re.match(yarn_new_api[2], ws_url) is not None:
              append_xml_root_tag_in_return = False

        if is_cookie_present is True:
           ws_header['Cookie'] = cookie_string
        if data is not None:
           if not use_xml:
              data = json.dumps(data)

        if query_headers is not None:
           for k in query_headers.keys():
               ws_header[k] = query_headers[k]
        if data is not None:
          logger.debug("Using Data: " + str(data))
        logger.info("ws_url: " + ws_url)
        ret_code, ret_data, ret_headers = cls.httpRequest(ws_url, ws_header, data=data, method=http_method, retry=1)
        logger.debug("Url Query response %s %s %s %s" % (ws_url, str(ret_code), str(ret_data), str(ret_headers)))
        return ret_code, cls.parsed_ws_response_result(ret_data, use_xml, append_xml_root_tag_in_return, use_xm2list), ret_headers

    @classmethod
    def get_ats_web_app_address(cls):
        return cls.ats_addr #'http://' + cls.getATSHost() + ':8188'
        
    @classmethod
    def set_ats_web_app_address(cls, addr):
        if addr:
            cls.ats_addr = addr
    
    @classmethod
    def get_ats_json_code_data_headers(cls, url_comp, user, return_exception_json=False, do_not_use_curl_in_secure_mode=False, use_user_auth_in_un_secure_mode=False, user_delegation_token=None,
                                      renew_cancel_delegation_token_use_curl=False, cookie=None, http_method='GET'):
       ws_url = cls.get_ats_web_app_address() + "/ws/v1/timeline"
       if url_comp is not None and len(url_comp.strip()) > 0:
          ws_url += '/' + url_comp
       logger.info("get_ats_json_code_data_headers ws_url = %s" % ws_url)
       return cls.query_yarn_web_service(ws_url, user, None, None, False, http_method, False, do_not_use_curl_in_secure_mode, use_user_auth_in_un_secure_mode, user_delegation_token,
                                             renew_cancel_delegation_token_use_curl, cookie_string=cookie)
    @classmethod
    def get_ats_json_data(cls, url_comp, user, return_exception_json=False, do_not_use_curl_in_secure_mode=False, use_user_auth_in_un_secure_mode=False, user_delegation_token=None, 
                          renew_cancel_delegation_token_use_curl=False, cookie=None, http_method='GET'):
        #`print >>sys.stderr, "get_ats_json_data url_comp = %s" % url_comp
        ret_code, ret_data, ret_headers = cls.get_ats_json_code_data_headers(url_comp, user, return_exception_json, do_not_use_curl_in_secure_mode, 
                                                                             use_user_auth_in_un_secure_mode, user_delegation_token, renew_cancel_delegation_token_use_curl, cookie, http_method)
        if ret_code < 100 or ret_code > 299:
           if ret_data is not None and 'exception' in ret_data:
               print >>sys.stderr, "Got Exception in repsonse JSON %s by user %s for urlcomp %s" %(str(ret_data), user, url_comp)
               if not return_exception_json:
                   ret_data = None
        return ret_data
  
    @classmethod
    def access_ats_ws_path(cls, url_comp, ids, app_id, user, expected_values=1, use_user_auth_in_un_secure_mode=False, delegation_token=None, cookie=None):
        p = cls.get_ats_json_data(url_comp, user, use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode, user_delegation_token=delegation_token, cookie=cookie)
        assert p is not None and type(p) == dict, "Got None/Null  in JSON reponse for user %s and url query comp %s " % (user, url_comp)
        assert 'exception' not in p, 'Got exception %s in reponse for user %s and url query comp %s ' % (str(p), user, url_comp)
        assert 'entities' in p, "entities' is not found in JSON response %s for user %s and url query comp %s" % (str(p), user)
        logger.info("list of entites " + str(len(p['entities'])))
        if len(p['entities']) < expected_values:
           return
        for e in p['entities']:
            if e['entity'] in ids:
               logger.debug("ATS WS " + url_comp + " contains " + e['entity'] + " which is also ran by our application " + app_id)
 

class Tez:
    __vertexId_dagId = {}
    __gStartTime = 0
    __gEndTime = 0

    @classmethod
    def __get_all_types_of_ids_from_tez_app_log__(cls, app_id, owner):
        '''
        Parser application logs for HistoryEventHandler Dag new to inited line in application logs of Tez App
        to find Dag/Vertex/Task/TaskAttempt/AppAttempt/Container Ids
        '''
        if app_id is None:
            return None
        if re.match("application_\d+_\d+", app_id) is None:
            return None
        (exit_code, std_out) = YARN.getLogsApplicationID(app_id, appOwner=owner, logoutput=False)
        if exit_code != 0:
            return None
        if std_out is None:
            return None
        lines = std_out.split('\n')
        if lines is None or len(lines) < 1:
            return None
        result = dict()
        result['TEZ_APPLICATION_ATTEMPT'] = {}
        result['TEZ_DAG_ID'] = {}
        result['TEZ_VERTEX_ID'] = {}
        result['TEZ_TASK_ID'] = {}
        result['TEZ_TASK_ATTEMPT_ID'] = {}
        result['TEZ_CONTAINER_ID'] = {}
        for line in lines:
            if re.search('HistoryEventHandler.*\[Event:AM_LAUNCHED\]', line) is not None:
                (app_attempt_id1, submit_time) = re.findall("appAttemptId=(.*),\s*appSubmitTime=(.*),\s*launchTime=", line)[0]
                result['TEZ_APPLICATION_ATTEMPT']["tez_" + app_attempt_id1] = submit_time
            m = re.search(r"DAGImpl: (.*?) transitioned from NEW to INITED", line)
            if m:
                result['TEZ_DAG_ID'][m.groups()[0]] = 1
            if re.search('HistoryEventHandler.*\[Event:DAG_FINISHED\]', line):
                f = re.findall(",.*?finishTime=(.*),.*?timeTaken=", line)[0]
                if cls.__gEndTime < f:
                    cls.__gEndTime = f
            if re.search('HistoryEventHandler.*\[Event:VERTEX_FINISHED\]', line) is not None:
                (dag_id, vertex_name, vertex_id) = re.findall(
                    "\[DAG:(.*)\]\[Event:VERTEX_FINISHED\]:\s*vertexName=(.*),\s*vertexId=(.*),\s*initRequestedTime=",line)[0]
                if vertex_id not in result['TEZ_VERTEX_ID']:
                    result['TEZ_VERTEX_ID'][vertex_id] = 1
                if vertex_id not in cls.__vertexId_dagId:
                    cls.__vertexId_dagId[vertex_id] = {}
                cls.__vertexId_dagId[vertex_id][dag_id] = vertex_name
            if re.search('HistoryEventHandler.*\[Event:TASK_FINISHED\]', line) is not None:
                (task_id, finish_time) = re.findall("taskId=(.*),\s*startTime=.*finishTime=(.*),\s*timeTaken", line)[0]
                if task_id not in result['TEZ_TASK_ID']:
                    result['TEZ_TASK_ID'][task_id] = finish_time
                else:
                    if finish_time != result['TEZ_TASK_ID'][task_id]:
                        result['TEZ_TASK_ID'][task_id] = finish_time
            if re.search('HistoryEventHandler.*\[Event:TASK_ATTEMPT_FINISHED\]', line) is not None:
                #print >>sys.stderr, "A " + line + "\n";
                (task_attempt_id, finish_time) = re.findall("taskAttemptId=(attempt_\d+_\d+_\d+_\d+_\d+_\d+),.*\s*startTime=.*,\s*finishTime=(.*),\s*timeTaken", line)[0]
                #print >>sys.stderr, "T " + task_attempt_id + "\n";
                if task_attempt_id not in result['TEZ_TASK_ATTEMPT_ID']:
                    result['TEZ_TASK_ATTEMPT_ID'][task_attempt_id] = finish_time
                else:
                    if finish_time != result['TEZ_TASK_ATTEMPT_ID'][task_attempt_id]:
                        result['TEZ_TASK_ATTEMPT_ID'][task_attempt_id] = finish_time
            if re.search('HistoryEventHandler.*\[Event:CONTAINER_LAUNCHED\]', line) is not None:
                (container_id, launch_time) = re.findall("containerId=(.*),\s*launchTime=(.*)", line)[0]
                result['TEZ_CONTAINER_ID']["tez_" + container_id] = launch_time
        return result


    @classmethod
    def __validate_json_related_identities__(cls, entity_type, eid, related_identities, part_uri):
        '''
        Iterates over TEZ_*_IDs in related Identities and validate that each item matches expected pattern
        '''

        if entity_type in ['TEZ_TASK_ATTEMPT_ID', 'TEZ_CONTAINER_ID']:
            assert len(related_identities) == 0, "Expected relatedentities to be empty for %s , but got %s in response %s%s" % (
                eid, str(len(related_identities)), entity_type, part_uri)
            return

        expected_pattern = ''
        related_identity = ''
        if entity_type == "TEZ_DAG_ID":
            expected_pattern = eid.replace("dag", "vertex") + '_\d+'
        elif entity_type == "TEZ_VERTEX_ID":
            expected_pattern = eid.replace("vertex", "task") + '_\d{6}'
            related_identity = 'TEZ_TASK_ID'
        elif entity_type == 'TEZ_TASK_ID':
            expected_pattern = eid.replace("task", "attempt") + '_\d+'
            related_identity = 'TEZ_TASK_ATTEMPT_ID'
        elif entity_type == 'TEZ_APPLICATION_ATTEMPT':
            expected_pattern = eid.replace("appattempt", "container(_e\d+)*")[0:-7] + '_\d+_\d{6}'
            related_identity = 'TEZ_CONTAINER_ID'

        if related_identity in related_identities:
            for i in related_identities[related_identity]:
                logger.info(eid + " contains " + i)
                assert re.match(expected_pattern,i) is not None, \
                    "Got %s which does not match %s, In response from %s%s" % (i, expected_pattern, entity_type, part_uri)
        if entity_type == 'TEZ_APPLICATION_ATTEMPT' and 'TEZ_DAG_ID' in related_identities:
            expected_pattern = eid.replace("tez_appattempt", "dag")[0:-7] + '_\d+'
            for d in related_identities['TEZ_DAG_ID']:
                assert re.match(expected_pattern,d) is not None, "Got %s which does not match %s, In response from %s%s" % (
                    d, expected_pattern, entity_type, part_uri)


    @classmethod
    def __validate_json_primary_filters__(cls, entity_type, app_id, eid, ids_dict, owner, primary_filters, part_uri, user,
                                          use_user_auth_in_un_secure_mode, delegation_token, cookie=None):
        '''
        Validates that user of dag/appattempt is same owner passed in method invocation
        And dn for dag is not Noe and empty string
        Iterates over TEZ_TASK_IDs, TEZ_VERTEX_IDs, TEZ_DAG_IDs in primaryfilters if any is there
        validates task/attempt/vertex id equal to expected ids
        Also access ATS WS API path using primary filters
        '''
        if entity_type == 'TEZ_CONTAINER_ID':
            assert len(primary_filters) >= 0, "Expected primaryfilers is zero-length array/dict but got %s in response of %s%s" % (
                str(len(primary_filters)), entity_type, part_uri)
            if len(primary_filters) > 0:
                assert 'exitStatus' in primary_filters or 'applicationId' in primary_filters
            if 'applicationId' in primary_filters:
                assert app_id in primary_filters['applicationId']
            return
        urls_to_access = []
        if entity_type in ['TEZ_DAG_ID', 'TEZ_APPLICATION_ATTEMPT']:
            part_uri2 = "&windowStart=" + str(cls.__gStartTime) + "&windowEnd=" + str(cls.__gEndTime) + "&limit=200"
            o_user = primary_filters['user'][0]
            assert o_user == owner
            urls_to_access.append("?primaryFilter=user:" + o_user + part_uri2)
            if entity_type == 'TEZ_APPLICATION_ATTEMPT':
                part_uri3 = "?primaryFilter=user:" + o_user + "&secondaryFilter=appSubmitTime:" + ids_dict[eid]
                urls_to_access.append(part_uri3)
            if entity_type == 'TEZ_DAG_ID':
                dn = primary_filters['dagName'][0]
                logger.info(eid + " having dagName " + dn)
                assert dn is not None and len(dn) > 0
                dn = urllib.quote_plus(dn)
                urls_to_access.append("?primaryFilter=dagName:" + dn + part_uri2)
        if entity_type in ['TEZ_VERTEX_ID', 'TEZ_TASK_ID', 'TEZ_TASK_ATTEMPT_ID']:
            dag_id_prefix = app_id.replace("application", "dag")
            dag_id_prefix += '_\d+'
            for d in primary_filters['TEZ_DAG_ID']:
                logger.info(eid + " dagId : " + d)
                assert re.match(dag_id_prefix,d) is not None, "Got DAG Id %s which does not match %s in response of %s%s" % (
                    d, dag_id_prefix, entity_type, part_uri)
                #if not Machine.isWindows():
                part_uri2 = d + "&windowStart=" + str(cls.__gStartTime) + "&windowEnd=" + str(cls.__gEndTime) + "&limit=200"
                urls_to_access.append("?primaryFilter=TEZ_DAG_ID:" + part_uri2)
        if entity_type in ['TEZ_TASK_ID', 'TEZ_TASK_ATTEMPT_ID']:
            vid = ""
            if entity_type == 'TEZ_TASK_ATTEMPT_ID':
                vid = eid.replace("attempt", "task")[0:-2]
                vid = vid.replace("task", "vertex")[0:-7]
            else:
                vid = eid.replace("task", "vertex")[0:-7]
            if 'TEZ_VERTEX_ID' in primary_filters:
                for v in primary_filters['TEZ_VERTEX_ID']:
                    assert v == vid, "Expected vertexId %s whereas got %s in response of %s%s" % (vid, v, entity_type, part_uri)
                    #if not Machine.isWindows():
                    part_uri2 = v + "&windowStart=" + str(cls.__gStartTime) + "&windowEnd=" + str(cls.__gEndTime) + "&limit=200"
                    urls_to_access.append("?primaryFilter=TEZ_VERTEX_ID:" + part_uri2)
                    part_uri3 = "?primaryFilter=TEZ_VERTEX_ID:" + v + "&secondaryFilter=endTime:" + ids_dict[eid]
                    urls_to_access.append(part_uri3)
        if entity_type == 'TEZ_TASK_ATTEMPT_ID' and 'TEZ_TASK_ID' in primary_filters:
            tid = eid.replace("attempt", "task")[0:-2]
            for t in primary_filters['TEZ_TASK_ID']:
                assert t == tid, "Expected taskId %s whereas got %s in response of %s%s" % (tid, t, entity_type, part_uri)
                #if not Machine.isWindows():
                part_uri2 = t + "&windowStart=" + str(cls.__gStartTime) + "&windowEnd=" + str(cls.__gEndTime) + "&limit=200"
                urls_to_access.append("?primaryFilter=TEZ_TASK_ID:" + part_uri2)

        for u in urls_to_access:
            YARN.access_ats_ws_path(entity_type + u, ids_dict, app_id, user,
                                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode, delegation_token=delegation_token,
                                    cookie=cookie)


    @classmethod
    def __validate_ats_ws_json_dag_plan__(cls, dag_plan, url_comp):
        '''
        Validates data contained by dagPlan
        If dagPlan contains vertices, validates that vertexName is not None and processorClass starts with org.apache.tez
        If dagPlan vertices contains outEdgeIds then each outEdgeId is number
        If dagPlan vertices contains inEdgeIds then each inEdgeId is number
        if dagPlan contains edges then validates edgeSourceClass, edgeDestinationClass, dataMovementType, schedulingType, dataSourceType
        Also validates dagPlan edges inputVertexName and outputVertexName is not None
        '''
        if 'vertices' in dag_plan:
            for v in dag_plan['vertices']:
                logger.debug("vertices Processor class : " + v['processorClass'] + " vertices About vertexName : " + str(v['vertexName']))
                assert 'processorClass' in v and v['processorClass'] is not None, \
                    "Expected processorClass in key vertices of DagPlan and processorClass not None in response" + url_comp
                assert 'vertexName' in v and v['vertexName'] is not None, \
                    "Expected vertexName in key vertices of DagPlan and vertexName not None in response" + url_comp
                if 'outEdgeIds' in v:
                    logger.debug("outEdgeIds = " + " ".join(v['outEdgeIds']))
                    for oe in v['outEdgeIds']:
                        assert re.match("^\d+$",oe) is not None, "Expected outEdgeIds as number but got %s in response of %s" % (str(oe),
                                                                                                                                 url_comp)
                if 'inEdgeIds' in v:
                    logger.debug(" inEdgeIds = " + " ".join(v['inEdgeIds']))
                    for ie in v['inEdgeIds']:
                        assert re.match("^\d+$",ie) is not None, "Expected inEdgeIds as number but got %s in response of %s" % (str(ie),
                                                                                                                                url_comp)
        if 'edges' in dag_plan:
            for e in dag_plan['edges']:
                logger.debug("edges edgeId : " + e['edgeId'] + " edges edgeSourceClass : " + e[
                    'edgeSourceClass'] + " edges edgeDestinationClass : " + e['edgeDestinationClass'])
                assert re.match("^\d+$", e['edgeId']) is not None, "Expected edgeIds as number but got %s in response of %s" % (str(e),
                                                                                                                                url_comp)
                assert 'edgeSourceClass' in e and e['edgeSourceClass'] is not None, \
                    "Expected edgeSourceClass in key edges of dagPlan and edgeSourceClass is not None but got %s in response of %s" % (
                        str(e['inputVertexName']), url_comp)
                assert 'edgeDestinationClass' in e and e['edgeDestinationClass'] is not None, \
                    "Expected edgeDestinationClass in key edges of dagPlan and edgeDestinationClass is not None but got %s in response " \
                    "of %s" % (str(e['outputVertexName']), url_comp)

                logger.debug("edges dataMovementType : " + e['dataMovementType'] + " edges schedulingType : " + e['schedulingType'] +
                             " edges dataSourceType : " + e['dataSourceType'])
                assert e['dataMovementType'] in ['ONE_TO_ONE', 'BROADCAST', 'SCATTER_GATHER', 'CUSTOM'], \
                    "Expected dataMovementType in on of 'ONE_TO_ONE,BROADCAST,SCATTER_GATHER,CUSTOME', but got %s in response of %s" % (
                        str(e['dataMovementType']), url_comp)
                assert e['schedulingType'] in ['SEQUENTIAL', 'CONCURRENT'], \
                    "Expected schedulingType in one of 'SEQUENTIAL,CONCURRENT' but got %s in response of %s" % (str(e['schedulingType']),
                                                                                                                url_comp)
                assert e['dataSourceType'] in ['PERSISTED', 'PERSISTED_RELIABLE', 'EPHEMERAL'], \
                    "Expected dataSourceType in one of 'PERSISTED,PERSISTED_RELIABLE,EPHEMERAL' but got %s in response of %s" % (
                        str(e['dataSourceType']), url_comp)
                logger.debug("edges About inputVertex : " + str(e['inputVertexName']) + " edges About outputVertex : " +
                             str(e['outputVertexName']))
                assert 'inputVertexName' in e and e['inputVertexName'] is not None, \
                    "Expected inputVertexName in key edges of dagPlan and inputVertexName is not None but got %s in response of %s" % (
                        str(e['inputVertexName']), url_comp)
                assert 'outputVertexName' in e and e['outputVertexName'] is not None, \
                    "Expected outputVertexName in key edges of dagPlan and outputVertexName is not None but got %s in response of %s" % (
                        str(e['outputVertexName']), url_comp)


    @classmethod
    def __validate_ats_ws_json_vertex_stats__(cls, vtx_id, stats, url_comp):
        '''
        Validates contents of stats of vertex JSON:
        Validation includes : maxTaskDuration, maxTaskDuration, minTaskDuration, avgTaskDuration, lastTaskFinishTime
                             firstTasksToStart, lastTasksToFinish, shortestDurationTasks, longestDurationTasks

        '''
        logger.debug("maxTaskDuration : " + str(stats['maxTaskDuration']) + " minTaskDuration : " + str(stats['minTaskDuration']))
        assert re.match("^-?\d+$", str(stats['maxTaskDuration'])) is not None, \
            "Expected maxTaskDuration as number but got %s in response of %s" % (str(stats['maxTaskDuration']), url_comp)
        assert re.match("^-?\d+$", str(stats['minTaskDuration'])) is not None, \
            "Expected minTaskDuration as number but got %s in response of %s" % (str(stats['minTaskDuration']), url_comp)
        logger.debug("avgTaskDuration : " + str(stats['avgTaskDuration']) + " firstTaskStartTime : " + str(stats['firstTaskStartTime']))
        assert re.match("^-?\d+\.?\d*$", str(stats['avgTaskDuration'])) is not None, \
            "Expected avgTaskDuration as number but got %s in response of %s" % (str(stats['avgTaskDuration']), url_comp)
        assert re.match("^-?\d+$", str(stats['firstTaskStartTime'])) is not None, \
            "Expected firstTaskStartTime as number but got %s in response of %s" % (str(stats['firstTaskStartTime']), url_comp)
        logger.debug("lastTaskFinishTime : " + str(stats['lastTaskFinishTime']))
        assert re.match("^-?\d+$", str(stats['lastTaskFinishTime'])) is not None, \
            "Expected lastTaskFinishTime as number but got %s in response of %s" % (str(stats['lastTaskFinishTime']), url_comp)

        task_id_prefix = vtx_id.replace("vertex", "task")
        other_keys = ['firstTasksToStart', 'lastTasksToFinish', 'shortestDurationTasks', 'longestDurationTasks']
        for k in other_keys:
            if k in stats:
                tasks = stats[k]
                if len(tasks) > 0:
                    for t in tasks:
                        assert re.match(task_id_prefix + "_\d{6}",t) is not None, \
                            'Expected task id like %s_\d{6} but got %s in response %s' % (task_id_prefix, t, url_comp)
      
    
    @classmethod
    def __validate_json_for_ids__(cls, entity_type, app_id, eid, ids_dict, owner, url_comp_to_query=None, fields_to_compare=None,
                                  again=False, other_user=None, json_data=None, assert_fail_on_error=False,
                                  use_user_auth_in_un_secure_mode=False, delegation_token=None, cookie=None):
        '''
        Validates content returned by ws/v1/TEZ_DAG_ID... calls
        By Default it queries Id=${entity_type} ws/v1/${entity_type}/${eid},
        but if url query component=${url_comp_to_query} is also specified the it queries ws/v1/${entity_type}+url_comp_to_query
        By defaults this API compares contents of entitype, event, events, starttime, relatedidentities, primaryfilters and otherinfo
        returned by JSON
        But if fields_to_compare=${fields_to_compare} is specified then it compares fields specified by fields_to_compare
        out of 'entitype, event, events, starttime, relatedidentities, primaryfilters and otherinfo returned  by JSON,
        used when we query specific filters in query

        '''
        #ats_store_class = 'org.apache.hadoop.yarn.server.timeline.EntityFileTimelineStore' #YARN.getConfigValue('yarn.timeline-service.store-class', '')
        #ats_store_class = 'org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore'
        #ats_store_class = 'org.apache.hadoop.yarn.server.timeline.RollingLevelDBTimelineStore'
        #ats_store_class = 'org.apache.hadoop.yarn.server.timeline.EntityFileCacheTimelineStore'
        ats_store_class = 'org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore'
        #ats_store_class2 = Hadoop.getmodifiedConfigValue('yarn-site.xml','yarn.timeline-service.store-class','')
        #if 'org.apache.hadoop.yarn.server.timeline.EntityFileTimelineStore'in [ats_store_class, ats_store_class2]:
        #Temproray workaround for BUG-43520, Which going fixed on 2.3-next
        if 'org.apache.hadoop.yarn.server.timeline.EntityFileTimelineStore' == ats_store_class:
            if entity_type in ['TEZ_APPLICATION_ATTEMPT', 'TEZ_APPLICATION']:
                return True
        #if 'org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore' == ats_store_class:
        #    if entity_type in ['TEZ_CONTAINER_ID']:
        #        return True
        part_uri = "/" + eid
        if url_comp_to_query is not None:
            part_uri = url_comp_to_query
        (entity_type_comp, entity_comp, start_time_comp, events_comp, related_identities_comp, primary_filters_comp, other_info_comp) = (
            True, True, True, True, True, True, True)
        if fields_to_compare is not None:
            if re.search("entityType", fields_to_compare, re.I) is None:
                entity_type_comp = False
            if re.search("entityId", fields_to_compare, re.I) is None:
                entity_comp = False
            if re.search("starttime", fields_to_compare, re.I) is None:
                start_time_comp = False
            if re.search("events", fields_to_compare, re.I) is None:
                events_comp = False
            if re.search("relatedentities", fields_to_compare, re.I) is None:
                related_identities_comp = False
            if re.search("primaryfilters", fields_to_compare, re.I) is None:
                primary_filters_comp = False
            if re.search("otherinfo", fields_to_compare, re.I) is None:
                other_info_comp = False
        user = owner
        if other_user is not None:
            user = other_user
        p = YARN.get_ats_json_data(entity_type + part_uri, user, use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                   user_delegation_token=delegation_token, cookie=cookie)
        if json_data is not None and type(json_data) == dict:
            p = json_data
        len_of_data = 0
        if p is not None and type(p) == dict and 'exception' not in p:
            len_of_data = len(p)
            if 'events' in p and '/events?entityId' in part_uri:
                len_of_data = len(p['events'])
        if p is None:
            if assert_fail_on_error:
                assert p is not None, 'Got empty JSON %s for query %s with user=%s' % (str(p), str(entity_type + part_uri), user)
            else:
                print >>sys.stderr, 'Got empty JSON %s for query %s with user=%s' % (str(p), str(entity_type + part_uri), user)
                return False
        if type(p) != dict:
            if assert_fail_on_error:
                assert type(p) == dict, 'For query %s with user=%s, Response is not in JSON: %s' % (str(entity_type + part_uri), user, str(p))
            else:
                print >>sys.stderr, 'For query: %s with user=%s, Response is not in JSON: %s' % (str(entity_type + part_uri),user, str(p))
                #traceback.print_stack()
                return False

        if 'exception' in p or len_of_data <= 0:
            if assert_fail_on_error:
                assert 'exception' not in p, 'Got exception JSON %s for query %s with user=%s' % (str(p),str(entity_type + part_uri), user)
                assert len_of_data > 0, 'Got exmpty response %s for query %s with user=%s' % (str(p), str(entity_type + part_uri), user)
            else:
                print >>sys.stderr, 'Got empty or exception JSON %s for query %s with user=%s' % (str(p), str(entity_type + part_uri),user)
                #traceback.print_stack()
                return False
        if entity_type_comp:
            '''
             Validates entitytype got JSON equal entityType specified during method call
            '''
            e_type = []
            if again:
                for e in p['events']:
                    e_type.append(e['entitytype'])
            else:
                e_type.append(p['entitytype'])
            logger.info(eid + " entityType = " + str(e_type))
            assert len(e_type) > 0, 'Got empty entitytype in repsonse of %s%s' % (entity_type, part_uri)
            found = False
            for etp in e_type:
                if etp == entity_type:
                    found = True
                    break
            assert found is True, "Expected entitytype for '%s' but got '%s' in response of %s%s" % (entity_type, str(e_type), entity_type,
                                                                                                     part_uri)
        if entity_comp:
            '''
            Validates entity == eid specified during method call
            '''
            ety = []
            if again:
                for et in p['events']:
                    ety.append(et['entity'])
            else:
                ety.append(p['entity'])
            logger.info(eid + " entity = " + str(ety))
            assert len(ety) > 0, 'Got empty entity in response of %s%s' % (entity_type, part_uri)
            found = False
            for ey in ety:
                if json_data is not None:
                    m = eid[:-3]
                    if re.search(m, ey) is not None:
                        logger.info("Using regex search instead of pattern equal to " + m + " " + str(re.search(m, ey)))
                        found = True
                        break
                if eid == ey:
                    found = True
                    break
            assert found is True, "Expected entity '%s' but got '%s' in response of %s%s" % (eid, str(ety), entity_type, part_uri)
        if start_time_comp:
            logger.info(eid + " starttime = " + str(p['starttime']))
            assert re.match("^\d+$", str(p['starttime'])) is not None, "Expected number but got %s in response of %s%s " % (
                str(p['starttime']), entity_type, part_uri)
        if events_comp:
            '''
            Validates events returned by JSON contains either of expected events
            and event time stamp is integer value
            '''
            events = []
            if again:
                for ev in p['events']:
                    events.append(ev['events'])
                    logger.info(eid + " Event found " + str(len(ev['events'])))
                    assert len(ev['events']) > 0
            else:
                events.append(p['events'])
                logger.info(eid + " Event found " + str(len(events[0])))
                assert len(events[0]) > 0
            expected_event_types = ['AM_STARTED', 'AM_LAUNCHED']
            if entity_type == 'TEZ_DAG_ID':
                expected_event_types = ['DAG_SUBMITTED', 'DAG_INITIALIZED', 'DAG_STARTED', 'DAG_FINISHED', 'DAG_RECOVERED']
            elif entity_type == 'TEZ_VERTEX_ID':
                expected_event_types = ['VERTEX_INITIALIZED', 'VERTEX_STARTED', 'VERTEX_FINISHED']
            elif entity_type == 'TEZ_TASK_ID':
                expected_event_types = ['TASK_STARTED', 'TASK_FINISHED']
            elif entity_type == 'TEZ_TASK_ATTEMPT_ID':
                expected_event_types = ['TASK_ATTEMPT_STARTED', 'TASK_ATTEMPT_FINISHED']
            elif entity_type == 'TEZ_CONTAINER_ID':
                expected_event_types = ['CONTAINER_LAUNCHED', 'CONTAINER_STOPPED']
            for evs in events:
                for i in evs:
                    logger.info(eid + " EventType " + i['eventtype'])
                    assert i['eventtype'] in expected_event_types is not None, \
                        "Got event %s where as expected is one of from %s in reponse %s%s" % (str(i['eventtype']),
                                                                                              str(expected_event_types), entity_type,
                                                                                              part_uri)
                    logger.info(eid + " Event Type: " + i['eventtype'] + " time: " + str(i['timestamp']))
                    assert re.match("^\d+$", str(i['timestamp'])) is not None, "Expected number but got %s in response of %s%s" % (
                        str(i['timestamp']), entity_type, part_uri)
        if related_identities_comp:
            if 'relatedentities' in p:
                cls.__validate_json_related_identities__(entity_type, eid, p['relatedentities'], part_uri)

        if primary_filters_comp:
            if 'primaryfilters' in p:
                cls.__validate_json_primary_filters__(entity_type, app_id, eid, ids_dict, owner, p['primaryfilters'], part_uri,
                                                      user, use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                                      delegation_token=delegation_token, cookie=cookie)

        if not other_info_comp:
            return True

        '''
        Validates otherinfo retuned by JSON response
        Validation includes timeTaken, status, startTime and endTime
        If entityType is TEZ_DAG_ID or TEZ_VERTEX_ID then also validates  initTime
        If enityType is TEZ_DAG_ID then validation inclu desapplicationId and existance of dagPlan calls DagPlan validotr method
        If entityType is TEZ_VETEX_ID then Validation includes startRequestedTime, startRequestedTime, initRequestedTime,
        vertexName, numTasks, processorClassName and then calls the StatsValidator
        If entityType is TEZ_APPLICATION_ATTEMPT then valides appSubmitTime
        And if entityType is TEZ_CONTAINER_ID then validates that otherinfo is empty array

        '''
        if entity_type == 'TEZ_CONTAINER_ID':
            return True
            othic = p['otherinfo']
            if 'endTime' in othic:
                assert re.match("^\d+$", str(othic['endTime'])) is not None, \
                    "Expected Number as endTime value but got %s in response of %s%s" % (str(othic['endTime']), entity_type, part_uri)
            else:
                print >>sys.stderr, "In JSON Response for %s%s key='otherinfo' does not contains endTime" % (entity_type, part_uri)
            if 'exitStatus' in othic:
                assert re.match("^-?\d+$", str(othic['exitStatus'])) is not None, \
                    "Expected Number as exitStatus value but got %s in response of %s%s" % (str(othic['exitStatus']), entity_type, part_uri)
            else:
                print >>sys.stderr, "In JSON Response for %s%s key='otherinfo' does not contains exitStatus" % (entity_type, part_uri)
            return True

        othi = p['otherinfo']
        # logger.info(Id + "timeTaken = " + str(othi['timeTaken']) + " initTime = " + str(othi['initTime']))
        if entity_type in ['TEZ_DAG_ID', 'TEZ_VERTEX_ID', 'TEZ_TASK_ID', 'TEZ_TASK_ATTEMPT_ID']:
            if 'timeTaken' in othi:
                assert re.match("^\d+$", str(othi['timeTaken'])) is not None, \
                    "Expected Number as timeTaken value but got %s in response of %s%s" % (str(othi['timeTaken']), entity_type, part_uri)
            else:
                print >>sys.stderr, "In JSON Response for %s%s key='otherinfo' does not contains timeTaken" % (entity_type, part_uri)
            if 'endTime' in othi:
                assert re.match("^\d+$", str(othi['endTime'])) is not None, \
                    "Expected Number as endTime value but got %s in response of %s%s" % (str(othi['endTime']), entity_type, part_uri)
            else:
                print >>sys.stderr, "In JSON Response for %s%s key='otherinfo' does not contains endTime" % (entity_type, part_uri)
            if 'startTime' in othi:
                assert re.match("^\d+$", str(othi['startTime'])) is not None, \
                    "Expected Number as startTime value but got %s in response of %s%s" % (str(othi['startTime']), entity_type, part_uri)
            else:
                print >>sys.stderr, "In JSON Response for %s%s key='otherinfo' does not contains startTime" % (entity_type, part_uri)
            if 'status' in othi:
                assert othi['status'] in ['SCHEDULED', 'RUNNING', 'SUCCEEDED', 'KILLED', 'FAILED'], \
                    "Expected either 'SCHEDULED,RUNNING,SUCCEEDED,KILLED,FAILED' as status value but got %s in response of %s%s" % (str(othi['status']),
                                                                                                                                    entity_type, part_uri)
            else:
                print >>sys.stderr, "In JSON Response for %s%s key='otherinfo' does not contains status" % (entity_type, part_uri)

            if 'counters' in othi and 'counterGroups' in othi['counters'] and len(othi['counters']['counterGroups']) > 0:
                for counterGroup in othi['counters']['counterGroups']:
                    if 'counters' in counterGroup and len(counterGroup['counters']) > 0:
                        for counter in counterGroup['counters']:
                            if 'counterName' in counter and 'counterValue' in counter:
                                logger.debug("counter name" + str(counter['counterName']) + " value " + str(
                                    counter['counterValue']))
                                if counter['counterValue'] is not None and len(str(counter['counterValue'])) > 0:
                                    assert re.match("^-?\d+$", str(counter[
                                        'counterValue'])) is not None, \
                                        "Expected counter value to be number, but got %s for counterName %s in reponse JSON of %s%s" % (
                                            str(counter['counterValue']), str(counter['counterName']), entity_type, part_uri)
                                else:
                                    print >>sys.stderr, str(eid) + " Got counter value None or empty for counterName: " + str(counter['counterName']) + " value :" + str(counter['counterValue'])
                            else:
                                print >>sys.stderr, str(eid) + " Either counterName and counterValue is not there " + str(counter)

            if entity_type == 'TEZ_DAG_ID' or entity_type == 'TEZ_VERTEX_ID':
                if 'initTime' in othi:
                    assert re.match("^\d+$", str(
                        othi['initTime'])) is not None, "Expected initTime as number but got %s in response of %s%s" % (
                        str(othi['initTime']), entity_type, part_uri)
                else:
                    print >>sys.stderr, "In JSON Response for %s%s key='otherinfo' does not contains initTime" % (entity_type, part_uri)

        if entity_type == 'TEZ_DAG_ID':
            assert othi['applicationId'] == app_id, "Expected applicdation id %s, but got %s in response of %s%s" % (
                app_id, str(othi['applicationId']), entity_type, part_uri)
            assert 'dagPlan' in othi
            cls.__validate_ats_ws_json_dag_plan__(othi['dagPlan'], entity_type + part_uri)

        if entity_type == 'TEZ_TVERTEX_ID':
            logger.debug("startRequestedTime = " + str(othi['startRequestedTime']) + " initRequestedTime  = " +
                         str(othi['initRequestedTime']))
            assert re.match("^\d+$", str(othi['startRequestedTime'])) is not None, \
                "Expected startRequestedTime as number but got %s in response of %s%s" % (str(othi['startRequestedTime']), entity_type,
                                                                                          part_uri)
            assert re.match("^\d+$", str(othi['initRequestedTime'])) is not None, \
                "Expected initTime as number but got %s in response of %s%s" % (str(othi['initRequestedTime']), entity_type, part_uri)
            logger.debug("vertexName = " + othi['vertexName'] + " numTasks = " + str(othi['numTasks']) + " processorClassName = " +
                         othi['processorClassName'])
            assert othi['vertexName'] is not None and len(othi['vertexName']) > 0, \
                "Expected vextexName not None and non-zero length string, but got %s in response of %s%s" % (str(othi['vertexName']),
                                                                                                             entity_type, part_uri)
            assert re.match("^\d+$", str(othi['numTasks'])) is not None, "Expected numTasks as number but got %s in response of %s%s" % (
                str(othi['numTasks']), entity_type, part_uri)
            assert re.match("^org\.apache\.tez\..*$", othi['processorClassName']) is not None, \
                "Expected processorClass containing org.apache.tez but got %s in response of %s%s" % (str(othi['processorClass']),
                                                                                                      entity_type, part_uri)
            if 'stats' in othi:
                cls.__validate_ats_ws_json_vertex_stats__(eid, othi['stats'], entity_type + part_uri)

        if entity_type == 'TEZ_APPLICATION_ATTEMPT':
            if 'appSubmitTime' in othi:
                logger.debug(str(othi['appSubmitTime']))
                assert re.match("^\d+$", str(othi['appSubmitTime'])) is not None, \
                    "Expected appSubmitTime as number but got %s in response of %s%s" % (str(othi['appSubmitTime']), entity_type, part_uri)
            else:
                print >>sys.stderr, "'appSubmitTime' does exists in reponse of %s%s" % (entity_type, part_uri)

        return True


    @classmethod
    def __validate_entity_type_ws_api__(cls, entity_type, app_id, ids_dict, owner, other_user=None,
                                        use_user_auth_in_un_secure_mode=False, privileged_users=None, delegation_token=None, cookie=None):
        '''
        Access entityType=${entity_type} with different IDs and different queries e.g fields, event and calls Ids JSON validator
        method. Also accesses entityType for DAG/VERTEX/APPLICATION_ATTEMPT with parameters such fromId, fromTs and primary,
        secondaryfilters
        '''
        user = owner
        all_privileged_users = [owner]
        if privileged_users is not None and type(privileged_users) == list:
            for u in privileged_users:
                all_privileged_users.append(u)
        assert_fail_on_error = False
        if other_user is not None:
            user = other_user
        if entity_type == 'TEZ_DAG_ID' or entity_type == 'TEZ_APPLICATION_ATTEMPT':
            if user in all_privileged_users:
                assert_fail_on_error = True

        fields_array = ['events', 'otherinfo', 'primaryfilters', 'otherinfo,primaryfilters',
                        'otherinfo,relatedentities', 'otherinfo,primaryfilters,relatedentities',
                        'otherinfo,primaryfilters', 'relatedentities,otherinfo']

        count = 0
        for Id in ids_dict.keys():
            if count >= 10:
                break
            #print >>sys.stderr, "XXXXXXX %s i=%s c=%s " % (str(entity_type), str(Id), str(ids_dict[Id]))
            r = cls.__validate_json_for_ids__(entity_type, app_id, Id, ids_dict, owner, other_user=user,
                                              assert_fail_on_error=assert_fail_on_error,
                                              use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                              delegation_token=delegation_token, cookie=cookie)
            count += 1
            if other_user is not None and user not in all_privileged_users:
                assert not r, "Expected call failing where it has passed"
            if r is True and user is all_privileged_users:
                continue

            if entity_type in ['TEZ_DAG_ID', 'TEZ_VERTEX_ID', 'TEZ_APPLICATION_ATTEMPT']:
                YARN.access_ats_ws_path(entity_type + "?fromId=" + Id, ids_dict, app_id, user,
                                        use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode, cookie=cookie)
                if entity_type == 'TEZ_VERTEX_ID':
                    for d in cls.__vertexId_dagId[Id].keys():
                        pu = "?primaryFilter=TEZ_DAG_ID:" + d + "&secondaryFilter=vertexName:" + \
                             urllib.quote_plus(cls.__vertexId_dagId[Id][d]) + "&windowStart=" + str(cls.__gStartTime) + \
                             "&windowEnd=" + str(cls.__gEndTime) + "&limit=200"
                        p = YARN.get_ats_json_data("TEZ_VERTEX_ID" + pu, user,
                                                   use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                                   user_delegation_token=delegation_token, cookie=cookie)
                        if p is not None and type(p) == dict and 'entities' in p and len(p['entities']) > 0:
                            for jd in p['entities']:
                                fields_to_compare = 'entitytype,events,entityId,starttime,otherinfo,primaryfilter'
                                cls.__validate_json_for_ids__(entity_type, app_id, Id, ids_dict, owner, url_comp_to_query=pu,
                                                              fields_to_compare=fields_to_compare, other_user=user, json_data=jd,
                                                              use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                                              delegation_token=delegation_token, cookie=cookie)
                u_comp = "/" + Id + "?fields="
                for f in fields_array:
                    cls.__validate_json_for_ids__(entity_type, app_id, Id, ids_dict, owner, url_comp_to_query=u_comp + f,
                                                  fields_to_compare=f, other_user=user,
                                                  use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                                  delegation_token=delegation_token, cookie=cookie)
                u_comp = '/events?entityId=' + Id
                cls.__validate_json_for_ids__(entity_type, app_id, Id, ids_dict, owner, url_comp_to_query=u_comp,
                                              fields_to_compare='events,entityType,entityId', again=True, other_user=user,
                                              use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                              delegation_token=delegation_token, cookie=cookie)
        #Due to BUG-44902, if yarn.timeline-service.store-class=org.apache.hadoop.yarn.server.timeline.RollingLevelDBTimelineStore then fromTs is not
        #supported in WebService Qurey in Timelineserver, so skipping fromTs if yarn.timeline-service.store-class is not equal to LeveldbTimelineStore
        #or EntityFileTimelineStore
        #timeline_store = 'org.apache.hadoop.yarn.server.timeline.EntityFileTimelineStore' #YARN.getConfigValue('yarn.timeline-service.store-class', '')
        #timeline_store = 'org.apache.hadoop.yarn.server.timeline.RollingLevelDBTimelineStore'
        #timeline_store = 'org.apache.hadoop.yarn.server.timeline.EntityFileCacheTimelineStore'
        #timeline_store = 'org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore'
        timeline_store = 'org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore'
        timeline_summary_store = 'org.apache.hadoop.yarn.server.timeline.RollingLevelDBTimelineStore'
        accepted_timeline_summary_store = ['org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore']
        accepted_store_classes = ['org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore', 'org.apache.hadoop.yarn.server.timeline.EntityFileCacheTimelineStore',
                                  'org.apache.hadoop.yarn.server.timeline.EntityFileTimelineStore', 'org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore']
        use_fromts = True
        if timeline_store not in accepted_store_classes:
            use_fromts = False
        if timeline_store == 'org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore':
            if timeline_summary_store not in accepted_timeline_summary_store:
                use_fromts = False
        if entity_type in ['TEZ_DAG_ID', 'TEZ_VERTEX_ID', 'TEZ_APPLICATION_ATTEMPT']:
            YARN.access_ats_ws_path(entity_type, ids_dict, app_id, user, use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                    delegation_token=delegation_token, cookie=cookie)
            if entity_type == 'TEZ_DAG_ID':
                from_ts_str = '?fromTs=' + cls.__gEndTime
                if not use_fromts:
                    from_ts_str = ''
                YARN.access_ats_ws_path(entity_type + from_ts_str, ids_dict, app_id, user,
                                        use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode, delegation_token=delegation_token,
                                        cookie=cookie)
                YARN.access_ats_ws_path(entity_type + 'fields=relatedentities,otherinfo&secondaryFilter=applicationId:' + app_id,
                                        ids_dict, app_id, user, use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                        delegation_token=delegation_token, cookie=cookie)
            elif entity_type in ['TEZ_VERTEX_ID', 'TEZ_APPLICATION_ATTEMPT']:
                part_uri = entity_type + '?windowStart=' + str(cls.__gStartTime) + '&limit=200'
                YARN.access_ats_ws_path(part_uri, ids_dict, app_id, user, use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                        delegation_token=delegation_token, cookie=cookie)
                part_uri = entity_type + '?windowEnd=' + str(cls.__gEndTime) + '&limit=200'
                YARN.access_ats_ws_path(part_uri, ids_dict, app_id, user, use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                        delegation_token=delegation_token, cookie=cookie)
                part_uri = entity_type + '?windowStart=' + str(cls.__gStartTime) + '&windowEnd=' + str(cls.__gEndTime) + '&limit=200'
                YARN.access_ats_ws_path(part_uri, ids_dict, app_id, user, use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                        delegation_token=delegation_token, cookie=cookie)
                if use_fromts:
                    part_uri = entity_type + '?fromTs=' + str(cls.__gEndTime) + '&limit=200'
                else:
                    part_uri = entity_type + '?limit=200'
                YARN.access_ats_ws_path(part_uri, ids_dict, app_id, user, use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                        delegation_token=delegation_token, cookie=cookie)
        elif entity_type in ['TASK_TASK_ID', 'TASK_TASK_ATTEMPT_ID', 'TEZ_CONTAINER_ID']:
            YARN.access_ats_ws_path(entity_type + '?limit=1000', ids_dict, app_id, user,
                                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode, delegation_token=delegation_token,
                                    cookie=cookie)
            part_uri = entity_type + '?windowStart=' + str(cls.__gStartTime) + '&windowEnd=' + str(cls.__gEndTime) + '&limit=200'
            YARN.access_ats_ws_path(part_uri, ids_dict, app_id, user, use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                    delegation_token=delegation_token, cookie=cookie)
        
    @classmethod
    def validate_ws_api(cls, app_id, owner, other_user=None, validate_only_with_other_user=False,
                        use_user_auth_in_un_secure_mode=False, privileged_users=None, delegation_token=None, cookie=None,
                        all_ids_dict=None):
        '''
        Package public method.
        Takes application Id and owner (user who submitted application).
        Get Application logs and parse it for getting DAG/VERTEX/TASK/TASK_ATTEMPT/APPLICTIO_ATTEMPT/CONTAINER IDs
        call WS JSON validators form type of IDs
        '''
        assert app_id is not None, "Got None as application Id"
        app_id = app_id.strip('\r').strip('\n').strip()
        print >>sys.stderr, "Application "+ app_id
        #if not Hadoop.isHadoop2() and not Hadoop.isTez():
        #    return None
        if type(all_ids_dict) != dict or len(all_ids_dict.keys()) <= 0:
            all_ids_dict = cls.__get_all_types_of_ids_from_tez_app_log__(app_id, owner)
        else:
            cls.__gStartTime = all_ids_dict['GLOBAL_START_TIME']
            cls.__gEndTime = all_ids_dict['GLOBAL_END_TIME']
        if type(all_ids_dict) != dict or len(all_ids_dict.keys()) <= 0:
            return None
        cls.__gStartTime = str(cls.__gStartTime)
        cls.__gEndTime = str(cls.__gEndTime)
        for ids_type in all_ids_dict.keys():
            if ids_type in ['GLOBAL_START_TIME', 'GLOBAL_END_TIME']:
                continue
            if other_user is not None and validate_only_with_other_user is True:
                cls.__validate_entity_type_ws_api__(ids_type, app_id, all_ids_dict[ids_type], owner, other_user,
                                                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                                    privileged_users=privileged_users, delegation_token=delegation_token, cookie=cookie)
                continue
            cls.__validate_entity_type_ws_api__(ids_type, app_id, all_ids_dict[ids_type], owner,
                                                use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                                privileged_users=privileged_users, delegation_token=delegation_token, cookie=cookie)
            if other_user is not None:
                cls.__validate_entity_type_ws_api__(ids_type, app_id, all_ids_dict[ids_type], owner, other_user,
                                                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                                    privileged_users=privileged_users, delegation_token=delegation_token, cookie=cookie)
        all_ids_dict['GLOBAL_START_TIME'] = str(cls.__gStartTime)
        all_ids_dict['GLOBAL_END_TIME'] = str(cls.__gEndTime)
        return all_ids_dict

def main():
    global LOG_PATH
    parser = OptionParser()
    parser.add_option("-a", "--ats-webapp-address", dest="ats_addr", action='store', type="string", help="[REQUIRED] ATS/Timeline Server Web UI address e.g. http://host:port/ws/v1/timeline")
    parser.add_option("-u", "--user", dest="user", action='store', type="string", default="ksingh", help="Application owner user")
    parser.add_option("-l", "--app-log-dir", dest="log_dir", action='store', type="string", help="[REQUIRED] Application logs containing direcory on local fs")
    (options, args) = parser.parse_args()
    if not options.ats_addr:
        print >>sys.stderr, "Required Option -a/--ats-webapp-address not provided"
        sys.exit(1)
    YARN.set_ats_web_app_address(options.ats_addr)
    if not options.log_dir:
        print >>sys.stderr, "Required Option -l/--log-dir not provided"
        sys.exit(1)
    LOG_PATH = options.log_dir
    #print len(sys.argv)
    if len(args) >= 1 and args[0] is not None:
        start_time = time.time()
        try:
            Tez.validate_ws_api(args[0], options.user)
        finally: 
            print >>sys.stderr, "QUERIES ran: %d , TimeTaken: %s" % (num_queries, str(time.time() - start_time)) 
            

if __name__ == "__main__":
    main()
