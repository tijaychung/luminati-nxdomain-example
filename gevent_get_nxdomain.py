from sql import *
from datetime import date
from config import *

import gevent.monkey
gevent.monkey.patch_socket()
from gevent.pool import Pool
import requests
import time
import re
import base64
import json
import os

POOL_SIZE= 50
zproxy_ips = getSPList()
session_list = {}
ip_cidlist = []

def fetch(scraper_id):
    print ("scraper_id: %d joined" % scraper_id)
    turn = 0
    SESSION_COUNTRY = 100
    country_list = getCountryList(100)
    while(True):
        my_zproxy_ips = list(filter(lambda v: (v[0] % POOL_SIZE) == scraper_id, zproxy_ips))
        zproxy_id, zproxy_ip =  my_zproxy_ips[ int((turn / 10 ) % len(my_zproxy_ips)) ]
        session_list[zproxy_id] = session_list.get( zproxy_id, 0 )
        session = session_list[zproxy_id]
        country = country_list[ int((turn / SESSION_COUNTRY) % len(country_list)) ]
        
        subdomain = "%s%s%s%03d%s" % (country, 'NX', nounce, zproxy_id, session) 
        #print zproxy_id, zproxy_ip, subdomain, session

        auth = 'AUTH_INFORMATION_PROVIDED_BY_LUMINATI-country-%s-session-%s:AUTH_KEY'  % ( country, session )
        url = "http://%s.gowritepaper.com" % subdomain
        t_msec_req = "%lf" % time.time()
       
        try:
            r = requests.get(url, timeout = 10, verify=False,
                proxies = {'http': 'http://'+auth+'@%s:22225' % zproxy_ip},
                headers = {'Proxy-Authorization': 'Basic '+base64.b64encode(auth.encode('utf-8')).decode('utf-8')})

            if(r.status_code == 200):
                header = r.raw._original_response.getheaders()
                ex_ip = re.findall(r"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+", str(header))[-1]
                cid = re.findall(r"cp[0-9]+", str(header))[-1]
                latency = re.findall(r"[0-9]+ms", str(header))[-1]
                t_msec_resp = "%lf" % time.time()
                

                result = {'t_msec_req': t_msec_req,
                        'subdomain': subdomain,
                        't_msec_resp': t_msec_resp,
                        'country': country,
                        'ex_ip': ex_ip,
                        'cid': cid,
                        'latency': latency,
                        'resp_header':str(header)}

                month = date.today().month
                day = date.today().day
                fname = "%02d%02d" % (month, day)

                if(not os.path.exists(os.path.join("nxdomain", fname))):
                    os.mkdir(os.path.join("nxdomain", fname))
                    
                if(not os.path.exists(os.path.join("nxdomain", fname, country))):
                    os.mkdir(os.path.join("nxdomain", fname, country))

                if(r.text == ""):
                    w = open('nxdomain/%s/%s/emptylist.tsv' % (fname, country) ,'a')
                    w.write('%s\n' % subdomain)
                    w.close()
                else:
                    w = open('nxdomain/%s/%s/%s' % (fname, country, subdomain) ,'w')
                    w.write(r.text)
                    w.close()
                log2DB('nxdomain_requester', result)
                '''
                ip_cidlist.append( (ex_ip, cid ))
                '''
        except:
            pass
    
        session_list[zproxy_id] += 1
        turn += 1  

if __name__ == "__main__":
    pool = Pool(POOL_SIZE)
    for scraper_id in range(0, POOL_SIZE):
        pool.spawn(fetch, scraper_id)
    pool.join()

