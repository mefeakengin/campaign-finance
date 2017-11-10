import os
import json

PROXY_FILE = "code/proxy.json"

if os.path.exists(PROXY_FILE):
    with open(PROXY_FILE, 'r') as f:
        data = json.loads(f.read())
        private_ip_addresses = data.get("private_ip_addresses")
else:
    data = {}
    private_ip_addresses = []

index = 0
disabled = {}

def get_timer():
    if os.path.exists(PROXY_FILE):
        return 4.0/len(private_ip_addresses)
    else:
        return 1

def get_proxies():
    global index

    if os.path.exists(PROXY_FILE):

        done = False
        while not done:
            if len(disabled.keys()) == len(private_ip_addresses):
                print "[proxies] all exhausted"
            private_ip_address = private_ip_addresses[index]

            if index+1 == len(private_ip_addresses):
                index = 0
            else:
                index += 1
            http = 'http://{0}:8888'.format(private_ip_address)
            if http not in disabled:
                done = True
                return {'http': http}

    else:

        return {}

def remove_proxy(proxies):
    http = proxies.get('http')
    disabled[http] = True
