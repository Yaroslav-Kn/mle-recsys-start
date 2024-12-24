import requests

events_store_url = "http://127.0.0.1:8020"

user_id = 1127794
for item_id in [18734992, 18734992, 7785, 4731479]:
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    params = {"user_id": user_id, "item_id": item_id}

    resp = requests.post(events_store_url + "/put", headers=headers, params=params)
    if resp.status_code == 200:
        result = resp.json()
    else:
        result = None
        print(f"status code: {resp.status_code}")
    
    print(result) 


headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
params = {"user_id": user_id, "k": 3}

resp = requests.post(events_store_url + "/get", headers=headers, params=params)
if resp.status_code == 200:
    result = resp.json()
else:
    result = None
    print(f"status code: {resp.status_code}")
    
print(result)