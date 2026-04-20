import requests, json
session = requests.Session()
resp = session.post('http://localhost:8088/api/v1/security/login',
    json={'username':'admin','password':'admin','provider':'db','refresh':True})
token = resp.json()['access_token']
session.headers.update({'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'})

# Check dataset list endpoint for cache_timeout
resp2 = session.get('http://localhost:8088/api/v1/dataset/')
print('Status:', resp2.status_code)
data = resp2.json().get('result', [])
for d in data:
    print(f"  {d.get('table_name')}: cache_timeout={d.get('cache_timeout')}, id={d.get('id')}")

# Try individual dataset endpoint
print('\nIndividual dataset GET:')
resp3 = session.get('http://localhost:8088/api/v1/dataset/1')
print('Status:', resp3.status_code)
if resp3.status_code == 200:
    result = resp3.json().get('result', {})
    print('cache_timeout:', result.get('cache_timeout'))
else:
    print('Error:', resp3.text[:200])
