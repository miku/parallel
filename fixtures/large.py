import json
for i in range(5000000):
    print(json.dumps({"name": "name-%s" % i, "id": i}))
