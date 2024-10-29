'''

this file is used to generate the basic config.json file for the test.

it can generate the file whith specified node number and stream number.

'''

import json

def generate_config(node_number, stream_number, group_id="666"):
    config = {
        "node_number": node_number,
        "stream_number": stream_number,
        "group_id": group_id,
        "name": "node",
        "node_configs": []
    }
    
    for i in range(0, node_number):
        node_config = {
            "name": f"node{i}",
            "addresses": [f"127.0.0.1:{8010+(i*10)+j}" for j in range(stream_number)],
            "priority": i
        }
        config["node_configs"].append(node_config)

    for i in range(0, node_number):
        config["name"] = f"node{i}"
        with open(f"{node_number}node{stream_number}stream{i}.json", "w") as f:
            json.dump(config, f, indent=4)
        
    return config

if __name__ == "__main__":
    node_number = int(input("Enter the node number: "))
    stream_number = int(input("Enter the stream number: "))

    config = generate_config(node_number, stream_number)

    print("Configs has been saved")
