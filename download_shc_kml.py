import os
import json
import requests
from tqdm.auto import tqdm
from itertools import chain
from utils.get_layer_info import get_layer_info
from utils.fetch_features import fetch_kml, parse_kml

DATA_DIR = "H:\shc_data"
SUB_DIR = "KML_FILES"
ROOT_DATA_PATH = os.path.join(DATA_DIR, SUB_DIR)
os.makedirs(ROOT_DATA_PATH, exist_ok=True)

def get_states_json():
    """
    Fetches the list of states from the Soil Health Card API and saves the response to a JSON file.
    """
    URL = "https://soilhealth4.dac.gov.in/"
    payload = {
        "operationName": "GetState",
        "variables": {},
        "query": "query GetState($getStateId: String, $code: String) {\n  getState(id: $getStateId, code: $code)\n}",
    }
    response = requests.post(URL, json=payload)
    # Ensure the response is successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Save the JSON to a file
        with open(os.path.join(ROOT_DATA_PATH, "getStates.json"), "w") as file:
            json.dump(data, file, indent=4)

        print("Response saved to getStates.json")
    else:
        print("Request failed with status code:", response.status_code)


def get_districts_by_state_json(state_hash, state_name):
    """
    Fetches the list of districts for a given state from the Soil Health Card API and saves the response to a JSON file.

    Args:
        state_hash (str): The unique identifier for the state.
        state_name (str): The name of the state.
    """
    URL = "https://soilhealth4.dac.gov.in/"
    payload = {
        "operationName": "GetdistrictAndSubdistrictBystate",
        "variables": {"state": f"{state_hash}"},
        "query": "query GetdistrictAndSubdistrictBystate($getdistrictAndSubdistrictBystateId: String, $name: String, $state: ID, $subdistrict: Boolean, $code: String, $aspirationaldistrict: Boolean) {\n  getdistrictAndSubdistrictBystate(\n    id: $getdistrictAndSubdistrictBystateId\n    name: $name\n    state: $state\n    subdistrict: $subdistrict\n    code: $code\n    aspirationaldistrict: $aspirationaldistrict\n  )\n}",
    }

    response = requests.post(URL, json=payload)
    # Ensure the response is successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        file_path = os.path.join(ROOT_DATA_PATH, "states", state_name, "getDistricts.json")

        # Save the JSON to a file
        with open(file_path, "w") as file:
            json.dump(
                data, file, indent=4
            )  # `indent=4` formats the JSON for readability

        print(f"Response saved to {file_path}")
    else:
        print("Request failed with status code:", response.status_code)


def getMetaFiles(st, dt=None):
    """
    Fetches metadata files for states and districts from the Soil Health Card API, and saves the JSON responses to files

    Args:
        st (str): The name of the state to fetch data for. If None, fetches for all states.
        dt (str, optional): The name of the district to fetch data for. If None, fetches for all districts.
    """
    STATES_PATH = os.path.join(ROOT_DATA_PATH, "states")
    os.makedirs(STATES_PATH, exist_ok=True)
    
    # check if getStates.json already exists in directory or not
    if os.path.exists(os.path.join(ROOT_DATA_PATH, "getStates.json")):
        print("\ngetStates.json already exists")
    else:
        # if not then call get_states_json() to get the json data
        get_states_json()

    # if exists then load the json data
    states = json.load(open(os.path.join(ROOT_DATA_PATH, "getStates.json")))

    states_dict = {}

    for state in states["data"]["getState"]:
        states_dict[state["name"].upper()] = [state["code"], state["_id"]]

    for key, values in tqdm(states_dict.items(), leave=True, position=0):
        if st is None or key == st:
            os.makedirs(os.path.join(STATES_PATH, key), exist_ok=True)
            state_name = key
            state_hash, state_code = values[1], values[0]
            print(f"\nGetting districts for {state_name.upper()}")
            district_file_path = os.path.join(STATES_PATH, state_name.upper(), "getDistricts.json")
            if os.path.exists(district_file_path):
                print(
                    f'\n{district_file_path} already exists'
                )
            else:
                get_districts_by_state_json(state_hash, state_name.upper())

            # save the districts json data to a file

            districts = json.load(open(district_file_path))

            districts_dict = {}

            for district in districts["data"]["getdistrictAndSubdistrictBystate"]:
                districts_dict[district["name"].upper()] = district["code"]

            # print(districts_dict)
            # Error reading ./states/TELANGANA/MANCHERIAL/features.json
            # Error reading ./states/MADHYA PRADESH/BARWANI/features.json

            for district in tqdm(districts_dict.keys(), leave=False, position=1):
                if dt is None or district == dt:
                    print(f"Getting KML for {district.upper()}\n\n")
                    # print(f"{state_code, districts_dict[district]}")

                    path = os.path.join(STATES_PATH, state_name, district)
                    os.makedirs(path, exist_ok=True)

                    layer_file_path = os.path.join(path, "getLayers.json")
                    if os.path.exists(layer_file_path):
                        print(f"\n{layer_file_path} already exists")
                    else:
                        get_layer_info(state_code, districts_dict[district], path)

                    json_path = os.path.join(path, "features.json")

                    if os.path.exists(json_path):
                        print(f"\n{json_path} already exists")
                        continue

                    layers_info = json.load(open(layer_file_path))
                    shcLayers = layers_info["shcLayers"]
                    # shcLayers = ["2024-25"]

                    fixedLayers = []
                    # for fl in layers_info["fixedLayers"]:
                    #     fixedLayers.append(fl["layerName"])

                    feature_list = []
                    # get the points from the KML map
                    for layer in tqdm(shcLayers, leave=False, position=2):
                        layer_name = (
                            f"{state_code}_{districts_dict[district]}_shc_{layer}"
                        )
                        file_path = os.path.join(path, f"{layer_name}.kml")

                        if os.path.exists(file_path):
                            print(f"{file_path} already exists\n\n")
                        else:
                            print(f"Getting KML for {layer_name}\n\n")
                            kmlFile = fetch_kml(layer_name)
                            with open(file_path, "wb") as file:
                                file.write(kmlFile)

                        parsed_features = parse_kml(file_path, fixedLayers, layer)
                        feature_list.append(parsed_features)

                    merged_feature_list = list(chain.from_iterable(feature_list))

                    # Write the extracted data to a JSON file
                    with open(json_path, "w") as json_file:
                        json.dump(merged_feature_list, json_file, indent=4)

                    print(f"Data successfully extracted to {json_path}\n\n")


def main():
    # fetch meta files from the geoserver such as states, districts, and layer information
    getMetaFiles(st=None, dt=None)
    # getMetaFiles(st="MADHYA PRADESH", dt="BARWANI")
    # getMetaFiles(st="TELANGANA", dt="MANCHERIAL")


if __name__ == "__main__":
    main()
