import requests

BASE_URL = "http://localhost:8000/api/v1"


def test_layers():
    print("Testing Layers...")
    try:
        r = requests.get(f"{BASE_URL}/geospatial/layers")
        assert r.status_code == 200, f"Status: {r.status_code}, Body: {r.text}"
        data = r.json()
        print(f"Layers found: {len(data['layers'])}")
        found = any(layer["layer_name"] == "czech_regions" for layer in data["layers"])
        if found:
            print("SUCCESS: czech_regions layer found.")
        else:
            print("ERROR: czech_regions layer NOT found.")
    except Exception as e:
        print(f"Layer test failed: {e}")


def test_single_layer():
    print("\nTesting Single Layer (czech_regions)...")
    try:
        r = requests.get(f"{BASE_URL}/geospatial/layers/czech_regions")
        assert r.status_code == 200, f"Status: {r.status_code}, Body: {r.text}"
        data = r.json()
        print(f"Layer retrieved: {data.get('layer_name')}")
        print("SUCCESS: Single layer retrieval works.")
    except Exception as e:
        print(f"Single Layer test failed: {e}")


def test_features():
    print("\nTesting Features...")
    try:
        # Test getting features with bbox (optional check) but first just list
        r = requests.get(
            f"{BASE_URL}/geospatial/features?layer_name=czech_regions&limit=5"
        )
        assert r.status_code == 200, f"Status: {r.status_code}, Body: {r.text}"
        data = r.json()
        features = data["features"]
        print(f"Features found: {len(features)}")

        if len(features) > 0:
            f = features[0]
            print(f"Sample Feature ID: {f['feature_id']}")
            props = f.get("properties", {})
            print(f"Properties: {props}")
            sensor_id = props.get("id")
            if sensor_id:
                print(f"SUCCESS: Found id link: {sensor_id}")

                # Test single feature retrieval
                print(f"Testing Single Feature ({f['feature_id']})...")
                r_single = requests.get(
                    f"{BASE_URL}/geospatial/features/{f['feature_id']}?layer_name=czech_regions"
                )
                assert (
                    r_single.status_code == 200
                ), f"Status: {r_single.status_code}, Body: {r_single.text}"
                print("SUCCESS: Single feature retrieval works.")

                return sensor_id
            else:
                print("ERROR: No id in properties.")
        else:
            print("ERROR: No features returned.")
    except Exception as e:
        print(f"Feature test failed: {e}")
    return None


def test_metadata():
    print("\nTesting Time Series Metadata...")
    try:
        r = requests.get(f"{BASE_URL}/time-series/metadata")
        assert r.status_code == 200, f"Status: {r.status_code}, Body: {r.text}"
        data = r.json()
        print(f"Metadata entries found: {len(data['series'])}")
        if len(data["series"]) > 0:
            print(f"Sample Series ID: {data['series'][0]['series_id']}")
            print("SUCCESS: Time series metadata retrieval works.")
        else:
            print("WARNING: No metadata found (unexpected if seeding worked).")
    except Exception as e:
        print(f"Metadata test failed: {e}")


def test_time_series(sensor_id):
    if not sensor_id:
        print("\nSkipping Time Series test because no id was found.")
        return

    print(f"\nTesting Time Series for sensor {sensor_id}...")
    # The seeding logic creates series_id = f"DS_{sensor_id}_LEVEL"
    series_id = f"DS_{sensor_id}_LEVEL"

    try:
        r = requests.get(f"{BASE_URL}/time-series/data?series_id={series_id}&limit=5")
        assert r.status_code == 200, f"Status: {r.status_code}, Body: {r.text}"
        data = r.json()
        points = data.get("data_points", [])
        print(f"Data points: {len(points)}")
        if len(points) > 0:
            print(f"Sample value: {points[0]['value']}")
            print("SUCCESS: Time series data retrieval works.")
        else:
            print("WARNING: No data points returned.")
    except Exception as e:
        print(f"Time Series test failed: {e}")


if __name__ == "__main__":
    print("Starting API Verification...")
    # Wait a bit for server to reload if it was just restarting
    # time.sleep(2)
    try:
        test_layers()
        test_single_layer()
        sensor_id = test_features()
        test_metadata()
        test_time_series(sensor_id)
        print("\nVerification Complete.")
    except Exception as e:
        print(f"\nVerification Script Failed: {e}")
