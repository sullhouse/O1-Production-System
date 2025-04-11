from google.cloud import bigquery
import json
import os
import requests
    
def upsert_advertiser(name, sourceAdvertiserId):
    """Checks to see if an advertiser exists in Operative.One and returns that ID.

    Args:
        name: The name of the advertiser from the OMS.
        sourceAdvertiserId: The source advertiser ID from the OMS.

    Returns:
        advertiserId: The ID of the advertiser in the Operative.One system.
    """
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
        AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        print("ℹ️ Using AWS credentials from environment variables")
    advertiser_id = 0
    base_url = "https://api.operative.one/v1/accounts"  # Replace with the actual API base URL
    headers = {
        "Authorization": "Bearer YOUR_ACCESS_TOKEN",  # Replace with the actual token
        "Content-Type": "application/json"
    }

    # Step 1: Search for the advertiser by name
    search_url = f"{base_url}/search"
    search_payload = {
        "filters": {
            "name": name,
            "type": "advertiser"
        }
    }
    response = requests.post(search_url, headers=headers, json=search_payload)

    if response.status_code == 200:
        results = response.json().get("data", [])
        if results:
            # Advertiser found
            advertiser_id = results[0]["id"]
            print(f"Advertiser found with id {advertiser_id}")
        else:
            # Advertiser not found, create a new one
            print(f"Advertiser '{name}' not found. Creating a new advertiser.")
            create_payload = {
                "name": name,
                "type": "advertiser",
                "externalId": sourceAdvertiserId
            }
            create_response = requests.post(base_url, headers=headers, json=create_payload)

            if create_response.status_code == 201:
                advertiser_data = create_response.json()
                advertiser_id = advertiser_data["id"]
                print(f"New advertiser created with id {advertiser_id}")
            else:
                print(f"Failed to create advertiser: {create_response.status_code} - {create_response.text}")
                raise Exception("Failed to create advertiser in Operative.One")
    else:
        print(f"Failed to search for advertiser: {response.status_code} - {response.text}")
        raise Exception("Failed to search for advertiser in Operative.One")

    return advertiser_id

def main(request):

    if request.is_json:
        # Get the JSON data
        request_json = request.get_json()

        advertiser_id = upsert_advertiser(request_json.get("name"), request_json.get("sourceAdvertiserId"))

        # Replace values in response JSON
        response_json = {
            "advertiserId": advertiser_id,
            "sourceAdvertiserId": request_json.get("sourceAdvertiserId"),
        }

        return response_json
    else:
        # Handle non-JSON requests (optional)
        return "Request is not a JSON object"