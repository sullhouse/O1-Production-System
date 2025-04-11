import json
import requests
from flask import Response
from lxml import etree

def main(request):
    """Fetches a list of products from the Operative.One API and POSTs them to the AOS API."""
    try:
        # Load credentials from the local_credentials.json file
        with open("local_credentials.json", "r") as cred_file:
            credentials = json.load(cred_file)

        # Find the credentials for "ds04-product.com"
        cred_name = "ds04-product.com"
        cred = next((c for c in credentials["credentials"] if c["name"] == cred_name), None)

        if not cred:
            raise ValueError(f"Credentials for {cred_name} not found.")

        # Extract O1 credentials
        o1_creds = cred["o1_credentials"]
        o1_api_url = o1_creds["api_url"]
        o1_api_user = o1_creds["api_user"]
        o1_api_pass = o1_creds["api_pass"]

        # Extract AOS credentials
        aos_creds = cred["aos_credentials"]
        aos_api_url = aos_creds["api_url"]
        aos_api_user = aos_creds["api_user"]
        aos_api_pass = aos_creds["api_pass"]
        aos_api_key = aos_creds["api_key"]
        aos_mayiservice_url = aos_creds["api_mayiservice_url"]
        aos_tenant_name = aos_creds["api_tenant_name"]

        # Define the Products API endpoint
        products_endpoint = f"{o1_api_url}/operativeone/restapi/products/"

        # Make the API call with pagination
        all_products = []
        startindex = 0
        count = 100
        more_products = True

        print("Fetching products, this may take a moment...")

        while more_products:
            paged_endpoint = f"{products_endpoint}?startindex={startindex}&count={count}"
            response = requests.get(
                paged_endpoint,
                auth=(o1_api_user, o1_api_pass),
                headers={
                    "Content-Type": "application/xml",
                    "Accept": "application/xml",
                    "version": "v2"
                }
            )

            if response.status_code == 200:
                # Parse the XML response using lxml
                root = etree.fromstring(response.content)

                # Define the namespaces
                namespaces = {
                    'default': 'http://www.operative.com/api',
                    'v2': 'http://www.operative.com/api/v2',
                    'v1': 'http://www.operative.com/api/v1'
                }

                page_products = []
                for product in root.xpath("//default:product", namespaces=namespaces):
                    product_name = product.find("v2:name", namespaces).text if product.find("v2:name", namespaces) is not None else "N/A"
                    product_id = product.find("v2:id", namespaces).text if product.find("v2:id", namespaces) is not None else "N/A"
                    product_status = product.find("v2:status", namespaces).text if product.find("v2:status", namespaces) is not None else "N/A"
                    product_type = product.find("v2:productType", namespaces).text if product.find("v2:productType", namespaces) is not None else "N/A"
                    page_products.append((product_id, product_status, product_name, product_type))
                
                # Add products to our master list
                all_products.extend(page_products)
                
                # If we got fewer products than requested, we've reached the end
                if len(page_products) < count:
                    more_products = False
                    print(f"Finished fetching products. Total found: {len(all_products)}")
                else:
                    print(f"Fetched {len(all_products)} products so far...")
                
                # Move to next page
                startindex += count
            else:
                error_message = f"Failed to fetch products. Status Code: {response.status_code}, Response: {response.text}"
                print(error_message)
                return Response(error_message, status=response.status_code)

        # Print the table to the terminal AFTER all products are fetched
        print(f"\n{'Product ID':<10} {'Status':<10} {'Product Name':<40} {'Product Type':<10}")
        print("-" * 80)
        for prod_id, status, name, prod_type in all_products:
            print(f"{prod_id:<10} {status:<10} {name:<60} {prod_type:<10}")

        # Get a bearer token for AOS API
        token_response = requests.post(
            f"{aos_mayiservice_url}{aos_tenant_name}",
            json={
                "expiration": 360,
                "password": aos_api_pass,
                "userId": aos_api_user,
                "apiKey": aos_api_key
            }
        )

        if token_response.status_code != 200:
            error_message = f"Failed to get bearer token. Status Code: {token_response.status_code}, Response: {token_response.text}"
            print(error_message)
            return Response(error_message, status=token_response.status_code)

        token = token_response.json().get("token")
        if not token:
            error_message = "Bearer token not found in response."
            print(error_message)
            return Response(error_message, status=500)

        # Prepare the JSON body for the AOS API
        ps_field_values = [
            {
                "externalId": prod_id,
                "status": status,
                "value": name
            }
            for prod_id, status, name, _ in all_products
        ]

        aos_response = requests.post(
            f"{aos_api_url}/target/v1/{aos_api_key}/psFields/883/values",
            json={"psFieldValues": ps_field_values},
            headers={"Authorization": f"Bearer {token}"}
        )

        if aos_response.status_code != 200:
            error_message = f"Failed to POST products to AOS API. Status Code: {aos_response.status_code}, Response: {aos_response.text}"
            print(error_message)
            return Response(error_message, status=aos_response.status_code)

        print("Products successfully POSTed to AOS API.")
        return Response(f"Products fetched and POSTed successfully. {len(all_products)} products processed.", status=200)

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        return Response(error_message, status=500)