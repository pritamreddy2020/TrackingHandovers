# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 17:57:50 2024

@author: avarsh1
"""

import json
import requests
from msal import PublicClientApplication




def get_form_responses(client_id, client_secret, tenant_id, form_id, access_token):


  # Base URL for the Forms API
  base_url = "https://graph.microsoft.com/v1.0"

  # Construct the URL for the specific form
  url = f"{base_url}/forms/{form_id}/responses"

  # Headers with the authorization token
  headers = {
      "Authorization": f"Bearer {access_token}"
  }

  try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for non-200 status codes

    # Parse the JSON response
    data = response.json()
    return data

  except requests.exceptions.RequestException as e:
    print(f"Error retrieving form responses: {e}")
    return None

def get_access_token(client_id, client_secret, tenant_id,config):
    import sys
    app = PublicClientApplication(client_id,
    authority = f"https://login.microsoftonline.com/{tenant_id}")
    result = None  # It is just an initial value. 
    
    flow = app.initiate_device_flow(scopes=config["scope"])
    if "user_code" not in flow:
        raise ValueError(
            "Fail to create device flow. Err: %s" % json.dumps(flow, indent=4))
    
    print(flow["message"])
    sys.stdout.flush()
    
    result = app.acquire_token_by_device_flow(flow)
    
    if "access_token" in result:
        access_token = result["access_token"]
    else:
        print(result.get("error"))
    
    # accounts = app.get_accounts()
    # if accounts:
    # # If so, you could then somehow display these accounts and let end user choose
    #     chosen = accounts[0]
    #     result = app.acquire_token_silent(scopes=["your_scope"], account=chosen)
        
    #     # At this point, you can save you can update your cache if you are using token caching
    #     # check result variable, if its None then you should interactively acquire a token
    #     if not result:
    #         # So no suitable token exists in cache. Let's get a new one from Microsoft Entra.
    #         result = app.acquire_token_by_one_of_the_actual_method(..., scopes=["User.Read"])
        
    #     if "access_token" in result:
    #         access_token = result["access_token"]
    #     else:
    #         print(result.get("error"))  

# Example usage (replace placeholders)
client_id = "47eca73d-97c1-4ccd-af23-7187b447aa62"
client_secret = "94636cb4-f3cb-4531-b58e-dddc14d8c1fa"
tenant_id="d2d302fb-0aef-4773-94a5-7950c6f64a35"
form_id="wLT0u8Kc0eUpXlQxvZKNSmNCg53IWNDkutZXhZnAhJUREJKSFpMNlhHMEFCQTVDRlU0RURTMEwzVy4u"
config = {"scope": ["User.Read"]}

# Access token retrieval logic (omitted for brevity, see previous responses)
access_token_response = get_access_token(client_id, client_secret, tenant_id,config)

if isinstance(access_token_response, dict) and "access_token" in access_token_response:
  access_token = access_token_response["access_token"]
  # Use the access token

  form_responses = get_form_responses(client_id, client_secret, tenant_id, form_id, access_token)
  if form_responses:
    # Process the retrieved form responses
    print("Successfully retrieved form responses!")
  else:
    print("Failed to retrieve form responses.")
else:
  print("Error: Failed to acquire access token")
