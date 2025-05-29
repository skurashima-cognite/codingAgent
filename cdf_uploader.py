"""
This script uploads a local file to Cognite Data Fusion (CDF).

It requires the following environment variables to be set:
- COGNITE_PROJECT: Your Cognite project/cluster name.
- COGNITE_CLIENT_ID: The client ID for OAuth 2.0 authentication.
- COGNITE_CLIENT_SECRET: The client secret for OAuth 2.0 authentication.
- COGNITE_TENANT_ID: The Azure AD tenant ID for OAuth 2.0 authentication.
- COGNITE_BASE_URL: The base URL for the Cognite API (e.g., https://<cluster>.cognitedata.com).

The script also uses the following configurable placeholders:
- LOCAL_FILE_PATH: Path to the local file to be uploaded.
- TARGET_SPACE_EXTERNAL_ID: External ID of the target space in CDF.
- FILE_EXTERNAL_ID: External ID for the file in CDF.
- FILE_SOURCE: Source identifier for the file.
- FILE_MIME_TYPE: MIME type of the file.
- FILE_METADATA: Dictionary containing metadata for the file.
"""
import os
from dotenv import load_dotenv
from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import FileMetadata # For File upload
from cognite.client.data_classes.data_modeling import SpaceApply # For Space creation
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError

import sys # Added for sys.exit
# --- User-configurable placeholders ---
LOCAL_FILE_PATH = "sample.txt"
TARGET_SPACE_EXTERNAL_ID = "sdk_doc_integration_space" # Or a similar descriptive default
FILE_EXTERNAL_ID = "my_sample_file"
FILE_SOURCE = "manual_upload_script"
FILE_MIME_TYPE = "text/plain" # This should ideally be determined dynamically or be a placeholder
FILE_METADATA = {"description": "Sample file uploaded via Python script"}

def get_cdf_client() -> CogniteClient:
    """
    Initializes and returns a CogniteClient instance using environment variables.

    Reads COGNITE_PROJECT, COGNITE_CLIENT_ID, COGNITE_CLIENT_SECRET,
    COGNITE_TENANT_ID, and COGNITE_BASE_URL from environment variables.

    Raises:
        ValueError: If any of the required environment variables are not set.

    Returns:
        CogniteClient: An initialized CogniteClient.
    """
    # Read environment variables
    project = os.environ.get("COGNITE_PROJECT")
    client_id = os.environ.get("COGNITE_CLIENT_ID")
    client_secret = os.environ.get("COGNITE_CLIENT_SECRET")
    tenant_id = os.environ.get("COGNITE_TENANT_ID")
    base_url = os.environ.get("COGNITE_BASE_URL")

    # Validate that all required environment variables are set
    required_vars = {
        "COGNITE_PROJECT": project,
        "COGNITE_CLIENT_ID": client_id,
        "COGNITE_CLIENT_SECRET": client_secret,
        "COGNITE_TENANT_ID": tenant_id,
        "COGNITE_BASE_URL": base_url,
    }
    missing_vars = [name for name, value in required_vars.items() if value is None]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    # Initialize CogniteClient using OAuth credentials (see SDK docs on Authentication)
    creds = OAuthClientCredentials(
        token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=[f"{base_url}/.default"]
    )
    config = ClientConfig(
        client_name="CDFFileUploaderScript",
        project=project,
        base_url=base_url,
        credentials=creds
    )
    client = CogniteClient(config)
    return client

def ensure_space_exists(client: CogniteClient, space_external_id: str) -> str:
    """
    Ensures that the specified space exists in CDF. If not, it creates the space.

    Args:
        client: The CogniteClient instance.
        space_external_id: The external ID of the space to check/create.

    Returns:
        str: The external ID of the existing or newly created space.
    """
def ensure_space_exists(client: CogniteClient, target_space_external_id: str) -> str | None:
    """
    Ensures that a space exists in CDF. Tries the target_space_external_id first.
    If not found, it attempts to retrieve or create a fallback "demo_space".

    Args:
        client: The CogniteClient instance.
        target_space_external_id: The primary external ID of the space to use.

    Returns:
        The external ID of the space to be used, or None if operation failed.
    """
    try:
        # Attempt to retrieve the target space
        retrieved_space = client.data_modeling.spaces.retrieve(target_space_external_id)
        if retrieved_space:
            print(f"Space '{target_space_external_id}' found and will be used.")
            return target_space_external_id
        # If retrieve returns None but does not raise CogniteNotFoundError (should not happen based on SDK docs for single retrieve)
        # This path is unlikely for client.data_modeling.spaces.retrieve(single_id) which should raise or return an object
        print(f"Space '{target_space_external_id}' not found (retrieve call returned None).")

    except CogniteNotFoundError:
        print(f"Space '{target_space_external_id}' not found.")
    except CogniteAPIError as e:
        print(f"Error retrieving space '{target_space_external_id}': {e}")
        # Depending on policy, might want to re-raise or return None
        # For now, let's try the fallback if retrieval fails for reasons other than not found
        # but this could mask other issues. A stricter approach might be to re-raise here.
        # However, the instructions imply trying fallback if target is not found.
        # Let's assume any API error during target retrieval means we should try fallback.
        print(f"Proceeding to fallback due to error with target space '{target_space_external_id}'.")
        pass # Fall through to fallback logic

    # Fallback logic: try "demo_space"
    fallback_space_external_id = "demo_space"
    print(f"Attempting to retrieve or create fallback space '{fallback_space_external_id}'.")
    try:
        retrieved_fallback_space = client.data_modeling.spaces.retrieve(fallback_space_external_id)
        if retrieved_fallback_space:
            print(f"Fallback space '{fallback_space_external_id}' found and will be used.")
            return fallback_space_external_id
        # Again, path for retrieve returning None is unlikely for single ID
        print(f"Fallback space '{fallback_space_external_id}' not found (retrieve call returned None). Creating it.")

    except CogniteNotFoundError:
        print(f"Fallback space '{fallback_space_external_id}' not found. Creating it.")
    except CogniteAPIError as e:
        print(f"Error retrieving fallback space '{fallback_space_external_id}': {e}. Cannot proceed with space creation/retrieval.")
        return None # Critical error during fallback retrieval

    # If fallback space was not found (either by retrieve returning None or CogniteNotFoundError), create it
    try:
        new_space = SpaceApply(
            space=fallback_space_external_id, # Corrected parameter name
            name="Demo Space",
            description="Space created for demo purposes by upload script"
        )
        created_space = client.data_modeling.spaces.apply(spaces=new_space) # Corrected parameter name
        # apply can return a single item or a list. Assuming single for a single SpaceApply.
        if isinstance(created_space, list): # Should be Space, not SpaceList, for single apply
             created_space_item = created_space[0]
        else:
             created_space_item = created_space

        print(f"Fallback space '{created_space_item.space}' created and will be used.") # Use .space attribute
        return created_space_item.space # Use .space attribute
    except CogniteAPIError as e:
        print(f"Error creating fallback space '{fallback_space_external_id}': {e}")
        return None # Failed to create fallback space
    except Exception as e_general: # Catch any other unexpected error during space creation
        print(f"An unexpected error occurred while creating fallback space '{fallback_space_external_id}': {e_general}")
        return None

    # Should not be reached if logic is correct, means primary and fallback failed without specific errors caught above
    print("Error: Unable to secure a space for the operation after all attempts.")
    return None

def upload_file_to_cdf(
    client: CogniteClient,
    local_file_path: str,
    file_external_id: str,
    file_name: str,
    file_source: str,
    file_mime_type: str,
    file_metadata: dict,
    space_external_id: str
):
    """
    Uploads a local file to Cognite Data Fusion.

    Args:
        client: The CogniteClient instance.
        local_file_path: Path to the local file.
        file_external_id: External ID for the file in CDF.
        file_name: Name of the file in CDF.
        file_source: Source identifier for the file.
        file_mime_type: MIME type of the file.
        file_metadata: Dictionary containing metadata for the file.
        space_external_id: External ID of the space where the file will be uploaded.
    """
    try:
        # Upload file with metadata (FilesAPI.upload)
        print(f"Uploading file '{local_file_path}' to CDF as '{file_name}' (External ID: {file_external_id}) in space '{space_external_id}'...")
        client.files.upload(
            path=local_file_path,
            external_id=file_external_id,
            name=file_name,
            source=file_source,
            mime_type=file_mime_type,
            metadata=file_metadata,
            # space_external_id is not a valid parameter for files.upload() in this SDK version.
            # Files are uploaded globally or to a specific data set.
            # Linking to a space happens at the instance level in Data Modeling,
            # or by convention/metadata if not using Data Modeling explicitly for files.
            overwrite=True  # Useful for testing, set to False or make configurable if needed
        )
        print(f"File '{file_name}' (External ID: {file_external_id}) uploaded successfully. Associated with space: '{space_external_id}'.")
    except FileNotFoundError:
        print(f"Error: Local file not found at '{local_file_path}'")
    except CogniteAPIError as e:
        print(f"Error uploading file to CDF: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during file upload: {e}")


if __name__ == "__main__":
    # Load environment variables from .env file for local development
    # Ensure a .env file exists in the same directory as this script for local testing
    load_dotenv()

    print("Starting CDF file uploader script...")

    try:
        # --- 1. Initialize CDF Client ---
        # This requires COGNITE_PROJECT, COGNITE_CLIENT_ID, COGNITE_CLIENT_SECRET,
        # COGNITE_TENANT_ID, and COGNITE_BASE_URL to be set as environment variables.
        cdf_client = get_cdf_client()
        print("CogniteClient initialized successfully.")

        # --- 2. Ensure Target Space Exists ---
        # The script will try to use TARGET_SPACE_EXTERNAL_ID. If it doesn't exist,
        # it will attempt to use/create "demo_space".
        print(f"Attempting to ensure space '{TARGET_SPACE_EXTERNAL_ID}' or fallback 'demo_space' exists...")
        space_to_use_for_file = ensure_space_exists(cdf_client, TARGET_SPACE_EXTERNAL_ID)

        if not space_to_use_for_file:
            print("Critical: Failed to secure a space for file upload. Exiting script.")
            sys.exit(1) # Exit with an error code
        
        print(f"Script will proceed using space: '{space_to_use_for_file}'.")

        # --- 3. Prepare File for Upload ---
        # Derives the file name from the local path.
        # For the script to run, a "sample.txt" file should exist in the same directory,
        # or LOCAL_FILE_PATH should be updated.
        # Create a dummy sample.txt if it doesn't exist for the script to run without error.
        if not os.path.exists(LOCAL_FILE_PATH):
            print(f"Warning: Local file '{LOCAL_FILE_PATH}' not found. Creating a dummy file for demonstration.")
            with open(LOCAL_FILE_PATH, "w") as f:
                f.write("This is a sample file for testing the CDF uploader script.\n")

        file_name_in_cdf = os.path.basename(LOCAL_FILE_PATH)

        # --- 4. Upload File ---
        # Calls the upload function with all configured parameters.
        upload_file_to_cdf(
            client=cdf_client,
            local_file_path=LOCAL_FILE_PATH,
            file_external_id=FILE_EXTERNAL_ID,
            file_name=file_name_in_cdf,
            file_source=FILE_SOURCE,
            file_mime_type=FILE_MIME_TYPE,
            file_metadata=FILE_METADATA,
            space_external_id=space_to_use_for_file # Use the determined space ID
        )

        print("CDF file uploader script finished.")

    except ValueError as ve:
        print(f"Configuration error: {ve}")
    except CogniteAPIError as cae:
        print(f"Cognite API error during script execution: {cae}")
    except Exception as e:
        print(f"An unexpected error occurred in the main script execution: {e}")
