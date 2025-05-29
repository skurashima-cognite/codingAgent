"""
Cognite Data Fusion File Upload Script

This script uploads a local file to a specified CDF space using the Cognite Python SDK.
If the specified space does not exist, it creates a new space named 'demo_space'.

User must set the following environment variables in the execution environment:
- COGNITE_PROJECT
- COGNITE_CLIENT_ID
- COGNITE_CLIENT_SECRET
- COGNITE_TENANT_ID
- COGNITE_BASE_URL

User-configurable placeholders are marked below. Please update them before running the script.

References:
- Cognite Python SDK documentation: https://cognite-sdk-python.readthedocs-hosted.com/en/latest/
"""

import os
import sys
from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata, FileWrite
from cognite.client.exceptions import CogniteAPIError

# ------------------- USER CONFIGURATION SECTION -------------------
# Update these placeholders before running the script
local_file_path = "<PATH_TO_LOCAL_FILE>"  # e.g., "/Users/you/data/myfile.csv"
target_space_external_id = "<TARGET_SPACE_EXTERNAL_ID>"  # e.g., "my_space"
file_external_id = "<FILE_EXTERNAL_ID>"  # e.g., "myfile-2025-05-29"
file_source = "<FILE_SOURCE>"  # e.g., "local_upload"
file_metadata = {"description": "<FILE_DESCRIPTION>"}  # e.g., {"description": "Sample file upload"}
# ------------------------------------------------------------------

def get_env_var(name):
    value = os.environ.get(name)
    if not value:
        print(f"[ERROR] Environment variable '{name}' is not set.")
        sys.exit(1)
    return value

def main():
    # Read CDF connection info from environment variables
    project = get_env_var("COGNITE_PROJECT")
    client_id = get_env_var("COGNITE_CLIENT_ID")
    client_secret = get_env_var("COGNITE_CLIENT_SECRET")
    tenant_id = get_env_var("COGNITE_TENANT_ID")
    base_url = get_env_var("COGNITE_BASE_URL")

    # Initialize CogniteClient (see SDK docs: 'Client Configuration')
    try:
        client = CogniteClient(
            client_name="cdf-file-upload-script",
            project=project,
            base_url=base_url,
            token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            client_id=client_id,
            client_secret=client_secret,
            token_scope=[f"https://{base_url.split('//')[1]}/.default"]
        )
    except Exception as e:
        print(f"[ERROR] Failed to initialize CogniteClient: {e}")
        sys.exit(1)

    # Check if the target space exists (see SDK docs: 'spaces.retrieve')
    try:
        space = client.spaces.retrieve(external_id=target_space_external_id)
        if not space:
            print(f"[INFO] Space '{target_space_external_id}' not found. Creating 'demo_space'.")
            space = client.spaces.create({"external_id": "demo_space"})
            space_external_id = "demo_space"
        else:
            print(f"[INFO] Space '{target_space_external_id}' found.")
            space_external_id = target_space_external_id
    except CogniteAPIError as e:
        print(f"[ERROR] Error retrieving or creating space: {e}")
        sys.exit(1)

    # Read the local file
    try:
        with open(local_file_path, "rb") as f:
            file_bytes = f.read()
    except FileNotFoundError:
        print(f"[ERROR] Local file '{local_file_path}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Error reading file: {e}")
        sys.exit(1)

    # Determine MIME type (optional, can use 'application/octet-stream' as default)
    import mimetypes
    mime_type, _ = mimetypes.guess_type(local_file_path)
    if not mime_type:
        mime_type = "application/octet-stream"

    # Upload file to CDF (see SDK docs: 'files.upload_bytes')
    try:
        file_metadata = FileWrite(
            external_id=file_external_id,
            name=os.path.basename(local_file_path),
            source=file_source,
            mime_type=mime_type,
            metadata=file_metadata,
            space_external_id=space_external_id
        )
        upload_result = client.files.upload_bytes(
            file_bytes,
            file_metadata=file_metadata
        )
        print(f"[SUCCESS] File uploaded to CDF. File ID: {upload_result.id}, External ID: {upload_result.external_id}")
    except CogniteAPIError as e:
        print(f"[ERROR] Cognite API error during file upload: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Unexpected error during file upload: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
