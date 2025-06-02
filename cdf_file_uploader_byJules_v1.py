#!/usr/bin/env python3
"""
This script provides a utility to upload files to Cognite Data Fusion (CDF)
and register them as CogniteFile data model instances.

**Purpose:**
1.  Authenticates to CDF using environment variables.
2.  Retrieves or creates a specified data modeling Space.
3.  Uploads a local file to the CDF Files API.
4.  Creates a data model instance (node) of type 'CogniteFile' in the
    specified Space, linking it to the uploaded file.

**Required Environment Variables:**
    - COGNITE_PROJECT: The CDF project name.
    - COGNITE_CLIENT_ID: The client ID for OAuth authentication.
    - COGNITE_CLIENT_SECRET: The client secret for OAuth authentication.
    - COGNITE_TENANT_ID: The Azure AD tenant ID.
    - COGNITE_BASE_URL: The base URL for the CDF API (e.g., https://<cluster>.cognitedata.com).
    Set these variables in your shell environment before running the script.
    Example:
        export COGNITE_PROJECT="my-cdf-project"
        export COGNITE_CLIENT_ID="your-client-id"
        # ... and so on

**Placeholder Variables in `main()`:**
    You MUST configure the placeholder variables within the `main()` function
    (marked with "<<< USER: Configure these variables >>>") to match your
    local file, target CDF resources, and desired metadata.

**Assumed Data Model View:**
    The script assumes the existence of a View with Id
    `ViewId(space="cdf_core", external_id="CogniteFile", version="v1")`.
    If your project uses a different View for CogniteFile instances (e.g., from
    a custom data model or a different version of the Core DM), you will need
    to update the `cognite_file_view_id` variable within the
    `create_cognite_file_node` function.
"""

import os
import mimetypes
import sys # For sys.exit

try:
    from cognite.client import CogniteClient, ClientConfig
    from cognite.client.credentials import OAuthClientCredentials
    from cognite.client.exceptions import (
        CogniteAPIError,
        CogniteNotFoundError,
        CogniteDuplicatedError, # Added for specific handling
    )
    from cognite.client.data_classes import FileMetadata
    from cognite.client.data_classes.data_modeling import (
        SpaceApply,
        NodeApply,
        NodeOrEdgeData,
        InstancesApplyResult,
    )
    from cognite.client.data_classes.data_modeling.ids import ViewId
    # datetimeToTimestamp is not directly used in this version but can be useful for manual timestamp properties
    # from cognite.client.utils._time import datetimeToTimestamp
except ImportError:
    print("ERROR: cognite-sdk not found. Please install it using 'pip install cognite-sdk'")
    sys.exit(1) # Exit if SDK is not available


def initialize_cognite_client() -> CogniteClient:
    """
    Initializes the CogniteClient using environment variables.
    (Function content remains the same as previously defined)
    """
    env_vars = {
        "COGNITE_PROJECT": os.environ.get("COGNITE_PROJECT"),
        "COGNITE_CLIENT_ID": os.environ.get("COGNITE_CLIENT_ID"),
        "COGNITE_CLIENT_SECRET": os.environ.get("COGNITE_CLIENT_SECRET"),
        "COGNITE_TENANT_ID": os.environ.get("COGNITE_TENANT_ID"),
        "COGNITE_BASE_URL": os.environ.get("COGNITE_BASE_URL"),
    }

    missing_vars = [key for key, value in env_vars.items() if value is None]
    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )

    token_url = (
        f"https://login.microsoftonline.com/{env_vars['COGNITE_TENANT_ID']}"
        "/oauth2/v2.0/token"
    )

    creds = OAuthClientCredentials(
        client_id=env_vars["COGNITE_CLIENT_ID"],
        client_secret=env_vars["COGNITE_CLIENT_SECRET"],
        token_url=token_url,
        scopes=[f"https://{env_vars['COGNITE_BASE_URL']}/.default"],
    )

    cnf = ClientConfig(
        client_name="JulesFileUploaderScript",
        project=env_vars["COGNITE_PROJECT"],
        base_url=env_vars["COGNITE_BASE_URL"],
        credentials=creds,
    )
    return CogniteClient(cnf)


def get_or_create_space(client: CogniteClient, target_space_external_id: str) -> str:
    """
    Retrieves an existing data modeling space or creates it if it doesn't exist.
    If the target space is not found, 'demo_space' is created and its external ID is used.
    (Function content remains the same as previously defined, with minor logging adjustment)
    """
    try:
        space_obj = client.data_modeling.spaces.retrieve(space=target_space_external_id)
        if space_obj: # Check if retrieve returned an object
            print(f"Successfully retrieved target data modeling space: '{target_space_external_id}'")
            return target_space_external_id
        else:
            # This path might not be hit if retrieve(..) raises CogniteNotFoundError or returns None
            # and CogniteNotFoundError is caught. Adding for logical completeness.
            print(f"Target space '{target_space_external_id}' not found by retrieve (returned None).")
    except CogniteNotFoundError:
        print(f"Target data modeling space '{target_space_external_id}' not found (CogniteNotFoundError).")
    except CogniteAPIError as e:
        # Log other API errors during retrieval but proceed to attempt creation of 'demo_space'
        print(f"API error retrieving space '{target_space_external_id}': {e}. Proceeding to create 'demo_space'.")

    # If target_space_external_id was not found or retrieval failed, attempt to create 'demo_space'
    default_space_xid = "demo_space" # Using a fixed default if target is not found
    print(f"Attempting to create or use default data modeling space: '{default_space_xid}' (as '{target_space_external_id}' was not found or retrieval failed).")
    new_space_apply_object = SpaceApply(
        space=default_space_xid, # Use the fixed default_space_xid
        name="Demo Space",
        description="A demonstration space automatically created by the Jules file uploader script.",
    )
    try:
        created_space = client.data_modeling.spaces.apply(space=new_space_apply_object)
        print(f"Successfully ensured/created data modeling space: '{created_space.space}'.")
        return created_space.space # Return the actual space externalId (demo_space in this case)
    except CogniteDuplicatedError: # If "demo_space" already exists from a previous run
         print(f"Default data modeling space '{default_space_xid}' already exists. Using it.")
         return default_space_xid
    except CogniteAPIError as e:
        print(f"Fatal API error creating data modeling space '{default_space_xid}': {e}")
        raise # Re-raise if creation of the fallback space fails, as it's a critical step.


def upload_file_to_cdf_files_api(
    client: CogniteClient,
    local_file_path: str,
    file_external_id: str,
    file_name: str = None,
    file_source: str = None,
    mime_type: str = None,
    user_metadata: dict = None,
    data_set_id: int = None,
) -> FileMetadata:
    """
    Uploads a local file to the Cognite Data Fusion (CDF) Files API.
    (Function content remains the same as previously defined)
    """
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Local file not found: {local_file_path}")

    effective_file_name = file_name or os.path.basename(local_file_path)

    effective_mime_type = mime_type
    if not effective_mime_type:
        effective_mime_type, _ = mimetypes.guess_type(local_file_path)
        if not effective_mime_type:
            effective_mime_type = "application/octet-stream"
        print(f"Determined MIME type for '{effective_file_name}' as: {effective_mime_type}")
    else:
        print(f"Using provided MIME type for '{effective_file_name}': {effective_mime_type}")

    print(f"Uploading file '{local_file_path}' to CDF Files API as '{effective_file_name}' with external_id '{file_external_id}'...")

    file_metadata = client.files.upload(
        path=local_file_path,
        external_id=file_external_id,
        name=effective_file_name,
        source=file_source,
        mime_type=effective_mime_type,
        metadata=user_metadata,
        data_set_id=data_set_id,
        overwrite=True,
    )
    print(f"Successfully uploaded file to Files API. CDF File external_id: '{file_metadata.external_id}', ID: {file_metadata.id}")
    return file_metadata


def create_cognite_file_node(
    client: CogniteClient,
    uploaded_file_metadata: FileMetadata,
    dm_space_external_id: str,
    cf_node_external_id: str,
    cf_node_name: str = None,
    cf_node_source: str = None,
    user_defined_dm_properties: dict = None,
) -> InstancesApplyResult:
    """
    Creates a CogniteFile data model instance (node) in the specified space.
    (Function content remains the same as previously defined)
    """
    effective_node_name = cf_node_name or uploaded_file_metadata.name
    effective_node_source = cf_node_source or uploaded_file_metadata.source

    # <<!>> IMPORTANT: Adjust if your CogniteFile ViewId is different!
    cognite_file_view_id = ViewId(space="cdf_core", external_id="CogniteFile", version="v1")

    properties = {
        "name": effective_node_name,
        "fileExternalId": uploaded_file_metadata.external_id,
    }

    if uploaded_file_metadata.mime_type:
        properties["mimeType"] = uploaded_file_metadata.mime_type
    if uploaded_file_metadata.metadata: # Ensure metadata is a dict
        properties["metadata"] = uploaded_file_metadata.metadata if isinstance(uploaded_file_metadata.metadata, dict) else {}
    else:
        properties["metadata"] = {}
    if uploaded_file_metadata.directory: # Check if directory is not None or empty
        properties["directory"] = uploaded_file_metadata.directory
    if effective_node_source:
        properties["source"] = effective_node_source
    
    # Timestamps: FileMetadata provides uploaded_time, created_time, last_updated_time as ms since epoch.
    # The CogniteFile model (cdf_core.CogniteFile.v1) might expect ISO8601 strings for corresponding fields
    # like 'sourceCreatedTime' or 'sourceModifiedTime'. If so, conversion is needed:
    # Example using uploaded_time for a 'sourceTimestamp' property if it's a Timestamp type:
    # if uploaded_file_metadata.uploaded_time:
    #     from datetime import datetime, timezone
    #     properties["sourceTimestamp"] = datetime.fromtimestamp(uploaded_file_metadata.uploaded_time / 1000, tz=timezone.utc).isoformat()
    # Add similar logic for other timestamp fields as required by your specific CogniteFile view definition.

    if user_defined_dm_properties:
        properties.update(user_defined_dm_properties)

    node_data = NodeOrEdgeData(source=cognite_file_view_id, properties=properties)
    cognite_file_node = NodeApply(
        space=dm_space_external_id,
        external_id=cf_node_external_id,
        sources=[node_data],
    )

    print(f"Creating CogniteFile data model node with external_id '{cf_node_external_id}' in space '{dm_space_external_id}'...")
    result = client.data_modeling.instances.apply(nodes=cognite_file_node, auto_create_start_nodes=True, auto_create_end_nodes=True)
    print(f"Successfully processed CogniteFile node creation. {len(result.nodes)} node(s) involved.")
    return result


def main():
    """
    Main function to orchestrate the file upload and data model instance creation.
    """
    # --- <<< USER: Configure these variables >>> ---
    # Path to the local file you want to upload
    LOCAL_FILE_PATH = "path/to/your/local/file.txt"  # REQUIRED

    # External ID for the target Data Modeling Space.
    # If this space doesn't exist, 'demo_space' will be created/used.
    TARGET_DM_SPACE_XID = "my_data_model_space"  # REQUIRED (used by get_or_create_space)

    # External ID for the file in CDF Files API.
    # This will also be used as the default external_id for the CogniteFile data model node.
    FILE_EXTERNAL_ID = "unique_file_external_id_001"  # REQUIRED

    # --- Optional Overrides for File Metadata (client.files API) ---
    FILE_NAME_IN_CDF = ""  # Optional: If empty, uses local file name.
    FILE_SOURCE = "JulesFileUploaderScript"  # Optional: Source of the file.
    FILE_MIME_TYPE = ""  # Optional: If empty, script guesses. E.g., "text/plain", "application/pdf".
    FILE_METADATA_DICT = {"category": "upload_test", "processed_by": "JulesScript"}  # Optional: Custom metadata.
    OPTIONAL_DATA_SET_ID = None  # Optional: INTEGER ID of an existing CDF Data Set. Not external_id.

    # --- Optional Overrides for CogniteFile Data Model Node ---
    # Defaults to FILE_EXTERNAL_ID if not set.
    COGNITE_FILE_NODE_EXTERNAL_ID = FILE_EXTERNAL_ID # Recommended to keep same as FILE_EXTERNAL_ID

    # Optional: If empty, uses the file name from uploaded file metadata.
    COGNITE_FILE_NODE_NAME = ""

    # Optional: If empty, uses the file source from uploaded file metadata (or FILE_SOURCE if provided).
    COGNITE_FILE_NODE_SOURCE = ""

    # Optional: Add or override properties for the CogniteFile node instance.
    # Ensure these properties are defined in your CogniteFile View.
    # Example: USER_DEFINED_DM_PROPERTIES = {"customProperty": "customValue", "domainSpecificType": "WellLog"}
    USER_DEFINED_DM_PROPERTIES = {}
    # --- <<< End of User Configuration >>> ---

    # Helper: Create dummy file if placeholder path is used and file doesn't exist
    if LOCAL_FILE_PATH == "path/to/your/local/file.txt":
        print(f"WARNING: LOCAL_FILE_PATH is set to a default placeholder: '{LOCAL_FILE_PATH}'.")
        print("         For a real run, please update this to an actual file path.")
        if not os.path.exists(LOCAL_FILE_PATH):
            try:
                print(f"         Attempting to create a dummy file at '{LOCAL_FILE_PATH}' for demonstration...")
                os.makedirs(os.path.dirname(LOCAL_FILE_PATH), exist_ok=True)
                with open(LOCAL_FILE_PATH, "w") as f:
                    f.write("This is a dummy file created by cdf_file_uploader.py for testing.\n")
                print(f"         Successfully created dummy file: '{LOCAL_FILE_PATH}'")
            except OSError as e:
                print(f"ERROR: Could not create dummy file at '{LOCAL_FILE_PATH}': {e}")
                print("       Please manually create the file or update LOCAL_FILE_PATH.")
                sys.exit(1)

    client = None # Initialize client to None for the finally block
    try:
        print("Starting CDF File Upload and Data Model Node Creation Process...")

        # 1. Initialize Cognite Client
        print("\nStep 1: Initializing Cognite Client...")
        client = initialize_cognite_client()
        print("Cognite Client initialized successfully.")
        # Basic check to confirm client is working (optional)
        # print(f"Client authenticated for project: {client.config.project}, user: {client.iam.token.inspect().subject}")


        # 2. Get or Create Data Modeling Space
        print(f"\nStep 2: Ensuring Data Modeling Space '{TARGET_DM_SPACE_XID}' exists...")
        actual_dm_space_xid = get_or_create_space(client, TARGET_DM_SPACE_XID)
        print(f"Using Data Modeling Space: '{actual_dm_space_xid}' for CogniteFile node.")


        # 3. Upload File to CDF Files API
        print(f"\nStep 3: Uploading local file '{LOCAL_FILE_PATH}' to CDF Files API...")
        uploaded_file_metadata = upload_file_to_cdf_files_api(
            client=client,
            local_file_path=LOCAL_FILE_PATH,
            file_external_id=FILE_EXTERNAL_ID,
            file_name=FILE_NAME_IN_CDF,
            file_source=FILE_SOURCE,
            mime_type=FILE_MIME_TYPE,
            user_metadata=FILE_METADATA_DICT,
            data_set_id=OPTIONAL_DATA_SET_ID,
        )
        print(f"File uploaded successfully to Files API. External ID: '{uploaded_file_metadata.external_id}'")
        # You can print more details from uploaded_file_metadata if needed


        # 4. Create CogniteFile Data Model Node
        print(f"\nStep 4: Creating CogniteFile Data Model Node '{COGNITE_FILE_NODE_EXTERNAL_ID}'...")
        dm_node_result = create_cognite_file_node(
            client=client,
            uploaded_file_metadata=uploaded_file_metadata,
            dm_space_external_id=actual_dm_space_xid,
            cf_node_external_id=COGNITE_FILE_NODE_EXTERNAL_ID,
            cf_node_name=COGNITE_FILE_NODE_NAME,
            cf_node_source=COGNITE_FILE_NODE_SOURCE,
            user_defined_dm_properties=USER_DEFINED_DM_PROPERTIES,
        )
        if dm_node_result and dm_node_result.nodes:
            print("CogniteFile Data Model Node processed successfully.")
            for node_res in dm_node_result.nodes:
                print(f"  - Node Details: External ID='{node_res.external_id}', Space='{node_res.space}', Version='{node_res.version}'")
        else:
            # This case might indicate an issue if no nodes were returned but no error was raised.
            print("Warning: CogniteFile node creation call completed but returned no node information.")

        print("\n--- Script finished successfully! ---")

    except ValueError as e: # For missing env vars or potentially invalid configs
        print(f"ERROR (Configuration): {e}")
        print("Please check your environment variables and script placeholder configurations.")
    except FileNotFoundError as e:
        print(f"ERROR (File System): {e}")
        print(f"Ensure the file specified in LOCAL_FILE_PATH ('{LOCAL_FILE_PATH}') exists.")
    except CogniteNotFoundError as e:
        print(f"ERROR (CDF Not Found): {e.message} (Code: {e.code}, Request ID: {e.x_request_id})")
        print("This could be due to a non-existent resource (e.g., data set ID, or an error during space retrieval).")
    except CogniteDuplicatedError as e:
        print(f"ERROR (CDF Duplicated): {e.message} (Code: {e.code}, Request ID: {e.x_request_id})")
        print("This usually means a resource with the same external ID already exists and overwrite is not permitted for that operation.")
    except CogniteAPIError as e: # Catch other Cognite API errors
        print(f"ERROR (Cognite API): {e.message} (Code: {e.code}, Request ID: {e.x_request_id})")
        print(f"Failed items: {e.failed}, Successful items: {e.successful}")
        # For detailed diagnostics, you might want to inspect e.extra if available and populated
        # if e.extra: print(f"Extra details: {e.extra}")
    except Exception as e: # Catch-all for any other unexpected errors
        print(f"UNEXPECTED ERROR: {type(e).__name__} - {e}")
        import traceback
        traceback.print_exc() # Prints the full stack trace for unexpected errors
    finally:
        if client: # Attempt to log out or clean up client if needed
            # For many script types, explicit logout isn't strictly necessary,
            # but good practice in long-running applications.
            # client.logout() # Or other cleanup
            pass
        print("\nScript execution completed (either successfully or with errors).")


if __name__ == "__main__":
    main()
