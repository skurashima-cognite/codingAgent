# Cognite Data Fusion (CDF) File Uploader

This script uploads a local file to a specified Cognite Data Fusion (CDF) space.
If the specified target space does not exist, it will attempt to use/create a fallback space named `demo_space`.

## Prerequisites

*   Python 3.8+
*   Poetry for dependency management (https://python-poetry.org/)
*   Access to a Cognite Data Fusion project with appropriate credentials.

## Setup

1.  **Clone the repository.**
2.  **Install dependencies using Poetry:**
    ```bash
    poetry install
    ```
3.  **Configure Environment Variables:**
    This script requires authentication credentials for CDF to be set as environment variables. Create a `.env` file in the root of the project or set these variables in your execution environment:

    ```
    COGNITE_PROJECT="your_cdf_project_name"
    COGNITE_CLIENT_ID="your_client_id"
    COGNITE_CLIENT_SECRET="your_client_secret"
    COGNITE_TENANT_ID="your_azure_ad_tenant_id"
    COGNITE_BASE_URL="your_cdf_cluster_base_url" # e.g., https://<cluster>.cognitedata.com
    ```
    **Note:** The `.env` file is for local development and should be added to `.gitignore` in a production environment to avoid committing secrets.

## Configuration

Open the `cdf_uploader.py` script and modify the following placeholder variables at the top of the file to suit your needs:

*   `LOCAL_FILE_PATH`: Path to the local file you want to upload (default: `"sample.txt"`).
*   `TARGET_SPACE_EXTERNAL_ID`: The external ID of the CDF space you want to associate the file with (default: `"sdk_doc_integration_space"`). If this space is not found, the script will try to use/create `"demo_space"`.
*   `FILE_EXTERNAL_ID`: The external ID for the file in CDF (default: `"my_sample_file"`).
*   `FILE_SOURCE`: A source identifier for the file (default: `"manual_upload_script"`).
*   `FILE_MIME_TYPE`: The MIME type of the file (default: `"text/plain"`). You might need to change this based on the file type.
*   `FILE_METADATA`: A dictionary for any metadata you want to associate with the file (default: `{"description": "Sample file uploaded via Python script"}`).

A `sample.txt` file is included as an example for `LOCAL_FILE_PATH`.

## Running the Script

Once dependencies are installed and environment variables are set (either in `.env` or the environment itself), run the script using Poetry:

```bash
poetry run python cdf_uploader.py
```

The script will output information about the client initialization, space handling, and file upload status.

## Error Handling

The script includes error handling for:
*   Missing environment variables.
*   File not found for upload.
*   Cognite API errors during client initialization, space operations, or file upload.
