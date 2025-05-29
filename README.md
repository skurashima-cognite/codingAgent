使用プロンプト

Please follow the instructions below to create a Python script for uploading files to Cognite Data Fusion (CDF).

Primary Documentation Resource: For all tasks related to the Cognite Python SDK, you must use the official documentation available at: https://cognite-sdk-python.readthedocs-hosted.com/en/latest/ Please treat this as your primary source of truth for SDK usage, methods, and best practices.

Security Imperative: This script requires authentication credentials for CDF. In accordance with Jules' official documentation, do not commit secrets such as API keys or credentials to your repository. The script should be designed to read authentication information from environment variables set within Jules' execution environment.

🎯 Ultimate Goal

Generate a Python script that uploads a local file to a specified CDF space. If the specified space does not exist, the script should create a new space named demo_space. The file should be registered in CDF as a CogniteFile object. The script must read CDF connection information from environment variables set in Jules' execution environment.

📝 Main Tasks

Consult Cognite Data Fusion Python SDK Documentation (Primary Resource):

Thoroughly review the official Cognite Python SDK documentation at https://cognite-sdk-python.readthedocs-hosted.com/en/latest/.

Pay particular attention to sections covering:

File Operations: Methods for uploading files (e.g., files.upload(), files.upload_bytes(), files.create()), and handling file objects.

Spaces: Creating and managing spaces (e.g., spaces.create(), spaces.retrieve(), spaces.list()).

CogniteFile Objects: The structure, attributes (especially external_id, name, source, mime_type, metadata), and how to correctly associate files with spaces.

Authentication and Client Configuration: How to initialize and configure the CogniteClient.

Create Python Script:

Client Initialization:

Write code to read CDF connection information using Python's os module (os.environ.get()) from the following environment variables, which are expected to be set in Jules' execution environment:

COGNITE_PROJECT

COGNITE_CLIENT_ID

COGNITE_CLIENT_SECRET

COGNITE_TENANT_ID

COGNITE_BASE_URL

Initialize the CogniteClient using these retrieved environment variables, following the practices outlined in the provided SDK documentation. Consider error handling for cases where environment variables are not set.

Space Existence Check:

Using the external_id of the space specified by the user, check if that space exists in CDF, referencing the SDK documentation for appropriate methods.

If it does not exist, create a new space with the external_id demo_space.

File Upload and Registration:

Read the file from the specified local file path.

Upload the file to the specified space (either the one found or the newly created demo_space), using methods detailed in the SDK documentation.

Register the uploaded file as a CogniteFile object in CDF. Ensure all necessary and relevant attributes (external_id, name, source, mime_type, metadata, space_external_id) are set correctly according to the SDK documentation.

Error Handling:

Implement appropriate exception handling for anticipated errors such as file not found, CDF connection errors, permission errors, missing required fields, or necessary environment variables not being set. Refer to the SDK documentation for common exceptions.

Placeholders for Configuration Values:

Values that the user needs to change within the script (e.g., local_file_path, target_space_external_id, file_external_id, file_source, file_metadata) should be clearly marked as placeholders or in a configuration section.

💻 Expected Deliverables

A runnable Python script that meets all the above requirements and is consistent with the practices in the specified SDK documentation.

Comments within the script explaining the purpose and operation of major processing blocks and Cognite Python SDK function calls, referencing the SDK documentation where appropriate.

Import statements for necessary libraries (e.g., cognite.client, os) at the beginning of the script.

A brief explanation in comments about user-configurable variables and how to run the script (especially regarding the need to set environment variables in Jules' execution environment).

✨ Considerations

Idempotency: (No change)

SDK Version: The script should be compatible with the SDK version documented at the provided URL.

Code Quality: (No change)

If you lack any information necessary to complete this task or have any questions, please do not hesitate to ask.
