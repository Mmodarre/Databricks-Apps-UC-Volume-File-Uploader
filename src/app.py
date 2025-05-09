import os
import io
from databricks import sql
import logging
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
import streamlit as st
import pandas as pd
import json
import sys

logger = logging.getLogger("databricks_uploader")
logger.setLevel(logging.DEBUG)


# Create console handler with a higher log level
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_format = logging.Formatter('%(levelname)s - %(message)s')
console_handler.setFormatter(console_format)


# Databricks config
cfg = Config()

# Log system information
logger.info("Application starting")
#logger.debug(f"Python version: {platform.python_version()}")
# logger.debug(f"Platform: {platform.platform()}")
# logger.debug(f"Working directory: {os.getcwd()}")
logger.debug(f"Environment variables: {json.dumps({k: v for k, v in os.environ.items() if 'DATABRICKS' in k and 'SECRET' not in k})}")

# Log available HTTP headers (without sensitive information)
def log_headers():
    try:
        if hasattr(st, 'context') and hasattr(st.context, 'headers'):
            headers = st.context.headers
            safe_headers = {k: v for k, v in headers.items() if not any(sensitive in k.lower() for sensitive in ['token', 'auth', 'secret', 'password'])}
            logger.debug(f"Available HTTP headers: {json.dumps(safe_headers)}")
    except Exception as e:
        logger.error(f"Error logging headers: {str(e)}")

# Try to log headers (will only work in Databricks App environment)
log_headers()

st.set_page_config(layout="wide")
st.title("📁 Databricks Volume File Uploader")
st.markdown("""
This app allows you to upload files to a Databricks Unity Catalog volume.
""")
is_databricks_app = os.environ.get("DATABRICKS_APP_NAME") is not None
def get_workspace_client():
    logger.debug("Initializing WorkspaceClient")
    # Flush stdout/stderr to ensure logs are visible
    sys.stdout.flush()
    sys.stderr.flush()
    
    try:
        if is_databricks_app:
            # Get user access token from headers for on-behalf-of-user authorization
            user_access_token = st.context.headers.get('X-Forwarded-Access-Token')
            
            if user_access_token:
                logger.info("Creating WorkspaceClient with user access token (on-behalf-of-user)")
                # Create a clean config with ONLY the user token for on-behalf-of-user authorization
                # Explicitly set host but don't use any other env variables to avoid conflicts
                host = os.environ.get("DATABRICKS_HOST")
                
                # Create a new config object with only the token, avoiding env vars
                config = Config(
                    token=user_access_token,
                    # Disable auto-detection of other credentials
                    auth_type="pat",
                    # Ensure no other auth methods are used
                    insecure=False
                )
                
                # Override any environment variables that might interfere
                client_config = {
                    "host": host,
                    "token": user_access_token,
                    "auth_type": "pat"
                }
                
                try:
                    # Create client with explicit config, avoiding environment variables
                    client = WorkspaceClient(config=config)
                    # Test the connection
                    me = client.current_user.me()
                    logger.info(f"Successfully authenticated as {me.user_name} using on-behalf-of-user")
                    return client
                except Exception as e:
                    logger.error(f"On-behalf-of-user authentication failed: {str(e)}")
                    # Fall back to app authentication
                    logger.info("Falling back to app authentication")
            
            # If no user token or user token failed, use app authentication
            logger.info("Creating WorkspaceClient with app authentication")
            
            # Get app credentials from environment variables
            client_id = os.environ.get("DATABRICKS_CLIENT_ID")
            client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
            host = os.environ.get("DATABRICKS_HOST")
            
            if client_id and client_secret and host:
                # Create a config with explicit app OAuth credentials
                config = Config(
                    host=host,
                    client_id=client_id,
                    client_secret=client_secret,
                    auth_type="oauth",
                    insecure=False
                )
                
                try:
                    client = WorkspaceClient(config=config)
                    me = client.current_user.me()
                    logger.info(f"Successfully authenticated as app service principal: {me.user_name}")
                    return client
                except Exception as e:
                    logger.error(f"App authentication failed: {str(e)}")
                    st.error("App authentication failed. Please contact the administrator.")
                    return None
            else:
                logger.error("Missing app authentication environment variables")
                st.error("App authentication configuration is incomplete. Please contact the administrator.")
                return None
        else:
            # In local mode, use provided credentials
            if not databricks_host or not databricks_token:
                logger.error("Databricks host and token are required when running locally")
                st.error("Databricks host and token are required when running locally")
                return None
            
            logger.info(f"Creating WorkspaceClient with provided host: {databricks_host}")
            # Create a config with the provided credentials
            config = Config(
                host=databricks_host,
                token=databricks_token,
                auth_type="pat",
                insecure=False
            )
            
            # Test the connection
            try:
                client = WorkspaceClient(config=config)
                me = client.current_user.me()
                logger.info(f"Successfully authenticated as {me.user_name}")
                return client
            except Exception as e:
                logger.error(f"Authentication failed with provided credentials: {str(e)}")
                st.error("Authentication failed with provided credentials")
                return None
    except Exception as e:
        logger.error(f"Error creating WorkspaceClient: {str(e)}")
        st.error(f"Error creating WorkspaceClient: {str(e)}")
        return None
with st.sidebar:
    st.header("Volume Configuration")
    
    # Get volume path components
    catalog = st.text_input("Catalog Name", "mehdidatalake_catalog")
    schema = st.text_input("Schema Name", "wwi_staging2")
    volume = st.text_input("Volume","apps_goose")
    
    st.markdown("---")
    st.header("Authentication")
    
    # Only show authentication fields if not running in a Databricks App
    if not is_databricks_app:
        st.warning("Running in local mode - authentication required")
        databricks_host = st.text_input("Databricks Host", os.environ.get("DATABRICKS_HOST", ""))
        databricks_token = st.text_input("Databricks Token", os.environ.get("DATABRICKS_TOKEN", ""), type="password")
        st.caption("Note: When deployed as a Databricks App, authentication is handled automatically.")
        
        # Log authentication information (without token)
        if databricks_host:
            logger.debug(f"Using provided Databricks host: {databricks_host}")
    else:
        st.success("Running as a Databricks App - authentication is automatic")
        # Check if we have user access token for on-behalf-of-user
        user_access_token = st.context.headers.get('X-Forwarded-Access-Token')
        if user_access_token:
            st.info("🔑 Using on-behalf-of-user authorization - actions will use your permissions")
            # Display info about the current user
            try:
                # Create a temporary client to get user info
                host = os.environ.get("DATABRICKS_HOST")
                config = Config(host=host, token=user_access_token, auth_type="pat", insecure=False)
                client = WorkspaceClient(config=config)
                me = client.current_user.me()
                st.success(f"Authenticated as: {me.user_name}")
            except Exception as e:
                st.warning(f"Could not verify user identity: {str(e)}")
        else:
            st.info("Using app service principal for authentication")
        logger.info("Using automatic authentication from Databricks App environment")

    # Add troubleshooting section in sidebar
    st.markdown("---")
    st.header("Troubleshooting")
    
    if st.checkbox("Show debug information"):
        st.code(f"""
App Name: {os.environ.get('DATABRICKS_APP_NAME', 'Not running as app')}
Host: {os.environ.get('DATABRICKS_HOST', 'Not set')}
Has User Token: {'Yes' if st.context.headers.get('X-Forwarded-Access-Token') else 'No'} (when running as app)
Has Client ID: {'Yes' if os.environ.get('DATABRICKS_CLIENT_ID') else 'No'}
        """)
        
        if st.button("Test Authentication"):
            with st.spinner("Testing authentication..."):
                client = get_workspace_client()
                if client:
                    try:
                        me = client.current_user.me()
                        st.success(f"Authentication successful! Connected as: {me.user_name}")
                    except Exception as e:
                        st.error(f"Authentication test failed: {str(e)}")
                else:
                    st.error("Could not initialize client. Check logs for details.")

# Main content area
st.header("Upload Files")

# File uploader
uploaded_files = st.file_uploader("Choose files to upload", accept_multiple_files=True)





# Function to upload file using Databricks SDK
def upload_with_sdk(file_data, file_name, catalog, schema, volume):
    logger.info(f"Attempting to upload file: {file_name}")
    logger.debug(f"Target location: catalog={catalog}, schema={schema}, volume={volume}")
    # Flush stdout/stderr to ensure logs are visible
    sys.stdout.flush()
    sys.stderr.flush()
    
    try:
        # Initialize the WorkspaceClient
        logger.debug("Getting WorkspaceClient")
        w = get_workspace_client()
        if not w:
            logger.error("Failed to initialize WorkspaceClient")
            return False, "Failed to initialize WorkspaceClient"
        
        # Define the target volume path
        volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{file_name}"
        logger.info(f"Uploading to volume path: {volume_path}")
        sys.stdout.flush()
        
        # Check if volume exists
        try:
            logger.debug(f"Checking if volume exists: /Volumes/{catalog}/{schema}/{volume}")
            # Try to list the volume to see if it exists and is accessible
            volume_root = f"/Volumes/{catalog}/{schema}/{volume}"
            w.files.list_directory_contents(volume_root)
            logger.info(f"Volume exists and is accessible: {volume_root}")
        except Exception as e:
            logger.warning(f"Volume check failed: {str(e)}")
            logger.info("Continuing with upload attempt despite volume check failure")
            sys.stderr.flush()
        
        # Upload the file to the volume
        logger.debug(f"Starting file upload: {file_name}")
        w.files.upload(volume_path, file_data, overwrite=True)
        logger.info(f"File uploaded successfully: {file_name}")
        sys.stdout.flush()
        
        return True, volume_path
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}", exc_info=True)
        sys.stderr.flush()
        return False, str(e)


if uploaded_files and st.button("Upload Files"):
    if not volume:
        logger.warning("Upload attempted without specifying volume name")
        st.error("Please specify a volume name.")
    else:
        logger.info(f"Starting upload process for {len(uploaded_files)} files to {catalog}.{schema}.{volume}")
        # Create a progress bar
        progress_bar = st.progress(0)
        status_container = st.container()
        
        # Process each file
        for i, uploaded_file in enumerate(uploaded_files):
            # Update progress
            progress = (i) / len(uploaded_files)
            progress_bar.progress(progress)
            
            # Get file data
            file_data = io.BytesIO(uploaded_file.getvalue())
            file_name = uploaded_file.name
            file_size = len(uploaded_file.getvalue())
            logger.debug(f"Processing file {i+1}/{len(uploaded_files)}: {file_name} ({file_size} bytes)")
            
            with status_container:
                st.write(f"Uploading {file_name}...")
                
                success, message = upload_with_sdk(file_data, file_name, catalog, schema, volume)
                
                if success:
                    logger.info(f"File uploaded successfully: {file_name} to {message}")
                    st.success(f"✅ {file_name} uploaded successfully to {message}")
                else:
                    logger.error(f"Failed to upload {file_name}: {message}")
                    st.error(f"❌ Failed to upload {file_name}: {message}")
        
        # Complete the progress bar
        progress_bar.progress(1.0)
        logger.info("Upload process completed")
        st.success("Upload process completed!")

# Display volume browser (if volume is specified)
if volume:
    st.header("Volume Contents")
    
    if st.button("Refresh Volume Contents"):
        logger.info(f"Refreshing volume contents for {catalog}.{schema}.{volume}")
        try:
            # Initialize the WorkspaceClient
            w = get_workspace_client()
            if not w:
                logger.error("Failed to initialize WorkspaceClient for volume listing")
                st.error("Failed to initialize WorkspaceClient")
            else:
                # Define the volume path
                volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
                logger.debug(f"Listing contents of volume path: {volume_path}")
                
                # List the contents of the volume
                try:
                    contents = w.files.list_directory_contents(volume_path)
                    contents = list(contents)  # Convert generator to list
                    logger.info("Successfully retrieved volume contents: %d items", len(contents))
                    
                    # Display the contents in a table
                    if contents:
                        file_data = []
                        for item in contents:
                            file_data.append({
                                "Name": item.path.split("/")[-1],
                                "Path": item.path,
                                "Size": f"{item.file_size} bytes" if item.file_size else "Directory",
                                "Type": "File" if item.file_size else "Directory"
                            })
                        
                        st.table(file_data)
                        logger.debug(f"Displayed {len(file_data)} items in volume browser")
                    else:
                        logger.info("Volume is empty")
                        st.info("The volume is empty.")
                except Exception as e:
                    error_msg = f"Could not list volume contents: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    st.warning(f"Could not list volume contents. Make sure the volume exists and you have permissions to access it. Error: {str(e)}")
        except Exception as e:
            error_msg = f"Error initializing WorkspaceClient for volume listing: {str(e)}"
            logger.error(error_msg, exc_info=True)
            st.error(f"Error initializing WorkspaceClient: {str(e)}")

st.caption("""
**About this app:**  
This Databricks App allows you to upload files to Unity Catalog volumes. It uses the Databricks Python SDK.
When running locally, you need to provide your Databricks host and token. When deployed as a Databricks App,
authentication is handled automatically using on-behalf-of-user authorization, which means the app will
use your permissions when accessing Unity Catalog volumes.
""")