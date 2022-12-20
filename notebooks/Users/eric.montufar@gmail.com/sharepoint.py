import os
import tempfile
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
# from tests import test_team_site_url, test_client_credentials

from io import BytesIO 
import tempfile
from office365.sharepoint.files.file import File



user_credentials = UserCredential('','')

client_url = "/".join(url.split("/")[:5])
file_url = "/" + "/".join(url.split("/")[3:])
ctx = ClientContext().with_credentials(user_credentials)
response = File.open_binary(ctx, file_url)
 
with open('C:/Users/test.mov','wb') as output_file:
    output_file.write(response.content)
    output_file.close()
