import requests
from lxml import etree
import minio
from minio.commonconfig import Tags
import urllib3
import sys
import mimetypes
import re
from typing import Optional
from pydantic import SecretStr
import functools
from io import BytesIO
from datetime import datetime, timedelta, timezone
from aniso8601 import parse_datetime
from loguru import logger
from config.integrations import MinioSettings, AuthMode

conf = MinioSettings()


def renew_authentication_token(func):
    """Renews LDAP token before method call if nearing expiration. No-op for API key mode."""
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.auth_mode == AuthMode.LDAP:
            if datetime.now(timezone.utc) >= self.token_expiration - timedelta(seconds=conf.token_renew_margin):
                logger.warning("Authentication token going to expire soon, renewing token")
                self._create_client()
        return func(self, *args, **kwargs)
    return wrapper


class S3Minio:
    def __init__(
        self,
        host: str = conf.host,
        username: str = conf.username,
        password: Optional[SecretStr] = conf.password,
        api_key: Optional[SecretStr] = conf.api_key,
    ):
        self.host = host
        self.username = username
        self.password = password
        self.api_key = api_key
        self.auth_mode = conf.auth_mode
        self.token_expiration = datetime.now(timezone.utc)
        self.http_client = urllib3.PoolManager(
            maxsize=conf.maxsize,
            cert_reqs='CERT_NONE',
        )
        self._create_client()

    # ── Client initialization ─────────────────────────────────────────────────

    def _create_client(self):
        """Dispatch to the correct auth method."""
        if self.auth_mode == AuthMode.LDAP:
            self._create_client_ldap()
        else:
            self._create_client_api_key()

    def _create_client_ldap(self):
        """Create Minio client using temporary LDAP credentials."""
        credentials = self._get_credentials()
        self.token_expiration = parse_datetime(credentials['Expiration']).astimezone(timezone.utc)
        self.client = minio.Minio(
            endpoint=self.host,
            access_key=credentials['AccessKeyId'],
            secret_key=credentials['SecretAccessKey'],
            session_token=credentials['SessionToken'],
            secure=True,
            http_client=self.http_client,
        )
        logger.info("Minio client created via LDAP, token expires at %s", self.token_expiration)
        self.client.list_buckets()

    def _create_client_api_key(self):
        """Create Minio client using static API key credentials."""
        self.client = minio.Minio(
            endpoint=self.host,
            access_key=self.username,
            secret_key=self.api_key.get_secret_value(),
            secure=True,
            http_client=self.http_client,
        )
        logger.info("Minio client created via API key")
        self.client.list_buckets()

    def _get_credentials(self, action: str = "AssumeRoleWithLDAPIdentity", version: str = "2011-06-15") -> dict:
        """Fetch temporary STS credentials for LDAP user."""
        params = {
            "Action": action,
            "LDAPUsername": self.username,
            "LDAPPassword": self.password.get_secret_value(),
            "Version": version,
            "DurationSeconds": conf.token_expiration,
        }
        response = requests.post(f"https://{self.host}", params=params, verify=False).content

        root = etree.fromstring(response)
        et = root.find("{*}AssumeRoleWithLDAPIdentityResult/{*}Credentials")
        if et is None:
            raise RuntimeError("Failed to parse LDAP credentials from STS response")

        credentials = {}
        for element in et:
            _, _, tag = element.tag.rpartition("}")
            credentials[tag] = element.text

        return credentials

    @staticmethod
    def dict_to_tags(tags: dict):
        converted = Tags.new_object_tags()
        for k, v in tags.items():
            converted[k] = v

        return converted

    @renew_authentication_token
    def upload_object(self,
                      file_path_or_file_object: str | BytesIO,
                      bucket_name: str,
                      metadata: dict | None = None,
                      tags: dict | None = None,
                      ):
        """
        Method to upload file to Minio storage
        :param file_path_or_file_object: file path or BytesIO object
        :param bucket_name: bucket name
        :param metadata: object metadata
        :param tags: object tags
        :return: response from Minio
        """
        file_object = file_path_or_file_object

        if type(file_path_or_file_object) == str:
            file_object = open(file_path_or_file_object, "rb")
            length = sys.getsizeof(file_object)
        else:
            length = file_object.getbuffer().nbytes

        # Handle metadata - remove empty values as it caueses S3 error at upload
        metadata = metadata or {}
        metadata = {k: v for k, v in metadata.items() if v}

        # Handle tags if provided
        if tags:
            tags = self.dict_to_tags(tags)

        # Just to be sure that pointer is at the beginning of the content
        file_object.seek(0)

        # TODO - check that bucket exists and it has access to it, maybe also try to create one
        logger.info(f"Uploading object to bucket {bucket_name}: {file_object.name}")
        response = self.client.put_object(
            bucket_name=bucket_name,
            object_name=file_object.name,
            data=file_object,
            length=length,
            content_type=mimetypes.guess_type(file_object.name)[0],
            metadata=metadata,
            tags=tags,
        )

        return response

    @renew_authentication_token
    def download_object(self, bucket_name: str, object_name: str):
        response = None
        try:
            object_name = object_name.replace("//", "/")
            file_data = self.client.get_object(bucket_name, object_name)
            response = file_data.read()
        except minio.error.S3Error as err:
            logger.error(f"Error downloading object {object_name} from bucket {bucket_name}: {err}", exc_info=True)

        return response

    @renew_authentication_token
    def object_exists(self, object_name: str, bucket_name: str) -> bool:
        """Check whether object exists in specified bucket by its object name"""
        exists = False
        try:
            self.client.stat_object(bucket_name, object_name)
            exists = True
        except minio.error.S3Error as e:
            pass

        return exists

    @renew_authentication_token
    def list_objects(self,
                     bucket_name: str,
                     prefix: str | None = None,
                     recursive: bool = False,
                     start_after: str | None = None,
                     include_user_meta: bool = True,
                     include_version: bool = False):
        """Return all object of specified bucket"""
        objects = []
        try:
            response = self.client.list_objects(bucket_name, prefix, recursive, start_after, include_user_meta,
                                                include_version)
            objects.extend(response)
        except minio.error.S3Error as err:
            logger.error(f"Error listing objects in bucket {bucket_name} with prefix {prefix}: {err}", exc_info=True)

        return objects


if __name__ == '__main__':
    # Test Minio API
    service = S3Minio()
    buckets = service.client.list_buckets()
    objects = service.list_objects(bucket_name='analyses')
    print(buckets)

