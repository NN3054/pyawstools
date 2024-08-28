import os
import time
import uuid
from typing import List, Optional

from ..config import Config
from ..constants import BaseEnum
from .base_client import S3Client


class S3BucketClient(S3Client):
    _bucket: BaseEnum = None

    def __init__(
        self,
        bucket: BaseEnum,
        aws_ak: Optional[str] = Config.aws_ak,
        aws_sk: Optional[str] = Config.aws_sk,
        region: Optional[str] = Config.aws_region,
        signature_version: Optional[str] = Config.aws_signature_version,
        max_pool_connections: int = Config.max_pool_connections,
    ):
        # super init
        super().__init__(
            aws_ak=aws_ak,
            aws_sk=aws_sk,
            region=region,
            signature_version=signature_version,
            max_pool_connections=max_pool_connections,
        )
        self._bucket = bucket

    def set_dispo_name(self, s3_path: str, dispo_name: str):
        self._set_dispo_name(self._bucket, s3_path, dispo_name)

    def gen_s3_path(self, filename: str):
        unique_uuid = str(uuid.uuid4().hex)

        time_str = str(time.time())

        filehash = unique_uuid + time_str

        if "." in filehash:
            filehash = filehash.replace(".", "")

        # add filextension of fp to filehash
        filextension = os.path.splitext(filename)[1]
        filehash = filehash + filextension
        return filehash

    def upload_file(
        self, local_path: str, s3_path: str = None, dispo_name: Optional[str] = None
    ) -> str:
        """Uploads a file to the images bucket. If no s3 key is provided, one will be generated.

        :param local_path: file path to upload
        :type local_path: str
        :param s3_path: s3 key the file should be uploaded to, defaults to None
        :type s3_path: str, optional
        :return: s3 key the file was uploaded to
        :rtype: str
        """

        if not s3_path:
            s3_path = self.gen_s3_path(local_path)
        self._upload_file(self._bucket, local_path, s3_path, dispo_name=dispo_name)
        return s3_path

    def upload_bytes(
        self,
        data: bytes,
        s3_path: str,
        dispo_name: Optional[str] = None,
    ):
        return self._upload_bytes(self._bucket, data, s3_path, dispo_name=dispo_name)

    def upload_file_obj(
        self, data: bytes, s3_path: str, dispo_name: Optional[str] = None
    ) -> str:
        self._upload_bytes(self._bucket, data, s3_path, dispo_name=dispo_name)
        return s3_path

    def download_file(self, s3_path: str, local_path: str):
        self._download_file(self._bucket, s3_path, local_path)

    def download_prefix(self, s3_prefix: str, local_dir: str):
        self._download_prefix(self._bucket, s3_prefix, local_dir)

    def list_keys_of_prefix(self, s3_prefix: str) -> List[str]:
        return self._list_keys_of_prefix(self._bucket, s3_prefix)

    def delete_object(self, s3_path: str):
        return self._delete_object(self._bucket, s3_path)

    def move_object(self, source_key: str, dest_key: str):
        self._move_object(self._bucket, source_key, self._bucket, dest_key)
        self.delete_object(source_key)

    def copy_object(self, source_key: str, dest_key: str):
        return self._copy_object(self._bucket, source_key, self._bucket, dest_key)

    def key_exist(self, s3_path: str) -> bool:
        return self._key_exists(self._bucket, s3_path)

    def get_file_obj(self, s3_path: str):
        return self._get_file_obj(self._bucket, s3_path)

    def copy_many(
        self,
        source_keys: List[str],
        dest_keys: List[str],
        max_workers: int = 10,
    ):
        copy_func = self.copy_object
        self._copy_many(source_keys, dest_keys, copy_func, max_workers=max_workers)

    def get_size(self, key: str) -> float:
        return self._get_size(self._bucket, key)

    def get_size_in_mb(self, key: str) -> float:
        return self._get_size_in_mb(self._bucket, key)

    def get_size_in_gb(self, key: str) -> float:
        return self._get_size_in_gb(self._bucket, key)

    def presigned_put_url(self, key: str, expires_in: int = 3600):
        return self._presigned_put_url(self._bucket, key, expires_in=expires_in)

    def presigned_get_url(self, key: str, expires_in: int = 3600):
        return self._presigned_get_url(self._bucket, key, expires_in=expires_in)
