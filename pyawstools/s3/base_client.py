import concurrent.futures
import gc
import math
import os
import time
import uuid
from io import BytesIO
from typing import List, Optional

import boto3
import botocore
from botocore.exceptions import ClientError

from ..config import Config
from ..constants import BaseEnum


class S3Client:
    def __init__(
        self,
        aws_ak: Optional[str] = Config.aws_ak,
        aws_sk: Optional[str] = Config.aws_sk,
        region: Optional[str] = Config.aws_region,
        signature_version: Optional[str] = Config.aws_signature_version,
        max_pool_connections: int = Config.max_pool_connections,
    ):
        client_config = botocore.config.Config(
            max_pool_connections=max_pool_connections,
            signature_version=signature_version,
            region_name=region,
        )
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_ak,
            aws_secret_access_key=aws_sk,
            config=client_config,
        )

    def _setup_client(self, client_config: botocore.config.Config, aws_ak, aws_sk):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_ak,
            aws_secret_access_key=aws_sk,
            config=client_config,
        )

    def _get_bucket(self, generic_bucket_name: BaseEnum) -> str:
        return generic_bucket_name.value

    def _set_dispo_name(self, bucket_name: BaseEnum, s3_path: str, dispo_name: str):
        # sets disposition name with copying
        bucket = self._get_bucket(bucket_name)

        try:
            self.s3.copy_object(
                Bucket=bucket,
                Key=s3_path,
                CopySource={"Bucket": bucket, "Key": s3_path},
                MetadataDirective="REPLACE",
                ContentDisposition=f'attachment; filename="{dispo_name}"',
            )
        except ClientError as e:
            print(f"Error setting disposition name: {e}")
            return False

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

    def _upload_file(
        self,
        bucket_name: BaseEnum,
        local_path: str,
        s3_path: str,
        dispo_name: Optional[str] = None,
    ):

        bucket = self._get_bucket(bucket_name)
        try:
            with open(local_path, "rb") as f:
                self.s3.upload_fileobj(f, bucket, s3_path)

            if dispo_name:
                self._set_dispo_name(bucket_name, s3_path, dispo_name)
        except ClientError as e:
            print(f"Error uploading file: {e}")
            return False
        return True

    def _upload_bytes(
        self,
        bucket_name: BaseEnum,
        data: bytes,
        s3_path: str,
        dispo_name: Optional[str] = None,
    ):
        bucket = self._get_bucket(bucket_name)
        try:
            with BytesIO(data) as fileobj:
                self.s3.upload_fileobj(fileobj, bucket, s3_path)

            if dispo_name:
                self._set_dispo_name(bucket_name, s3_path, dispo_name)
        except ClientError as e:
            print(f"Error uploading bytes: {e}")
            return False
        return True

    def _download_file(self, bucket_name: BaseEnum, s3_path: str, local_path: str):
        bucket = self._get_bucket(bucket_name)
        try:
            with open(local_path, "wb") as f:
                self.s3.download_fileobj(bucket, s3_path, f)
        except ClientError as e:
            print(f"Error downloading file: {e}")
            return False
        return True

    def _download_prefix(self, bucket_name: BaseEnum, s3_prefix: str, folder_path: str):
        os.makedirs(folder_path, exist_ok=True)
        bucket = self._get_bucket(bucket_name)
        try:
            response = self.s3.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)
            for obj in response.get("Contents", []):
                # create local directories if they don't exist
                local_dir = os.path.join(
                    folder_path, os.path.dirname(obj["Key"][len(s3_prefix) :])
                )
                os.makedirs(local_dir, exist_ok=True)
                local_path = os.path.join(local_dir, os.path.basename(obj["Key"]))
                with open(local_path, "wb") as f:
                    self.s3.download_fileobj(bucket, obj["Key"], f)
            return True
        except ClientError as e:
            print(f"Error downloading prefix: {e}")
            return False

    def _list_keys_of_prefix(self, bucket_name: BaseEnum, s3_prefix: str) -> List[str]:
        bucket = self._get_bucket(bucket_name)
        try:
            response = self.s3.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)
            return [obj["Key"] for obj in response.get("Contents", [])]
        except ClientError as e:
            print(f"Error listing keys of prefix: {e}")
            return []

    def _delete_object(self, bucket_name: BaseEnum, s3_path: str):
        bucket = self._get_bucket(bucket_name)
        try:
            self.s3.delete_object(Bucket=bucket, Key=s3_path)
        except ClientError as e:
            print(f"Error deleting object: {e}")
            return False
        return True

    def _move_object(
        self,
        source_bucket: BaseEnum,
        source_key: str,
        dest_bucket: BaseEnum,
        dest_key: str,
    ):
        self._copy_object(source_bucket, source_key, dest_bucket, dest_key)
        self._delete_object(source_bucket, source_key)

    def _copy_object(
        self,
        source_bucket: BaseEnum,
        source_key: str,
        dest_bucket: BaseEnum,
        dest_key: str,
    ):
        source_bucket = self._get_bucket(source_bucket)
        dest_bucket = self._get_bucket(dest_bucket)
        try:
            self.s3.copy_object(
                Bucket=dest_bucket,
                Key=dest_key,
                CopySource={"Bucket": source_bucket, "Key": source_key},
            )
        except ClientError as e:
            print(
                f"Error copying object from {source_key} in bucket {source_bucket} to target key {dest_key} in target bucket {dest_bucket}: {e}"
            )
            return False
        return True

    def _key_exists(self, bucket_name: BaseEnum, s3_path: str) -> bool:
        bucket = self._get_bucket(bucket_name)
        try:
            self.s3.head_object(Bucket=bucket, Key=s3_path)
            return True
        except ClientError:
            return False

    def _get_file_obj(self, bucket_name: BaseEnum, s3_path: str) -> BytesIO:
        bucket = self._get_bucket(bucket_name)
        try:
            response = self.s3.get_object(Bucket=bucket, Key=s3_path)
            return response["Body"].read()
        except ClientError as e:
            print(f"Error downloading bytes: {e}")
            return None

    def _copy_many(
        self,
        source_keys: List[str],
        dest_keys: List[str],
        copy_func,
        max_workers: int = 10,
        batch_size: int = 1000,
    ):
        if len(source_keys) != len(dest_keys):
            raise ValueError("source_keys and dest_keys must be the same length")

        batch_count = math.ceil(len(source_keys) / batch_size)

        for i in range(0, len(source_keys), batch_size):
            batch_source_keys = source_keys[i : i + batch_size]
            batch_dest_keys = dest_keys[i : i + batch_size]

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                futures = []
                for source_key, dest_key in zip(batch_source_keys, batch_dest_keys):
                    futures.append(executor.submit(copy_func, source_key, dest_key))
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Error in copying: {e}")

            # Force garbage collection after each batch to free up memory
            gc.collect()
            try:
                print(f"Finished batch copy {int((i/1000) + 1)}/{batch_count}")
            except Exception as e:
                print(f"Error in logging: {e}")

    def _download_many(
        self,
        keys: List[str],
        local_paths: List[str],
        download_func,
        max_workers: int = 10,
    ):
        if len(keys) != len(local_paths):
            raise ValueError("keys and local_paths must be the same length")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for key, local_path in zip(keys, local_paths):
                futures.append(executor.submit(download_func, key, local_path))
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def _get_size(self, bucket: BaseEnum, key: str) -> float:
        bucket = self._get_bucket(bucket)
        response = self.s3.head_object(Bucket=bucket, Key=key)
        return response["ContentLength"]

    def _get_size_in_mb(self, bucket: BaseEnum, key: str) -> float:
        return self._get_size(bucket, key) / 1024 / 1024

    def _get_size_in_gb(self, bucket: BaseEnum, key: str) -> float:
        return self._get_size(bucket, key) / 1024 / 1024 / 1024

    def _presigned_put_url(self, bucket: BaseEnum, key: str, expires_in: int = 3600):
        bucket = self._get_bucket(bucket)
        return self.s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_in,
        )

    def _presigned_get_url(self, bucket: BaseEnum, key: str, expires_in: int = 3600):
        bucket = self._get_bucket(bucket)
        return self.s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_in,
        )
