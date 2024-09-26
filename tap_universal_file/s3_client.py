from typing import Any
from urllib.parse import urlparse

import boto3
import fsspec


class S3FileSystem:
    """This is to overwrite the functionality of reading the files for S3."""

    def __init__(self, config: dict[str, Any]) -> None:  # noqa: FA102
        """Initialize a new S3FileManager instance.

        Args:
            config: Configuration
        """
        self.config: dict[str, Any] = config
        self.client = boto3.client("s3")
        fsconfig = {
            "anon": False,
        }
        if "AWS_ACCESS_KEY_ID" in config and "AWS_SECRET_ACCESS_KEY" in config:
            fsconfig = {
                **fsconfig,
                "key": config["AWS_ACCESS_KEY_ID"],
                "secret": config["AWS_SECRET_ACCESS_KEY"],
            }
        o = urlparse(f's3://{config["filepath"]}', allow_fragments=False)
        self.bucket = o.netloc
        self.prefix = o.path.lstrip("/")
        self.fs = fsspec.filesystem(
            protocol="s3",
            **fsconfig,
        )

    def find(self, path):  # noqa: ANN201, ANN001, ARG002
        """Return as list of s3 objects.

        Args:
            path (str): S3 path

        """
        files = []
        self.paginator = self.client.get_paginator("list_objects_v2")
        pages = self.paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)
        for page in pages:
            for obj in page["Contents"]:
                files.append(obj)  # noqa: PERF402

        return files

    def info(self, file):  # noqa: ANN001, ANN201
        """Return updated s3 file object which works with fsspec.

        Args:
            file (dict): file object
        """
        # adjust keys to work with fsspec
        file["name"] = f"{self.bucket}/{file['Key']}"
        file["type"] = "file"
        file["size"] = file["Size"]
        return file

    def open(self, **args):  # noqa: ANN003, D102, ANN201
        return self.fs.open(**args)
