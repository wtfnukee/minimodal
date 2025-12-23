"""Tests for volume persistence."""

import sys
import pytest
import tempfile
from pathlib import Path

# Get the host Python version for container compatibility
HOST_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"


class TestVolumeAPI:
    """Test Volume API methods for file upload/download."""

    def test_volume_upload_download_via_api(self, server_process, client, temp_dir):
        """Test volume file upload and download via API."""
        from minimodal import Volume

        vol = Volume.persisted("test-vol-api")

        # Create a temp file to upload
        upload_file = temp_dir / "upload.txt"
        upload_file.write_text("local_content")

        # Upload via API
        vol.put_file(str(upload_file), "/uploaded.txt")

        # Download and verify
        download_file = temp_dir / "download.txt"
        vol.get_file("/uploaded.txt", str(download_file))
        assert download_file.read_text() == "local_content"

    def test_volume_list_files(self, server_process, client, temp_dir):
        """Test listing files in a volume."""
        from minimodal import Volume

        vol = Volume.persisted("test-vol-list")

        # Create temp files
        file1 = temp_dir / "file1.txt"
        file2 = temp_dir / "file2.txt"
        file1.write_text("content1")
        file2.write_text("content2")

        vol.put_file(str(file1), "/file1.txt")
        vol.put_file(str(file2), "/file2.txt")

        files = vol.list_files("/")
        assert "file1.txt" in files
        assert "file2.txt" in files

    def test_volume_delete_file(self, server_process, client, temp_dir):
        """Test deleting a file from a volume."""
        from minimodal import Volume

        vol = Volume.persisted("test-vol-delete")

        # Create and upload a file
        upload_file = temp_dir / "deleteme.txt"
        upload_file.write_text("to_delete")
        vol.put_file(str(upload_file), "/deleteme.txt")

        # Verify it exists
        files = vol.list_files("/")
        assert "deleteme.txt" in files

        # Delete it
        vol.delete_file("/deleteme.txt")

        # Verify it's gone
        files = vol.list_files("/")
        assert "deleteme.txt" not in files


class TestVolumeInDocker:
    """Test volumes mounted into Docker containers.

    These tests require Docker to be running and available.
    Functions use Images to ensure Docker execution.
    """

    def test_volume_write_then_read(
        self, server_process, worker_process, client, volume_path
    ):
        """Test writing to volume from Docker container then reading from host."""
        from minimodal import App, Volume, Image

        vol = Volume.persisted("test-vol-docker-1")

        # Use Image to ensure Docker execution
        image = Image.debian_slim(python_version=HOST_PYTHON_VERSION)

        app = App("test-volume-write-docker")

        @app.function(image=image, volumes={"/data": "test-vol-docker-1"})
        def write_to_volume():
            with open("/data/test.txt", "w") as f:
                f.write("Hello from container!")
            return "written"

        app.deploy()

        result = write_to_volume.remote()
        assert result == "written"

        # Verify file exists on host
        assert (volume_path / "test-vol-docker-1" / "test.txt").exists()
        assert (
            volume_path / "test-vol-docker-1" / "test.txt"
        ).read_text() == "Hello from container!"

    def test_volume_data_persists_across_invocations(
        self, server_process, worker_process, client
    ):
        """Test that volume data persists between Docker function calls."""
        from minimodal import App, Volume, Image

        vol = Volume.persisted("test-vol-persist-docker")
        image = Image.debian_slim(python_version=HOST_PYTHON_VERSION)

        app = App("test-volume-persist-docker")

        @app.function(image=image, volumes={"/data": "test-vol-persist-docker"})
        def append_to_file(text):
            path = "/data/log.txt"
            existing = ""
            try:
                with open(path) as f:
                    existing = f.read()
            except FileNotFoundError:
                pass
            with open(path, "w") as f:
                f.write(existing + text + "\n")
            return existing + text + "\n"

        app.deploy()

        append_to_file.remote("line1")
        append_to_file.remote("line2")
        result = append_to_file.remote("line3")

        assert "line1" in result
        assert "line2" in result
        assert "line3" in result

    def test_volume_nested_directories(self, server_process, worker_process, client):
        """Test creating nested directories in volume."""
        from minimodal import App, Volume, Image

        vol = Volume.persisted("test-vol-nested-docker")
        image = Image.debian_slim(python_version=HOST_PYTHON_VERSION)

        app = App("test-volume-nested-docker")

        @app.function(image=image, volumes={"/data": "test-vol-nested-docker"})
        def create_nested():
            import os

            os.makedirs("/data/a/b/c", exist_ok=True)
            with open("/data/a/b/c/deep.txt", "w") as f:
                f.write("deeply nested")
            return "done"

        app.deploy()

        result = create_nested.remote()
        assert result == "done"

    def test_multiple_volumes(self, server_process, worker_process, client):
        """Test mounting multiple volumes."""
        from minimodal import App, Volume, Image

        vol1 = Volume.persisted("test-multi-vol-1-docker")
        vol2 = Volume.persisted("test-multi-vol-2-docker")
        image = Image.debian_slim(python_version=HOST_PYTHON_VERSION)

        app = App("test-multi-volumes-docker")

        @app.function(
            image=image,
            volumes={
                "/data1": "test-multi-vol-1-docker",
                "/data2": "test-multi-vol-2-docker",
            },
        )
        def use_multiple_volumes():
            with open("/data1/file1.txt", "w") as f:
                f.write("volume 1")
            with open("/data2/file2.txt", "w") as f:
                f.write("volume 2")
            return "done"

        app.deploy()

        result = use_multiple_volumes.remote()
        assert result == "done"

    def test_volume_read_uploaded_file(
        self, server_process, worker_process, client, temp_dir
    ):
        """Test container can read file uploaded via API."""
        from minimodal import App, Volume, Image

        vol = Volume.persisted("test-vol-read-upload")

        # Upload a file via API
        upload_file = temp_dir / "uploaded.txt"
        upload_file.write_text("uploaded content")
        vol.put_file(str(upload_file), "/uploaded.txt")

        image = Image.debian_slim(python_version=HOST_PYTHON_VERSION)
        app = App("test-vol-read-upload")

        @app.function(image=image, volumes={"/data": "test-vol-read-upload"})
        def read_uploaded():
            with open("/data/uploaded.txt") as f:
                return f.read()

        app.deploy()

        result = read_uploaded.remote()
        assert result == "uploaded content"
