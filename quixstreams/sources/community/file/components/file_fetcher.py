import logging
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import BinaryIO, Callable, Iterable, Optional

__all__ = ("FileFetcher",)

logger = logging.getLogger(__name__)


class FileFetcher:
    """
    Serves a file's content as a stream while downloading another in the background.
    """

    def __init__(
        self, downloader: Callable[[Path], BinaryIO], filepaths: Iterable[Path]
    ):
        self._files = iter(filepaths)
        self._downloader = downloader
        self._stopped: bool = False
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._downloading_file_name: Optional[Path] = None
        self._downloading_file_content: Optional[Future] = None
        self._download_next_file()

    def __iter__(self) -> "FileFetcher":
        return self

    def __next__(self) -> tuple[Path, BinaryIO]:
        if self._stopped:
            raise StopIteration

        try:
            file_name = self._downloading_file_name
            file_content = self._downloading_file_content.result()
            self._download_next_file()
            return file_name, file_content
        except Exception as e:
            logger.error("FileFetcher encountered an error", exc_info=e)
            self.stop()
            raise e

    def stop(self):
        if not self._stopped:
            logger.debug("Stopping file download thread...")
            self._stopped = True
            self._executor.shutdown(wait=True, cancel_futures=True)

    def _download_next_file(self):
        try:
            self._downloading_file_name = next(self._files)
            logger.debug(f"Beginning download of {self._downloading_file_name}...")
            self._downloading_file_content = self._executor.submit(
                self._downloader, self._downloading_file_name
            )
        except StopIteration:
            logger.info("No further files to download.")
            self.stop()
