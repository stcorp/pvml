import os
from collections import namedtuple
from dataclasses import dataclass
import datetime
import glob
import logging
from pathlib import Path
import re
import shutil
from typing import List, Optional

from . import joborder
from .config import Config
from .exceptions import Error


logger = logging.getLogger("pvml")


Product = namedtuple("Product", ["product_type", "filename", "reference", "validity_start", "validity_stop"])


@dataclass
class ArchiveOptions:
    use_symlinks: bool = False


def parse_config(config, tree):
    options = config.archive_options
    if options is None:
        options = ArchiveOptions()
    value = tree.findtext("useSymlinks")
    if value in ["true", "1"]:
        options.use_symlinks = True
    return options


class Archive:
    def __init__(self, config: Config):
        self._config = config
        if self._config.archive_options is not None:
            self._archive_options = self._config.archive_options
        else:
            self._archive_options = ArchiveOptions()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass

    def _determine_product_type(self, filename: str) -> Optional[str]:
        for product_type, product_type_config in self._config.product_types.items():
            if product_type_config.match_expression is not None:
                if re.match(product_type_config.match_expression, filename) is not None:
                    return product_type
        return None

    def _product_type_uses_stem(self, product_type: str) -> bool:
        product_type_config = self._config.product_types.get(product_type)
        if product_type_config is not None:
            if product_type_config.stem_expression is not None or product_type_config.stem_as_physical_dbl:
                return True
        return False

    def _extract_start_time(self, product_type: str, filename: str) -> Optional[datetime.datetime]:
        try:
            product_type_config = self._config.product_types[product_type]
        except KeyError:
            return None
        if product_type_config.start_time_expression is None or product_type_config.start_time_format is None:
            return None
        match_obj = re.match(product_type_config.start_time_expression, filename)
        if match_obj is None:
            return None
        return self._config.parse_timestamp(match_obj.group(1), [product_type_config.start_time_format])

    def _extract_stop_time(self, product_type: str, filename: str) -> Optional[datetime.datetime]:
        try:
            product_type_config = self._config.product_types[product_type]
        except KeyError:
            return None
        if product_type_config.stop_time_expression is None or product_type_config.stop_time_format is None:
            return None
        match_obj = re.match(product_type_config.stop_time_expression, filename)
        if match_obj is None:
            return None
        return self._config.parse_timestamp(match_obj.group(1), [product_type_config.stop_time_format])

    def _copy_product(self, path: Path, target_path: Path):
        if not path.exists():
            raise Error(f"cannot access '{path}' (does not exist or no permission)")
        dest = target_path / path.name
        if self._archive_options.use_symlinks:
            logger.info(f"creating symlink for {os.path.basename(path)} in working directory")
            try:
                if dest.is_symlink():
                    # remove old link
                    os.remove(dest)
                os.symlink(path, dest)
            except Exception as e:
                raise Error(f"could not create symlink for {path} at {dest} ({e.__str__()})")
        else:
            logger.info(f"copying {os.path.basename(path)} to workspace directory")
            try:
                if path.is_dir():
                    shutil.copytree(path, dest)
                else:
                    shutil.copy(path, dest)
            except Exception as e:
                raise Error(f"could not copy {path} to {dest} ({e.__str__()})")

    def resolve_reference(self, reference: str, product_type: str) -> Product:
        config_product_type = self._determine_product_type(os.path.basename(reference.rstrip(os.sep)))
        if config_product_type is not None and product_type != config_product_type:
            raise Error(f"inconsistent product types for product reference '{reference}' ('{product_type}' " +
                        f"specified, '{config_product_type}' from config)")

        if not os.path.isabs(reference):
            if self._config.job_config_file is not None:
                reference = os.path.join(self._config.job_config_file.parent, reference)
            else:
                raise Error(f"product reference '{reference}' is not an absolute path")

        if ('?' in reference or '*' in reference or '[' in reference) and \
                not self._product_type_uses_stem(product_type):
            files = glob.glob(reference)
            if len(files) == 0:
                raise Error(f"could not find input product matching pattern '{reference}'")
            if len(files) > 1:
                raise Error("found multiple matches for input product pattern '{reference}'")
            reference = files[0]

        reference = os.path.abspath(reference.rstrip(os.sep))
        filename = os.path.basename(reference)

        validity_start = self._extract_start_time(product_type, filename)
        validity_stop = self._extract_stop_time(product_type, filename)
        return Product(product_type, filename, reference, validity_start, validity_stop)

    def resolve_mmfi(self, product_type: str, retrieval_mode: str,
                     validity_start: datetime.datetime, validity_stop: datetime.datetime,
                     dt0: datetime.timedelta, dt1: datetime.timedelta) -> List[Product]:
        # The local archive backend does not support querying of products, so we always return an empty list
        return []

    def retrieve(self, inputs: List[joborder.Input], target_path: Path) -> None:
        logger.info("retrieving input products")
        seen = set()
        for input in inputs:
            use_stem = self._product_type_uses_stem(input.product_type)
            for product in input.products:
                root = product.reference.rstrip(os.sep)
                if root in seen:
                    continue

                if use_stem:
                    stem = root + "*"
                    paths = glob.glob(stem)
                    if len(paths) == 0:
                        raise Error(f"could not find any files matching the pattern '{stem}'")
                    for path in paths:
                        self._copy_product(Path(path), target_path)
                else:
                    self._copy_product(Path(root), target_path)

                seen.add(root)

    def ingest(self, outputs: List[joborder.Output], inputs: List[joborder.Input]) -> None:
        # The local archive backend does not support storing any outputs
        pass
