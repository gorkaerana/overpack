from __future__ import annotations
import csv
import json
from collections import deque
from functools import lru_cache
from hashlib import md5
from io import StringIO
from pathlib import Path
from typing import TypeAlias
import xml.etree.ElementTree as ET
from zipfile import Path as ZipPath

import dict2xml  # type: ignore
import msgspec

from meddle import Command  # type: ignore


Record: TypeAlias = dict[str, str]  # TODO: maybe support more data types?


def md5_hash(s: str | bytes) -> str:
    return md5(s.encode() if isinstance(s, str) else s).hexdigest()


def first_child_with_suffix(path: ZipPath, suffix: str) -> ZipPath | None:
    return next((p for p in path.iterdir() if p.suffix.lower() == suffix), None)


def has_child_with_suffix(path: ZipPath, suffix: str) -> bool:
    return first_child_with_suffix(path, suffix) is not None


def is_data_component(path: ZipPath) -> bool:
    return has_child_with_suffix(path, ".csv") and has_child_with_suffix(path, ".xml")


def is_configuration_component(path: ZipPath) -> bool:
    return (
        has_child_with_suffix(path, ".mdl") or has_child_with_suffix(path, ".json")
    ) and has_child_with_suffix(path, ".md5")


class JavaSdkCode(msgspec.Struct):
    path: Path
    content: str

    @classmethod
    def load(cls, path: ZipPath):
        return cls(
            path=next(p for p in Path(str(path)).parents if p.suffix == ".vpk"),
            content=path.read_text(),
        )

    def __repr__(self):
        return f"{self.__class__.__name__}(path={str(self.path)})"


class Component(msgspec.Struct):
    number: str

    @classmethod
    def load(cls, path) -> Component:
        raise NotImplementedError

    def __repr__(self):
        return f"{self.__class__.__name__}(number={repr(self.number)})"


class Data(msgspec.Struct, frozen=True):
    raw: str

    @property
    @lru_cache
    def records(self) -> list[Record]:
        return list(csv.DictReader(StringIO(self.raw)))

    @property
    @lru_cache
    def checksum(self) -> str:
        return md5_hash(self.raw)

    def dumps(self) -> str:
        return self.raw


class Manifest(msgspec.Struct, frozen=True):
    raw: str

    @property
    @lru_cache
    def parsed(self) -> ET.Element:
        return ET.Element(self.raw)

    def dumps(self) -> str:
        return self.raw


class DataComponent(Component):
    data: Data
    manifest: Manifest | None = None

    def generate_manifest(
        self,
        label: str,
        object_name: str,
        data_type: str,
        action: str,
        step_required: bool = False,
        record_migration_mode: bool = False,
    ) -> Manifest:
        d = {
            "stepheader": {
                "label": label,
                "steprequired": step_required,
                "checksum": self.data.checksum,
                "datastepheader": {
                    "object": object_name,
                    "idparam": None,
                    "datatype": data_type,
                    "action": action,
                    "recordmigrationmode": record_migration_mode,
                    "recordcount": len(self.data.records),
                },
            }
        }
        s = dict2xml.Converter("").build(d, closed_tags_for=[None])
        return Manifest(s)

    @classmethod
    def load(cls, path: ZipPath) -> DataComponent:
        csv_path = first_child_with_suffix(path, ".csv")
        if csv_path is None:
            raise ValueError(f"Expected a .csv file in data component {str(path)}.")
        xml_path = first_child_with_suffix(path, ".xml")
        if xml_path is None:
            raise ValueError(f"Expected a .xml file in data component {str(path)}.")
        return cls(
            number=path.stem,
            data=Data(csv_path.read_text()),
            manifest=Manifest(xml_path.read_text()),
        )


class Md5(msgspec.Struct):
    hash: str
    component_info: str

    @classmethod
    def load(cls, path: ZipPath) -> Md5:
        return cls(*path.read_text().split())

    def dumps(self) -> str:
        return f"{self.hash} {self.component_info}"


class Mdl(msgspec.Struct, frozen=True):
    raw: str

    @property
    @lru_cache
    def command(self) -> Command:
        return Command.loads(self.raw)

    def dumps(self) -> str:
        return self.raw


class ConfigurationComponent(Component):
    md5: Md5 | None = None
    mdl: Mdl | None = None
    workflow: dict | None = None
    dep: list[Record] | None = None

    def __post_init__(self):
        if (self.workflow is not None) and (self.mdl is not None):
            raise ValueError(
                f"A configuration component ought to have either a MDL or a workflow description. Got both in {repr(self.number)}."
            )

        if (self.workflow is None) and (self.mdl is None):
            raise ValueError(
                f"A configuration component ought to have either a MDL or a workflow description. Got none in {repr(self.number)}."
            )

    def generate_md5(self) -> Md5:
        if self.mdl is not None:
            return Md5(
                hash=md5_hash(self.mdl.raw),
                component_info=".".join(
                    [
                        self.mdl.command.component_type_name,
                        self.mdl.command.component_name,
                    ]
                ),
            )
        elif self.workflow is not None:
            return Md5(
                hash=self.workflow["checksum"],
                component_info=".".join(
                    [
                        self.workflow["procDef"]["lifecyclePublicKey"],
                        self.workflow["procDef"]["publicKey"],
                    ]
                ),
            )
        raise Exception("Unreachable")

    @classmethod
    def load(cls, path: ZipPath) -> ConfigurationComponent:
        md5_path = first_child_with_suffix(path, ".md5")
        if md5_path is None:
            raise ValueError(
                f"Expected a .md5 file in configuration component {str(path)}."
            )
        mdl_path = first_child_with_suffix(path, ".mdl")
        json_path = first_child_with_suffix(path, ".json")
        if (mdl_path is None) and (json_path is None):
            raise ValueError(
                f"Expected either a .mdl or .json file in configuration component {str(path)}."
            )
        dep_path = first_child_with_suffix(path, ".dep")
        return cls(
            number=path.stem,
            md5=Md5.load(md5_path),
            mdl=None if mdl_path is None else Mdl(mdl_path.read_text()),
            workflow=None if json_path is None else json.loads(json_path.read_text()),
            dep=None if dep_path is None else list(csv.DictReader(dep_path.open())),
        )


class Vpk(msgspec.Struct):
    manifest: Manifest
    components: list[Component]
    codes: list[JavaSdkCode]

    @classmethod
    def load(cls, path: Path) -> Vpk:
        components: list[Component] = []
        for component_dir in ZipPath(path, "components/").iterdir():
            component: Component
            if not component_dir.is_dir():
                # This should be unnecessary as per the specs, but people
                # edit VPK files in MacOS, which leaves behind stuff like
                # `.DS_Store` files
                continue
            if is_data_component(component_dir):
                component = DataComponent.load(component_dir)
            elif is_configuration_component(component_dir):
                component = ConfigurationComponent.load(component_dir)
            else:
                raise ValueError(
                    f"{str(component_dir)} is neither a data nor a configuration component."
                )
            components.append(component)
        # Sadly `zipfile.Path` does not implement `pathlib.Path.rglob`, so we
        # are forced to recurse manually
        codes: list[JavaSdkCode] = []
        q = deque(ZipPath(path).iterdir())
        while q:
            p = q.popleft()
            if p.is_dir() and (p.name != "__MACOSX"):
                # Second clause ought to be unnecessary but people edit VPK's
                # in MacOS, leaving unspec'd stuff behind
                q.extend(p.iterdir())
            elif p.suffix == ".java":
                codes.append(JavaSdkCode.load(p))

        return cls(
            manifest=Manifest(ZipPath(path, "vaultpackage.xml").read_text()),
            components=components,
            codes=codes,
        )
