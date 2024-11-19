from __future__ import annotations
import csv
import json
import shutil
from collections import deque
from functools import cached_property
from hashlib import md5
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TypeAlias
import xml.etree.ElementTree as ET
from zipfile import Path as ZipPath

import dict2xml  # type: ignore
import msgspec

from meddle import Command


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
        # This is just a very verbose way of stripping the VPK's root. E.g.
        # /path/to/package.vpk/java/so/many/folders/file.java -> java/so/many/folders/file.java
        p = Path(str(path))
        stripped = p.relative_to(next(_ for _ in p.parents if _.suffix == ".vpk"))
        return cls(path=stripped, content=path.read_text())

    def __repr__(self):
        return f"{self.__class__.__name__}(path={str(self.path)})"

    def dumps(self):
        return self.content

    def dump(self, path: Path):
        target_path = path / self.path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(self.dumps())


class Component(msgspec.Struct):
    number: str

    @classmethod
    def load(cls, path) -> Component:
        raise NotImplementedError

    def dump(self, path):
        raise NotImplementedError

    def __repr__(self):
        return f"{self.__class__.__name__}(number={repr(self.number)})"


class Data(msgspec.Struct, dict=True):
    raw: str

    @cached_property
    def records(self) -> list[Record]:
        return list(csv.DictReader(StringIO(self.raw)))

    @cached_property
    def checksum(self) -> str:
        return md5_hash(self.raw)

    def dumps(self) -> str:
        return self.raw


class Manifest(msgspec.Struct, dict=True):
    raw: str

    @cached_property
    def parsed(self) -> ET.Element:
        return ET.Element(self.raw)

    def dumps(self) -> str:
        return self.raw

    def dump(self, path: Path):
        path.write_text(self.dumps())


class DataComponent(Component):
    label: str
    data: Data
    manifest: Manifest | None = None

    def generate_manifest(
        self,
        object_name: str,
        data_type: str,
        action: str,
        step_required: bool = False,
        record_migration_mode: bool = False,
    ) -> Manifest:
        d = {
            "stepheader": {
                "label": self.label,
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
        if csv_path.stem != xml_path.stem:
            raise ValueError(
                f".csv and .xml file names ought to match. Got {csv_path.stem} and {xml_path.stem}, respectively."
            )
        return cls(
            number=path.stem,
            label=csv_path.stem,
            data=Data(csv_path.read_text()),
            manifest=Manifest(xml_path.read_text()),
        )

    def dump(self, path: Path) -> tuple[Path, Path]:
        if self.manifest is None:
            # With a better understanding of the contents and structure of a data
            # components manifest file, perhaps this could be generated generally
            raise ValueError(
                "Cannot serialize a data component without a manifest. Generate one first, e.g., via 'DataComponent.generate_manifest'."
            )
        subdir = path / self.number
        subdir.mkdir(parents=True, exist_ok=True)
        csv_path = subdir / f"{self.label}.csv"
        xml_path = subdir / f"{self.label}.xml"
        csv_path.write_text(self.data.dumps())
        xml_path.write_text(self.manifest.dumps())
        return csv_path, xml_path


class Md5(msgspec.Struct):
    hash: str
    component_info: str

    @classmethod
    def load(cls, path: ZipPath) -> Md5:
        return cls(*path.read_text().split())

    def dumps(self) -> str:
        return f"{self.hash} {self.component_info}"


class Mdl(msgspec.Struct, dict=True):
    raw: str

    @cached_property
    def command(self) -> Command:
        return Command.loads(self.raw)

    def dumps(self) -> str:
        return self.raw


class ConfigurationComponent(Component):
    component_type_name: str
    component_name: str
    md5: Md5 | None = None
    mdl: Mdl | None = None
    workflow: dict | None = None
    dep: Data | None = None

    def __post_init__(self):
        if (self.workflow is not None) and (self.mdl is not None):
            raise ValueError(
                f"A configuration component ought to have either a MDL or a workflow description. Got both in {repr(self.number)}."
            )

        if (self.workflow is None) and (self.mdl is None):
            raise ValueError(
                f"A configuration component ought to have either a MDL or a workflow description. Got none in {repr(self.number)}."
            )

        if (self.mdl is not None) and (
            (self.mdl.command.component_type_name != self.component_type_name)
            or (self.mdl.command.component_name != self.component_name)
        ):
            raise ValueError(self.number)
        if (self.workflow is not None) and (
            (self.component_type_name != "Workflow")
            or (
                self.component_name
                != ".".join(
                    [
                        self.workflow["procDef"]["lifecyclePublicKey"],
                        self.workflow["procDef"]["publicKey"],
                    ]
                )
            )
        ):
            raise ValueError(self.number)

    def generate_md5(self) -> Md5:
        component_info = ".".join([self.component_type_name, self.component_name])
        if self.mdl is not None:
            return Md5(hash=md5_hash(self.mdl.raw), component_info=component_info)
        elif self.workflow is not None:
            return Md5(hash=self.workflow["checksum"], component_info=component_info)
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
        if mdl_path is None and json_path is not None:
            component_type_name, component_name = json_path.stem.split(".", maxsplit=1)
        elif json_path is None and mdl_path is not None:
            component_type_name, component_name = mdl_path.stem.split(".", maxsplit=1)
        else:
            raise Exception("Unreachable")
        return cls(
            component_type_name=component_type_name,
            component_name=component_name,
            number=path.stem,
            md5=Md5.load(md5_path),
            mdl=None if mdl_path is None else Mdl(mdl_path.read_text()),
            workflow=None if json_path is None else json.loads(json_path.read_text()),
            dep=None if dep_path is None else Data(dep_path.read_text()),
        )

    def dump(self, path: Path) -> tuple[Path, Path | None, Path | None, Path | None]:
        stem = f"{self.component_type_name}.{self.component_name}"
        subdir = path / self.number
        subdir.mkdir(parents=True, exist_ok=True)
        md5_path = subdir / f"{stem}.md5"
        md5_path.write_text(
            self.generate_md5().dumps() if self.md5 is None else self.md5.dumps()
        )
        mdl_path, workflow_path, dep_path = None, None, None
        if self.mdl is not None:
            mdl_path = subdir / f"{stem}.mdl"
            mdl_path.write_text(self.mdl.dumps())
        if self.workflow is not None:
            workflow_path = subdir / f"{stem}.json"
            workflow_path.write_text(json.dumps(self.workflow))
        if self.dep is not None:
            dep_path = subdir / f"{stem}.dep"
            dep_path.write_text(self.dep.dumps())

        return md5_path, mdl_path, workflow_path, dep_path


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

    def dump(self, path: Path):
        with TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            tmp_vpk = tmp_dir_path / path.stem
            tmp_vpk.mkdir()
            self.manifest.dump(tmp_vpk / "vaultpackage.xml")
            for component in self.components:
                # TODO: do we want to rename the folders if they have been modified?
                component.dump(tmp_vpk / "components")
            for code in self.codes:
                code.dump(tmp_vpk)
            shutil.make_archive(str(tmp_vpk), "zip")
            shutil.move(str(tmp_vpk.with_suffix(".zip")), path)
