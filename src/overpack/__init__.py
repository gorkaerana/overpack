from __future__ import annotations
import csv
import json
from collections import deque
from functools import cached_property
from hashlib import md5
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TypeAlias
import xml.etree.ElementTree as ET
from zipfile import Path as ZipPath, ZipFile

import dict2xml  # type: ignore
import msgspec

from meddle import Command


Record: TypeAlias = dict[str, str]  # TODO: maybe support more data types?


def md5_hash(s: str | bytes) -> str:
    """A small utility function to compute the MD5 checksum of a given string
    or bytes `s`
    """
    return md5(s.encode() if isinstance(s, str) else s).hexdigest()


def first_child_with_suffix(path: ZipPath, suffix: str) -> ZipPath | None:
    """Return the first file path under `path` with extension `suffix`, by
    iterating with `iterdir`. Returns `None` if none could be found.
    """
    return next((p for p in path.iterdir() if p.suffix.lower() == suffix), None)


def has_child_with_suffix(path: ZipPath, suffix: str) -> bool:
    """Flags whether `path` contains a file underneath with extension `suffix`"""
    return first_child_with_suffix(path, suffix) is not None


def is_data_component(path: ZipPath) -> bool:
    """Checks whether `path`"""
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

    def dump(self, path: Path) -> Path:
        """`path` is the one to the VPK root, *before zipping*. E.g.
        /path/to/package, which will eventually result in /path/to/package.vpk
        """
        target_path = path / self.path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(self.dumps())
        return target_path


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

    def dump(self, path: Path) -> Path:
        """`path` is the one to the VPK root, *before zipping*. E.g.
        /path/to/package, which will eventually result in /path/to/package.vpk
        """
        target_path = path / "vaultpackage.xml"
        target_path.write_text(self.dumps())
        return target_path


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
        if csv_path.stem != xml_path.stem:
            raise ValueError(
                f".csv ({repr(csv_path)}) and .xml ({repr(xml_path)}) files names differ in {str(path)}."
            )
        return cls(
            number=path.stem,
            label=csv_path.stem,
            data=Data(csv_path.read_text()),
            manifest=Manifest(xml_path.read_text()),
        )

    def dump(self, path: Path) -> tuple[Path, Path]:
        """`path` is the one to the VPK root, *before zipping*. E.g.
        /path/to/package, which will eventually result in /path/to/package.vpk
        """
        if self.manifest is None:
            # With a better understanding of the contents and structure of a data
            # components manifest file, perhaps this could be generated generally
            raise ValueError(
                "Cannot serialize a data component without a manifest. Generate one first, e.g., via 'DataComponent.generate_manifest'."
            )
        subdir = path / "components" / self.number
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
        ctn, cn = self.component_type_name, self.component_name
        if (self.workflow is not None) and (self.mdl is not None):
            raise ValueError(
                f"A configuration component ought to have either a MDL or a workflow description. Got both in {repr(self.number)}."
            )

        if (self.workflow is None) and (self.mdl is None):
            raise ValueError(
                f"A configuration component ought to have either a MDL or a workflow description. Got none in {repr(self.number)}."
            )

        if self.mdl is not None:
            mdl_ctn = self.mdl.command.component_type_name
            mdl_cn = self.mdl.command.component_name
            if (mdl_ctn != ctn) or (mdl_cn != cn):
                raise ValueError(
                    "The component type name and component name specified in the file "
                    f"name and within ought to be the same. Got {repr(ctn)} and "
                    f"{repr(cn)} from the file name, and {repr(mdl_ctn)} "
                    f"and {repr(mdl_cn)} from MDL, in {repr(self.number)}."
                )
        if self.workflow is not None:
            w_cn = ".".join(
                [
                    self.workflow["procDef"]["lifecyclePublicKey"],
                    self.workflow["procDef"]["publicKey"],
                ]
            )
            if (ctn != "Workflow") or (cn != w_cn):
                raise ValueError(
                    "The component type name and component name specified in the file "
                    f"name and within the workflow JSON file ought to be the same. Got "
                    f"{repr(ctn)} and {repr(cn)} from the file name, and {repr('Workflow')} "
                    f"and {repr(w_cn)} from workflow JSON, in {repr(self.number)}."
                )

    def generate_md5(self) -> Md5:
        component_info = ".".join([self.component_type_name, self.component_name])
        if self.mdl is not None:
            return Md5(hash=md5_hash(self.mdl.raw), component_info=component_info)
        elif self.workflow is not None:
            return Md5(hash=self.workflow["checksum"], component_info=component_info)
        raise ValueError(
            f"Could not generate a 'Md5' object for {repr(self.number)} since it contains neither a .mdl file nor a .json file"
        )

    @classmethod
    def load(cls, path: ZipPath) -> ConfigurationComponent:
        md5_path = first_child_with_suffix(path, ".md5")
        if md5_path is None:
            raise ValueError(
                f"Expected a .md5 file in configuration component {str(path)}."
            )
        mdl_path = first_child_with_suffix(path, ".mdl")
        json_path = first_child_with_suffix(path, ".json")
        if mdl_path is None and json_path is not None:
            component_type_name, component_name = json_path.stem.split(".", maxsplit=1)
        elif json_path is None and mdl_path is not None:
            component_type_name, component_name = mdl_path.stem.split(".", maxsplit=1)
        else:
            raise ValueError(
                f"Expected either a .mdl or .json file in configuration component {str(path)}."
            )
        if (ci := f"{component_type_name}.{component_name}") != md5_path.stem:
            raise ValueError(
                f"File names for .json/.mdl, and .md5 files ought to be the same. Got {repr(ci)}, and {str(md5_path)}, respectively."
            )
        dep_path = first_child_with_suffix(path, ".dep")
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
        """`path` is the one to the VPK root, *before zipping*. E.g.
        /path/to/package, which will eventually result in /path/to/package.vpk
        """
        stem = f"{self.component_type_name}.{self.component_name}"
        subdir = path / "components" / self.number
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
        """`path` is the one to the VPK package. E.g., /path/to/package.vpk"""
        # For whichever reason, it is considerably faster (by an order of magnitude) to
        # operate in a temporary directory and add files to the ZIP file individually
        # with `ZipFile.write`; instead of operating in the temporary directory and
        # and creating the ZIP file via `shutil.make_archive`.
        with TemporaryDirectory() as tmp_dir, ZipFile(path, mode="w") as zip_file:
            tmp_dir_path = Path(tmp_dir)
            tmp_vpk = tmp_dir_path / path.stem
            tmp_vpk.mkdir()
            # Notice in what follows the "`dump` in one line, `ZipFile.write` in
            # the following line" pattern. The order matters since the file path
            # passed to `ZipFile.write` ought to exist
            manifest_path = self.manifest.dump(tmp_vpk)
            zip_file.write(manifest_path)
            for component in self.components:
                # TODO: do we want to rename the folders if they have been modified?
                component_file_paths = component.dump(tmp_vpk)
                p: Path
                for p in filter(None, component_file_paths):
                    zip_file.write(p)
            for code in self.codes:
                code_file_path = code.dump(tmp_vpk)
                zip_file.write(code_file_path)
