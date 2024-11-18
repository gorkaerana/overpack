from __future__ import annotations
import csv
from collections import deque
from pathlib import Path
from typing import TypeAlias
import xml.etree.ElementTree as ET
from zipfile import Path as ZipPath


import msgspec


from meddle import Command


Record: TypeAlias = dict[str, str]  # TODO: maybe support more data types?


def first_child_with_suffix(path: ZipPath, suffix: str) -> ZipPath | None:
    return next((p for p in path.iterdir() if p.suffix.lower() == suffix), None)


def is_data_component(path: ZipPath) -> bool:
    return first_child_with_suffix(path, ".csv") and first_child_with_suffix(
        path, ".xml"
    )


def is_configuration_component(path: ZipPath) -> bool:
    return first_child_with_suffix(path, ".mdl") and first_child_with_suffix(
        path, ".md5"
    )


class JavaSdkCode(msgspec.Struct):
    path: Path
    content: str

    @classmethod
    def load(cls, path: ZipPath):
        return cls(path=path._base(), content=path.read_text())

    def __repr__(self):
        return f"{self.__class__.__name__}(path={str(self.path)})"


class Component(msgspec.Struct):
    number: str

    @classmethod
    def load(cls, path) -> Component:
        raise NotImplementedError

    def __repr__(self):
        return f"{self.__class__.__name__}(number={repr(self.number)})"


class DataComponent(Component):
    data: list[Record]
    metadata: ET.Element

    @classmethod
    def load(cls, path: ZipPath) -> DataComponent:
        csv_path = first_child_with_suffix(path, ".csv")
        if csv_path is None:
            raise ValueError(f"Expected a .csv file in data component {str(path)}.")
        records = list(csv.DictReader(csv_path.open()))
        xml_path = first_child_with_suffix(path, ".xml")
        if xml_path is None:
            raise ValueError(f"Expected a .xml file in data component {str(path)}.")
        return cls(
            number=path.stem, data=records, metadata=ET.fromstring(xml_path.read_text())
        )


class Md5(msgspec.Struct):
    hash: str
    component_info: str

    @classmethod
    def load(cls, path: ZipPath) -> Md5:
        hash_, component_info = path.read_text().split()
        return cls(hash=hash_, component_info=component_info)


class ConfigurationComponent(Component):
    mdl: Command
    md5: Md5

    @classmethod
    def load(cls, path: ZipPath) -> ConfigurationComponent:
        mdl_path = first_child_with_suffix(path, ".mdl")
        if mdl_path is None:
            raise ValueError(
                f"Expected a .mdl file in configuration component {str(path)}."
            )
        md5_path = first_child_with_suffix(path, ".md5")
        if md5_path is None:
            raise ValueError(
                f"Expected a .md5 file in configuration component {str(path)}."
            )
        return cls(
            number=path.stem,
            mdl=Command.loads(mdl_path.read_text()),
            md5=Md5.load(md5_path),
        )


class Vpk(msgspec.Struct):
    manifest: ET.Element
    components: list[Component]
    codes: list[JavaSdkCode]

    @classmethod
    def load(cls, path: Path) -> Vpk:
        components: list[Component] = []
        for component_dir in ZipPath(path, "components/").iterdir():
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
                # TODO: what the heck is up with the configuration component
                # containing a .json file?
                # Multichannel_vsdk-http-sample-components.vpk/components/00070/
                # Clinical_vsdk-http-sample-components.vpk/components/0070/
                # RIM_vsdk-http-sample-components.vpk/components/00070/
                # Base_vsdk-http-sample-components.vpk/components/00070/
                # Quaility_vsdk-http-sample-components.vpk/components/00070/
                raise ValueError(
                    f"{str(component_dir)} is neither a data nor a configuration component."
                )
            components.append(component)
        # Sadly `zipfile.Path` does not implement `pathlib.Path.rglob`, so we
        # are forced to recurse manually
        codes: list[JavaSdkCode] = []
        q = deque(list(ZipPath(path).iterdir()))
        while q:
            p = q.popleft()
            if p.is_dir():
                q.extend(p.iterdir())
            elif (p.suffix == ".java") and ("__MACOSX" not in str(p)):
                # Second clause ought to be unnecessary but people edit VPK's
                # in MacOS, leaving unspec'd stuff behind
                codes.append(JavaSdkCode.load(p))

        return cls(
            manifest=ET.fromstring(ZipPath(path, "vaultpackage.xml").read_text()),
            components=components,
            codes=codes,
        )
