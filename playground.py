from pathlib import Path

from overpack import Vpk, DataComponent, ConfigurationComponent

for p in Path("./tests/vpk_examples/").glob("*.vpk"):
    vpk = Vpk.load(p)
    for c in vpk.components:
        if isinstance(c, ConfigurationComponent):
            c.generate_md5()
        elif isinstance(c, DataComponent):
            c.generate_manifest(
                object_name="object_name_placeholder",
                data_type="data_type_placeholder",
                action="action_placeholder",
            )
    vpk.dump(Path("/mnt/c/Users/GorkaEraña/Downloads/kaka") / f"{p.name}")
