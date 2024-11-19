from pathlib import Path

from overpack import Vpk, DataComponent, ConfigurationComponent

for p in Path("./tests/vpk_examples/").glob("*.vpk"):
    vpk = Vpk.load(p)
    print(p)
    print(vpk)
    for c in vpk.components:
        if isinstance(c, ConfigurationComponent):
            print(c.generate_md5())
        elif isinstance(c, DataComponent):
            print(
                c.generate_manifest(
                    label="label",
                    object_name="object_name",
                    data_type="data_type",
                    action="action",
                ).parsed
            )
    print()
