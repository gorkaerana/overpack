from pathlib import Path

from overpack import Vpk

for p in Path("./tests/vpk_examples/").glob("*.vpk"):
    if p.name in {
        "Multichannel_vsdk-http-sample-components.vpk",
        "Clinical_vsdk-http-sample-components.vpk",
        "RIM_vsdk-http-sample-components.vpk",
        "Base_vsdk-http-sample-components.vpk",
        "Quaility_vsdk-http-sample-components.vpk",
    }:
        continue
    vpk = Vpk.load(p)
    print(p)
    print(vpk)
    print()
