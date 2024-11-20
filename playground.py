from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from overpack import Vpk, DataComponent, ConfigurationComponent


def f(p: Path) -> Path:
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
    rp = Path("/tmp/vpk_playground") / f"{p.name}"
    # rp = Path("/mnt/c/Users/GorkaEraña/Downloads/kaka") / f"{p.name}"
    vpk.dump(rp)
    print(f"Wrote {rp}")
    return rp


vpk_files = list(Path("./tests/vpk_examples/").glob("*.vpk"))
max_workers = len(vpk_files)
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(f, p) for p in vpk_files]
    for future in as_completed(futures):
        future.result()
