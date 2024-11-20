from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from overpack import Vpk, DataComponent, ConfigurationComponent


def f(vpk_path: Path, out_dir: Path) -> Path:
    vpk = Vpk.load(vpk_path)
    for c in vpk.components:
        if isinstance(c, ConfigurationComponent):
            c.generate_md5()
        elif isinstance(c, DataComponent):
            c.generate_manifest(
                object_name="object_name_placeholder",
                data_type="data_type_placeholder",
                action="action_placeholder",
            )
    rp = out_dir / (vpk_path.name if vpk_path.is_file() else f"{vpk_path.stem}.vpk2")
    vpk.dump(rp)
    print(f"Wrote {rp}")
    return rp


vpk_files = [
    p
    for p in Path("./tests/vpk_examples/").iterdir()
    if (p.suffix == ".vpk") or p.is_dir()
]
out_dir = Path("/tmp/vpk_playground")
out_dir.mkdir(exist_ok=True)
max_workers = len(vpk_files)
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(f, p, out_dir) for p in vpk_files]
    for future in as_completed(futures):
        future.result()
