# `overpack`

`overpack` is a Python library to read, write, manipulate, and manage Veeva Vault VPK.

## Table of contents
- [Recipes](#recipes)
  - [Reading](#reading)
  - [Comparing](#comparing)
  - [Manipulating](#manipulating)
  - [Writing](#writing)

## Recipes

The following recipes build on [KANBAN-BOARD-CONFIG.vpk](https://github.com/veeva/Vault-Kanban-Board/blob/main/KANBAN-BOARD-CONFIG.vpk).

```python
from overpack import Vpk
from rich.pretty import pprint
```

### Reading
```python
vpk = Vpk.load("/path/to/KANBAN-BOARD-CONFIG.vpk")
pprint(vpk)
```

results in

```bash
Vpk(
│   manifest=Manifes(<ommited for aesthetic reasons>),
│   components=[
│   │   <a very long list of `ConfigurationComponent` and `DataComponent` objects, ommited for aesthetic reasons>
│   ],
│   codes=[
│   │   <a list of `JavaSdkCode` objects, ommited for aesthetic reasons>
│   ]
)
```

### Comparing

### Manipulating

### Writing
