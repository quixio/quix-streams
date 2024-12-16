import pkgutil
from pathlib import Path


def list_modules_in_dir(dir_path, current_module="", modules=None) -> list[str]:
    if not modules:
        modules = []
    for finder, name, pkg in pkgutil.iter_modules([dir_path]):
        full_module_name = current_module + ("." if current_module else "") + name
        if pkg:
            modules = list_modules_in_dir(
                finder.path + f"/{name}", full_module_name, modules
            )
        else:
            modules.append(full_module_name)
    return modules


pytest_plugins = list_modules_in_dir(
    f"{Path(__file__).parent.absolute()}/fixtures", "tests.fixtures"
)
