"""Создаёт проверяемые копии шаблонов, не изменяя исходные алгоритмы."""

from pathlib import Path
import shutil


PROJECT_DIR = Path(__file__).resolve().parent.parent
BUILD_DIR = PROJECT_DIR / ".local_build"
SOURCE_DIRS = (PROJECT_DIR / "algorithms", PROJECT_DIR / "examples")
TAGS_PLACEHOLDER = "<<TAGS>>"


def render_source(source: str) -> str:
    rendered_lines: list[str] = []
    for line in source.splitlines(keepends=True):
        if line.strip() == TAGS_PLACEHOLDER:
            indentation = line[: len(line) - len(line.lstrip())]
            rendered_lines.append(
                f"{indentation}pass  # Локальная замена шаблона {TAGS_PLACEHOLDER}\n"
            )
        else:
            rendered_lines.append(line)
    return "".join(rendered_lines)


def main() -> None:
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir()

    source_files = sorted(
        path
        for source_dir in SOURCE_DIRS
        for path in source_dir.rglob("*.py")
    )
    for source_path in source_files:
        target_path = BUILD_DIR / source_path.relative_to(PROJECT_DIR)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(
            render_source(source_path.read_text(encoding="utf-8")),
            encoding="utf-8",
        )
        print(f"  {source_path.relative_to(PROJECT_DIR)}")


if __name__ == "__main__":
    main()
