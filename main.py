from __future__ import annotations
import sys
from app.config.settings import get_settings
from app.container_f.container import Container

def main() -> int:
    settings = get_settings()
    print(
        f"Starting scraper: max_pages={settings.max_pages}, "
        f"workers={settings.worker_count}",
        flush=True,
    )

    container = Container.build(settings)
    print("Container built, starting engine...", flush=True)

    try:
        container.engine.run()
        print("Scraper completed successfully!", flush=True)
        return 0

    except KeyboardInterrupt:
        print("\nScraper interrupted by user.")
        return 1

    except Exception as exc:
        import traceback
        print(f"Fatal error: {exc}", file=sys.stderr, flush=True)
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    sys.exit(main())