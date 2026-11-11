"""Feasibility test: can we download a PriceFull file of Shufersal, Rami Levy, Yohananof and Hazi Hinam from a GitHub server (outside Israel)?
Prints what was downloaded and a few sample products. Nothing is stored."""
import asyncio, gzip, io, re, zipfile, traceback
from il_supermarket_scarper import ScarpingTask, ScraperFactory
from il_supermarket_scarper.utils import _now
from il_supermarket_scarper.utils.file_types import FileTypesFilters

def text_of(content):
    for opener in (lambda c: gzip.decompress(c), lambda c: zipfile.ZipFile(io.BytesIO(c)).read(zipfile.ZipFile(io.BytesIO(c)).namelist()[0]), lambda c: c):
        try:
            return opener(content).decode("utf-8", "ignore")
        except Exception:
            pass
    return ""

async def run(chain):
    print(f"\n===== {chain} =====")
    task = ScarpingTask(
        output_configuration={"output_mode": "queue", "queue_type": "memory"},
        status_configuration={"database_type": "json", "base_path": "status_logs"},
        multiprocessing=1, enabled_scrapers=[chain],
        files_types=[FileTypesFilters.PRICE_FULL_FILE])
    task.start(limit=2, when_date=_now())
    got = 0
    for name, out in task.consume().items():
        async for msg in out.queue_handler.get_all_messages():
            got += 1
            xml = text_of(msg["file_content"])
            print(f"file: {msg['file_name']}  size: {len(msg['file_content'])} bytes  xml chars: {len(xml)}")
            shown = 0
            items = re.findall(r"<Item>.*?</Item>", xml, re.S)
            print("items in file:", len(items))
            for it in items[:400]:
                nm = re.search(r"<ItemNm>(.*?)</ItemNm>|<ItemName>(.*?)</ItemName>", it, re.S)
                pr = re.search(r"<ItemPrice>(.*?)</ItemPrice>", it)
                bc = re.search(r"<ItemCode>(.*?)</ItemCode>", it)
                n = (nm.group(1) or nm.group(2)) if nm else ""
                if "חלב" in n and "תנובה" in n and "1 ליטר" in n or ("חלב" in n and "תנובה" in n and shown < 3):
                    shown += 1
                    print("  sample:", bc.group(1) if bc else "?", n.strip(), pr.group(1) if pr else "?")
    task.join()
    print(f"{chain}: {got} file(s) downloaded")
    return got

async def main():
    summary = {}
    for chain in ("SHUFERSAL", "RAMI_LEVY", "YOHANANOF", "HAZI_HINAM"):
        try:
            summary[chain] = await run(chain)
        except Exception:
            print(f"{chain}: FAILED"); traceback.print_exc(); summary[chain] = -1
    print("\n===== SUMMARY =====")
    for c, n in summary.items():
        print(f"{c}: " + ("FAILED (error)" if n < 0 else (f"OK, {n} file(s)" if n else "NO FILES (blocked or empty)")))

asyncio.run(main())
