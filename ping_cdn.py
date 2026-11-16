import asyncio
import ipaddress
import json
import sys
import time

INPUT_FILE = "cdn_ips.json"
OUTPUT_FILE = "alive_gcore_ips.json"
CONCURRENCY = 250
TIMEOUT = 1.5


def load_gcore_ips():
    """Загружает все Anycast, точные edge-серверы и разворачивает публичные подсети Gcore."""
    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Ошибка: файл {INPUT_FILE} не найден.")
        sys.exit(1)

    ips = set()

    # 1. Anycast и точные edge IPv4
    for key in ("anycast_primary_ipv4", "edge_servers_ipv4"):
        for ip in data.get(key, []):
            if "." in ip and ":" not in ip:
                ips.add(ip.strip())

    # 2. Публичные подсети Gcore
    for cidr in data.get("subnets_ipv4", []):
        if cidr.startswith("172.31.") or cidr.startswith("10.") or cidr.startswith("192.168."):
            continue
        try:
            net = ipaddress.ip_network(cidr, strict=False)
            if net.prefixlen >= 24:
                # Для диапазонов /24 - /32 разворачиваем хосты полностью
                for ip in (net.hosts() if net.prefixlen <= 30 else net):
                    ips.add(str(ip))
            else:
                # Для более крупных берём граничные хосты
                ips.add(str(net.network_address + 1))
                ips.add(str(net.network_address + 2))
                ips.add(str(net.broadcast_address - 1))
        except Exception:
            continue

    return sorted(ips)


async def check_tcp(ip, sem):
    """Асинхронный замер TCP handshake по 443 порту."""
    async with sem:
        t0 = time.time()
        try:
            r, w = await asyncio.wait_for(asyncio.open_connection(ip, 443), timeout=TIMEOUT)
            latency_ms = round((time.time() - t0) * 1000, 1)
            w.close()
            await w.wait_closed()
            return ip, latency_ms
        except Exception:
            return ip, None


async def main():
    ips = load_gcore_ips()
    total = len(ips)

    print("=== Gcore CDN Latency Checker ===")
    print(f"Всего адресов Gcore для проверки: {total}")
    print(f"Запуск асинхронного TCP-пинга (порт 443, потоков: {CONCURRENCY})...\n")

    t_start = time.time()
    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = [asyncio.create_task(check_tcp(ip, sem)) for ip in ips]

    done_results = await asyncio.gather(*tasks)
    elapsed = time.time() - t_start

    alive_results = [
        {"ip": ip, "latency_ms": ms}
        for ip, ms in done_results
        if ms is not None
    ]
    alive_results.sort(key=lambda x: x["latency_ms"])
    alive_count = len(alive_results)

    # Сохраняем в JSON
    output_data = {
        "provider": "Gcore CDN",
        "summary": {
            "total_tested": total,
            "total_alive": alive_count,
            "check_duration_sec": round(elapsed, 2),
            "timestamp": int(time.time()),
        },
        "alive_ips": alive_results,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"Проверка завершена за {elapsed:.2f} сек!")
    print(f"Ответили (живые): {alive_count} из {total} ({round(alive_count / total * 100, 1)}%)\n")

    print("Топ-15 самых быстрых IP Gcore CDN:")
    print("-" * 34)
    print(f"{'IP адрес':<18} | {'Пинг (мс)':<10}")
    print("-" * 34)
    for item in alive_results[:15]:
        print(f"{item['ip']:<18} | {item['latency_ms']:<10}")
    print("-" * 34)
    print(f"\nПолный список всех {alive_count} ответивших IP сохранён в: {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
