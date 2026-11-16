import asyncio
import ipaddress
import json
import sys
import time

INPUT_FILE = "cdn_ips.json"
OUTPUT_FILE = "alive_cdn_ips.json"
CONCURRENCY = 250
TIMEOUT = 1.5


def extract_ips(provider_data):
    """Собирает IP-адреса: точные хосты + репрезентативные IP из подсетей."""
    ips = set()

    # Точные IP-адреса хостов
    for key in ("anycast_primary_ipv4", "edge_servers_ipv4", "popular_anycast_ipv4"):
        for ip in provider_data.get(key, []):
            if "." in ip and ":" not in ip:
                ips.add(ip.strip())

    # CIDR подсети: берём первые и последние хосты диапазонов
    for key in ("subnets_ipv4", "global_subnets_ipv4", "regional_edge_subnets_ipv4"):
        for cidr in provider_data.get(key, []):
            try:
                net = ipaddress.ip_network(cidr, strict=False)
                if net.num_addresses == 1:
                    ips.add(str(net.network_address))
                elif net.num_addresses <= 4:
                    for ip in net:
                        ips.add(str(ip))
                else:
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
    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Ошибка: файл {INPUT_FILE} не найден.")
        sys.exit(1)

    provider_ip_map = {}
    all_unique_ips = set()

    for provider_id, prov_data in data.items():
        ips = extract_ips(prov_data)
        provider_ip_map[provider_id] = ips
        all_unique_ips.update(ips)

    total_ips = len(all_unique_ips)
    print(f"Найдено провайдеров: {len(provider_ip_map)}")
    print(f"Всего уникальных IP для проверки: {total_ips}")
    print(f"Запуск асинхронного TCP-пинга (порт 443, потоков: {CONCURRENCY})...\n")

    t_start = time.time()
    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = [asyncio.create_task(check_tcp(ip, sem)) for ip in all_unique_ips]

    done_results = await asyncio.gather(*tasks)
    ip_latency = {ip: ms for ip, ms in done_results if ms is not None}
    elapsed = time.time() - t_start

    # Формируем итоговую структуру по компаниям
    output_result = {
        "summary": {
            "total_tested": total_ips,
            "total_alive": len(ip_latency),
            "check_duration_sec": round(elapsed, 2),
            "timestamp": int(time.time()),
        },
        "providers": {},
    }

    print(f"Проверка завершена за {elapsed:.2f} сек!\n")
    print("=" * 48)
    print(f"{'Компания / Провайдер':<22} | {'Ответили':<8} | {'Всего':<6}")
    print("-" * 48)

    for provider_id, prov_data in data.items():
        prov_ips = provider_ip_map[provider_id]
        alive_ips = [
            {"ip": ip, "latency_ms": ip_latency[ip]}
            for ip in prov_ips
            if ip in ip_latency
        ]
        alive_ips.sort(key=lambda x: x["latency_ms"])

        name = prov_data.get("name", provider_id)
        output_result["providers"][provider_id] = {
            "name": name,
            "alive_count": len(alive_ips),
            "total_tested": len(prov_ips),
            "fastest_ips": alive_ips,
        }

        print(f"{name:<22} | {len(alive_ips):<8} | {len(prov_ips):<6}")

    print("=" * 48)
    print(f"Итого живых IP: {len(ip_latency)} из {total_ips}\n")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_result, f, indent=2, ensure_ascii=False)

    print(f"Результаты успешно сохранены в файл: {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
