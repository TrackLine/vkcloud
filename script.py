#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
vkcloud_find_and_attach_fip.py

VK Cloud (OpenStack) — бесконечно выделяет floating IP из внешней сети,
пока не попадётся IP из 95.163.248.0/22. Неподходящие адреса сразу удаляет,
подходящий привязывает к ВМ и завершает работу.

Аутентификация: username/password (Keystone v3) с авто-переавторизацией.
Поддерживает параллельную работу нескольких воркеров.
"""

import ipaddress
import os
import sys
import time
import threading

# Автоматическая загрузка переменных из .env файла
try:
    from dotenv import load_dotenv
    # Загружаем переменные из .env файла (если он существует)
    load_dotenv()
except ImportError:
    # Если python-dotenv не установлен, просто продолжаем без загрузки .env
    pass

from openstack import connection
from openstack import exceptions as os_exc
from keystoneauth1 import exceptions as ks_exc

# Опциональная поддержка уведомлений через apprise
try:
    from apprise import Apprise
    APPRISE_AVAILABLE = True
except ImportError:
    APPRISE_AVAILABLE = False

# ========= НАСТРОЙКИ ПОДКЛЮЧЕНИЯ (из переменных окружения) =========
def get_auth():
    """Получает параметры аутентификации из переменных окружения."""
    auth = {
        "auth_url": os.getenv("VKCLOUD_AUTH_URL", "https://infra.mail.ru:35357/v3/"),
        "username": os.getenv("VKCLOUD_USERNAME"),
        "password": os.getenv("VKCLOUD_PASSWORD"),
        "project_id": os.getenv("VKCLOUD_PROJECT_ID"),
        "user_domain_name": os.getenv("VKCLOUD_USER_DOMAIN_NAME", "users"),
        "region_name": os.getenv("VKCLOUD_REGION_NAME", "RegionOne"),
        "interface": os.getenv("VKCLOUD_INTERFACE", "public"),
    }
    
    # Опциональные параметры
    verify = os.getenv("VKCLOUD_VERIFY")
    if verify:
        if verify.lower() == "false":
            auth["verify"] = False
        else:
            auth["verify"] = verify
    
    # Проверка обязательных параметров
    required = ["username", "password", "project_id"]
    missing = [k for k in required if not auth.get(k)]
    if missing:
        raise SystemExit(f"❌ Отсутствуют обязательные переменные окружения: {', '.join(f'VKCLOUD_{k.upper()}' for k in missing)}")
    
    return auth

# ========= ПАРАМЕТРЫ РАБОТЫ (из переменных окружения) =========
SERVER_ID_OR_NAME = os.getenv("VKCLOUD_SERVER_ID_OR_NAME")
EXT_NET_NAME = os.getenv("VKCLOUD_EXT_NET_NAME")  # None если не задано
PORT_ID = os.getenv("VKCLOUD_PORT_ID")  # None если не задано
SLEEP_BETWEEN_ATTEMPTS = float(os.getenv("VKCLOUD_SLEEP_BETWEEN_ATTEMPTS", "0.6"))
ASSOC_WAIT = float(os.getenv("VKCLOUD_ASSOC_WAIT", "8.0"))
TARGET_NET_STR = os.getenv("VKCLOUD_TARGET_NET", "95.163.248.0/22")
TARGET_NET = ipaddress.ip_network(TARGET_NET_STR)
WORKERS_COUNT = int(os.getenv("VKCLOUD_WORKERS_COUNT", "1"))

# Уведомления через apprise (опционально)
APPRISE_URL = os.getenv("VKCLOUD_APPRISE_URL")  # URL для уведомлений через apprise

# Глобальная переменная для остановки всех воркеров
stop_event = threading.Event()
success_lock = threading.Lock()
success_achieved = False
success_ip = None
success_worker_id = None

# ========= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =========
def get_conn(auth_config=None) -> connection.Connection:
    """Создает новое соединение с OpenStack."""
    if auth_config is None:
        auth_config = get_auth()
    conn = connection.Connection(**auth_config)
    # поднимет исключение, если авторизация не удалась
    conn.authorize()
    return conn

def ensure_conn_alive(conn: connection.Connection) -> connection.Connection:
    """Проверяет, что токен жив; при невалидности — пересоздаёт соединение."""
    try:
        conn.authorize()
        return conn
    except (ks_exc.Unauthorized, ks_exc.NotFound):
        # токен протух/невалиден — пересобираем коннект
        return get_conn()

def in_target_range(ip: str) -> bool:
    try:
        return ipaddress.ip_address(ip) in TARGET_NET
    except ValueError:
        return False

def find_server(conn: connection.Connection, server_id_or_name: str):
    srv = conn.compute.find_server(server_id_or_name, ignore_missing=True)
    if not srv:
        raise SystemExit(f"❌ ВМ '{server_id_or_name}' не найдена")
    return conn.compute.get_server(srv.id)

def pick_port(conn: connection.Connection, server, explicit_port_id=None):
    if explicit_port_id:
        port = conn.network.get_port(explicit_port_id)
        if not port:
            raise SystemExit(f"❌ Порт '{explicit_port_id}' не найден")
        if port.device_id != server.id:
            raise SystemExit("❌ Указанный порт не принадлежит этой ВМ")
        return port
    ports = list(conn.network.ports(device_id=server.id))
    if not ports:
        raise SystemExit("❌ У ВМ нет сетевых портов")
    # активные первыми
    ports.sort(key=lambda p: (p.status != "ACTIVE", getattr(p, "created_at", "")))
    return ports[0]

def find_external_network(conn: connection.Connection, name_or_id: str | None):
    if name_or_id:
        return conn.network.find_network(name_or_id, ignore_missing=False)
    # авто-поиск первой внешней сети (router:external)
    for net in conn.network.networks():
        if getattr(net, "is_router_external", False):
            return net
    raise SystemExit("❌ Внешняя сеть не найдена. Укажите EXT_NET_NAME явно.")

def allocate_fip(conn: connection.Connection, ext_net_id: str):
    return conn.network.create_ip(floating_network_id=ext_net_id)

def associate_fip(conn: connection.Connection, fip, port):
    return conn.network.update_ip(fip, port_id=port.id)

def release_fip(conn: connection.Connection, fip):
    try:
        conn.network.delete_ip(fip, ignore_missing=True)
    except Exception as e:
        print(f"⚠️ Ошибка удаления IP {getattr(fip, 'floating_ip_address', '?')}: {e}", file=sys.stderr)

def wait_for_association(conn: connection.Connection, fip_id: str, port_id: str,
                         timeout: float = ASSOC_WAIT, poll: float = 0.5) -> bool:
    waited = 0.0
    while waited < timeout:
        # Проверяем, не нужно ли остановиться
        if stop_event.is_set():
            return False
        f = conn.network.get_ip(fip_id)
        if getattr(f, "port_id", None) == port_id:
            return True
        time.sleep(poll)
        waited += poll
        # Проверка после паузы
        if stop_event.is_set():
            return False
    return False

def send_notification(title: str, body: str, notification_type: str = "info"):
    """Отправляет уведомление через apprise, если настроено."""
    if not APPRISE_AVAILABLE or not APPRISE_URL:
        return
    
    try:
        apobj = Apprise()
        apobj.add(APPRISE_URL)
        
        # Определяем приоритет и иконку в зависимости от типа
        if notification_type == "success":
            body = f"✅ {body}"
        elif notification_type == "error":
            body = f"❌ {body}"
        else:
            body = f"ℹ️ {body}"
        
        apobj.notify(
            body=body,
            title=title,
        )
    except Exception as e:
        print(f"⚠️ Ошибка отправки уведомления: {e}", file=sys.stderr)

# ========= ОСНОВНОЙ СЦЕНАРИЙ =========
def worker(worker_id: int, server_id_or_name: str, port_id: str, ext_net_id: str):
    """Функция воркера для параллельного поиска floating IP."""
    global success_achieved
    
    auth_config = get_auth()
    conn = get_conn(auth_config)
    
    print(f"[Воркер {worker_id}] 🔗 Подключен к VK Cloud")
    
    while not stop_event.is_set():
        # Проверяем, не достиг ли успех другой воркер
        with success_lock:
            if success_achieved:
                print(f"[Воркер {worker_id}] 🛑 Остановка: успех достигнут другим воркером")
                break
        
        # Дополнительная проверка перед началом итерации
        if stop_event.is_set():
            break
        
        # Гарантируем живость токена перед каждой итерацией
        conn = ensure_conn_alive(conn)
        
        # Проверка после переавторизации
        if stop_event.is_set():
            break
        
        fip = None
        try:
            # Проверка перед выделением IP
            if stop_event.is_set():
                break
            
            # 1) выделяем floating IP
            fip = allocate_fip(conn, ext_net_id)
            ip = getattr(fip, "floating_ip_address", None)
            if not ip:
                print(f"[Воркер {worker_id}] ⚠️  Получен FIP без адреса — освобождаю и повторяю…")
                release_fip(conn, fip)
                fip = None
                time.sleep(SLEEP_BETWEEN_ATTEMPTS)
                continue

            print(f"[Воркер {worker_id}] 🔹 Выделен IP: {ip}")

            # Проверка после выделения IP
            if stop_event.is_set():
                release_fip(conn, fip)
                break

            # 2) проверяем диапазон
            if in_target_range(ip):
                # Проверка перед привязкой - может другой воркер уже успел
                if stop_event.is_set():
                    release_fip(conn, fip)
                    break
                
                print(f"[Воркер {worker_id}] ✅ IP {ip} принадлежит {TARGET_NET}. Привязываю к порту {port_id}…")
                port = conn.network.get_port(port_id)
                associate_fip(conn, fip, port)

                # Проверка после привязки
                if stop_event.is_set():
                    release_fip(conn, fip)
                    break

                # 3) ждём подтверждения привязки
                if wait_for_association(conn, fip.id, port_id):
                    global success_ip, success_worker_id
                    with success_lock:
                        if not success_achieved:
                            success_achieved = True
                            success_ip = ip
                            success_worker_id = worker_id
                            stop_event.set()
                            print(f"[Воркер {worker_id}] 🎉 Привязка подтверждена. Готово!")
                            print(f"[Воркер {worker_id}] 📌 Итоговый IP: {ip}")
                            # Отправляем уведомление об успехе
                            send_notification(
                                "VK Cloud: Floating IP привязан",
                                f"IP {ip} успешно привязан к ВМ воркером {worker_id}",
                                "success"
                            )
                        else:
                            # Другой воркер уже успел
                            print(f"[Воркер {worker_id}] ⚠️ Другой воркер уже привязал IP, освобождаю…")
                            release_fip(conn, fip)
                    break
                else:
                    print(f"[Воркер {worker_id}] ⚠️ Привязка не подтвердилась, освобождаю IP и продолжаю…")
                    release_fip(conn, fip)
                    fip = None

            else:
                print(f"[Воркер {worker_id}] ❌ IP {ip} не из {TARGET_NET}, удаляю…")
                release_fip(conn, fip)
                fip = None

        except KeyboardInterrupt:
            # Пробрасываем KeyboardInterrupt наверх
            raise

        except (ks_exc.Unauthorized, ks_exc.NotFound) as e:
            # На всякий случай, если токен «упал» посреди операций
            print(f"[Воркер {worker_id}] 🔁 Токен невалиден: {e}. Переавторизация…")
            try:
                if fip:
                    release_fip(conn, fip)
            finally:
                conn = get_conn(auth_config)

        except os_exc.HttpException as e:
            print(f"[Воркер {worker_id}] ⚠️ Ошибка API (HTTP): {e}", file=sys.stderr)
            if fip:
                release_fip(conn, fip)

        except Exception as e:
            if not stop_event.is_set():
                print(f"[Воркер {worker_id}] ⚠️ Неожиданная ошибка: {e}", file=sys.stderr)
            if fip:
                release_fip(conn, fip)

        # Проверка перед паузой
        if stop_event.is_set():
            break
        
        time.sleep(SLEEP_BETWEEN_ATTEMPTS)
        
        # Проверка после паузы
        if stop_event.is_set():
            break
    
    # Финальное сообщение при остановке
    if stop_event.is_set() and not success_achieved:
        print(f"[Воркер {worker_id}] 🛑 Остановлен")

def main():
    # Проверка обязательных параметров
    if not SERVER_ID_OR_NAME:
        raise SystemExit("❌ Отсутствует обязательная переменная окружения: VKCLOUD_SERVER_ID_OR_NAME")
    
    if WORKERS_COUNT < 1:
        raise SystemExit("❌ Количество воркеров должно быть >= 1")
    
    # Отправляем уведомление о старте
    send_notification(
        "VK Cloud: Запуск поиска Floating IP",
        f"Запущено {WORKERS_COUNT} воркер(ов) для поиска IP в подсети {TARGET_NET}",
        "info"
    )
    
    print("🔗 Подключаюсь к VK Cloud (password auth)…")
    conn = get_conn()

    # Ресурсы (получаем один раз для всех воркеров)
    server = find_server(conn, SERVER_ID_OR_NAME)
    port = pick_port(conn, server, PORT_ID)
    ext_net = find_external_network(conn, EXT_NET_NAME)

    print(f"🖥️  ВМ: {server.name} ({server.id})")
    print(f"🔌 Порт: {port.id}")
    print(f"🌐 Внешняя сеть: {ext_net.name} ({ext_net.id})")
    print(f"🎯 Целевая подсеть: {TARGET_NET}")
    print(f"👷 Количество воркеров: {WORKERS_COUNT}")
    print("🚀 Запускаю параллельный поиск подходящего floating IP…")

    # Запускаем воркеры
    threads = []
    for i in range(1, WORKERS_COUNT + 1):
        t = threading.Thread(
            target=worker,
            args=(i, SERVER_ID_OR_NAME, port.id, ext_net.id),
            daemon=False
        )
        t.start()
        threads.append(t)
        time.sleep(0.1)  # Небольшая задержка между запусками

    try:
        # Ждем завершения всех воркеров
        for t in threads:
            t.join()
        
        if success_achieved:
            print(f"\n✅ Успешно завершено! IP {success_ip} привязан воркером {success_worker_id}")
            return 0
        else:
            print("⚠️ Все воркеры завершились, но успех не достигнут")
            send_notification(
                "VK Cloud: Поиск завершен без результата",
                "Все воркеры завершились, но подходящий IP не найден",
                "error"
            )
            return 1
            
    except KeyboardInterrupt:
        print("\n🛑 Остановлено пользователем.")
        stop_event.set()
        send_notification(
            "VK Cloud: Поиск остановлен",
            "Поиск Floating IP остановлен пользователем",
            "info"
        )
        for t in threads:
            t.join(timeout=2)
        return 2

if __name__ == "__main__":
    sys.exit(main())