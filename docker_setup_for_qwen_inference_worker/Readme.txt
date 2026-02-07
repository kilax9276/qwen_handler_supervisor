Делаем сеть:
Вариант 1 (рекомендую): одна общая сеть (external) на всех, статические IP внутри неё
Шаг 1) Создайте сеть один раз (общую)
	# если camoufox-1 уже поднят — сначала остановите его, иначе сеть не удалить/переделать
	docker compose -p camoufox1 -f compose-1.yml down

	# на всякий: удалить старую сеть, если осталась
	docker network rm camoufox1_camoufox_net 2>/dev/null || true

	# создать ОДНУ общую сеть с нужной подсетью
	docker network create --driver bridge --subnet 172.30.0.0/24 camoufox_net




Рекомендованный способ: собрать один образ и запускать разные compose
1) Соберите образ один раз
docker build -t camoufox:latest .

2) В каждом docker-compose.yml используйте image: (а не build:)

compose-1.yml

services:
  camoufox:
    image: camoufox:latest
    container_name: camoufox-1
    ports:
      - "5903:5903"
      - "8000:8000"
    networks:
      camoufox_net:
        ipv4_address: 172.30.0.11

compose-2.yml
services:
  camoufox:
    image: camoufox:latest
    container_name: camoufox-2
    ports:
      - "5904:5903"
      - "8001:8000"
    networks:
      camoufox_net:
        ipv4_address: 172.30.0.12

3) Запускайте каждый как отдельный “проект”
docker compose -p camoufox1 -f compose-1.yml up -d
docker compose -p camoufox2 -f compose-2.yml up -d
