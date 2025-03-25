import socket

# Функции для конвертации IP-адресов
def ip_to_int(ip):
    return int.from_bytes(socket.inet_aton(ip), byteorder='big')

def int_to_ip(ip_int):
    return socket.inet_ntoa(ip_int.to_bytes(4, byteorder='big'))

# Диапазон IP-адресов
start_ip = "66.22.216.0"
end_ip = "66.22.217.255"

# Преобразуем IP в целые числа
start = ip_to_int(start_ip)
end = ip_to_int(end_ip)

# Открываем файл для записи
with open("ip_range.txt", "w") as file:
    for ip_int in range(start, end + 1):
        ip = int_to_ip(ip_int)
        file.write(f"{ip}\n")

print("IP-адреса записаны в файл ip_range.txt")
